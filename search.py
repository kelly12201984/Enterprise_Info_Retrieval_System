#!/usr/bin/env python3
"""
TankFinder quick search CLI.
Examples:
  python search.py "open top" --near 1 --compress
  python search.py "floating roof" --years 2018-2022
  python search.py --job 092-25 --show-files
"""
import argparse, re, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[0]
DB_PATH = ROOT / "tankfinder.db"

def build_match_expr(q: str, near: int | None) -> str:
    """
    Build an FTS5 MATCH expression.
    - If near is provided, chain terms with NEAR (no distance to avoid parser issues in some builds).
    - Otherwise AND all tokens.
    """
    toks = [t for t in re.split(r"\W+", q.lower()) if t]
    if not toks:
        return ""
    if near and len(toks) >= 2:
        # Use plain NEAR (default distance). Some SQLite builds error on "NEAR/N" via bound params.
        expr = f"\"{toks[0]}\""
        for t in toks[1:]:
            expr += f" NEAR \"{t}\""
        return expr
    # default: AND all tokens
    return " AND ".join(f"\"{t}\"" for t in toks)

def year_filters(years: str | None) -> list[str]:
    """
    Turn '2019-2022,2024' into LIKE filters on jobs.root_path (Windows path uses backslashes).
    """
    if not years:
        return []
    parts: list[str] = []
    for chunk in years.split(","):
        chunk = chunk.strip()
        if "-" in chunk:
            a, b = chunk.split("-", 1)
            try:
                a = int(a); b = int(b)
                for y in range(min(a, b), max(a, b) + 1):
                    parts.append(str(y))
            except ValueError:
                continue
        else:
            if chunk.isdigit():
                parts.append(chunk)
    return [f"j.root_path LIKE '%\\{y}\\%'" for y in sorted(set(parts))]

def main():
    ap = argparse.ArgumentParser(description="TankFinder search (CLI)")
    ap.add_argument("query", nargs="?", default="", help="keywords to search (filename + PDF/text)")
    ap.add_argument("--near", type=int, default=None, help="chain terms with NEAR (default distance)")
    ap.add_argument("--job", type=str, help="restrict to a specific job_id (e.g., 092-25)")
    ap.add_argument("--years", type=str, help="year filter, e.g. '2018-2021,2024'")
    ap.add_argument("--compress", action="store_true", help="only jobs with COMPRESS evidence")
    ap.add_argument("--ame", action="store_true", help="only jobs with AME evidence")
    ap.add_argument("--cad", action="store_true", help="only jobs with DWG/DXF")
    ap.add_argument("--pdf", action="store_true", help="only jobs with PDFs")
    ap.add_argument("--limit", type=int, default=50, help="max jobs to show")
    ap.add_argument("--show-files", dest="show_files", action="store_true", help="also list matching files")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH}", file=sys.stderr); sys.exit(2)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    where = []
    params = []

    # Build the FTS join & predicate for the main query
    if args.query:
        match_expr = build_match_expr(args.query, args.near)
        if not match_expr:
            print("No valid terms in query.", file=sys.stderr); sys.exit(1)
        fts_join = "JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
        fts_pred = "ff.content MATCH ?"
        params.append(match_expr)
    else:
        # No query: don't require an FTS row
        fts_join = "LEFT JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
        fts_pred = "1=1"

    # Job-level filters
    if args.job:
        where.append("j.job_id = ?"); params.append(args.job)
    if args.compress: where.append("j.has_compress = 1")
    if args.ame:      where.append("j.has_ame = 1")
    if args.cad:      where.append("j.has_dwg_dxf = 1")
    if args.pdf:      where.append("j.has_pdf = 1")
    year_like = year_filters(args.years)
    if year_like:
        where.append("(" + " OR ".join(year_like) + ")")

    where_sql = " AND ".join([fts_pred] + where) if where else fts_pred

    # Aggregate to jobs (rank by number of matching files)
    sql = f"""
    WITH hits AS (
      SELECT DISTINCT f.job_id, f.file_hash16
      FROM files f
      {fts_join}
      JOIN jobs j ON j.job_id = f.job_id
      WHERE f.deleted = 0 AND {where_sql}
    )
    SELECT j.job_id,
           j.root_path,
           j.has_compress, j.has_ame, j.has_dwg_dxf, j.has_pdf,
           COUNT(h.file_hash16) AS n_hits
    FROM hits h
    JOIN jobs j ON j.job_id = h.job_id
    GROUP BY j.job_id, j.root_path, j.has_compress, j.has_ame, j.has_dwg_dxf, j.has_pdf
    ORDER BY n_hits DESC, j.job_id
    LIMIT ?
    """
    rows = con.execute(sql, (*params, args.limit)).fetchall()
    if not rows:
        print("No results.")
        con.close()
        return

    # Print job summary
    for r in rows:
        badges = []
        if r["has_compress"]: badges.append("COMPRESS")
        if r["has_ame"]:      badges.append("AME")
        if r["has_dwg_dxf"]:  badges.append("CAD")
        if r["has_pdf"]:      badges.append("PDF")
        print(f"{r['job_id']}  [{', '.join(badges) or '-'}]  hits={r['n_hits']}  {r['root_path']}")

        # Optionally list the matching files for each job
        if args.show_files:
            if args.query:
                files_sql = """
                SELECT f.rel_path
                FROM files f
                JOIN fts_files ff ON ff.file_hash16 = f.file_hash16
                WHERE f.deleted=0 AND f.job_id=? AND ff.content MATCH ?
                ORDER BY f.rel_path
                LIMIT 50
                """
                file_params = (r["job_id"], params[0])
            else:
                files_sql = """
                SELECT f.rel_path
                FROM files f
                WHERE f.deleted=0 AND f.job_id=?
                ORDER BY f.rel_path
                LIMIT 50
                """
                file_params = (r["job_id"],)

            for fr in con.execute(files_sql, file_params):
                print(f"   - {fr['rel_path']}")

    con.close()

if __name__ == "__main__":
    main()
