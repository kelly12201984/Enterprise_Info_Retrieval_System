# run_sql.py — run a .sql file against tankfinder.db, print & save CSVs
import sys, sqlite3, pathlib, csv, time

ROOT = pathlib.Path(__file__).parent
DB   = ROOT / "tankfinder.db"
OUT  = ROOT / "SQL_results"; OUT.mkdir(exist_ok=True)

def main(sql_path: pathlib.Path):
    sql = sql_path.read_text(encoding="utf-8")
    con = sqlite3.connect(DB)
    cur = con.cursor()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    idx = 0

    def run(stmt: str):
        nonlocal idx
        stmt = stmt.strip()
        if not stmt:
            return
        idx += 1
        try:
            cur.execute(stmt)
            if cur.description:
                cols = [c[0] for c in cur.description]
                rows = cur.fetchall()
                print(f"\n-- Result {idx}: {stmt.replace(chr(10),' ')[:120]}")
                print("\t".join(cols))
                for r in rows:
                    print("\t".join("" if v is None else str(v) for v in r))
                out = OUT / f"{sql_path.stem}_{stamp}_{idx:02}.csv"
                with out.open("w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f); w.writerow(cols); w.writerows(rows)
                print(f"[saved] {out}")
            else:
                print(f"\n-- OK ({cur.rowcount} row(s) affected)")
        except Exception as e:
            print(f"\n[error] {e}\n  in: {stmt}")

    for stmt in sql.split(";"):
        run(stmt)

    con.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_sql.py path\\to\\file.sql"); sys.exit(2)
    main(pathlib.Path(sys.argv[1]))
