#!/usr/bin/env python3
# TankFinder EDA — runs against tankfinder.db and writes summaries to eda_out/
import os, re, csv, sqlite3, math
from pathlib import Path
from collections import Counter, defaultdict
DB = Path(__file__).with_name("tankfinder.db")
OUT = Path(__file__).with_name("eda_out")

TEXTY_EXT = {".txt",".xml",".html",".htm",".xmt_txt",".csv"}
CALC_EXT  = TEXTY_EXT | {".cw7",".mdl",".out",".lst"}  # evidence (binary files counted as evidence)

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def infer_year_from_path(path: str):
    # grab the first 4-digit 19xx/20xx in the path
    m = re.search(r"\b(19|20)\d{2}\b", path)
    return int(m.group(0)) if m else None

def write_csv(name, rows, header):
    with (OUT/name).open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(header); w.writerows(rows)

def pct(a,b): return 0 if b==0 else round(100*a/b,1)

def main():
    if not DB.exists():
        print(f"DB not found: {DB}"); return
    ensure_dir(OUT)
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    # ---------- Pull base tables ----------
    jobs = con.execute("SELECT job_id, root_path, has_compress, has_ame, has_dwg_dxf, has_pdf FROM jobs").fetchall()
    files = con.execute("SELECT job_id, rel_path, ext, size_bytes, mtime_utc, detector_hits FROM files WHERE deleted=0").fetchall()

    # ---------- Derive metrics ----------
    # year per job
    job_year = {}
    for j in jobs:
        y = infer_year_from_path(j["root_path"])
        job_year[j["job_id"]] = y

    # calc evidence per job (on the fly)
    job_has_calc = defaultdict(int)
    job_has_texty = defaultdict(int)
    job_pathlen = defaultdict(list)
    job_depths = defaultdict(list)
    long_path_buckets = Counter()
    ext_counts = Counter()

    for f in files:
        jid = f["job_id"]
        ext = (f["ext"] or "").lower()
        ext_counts[ext] += 1

        # path stats
        full = (next(j["root_path"] for j in jobs if j["job_id"]==jid) + "\\" + f["rel_path"])
        plen = len(full)
        job_pathlen[jid].append(plen)
        depth = f["rel_path"].count("\\") + 1
        job_depths[jid].append(depth)

        if plen <= 180: long_path_buckets["<=180"] += 1
        elif plen <= 260: long_path_buckets["181-260"] += 1
        elif plen <= 320: long_path_buckets["261-320"] += 1
        elif plen <= 400: long_path_buckets["321-400"] += 1
        else: long_path_buckets[">400"] += 1

        # calc evidence logic
        hits = (f["detector_hits"] or "").lower()
        if ("compress" in hits) or ("ametank" in hits) or (ext in CALC_EXT):
            job_has_calc[jid] = 1
        if ext in TEXTY_EXT:
            job_has_texty[jid] = 1

    # ---------- Summaries ----------
    total_jobs = len(jobs)
    total_files = len(files)
    jobs_2019p = [j for j in jobs if (job_year.get(j["job_id"]) or 0) >= 2019]
    jobs_pre2019 = [j for j in jobs if ((job_year.get(j["job_id"]) or 0) and job_year[j["job_id"]] < 2019)]

    calc_jobs = sum(job_has_calc[j["job_id"]] for j in jobs)
    calc_jobs_2019p = sum(job_has_calc[j["job_id"]] for j in jobs_2019p)
    calc_jobs_pre2019 = sum(job_has_calc[j["job_id"]] for j in jobs_pre2019)

    # by year coverage
    year_buckets = defaultdict(list)
    for j in jobs:
        y = job_year.get(j["job_id"])
        year_buckets[y].append(j["job_id"])
    cov_by_year = []
    for y, jids in sorted(year_buckets.items()):
        if y is None: continue
        n = len(jids)
        n_calc = sum(job_has_calc[jid] for jid in jids)
        n_texty = sum(job_has_texty[jid] for jid in jids)
        cov_by_year.append([y, n, n_calc, pct(n_calc,n), n_texty, pct(n_texty,n)])

    # pathlen/depth per year (medians)
    def median(lst): 
        if not lst: return None
        s=sorted(lst); k=len(s); 
        return s[k//2] if k%2 else (s[k//2-1]+s[k//2])/2
    pathlen_by_year = []
    depth_by_year = []
    for y, jids in sorted(year_buckets.items()):
        if y is None: continue
        lens=[]; deps=[]
        for jid in jids:
            lens += job_pathlen.get(jid, [])
            deps += job_depths.get(jid, [])
        pathlen_by_year.append([y, len(lens), median(lens), max(lens) if lens else None])
        depth_by_year.append([y, len(deps), median(deps), max(deps) if deps else None])

    # jobs with CAD but no calc evidence (potential gaps)
    cad_no_calc = []
    for j in jobs:
        jid = j["job_id"]
        if j["has_dwg_dxf"] and not job_has_calc[jid]:
            cad_no_calc.append([jid, job_year.get(jid), j["root_path"]])

    # top extensions
    top_ext = sorted(ext_counts.items(), key=lambda x: x[1], reverse=True)[:50]

    # write outputs
    write_csv("summary.csv", [
        ["total_jobs", total_jobs],
        ["total_files", total_files],
        ["jobs_2019plus", len(jobs_2019p)],
        ["jobs_pre2019", len(jobs_pre2019)],
        ["jobs_with_calc_any", calc_jobs, f"{pct(calc_jobs,total_jobs)}%"],
        ["jobs_with_calc_2019plus", calc_jobs_2019p, f"{pct(calc_jobs_2019p,len(jobs_2019p) or 1)}%"],
        ["jobs_with_calc_pre2019", calc_jobs_pre2019, f"{pct(calc_jobs_pre2019,len(jobs_pre2019) or 1)}%"],
    ], ["metric","value","pct"])

    write_csv("coverage_by_year.csv", cov_by_year, ["year","jobs","jobs_with_calc","calc_pct","jobs_with_texty","texty_pct"])
    write_csv("pathlen_by_year.csv", pathlen_by_year, ["year","n_files","median_path_len","max_path_len"])
    write_csv("depth_by_year.csv", depth_by_year, ["year","n_files","median_depth","max_depth"])
    write_csv("long_path_hist.csv", sorted(long_path_buckets.items(), key=lambda x: x[0]), ["bucket","files"])
    write_csv("top_extensions.csv", [(k or "(none)",v) for k,v in top_ext], ["ext","files"])
    write_csv("cad_but_no_calc.csv", sorted(cad_no_calc, key=lambda r: (r[1] or 0, r[0])), ["job_id","year","root_path"])

    # sample of calc artifacts to eyeball rules
    calc_examples = []
    for f in files:
        ext = (f["ext"] or "").lower()
        hits = (f["detector_hits"] or "").lower()
        if ("compress" in hits) or ("ametank" in hits) or (ext in CALC_EXT):
            full = f["rel_path"]
            calc_examples.append([f["job_id"], ext, full[:240]])
    write_csv("calc_examples_sample.csv", calc_examples[:500], ["job_id","ext","rel_path"])

    print(f"[EDA] wrote outputs to {OUT}")
    con.close()

if __name__ == "__main__":
    main()
