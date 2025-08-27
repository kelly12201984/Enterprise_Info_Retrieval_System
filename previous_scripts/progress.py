import os, time, sqlite3
DB = r"P:\Chris\Tankfinder\tankfinder.db"  # fix path/casing if needed

def snap():
    con = sqlite3.connect(DB)
    c = con.cursor()
    jobs = c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    files = c.execute("SELECT COUNT(*) FROM files WHERE deleted=0").fetchone()[0]
    fts   = c.execute("SELECT COUNT(*) FROM fts_files").fetchone()[0]
    last  = c.execute("SELECT job_id,last_seen FROM jobs ORDER BY last_seen DESC LIMIT 1").fetchone()
    con.close()
    return jobs, files, fts, (last or ("-", "-"))

def main():
    prev = None
    while True:
        try:
            jobs, files, fts, last = snap()
        except Exception as e:
            jobs=files=fts=0; last=("-", f"{e}")
        os.system("cls")
        print("TankFinder progress (read-only)\n")
        print(f"Jobs: {jobs:,}")
        print(f"Files (active): {files:,}")
        print(f"FTS rows: {fts:,}")
        print(f"Most recently seen job: {last[0]}  @ {last[1]}")
        if prev:
            dj = jobs - prev[0]; df = files - prev[1]; dt = fts - prev[2]
            print(f"\nΔ since last refresh: jobs {dj:+}, files {df:+}, fts {dt:+}")
        prev = (jobs, files, fts)
        time.sleep(5)

if __name__ == "__main__":
    main()
