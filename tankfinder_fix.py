# tankfinder_fix.py
import os, sys, sqlite3, time, traceback, pathlib, urllib.parse

def to_unc(p: str) -> str:
    p = os.path.abspath(p)
    if p.startswith("\\\\?\\") or p.startswith("\\\\"):
        return p
    return p  # drive letters are fine too, but UNC is preferred

def ensure_exists(db_path: str):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")

def enable_wal(db_path: str):
    con = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    try:
        jm0 = con.execute("PRAGMA journal_mode;").fetchone()[0]
        if str(jm0).lower() != "wal":
            got = jm0
            for _ in range(6):
                try:
                    got = con.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
                    break
                except sqlite3.OperationalError:
                    time.sleep(2)
            print(f"[FIX] journal_mode: {jm0} -> {got}")
        else:
            print("[OK ] journal_mode already WAL")

        for pragma in (
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA busy_timeout=8000;",
            "PRAGMA temp_store=MEMORY;",
            "PRAGMA wal_autocheckpoint=2000;",
            "PRAGMA optimize;",
        ):
            try:
                con.execute(pragma)
            except Exception:
                pass

        try:
            ck = con.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
            print(f"[INFO] wal_checkpoint(PASSIVE) -> {ck}")
        except Exception as e:
            print(f"[WARN] checkpoint skipped: {e}")
    finally:
        con.close()

def probe_writer_active(db_path: str):
    tmp = sqlite3.connect(db_path, timeout=1, isolation_level=None)
    try:
        tmp.execute("PRAGMA busy_timeout=500;")
        try:
            tmp.execute("BEGIN IMMEDIATE;")
            tmp.execute("ROLLBACK;")
            return False
        except sqlite3.OperationalError as e:
            return "database is locked" in str(e).lower()
    finally:
        tmp.close()

def try_ro(db_path: str):
    uri = "file:" + urllib.parse.quote(db_path, safe="/:\\") + "?mode=ro&cache=shared"
    con = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
    try:
        con.execute("PRAGMA query_only=ON;")
        con.execute("PRAGMA busy_timeout=8000;")
        cnt = con.execute("SELECT count(*) FROM sqlite_schema;").fetchone()[0]
        print(f"[OK ] RO test succeeded (sqlite_schema count={cnt})")
    finally:
        con.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python tankfinder_fix.py <path-to-tankfinder.db-or-folder>")
        sys.exit(1)

    arg = sys.argv[1]
    db_path = None
    p = pathlib.Path(arg)

    if p.is_dir():
        # pick the biggest plausible DB in the folder
        cands = sorted(
            [q for q in p.glob("*.db")] + [p / "tankfinder.db"],
            key=lambda x: x.stat().st_size if x.exists() else -1,
            reverse=True
        )
        if cands and cands[0].exists():
            db_path = str(cands[0])
        else:
            # also accept a DB named exactly 'tankfinder' without .db
            nf = p / "tankfinder"
            if nf.exists():
                db_path = str(nf)
    else:
        db_path = str(p)

    if not db_path:
        print("[FATAL] Could not locate a DB file in", arg); sys.exit(2)

    db_path = to_unc(db_path)
    print("=== TankFinder Diagnose + Fix ===")
    print("[PATH]", db_path)
    ensure_exists(db_path)

    # show presence
    wal = db_path + "-wal"; shm = db_path + "-shm"
    print(f"[INFO] DB exists: {os.path.exists(db_path)} size={os.path.getsize(db_path):,}")
    print(f"[INFO] WAL exists: {os.path.exists(wal)} size={os.path.getsize(wal) if os.path.exists(wal) else 'n/a'}")
    print(f"[INFO] SHM exists: {os.path.exists(shm)} size={os.path.getsize(shm) if os.path.exists(shm) else 'n/a'}")

    enable_wal(db_path)
    w = probe_writer_active(db_path)
    print(f"[LOCK] Writer active now: {w}")
    try_ro(db_path)

if __name__ == "__main__":
    main()
