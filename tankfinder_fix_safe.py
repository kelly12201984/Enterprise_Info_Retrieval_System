# tankfinder_fix_safe.py
import os, sys, sqlite3, time, urllib.parse

def file_presence(db):
    wal, shm = db + "-wal", db + "-shm"
    print(f"[INFO] DB exists: {os.path.exists(db)} size={os.path.getsize(db) if os.path.exists(db) else 'n/a'}")
    print(f"[INFO] WAL exists: {os.path.exists(wal)} size={os.path.getsize(wal) if os.path.exists(wal) else 'n/a'}")
    print(f"[INFO] SHM exists: {os.path.exists(shm)} size={os.path.getsize(shm) if os.path.exists(shm) else 'n/a'}")

def try_read_only(db):
    uri = "file:" + urllib.parse.quote(db, safe="/:\\") + "?mode=ro&cache=shared"
    con = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
    try:
        con.execute("PRAGMA query_only=ON;")
        con.execute("PRAGMA busy_timeout=8000;")
        cnt = con.execute("SELECT count(*) FROM sqlite_schema;").fetchone()[0]
        print(f"[OK ] RO test succeeded (sqlite_schema count={cnt})")
    finally:
        con.close()

def try_report_or_set_wal(db):
    """
    Best-effort: if locked, just report and skip.
    """
    try:
        con = sqlite3.connect(db, timeout=2, isolation_level=None)
    except sqlite3.OperationalError as e:
        print(f"[WARN] RW connect blocked: {e} (will skip WAL change)")
        return
    try:
        try:
            jm0 = con.execute("PRAGMA journal_mode;").fetchone()[0]
            print(f"[INFO] journal_mode={jm0}")
        except sqlite3.OperationalError as e:
            print(f"[WARN] journal_mode check blocked: {e}")
            jm0 = None

        if jm0 and str(jm0).lower() != "wal":
            # Try once, but don't die if locked
            try:
                got = con.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
                print(f"[FIX] journal_mode -> {got}")
            except sqlite3.OperationalError as e:
                print(f"[WARN] WAL switch blocked: {e} (skipping)")
        # Set non-invasive pragmas; ignore if locked
        for pragma in (
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA busy_timeout=8000;",
            "PRAGMA temp_store=MEMORY;",
            "PRAGMA wal_autocheckpoint=2000;",
            "PRAGMA optimize;",
        ):
            try:
                con.execute(pragma)
            except sqlite3.OperationalError:
                pass

        # Passive checkpoint if possible
        try:
            ck = con.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
            print(f"[INFO] wal_checkpoint(PASSIVE) -> {ck}")
        except sqlite3.OperationalError as e:
            print(f"[WARN] checkpoint blocked: {e}")
    finally:
        con.close()

def probe_writer_now(db):
    try:
        t = sqlite3.connect(db, timeout=1, isolation_level=None)
        try:
            t.execute("PRAGMA busy_timeout=500;")
            try:
                t.execute("BEGIN IMMEDIATE;")
                t.execute("ROLLBACK;")
                print("[LOCK] Writer active now: False")
            except sqlite3.OperationalError as e:
                print(f"[LOCK] Writer active now: { 'database is locked' in str(e).lower() }")
        finally:
            t.close()
    except sqlite3.OperationalError as e:
        print(f"[LOCK] Probe could not connect RW: {e} (likely writer holds exclusive)")

def main():
    if len(sys.argv) != 2:
        print("Usage: python tankfinder_fix_safe.py <path-to-tankfinder.db>")
        sys.exit(1)
    db = sys.argv[1]
    print("=== TankFinder Safe Diagnose ===")
    print("[PATH]", db)
    if not os.path.exists(db):
        print("[FATAL] DB not found"); sys.exit(2)
    file_presence(db)
    try_report_or_set_wal(db)  # best-effort; won’t crash on lock
    probe_writer_now(db)       # tells us if indexer is currently writing
    try_read_only(db)          # confirms GUI-style read works

if __name__ == "__main__":
    main()
