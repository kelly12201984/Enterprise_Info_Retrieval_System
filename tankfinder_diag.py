import os, sys, time, sqlite3, pathlib, traceback

def p(msg=""):
    print(msg)

def main(db_path: str):
    db_path = str(pathlib.Path(db_path).resolve())
    p("=== TankFinder DB Diagnostic ===")
    p(f"DB path: {db_path}")
    p(f"Python: {sys.version.split()[0]}")
    p(f"sqlite3 module version: {sqlite3.version}")
    p(f"SQLite library version: {sqlite3.sqlite_version}")
    p()

    # Files present?
    wal = db_path + "-wal"
    shm = db_path + "-shm"
    p(f"Exists: DB={os.path.exists(db_path)} WAL={os.path.exists(wal)} SHM={os.path.exists(shm)}")
    if os.path.exists(db_path):
        p(f"DB size: {os.path.getsize(db_path):,} bytes")
    if os.path.exists(wal):
        p(f"WAL size: {os.path.getsize(wal):,} bytes")
    p()

    # 1) Open read-only (will fail if file missing or perms)
    try:
        import urllib.parse
        uri = "file:" + urllib.parse.quote(db_path, safe="/:\\") + "?mode=ro&cache=shared"
        con_ro = sqlite3.connect(uri, uri=True, timeout=2, isolation_level=None)
        p("[RO] Connected OK")
        try:
            cur = con_ro.execute("SELECT count(*) FROM sqlite_schema;")
            p(f"[RO] sqlite_schema count: {cur.fetchone()[0]}")
            cur.close()
        finally:
            con_ro.close()
    except Exception as e:
        p("[RO] FAILED to open / read")
        p("Error: " + repr(e))
        p(traceback.format_exc())
        p()

    # 2) Open read-write (autocommit) to inspect pragmas
    con = None
    try:
        con = sqlite3.connect(db_path, timeout=3, isolation_level=None)
        p("[RW] Connected OK")
        # Report key pragmas
        def q1(sql):
            try:
                return con.execute(sql).fetchone()[0]
            except Exception:
                return None

        jm = q1("PRAGMA journal_mode;")
        syn = q1("PRAGMA synchronous;")
        acu = q1("PRAGMA wal_autocheckpoint;")
        qro = q1("PRAGMA query_only;")
        ruo = q1("PRAGMA read_uncommitted;")
        p(f"[RW] journal_mode={jm}, synchronous={syn}, wal_autocheckpoint={acu}, query_only={qro}, read_uncommitted={ruo}")

        ps = q1("PRAGMA page_size;")
        pc = q1("PRAGMA page_count;")
        fl = q1("PRAGMA freelist_count;")
        p(f"[RW] pages: size={ps}, count={pc}, freelist={fl}")

        # 3) Is a writer currently holding a lock? (non-destructive)
        # BEGIN IMMEDIATE needs a RESERVED lock; if someone else is writing, this raises "database is locked".
        writer_active = None
        try:
            con.execute("PRAGMA busy_timeout=500;")
            con.execute("BEGIN IMMEDIATE;")
            con.execute("ROLLBACK;")
            writer_active = False
        except sqlite3.OperationalError as e:
            writer_active = "database is locked" in str(e).lower()
        p(f"[LOCK] Writer active now: {writer_active}")

        # 4) WAL checkpoint status (PASSIVE = safe; does not force anything)
        # returns (busy, log, checkpointed)
        ck = None
        try:
            ck = con.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
        except Exception:
            pass
        p(f"[WAL] checkpoint(PASSIVE) result: {ck}")
    except Exception as e:
        p("[RW] FAILED to open")
        p("Error: " + repr(e))
        p(traceback.format_exc())
    finally:
        if con:
            con.close()

    # 5) Database list mapping (useful for confirming main db file)
    try:
        con2 = sqlite3.connect(db_path, timeout=2, isolation_level=None)
        rows = con2.execute("PRAGMA database_list;").fetchall()
        p("[DBLIST] attached databases:")
        for r in rows:
            p(f"  name={r[1]} file={r[2]}")
        con2.close()
    except Exception:
        pass

    p("\n=== Guidance ===")
    p("- If journal_mode != WAL, we should switch to WAL so GUI reads don't block on writer.")
    p("- If 'Writer active now: True', the indexer has a transaction open (big batch, VACUUM, or long write).")
    p("- Nonzero 'WAL size' with large 'log' in checkpoint result suggests long-uncheckpointed WAL; short commits help.")
    p("- If RO fails but RW works, permissions or path mapping might be off.")
    p("Paste this entire output to me.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tankfinder_diag.py <path-to-tankfinder.db>")
        sys.exit(1)
    main(sys.argv[1])
