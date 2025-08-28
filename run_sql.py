import sqlite3, sys, pathlib
DB = pathlib.Path("tankfinder.db")
sql = sys.stdin.read()
con = sqlite3.connect(DB)
con.executescript(sql)
con.commit()
print("OK")
