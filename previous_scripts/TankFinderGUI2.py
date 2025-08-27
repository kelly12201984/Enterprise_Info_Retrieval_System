# app/TankFinderGUI.py
#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, messagebox
import os, re, sqlite3, subprocess, threading, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "tankfinder.db"
INDEXER = ROOT / "indexer" / "indexer.py"

def build_match_expr(q: str, use_near: bool, near_dist: int = 50) -> str:
    toks = [t for t in re.split(r"\W+", (q or "").lower()) if t]
    if not toks:
        return ""
    if use_near and len(toks) >= 2:
        return f" NEAR/{near_dist} ".join(f"\"{t}\"" for t in toks)
    return " AND ".join(f"\"{t}\"" for t in toks)

def parse_year_list(years: str | None):
    if not years: return []
    out = []
    for chunk in years.split(","):
        c = chunk.strip()
        if "-" in c:
            a,b = c.split("-",1)
            try:
                a=int(a); b=int(b)
                out += [str(y) for y in range(min(a,b), max(a,b)+1)]
            except: pass
        elif c.isdigit():
            out.append(c)
    return sorted(set(out))

def open_file_resilient(path: Path) -> None:
    p = str(path)
    try:
        os.startfile(p)  # type: ignore[attr-defined]
    except Exception as e1:
        try:
            subprocess.run(f'explorer /select,"{p}"', shell=True)
        except Exception as e2:
            raise RuntimeError(f"{e1}\n\nExplorer fallback failed: {e2}")

def open_folder(path: Path) -> None:
    try:
        os.startfile(str(path))  # type: ignore[attr-defined]
    except Exception as e:
        raise RuntimeError(str(e))

HEADINGS = {
    "job_id":"job_id", "hits":"hits",
    "pdfs":"#pdf", "cad":"#cad", "compress":"#compress", "ame":"#ame",
    "badges":"badges", "root_path":"root_path",
}

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TankFinder"); self.geometry("1220x740"); self.minsize(980,560)

        top = ttk.Frame(self, padding=8); top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Search:").grid(row=0, column=0, sticky="w")
        self.q_var = tk.StringVar()
        self.q_entry = ttk.Entry(top, textvariable=self.q_var, width=46)
        self.q_entry.grid(row=0, column=1, padx=(4,12), sticky="we")
        self.q_entry.focus_set()
        self.q_entry.bind("<Return>", lambda e: self.run_search())

        self.near_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="Use NEAR", variable=self.near_var).grid(row=0, column=2, sticky="w")

        ttk.Label(top, text="Years:").grid(row=0, column=3, sticky="e", padx=(12,2))
        self.years_var = tk.StringVar(value="2019-2025")
        ye = ttk.Entry(top, textvariable=self.years_var, width=16)
        ye.grid(row=0, column=4, sticky="w")
        ye.bind("<Return>", lambda e: self.run_search())

        self.compress_var = tk.BooleanVar(value=False)
        self.ame_var      = tk.BooleanVar(value=False)
        self.cad_var      = tk.BooleanVar(value=False)
        self.pdf_var      = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="COMPRESS", variable=self.compress_var).grid(row=0, column=5, padx=(12,0))
        ttk.Checkbutton(top, text="AME",      variable=self.ame_var).grid(row=0, column=6)
        ttk.Checkbutton(top, text="CAD",      variable=self.cad_var).grid(row=0, column=7)
        ttk.Checkbutton(top, text="PDF",      variable=self.pdf_var).grid(row=0, column=8)

        ttk.Label(top, text="Limit:").grid(row=0, column=9, sticky="e", padx=(12,2))
        self.limit_var = tk.IntVar(value=50)
        le = ttk.Entry(top, textvariable=self.limit_var, width=7)
        le.grid(row=0, column=10, sticky="w")
        le.bind("<Return>", lambda e: self.run_search())

        ttk.Button(top, text="Search", command=self.run_search).grid(row=0, column=11, padx=(12,0))
        ttk.Button(top, text="SQL…", command=self.open_sql_console).grid(row=0, column=12, padx=(8,0))
        self.full_refresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Full", variable=self.full_refresh_var).grid(row=0, column=13, padx=(12,2))
        ttk.Button(top, text="Refresh Index", command=self.refresh_index).grid(row=0, column=14)
        top.columnconfigure(1, weight=1)

        panes = ttk.Panedwindow(self, orient=tk.HORIZONTAL); panes.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0,8))

        left = ttk.Frame(panes); ttk.Label(left, text="Jobs (ranked by hits)").pack(anchor="w")
        self.job_cols = ("job_id","hits","pdfs","cad","compress","ame","badges","root_path")
        jw = ttk.Frame(left); jw.pack(fill=tk.BOTH, expand=True)
        self.jobs = ttk.Treeview(jw, columns=self.job_cols, show="headings")
        js = ttk.Scrollbar(jw, orient="vertical", command=self.jobs.yview)
        self.jobs.configure(yscrollcommand=js.set)
        for c in self.job_cols:
            self.jobs.heading(c, text=HEADINGS[c], command=lambda col=c: self.sort_tree(self.jobs, col))
            self.jobs.column(c, width={"job_id":90,"hits":70,"pdfs":60,"cad":60,"compress":90,"ame":60,"badges":180,"root_path":650}[c], anchor="w")
        self.jobs.grid(row=0, column=0, sticky="nsew"); js.grid(row=0, column=1, sticky="ns")
        jw.rowconfigure(0, weight=1); jw.columnconfigure(0, weight=1)
        self.jobs.bind("<<TreeviewSelect>>", self.on_job_select)
        self.jobs.bind("<Double-1>", self.on_open_job)
        self.jobs.bind("<Return>",   self.on_open_job)
        btns = ttk.Frame(left); btns.pack(fill=tk.X, pady=(4,0))
        ttk.Button(btns, text="Open Job Folder", command=self.on_open_job).pack(side=tk.LEFT)
        ttk.Button(btns, text="Copy Job Path", command=self.copy_job_path).pack(side=tk.LEFT, padx=6)
        panes.add(left, weight=1)

        right = ttk.Frame(panes)
        hdr = ttk.Frame(right); hdr.pack(fill=tk.X)
        ttk.Label(hdr, text="Files").pack(side=tk.LEFT)
        ttk.Label(hdr, text="   Show:").pack(side=tk.LEFT)
        self.file_filter_var = tk.StringVar(value="All")
        self.file_filter = ttk.Combobox(hdr, textvariable=self.file_filter_var, width=12,
                                        values=["All","PDFs","CAD","COMPRESS","AME","Text"])
        self.file_filter.state(["readonly"])
        self.file_filter.pack(side=tk.LEFT, padx=(4,0))
        self.file_filter.bind("<<ComboboxSelected>>", lambda e: self.refresh_file_list())

        fw = ttk.Frame(right); fw.pack(fill=tk.BOTH, expand=True)
        self.files = ttk.Treeview(fw, columns=("rel_path",), show="headings")
        fs = ttk.Scrollbar(fw, orient="vertical", command=self.files.yview)
        self.files.configure(yscrollcommand=fs.set)
        self.files.heading("rel_path", text="rel_path", command=lambda: self.sort_tree(self.files, "rel_path"))
        self.files.column("rel_path", width=780, anchor="w")
        self.files.grid(row=0, column=0, sticky="nsew"); fs.grid(row=0, column=1, sticky="ns")
        fw.rowconfigure(0, weight=1); fw.columnconfigure(0, weight=1)
        self.files.bind("<Double-1>", self.on_open_file)
        self.files.bind("<Return>",   self.on_open_file)
        fbtns = ttk.Frame(right); fbtns.pack(fill=tk.X, pady=(4,0))
        ttk.Button(fbtns, text="Open File", command=self.on_open_file).pack(side=tk.LEFT)
        ttk.Button(fbtns, text="Copy File Path", command=self.copy_file_path).pack(side=tk.LEFT, padx=6)
        panes.add(right, weight=1)

        self.status = tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.status, anchor="w", padding=(8,4)).pack(side=tk.BOTTOM, fill=tk.X)

        if not DB_PATH.exists():
            messagebox.showerror("TankFinder", f"Database not found:\n{DB_PATH}")
            self.destroy(); return
        self.con = sqlite3.connect(f"file:{DB_PATH}?mode=ro&cache=shared", uri=True, timeout=5.0)
        self.con.row_factory = sqlite3.Row
        try:
            self.con.execute("PRAGMA query_only=ON"); self.con.execute("PRAGMA busy_timeout=4000")
        except Exception: pass

        try:
            cols = [r[1] for r in self.con.execute("PRAGMA table_info(jobs)").fetchall()]
            self.has_job_year = ("job_year" in cols)
        except Exception:
            self.has_job_year = False

        self.bind("<Escape>", lambda e: (self.q_var.set(""), self.run_search()))
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def set_status(self, msg: str): self.after(0, self.status.set, msg)

    def on_close(self):
        try: self.con.close()
        except Exception: pass
        self.destroy()

    def sort_tree(self, tv: ttk.Treeview, col: str):
        data = [(tv.set(k, col), k) for k in tv.get_children("")]
        try: data = [(float(v) if v not in ("","-") else -1e99, k) for v,k in data]
        except Exception: pass
        ascending = not tv.heading(col, "text").endswith("▲")
        data.sort(reverse=not ascending)
        for i,(_,k) in enumerate(data): tv.move(k, "", i)
        columns = self.job_cols if tv is self.jobs else ("rel_path",)
        for c in columns: tv.heading(c, text=HEADINGS.get(c,c))
        tv.heading(col, text=f"{HEADINGS.get(col,col)} {'▲' if ascending else '▼'}")

    def run_search(self):
        self.set_status("Searching…")
        where, params = [], []
        q = self.q_var.get().strip()
        match_expr = build_match_expr(q, use_near=self.near_var.get())
        if q:
            fts_join = "JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
            fts_pred = "ff.content MATCH ?"; params.append(match_expr)
        else:
            fts_join = "LEFT JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
            fts_pred = "1=1"

        if self.compress_var.get(): where.append("j.has_compress = 1")
        if self.ame_var.get():      where.append("j.has_ame = 1")
        if self.cad_var.get():      where.append("j.has_dwg_dxf = 1")
        if self.pdf_var.get():      where.append("j.has_pdf = 1")

        years = parse_year_list(self.years_var.get())
        if years:
            if self.has_job_year:
                where.append(f"j.job_year IN ({','.join('?' for _ in years)})")
                params.extend(years)
            else:
                likes = [f"j.root_path LIKE '%\\{y}\\%'" for y in years]
                where.append("(" + " OR ".join(likes) + ")")

        where_sql = " AND ".join([fts_pred] + where) if where else fts_pred
        sql = f"""
        WITH hits AS (
          SELECT DISTINCT f.job_id, f.file_hash16
          FROM files f
          {fts_join}
          JOIN jobs j ON j.job_id=f.job_id
          WHERE f.deleted=0 AND {where_sql}
        )
        SELECT
          j.job_id, j.root_path,
          j.has_compress, j.has_ame, j.has_dwg_dxf, j.has_pdf,
          COUNT(h.file_hash16) AS n_hits,
          (SELECT COUNT(*) FROM files x WHERE x.job_id=j.job_id AND x.deleted=0 AND x.ext='.pdf') AS n_pdf,
          (SELECT COUNT(*) FROM files x WHERE x.job_id=j.job_id AND x.deleted=0 AND x.ext IN('.dwg','.dxf')) AS n_cad,
          (SELECT COUNT(*) FROM files x WHERE x.job_id=j.job_id AND x.deleted=0 AND (
               instr(x.detector_hits,'compress')>0 OR x.ext IN('.cw7','.xml','.out','.lst','.txt','.html','.htm'))) AS n_compress,
          (SELECT COUNT(*) FROM files x WHERE x.job_id=j.job_id AND x.deleted=0 AND (
               instr(x.detector_hits,'ametank')>0 OR x.ext IN('.mdl','.xmt_txt','.txt','.html','.htm'))) AS n_ame
        FROM hits h
        JOIN jobs j ON j.job_id=h.job_id
        GROUP BY j.job_id, j.root_path, j.has_compress, j.has_ame, j.has_dwg_dxf, j.has_pdf
        ORDER BY n_hits DESC, j.job_id
        LIMIT ?
        """
        try:
            rows = self.con.execute(sql, (*params, int(self.limit_var.get()))).fetchall()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                messagebox.showwarning("TankFinder", "Index is updating—try again in a moment.")
            else:
                messagebox.showerror("Query error", str(e))
            self.set_status("Error"); return
        except Exception as e:
            messagebox.showerror("Query error", str(e)); self.set_status("Error"); return

        self.jobs.delete(*self.jobs.get_children())
        for r in rows:
            badges = []
            if r["has_compress"]: badges.append("COMPRESS")
            if r["has_ame"]:      badges.append("AME")
            if r["has_dwg_dxf"]:  badges.append("CAD")
            if r["has_pdf"]:      badges.append("PDF")
            self.jobs.insert("", "end", iid=r["job_id"],
                values=(r["job_id"], r["n_hits"], r["n_pdf"], r["n_cad"], r["n_compress"], r["n_ame"],
                        ", ".join(badges) or "-", r["root_path"]))
        self.files.delete(*self.files.get_children())
        self.set_status(f"{len(rows)} job(s)")

    def _file_filter_sql(self):
        s = self.file_filter_var.get()
        if s == "All":       return "1=1"
        if s == "PDFs":      return "f.ext='.pdf'"
        if s == "CAD":       return "f.ext IN('.dwg','.dxf')"
        if s == "COMPRESS":  return "(instr(f.detector_hits,'compress')>0 OR f.ext IN('.cw7','.xml','.out','.lst','.txt','.html','.htm'))"
        if s == "AME":       return "(instr(f.detector_hits,'ametank')>0 OR f.ext IN('.mdl','.xmt_txt','.txt','.html','.htm'))"
        if s == "Text":      return "f.ext IN('.txt','.xml','.html','.htm','.xmt_txt','.csv')"
        return "1=1"

    def on_job_select(self, *_): self.refresh_file_list()

    def refresh_file_list(self):
        sel = self.jobs.selection()
        if not sel: self.files.delete(*self.files.get_children()); return
        job_id = sel[0]
        q = self.q_var.get().strip()
        pred = self._file_filter_sql()
        if q:
            match_expr = build_match_expr(q, use_near=self.near_var.get())
            sql = f"""
            SELECT f.rel_path FROM files f
            JOIN fts_files ff ON ff.file_hash16 = f.file_hash16
            WHERE f.deleted=0 AND f.job_id=? AND ff.content MATCH ? AND {pred}
            ORDER BY f.rel_path LIMIT 1000"""
            params = (job_id, match_expr)
        else:
            sql = f"""
            SELECT f.rel_path FROM files f
            WHERE f.deleted=0 AND f.job_id=? AND {pred}
            ORDER BY f.rel_path LIMIT 1000"""
            params = (job_id,)
        self.files.delete(*self.files.get_children())
        for fr in self.con.execute(sql, params):
            self.files.insert("", "end", values=(fr["rel_path"],))

    def get_selected_job_root(self) -> Path | None:
        sel = self.jobs.selection()
        if not sel: return None
        row = self.con.execute("SELECT root_path FROM jobs WHERE job_id=?", (sel[0],)).fetchone()
        return Path(row["root_path"]) if row else None

    def on_open_job(self, *_):
        root = self.get_selected_job_root()
        if not root: return
        try: open_folder(root)
        except Exception as e: messagebox.showerror("Open failed", f"Couldn't open:\n{root}\n\n{e}")

    def copy_job_path(self):
        root = self.get_selected_job_root()
        if root:
            self.clipboard_clear(); self.clipboard_append(str(root))
            self.set_status("Job path copied")

    def on_open_file(self, *_):
        sel_job = self.jobs.selection(); sel_file = self.files.selection()
        if not sel_job or not sel_file: return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if not full: return
        try:
            if len(str(full)) >= 248:
                self.set_status("Very long path; enable Windows long-path policy.")
            open_file_resilient(full)
        except Exception as e:
            exists = full.exists()
            msg = (f"Couldn't open:\n{full}\n\n"
                   f"{'[Missing/moved]' if not exists else ''}"
                   f"{'' if exists else '  (or Windows long-path not enabled)'}\n"
                   f"(length: {len(str(full))} chars)\n\n{e}")
            messagebox.showerror("Open failed", msg)

    def copy_file_path(self):
        sel_job = self.jobs.selection(); sel_file = self.files.selection()
        if not sel_job or not sel_file: return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if full:
            self.clipboard_clear(); self.clipboard_append(str(full))
            self.set_status("File path copied")

    def open_sql_console(self):
        win = tk.Toplevel(self); win.title("TankFinder SQL (read-only)"); win.geometry("980x520")
        txt = tk.Text(win, wrap="none", height=8); txt.pack(side=tk.TOP, fill=tk.BOTH, expand=False, padx=8, pady=(8,4))
        txt.insert("1.0",
                   "SELECT COUNT(*) AS jobs FROM jobs;\n"
                   "SELECT COUNT(*) AS files FROM files WHERE deleted=0;\n"
                   "-- Jobs by year:\n"
                   "SELECT job_year, COUNT(*) AS jobs FROM jobs GROUP BY job_year ORDER BY job_year;")
        rf = ttk.Frame(win); rf.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True, padx=8, pady=(0,8))
        tv = ttk.Treeview(rf, columns=("col1",), show="headings"); vs = ttk.Scrollbar(rf, orient="vertical", command=tv.yview)
        tv.configure(yscrollcommand=vs.set); tv.grid(row=0, column=0, sticky="nsew"); vs.grid(row=0, column=1, sticky="ns")
        rf.rowconfigure(0, weight=1); rf.columnconfigure(0, weight=1)
        def run_sql():
            sql = txt.get("1.0", "end").strip()
            if not sql: return
            stmts = [s.strip() for s in sql.split(";") if s.strip()]
            try:
                last_cols, last_rows = None, None
                for stmt in stmts:
                    cur = self.con.execute(stmt)
                    if cur.description:
                        cols_now = [c[0] for c in cur.description]
                        rows_now = cur.fetchall()
                        last_cols, last_rows = cols_now, rows_now
                if last_cols is not None:
                    tv.delete(*tv.get_children()); tv["columns"] = last_cols
                    for c in last_cols: tv.heading(c, text=c); tv.column(c, width=max(120, int(900/len(last_cols))), anchor="w")
                    for r in last_rows: tv.insert("", "end", values=[("" if v is None else str(v)) for v in r])
            except Exception as e:
                messagebox.showerror("SQL error", str(e))
        ttk.Button(win, text="Run (F5)", command=run_sql).pack(side=tk.TOP, anchor="e", padx=8, pady=(0,6))
        win.bind("<F5>", lambda e: run_sql())

    def refresh_index(self):
        if not INDEXER.exists():
            messagebox.showerror("TankFinder", f"Indexer not found:\n{INDEXER}"); return
        full = self.full_refresh_var.get()
        cmd = [os.fspath(Path(os.sys.executable)), os.fspath(INDEXER)]
        if not full: cmd += ["--limit","2000"]
        self.set_status("Refreshing index…")
        def runner():
            try:
                p = subprocess.Popen(cmd, cwd=os.fspath(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                while True:
                    line = p.stdout.readline()
                    if not line:
                        if p.poll() is not None: break
                        time.sleep(0.1); continue
                    msg = line.strip()
                    self.set_status(msg if len(msg)<140 else msg[:140]+"…")
                code = p.wait()
                self.set_status("Index refresh complete" if code==0 else f"Index refresh exited with code {code}")
            except Exception as e:
                self.set_status(f"Index refresh failed: {e}")
        threading.Thread(target=runner, daemon=True).start()

if __name__ == "__main__":
    App().mainloop()
