#!/usr/bin/env python3
# TankFinder GUI (Tkinter) — FTS5 NEAR fix + job_id sorts by year + job count
# + long-path open + parent-folder fallback + Recent searches dropdown
import tkinter as tk
from tkinter import ttk, messagebox
import os, re, sqlite3, subprocess, threading, time, json, sys
from pathlib import Path

# -------- paths (works for EXE and source runs) --------
def app_root() -> Path:
    # folder of the EXE when frozen, else project root
    return Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) \
           else Path(__file__).resolve().parents[1]

def resolve_db_path() -> Path:
    base = app_root()
    # 1) explicit override
    env = os.getenv("TANKFINDER_DB")
    if env:
        p = Path(env)
        if p.exists():
            return p
    # 2) next to EXE/script
    c1 = base / "tankfinder.db"
    if c1.exists():
        return c1
    # 3) parent of /dist (when EXE is inside /dist)
    c2 = base.parent / "tankfinder.db"
    if c2.exists():
        return c2
    # default path (for a clear error message later)
    return c1

ROOT    = app_root()
DB_PATH = resolve_db_path()
INDEXER = ROOT / "indexer" / "indexer.py"

RECENTS_DIR = Path(os.getenv("LOCALAPPDATA", str(ROOT))) / "TankFinder"
RECENTS_DIR.mkdir(parents=True, exist_ok=True)
RECENTS_PATH = RECENTS_DIR / "recent.json"

# ---------- helpers ----------
def build_match_expr(q: str, use_near: bool, near_dist: int = 50) -> str:
    """FTS5 MATCH; distance form is NEAR("a" "b" "c", N)."""
    toks = [t for t in re.split(r"\W+", (q or "").lower()) if t]
    if not toks:
        return ""
    if use_near and len(toks) >= 2:
        inner = " ".join(f"\"{t}\"" for t in toks)
        return f"NEAR({inner}, {near_dist})"
    return " AND ".join(f"\"{t}\"" for t in toks)

def year_filters(years: str | None):
    if not years: return []
    parts = []
    for chunk in years.split(","):
        c = chunk.strip()
        if "-" in c:
            a,b = c.split("-",1)
            try:
                a=int(a); b=int(b)
                for y in range(min(a,b), max(a,b)+1): parts.append(str(y))
            except Exception:
                pass
        elif c.isdigit():
            parts.append(c)
    return [f"j.root_path LIKE '%\\{y}\\%'" for y in sorted(set(parts))]

def to_long_path(p: Path) -> str:
    s = str(p)
    if s.startswith("\\\\"):            # UNC -> \\?\UNC\server\share\...
        return "\\\\?\\UNC" + s[1:]
    return "\\\\?\\" + s                # Drive letter

def exists_long(p: Path) -> bool:
    try:
        if p.exists(): return True
    except Exception:
        pass
    try:
        return os.path.exists(to_long_path(p))
    except Exception:
        return False

def open_file_resilient(path: Path) -> None:
    """Try normal open, then long-path open, else Explorer /select (normal then long)."""
    s = str(path)
    sl = to_long_path(path)
    try:
        os.startfile(s)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    try:
        os.startfile(sl)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    try:
        subprocess.run(f'explorer /select,"{s}"', shell=True)
        return
    except Exception:
        pass
    subprocess.run(f'explorer /select,"{sl}"', shell=True)

def open_folder(path: Path) -> None:
    s = str(path)
    try:
        os.startfile(s)  # type: ignore[attr-defined]
    except Exception:
        os.startfile(to_long_path(path))  # type: ignore[attr-defined]

def job_year_from_job_id(job_id: str) -> int:
    try:
        yy = int(job_id.split("-")[-1])
        return 1900 + yy if yy >= 90 else 2000 + yy
    except Exception:
        return 0

def load_recents() -> list[str]:
    try:
        RECENTS_DIR.mkdir(parents=True, exist_ok=True)
        if RECENTS_PATH.exists():
            with open(RECENTS_PATH, "r", encoding="utf-8") as f:
                v = json.load(f)
                if isinstance(v, list): return [str(x) for x in v][:50]
    except Exception:
        pass
    return []

def save_recents(v: list[str]) -> None:
    try:
        RECENTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(RECENTS_PATH, "w", encoding="utf-8") as f:
            json.dump(v[:50], f, ensure_ascii=False, indent=0)
    except Exception:
        pass

# ---------- app ----------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TankFinder")
        self.geometry("1280x760")
        self.minsize(980, 560)

        self.recents = load_recents()

        # Top controls
        top = ttk.Frame(self, padding=8)
        top.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(top, text="Search:").grid(row=0, column=0, sticky="w")
        self.q_var = tk.StringVar()
        self.q_entry = ttk.Entry(top, textvariable=self.q_var, width=40)
        self.q_entry.grid(row=0, column=1, padx=(4,12), sticky="we")
        self.q_entry.focus_set()
        self.q_entry.bind("<Return>", lambda e: self.run_search())

        self.near_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="Use NEAR", variable=self.near_var).grid(row=0, column=2, sticky="w")

        ttk.Label(top, text="Years:").grid(row=0, column=3, sticky="e", padx=(12,2))
        self.years_var = tk.StringVar(value="2019-2025")
        years_entry = ttk.Entry(top, textvariable=self.years_var, width=14)
        years_entry.grid(row=0, column=4, sticky="w")
        years_entry.bind("<Return>", lambda e: self.run_search())

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
        limit_entry = ttk.Entry(top, textvariable=self.limit_var, width=6)
        limit_entry.grid(row=0, column=10, sticky="w")
        limit_entry.bind("<Return>", lambda e: self.run_search())

        ttk.Button(top, text="Search",    command=self.run_search).grid(row=0, column=11, padx=(12,0))
        ttk.Button(top, text="Nerd Mode", command=self.open_sql_console).grid(row=0, column=12, padx=(8,0))

        # Recent searches
        ttk.Label(top, text="Recent:").grid(row=0, column=13, sticky="e", padx=(12,2))
        self.recent_var = tk.StringVar()
        self.recent_box = ttk.Combobox(top, textvariable=self.recent_var, width=28, values=self.recents, state="readonly")
        self.recent_box.grid(row=0, column=14, sticky="w")
        self.recent_box.bind("<<ComboboxSelected>>", lambda e: (self.q_var.set(self.recent_var.get()), self.run_search()))

        # Refresh Index controls (quick/full)
        self.full_refresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Full", variable=self.full_refresh_var).grid(row=0, column=15, padx=(12,2))
        ttk.Button(top, text="Refresh Index", command=self.refresh_index).grid(row=0, column=16)

        top.columnconfigure(1, weight=1)

        # Panes
        panes = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        panes.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0,8))

        # Jobs pane
        left = ttk.Frame(panes)
        self.jobs_title = ttk.Label(left, text="Jobs (ranked by hits)")
        self.jobs_title.pack(anchor="w")

        self.job_cols = ("job_id","hits","pdfs","cad","compress","ame","badges","root_path")
        headings = {
            "job_id":"job_id", "hits":"hits",
            "pdfs":"#pdf", "cad":"#cad", "compress":"#compress", "ame":"#ame",
            "badges":"badges", "root_path":"root_path"
        }

        job_wrap = ttk.Frame(left); job_wrap.pack(fill=tk.BOTH, expand=True)
        self.jobs = ttk.Treeview(job_wrap, columns=self.job_cols, show="headings")
        job_scroll = ttk.Scrollbar(job_wrap, orient="vertical", command=self.jobs.yview)
        self.jobs.configure(yscrollcommand=job_scroll.set)
        for c in self.job_cols:
            self.jobs.heading(c, text=headings[c], command=lambda col=c: self.sort_tree(self.jobs, col))
            self.jobs.column(c, width={"job_id":90,"hits":70,"pdfs":60,"cad":60,"compress":90,"ame":60,
                                       "badges":180,"root_path":650}[c], anchor="w")
        self.jobs.grid(row=0, column=0, sticky="nsew")
        job_scroll.grid(row=0, column=1, sticky="ns")
        job_wrap.rowconfigure(0, weight=1); job_wrap.columnconfigure(0, weight=1)

        self.jobs.bind("<<TreeviewSelect>>", self.on_job_select)
        self.jobs.bind("<Double-1>", self.on_open_job)
        self.jobs.bind("<Return>",   self.on_open_job)

        btns = ttk.Frame(left); btns.pack(fill=tk.X, pady=(4,0))
        ttk.Button(btns, text="Open Job Folder", command=self.on_open_job).pack(side=tk.LEFT)
        ttk.Button(btns, text="Copy Job Path", command=self.copy_job_path).pack(side=tk.LEFT, padx=6)
        panes.add(left, weight=1)

        # Files pane
        right = ttk.Frame(panes)
        header = ttk.Frame(right); header.pack(fill=tk.X)
        ttk.Label(header, text="Files").pack(side=tk.LEFT)

        ttk.Label(header, text="   Show:").pack(side=tk.LEFT)
        self.file_filter_var = tk.StringVar(value="All")
        self.file_filter = ttk.Combobox(
            header, textvariable=self.file_filter_var, width=14,
            values=["All","PDFs","CAD","Images","Excel","Word","PowerPoint","Text","COMPRESS","AME"]
        )
        self.file_filter.state(["readonly"])
        self.file_filter.pack(side=tk.LEFT, padx=(4,0))
        self.file_filter.bind("<<ComboboxSelected>>", lambda e: self.refresh_file_list())

        files_wrap = ttk.Frame(right); files_wrap.pack(fill=tk.BOTH, expand=True)
        self.files = ttk.Treeview(files_wrap, columns=("rel_path",), show="headings")
        files_scroll = ttk.Scrollbar(files_wrap, orient="vertical", command=self.files.yview)
        self.files.configure(yscrollcommand=files_scroll.set)
        self.files.heading("rel_path", text="rel_path", command=lambda: self.sort_tree(self.files, "rel_path"))
        self.files.column("rel_path", width=780, anchor="w")
        self.files.grid(row=0, column=0, sticky="nsew")
        files_scroll.grid(row=0, column=1, sticky="ns")
        files_wrap.rowconfigure(0, weight=1); files_wrap.columnconfigure(0, weight=1)
        self.files.bind("<Double-1>", self.on_open_file)
        self.files.bind("<Return>",   self.on_open_file)

        fbtns = ttk.Frame(right); fbtns.pack(fill=tk.X, pady=(4,0))
        ttk.Button(fbtns, text="Open File", command=self.on_open_file).pack(side=tk.LEFT)
        ttk.Button(fbtns, text="Copy File Path", command=self.copy_file_path).pack(side=tk.LEFT, padx=6)
        panes.add(right, weight=1)

        # Status
        self.status = tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.status, anchor="w", padding=(8,4)).pack(side=tk.BOTTOM, fill=tk.X)

        # DB
        #!/usr/bin/env python3
# TankFinder GUI (Tkinter) — FTS5 NEAR fix + job_id sorts by year + job count
# + long-path open + parent-folder fallback + Recent searches dropdown


    ROOT = Path(__file__).resolve().parents[1]
    DB_PATH = ROOT / "tankfinder.db"
    INDEXER = ROOT / "indexer" / "indexer.py"

    RECENTS_DIR  = Path(os.getenv("LOCALAPPDATA", str(ROOT))) / "TankFinder"
    RECENTS_PATH = RECENTS_DIR / "recent.json"

    def app_root() -> Path:
        # folder of the EXE when frozen, else project root
        if getattr(sys, "frozen", False):
            return Path(sys.executable).resolve().parent
        return Path(__file__).resolve().parents[1]

    def resolve_db_path() -> Path:
        base = app_root()
        candidates = [
            base / "tankfinder.db",          # next to EXE (preferred)
            base.parent / "tankfinder.db",   # parent (e.g., when EXE left in /dist)
            Path(os.getenv("TANKFINDER_DB", "")),  # explicit override
        ]
        for c in candidates:
            if c and str(c) != "" and c.exists():
                return c
        # fall back to "next to EXE" even if missing so the error message is specific
        return candidates[0]

    ROOT = app_root()
    DB_PATH = resolve_db_path()

    # ---------- sorting ----------
    def sort_tree(self, tv: ttk.Treeview, col: str):
        # Special: sort job_id by parsed year
        if tv is self.jobs and col == "job_id":
            rows = list(tv.get_children(""))
            ascending = not tv.heading(col, "text").endswith("▲")
            rows.sort(key=lambda k: (job_year_from_job_id(tv.set(k, "job_id")), tv.set(k, "job_id")),
                      reverse=not ascending)
            for idx, k in enumerate(rows):
                tv.move(k, "", idx)
            for c in self.job_cols: tv.heading(c, text=c)
            tv.heading(col, text=f"{col} {'▲' if ascending else '▼'}")
            return

        data = [(tv.set(k, col), k) for k in tv.get_children("")]
        try:
            data = [(float(v) if v not in ("", "-") else -1e99, k) for v, k in data]
        except Exception:
            pass
        ascending = not tv.heading(col, "text").endswith("▲")
        data.sort(reverse=not ascending)
        for idx, (_, k) in enumerate(data): tv.move(k, "", idx)
        columns = self.job_cols if tv is self.jobs else ("rel_path",)
        for c in columns: tv.heading(c, text=c)
        tv.heading(col, text=f"{col} {'▲' if ascending else '▼'}")

    # ---------- queries ----------
    def push_recent(self, q: str):
        if not q: return
        # put q at front, dedupe, clamp 20
        new = [q] + [x for x in self.recents if x != q]
        self.recents = new[:20]
        save_recents(self.recents)
        self.recent_box["values"] = self.recents

    def run_search(self):
        self.status.set("Searching…"); self.update_idletasks()

        where = []; params = []
        q = self.q_var.get().strip()
        match_expr = build_match_expr(q, use_near=self.near_var.get(), near_dist=50)
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
        ylikes = year_filters(self.years_var.get())
        if ylikes: where.append("(" + " OR ".join(ylikes) + ")")
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
            if q and self.near_var.get() and len(rows) == 0:
                match_expr2 = build_match_expr(q, use_near=False)
                params2 = [match_expr2] + params[1:]
                rows = self.con.execute(sql, (*params2, int(self.limit_var.get()))).fetchall()
                self.status.set("No NEAR hits; fell back to AND")
        except Exception as e:
            messagebox.showerror("Query error", str(e)); self.status.set("Error"); return

        # populate jobs
        self.jobs.delete(*self.jobs.get_children())
        for r in rows:
            badges = []
            if r["has_compress"]: badges.append("COMPRESS")
            if r["has_ame"]:      badges.append("AME")
            if r["has_dwg_dxf"]:  badges.append("CAD")
            if r["has_pdf"]:      badges.append("PDF")
            self.jobs.insert(
                "", "end", iid=r["job_id"],
                values=(r["job_id"], r["n_hits"], r["n_pdf"], r["n_cad"], r["n_compress"], r["n_ame"],
                        ", ".join(badges) or "-", r["root_path"])
            )
        self.files.delete(*self.files.get_children())

        count = len(rows)
        self.status.set(f"{count} job(s)")
        self.jobs_title.config(text=f"Jobs (ranked by hits) — {count} found")
        self.push_recent(q)

    def _file_filter_sql(self):
        c = self.file_filter_var.get()
        if c == "All":        return "1=1"
        if c == "PDFs":       return "f.ext='.pdf'"
        if c == "CAD":        return "f.ext IN('.dwg','.dxf')"
        if c == "Images":     return "f.ext IN('.jpg','.jpeg','.png','.bmp','.tif','.tiff','.heic')"
        if c == "Excel":      return "f.ext IN('.xlsx','.xlsm','.xls','.csv')"
        if c == "Word":       return "f.ext IN('.docx','.doc')"
        if c == "PowerPoint": return "f.ext IN('.pptx','.ppt')"
        if c == "Text":       return "f.ext IN('.txt','.xml','.html','.htm','.xmt_txt','.md','.log','.csv')"
        if c == "COMPRESS":   return "(instr(f.detector_hits,'compress')>0 OR f.ext IN('.cw7','.xml','.out','.lst','.txt','.html','.htm'))"
        if c == "AME":        return "(instr(f.detector_hits,'ametank')>0 OR f.ext IN('.mdl','.xmt_txt','.txt','.html','.htm'))"
        return "1=1"

    def on_job_select(self, *_): self.refresh_file_list()

    def refresh_file_list(self):
        sel = self.jobs.selection()
        if not sel:
            self.files.delete(*self.files.get_children()); return
        job_id = sel[0]
        q = self.q_var.get().strip()
        pred = self._file_filter_sql()

        if q:
            match_expr = build_match_expr(q, use_near=self.near_var.get(), near_dist=50)
            sql = f"""
            SELECT f.rel_path
            FROM files f
            JOIN fts_files ff ON ff.file_hash16 = f.file_hash16
            WHERE f.deleted=0 AND f.job_id=? AND ff.content MATCH ? AND {pred}
            ORDER BY f.rel_path
            LIMIT 1000
            """
            params = (job_id, match_expr)
        else:
            sql = f"""
            SELECT f.rel_path
            FROM files f
            WHERE f.deleted=0 AND f.job_id=? AND {pred}
            ORDER BY f.rel_path
            LIMIT 1000
            """
            params = (job_id,)

        try:
            rows = self.con.execute(sql, params).fetchall()
            if q and self.near_var.get() and len(rows) == 0:
                match_expr2 = build_match_expr(q, use_near=False)
                rows = self.con.execute(sql, (job_id, match_expr2)).fetchall()
                self.status.set("No NEAR hits; fell back to AND")
        except Exception as e:
            messagebox.showerror("Query error", str(e)); return

        self.files.delete(*self.files.get_children())
        for fr in rows:
            self.files.insert("", "end", values=(fr["rel_path"],))

    # --- job/file actions ---
    def get_selected_job_root(self) -> Path | None:
        sel = self.jobs.selection()
        if not sel: return None
        row = self.con.execute("SELECT root_path FROM jobs WHERE job_id=?", (sel[0],)).fetchone()
        return Path(row["root_path"]) if row else None

    def on_open_job(self, *_):
        root = self.get_selected_job_root()
        if not root: return
        try:
            open_folder(root)
        except Exception as e:
            messagebox.showerror("Open failed", f"Couldn't open:\n{root}\n\n{e}")

    def copy_job_path(self):
        root = self.get_selected_job_root()
        if root:
            self.clipboard_clear(); self.clipboard_append(str(root))
            self.status.set("Job path copied")

    def on_open_file(self, *_):
        sel_job = self.jobs.selection(); sel_file = self.files.selection()
        if not sel_job or not sel_file:
            self.status.set("Select a job and a file first"); return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if not full:
            return

        if not exists_long(full):
            # Likely moved/renamed or too-long path; open parent so user still lands near it.
            try:
                open_folder(full.parent)
                self.status.set("File missing/long-path; opened parent folder")
            except Exception as e2:
                messagebox.showerror("Open failed", f"Path not found (moved/renamed or long path?):\n{full}\n\n{e2}")
            return

        try:
            open_file_resilient(full)
            self.status.set("Opened")
        except Exception as e:
            try:
                open_folder(full.parent)
                self.status.set("Opened parent folder (fallback)")
            except Exception as e2:
                messagebox.showerror("Open failed", f"{full}\n\n{e}\n\nFallback also failed:\n{e2}")

    def copy_file_path(self):
        sel_job = self.jobs.selection(); sel_file = self.files.selection()
        if not sel_job or not sel_file: return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if full:
            self.clipboard_clear(); self.clipboard_append(str(full))
            self.status.set("File path copied")

    # ---------- SQL console ----------
    def open_sql_console(self):
        # Read-only SQL console with resizable editor and a result tab per statement
        win = tk.Toplevel(self)
        win.title("TankFinder — Nerd Mode (read-only SQL)")
        win.geometry("1100x640")
        win.minsize(900, 520)

        # Split the window vertically: editor on top, results (tabs) below
        split = ttk.Panedwindow(win, orient=tk.VERTICAL)
        split.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # ---- Editor area (top)
        top = ttk.Frame(split)
        split.add(top, weight=1)

        # Text editor + vertical scrollbar
        txt_scroll = ttk.Scrollbar(top, orient="vertical")
        txt = tk.Text(top, wrap="none", height=14, undo=True)
        txt.configure(yscrollcommand=txt_scroll.set)
        txt_scroll.configure(command=txt.yview)

        txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        txt_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Seed starter SQL (safe to overwrite)
        txt.insert(
            "1.0",
            "SELECT COUNT(*) AS jobs FROM jobs;\n"
            "SELECT COUNT(*) AS files FROM files WHERE deleted=0;\n"
            "-- Jobs by year:\n"
            "SELECT job_year, COUNT(*) AS jobs FROM jobs GROUP BY job_year ORDER BY job_year;\n"
        )

        # ---- Results area (bottom): a Notebook with one tab per statement
        res = ttk.Notebook(split)
        split.add(res, weight=2)

        def run_sql():
            # Clear any prior result tabs
            for tab_id in list(res.tabs()):
                res.forget(tab_id)

            raw = txt.get("1.0", "end")
            statements = [s.strip() for s in raw.split(";") if s.strip()]
            if not statements:
                return

            for i, stmt in enumerate(statements, 1):
                try:
                    cur = self.con.execute(stmt)

                    # If it's not a SELECT (no columns), show a small "OK" note
                    if not cur.description:
                        frm = ttk.Frame(res)
                        res.add(frm, text=f"#{i}")
                        note = tk.Text(frm, height=3)
                        note.pack(fill=tk.BOTH, expand=True)
                        note.insert("1.0", "OK")
                        note.configure(state="disabled")
                        continue

                    cols = [c[0] for c in cur.description]
                    rows = cur.fetchall()

                    frm = ttk.Frame(res)
                    res.add(frm, text=f"#{i}")

                    tv = ttk.Treeview(frm, columns=cols, show="headings")
                    vs = ttk.Scrollbar(frm, orient="vertical", command=tv.yview)
                    tv.configure(yscrollcommand=vs.set)

                    tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                    vs.pack(side=tk.RIGHT, fill=tk.Y)

                    for c in cols:
                        tv.heading(c, text=c)
                        tv.column(c, width=max(120, int(900 / max(1, len(cols)))), anchor="w")

                    for r in rows:
                        tv.insert("", "end", values=[("" if v is None else str(v)) for v in r])

                except Exception as e:
                    frm = ttk.Frame(res)
                    res.add(frm, text=f"#{i} (error)")
                    t = tk.Text(frm, height=6, foreground="red")
                    t.pack(fill=tk.BOTH, expand=True)
                    t.insert("1.0", str(e))
                    t.configure(state="disabled")

        # Run button + F5 binding
        btnbar = ttk.Frame(win)
        btnbar.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Button(btnbar, text="Run (F5)", command=run_sql).pack(side=tk.RIGHT)

        win.bind("<F5>", lambda _e: run_sql())

   
    # ---------- Refresh index ----------
    def refresh_index(self):
        if not INDEXER.exists():
            messagebox.showerror("TankFinder", f"Indexer not found:\n{INDEXER}")
            return
        full = self.full_refresh_var.get()
        cmd = [os.fspath(Path(os.sys.executable)), os.fspath(INDEXER)]
        if not full:
            cmd += ["--limit", "2000"]
        self.status.set("Refreshing index…"); self.update_idletasks()

        def runner():
            try:
                proc = subprocess.Popen(cmd, cwd=os.fspath(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                last_line = ""
                while True:
                    line = proc.stdout.readline()
                    if not line:
                        if proc.poll() is not None: break
                        time.sleep(0.1); continue
                    last_line = line.strip()
                    self.status.set(last_line if len(last_line) < 140 else last_line[:140] + "…")
                code = proc.wait()
                if code == 0:
                    self.status.set("Index refresh complete" + (" (full)" if full else ""))
                else:
                    self.status.set(f"Index refresh exited with code {code}")
            except Exception as e:
                self.status.set(f"Index refresh failed: {e}")

        threading.Thread(target=runner, daemon=True).start()

if __name__ == "__main__":
    app = App()
    app.mainloop()
