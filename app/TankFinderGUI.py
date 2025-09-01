#!/usr/bin/env python3
# TankFinder GUI (Tkinter)
# - Caps headers
# - FILES column renamed to "KEYWORD FILES"
# - Center most columns; JOB/QUOTE ID stays left
# - Status line centered, bigger, and used for progress/errors
# - "UPDATE DATABASE" button with confirm + progress pulse
# - "RESET" button to clear search + results
# - Better long-path file opening and UNC handling
# - NEAR fallback stays (shows a clear status)
import tkinter as tk
from tkinter import ttk, messagebox
import os, re, sqlite3, subprocess, threading, time, json, sys
from pathlib import Path

# ---------- location/DB resolution ----------
def app_root() -> Path:
    if getattr(sys, "frozen", False):  # running as EXE
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]

def resolve_db_path() -> Path:
    base = app_root()
    candidates = [
        base / "tankfinder.db",        # next to EXE (preferred)
        base.parent / "tankfinder.db", # parent of EXE (when left in /dist)
    ]

    env_db = os.getenv("TANKFINDER_DB")
    if env_db:
        p = Path(env_db)
        if p.is_file():
            return p

    for c in candidates:
        if c.is_file():
            return c

    # fall back to the preferred location for a clear error later
    return candidates[0]

ROOT    = app_root()
DB_PATH = resolve_db_path()
INDEXER = ROOT / "indexer" / "indexer.py"

# ---------- helpers ----------
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def build_match_expr(q: str, use_near: bool) -> str:
    toks = [t for t in re.split(r"\W+", (q or "").lower()) if t]
    if not toks: return ""
    if use_near and len(toks) >= 2:
        expr = f"\"{toks[0]}\""
        for t in toks[1:]:
            expr += f" NEAR \"{t}\""
        return expr
    return " AND ".join(f"\"{t}\"" for t in toks)

def year_filters(years: str | None):
    if not years: return []
    parts = []
    for chunk in years.split(","):
        c = chunk.strip()
        if "-" in c:
            a, b = c.split("-", 1)
            try:
                a = int(a); b = int(b)
                for y in range(min(a,b), max(a,b)+1):
                    parts.append(str(y))
            except Exception:
                pass
        elif c.isdigit():
            parts.append(c)
    return [f"j.root_path LIKE '%\\{y}\\%'" for y in sorted(set(parts))]

def _to_extended_path(p: Path) -> str:
    """Return a Windows extended-length path for long paths."""
    s = str(p)
    if s.startswith("\\\\"):
        # UNC -> \\?\UNC\server\share\...
        return "\\\\?\\UNC\\" + s.lstrip("\\")
    return "\\\\?\\" + s

def open_file_resilient(path: Path) -> None:
    """Try multiple ways to open a file, including long paths and a final Explorer select."""
    p = str(path)
    # 1) normal
    try:
        os.startfile(p)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    # 2) extended long-path
    try:
        os.startfile(_to_extended_path(path))  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    # 3) PowerShell Start-Process (sometimes helps with associations)
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", f"Start-Process -FilePath '{p}'"],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return
    except Exception:
        pass
    # 4) Explorer select as last resort
    try:
        subprocess.run(f'explorer /select,"{p}"', shell=True)
    except Exception as e:
        raise RuntimeError(str(e))

def open_folder(path: Path) -> None:
    try:
        os.startfile(str(path))  # type: ignore[attr-defined]
    except Exception as e:
        raise RuntimeError(str(e))

def fmt_status(s: str) -> str:
    return s.replace("\n", " ")[:200]

# ---------- main app ----------
# ---------- app ----------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("TankFinder")
        self.geometry("1280x760")
        self.minsize(1020, 600)
            # styles
        s = ttk.Style(self)
        s.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

            # ---- top bar (query) ----
        top = ttk.Frame(self, padding=8)
        top.pack(side=tk.TOP, fill=tk.X)

        # SHOW (far left)
        ttk.Label(top, text="SHOW:").grid(row=0, column=0, sticky="e", padx=(0,4))
        self.show_var = tk.StringVar(value="ALL")
        self.show_combo = ttk.Combobox(top, textvariable=self.show_var, width=10,
                                    values=["ALL","JOBS","QUOTES"], state="readonly")
        self.show_combo.grid(row=0, column=1, sticky="w", padx=(0,12))
        self.show_combo.bind("<<ComboboxSelected>>", lambda e: self.run_search())

        # SEARCH label + entry (after SHOW)
        ttk.Label(top, text="SEARCH:").grid(row=0, column=2, sticky="w")
        self.q_var = tk.StringVar()
        self.q_entry = ttk.Entry(top, textvariable=self.q_var, width=48)
        self.q_entry.grid(row=0, column=3, padx=(6,12), sticky="we")
        self.q_entry.bind("<Return>", lambda e: self.run_search())

        # TIGHTEN SEARCH
        self.near_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="TIGHTEN SEARCH", variable=self.near_var).grid(row=0, column=4, sticky="w")

        # YEARS
        ttk.Label(top, text="YEARS:").grid(row=0, column=5, sticky="e", padx=(12,4))
        self.years_var = tk.StringVar(value="2019-2025")
        ttk.Entry(top, textvariable=self.years_var, width=16).grid(row=0, column=6, sticky="w")

        # quick badges
        self.compress_var = tk.BooleanVar(value=False)
        self.ame_var      = tk.BooleanVar(value=False)
        self.cad_var      = tk.BooleanVar(value=False)
        self.pdf_var      = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="COMPRESS", variable=self.compress_var).grid(row=0, column=7, padx=(12,0))
        ttk.Checkbutton(top, text="AME",      variable=self.ame_var).grid(row=0, column=8)
        ttk.Checkbutton(top, text="CAD",      variable=self.cad_var).grid(row=0, column=9)
        ttk.Checkbutton(top, text="PDF",      variable=self.pdf_var).grid(row=0, column=10)

        # LIMIT
        ttk.Label(top, text="LIMIT:").grid(row=0, column=11, sticky="e", padx=(12,4))
        self.limit_var = tk.IntVar(value=50)
        ttk.Entry(top, textvariable=self.limit_var, width=7).grid(row=0, column=12, sticky="w")

        # SEARCH button (at the end of the row)
        ttk.Button(top, text="SEARCH", command=self.run_search).grid(row=0, column=13, padx=(12,0))

        # make the text entry stretch
        top.columnconfigure(3, weight=1)


            # ---- panes ----
        panes = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        panes.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # ---- left: jobs ----
        left = ttk.Frame(panes)
        ttk.Label(left, text="JOBS (RANKED BY HITS)").pack(anchor="w")

        self.job_cols = ("job_id","hits","pdfs","cad","compress","ame","badges","root_path")
        headings = {
            "job_id":"JOB/QUOTE ID", "hits":"HITS",
            "pdfs":"#PDF", "cad":"#CAD", "compress":"#COMPRESS", "ame":"#AME",
            "badges":"BADGES", "root_path":"FOLDER LOCATION"
        }

        job_wrap = ttk.Frame(left)
        job_wrap.pack(fill=tk.BOTH, expand=True)

        self.jobs = ttk.Treeview(job_wrap, columns=self.job_cols, show="headings")  # no fixed height
        job_vscroll = ttk.Scrollbar(job_wrap, orient="vertical", command=self.jobs.yview)
        job_hscroll = ttk.Scrollbar(job_wrap, orient="horizontal", command=self.jobs.xview)
        self.jobs.configure(yscrollcommand=job_vscroll.set, xscrollcommand=job_hscroll.set)

        width_map = {"job_id":120,"hits":80,"pdfs":68,"cad":68,"compress":100,"ame":68,"badges":220,"root_path":720}
        for c in self.job_cols:
            self.jobs.heading(c, text=headings[c], command=lambda col=c: self.sort_tree(self.jobs, col))
            # left-align job_id, badges, root_path; center others
            anchor = "w" if c in ("job_id","badges","root_path") else "center"
            self.jobs.column(c, width=width_map[c], anchor=anchor, stretch=True)

        # also left-align those headers themselves
        for c in ("job_id","badges","root_path"):
            self.jobs.heading(c, anchor="w")

        self.jobs.grid(row=0, column=0, sticky="nsew")
        job_vscroll.grid(row=0, column=1, sticky="ns")
        job_hscroll.grid(row=1, column=0, sticky="ew")
        job_wrap.rowconfigure(0, weight=1)
        job_wrap.columnconfigure(0, weight=1)

        self.jobs.bind("<<TreeviewSelect>>", self.on_job_select)
        self.jobs.bind("<Double-1>", self.on_open_job)
        self.jobs.bind("<Return>",   self.on_open_job)

        btns = ttk.Frame(left)
        btns.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(btns, text="OPEN JOB FOLDER", command=self.on_open_job).pack(side=tk.LEFT)
        ttk.Button(btns, text="COPY JOB PATH",   command=self.copy_job_path).pack(side=tk.LEFT, padx=6)

        panes.add(left, weight=1)

        # ---- right: files ----
        right = ttk.Frame(panes)
        header = ttk.Frame(right); header.pack(fill=tk.X)
        ttk.Label(header, text="FILES").pack(side=tk.LEFT)

        ttk.Label(header, text="   SHOW:").pack(side=tk.LEFT)
        self.file_filter_var = tk.StringVar(value="All")
        self.file_filter = ttk.Combobox(
            header, textvariable=self.file_filter_var, width=12,
            values=["All","PDFs","CAD","COMPRESS","AME","Text"], state="readonly"
        )
        self.file_filter.pack(side=tk.LEFT, padx=(4, 0))
        self.file_filter.bind("<<ComboboxSelected>>", lambda e: self.refresh_file_list())

        files_wrap = ttk.Frame(right)
        files_wrap.pack(fill=tk.BOTH, expand=True)

        self.files = ttk.Treeview(files_wrap, columns=("rel_path",), show="headings")  # no fixed height
        files_vscroll = ttk.Scrollbar(files_wrap, orient="vertical", command=self.files.yview)
        files_hscroll = ttk.Scrollbar(files_wrap, orient="horizontal", command=self.files.xview)
        self.files.configure(yscrollcommand=files_vscroll.set, xscrollcommand=files_hscroll.set)

        self.files.heading("rel_path", text="JOB FILES", anchor="w",
                        command=lambda: self.sort_tree(self.files, "rel_path"))
        self.files.column("rel_path", width=820, anchor="w", stretch=True)

        self.files.grid(row=0, column=0, sticky="nsew")
        files_vscroll.grid(row=0, column=1, sticky="ns")
        files_hscroll.grid(row=1, column=0, sticky="ew")
        files_wrap.rowconfigure(0, weight=1)
        files_wrap.columnconfigure(0, weight=1)

        fbtns = ttk.Frame(right)
        fbtns.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(fbtns, text="OPEN FILE",      command=self.on_open_file).pack(side=tk.LEFT)
        ttk.Button(fbtns, text="COPY FILE PATH", command=self.copy_file_path).pack(side=tk.LEFT, padx=6)

        panes.add(right, weight=1)


            # ---- bottom bar (status + actions) ----
        bottom = ttk.Frame(self, padding=(8, 6))
        bottom.pack(side=tk.BOTTOM, fill=tk.X)

        # 3 zones: LEFT (nerd), CENTER (status), RIGHT (actions)
        bottom.grid_columnconfigure(0, weight=0)   # left controls
        bottom.grid_columnconfigure(1, weight=1)   # status expands
        bottom.grid_columnconfigure(2, weight=0)   # right controls

        # LEFT: NERD MODE (anchor hard-left, under OPEN JOB FOLDER visually)
        ttk.Button(bottom, text="NERD MODE", command=self.open_sql_console)\
        .grid(row=0, column=0, sticky="w")

        # CENTER: centered status (bold)
        self.status_var = tk.StringVar(value="READY")
        self.status = self.status_var
        self.status_label = ttk.Label(bottom, textvariable=self.status_var, anchor="center")
        self.status_label.configure(font=("Segoe UI", 14, "bold"))
        self.status_label.grid(row=0, column=1, sticky="ew", padx=8)

        # RIGHT: CLEAR RESULTS + UPDATE DATABASE
        right = ttk.Frame(bottom)
        right.grid(row=0, column=2, sticky="e")
        ttk.Button(right, text="CLEAR RESULTS", command=self.clear_search).pack(side=tk.LEFT, padx=(0,8))
        ttk.Button(right, text="UPDATE DATABASE", command=self.refresh_index).pack(side=tk.LEFT)

        # keybinds
        self.bind("<Escape>", lambda _e: self.clear_search())



        # ---- DB open (robust) ----
        dbp = DB_PATH

        # Show progress in the centered status ASAP
        try:
            self.status.set(f"OPENING DB…")
        except Exception:
            pass

        # Fast sanity checks (avoid picking a directory or a bad env value)
        if not dbp or str(dbp).strip() == "":
            messagebox.showerror("TankFinder", "Database path is empty."); self.destroy(); return
        if dbp.is_dir():
            messagebox.showerror("TankFinder", f"Database path is a directory, not a file:\n{dbp}")
            self.destroy(); return

        # If this is a mapped drive/UNC that’s slow or disconnected, exists() can stall.
        # Do a lightweight stat in a short thread and bail if it’s taking too long.
        exists_flag = {"ok": False}
        def _probe():
            try:
                exists_flag["ok"] = dbp.exists() and dbp.is_file()
            except Exception:
                exists_flag["ok"] = False
        t = threading.Thread(target=_probe, daemon=True)
        t.start(); t.join(2.0)  # wait up to 2 seconds
        if not exists_flag["ok"]:
            messagebox.showerror("TankFinder", f"Database not found or not reachable:\n{dbp}")
            self.destroy(); return

        def _uri(p: Path) -> str:
            # Use immutable=1 to hint SQLite this file won’t change (faster on network)
            return "file:" + p.resolve().as_posix() + "?mode=ro&immutable=1"

        try:
            # Keep timeout small so we don't hang forever if the share hiccups
            self.con = sqlite3.connect(_uri(dbp), uri=True, timeout=2.0)
        except Exception:
            try:
                self.con = sqlite3.connect(str(dbp), timeout=2.0)
            except Exception as e:
                messagebox.showerror("TankFinder", f"Couldn't open database:\n{dbp}\n\n{e}")
                self.destroy(); return

        try:
            self.con.execute("PRAGMA query_only=ON;")
        except Exception:
            pass
        self.con.row_factory = sqlite3.Row
        self.status.set("READY")
        print("[TankFinder] DB opened OK.")


    # ---------------- helpers / actions ----------------
    def set_status(self, msg: str, *, transient_ms: int | None = None):
        if hasattr(self, "status_var"):
            self.status_var.set(msg)
            if transient_ms:
                self.after(transient_ms, self.clear_status)

    def clear_status(self):
        if hasattr(self, "status_var"):
            self.status_var.set("")

    def _clear_tree(self, tree: ttk.Treeview):
        for iid in tree.get_children():
            tree.delete(iid)

    def clear_search(self):
        # text inputs
        if hasattr(self, "q_var"):     self.q_var.set("")
        if hasattr(self, "years_var"): self.years_var.set("")

        # toggles
        if hasattr(self, "near_var"):     self.near_var.set(True)
        if hasattr(self, "compress_var"): self.compress_var.set(False)
        if hasattr(self, "ame_var"):      self.ame_var.set(False)
        if hasattr(self, "cad_var"):      self.cad_var.set(False)
        if hasattr(self, "pdf_var"):      self.pdf_var.set(False)

        # dropdowns
        if hasattr(self, "show_var"):        self.show_var.set("All")
        if hasattr(self, "file_filter_var"): self.file_filter_var.set("All")

        # tables
        if hasattr(self, "jobs"):  self._clear_tree(self.jobs)
        if hasattr(self, "files"): self._clear_tree(self.files)

        # focus & status
        if hasattr(self, "q_entry"): self.q_entry.focus_set()
        self.set_status("Cleared. Ready.", transient_ms=1600)

        # ---- DB open (robust) ----
        dbp = DB_PATH

        if not dbp.exists():
            messagebox.showerror("TankFinder", f"Database not found:\n{dbp}")
            self.destroy(); return

        def _uri(p: Path) -> str:
            # SQLite likes forward slashes in file: URIs
            return "file:" + p.resolve().as_posix() + "?mode=ro"

        try:
            # Prefer strict read-only via URI
            self.con = sqlite3.connect(_uri(dbp), uri=True)
        except Exception:
            # Fallback to plain path (helps on some Windows setups)
            try:
                self.con = sqlite3.connect(str(dbp))
            except Exception as e:
                messagebox.showerror("TankFinder", f"Couldn't open database:\n{dbp}\n\n{e}")
                self.destroy(); return

        # Belt-and-suspenders: keep connection query-only
        try:
            self.con.execute("PRAGMA query_only=ON;")
        except Exception:
            pass

        self.con.row_factory = sqlite3.Row
        # Optional: show where we're connected
        # self.status.set(f"DB: {dbp}")


    # ---------- sorting ----------
    def sort_tree(self, tv: ttk.Treeview, col: str):
        data = [(tv.set(k, col), k) for k in tv.get_children("")]

        def key_jobid(v):
            # sort by year (YY) then numeric job within year when possible
            s = str(v)
            # JOB pattern 123-45
            m = re.search(r"\b(\d{3})-(\d{2})\b", s)
            if m:
                num = int(m.group(1)); yy = int(m.group(2))
                year = 1900 + yy if yy >= 90 else 2000 + yy
                return (year, num, s)
            # QUOTE pattern Q####-YY
            mq = re.search(r"\bQ(\d+)-(\d{2})\b", s, re.I)
            if mq:
                qn = int(mq.group(1)); yy = int(mq.group(2))
                year = 1900 + yy if yy >= 90 else 2000 + yy
                return (year, qn, s)
            return (9999, 999999, s)

        try:
            if col == "job_id":
                data.sort(key=lambda x: key_jobid(x[0]))
            else:
                data = [(float(v) if v not in ("", "-") else -1e99, k) for v, k in data]
                data.sort()
        except Exception:
            data.sort()

        # toggle direction
        ascending = not self.jobs.heading(col, "text").endswith("▲") if tv is self.jobs else True
        if not ascending: data.reverse()
        for idx, (_, k) in enumerate(data):
            tv.move(k, "", idx)

        # reset headings
        columns = self.job_cols if tv is self.jobs else ("rel_path",)
        for c in columns:
            txt = self.jobs.heading(c, "text") if tv is self.jobs else self.files.heading(c, "text")
            (self.jobs if tv is self.jobs else self.files).heading(c, text=txt.replace(" ▲","").replace(" ▼",""))
        (self.jobs if tv is self.jobs else self.files).heading(col,
            text=(self.jobs.heading(col, "text") if tv is self.jobs else self.files.heading(col, "text")) + (" ▲" if ascending else " ▼"))

    # ---------- queries ----------
    def run_search(self):
        self.status.set("SEARCHING…"); self.update_idletasks()

        where, params = [], []
        q = (self.q_var.get() or "").strip()
        match_expr = build_match_expr(q, use_near=self.near_var.get())
        used_near = bool(self.near_var.get() and " NEAR " in match_expr)

        # FTS join/predicate
        if q:
            fts_join = "JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
            fts_pred = "ff.content MATCH ?"
            params.append(match_expr)
        else:
            fts_join = "LEFT JOIN fts_files ff ON ff.file_hash16 = f.file_hash16"
            fts_pred = "1=1"

        # quick filters
        if self.compress_var.get(): where.append("j.has_compress = 1")
        if self.ame_var.get():      where.append("j.has_ame = 1")
        if self.cad_var.get():      where.append("j.has_dwg_dxf = 1")
        if self.pdf_var.get():      where.append("j.has_pdf = 1")

        ylikes = year_filters(self.years_var.get())
        if ylikes: where.append("(" + " OR ".join(ylikes) + ")")

        # SHOW filter (ALL/JOBS/QUOTES)
        show = (self.show_var.get() if hasattr(self, "show_var") else "ALL").upper()
        if show == "JOBS":
            where.append("j.job_id NOT LIKE 'Q%'")
        elif show == "QUOTES":
            where.append("j.job_id LIKE 'Q%'")

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

        def _fill_jobs(rows):
            self.jobs.delete(*self.jobs.get_children())
            for r in rows:
                badges = []
                if r["has_compress"]: badges.append("COMPRESS")
                if r["has_ame"]:      badges.append("AME")
                if r["has_dwg_dxf"]:  badges.append("CAD")
                if r["has_pdf"]:      badges.append("PDF")
                # Quote badge if a quote job has at least one PDF
                if str(r["job_id"]).upper().startswith("Q") and r["n_pdf"] > 0:
                    badges.append("QUOTE.PDF")
                self.jobs.insert(
                    "", "end", iid=r["job_id"],
                    values=(r["job_id"], r["n_hits"], r["n_pdf"], r["n_cad"], r["n_compress"], r["n_ame"],
                            ", ".join(badges) or "-", r["root_path"])
                )

        try:
            rows = self.con.execute(sql, (*params, int(self.limit_var.get()))).fetchall()
            _fill_jobs(rows)
            self.files.delete(*self.files.get_children())
        except Exception as e:
            messagebox.showerror("Query error", str(e))
            self.status.set("ERROR")
            return

        # NEAR fallback → AND
        if used_near and not rows:
            try:
                match_and = build_match_expr(q, use_near=False)
                rows2 = self.con.execute(sql, (match_and, int(self.limit_var.get()))).fetchall()
            except Exception:
                rows2 = []
            _fill_jobs(rows2)
            self.status.set(f"No NEAR hits; fell back to AND — {len(rows2)} job(s)")
        else:
            self.status.set(f"{len(rows)} job(s)")


    def _file_filter_sql(self):
        choice = self.file_filter_var.get()
        if choice == "All":       return "1=1"
        if choice == "PDFs":      return "f.ext='.pdf'"
        if choice == "CAD":       return "f.ext IN('.dwg','.dxf')"
        if choice == "COMPRESS":  return "(instr(f.detector_hits,'compress')>0 OR f.ext IN('.cw7','.xml','.out','.lst','.txt','.html','.htm'))"
        if choice == "AME":       return "(instr(f.detector_hits,'ametank')>0 OR f.ext IN('.mdl','.xmt_txt','.txt','.html','.htm'))"
        if choice == "Text":      return "f.ext IN('.txt','.xml','.html','.htm','.xmt_txt','.csv')"
        return "1=1"

    def on_job_select(self, *_):
        self.refresh_file_list()

    def refresh_file_list(self):
        sel = self.jobs.selection()
        if not sel:
            self.files.delete(*self.files.get_children()); return
        job_id = sel[0]
        q = self.q_var.get().strip()
        pred = self._file_filter_sql()
        if q:
            match_expr = build_match_expr(q, use_near=self.near_var.get())
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
        self.files.delete(*self.files.get_children())
        for fr in self.con.execute(sql, params):
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
        if not sel_job or not sel_file: return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if not full:
            return
        try:
            if not full.exists():
                # try long-path check before giving up
                if not Path(_to_extended_path(full)).exists():
                    raise FileNotFoundError("Missing or moved")
            open_file_resilient(full)
            self.status.set("Opening file…")
        except Exception:
            # final fallback: open the parent so user can see/select
            try:
                open_folder(full.parent)
                self.status.set("File missing/long-path; opened parent folder")
            except Exception as e2:
                messagebox.showerror("Open failed", f"Couldn't open:\n{full}\n\n{e2}")

    def copy_file_path(self):
        sel_job = self.jobs.selection(); sel_file = self.files.selection()
        if not sel_job or not sel_file: return
        root = self.get_selected_job_root()
        rel = self.files.item(sel_file[0], "values")[0]
        full = (root / rel) if root else None
        if full:
            self.clipboard_clear(); self.clipboard_append(str(full))
            self.status.set("File path copied")

    def reset_all(self):
        self.q_var.set("")
        self.years_var.set("2019-2025")
        self.near_var.set(True)
        self.compress_var.set(False); self.ame_var.set(False)
        self.cad_var.set(False); self.pdf_var.set(False)
        self.limit_var.set(50)
        self.jobs.delete(*self.jobs.get_children())
        self.files.delete(*self.files.get_children())
        self.status.set("RESET")

    # ---------- SQL console ----------
    def open_sql_console(self):
        win = tk.Toplevel(self); win.title("TankFinder — Nerd Mode (read-only SQL)"); win.geometry("1100x640")
        win.minsize(900, 520)

        split = ttk.Panedwindow(win, orient=tk.VERTICAL); split.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        top = ttk.Frame(split); split.add(top, weight=1)
        txt_scroll = ttk.Scrollbar(top, orient="vertical")
        txt = tk.Text(top, wrap="none", height=14, undo=True)
        txt.configure(yscrollcommand=txt_scroll.set); txt_scroll.configure(command=txt.yview)
        txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True); txt_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        txt.insert("1.0",
            "SELECT COUNT(*) AS jobs FROM jobs;\n"
            "SELECT COUNT(*) AS files FROM files WHERE deleted=0;\n"
            "-- Jobs by year:\n"
            "SELECT job_year, COUNT(*) AS jobs FROM jobs GROUP BY job_year ORDER BY job_year;\n"
        )

        res = ttk.Notebook(split); split.add(res, weight=2)

        def run_sql():
            for tab_id in list(res.tabs()): res.forget(tab_id)
            raw = txt.get("1.0", "end"); statements = [s.strip() for s in raw.split(";") if s.strip()]
            if not statements: return
            for i, stmt in enumerate(statements, 1):
                try:
                    cur = self.con.execute(stmt)
                    if not cur.description:
                        frm = ttk.Frame(res); res.add(frm, text=f"#{i}")
                        note = tk.Text(frm, height=3); note.pack(fill=tk.BOTH, expand=True)
                        note.insert("1.0", "OK"); note.configure(state="disabled"); continue
                    cols = [c[0] for c in cur.description]; rows = cur.fetchall()
                    frm = ttk.Frame(res); res.add(frm, text=f"#{i}")
                    tv = ttk.Treeview(frm, columns=cols, show="headings"); vs = ttk.Scrollbar(frm, orient="vertical", command=tv.yview)
                    tv.configure(yscrollcommand=vs.set); tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True); vs.pack(side=tk.RIGHT, fill=tk.Y)
                    for c in cols:
                        tv.heading(c, text=c); tv.column(c, width=max(120, int(900 / max(1, len(cols)))), anchor="w")
                    for r in rows:
                        tv.insert("", "end", values=[("" if v is None else str(v)) for v in r])
                except Exception as e:
                    frm = ttk.Frame(res); res.add(frm, text=f"#{i} (error)")
                    t = tk.Text(frm, height=6, foreground="red"); t.pack(fill=tk.BOTH, expand=True)
                    t.insert("1.0", str(e)); t.configure(state="disabled")

        btnbar = ttk.Frame(win); btnbar.pack(fill=tk.X, padx=8, pady=(0,8))
        ttk.Button(btnbar, text="Run (F5)", command=run_sql).pack(side=tk.RIGHT)
        win.bind("<F5>", lambda _e: run_sql())

    # ---------- Update database (indexer) ----------
    def refresh_index(self):
        if not INDEXER.exists():
            messagebox.showerror("TankFinder", f"Indexer not found:\n{INDEXER}"); return

        if not messagebox.askyesno("UPDATE DATABASE", "You're updating the database outside of its schedule update. This may take a few minutes. Continue?"):
            return
       
        # progress popup
        prog = tk.Toplevel(self); prog.title("Updating…")
        ttk.Label(prog, text="Running indexer…").pack(padx=12, pady=(12,6))
        pb = ttk.Progressbar(prog, mode="indeterminate", length=340); pb.pack(padx=12, pady=(0,12)); pb.start(20)
        prog.geometry("+%d+%d" % (self.winfo_rootx()+120, self.winfo_rooty()+120))
        prog.transient(self); prog.grab_set()

        def runner():
            try:
                cmd = [os.fspath(Path(os.sys.executable)), os.fspath(INDEXER)]
                # quick pass: keep as-is (you can extend with flags later)
                proc = subprocess.Popen(cmd, cwd=os.fspath(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                last_line = ""
                while True:
                    line = proc.stdout.readline()
                    if not line:
                        if proc.poll() is not None: break
                        time.sleep(0.1); continue
                    last_line = line.strip()
                    self.status.set(fmt_status(last_line))
                code = proc.wait()
                if code == 0:
                    self.status.set("Index refresh complete")
                else:
                    self.status.set(f"Index refresh exited with code {code}")
            except Exception as e:
                self.status.set(f"Index refresh failed: {e}")
            finally:
                try:
                    pb.stop(); prog.grab_release(); prog.destroy()
                except Exception:
                    pass

        threading.Thread(target=runner, daemon=True).start()

# ---------- main ----------
if __name__ == "__main__":
    App().mainloop()
