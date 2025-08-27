PRAGMA foreign_keys = ON;

-- ===== JOBS =====
CREATE TABLE IF NOT EXISTS jobs (
  job_id             TEXT PRIMARY KEY,
  root_path          TEXT NOT NULL,
  job_year           INTEGER,
  customer           TEXT,
  first_seen         TEXT NOT NULL,
  last_seen          TEXT NOT NULL,
  last_modified_utc  TEXT,
  file_count_total   INTEGER NOT NULL DEFAULT 0,
  byte_size_total    INTEGER NOT NULL DEFAULT 0,
  folder_depth_max   INTEGER,
  has_compress       INTEGER NOT NULL DEFAULT 0,
  has_ame            INTEGER NOT NULL DEFAULT 0,
  has_dwg_dxf        INTEGER NOT NULL DEFAULT 0,
  has_pdf            INTEGER NOT NULL DEFAULT 0,
  has_photos         INTEGER NOT NULL DEFAULT 0,
  has_legacy_calc    INTEGER NOT NULL DEFAULT 0,
  keywords_sample    TEXT,
  score_completeness INTEGER NOT NULL DEFAULT 0,
  errors_count       INTEGER NOT NULL DEFAULT 0,
  notes              TEXT
);

-- ===== FILES =====
CREATE TABLE IF NOT EXISTS files (
  file_hash16   TEXT PRIMARY KEY,
  job_id        TEXT NOT NULL,
  rel_path      TEXT NOT NULL,
  ext           TEXT NOT NULL,
  size_bytes    INTEGER NOT NULL,
  mtime_utc     TEXT NOT NULL,
  kind          TEXT,
  tokens_fname  TEXT,
  detector_hits TEXT,
  deleted       INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0,1)),
  FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

-- ===== Indexes =====
CREATE INDEX IF NOT EXISTS idx_files_job_del       ON files(job_id, deleted);
CREATE INDEX IF NOT EXISTS idx_files_job_ext_del   ON files(job_id, ext, deleted);
CREATE INDEX IF NOT EXISTS idx_files_hash16        ON files(file_hash16);
CREATE INDEX IF NOT EXISTS idx_jobs_year           ON jobs(job_year);
CREATE INDEX IF NOT EXISTS idx_jobs_flags          ON jobs(has_compress, has_ame, has_dwg_dxf, has_pdf);

-- ===== FTS (better tokenizer for engineering tokens) =====
CREATE VIRTUAL TABLE IF NOT EXISTS fts_files USING fts5(
  content,
  file_hash16 UNINDEXED,
  tokenize = "unicode61 separators '-_./()[]{}' remove_diacritics 2"
);
