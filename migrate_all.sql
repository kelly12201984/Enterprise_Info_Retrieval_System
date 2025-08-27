PRAGMA foreign_keys = OFF;
BEGIN;

-- Add job_year if missing
ALTER TABLE jobs ADD COLUMN job_year INTEGER;

-- Populate job_year from existing 'year' column if present
UPDATE jobs SET job_year = year WHERE job_year IS NULL AND year IS NOT NULL;

-- Fallback: derive from job_id ###-YY (>=90 -> 19YY else 20YY)
UPDATE jobs
SET job_year = CASE
  WHEN CAST(substr(job_id, instr(job_id,'-')+1, 2) AS INT) >= 90
       THEN 1900 + CAST(substr(job_id, instr(job_id,'-')+1, 2) AS INT)
  ELSE 2000 + CAST(substr(job_id, instr(job_id,'-')+1, 2) AS INT)
END
WHERE job_year IS NULL AND job_id GLOB '*-??';

-- Normalize NULL counters/flags
UPDATE jobs SET
  file_count_total   = COALESCE(file_count_total,0),
  byte_size_total    = COALESCE(byte_size_total,0),
  has_compress       = COALESCE(has_compress,0),
  has_ame            = COALESCE(has_ame,0),
  has_dwg_dxf        = COALESCE(has_dwg_dxf,0),
  has_pdf            = COALESCE(has_pdf,0),
  has_photos         = COALESCE(has_photos,0),
  has_legacy_calc    = COALESCE(has_legacy_calc,0),
  score_completeness = COALESCE(score_completeness,0),
  errors_count       = COALESCE(errors_count,0);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_files_job_del       ON files(job_id, deleted);
CREATE INDEX IF NOT EXISTS idx_files_job_ext_del   ON files(job_id, ext, deleted);
CREATE INDEX IF NOT EXISTS idx_files_hash16        ON files(file_hash16);
CREATE INDEX IF NOT EXISTS idx_jobs_year           ON jobs(job_year);
CREATE INDEX IF NOT EXISTS idx_jobs_flags          ON jobs(has_compress, has_ame, has_dwg_dxf, has_pdf);

-- Rebuild FTS with unicode61+separators
DROP TABLE IF EXISTS fts_files;
CREATE VIRTUAL TABLE fts_files USING fts5(
  content,
  file_hash16 UNINDEXED,
  tokenize = "unicode61 separators '-_./()[]{}' remove_diacritics 2"
);

COMMIT;
PRAGMA foreign_keys = ON;
