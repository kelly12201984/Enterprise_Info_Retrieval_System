CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    root_path TEXT,
    year INT,
    customer TEXT,
    first_seen TEXT,
    last_seen TEXT,
    last_modified_utc TEXT,
    file_count_total INT,
    byte_size_total INT,
    folder_depth_max INT,
    has_compress INT DEFAULT 0,
    has_ame INT DEFAULT 0,
    has_dwg_dxf INT DEFAULT 0,
    has_pdf INT DEFAULT 0,
    has_photos INT DEFAULT 0,
    keywords_sample TEXT,
    score_completeness INT DEFAULT 0,
    errors_count INT DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS files (
    file_hash16 TEXT PRIMARY KEY,
    job_id TEXT,
    rel_path TEXT,
    ext TEXT,
    size_bytes INT,
    mtime_utc TEXT,
    kind TEXT,
    tokens_fname TEXT,
    detector_hits TEXT,
    deleted INT DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS fts_files
USING fts5(content, file_hash16, tokenize='porter');
