UPDATE jobs SET
  has_pdf = EXISTS(
    SELECT 1 FROM files f
    WHERE f.job_id = jobs.job_id AND f.deleted=0
      AND (f.ext='.pdf' OR instr(f.detector_hits,'pdf')>0)
  ),
  has_dwg_dxf = EXISTS(
    SELECT 1 FROM files f
    WHERE f.job_id = jobs.job_id AND f.deleted=0
      AND (f.ext IN('.dwg','.dxf') OR instr(f.detector_hits,'cad')>0)
  ),
  has_compress = EXISTS(
    SELECT 1 FROM files f
    WHERE f.job_id = jobs.job_id AND f.deleted=0
      AND instr(f.detector_hits,'compress')>0
  ),
  has_ame = EXISTS(
    SELECT 1 FROM files f
    WHERE f.job_id = jobs.job_id AND f.deleted=0
      AND instr(f.detector_hits,'ametank')>0
  ),
  last_modified_utc = (
    SELECT MAX(mtime_utc) FROM files f
    WHERE f.job_id = jobs.job_id AND f.deleted=0
  );
