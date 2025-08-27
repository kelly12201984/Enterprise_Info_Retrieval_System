-- SQLite
SELECT COUNT(*) AS jobs FROM jobs;
SELECT COUNT(*) AS files FROM files WHERE deleted=0;

SELECT job_id, file_count_total, has_pdf, has_dwg_dxf, has_compress, has_ame, last_modified_utc
FROM jobs
ORDER BY last_modified_utc DESC
LIMIT 20;

SELECT rel_path
FROM files
WHERE job_id='092-25' AND deleted=0
  AND tokens_fname LIKE '%open%' AND tokens_fname LIKE '%top%'
ORDER BY rel_path;
