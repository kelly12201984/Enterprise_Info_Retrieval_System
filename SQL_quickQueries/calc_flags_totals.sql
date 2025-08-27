SELECT
  SUM(CASE WHEN has_compress=1 THEN 1 ELSE 0 END) AS jobs_with_compress,
  SUM(CASE WHEN has_ame=1      THEN 1 ELSE 0 END) AS jobs_with_ame
FROM jobs;
