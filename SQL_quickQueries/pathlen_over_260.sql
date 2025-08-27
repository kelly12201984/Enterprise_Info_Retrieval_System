SELECT COUNT(*) AS over_260
FROM files f JOIN jobs j ON j.job_id=f.job_id
WHERE LENGTH(j.root_path || '\' || f.rel_path) > 260 AND f.deleted=0;
