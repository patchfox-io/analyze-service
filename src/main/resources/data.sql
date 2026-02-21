CREATE OR REPLACE FUNCTION TABULATE_PACKAGE_INDEX_DATA_BATCHED(
    purls_arg_str_encoded_array varchar,
    datasource_metrics_id_arg bigint,
    array_delimiter_arg varchar
)
RETURNS void
AS '
DECLARE
    purls_arg varchar[];
    dataset_metrics_tmp record;
    batch_counts record;
BEGIN
    -- Convert string to array once
    SELECT string_to_array(purls_arg_str_encoded_array, array_delimiter_arg) INTO purls_arg;
    
    -- Get the dataset metrics record once
    SELECT * FROM dataset_metrics dsm INTO dataset_metrics_tmp WHERE dsm.id = datasource_metrics_id_arg;
    
    -- Process this batch counting instances (duplicates matter)
    -- For each purl in the array, count it separately even if it appears multiple times
    SELECT
        SUM(CASE WHEN p.number_versions_behind_head > 0 THEN 1 ELSE 0 END) AS downlevel_count,
        SUM(CASE WHEN p.number_major_versions_behind_head > 0 THEN 1 END) AS downlevel_major_count,
        SUM(CASE WHEN p.number_minor_versions_behind_head > 0 THEN 1 END) AS downlevel_minor_count,
        SUM(CASE WHEN p.number_patch_versions_behind_head > 0 THEN 1 END) AS downlevel_patch_count,
        SUM(CASE WHEN p.most_recent_version_published_at < NOW()::DATE - interval ''6 months'' THEN 1 ELSE 0 END) AS stale_six_months_count,
        SUM(CASE WHEN p.most_recent_version_published_at < NOW()::DATE - interval ''12 months'' THEN 1 ELSE 0 END) AS stale_one_year_count,
        SUM(CASE WHEN p.most_recent_version_published_at < NOW()::DATE - interval ''18 months'' THEN 1 ELSE 0 END) AS stale_eighteen_months_count,
        SUM(CASE WHEN p.most_recent_version_published_at < NOW()::DATE - interval ''24 months'' THEN 1 ELSE 0 END) AS stale_two_years_count
    INTO batch_counts
    FROM unnest(purls_arg) WITH ORDINALITY AS u(purl_element, idx)
    LEFT JOIN package p ON p.purl = u.purl_element AND p.most_recent_version IS NOT NULL;
    
    -- Update the metrics by adding to existing values (accumulating)
    UPDATE dataset_metrics dsm
    SET
        downlevel_packages = dsm.downlevel_packages + batch_counts.downlevel_count,
        downlevel_packages_major = dsm.downlevel_packages_major + batch_counts.downlevel_major_count,
        downlevel_packages_minor = dsm.downlevel_packages_minor + batch_counts.downlevel_minor_count,
        downlevel_packages_patch = dsm.downlevel_packages_patch + batch_counts.downlevel_patch_count,
        stale_packages = dsm.stale_packages + batch_counts.stale_six_months_count,
        stale_packages_six_months = dsm.stale_packages_six_months + batch_counts.stale_six_months_count,
        stale_packages_one_year = dsm.stale_packages_one_year + batch_counts.stale_one_year_count,
        stale_packages_one_year_six_months = dsm.stale_packages_one_year_six_months + batch_counts.stale_eighteen_months_count,
        stale_packages_two_years = dsm.stale_packages_two_years + batch_counts.stale_two_years_count
    WHERE dsm.id = dataset_metrics_tmp.id;
END;
' LANGUAGE PLPGSQL;



-- CREATE OR REPLACE FUNCTION filter_purls_with_findings(p_purls_string TEXT)
-- RETURNS TEXT[] AS '
-- DECLARE
--     p_purl_array TEXT[];
--     result_array TEXT[];
-- BEGIN
--     -- Convert comma-delimited string to array
--     p_purl_array := string_to_array(p_purls_string, '','');
    
--     -- Directly query for PURLs with findings
--     SELECT array_agg(DISTINCT p.purl) INTO result_array
--     FROM package p
--     JOIN package_finding pf ON p.id = pf.package_id
--     WHERE p.purl = ANY(p_purl_array);
    
--     -- Return the result array
--     RETURN result_array;
-- END;
-- ' LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION filter_purls_with_findings(p_purls_string TEXT)
RETURNS TEXT[] AS '
DECLARE
    result_array TEXT[];
BEGIN
    -- Use unnest to force index usage instead of ANY(array)
    WITH purl_list AS (
        SELECT unnest(string_to_array(p_purls_string, '','')) AS purl
    )
    SELECT array_agg(DISTINCT p.purl) INTO result_array
    FROM purl_list pl
    INNER JOIN package p ON p.purl = pl.purl
    INNER JOIN package_finding pf ON p.id = pf.package_id;
    
    -- Return the result array (handle NULL case)
    RETURN COALESCE(result_array, ARRAY[]::TEXT[]);
END;
' LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE update_dataset_metrics_findings_counts(
  p_purls_string TEXT,
  p_dataset_metrics_id BIGINT
)
LANGUAGE plpgsql
AS '
BEGIN

  -- Single query optimization - eliminate temporary table and use direct CTE
  WITH purl_list AS (
    SELECT unnest(string_to_array(p_purls_string, '','')) AS purl
  ),
  relevant_packages AS (
    SELECT DISTINCT p.id
    FROM package p
    INNER JOIN purl_list pl ON p.purl = pl.purl
  ),
  findings_data AS (
    SELECT 
      rp.id as package_id,
      fd.severity
    FROM relevant_packages rp
    INNER JOIN package_finding pf ON rp.id = pf.package_id
    INNER JOIN finding f ON pf.finding_id = f.id
    INNER JOIN finding_data fd ON f.id = fd.finding_id
  ),
  aggregated_metrics AS (
    SELECT
      -- Package counts (distinct packages with findings of each severity)
      COUNT(DISTINCT package_id) AS packages_with_findings_count,
      COUNT(DISTINCT CASE WHEN severity = ''CRITICAL'' THEN package_id END) AS packages_with_critical_findings_count,
      COUNT(DISTINCT CASE WHEN severity = ''HIGH'' THEN package_id END) AS packages_with_high_findings_count,
      COUNT(DISTINCT CASE WHEN severity = ''MEDIUM'' THEN package_id END) AS packages_with_medium_findings_count,
      COUNT(DISTINCT CASE WHEN severity = ''LOW'' THEN package_id END) AS packages_with_low_findings_count,
      
      -- Finding counts (total findings by severity)
      COUNT(*) AS total_findings_count,
      COUNT(CASE WHEN severity = ''CRITICAL'' THEN 1 END) AS critical_findings_count,
      COUNT(CASE WHEN severity = ''HIGH'' THEN 1 END) AS high_findings_count,
      COUNT(CASE WHEN severity = ''MEDIUM'' THEN 1 END) AS medium_findings_count,
      COUNT(CASE WHEN severity = ''LOW'' THEN 1 END) AS low_findings_count
    FROM findings_data
  )
  UPDATE dataset_metrics 
  SET
    packages_with_findings = COALESCE(am.packages_with_findings_count, 0),
    packages_with_critical_findings = COALESCE(am.packages_with_critical_findings_count, 0),
    packages_with_high_findings = COALESCE(am.packages_with_high_findings_count, 0),
    packages_with_medium_findings = COALESCE(am.packages_with_medium_findings_count, 0),
    packages_with_low_findings = COALESCE(am.packages_with_low_findings_count, 0),
    total_findings = COALESCE(am.total_findings_count, 0),
    critical_findings = COALESCE(am.critical_findings_count, 0),
    high_findings = COALESCE(am.high_findings_count, 0),
    medium_findings = COALESCE(am.medium_findings_count, 0),
    low_findings = COALESCE(am.low_findings_count, 0)
  FROM aggregated_metrics am
  WHERE id = p_dataset_metrics_id;
END;
';



-- CREATE OR REPLACE PROCEDURE update_dataset_metrics_findings_counts(
--  p_purls_string TEXT,
--  p_dataset_metrics_id BIGINT
-- )
-- LANGUAGE plpgsql
-- AS '
-- DECLARE
--  total_findings_count BIGINT := 0;
--  critical_findings_count BIGINT := 0;
--  high_findings_count BIGINT := 0;
--  medium_findings_count BIGINT := 0;
--  low_findings_count BIGINT := 0;
--  packages_with_findings_count BIGINT := 0;
--  packages_with_critical_findings_count BIGINT := 0;
--  packages_with_high_findings_count BIGINT := 0;
--  packages_with_medium_findings_count BIGINT := 0;
--  packages_with_low_findings_count BIGINT := 0;
--  p_purl_array TEXT[];
-- BEGIN
--  -- Convert comma-delimited string to array
--  p_purl_array := string_to_array(p_purls_string, '','');
 
--  -- Create a temporary table for the PURLs list
--  CREATE TEMPORARY TABLE purl_list (purl TEXT);
 
--  -- Populate the temporary table with PURLs from the array
--  INSERT INTO purl_list 
--  SELECT unnest(p_purl_array);
 
--  -- Create index on the temporary table to speed up joins
--  CREATE INDEX idx_temp_purl_list ON purl_list(purl);
 
--  -- Identify relevant packages (those with findings and in our PURL list)
--  CREATE TEMPORARY TABLE relevant_packages AS
--  SELECT DISTINCT p.id, p.purl
--  FROM package p
--  JOIN package_finding pf ON p.id = pf.package_id
--  JOIN purl_list pl ON p.purl = pl.purl;
 
--  -- Now use a single query to compute all metrics using the relevant_packages table
--  WITH package_counts AS (
--    SELECT
--      COUNT(DISTINCT rp.id) AS packages_with_findings_count,
--      COUNT(DISTINCT CASE WHEN fd.severity = ''CRITICAL'' THEN rp.id END) AS packages_with_critical_findings_count,
--      COUNT(DISTINCT CASE WHEN fd.severity = ''HIGH'' THEN rp.id END) AS packages_with_high_findings_count,
--      COUNT(DISTINCT CASE WHEN fd.severity = ''MEDIUM'' THEN rp.id END) AS packages_with_medium_findings_count,
--      COUNT(DISTINCT CASE WHEN fd.severity = ''LOW'' THEN rp.id END) AS packages_with_low_findings_count,
--      COUNT(*) AS total_findings_count,
--      COUNT(CASE WHEN fd.severity = ''CRITICAL'' THEN 1 END) AS critical_findings_count,
--      COUNT(CASE WHEN fd.severity = ''HIGH'' THEN 1 END) AS high_findings_count,
--      COUNT(CASE WHEN fd.severity = ''MEDIUM'' THEN 1 END) AS medium_findings_count,
--      COUNT(CASE WHEN fd.severity = ''LOW'' THEN 1 END) AS low_findings_count
--    FROM
--      relevant_packages rp
--      JOIN package_finding pf ON rp.id = pf.package_id
--      JOIN finding f ON pf.finding_id = f.id
--      JOIN finding_data fd ON f.id = fd.finding_id
--  )
--  UPDATE dataset_metrics dm
--  SET
--    packages_with_findings = COALESCE(pc.packages_with_findings_count, 0),
--    packages_with_critical_findings = COALESCE(pc.packages_with_critical_findings_count, 0),
--    packages_with_high_findings = COALESCE(pc.packages_with_high_findings_count, 0),
--    packages_with_medium_findings = COALESCE(pc.packages_with_medium_findings_count, 0),
--    packages_with_low_findings = COALESCE(pc.packages_with_low_findings_count, 0),
--    total_findings = COALESCE(pc.total_findings_count, 0),
--    critical_findings = COALESCE(pc.critical_findings_count, 0),
--    high_findings = COALESCE(pc.high_findings_count, 0),
--    medium_findings = COALESCE(pc.medium_findings_count, 0),
--    low_findings = COALESCE(pc.low_findings_count, 0)
--  FROM
--    package_counts pc
--  WHERE
--    dm.id = p_dataset_metrics_id;
   
--  -- Clean up
--  DROP TABLE IF EXISTS relevant_packages;
--  DROP TABLE IF EXISTS purl_list;
-- END;
-- ';



-- CREATE OR REPLACE PROCEDURE update_edit_finding_counts(
--  package_purls_list VARCHAR,
--  commit_dt TIMESTAMP WITH TIME ZONE,
--  datasource_purl VARCHAR
-- )
-- LANGUAGE plpgsql
-- AS '
-- DECLARE
--  datasource_id_arg BIGINT;
-- BEGIN

--  -- Get the datasource ID from the provided PURL
--  SELECT id INTO datasource_id_arg FROM datasource WHERE purl = datasource_purl;
 
--  IF datasource_id_arg IS NULL THEN
--    RAISE NOTICE ''Datasource with PURL % not found'', datasource_purl;
--    RETURN;
--  END IF;

--  -- Single bulk update using CTEs - eliminates the loop entirely
--  WITH package_purls AS (
--    SELECT unnest(string_to_array(package_purls_list, '','')) AS purl
--  ),
--  relevant_edits AS (
--    SELECT 
--      e.id as edit_id,
--      e.after as package_purl
--    FROM edit e
--    INNER JOIN package_purls pp ON e.after = pp.purl
--    WHERE e.datasource_id = datasource_id_arg
--      AND e.commit_date_time = commit_dt
--  ),
--  finding_counts_by_edit AS (
--    SELECT 
--      re.edit_id,
--      SUM(CASE WHEN fd.severity = ''CRITICAL'' THEN 1 ELSE 0 END) AS critical_count,
--      SUM(CASE WHEN fd.severity = ''HIGH'' THEN 1 ELSE 0 END) AS high_count,
--      SUM(CASE WHEN fd.severity = ''MEDIUM'' THEN 1 ELSE 0 END) AS medium_count,
--      SUM(CASE WHEN fd.severity = ''LOW'' THEN 1 ELSE 0 END) AS low_count
--    FROM relevant_edits re
--    INNER JOIN package p ON p.purl = re.package_purl
--    INNER JOIN package_finding pf ON p.id = pf.package_id
--    INNER JOIN finding f ON f.id = pf.finding_id
--    INNER JOIN finding_data fd ON fd.finding_id = f.id
--    GROUP BY re.edit_id
--  )
--  UPDATE edit e
--  SET 
--    critical_findings = COALESCE(fc.critical_count, 0),
--    high_findings = COALESCE(fc.high_count, 0),
--    medium_findings = COALESCE(fc.medium_count, 0),
--    low_findings = COALESCE(fc.low_count, 0)
--  FROM finding_counts_by_edit fc
--  WHERE e.id = fc.edit_id;

-- END;
-- ';


CREATE OR REPLACE PROCEDURE update_edit_finding_counts(
 package_purls_list VARCHAR,
 commit_dt TIMESTAMP WITH TIME ZONE,
 datasource_purl VARCHAR
)
LANGUAGE plpgsql
AS '
DECLARE
 datasource_id_arg BIGINT;
BEGIN
 -- Get the datasource ID from the provided PURL
 SELECT id INTO datasource_id_arg FROM datasource WHERE purl = datasource_purl;
 
 IF datasource_id_arg IS NULL THEN
   RAISE NOTICE ''Datasource with PURL % not found'', datasource_purl;
   RETURN;
 END IF;

 -- Single bulk update - but keep the original logic of matching packages to edits
 WITH package_purls AS (
   SELECT unnest(string_to_array(package_purls_list, '','')) AS purl
 ),
 edit_package_matches AS (
   SELECT 
     e.id as edit_id,
     pp.purl as package_purl
   FROM package_purls pp
   INNER JOIN edit e ON e.after = pp.purl  -- This is the key fix!
   WHERE e.datasource_id = datasource_id_arg
     AND e.commit_date_time = commit_dt
 ),
 finding_counts_by_edit AS (
   SELECT 
     epm.edit_id,
     SUM(CASE WHEN fd.severity = ''CRITICAL'' THEN 1 ELSE 0 END) AS critical_count,
     SUM(CASE WHEN fd.severity = ''HIGH'' THEN 1 ELSE 0 END) AS high_count,
     SUM(CASE WHEN fd.severity = ''MEDIUM'' THEN 1 ELSE 0 END) AS medium_count,
     SUM(CASE WHEN fd.severity = ''LOW'' THEN 1 ELSE 0 END) AS low_count
   FROM edit_package_matches epm
   INNER JOIN package p ON p.purl = epm.package_purl
   INNER JOIN package_finding pf ON p.id = pf.package_id
   INNER JOIN finding f ON f.id = pf.finding_id
   INNER JOIN finding_data fd ON fd.finding_id = f.id
   GROUP BY epm.edit_id
 )
 UPDATE edit e
 SET 
   critical_findings = COALESCE(fc.critical_count, 0),
   high_findings = COALESCE(fc.high_count, 0),
   medium_findings = COALESCE(fc.medium_count, 0),
   low_findings = COALESCE(fc.low_count, 0)
 FROM finding_counts_by_edit fc
 WHERE e.id = fc.edit_id;

END;
';


CREATE OR REPLACE FUNCTION get_edit_pairs(commit_dt TIMESTAMP WITH TIME ZONE)
RETURNS TABLE (
    before_value VARCHAR,
    after_value VARCHAR
)
LANGUAGE plpgsql
AS '
BEGIN
    -- Return distinct before/after pairs for edits with the given commit date
    RETURN QUERY
    SELECT DISTINCT e.before, e.after
    FROM edit e
    WHERE e.commit_date_time = commit_dt;
END;
';


CREATE OR REPLACE PROCEDURE create_findings_performance_indexes()
LANGUAGE plpgsql
AS '
BEGIN
  -- Indexes for dataset metrics procedure
  CREATE INDEX IF NOT EXISTS idx_package_purl ON package(purl);
  CREATE INDEX IF NOT EXISTS idx_package_finding_package_id ON package_finding(package_id);
  CREATE INDEX IF NOT EXISTS idx_package_finding_finding_id ON package_finding(finding_id);
  CREATE INDEX IF NOT EXISTS idx_finding_data_finding_id ON finding_data(finding_id);
  CREATE INDEX IF NOT EXISTS idx_finding_data_severity ON finding_data(severity);
  
  -- Indexes for edit finding counts procedure
  CREATE INDEX IF NOT EXISTS idx_datasource_purl ON datasource(purl);
  CREATE INDEX IF NOT EXISTS idx_edit_datasource_commit_after ON edit(datasource_id, commit_date_time, after);
  CREATE INDEX IF NOT EXISTS idx_edit_id ON edit(id);
  
  RAISE NOTICE ''All performance indexes created successfully'';
END;
';






CREATE OR REPLACE FUNCTION update_edit_and_dataset_metrics_findings(
  edit_package_purls_list VARCHAR,
  edit_commit_dt TIMESTAMP WITH TIME ZONE,
  edit_datasource_purl VARCHAR,
  dataset_package_purls_list VARCHAR,
  dataset_metrics_id BIGINT
) RETURNS void
LANGUAGE plpgsql
AS '
DECLARE
  start_time TIMESTAMP := clock_timestamp();
  call_id UUID := gen_random_uuid();
  datasource_id_arg BIGINT;
  package_id_array BIGINT[];
  edit_package_id_array BIGINT[];
BEGIN

-- Create timing table if it doesn''t exist
CREATE TABLE IF NOT EXISTS procedure_timing (
    id SERIAL PRIMARY KEY,
    procedure_call_id UUID,
    step_name TEXT,
    elapsed_ms NUMERIC,
    timestamp TIMESTAMP DEFAULT clock_timestamp()
);

  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Starting procedure'', 0);

  -- Get the datasource ID from the provided PURL
  SELECT id INTO datasource_id_arg FROM datasource WHERE purl = edit_datasource_purl;
  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Got datasource ID'', EXTRACT(epoch FROM (clock_timestamp() - start_time)) * 1000);

  IF datasource_id_arg IS NULL THEN
    RAISE NOTICE ''Datasource with PURL % not found'', edit_datasource_purl;
    RETURN;
  END IF;

  -- Convert edit PURLs to package ID array
  SELECT ARRAY(
    SELECT p.id 
    FROM unnest(string_to_array(edit_package_purls_list, '','')) AS purl_input
    INNER JOIN package p ON p.purl = purl_input
  ) INTO edit_package_id_array;
  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Converted edit PURLs'', EXTRACT(epoch FROM (clock_timestamp() - start_time)) * 1000);

  -- Update edit findings using direct array lookups
  UPDATE edit e
  SET 
    critical_findings = (SELECT COUNT(*) FROM package_critical_finding pcf WHERE pcf.package_id = p.id),
    high_findings = (SELECT COUNT(*) FROM package_high_finding phf WHERE phf.package_id = p.id),
    medium_findings = (SELECT COUNT(*) FROM package_medium_finding pmf WHERE pmf.package_id = p.id),
    low_findings = (SELECT COUNT(*) FROM package_low_finding plf WHERE plf.package_id = p.id)
  FROM package p
  WHERE e.after = p.purl
    AND e.datasource_id = datasource_id_arg
    AND e.commit_date_time = edit_commit_dt
    AND p.id = ANY(edit_package_id_array);
  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Updated edit findings'', EXTRACT(epoch FROM (clock_timestamp() - start_time)) * 1000);

  -- Convert dataset PURLs to package ID array
  SELECT ARRAY(
    SELECT p.id 
    FROM unnest(string_to_array(dataset_package_purls_list, '','')) AS purl_input
    INNER JOIN package p ON p.purl = purl_input
  ) INTO package_id_array;
  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Converted package PURLs'', EXTRACT(epoch FROM (clock_timestamp() - start_time)) * 1000);

  -- Update dataset metrics using direct array-based counts
  UPDATE dataset_metrics 
  SET
    -- Package counts (distinct packages with findings of each severity)
    packages_with_critical_findings = (
      SELECT COUNT(DISTINCT package_id) 
      FROM package_critical_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    packages_with_high_findings = (
      SELECT COUNT(DISTINCT package_id) 
      FROM package_high_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    packages_with_medium_findings = (
      SELECT COUNT(DISTINCT package_id) 
      FROM package_medium_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    packages_with_low_findings = (
      SELECT COUNT(DISTINCT package_id) 
      FROM package_low_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    packages_with_findings = (
      SELECT COUNT(DISTINCT package_id) 
      FROM (
        SELECT package_id FROM package_critical_finding WHERE package_id = ANY(package_id_array)
        UNION
        SELECT package_id FROM package_high_finding WHERE package_id = ANY(package_id_array)
        UNION
        SELECT package_id FROM package_medium_finding WHERE package_id = ANY(package_id_array)
        UNION
        SELECT package_id FROM package_low_finding WHERE package_id = ANY(package_id_array)
      ) all_packages_with_findings
    ),
    -- Finding counts (total findings by severity)
    critical_findings = (
      SELECT COUNT(*) 
      FROM package_critical_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    high_findings = (
      SELECT COUNT(*) 
      FROM package_high_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    medium_findings = (
      SELECT COUNT(*) 
      FROM package_medium_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    low_findings = (
      SELECT COUNT(*) 
      FROM package_low_finding 
      WHERE package_id = ANY(package_id_array)
    ),
    total_findings = (
      SELECT 
        COALESCE((SELECT COUNT(*) FROM package_critical_finding WHERE package_id = ANY(package_id_array)), 0) +
        COALESCE((SELECT COUNT(*) FROM package_high_finding WHERE package_id = ANY(package_id_array)), 0) +
        COALESCE((SELECT COUNT(*) FROM package_medium_finding WHERE package_id = ANY(package_id_array)), 0) +
        COALESCE((SELECT COUNT(*) FROM package_low_finding WHERE package_id = ANY(package_id_array)), 0)
    )
  WHERE id = dataset_metrics_id;
  INSERT INTO procedure_timing (procedure_call_id, step_name, elapsed_ms) 
  VALUES (call_id, ''Updated dataset_metrics and END PROC'', EXTRACT(epoch FROM (clock_timestamp() - start_time)) * 1000);

END;
';





-- CREATE OR REPLACE FUNCTION update_edit_and_dataset_metrics_findings(
--   edit_package_purls_list VARCHAR,
--   edit_commit_dt TIMESTAMP WITH TIME ZONE,
--   edit_datasource_purl VARCHAR,
--   dataset_package_purls_list VARCHAR,
--   dataset_metrics_id BIGINT
-- )
-- LANGUAGE plpgsql
-- AS '
-- DECLARE
--   datasource_id_arg BIGINT;
--   package_id_array BIGINT[];
--   edit_package_id_array BIGINT[];
-- BEGIN

--   -- Get the datasource ID from the provided PURL
--   SELECT id INTO datasource_id_arg FROM datasource WHERE purl = edit_datasource_purl;
  
--   IF datasource_id_arg IS NULL THEN
--     RAISE NOTICE ''Datasource with PURL % not found'', edit_datasource_purl;
--     RETURN;
--   END IF;

--   -- Convert edit PURLs to package ID array
--   SELECT ARRAY(
--     SELECT p.id 
--     FROM unnest(string_to_array(edit_package_purls_list, '','')) AS purl_input
--     INNER JOIN package p ON p.purl = purl_input
--   ) INTO edit_package_id_array;

--   -- Update edit findings using direct array lookups
--   UPDATE edit e
--   SET 
--     critical_findings = (SELECT COUNT(*) FROM package_critical_finding pcf WHERE pcf.package_id = p.id),
--     high_findings = (SELECT COUNT(*) FROM package_high_finding phf WHERE phf.package_id = p.id),
--     medium_findings = (SELECT COUNT(*) FROM package_medium_finding pmf WHERE pmf.package_id = p.id),
--     low_findings = (SELECT COUNT(*) FROM package_low_finding plf WHERE plf.package_id = p.id)
--   FROM package p
--   WHERE e.after = p.purl
--     AND e.datasource_id = datasource_id_arg
--     AND e.commit_date_time = edit_commit_dt
--     AND p.id = ANY(edit_package_id_array);

--   -- Convert dataset PURLs to package ID array
--   SELECT ARRAY(
--     SELECT p.id 
--     FROM unnest(string_to_array(dataset_package_purls_list, '','')) AS purl_input
--     INNER JOIN package p ON p.purl = purl_input
--   ) INTO package_id_array;

--   -- Update dataset metrics using direct array-based counts
--   UPDATE dataset_metrics 
--   SET
--     -- Package counts (distinct packages with findings of each severity)
--     packages_with_critical_findings = (
--       SELECT COUNT(DISTINCT package_id) 
--       FROM package_critical_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     packages_with_high_findings = (
--       SELECT COUNT(DISTINCT package_id) 
--       FROM package_high_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     packages_with_medium_findings = (
--       SELECT COUNT(DISTINCT package_id) 
--       FROM package_medium_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     packages_with_low_findings = (
--       SELECT COUNT(DISTINCT package_id) 
--       FROM package_low_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     packages_with_findings = (
--       SELECT COUNT(DISTINCT package_id) 
--       FROM (
--         SELECT package_id FROM package_critical_finding WHERE package_id = ANY(package_id_array)
--         UNION
--         SELECT package_id FROM package_high_finding WHERE package_id = ANY(package_id_array)
--         UNION
--         SELECT package_id FROM package_medium_finding WHERE package_id = ANY(package_id_array)
--         UNION
--         SELECT package_id FROM package_low_finding WHERE package_id = ANY(package_id_array)
--       ) all_packages_with_findings
--     ),
    
--     -- Finding counts (total findings by severity)
--     critical_findings = (
--       SELECT COUNT(*) 
--       FROM package_critical_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     high_findings = (
--       SELECT COUNT(*) 
--       FROM package_high_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     medium_findings = (
--       SELECT COUNT(*) 
--       FROM package_medium_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     low_findings = (
--       SELECT COUNT(*) 
--       FROM package_low_finding 
--       WHERE package_id = ANY(package_id_array)
--     ),
--     total_findings = (
--       SELECT 
--         COALESCE((SELECT COUNT(*) FROM package_critical_finding WHERE package_id = ANY(package_id_array)), 0) +
--         COALESCE((SELECT COUNT(*) FROM package_high_finding WHERE package_id = ANY(package_id_array)), 0) +
--         COALESCE((SELECT COUNT(*) FROM package_medium_finding WHERE package_id = ANY(package_id_array)), 0) +
--         COALESCE((SELECT COUNT(*) FROM package_low_finding WHERE package_id = ANY(package_id_array)), 0)
--     )
--   WHERE id = dataset_metrics_id;

-- END;
-- ';




-- works but is too slow 
-- CREATE OR REPLACE PROCEDURE update_edit_and_dataset_metrics_findings(
--   edit_package_purls_list VARCHAR,
--   edit_commit_dt TIMESTAMP WITH TIME ZONE,
--   edit_datasource_purl VARCHAR,
--   dataset_package_purls_list VARCHAR,
--   dataset_metrics_id BIGINT
-- )
-- LANGUAGE plpgsql
-- AS '
-- DECLARE
--   datasource_id_arg BIGINT;
-- BEGIN

--   -- Create missing indexes on severity tables if they do not exist
--   CREATE INDEX IF NOT EXISTS idx_package_critical_finding_package_id ON package_critical_finding(package_id);
--   CREATE INDEX IF NOT EXISTS idx_package_high_finding_package_id ON package_high_finding(package_id);
--   CREATE INDEX IF NOT EXISTS idx_package_medium_finding_package_id ON package_medium_finding(package_id);
--   CREATE INDEX IF NOT EXISTS idx_package_low_finding_package_id ON package_low_finding(package_id);

--   -- Get the datasource ID from the provided PURL
--   SELECT id INTO datasource_id_arg FROM datasource WHERE purl = edit_datasource_purl;
  
--   IF datasource_id_arg IS NULL THEN
--     RAISE NOTICE ''Datasource with PURL % not found'', edit_datasource_purl;
--     RETURN;
--   END IF;

--   -- Update edit findings (optimized to eliminate ALL JOINs)
--   WITH edit_package_purls AS (
--     SELECT DISTINCT unnest(string_to_array(edit_package_purls_list, '','')) AS purl_value
--   ),
--   edit_packages AS (
--     SELECT e.id as edit_id, p.id as package_id
--     FROM edit_package_purls epp
--     INNER JOIN edit e ON e.after = epp.purl_value
--     INNER JOIN package p ON p.purl = epp.purl_value
--     WHERE e.datasource_id = datasource_id_arg
--       AND e.commit_date_time = edit_commit_dt
--   ),
--   edit_finding_counts AS (
--     SELECT 
--       ep.edit_id,
--       (SELECT COUNT(pcf.finding_id) FROM package_critical_finding pcf WHERE pcf.package_id = ep.package_id) as critical_count,
--       (SELECT COUNT(phf.finding_id) FROM package_high_finding phf WHERE phf.package_id = ep.package_id) as high_count,
--       (SELECT COUNT(pmf.finding_id) FROM package_medium_finding pmf WHERE pmf.package_id = ep.package_id) as medium_count,
--       (SELECT COUNT(plf.finding_id) FROM package_low_finding plf WHERE plf.package_id = ep.package_id) as low_count
--     FROM edit_packages ep
--   )
--   UPDATE edit e
--   SET 
--     critical_findings = COALESCE(efc.critical_count, 0),
--     high_findings = COALESCE(efc.high_count, 0),
--     medium_findings = COALESCE(efc.medium_count, 0),
--     low_findings = COALESCE(efc.low_count, 0)
--   FROM edit_finding_counts efc
--   WHERE e.id = efc.edit_id;

--   -- OPTIMIZED dataset metrics: use INNER JOINs to eliminate expensive LEFT JOINs
--   WITH package_ids AS (
--     SELECT p.id
--     FROM unnest(string_to_array(dataset_package_purls_list, '','')) AS purl_input
--     INNER JOIN package p ON p.purl = purl_input
--   ),
--   critical_findings AS (
--     SELECT pcf.package_id, COUNT(pcf.finding_id) as finding_count
--     FROM package_critical_finding pcf
--     INNER JOIN package_ids pi ON pcf.package_id = pi.id
--     GROUP BY pcf.package_id
--   ),
--   high_findings AS (
--     SELECT phf.package_id, COUNT(phf.finding_id) as finding_count
--     FROM package_high_finding phf
--     INNER JOIN package_ids pi ON phf.package_id = pi.id
--     GROUP BY phf.package_id
--   ),
--   medium_findings AS (
--     SELECT pmf.package_id, COUNT(pmf.finding_id) as finding_count
--     FROM package_medium_finding pmf
--     INNER JOIN package_ids pi ON pmf.package_id = pi.id
--     GROUP BY pmf.package_id
--   ),
--   low_findings AS (
--     SELECT plf.package_id, COUNT(plf.finding_id) as finding_count
--     FROM package_low_finding plf
--     INNER JOIN package_ids pi ON plf.package_id = pi.id
--     GROUP BY plf.package_id
--   ),
--   packages_with_any_findings AS (
--     SELECT package_id FROM critical_findings
--     UNION
--     SELECT package_id FROM high_findings
--     UNION
--     SELECT package_id FROM medium_findings
--     UNION
--     SELECT package_id FROM low_findings
--   ),
--   aggregated_counts AS (
--     SELECT
--       (SELECT COUNT(*) FROM packages_with_any_findings) AS packages_with_findings_count,
--       (SELECT COUNT(DISTINCT package_id) FROM critical_findings) AS packages_with_critical_findings_count,
--       (SELECT COUNT(DISTINCT package_id) FROM high_findings) AS packages_with_high_findings_count,
--       (SELECT COUNT(DISTINCT package_id) FROM medium_findings) AS packages_with_medium_findings_count,
--       (SELECT COUNT(DISTINCT package_id) FROM low_findings) AS packages_with_low_findings_count,
--       (SELECT COALESCE(SUM(finding_count), 0) FROM critical_findings) AS critical_findings_count,
--       (SELECT COALESCE(SUM(finding_count), 0) FROM high_findings) AS high_findings_count,
--       (SELECT COALESCE(SUM(finding_count), 0) FROM medium_findings) AS medium_findings_count,
--       (SELECT COALESCE(SUM(finding_count), 0) FROM low_findings) AS low_findings_count
--   )
--   UPDATE dataset_metrics dm
--   SET
--     packages_with_findings = ac.packages_with_findings_count,
--     packages_with_critical_findings = ac.packages_with_critical_findings_count,
--     packages_with_high_findings = ac.packages_with_high_findings_count,
--     packages_with_medium_findings = ac.packages_with_medium_findings_count,
--     packages_with_low_findings = ac.packages_with_low_findings_count,
--     total_findings = ac.critical_findings_count + ac.high_findings_count + 
--                     ac.medium_findings_count + ac.low_findings_count,
--     critical_findings = ac.critical_findings_count,
--     high_findings = ac.high_findings_count,
--     medium_findings = ac.medium_findings_count,
--     low_findings = ac.low_findings_count
--   FROM aggregated_counts ac
--   WHERE dm.id = dataset_metrics_id;

-- END;
-- ';




-- 1. Fast edit creation function (ORIGINAL from your working system)
CREATE OR REPLACE FUNCTION detect_and_create_package_edits_fast(
    p_dataset_metrics_id BIGINT,
    p_datasource_event_id BIGINT,
    p_current_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_historical_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_datasource_purl VARCHAR,
    p_current_package_purls VARCHAR,
    p_current_package_families VARCHAR,  
    p_historical_package_purls VARCHAR DEFAULT NULL,
    p_historical_package_families VARCHAR DEFAULT NULL,
    p_already_seen_current_commit BOOLEAN DEFAULT FALSE,
    p_historical_and_current_same BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    edit_type VARCHAR(10),
    before_purl VARCHAR,
    after_purl VARCHAR,
    is_same_edit BOOLEAN,
    same_edit_count INTEGER,
    is_pf_recommended_edit BOOLEAN
) 
AS '
DECLARE
    current_purl_array VARCHAR[];
    current_family_array VARCHAR[];
    historical_purl_array VARCHAR[];
    historical_family_array VARCHAR[];
    current_array_length INTEGER;
    historical_array_length INTEGER;
    datasource_exists_in_historical BOOLEAN := FALSE;
BEGIN
    -- Convert comma-delimited strings to arrays
    current_purl_array := string_to_array(p_current_package_purls, '','');
    current_family_array := string_to_array(p_current_package_families, '','');
    
    IF p_historical_package_purls IS NOT NULL AND p_historical_package_purls != '''' THEN
        historical_purl_array := string_to_array(p_historical_package_purls, '','');
        historical_family_array := string_to_array(p_historical_package_families, '','');
        datasource_exists_in_historical := TRUE;
    END IF;
    
    current_array_length := COALESCE(array_length(current_purl_array, 1), 0);
    historical_array_length := COALESCE(array_length(historical_purl_array, 1), 0);
    
    -- CASE 1: First cycle through loop - everything is CREATE
    IF NOT p_already_seen_current_commit AND p_historical_and_current_same THEN
        RETURN QUERY
        WITH current_packages AS (
            SELECT unnest(current_purl_array) AS purl
        )
        SELECT 
            ''CREATE''::VARCHAR(10) as edit_type,
            ''''::VARCHAR as before_purl,
            cp.purl::VARCHAR as after_purl,
            FALSE::BOOLEAN as is_same_edit,
            0::INTEGER as same_edit_count,
            FALSE::BOOLEAN as is_pf_recommended_edit
        FROM current_packages cp
        WHERE cp.purl IS NOT NULL AND cp.purl != '''';
        RETURN;
    END IF;
    
    -- CASE 2a: New datasource - everything is CREATE  
    IF NOT datasource_exists_in_historical OR historical_array_length = 0 THEN
        RETURN QUERY
        WITH current_packages AS (
            SELECT unnest(current_purl_array) AS purl
        )
        SELECT 
            ''CREATE''::VARCHAR(10) as edit_type,
            ''''::VARCHAR as before_purl,
            cp.purl::VARCHAR as after_purl,
            FALSE::BOOLEAN as is_same_edit,
            0::INTEGER as same_edit_count,
            FALSE::BOOLEAN as is_pf_recommended_edit
        FROM current_packages cp
        WHERE cp.purl IS NOT NULL AND cp.purl != '''';
        RETURN;
    END IF;
    
    -- CASE 2b: Existing datasource - Use fast set-based operations
    RETURN QUERY
    -- WITH current_packages AS (
    --     SELECT 
    --         unnest(current_purl_array) AS purl,
    --         unnest(current_family_array) AS family
    -- ),
    -- historical_packages AS (
    --     SELECT 
    --         unnest(historical_purl_array) AS purl,
    --         unnest(historical_family_array) AS family
    -- ),
    WITH current_packages AS (
        SELECT 
            purl_element AS purl,
            family_element AS family
        FROM 
            unnest(current_purl_array) WITH ORDINALITY AS t1(purl_element, purl_ord)
        JOIN 
            unnest(current_family_array) WITH ORDINALITY AS t2(family_element, family_ord)
            ON t1.purl_ord = t2.family_ord
    ),
    historical_packages AS (
        SELECT 
            purl_element AS purl,
            family_element AS family
        FROM 
            unnest(historical_purl_array) WITH ORDINALITY AS t1(purl_element, purl_ord)
        JOIN 
            unnest(historical_family_array) WITH ORDINALITY AS t2(family_element, family_ord)
            ON t1.purl_ord = t2.family_ord
    ), 
    packages_needing_edits AS (
        SELECT cp.purl, cp.family
        FROM current_packages cp
        LEFT JOIN historical_packages hp ON cp.purl = hp.purl
        WHERE cp.purl IS NOT NULL AND cp.purl != ''''
          AND hp.purl IS NULL
    ),
    edit_analysis AS (
        SELECT 
            pne.purl as current_purl,
            pne.family as current_family,
            hp.purl as matching_historical_purl,
            CASE 
                WHEN hp.purl IS NOT NULL THEN ''UPDATE''
                ELSE ''CREATE''
            END as edit_type_determined,
            CASE 
                WHEN hp.purl IS NOT NULL THEN hp.purl
                ELSE ''''
            END as before_purl_determined
        FROM packages_needing_edits pne
        LEFT JOIN historical_packages hp ON pne.family = hp.family
    ),
    delete_candidates AS (
        SELECT hp.purl as historical_purl
        FROM historical_packages hp
        LEFT JOIN current_packages cp ON hp.purl = cp.purl
        WHERE hp.purl IS NOT NULL AND hp.purl != ''''
          AND cp.purl IS NULL
    ),
    actual_deletes AS (
        SELECT dc.historical_purl
        FROM delete_candidates dc
        LEFT JOIN edit_analysis ea ON dc.historical_purl = ea.before_purl_determined
        WHERE ea.before_purl_determined IS NULL
    ),
    all_edits AS (
        SELECT 
            ea.edit_type_determined as edit_type,
            ea.before_purl_determined as before_purl,
            ea.current_purl as after_purl
        FROM edit_analysis ea
        
        UNION ALL
        
        SELECT 
            ''DELETE'' as edit_type,
            ad.historical_purl as before_purl,
            '''' as after_purl
        FROM actual_deletes ad
    )
    SELECT 
        ae.edit_type::VARCHAR(10) as edit_type,
        ae.before_purl::VARCHAR as before_purl,
        ae.after_purl::VARCHAR as after_purl,
        FALSE::BOOLEAN as is_same_edit,
        0::INTEGER as same_edit_count,
        FALSE::BOOLEAN as is_pf_recommended_edit
    FROM all_edits ae;
    
    RETURN;
END;
'
LANGUAGE plpgsql;

-- 2. Fast insert procedure (ORIGINAL from your working system)
CREATE OR REPLACE PROCEDURE insert_package_edits_fast(
    p_dataset_metrics_id BIGINT,
    p_datasource_event_id BIGINT,
    p_current_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_historical_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_datasource_purl VARCHAR,
    p_current_package_purls VARCHAR,
    p_current_package_families VARCHAR,
    p_historical_package_purls VARCHAR DEFAULT NULL,
    p_historical_package_families VARCHAR DEFAULT NULL,
    p_already_seen_current_commit BOOLEAN DEFAULT FALSE,
    p_historical_and_current_same BOOLEAN DEFAULT FALSE
)
AS '
DECLARE
    edit_rec RECORD;
    event_commit_dt TIMESTAMP WITH TIME ZONE;
    event_event_dt TIMESTAMP WITH TIME ZONE;
    datasource_id_val BIGINT;
    edit_count INTEGER := 0;
BEGIN
    SELECT de.commit_date_time, de.event_date_time, de.datasource_id
    INTO event_commit_dt, event_event_dt, datasource_id_val
    FROM datasource_event de
    WHERE de.id = p_datasource_event_id;
    
    IF datasource_id_val IS NULL THEN
        RAISE EXCEPTION ''Datasource event not found for ID: %'', p_datasource_event_id;
    END IF;
    
    FOR edit_rec IN 
        SELECT * FROM detect_and_create_package_edits_fast(
            p_dataset_metrics_id,
            p_datasource_event_id, 
            p_current_commit_datetime,
            p_historical_commit_datetime,
            p_datasource_purl,
            p_current_package_purls,
            p_current_package_families,
            p_historical_package_purls,
            p_historical_package_families,
            p_already_seen_current_commit,
            p_historical_and_current_same
        )
    LOOP
        INSERT INTO edit (
            dataset_metrics_id,
            datasource_id,
            commit_date_time,
            event_date_time,
            edit_type,
            before,
            after,
            is_user_edit,
            is_same_edit,
            same_edit_count,
            is_pf_recommended_edit,
            critical_findings,
            high_findings,
            medium_findings,
            low_findings
        ) VALUES (
            p_dataset_metrics_id,
            datasource_id_val,
            event_commit_dt,
            event_event_dt,
            edit_rec.edit_type,
            edit_rec.before_purl,
            edit_rec.after_purl,
            TRUE,
            FALSE,
            0,
            edit_rec.is_pf_recommended_edit,
            0,
            0,
            0,
            0
        );
        
        edit_count := edit_count + 1;
    END LOOP;
    
END;
'
LANGUAGE plpgsql;

-- 3. Statistics calculation function (ORIGINAL from your working system)
CREATE OR REPLACE FUNCTION calculate_edit_statistics(
    p_dataset_metrics_id BIGINT,
    p_commit_datetime TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
    total_patches INTEGER,
    same_patches INTEGER,
    different_patches INTEGER,
    pf_patches INTEGER
)
AS '
BEGIN
    IF p_dataset_metrics_id IS NULL THEN
        RAISE EXCEPTION ''Dataset metrics ID cannot be NULL'';
    END IF;
    
    IF p_commit_datetime IS NULL THEN
        RAISE EXCEPTION ''Commit datetime cannot be NULL'';
    END IF;
    
    RETURN QUERY
    SELECT 
        COUNT(*)::INTEGER as total_patches,
        COUNT(CASE WHEN e.is_same_edit = TRUE THEN 1 END)::INTEGER as same_patches,
        COUNT(CASE WHEN e.is_same_edit = FALSE OR e.is_same_edit IS NULL THEN 1 END)::INTEGER as different_patches,
        COUNT(CASE WHEN e.is_pf_recommended_edit = TRUE THEN 1 END)::INTEGER as pf_patches
    FROM edit e
    WHERE e.dataset_metrics_id = p_dataset_metrics_id
      AND e.commit_date_time = p_commit_datetime;
END;
' 
LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE update_same_and_pf_edits_batch(
    p_dataset_metrics_id BIGINT,
    p_commit_datetime TIMESTAMP WITH TIME ZONE
) 
AS '
DECLARE
    cutoff_date TIMESTAMP WITH TIME ZONE;
BEGIN
    cutoff_date := p_commit_datetime - INTERVAL ''90 days'';
    
    -- Single bulk update using CTEs to eliminate the loop entirely
    WITH current_edits AS (
        SELECT id, edit_type, before, after
        FROM edit 
        WHERE dataset_metrics_id = p_dataset_metrics_id
          AND commit_date_time = p_commit_datetime
    ),
    historical_edit_counts AS (
        SELECT 
            ce.id as current_edit_id,
            ce.edit_type,
            ce.before as current_before,
            ce.after as current_after,
            COUNT(he.id) as same_count,
            -- Check for PF edits (same edit within same dataset)
            COUNT(CASE WHEN he.dataset_metrics_id = p_dataset_metrics_id THEN 1 END) > 0 as has_pf_edit
        FROM current_edits ce
        LEFT JOIN edit he ON (
            -- Match logic based on edit type
            CASE 
                WHEN ce.edit_type = ''CREATE'' THEN 
                    (he.before = '''' OR he.before IS NULL) AND he.after = ce.after
                WHEN ce.edit_type = ''UPDATE'' THEN 
                    he.before = ce.before AND he.after = ce.after
                WHEN ce.edit_type = ''DELETE'' THEN 
                    he.before = ce.before AND (he.after = '''' OR he.after IS NULL)
                ELSE FALSE
            END
            AND he.commit_date_time < p_commit_datetime
            AND he.commit_date_time >= cutoff_date
        )
        GROUP BY ce.id, ce.edit_type, ce.before, ce.after
    )
    UPDATE edit e
    SET 
        is_same_edit = (hec.same_count > 0),
        same_edit_count = hec.same_count,
        is_pf_recommended_edit = hec.has_pf_edit
    FROM historical_edit_counts hec
    WHERE e.id = hec.current_edit_id;
END;
'
LANGUAGE plpgsql;




-- CREATE OR REPLACE PROCEDURE update_same_and_pf_edits_batch(
--     p_dataset_metrics_id BIGINT,
--     p_commit_datetime TIMESTAMP WITH TIME ZONE
-- ) 
-- AS '
-- DECLARE
--     cutoff_date TIMESTAMP WITH TIME ZONE;
--     edit_record RECORD;
--     same_count INTEGER;
--     is_pf_edit BOOLEAN;
-- BEGIN
--     cutoff_date := p_commit_datetime - INTERVAL ''90 days'';
    
--     -- Process edits one by one (simple but should work)
--     FOR edit_record IN 
--         SELECT id, edit_type, before, after
--         FROM edit 
--         WHERE dataset_metrics_id = p_dataset_metrics_id
--           AND commit_date_time = p_commit_datetime
--     LOOP
--         same_count := 0;
--         is_pf_edit := FALSE;
        
--         -- Count same edits based on edit type
--         IF edit_record.edit_type = ''CREATE'' THEN
--             SELECT COUNT(*) INTO same_count
--             FROM edit e 
--             WHERE (e.before = '''' OR e.before IS NULL)
--               AND e.after = edit_record.after
--               AND e.commit_date_time < p_commit_datetime
--               AND e.commit_date_time >= cutoff_date;
              
--             -- Check PF edit
--             SELECT EXISTS(
--                 SELECT 1 FROM edit e 
--                 WHERE (e.before = '''' OR e.before IS NULL)
--                   AND e.after = edit_record.after
--                   AND e.commit_date_time < p_commit_datetime
--                   AND e.commit_date_time >= cutoff_date
--                   AND e.dataset_metrics_id = p_dataset_metrics_id
--             ) INTO is_pf_edit;
            
--         ELSIF edit_record.edit_type = ''UPDATE'' THEN
--             SELECT COUNT(*) INTO same_count
--             FROM edit e 
--             WHERE e.before = edit_record.before
--               AND e.after = edit_record.after
--               AND e.commit_date_time < p_commit_datetime
--               AND e.commit_date_time >= cutoff_date;
              
--             -- Check PF edit
--             SELECT EXISTS(
--                 SELECT 1 FROM edit e 
--                 WHERE e.before = edit_record.before
--                   AND e.after = edit_record.after
--                   AND e.commit_date_time < p_commit_datetime
--                   AND e.commit_date_time >= cutoff_date
--                   AND e.dataset_metrics_id = p_dataset_metrics_id
--             ) INTO is_pf_edit;
            
--         ELSIF edit_record.edit_type = ''DELETE'' THEN
--             SELECT COUNT(*) INTO same_count
--             FROM edit e 
--             WHERE e.before = edit_record.before
--               AND (e.after = '''' OR e.after IS NULL)
--               AND e.commit_date_time < p_commit_datetime
--               AND e.commit_date_time >= cutoff_date;
              
--             -- Check PF edit
--             SELECT EXISTS(
--                 SELECT 1 FROM edit e 
--                 WHERE e.before = edit_record.before
--                   AND (e.after = '''' OR e.after IS NULL)
--                   AND e.commit_date_time < p_commit_datetime
--                   AND e.commit_date_time >= cutoff_date
--                   AND e.dataset_metrics_id = p_dataset_metrics_id
--             ) INTO is_pf_edit;
--         END IF;
        
--         -- Update the edit record
--         UPDATE edit 
--         SET 
--             is_same_edit = (same_count > 0),
--             same_edit_count = same_count,
--             is_pf_recommended_edit = is_pf_edit
--         WHERE id = edit_record.id;
--     END LOOP;
-- END;
-- '
-- LANGUAGE plpgsql;

-- 5. Main orchestration function (ORIGINAL with optimized same-edit detection)
CREATE OR REPLACE FUNCTION process_package_edits_and_statistics(
    p_dataset_metrics_id BIGINT,
    p_datasource_event_id BIGINT,
    p_current_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_historical_commit_datetime TIMESTAMP WITH TIME ZONE,
    p_datasource_purl VARCHAR,
    p_current_package_purls VARCHAR,
    p_current_package_families VARCHAR,
    p_historical_package_purls VARCHAR DEFAULT NULL,
    p_historical_package_families VARCHAR DEFAULT NULL,
    p_already_seen_current_commit BOOLEAN DEFAULT FALSE,
    p_historical_and_current_same BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    total_patches INTEGER,
    same_patches INTEGER,
    different_patches INTEGER,
    pf_patches INTEGER
)
AS '
BEGIN
    -- Step 1: Fast insert of package edits (without same-edit detection)
    CALL insert_package_edits_fast(
        p_dataset_metrics_id,
        p_datasource_event_id,
        p_current_commit_datetime,
        p_historical_commit_datetime,
        p_datasource_purl,
        p_current_package_purls,
        p_current_package_families,
        p_historical_package_purls,
        p_historical_package_families,
        p_already_seen_current_commit,
        p_historical_and_current_same
    );
    
    -- Step 2: OPTIMIZED batch update same-edit and PF detection
    CALL update_same_and_pf_edits_batch(p_dataset_metrics_id, p_current_commit_datetime);
    
    -- Step 3: Calculate and return the statistics
    RETURN QUERY
    SELECT * FROM calculate_edit_statistics(p_dataset_metrics_id, p_current_commit_datetime);
END;
'
LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION get_edit_pairs_for_cache(
    p_dataset_metrics_id BIGINT,
    p_commit_datetime TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
    before_value VARCHAR,
    after_value VARCHAR
)
LANGUAGE plpgsql
AS '
BEGIN
    RETURN QUERY
    SELECT DISTINCT e.before, e.after
    FROM edit e
    WHERE e.dataset_metrics_id = p_dataset_metrics_id 
      AND e.commit_date_time = p_commit_datetime;
END;
';





CREATE OR REPLACE FUNCTION get_dsm_ids_and_commit_datetimes(
  commit_datetime_in TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
  id BIGINT,
  commit_datetime TIMESTAMP WITH TIME ZONE
)
AS '
BEGIN
    RETURN QUERY
    SELECT dsm.id, dsm.commit_date_time FROM dataset_metrics dsm
    WHERE is_current
    AND dsm.commit_date_time < commit_datetime_in 
    ORDER BY dsm.commit_date_time ASC;
END;
'
LANGUAGE plpgsql;




-- Create necessary indexes for performance
CREATE OR REPLACE PROCEDURE create_edit_indexes()
AS '
BEGIN
  CREATE INDEX IF NOT EXISTS idx_edit_before_after_commit_time 
  ON edit (before, after, commit_date_time);

  CREATE INDEX IF NOT EXISTS idx_edit_dataset_metrics_commit_time 
  ON edit (dataset_metrics_id, commit_date_time);

  CREATE INDEX IF NOT EXISTS idx_edit_commit_date_time 
  ON edit (commit_date_time);
  
  CREATE INDEX IF NOT EXISTS idx_package_family_dataset_metrics_id
  ON package_family (dataset_metrics_id);
  
  RAISE NOTICE ''Edit indexes created successfully'';
END;
'
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION create_datasource_metrics_record(
    p_datasource_event_id BIGINT,
    p_previous_datasource_metrics_id BIGINT,
    p_current_dataset_metrics_id BIGINT,
    p_previous_dataset_metrics_id BIGINT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS '
DECLARE
    new_datasource_metrics_id BIGINT;
BEGIN
    -- If no previous datasource metrics, just use current values (first record scenario)
    IF p_previous_datasource_metrics_id IS NULL THEN
        INSERT INTO datasource_metrics (
            datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
            total_findings, critical_findings, high_findings, medium_findings, low_findings,
            findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year,
            high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year,
            low_findings_avoided_by_patching_past_year,
            findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days,
            high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days,
            low_findings_in_backlog_between_thirty_and_sixty_days,
            findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days,
            high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days,
            low_findings_in_backlog_between_sixty_and_ninety_days,
            findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days,
            high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days,
            low_findings_in_backlog_over_ninety_days,
            packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
            packages_with_medium_findings, packages_with_low_findings,
            downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
            stale_packages, stale_packages_six_months, stale_packages_one_year, stale_packages_one_year_six_months,
            stale_packages_two_years, patches, same_patches, different_patches, patch_fox_patches,
            patch_efficacy_score, patch_impact, patch_effort
        )
        SELECT 
            curr_dsm.datasource_event_count, de.commit_date_time, de.event_date_time, de.txid, de.job_id, d.purl,
            curr_dsm.total_findings, curr_dsm.critical_findings, curr_dsm.high_findings, curr_dsm.medium_findings, curr_dsm.low_findings,
            curr_dsm.findings_avoided_by_patching_past_year, curr_dsm.critical_findings_avoided_by_patching_past_year,
            curr_dsm.high_findings_avoided_by_patching_past_year, curr_dsm.medium_findings_avoided_by_patching_past_year,
            curr_dsm.low_findings_avoided_by_patching_past_year,
            curr_dsm.findings_in_backlog_between_thirty_and_sixty_days, curr_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days,
            curr_dsm.high_findings_in_backlog_between_thirty_and_sixty_days, curr_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days,
            curr_dsm.low_findings_in_backlog_between_thirty_and_sixty_days,
            curr_dsm.findings_in_backlog_between_sixty_and_ninety_days, curr_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days,
            curr_dsm.high_findings_in_backlog_between_sixty_and_ninety_days, curr_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days,
            curr_dsm.low_findings_in_backlog_between_sixty_and_ninety_days,
            curr_dsm.findings_in_backlog_over_ninety_days, curr_dsm.critical_findings_in_backlog_over_ninety_days,
            curr_dsm.high_findings_in_backlog_over_ninety_days, curr_dsm.medium_findings_in_backlog_over_ninety_days,
            curr_dsm.low_findings_in_backlog_over_ninety_days,
            curr_dsm.packages, curr_dsm.packages_with_findings, curr_dsm.packages_with_critical_findings, curr_dsm.packages_with_high_findings,
            curr_dsm.packages_with_medium_findings, curr_dsm.packages_with_low_findings,
            curr_dsm.downlevel_packages, curr_dsm.downlevel_packages_major, curr_dsm.downlevel_packages_minor, curr_dsm.downlevel_packages_patch,
            curr_dsm.stale_packages, curr_dsm.stale_packages_six_months, curr_dsm.stale_packages_one_year, curr_dsm.stale_packages_one_year_six_months,
            curr_dsm.stale_packages_two_years, curr_dsm.patches, curr_dsm.same_patches, curr_dsm.different_patches, curr_dsm.patch_fox_patches,
            curr_dsm.patch_efficacy_score, curr_dsm.patch_impact, curr_dsm.patch_effort
        FROM datasource_event de
        JOIN datasource d ON de.datasource_id = d.id
        JOIN dataset_metrics curr_dsm ON curr_dsm.id = p_current_dataset_metrics_id
        WHERE de.id = p_datasource_event_id
        RETURNING id INTO new_datasource_metrics_id;
    ELSE
        -- Delta calculation: just the difference between current and previous
        INSERT INTO datasource_metrics (
            datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
            total_findings, critical_findings, high_findings, medium_findings, low_findings,
            findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year,
            high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year,
            low_findings_avoided_by_patching_past_year,
            findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days,
            high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days,
            low_findings_in_backlog_between_thirty_and_sixty_days,
            findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days,
            high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days,
            low_findings_in_backlog_between_sixty_and_ninety_days,
            findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days,
            high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days,
            low_findings_in_backlog_over_ninety_days,
            packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
            packages_with_medium_findings, packages_with_low_findings,
            downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
            stale_packages, stale_packages_six_months, stale_packages_one_year, stale_packages_one_year_six_months,
            stale_packages_two_years, patches, same_patches, different_patches, patch_fox_patches,
            patch_efficacy_score, patch_impact, patch_effort
        )
        SELECT 
            (curr_dsm.datasource_event_count - prev_dsm.datasource_event_count),
            de.commit_date_time, de.event_date_time, de.txid, de.job_id, d.purl,
            (curr_dsm.total_findings - prev_dsm.total_findings),
            (curr_dsm.critical_findings - prev_dsm.critical_findings),
            (curr_dsm.high_findings - prev_dsm.high_findings),
            (curr_dsm.medium_findings - prev_dsm.medium_findings),
            (curr_dsm.low_findings - prev_dsm.low_findings),
            (curr_dsm.findings_avoided_by_patching_past_year - prev_dsm.findings_avoided_by_patching_past_year),
            (curr_dsm.critical_findings_avoided_by_patching_past_year - prev_dsm.critical_findings_avoided_by_patching_past_year),
            (curr_dsm.high_findings_avoided_by_patching_past_year - prev_dsm.high_findings_avoided_by_patching_past_year),
            (curr_dsm.medium_findings_avoided_by_patching_past_year - prev_dsm.medium_findings_avoided_by_patching_past_year),
            (curr_dsm.low_findings_avoided_by_patching_past_year - prev_dsm.low_findings_avoided_by_patching_past_year),
            (curr_dsm.findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.findings_in_backlog_between_thirty_and_sixty_days),
            (curr_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days),
            (curr_dsm.high_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.high_findings_in_backlog_between_thirty_and_sixty_days),
            (curr_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days),
            (curr_dsm.low_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.low_findings_in_backlog_between_thirty_and_sixty_days),
            (curr_dsm.findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.findings_in_backlog_between_sixty_and_ninety_days),
            (curr_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days),
            (curr_dsm.high_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.high_findings_in_backlog_between_sixty_and_ninety_days),
            (curr_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days),
            (curr_dsm.low_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.low_findings_in_backlog_between_sixty_and_ninety_days),
            (curr_dsm.findings_in_backlog_over_ninety_days - prev_dsm.findings_in_backlog_over_ninety_days),
            (curr_dsm.critical_findings_in_backlog_over_ninety_days - prev_dsm.critical_findings_in_backlog_over_ninety_days),
            (curr_dsm.high_findings_in_backlog_over_ninety_days - prev_dsm.high_findings_in_backlog_over_ninety_days),
            (curr_dsm.medium_findings_in_backlog_over_ninety_days - prev_dsm.medium_findings_in_backlog_over_ninety_days),
            (curr_dsm.low_findings_in_backlog_over_ninety_days - prev_dsm.low_findings_in_backlog_over_ninety_days),
            (curr_dsm.packages - prev_dsm.packages),
            (curr_dsm.packages_with_findings - prev_dsm.packages_with_findings),
            (curr_dsm.packages_with_critical_findings - prev_dsm.packages_with_critical_findings),
            (curr_dsm.packages_with_high_findings - prev_dsm.packages_with_high_findings),
            (curr_dsm.packages_with_medium_findings - prev_dsm.packages_with_medium_findings),
            (curr_dsm.packages_with_low_findings - prev_dsm.packages_with_low_findings),
            (curr_dsm.downlevel_packages - prev_dsm.downlevel_packages),
            (curr_dsm.downlevel_packages_major - prev_dsm.downlevel_packages_major),
            (curr_dsm.downlevel_packages_minor - prev_dsm.downlevel_packages_minor),
            (curr_dsm.downlevel_packages_patch - prev_dsm.downlevel_packages_patch),
            (curr_dsm.stale_packages - prev_dsm.stale_packages),
            (curr_dsm.stale_packages_six_months - prev_dsm.stale_packages_six_months),
            (curr_dsm.stale_packages_one_year - prev_dsm.stale_packages_one_year),
            (curr_dsm.stale_packages_one_year_six_months - prev_dsm.stale_packages_one_year_six_months),
            (curr_dsm.stale_packages_two_years - prev_dsm.stale_packages_two_years),
            (curr_dsm.patches - prev_dsm.patches),
            (curr_dsm.same_patches - prev_dsm.same_patches),
            (curr_dsm.different_patches - prev_dsm.different_patches),
            (curr_dsm.patch_fox_patches - prev_dsm.patch_fox_patches),
            (curr_dsm.patch_efficacy_score - prev_dsm.patch_efficacy_score),
            (curr_dsm.patch_impact - prev_dsm.patch_impact),
            (curr_dsm.patch_effort - prev_dsm.patch_effort)
        FROM datasource_event de
        JOIN datasource d ON de.datasource_id = d.id
        JOIN dataset_metrics curr_dsm ON curr_dsm.id = p_current_dataset_metrics_id
        JOIN dataset_metrics prev_dsm ON prev_dsm.id = p_previous_dataset_metrics_id
        WHERE de.id = p_datasource_event_id
        RETURNING id INTO new_datasource_metrics_id;
    END IF;
    
    RETURN new_datasource_metrics_id;
END;
';



--
-- not computing patches, same_patches, different_patches,_patchfox patches correctly 
--
--
--
--
--
-- CREATE OR REPLACE FUNCTION create_datasource_metrics_record(
--     p_datasource_event_id BIGINT,
--     p_previous_datasource_metrics_id BIGINT,
--     p_current_dataset_metrics_id BIGINT,
--     p_previous_dataset_metrics_id BIGINT
-- )
-- RETURNS BIGINT
-- LANGUAGE plpgsql
-- AS '
-- DECLARE
--     new_datasource_metrics_id BIGINT;
-- BEGIN
--     -- If no previous dataset metrics, just use current values (first record scenario)
--     IF p_previous_dataset_metrics_id IS NULL THEN
--         INSERT INTO datasource_metrics (
--             datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--             total_findings, critical_findings, high_findings, medium_findings, low_findings,
--             findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year,
--             high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year,
--             low_findings_avoided_by_patching_past_year,
--             findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days,
--             high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days,
--             low_findings_in_backlog_between_thirty_and_sixty_days,
--             findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days,
--             high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days,
--             low_findings_in_backlog_between_sixty_and_ninety_days,
--             findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days,
--             high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days,
--             low_findings_in_backlog_over_ninety_days,
--             packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--             packages_with_medium_findings, packages_with_low_findings,
--             downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--             stale_packages, stale_packages_six_months, stale_packages_one_year, stale_packages_one_year_six_months,
--             stale_packages_two_years, patches, same_patches, different_patches, patch_fox_patches,
--             patch_efficacy_score, patch_impact, patch_effort
--         )
--         SELECT 
--             curr_dsm.datasource_event_count, de.commit_date_time, de.event_date_time, de.txid, de.job_id, d.purl,
--             curr_dsm.total_findings, curr_dsm.critical_findings, curr_dsm.high_findings, curr_dsm.medium_findings, curr_dsm.low_findings,
--             curr_dsm.findings_avoided_by_patching_past_year, curr_dsm.critical_findings_avoided_by_patching_past_year,
--             curr_dsm.high_findings_avoided_by_patching_past_year, curr_dsm.medium_findings_avoided_by_patching_past_year,
--             curr_dsm.low_findings_avoided_by_patching_past_year,
--             curr_dsm.findings_in_backlog_between_thirty_and_sixty_days, curr_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days,
--             curr_dsm.high_findings_in_backlog_between_thirty_and_sixty_days, curr_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days,
--             curr_dsm.low_findings_in_backlog_between_thirty_and_sixty_days,
--             curr_dsm.findings_in_backlog_between_sixty_and_ninety_days, curr_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days,
--             curr_dsm.high_findings_in_backlog_between_sixty_and_ninety_days, curr_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days,
--             curr_dsm.low_findings_in_backlog_between_sixty_and_ninety_days,
--             curr_dsm.findings_in_backlog_over_ninety_days, curr_dsm.critical_findings_in_backlog_over_ninety_days,
--             curr_dsm.high_findings_in_backlog_over_ninety_days, curr_dsm.medium_findings_in_backlog_over_ninety_days,
--             curr_dsm.low_findings_in_backlog_over_ninety_days,
--             curr_dsm.packages, curr_dsm.packages_with_findings, curr_dsm.packages_with_critical_findings, curr_dsm.packages_with_high_findings,
--             curr_dsm.packages_with_medium_findings, curr_dsm.packages_with_low_findings,
--             curr_dsm.downlevel_packages, curr_dsm.downlevel_packages_major, curr_dsm.downlevel_packages_minor, curr_dsm.downlevel_packages_patch,
--             curr_dsm.stale_packages, curr_dsm.stale_packages_six_months, curr_dsm.stale_packages_one_year, curr_dsm.stale_packages_one_year_six_months,
--             curr_dsm.stale_packages_two_years, curr_dsm.patches, curr_dsm.same_patches, curr_dsm.different_patches, curr_dsm.patch_fox_patches,
--             curr_dsm.patch_efficacy_score, curr_dsm.patch_impact, curr_dsm.patch_effort
--         FROM datasource_event de
--         JOIN datasource d ON de.datasource_id = d.id
--         JOIN dataset_metrics curr_dsm ON curr_dsm.id = p_current_dataset_metrics_id
--         WHERE de.id = p_datasource_event_id
--         RETURNING id INTO new_datasource_metrics_id;
--     ELSE
--         -- Delta calculation: baseline + (current - previous)
--         INSERT INTO datasource_metrics (
--             datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--             total_findings, critical_findings, high_findings, medium_findings, low_findings,
--             findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year,
--             high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year,
--             low_findings_avoided_by_patching_past_year,
--             findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days,
--             high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days,
--             low_findings_in_backlog_between_thirty_and_sixty_days,
--             findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days,
--             high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days,
--             low_findings_in_backlog_between_sixty_and_ninety_days,
--             findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days,
--             high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days,
--             low_findings_in_backlog_over_ninety_days,
--             packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--             packages_with_medium_findings, packages_with_low_findings,
--             downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--             stale_packages, stale_packages_six_months, stale_packages_one_year, stale_packages_one_year_six_months,
--             stale_packages_two_years, patches, same_patches, different_patches, patch_fox_patches,
--             patch_efficacy_score, patch_impact, patch_effort
--         )
--         SELECT 
--             curr_dsm.datasource_event_count, de.commit_date_time, de.event_date_time, de.txid, de.job_id, d.purl,
--             COALESCE(prev_dsource.total_findings, 0) + (curr_dsm.total_findings - prev_dsm.total_findings),
--             COALESCE(prev_dsource.critical_findings, 0) + (curr_dsm.critical_findings - prev_dsm.critical_findings),
--             COALESCE(prev_dsource.high_findings, 0) + (curr_dsm.high_findings - prev_dsm.high_findings),
--             COALESCE(prev_dsource.medium_findings, 0) + (curr_dsm.medium_findings - prev_dsm.medium_findings),
--             COALESCE(prev_dsource.low_findings, 0) + (curr_dsm.low_findings - prev_dsm.low_findings),
--             COALESCE(prev_dsource.findings_avoided_by_patching_past_year, 0) + (curr_dsm.findings_avoided_by_patching_past_year - prev_dsm.findings_avoided_by_patching_past_year),
--             COALESCE(prev_dsource.critical_findings_avoided_by_patching_past_year, 0) + (curr_dsm.critical_findings_avoided_by_patching_past_year - prev_dsm.critical_findings_avoided_by_patching_past_year),
--             COALESCE(prev_dsource.high_findings_avoided_by_patching_past_year, 0) + (curr_dsm.high_findings_avoided_by_patching_past_year - prev_dsm.high_findings_avoided_by_patching_past_year),
--             COALESCE(prev_dsource.medium_findings_avoided_by_patching_past_year, 0) + (curr_dsm.medium_findings_avoided_by_patching_past_year - prev_dsm.medium_findings_avoided_by_patching_past_year),
--             COALESCE(prev_dsource.low_findings_avoided_by_patching_past_year, 0) + (curr_dsm.low_findings_avoided_by_patching_past_year - prev_dsm.low_findings_avoided_by_patching_past_year),
--             COALESCE(prev_dsource.findings_in_backlog_between_thirty_and_sixty_days, 0) + (curr_dsm.findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.findings_in_backlog_between_thirty_and_sixty_days),
--             COALESCE(prev_dsource.critical_findings_in_backlog_between_thirty_and_sixty_days, 0) + (curr_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.critical_findings_in_backlog_between_thirty_and_sixty_days),
--             COALESCE(prev_dsource.high_findings_in_backlog_between_thirty_and_sixty_days, 0) + (curr_dsm.high_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.high_findings_in_backlog_between_thirty_and_sixty_days),
--             COALESCE(prev_dsource.medium_findings_in_backlog_between_thirty_and_sixty_days, 0) + (curr_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.medium_findings_in_backlog_between_thirty_and_sixty_days),
--             COALESCE(prev_dsource.low_findings_in_backlog_between_thirty_and_sixty_days, 0) + (curr_dsm.low_findings_in_backlog_between_thirty_and_sixty_days - prev_dsm.low_findings_in_backlog_between_thirty_and_sixty_days),
--             COALESCE(prev_dsource.findings_in_backlog_between_sixty_and_ninety_days, 0) + (curr_dsm.findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.findings_in_backlog_between_sixty_and_ninety_days),
--             COALESCE(prev_dsource.critical_findings_in_backlog_between_sixty_and_ninety_days, 0) + (curr_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.critical_findings_in_backlog_between_sixty_and_ninety_days),
--             COALESCE(prev_dsource.high_findings_in_backlog_between_sixty_and_ninety_days, 0) + (curr_dsm.high_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.high_findings_in_backlog_between_sixty_and_ninety_days),
--             COALESCE(prev_dsource.medium_findings_in_backlog_between_sixty_and_ninety_days, 0) + (curr_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.medium_findings_in_backlog_between_sixty_and_ninety_days),
--             COALESCE(prev_dsource.low_findings_in_backlog_between_sixty_and_ninety_days, 0) + (curr_dsm.low_findings_in_backlog_between_sixty_and_ninety_days - prev_dsm.low_findings_in_backlog_between_sixty_and_ninety_days),
--             COALESCE(prev_dsource.findings_in_backlog_over_ninety_days, 0) + (curr_dsm.findings_in_backlog_over_ninety_days - prev_dsm.findings_in_backlog_over_ninety_days),
--             COALESCE(prev_dsource.critical_findings_in_backlog_over_ninety_days, 0) + (curr_dsm.critical_findings_in_backlog_over_ninety_days - prev_dsm.critical_findings_in_backlog_over_ninety_days),
--             COALESCE(prev_dsource.high_findings_in_backlog_over_ninety_days, 0) + (curr_dsm.high_findings_in_backlog_over_ninety_days - prev_dsm.high_findings_in_backlog_over_ninety_days),
--             COALESCE(prev_dsource.medium_findings_in_backlog_over_ninety_days, 0) + (curr_dsm.medium_findings_in_backlog_over_ninety_days - prev_dsm.medium_findings_in_backlog_over_ninety_days),
--             COALESCE(prev_dsource.low_findings_in_backlog_over_ninety_days, 0) + (curr_dsm.low_findings_in_backlog_over_ninety_days - prev_dsm.low_findings_in_backlog_over_ninety_days),
--             COALESCE(prev_dsource.packages, 0) + (curr_dsm.packages - prev_dsm.packages),
--             COALESCE(prev_dsource.packages_with_findings, 0) + (curr_dsm.packages_with_findings - prev_dsm.packages_with_findings),
--             COALESCE(prev_dsource.packages_with_critical_findings, 0) + (curr_dsm.packages_with_critical_findings - prev_dsm.packages_with_critical_findings),
--             COALESCE(prev_dsource.packages_with_high_findings, 0) + (curr_dsm.packages_with_high_findings - prev_dsm.packages_with_high_findings),
--             COALESCE(prev_dsource.packages_with_medium_findings, 0) + (curr_dsm.packages_with_medium_findings - prev_dsm.packages_with_medium_findings),
--             COALESCE(prev_dsource.packages_with_low_findings, 0) + (curr_dsm.packages_with_low_findings - prev_dsm.packages_with_low_findings),
--             COALESCE(prev_dsource.downlevel_packages, 0) + (curr_dsm.downlevel_packages - prev_dsm.downlevel_packages),
--             COALESCE(prev_dsource.downlevel_packages_major, 0) + (curr_dsm.downlevel_packages_major - prev_dsm.downlevel_packages_major),
--             COALESCE(prev_dsource.downlevel_packages_minor, 0) + (curr_dsm.downlevel_packages_minor - prev_dsm.downlevel_packages_minor),
--             COALESCE(prev_dsource.downlevel_packages_patch, 0) + (curr_dsm.downlevel_packages_patch - prev_dsm.downlevel_packages_patch),
--             COALESCE(prev_dsource.stale_packages, 0) + (curr_dsm.stale_packages - prev_dsm.stale_packages),
--             COALESCE(prev_dsource.stale_packages_six_months, 0) + (curr_dsm.stale_packages_six_months - prev_dsm.stale_packages_six_months),
--             COALESCE(prev_dsource.stale_packages_one_year, 0) + (curr_dsm.stale_packages_one_year - prev_dsm.stale_packages_one_year),
--             COALESCE(prev_dsource.stale_packages_one_year_six_months, 0) + (curr_dsm.stale_packages_one_year_six_months - prev_dsm.stale_packages_one_year_six_months),
--             COALESCE(prev_dsource.stale_packages_two_years, 0) + (curr_dsm.stale_packages_two_years - prev_dsm.stale_packages_two_years),
--             COALESCE(prev_dsource.patches, 0) + (curr_dsm.patches),
--             COALESCE(prev_dsource.same_patches, 0) + (curr_dsm.same_patches),
--             COALESCE(prev_dsource.different_patches, 0) + (curr_dsm.different_patches),
--             COALESCE(prev_dsource.patch_fox_patches, 0) + (curr_dsm.patch_fox_patches),
--             COALESCE(prev_dsource.patch_efficacy_score, 0) + (curr_dsm.patch_efficacy_score - prev_dsm.patch_efficacy_score),
--             COALESCE(prev_dsource.patch_impact, 0) + (curr_dsm.patch_impact - prev_dsm.patch_impact),
--             COALESCE(prev_dsource.patch_effort, 0) + (curr_dsm.patch_effort - prev_dsm.patch_effort)
--         FROM datasource_event de
--         JOIN datasource d ON de.datasource_id = d.id
--         JOIN dataset_metrics curr_dsm ON curr_dsm.id = p_current_dataset_metrics_id
--         JOIN dataset_metrics prev_dsm ON prev_dsm.id = p_previous_dataset_metrics_id
--         LEFT JOIN datasource_metrics prev_dsource ON prev_dsource.id = p_previous_datasource_metrics_id
--         WHERE de.id = p_datasource_event_id
--         RETURNING id INTO new_datasource_metrics_id;
--     END IF;
    
--     RETURN new_datasource_metrics_id;
-- END;
-- ';






CREATE OR REPLACE FUNCTION update_datasource_metrics_current(
    p_datasource_metrics_id BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS '
BEGIN
    -- Use PostgreSQLs ON CONFLICT to handle insert or update in one statement
    INSERT INTO datasource_metrics_current (
        datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
        total_findings, critical_findings, high_findings, medium_findings, low_findings,
        packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
        packages_with_medium_findings, packages_with_low_findings,
        downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
        stale_packages, patches, same_patches, different_patches, patch_fox_patches
    )
    SELECT 
        datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
        total_findings, critical_findings, high_findings, medium_findings, low_findings,
        packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
        packages_with_medium_findings, packages_with_low_findings,
        downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
        stale_packages, patches, same_patches, different_patches, patch_fox_patches
    FROM datasource_metrics dm
    WHERE dm.id = p_datasource_metrics_id
    ON CONFLICT (purl) DO UPDATE SET
        commit_date_time = EXCLUDED.commit_date_time,
        event_date_time = EXCLUDED.event_date_time,
        txid = EXCLUDED.txid,
        job_id = EXCLUDED.job_id,
        -- Accumulate deltas to maintain cumulative totals for findings and packages
        datasource_event_count = GREATEST(0, datasource_metrics_current.datasource_event_count + EXCLUDED.datasource_event_count),
        total_findings = GREATEST(0, datasource_metrics_current.total_findings + EXCLUDED.total_findings),
        critical_findings = GREATEST(0, datasource_metrics_current.critical_findings + EXCLUDED.critical_findings),
        high_findings = GREATEST(0, datasource_metrics_current.high_findings + EXCLUDED.high_findings),
        medium_findings = GREATEST(0, datasource_metrics_current.medium_findings + EXCLUDED.medium_findings),
        low_findings = GREATEST(0, datasource_metrics_current.low_findings + EXCLUDED.low_findings),
        packages = GREATEST(0, datasource_metrics_current.packages + EXCLUDED.packages),
        packages_with_findings = GREATEST(0, datasource_metrics_current.packages_with_findings + EXCLUDED.packages_with_findings),
        packages_with_critical_findings = GREATEST(0, datasource_metrics_current.packages_with_critical_findings + EXCLUDED.packages_with_critical_findings),
        packages_with_high_findings = GREATEST(0, datasource_metrics_current.packages_with_high_findings + EXCLUDED.packages_with_high_findings),
        packages_with_medium_findings = GREATEST(0, datasource_metrics_current.packages_with_medium_findings + EXCLUDED.packages_with_medium_findings),
        packages_with_low_findings = GREATEST(0, datasource_metrics_current.packages_with_low_findings + EXCLUDED.packages_with_low_findings),
        downlevel_packages = GREATEST(0, datasource_metrics_current.downlevel_packages + EXCLUDED.downlevel_packages),
        downlevel_packages_major = GREATEST(0, datasource_metrics_current.downlevel_packages_major + EXCLUDED.downlevel_packages_major),
        downlevel_packages_minor = GREATEST(0, datasource_metrics_current.downlevel_packages_minor + EXCLUDED.downlevel_packages_minor),
        downlevel_packages_patch = GREATEST(0, datasource_metrics_current.downlevel_packages_patch + EXCLUDED.downlevel_packages_patch),
        stale_packages = GREATEST(0, datasource_metrics_current.stale_packages + EXCLUDED.stale_packages),
        -- Set patches values to -1 (not applicable at datasource level)
        patches = -1,
        same_patches = -1,
        different_patches = -1,
        patch_fox_patches = -1;
END;
';






-- CREATE OR REPLACE FUNCTION update_datasource_metrics_current(
--     p_datasource_metrics_id BIGINT
-- )
-- RETURNS VOID
-- LANGUAGE plpgsql
-- AS '
-- BEGIN
--     -- Use PostgreSQL''s ON CONFLICT to handle insert or update in one statement
--     INSERT INTO datasource_metrics_current (
--         datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--         total_findings, critical_findings, high_findings, medium_findings, low_findings,
--         packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--         packages_with_medium_findings, packages_with_low_findings,
--         downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--         stale_packages, patches, same_patches, different_patches, patch_fox_patches
--     )
--     SELECT 
--         datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--         total_findings, critical_findings, high_findings, medium_findings, low_findings,
--         packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--         packages_with_medium_findings, packages_with_low_findings,
--         downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--         stale_packages, patches, same_patches, different_patches, patch_fox_patches
--     FROM datasource_metrics dm
--     WHERE dm.id = p_datasource_metrics_id
--     ON CONFLICT (purl) DO UPDATE SET
--         datasource_event_count = EXCLUDED.datasource_event_count,
--         commit_date_time = EXCLUDED.commit_date_time,
--         event_date_time = EXCLUDED.event_date_time,
--         txid = EXCLUDED.txid,
--         job_id = EXCLUDED.job_id,
--         -- Accumulate deltas to maintain cumulative totals
--         total_findings = GREATEST(0, datasource_metrics_current.total_findings + EXCLUDED.total_findings),
--         critical_findings = GREATEST(0, datasource_metrics_current.critical_findings + EXCLUDED.critical_findings),
--         high_findings = GREATEST(0, datasource_metrics_current.high_findings + EXCLUDED.high_findings),
--         medium_findings = GREATEST(0, datasource_metrics_current.medium_findings + EXCLUDED.medium_findings),
--         low_findings = GREATEST(0, datasource_metrics_current.low_findings + EXCLUDED.low_findings),
--         packages = GREATEST(0, datasource_metrics_current.packages + EXCLUDED.packages),
--         packages_with_findings = GREATEST(0, datasource_metrics_current.packages_with_findings + EXCLUDED.packages_with_findings),
--         packages_with_critical_findings = GREATEST(0, datasource_metrics_current.packages_with_critical_findings + EXCLUDED.packages_with_critical_findings),
--         packages_with_high_findings = GREATEST(0, datasource_metrics_current.packages_with_high_findings + EXCLUDED.packages_with_high_findings),
--         packages_with_medium_findings = GREATEST(0, datasource_metrics_current.packages_with_medium_findings + EXCLUDED.packages_with_medium_findings),
--         packages_with_low_findings = GREATEST(0, datasource_metrics_current.packages_with_low_findings + EXCLUDED.packages_with_low_findings),
--         downlevel_packages = GREATEST(0, datasource_metrics_current.downlevel_packages + EXCLUDED.downlevel_packages),
--         downlevel_packages_major = GREATEST(0, datasource_metrics_current.downlevel_packages_major + EXCLUDED.downlevel_packages_major),
--         downlevel_packages_minor = GREATEST(0, datasource_metrics_current.downlevel_packages_minor + EXCLUDED.downlevel_packages_minor),
--         downlevel_packages_patch = GREATEST(0, datasource_metrics_current.downlevel_packages_patch + EXCLUDED.downlevel_packages_patch),
--         stale_packages = GREATEST(0, datasource_metrics_current.stale_packages + EXCLUDED.stale_packages),
--         patches = GREATEST(0, datasource_metrics_current.patches + EXCLUDED.patches),
--         same_patches = GREATEST(0, datasource_metrics_current.same_patches + EXCLUDED.same_patches),
--         different_patches = GREATEST(0, datasource_metrics_current.different_patches + EXCLUDED.different_patches),
--         patch_fox_patches = GREATEST(0, datasource_metrics_current.patch_fox_patches + EXCLUDED.patch_fox_patches);
-- END;
-- ';


-- 
--
-- below accumulates values when it sould be replacing them 
--

-- CREATE OR REPLACE FUNCTION update_datasource_metrics_current(
--     p_datasource_metrics_id BIGINT
-- )
-- RETURNS VOID
-- LANGUAGE plpgsql
-- AS '
-- BEGIN
--     -- Use PostgreSQL''s ON CONFLICT to handle insert or update in one statement
--     INSERT INTO datasource_metrics_current (
--         datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--         total_findings, critical_findings, high_findings, medium_findings, low_findings,
--         packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--         packages_with_medium_findings, packages_with_low_findings,
--         downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--         stale_packages, patches, same_patches, different_patches, patch_fox_patches
--     )
--     SELECT 
--         datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
--         total_findings, critical_findings, high_findings, medium_findings, low_findings,
--         packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
--         packages_with_medium_findings, packages_with_low_findings,
--         downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
--         stale_packages, patches, same_patches, different_patches, patch_fox_patches
--     FROM datasource_metrics dm
--     WHERE dm.id = p_datasource_metrics_id
--     ON CONFLICT (purl) DO UPDATE SET
--         datasource_event_count = EXCLUDED.datasource_event_count,
--         commit_date_time = EXCLUDED.commit_date_time,
--         event_date_time = EXCLUDED.event_date_time,
--         txid = EXCLUDED.txid,
--         job_id = EXCLUDED.job_id,
--         total_findings = GREATEST(0, datasource_metrics_current.total_findings + EXCLUDED.total_findings),
--         critical_findings = GREATEST(0, datasource_metrics_current.critical_findings + EXCLUDED.critical_findings),
--         high_findings = GREATEST(0, datasource_metrics_current.high_findings + EXCLUDED.high_findings),
--         medium_findings = GREATEST(0, datasource_metrics_current.medium_findings + EXCLUDED.medium_findings),
--         low_findings = GREATEST(0, datasource_metrics_current.low_findings + EXCLUDED.low_findings),
--         packages = GREATEST(0, datasource_metrics_current.packages + EXCLUDED.packages),
--         packages_with_findings = GREATEST(0, datasource_metrics_current.packages_with_findings + EXCLUDED.packages_with_findings),
--         packages_with_critical_findings = GREATEST(0, datasource_metrics_current.packages_with_critical_findings + EXCLUDED.packages_with_critical_findings),
--         packages_with_high_findings = GREATEST(0, datasource_metrics_current.packages_with_high_findings + EXCLUDED.packages_with_high_findings),
--         packages_with_medium_findings = GREATEST(0, datasource_metrics_current.packages_with_medium_findings + EXCLUDED.packages_with_medium_findings),
--         packages_with_low_findings = GREATEST(0, datasource_metrics_current.packages_with_low_findings + EXCLUDED.packages_with_low_findings),
--         downlevel_packages = GREATEST(0, datasource_metrics_current.downlevel_packages + EXCLUDED.downlevel_packages),
--         downlevel_packages_major = GREATEST(0, datasource_metrics_current.downlevel_packages_major + EXCLUDED.downlevel_packages_major),
--         downlevel_packages_minor = GREATEST(0, datasource_metrics_current.downlevel_packages_minor + EXCLUDED.downlevel_packages_minor),
--         downlevel_packages_patch = GREATEST(0, datasource_metrics_current.downlevel_packages_patch + EXCLUDED.downlevel_packages_patch),
--         stale_packages = GREATEST(0, datasource_metrics_current.stale_packages + EXCLUDED.stale_packages),
--         patches = GREATEST(0, datasource_metrics_current.patches + EXCLUDED.patches),
--         same_patches = GREATEST(0, datasource_metrics_current.same_patches + EXCLUDED.same_patches),
--         different_patches = GREATEST(0, datasource_metrics_current.different_patches + EXCLUDED.different_patches),
--         patch_fox_patches = GREATEST(0, datasource_metrics_current.patch_fox_patches + EXCLUDED.patch_fox_patches);
-- END;
-- ';


CREATE OR REPLACE FUNCTION update_dataset_metrics_patches(
  dataset_metrics_id BIGINT,
  p_patches BIGINT,
  p_same_patches BIGINT,
  p_different_patches BIGINT,
  p_patch_fox_patches BIGINT
) RETURNS void
LANGUAGE plpgsql
AS '
BEGIN
  UPDATE dataset_metrics 
  SET
    patches = p_patches,
    same_patches = p_same_patches,
    different_patches = p_different_patches,
    patch_fox_patches = p_patch_fox_patches
  WHERE id = dataset_metrics_id;
END;
';


