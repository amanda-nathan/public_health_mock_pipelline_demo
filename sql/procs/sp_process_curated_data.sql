USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE PROCEDURE sp_process_curated_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  health_row_count INTEGER;
  env_row_count INTEGER;
  error_msg STRING := '';
  proc_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  result_msg STRING := '';
BEGIN
  
  -- ✅ Fixed: Ensure proper schema context within procedure
  USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
  USE SCHEMA CURATED;
  
  -- Log procedure start
  INSERT INTO logging.pipeline_execution_log 
    (procedure_name, execution_start, execution_status, user_name, warehouse_name)
  VALUES 
    ('sp_process_curated_data', :proc_start, 'RUNNING', CURRENT_USER(), CURRENT_WAREHOUSE());
  
  -- Process health indicators data
  MERGE INTO curated_health_indicators AS target
  USING (
    SELECT 
      CONCAT(state_abbr, '_', locationid, '_', measureid) as location_key,
      state_abbr,
      county_name,
      category as measure_category,
      measure as measure_name,
      data_value as measure_value,
      population,
      latitude,
      longitude,
      year as data_year,
      datasource as data_source,
      CASE 
        WHEN data_value IS NULL THEN 'MISSING'
        WHEN data_value < 0 THEN 'INVALID'
        ELSE 'VALID'
      END as data_quality_flag
    FROM landing_raw.raw_cdc_places_data
  ) AS source
  ON target.location_key = source.location_key
  WHEN MATCHED THEN UPDATE SET
    measure_value = source.measure_value,
    population = source.population,
    data_quality_flag = source.data_quality_flag,
    load_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    location_key, state_abbr, county_name, measure_category, measure_name,
    measure_value, population, latitude, longitude, data_year, data_source, data_quality_flag
  ) VALUES (
    source.location_key, source.state_abbr, source.county_name, source.measure_category, 
    source.measure_name, source.measure_value, source.population, source.latitude, 
    source.longitude, source.data_year, source.data_source, source.data_quality_flag
  );
  
  health_row_count := SQLROWCOUNT;
  
  -- Process environmental data
  MERGE INTO curated_environmental_data AS target
  USING (
    SELECT 
      location_id as location_key,
      county,
      air_quality_index,
      pm25_concentration,
      lead_exposure_risk as lead_risk_level,
      water_quality_score,
      environmental_justice_score,
      vulnerable_population_pct,
      facility_name,
      facility_address,
      last_inspection_date,
      compliance_status,
      year as data_year,
      CASE 
        WHEN air_quality_index IS NULL OR water_quality_score IS NULL THEN 'MISSING'
        WHEN air_quality_index < 0 OR water_quality_score < 0 THEN 'INVALID'
        ELSE 'VALID'
      END as data_quality_flag
    FROM landing_raw.raw_environmental_health_data
  ) AS source
  ON target.location_key = source.location_key
  WHEN MATCHED THEN UPDATE SET
    air_quality_index = source.air_quality_index,
    pm25_concentration = source.pm25_concentration,
    water_quality_score = source.water_quality_score,
    environmental_justice_score = source.environmental_justice_score,
    vulnerable_population_pct = source.vulnerable_population_pct,
    compliance_status = source.compliance_status,
    data_quality_flag = source.data_quality_flag,
    load_timestamp = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    location_key, county, air_quality_index, pm25_concentration, lead_risk_level,
    water_quality_score, environmental_justice_score, vulnerable_population_pct,
    facility_name, facility_address, last_inspection_date, compliance_status, 
    data_year, data_quality_flag
  ) VALUES (
    source.location_key, source.county, source.air_quality_index, source.pm25_concentration,
    source.lead_risk_level, source.water_quality_score, source.environmental_justice_score,
    source.vulnerable_population_pct, source.facility_name, source.facility_address,
    source.last_inspection_date, source.compliance_status, source.data_year, source.data_quality_flag
  );
  
  env_row_count := SQLROWCOUNT;
  
  result_msg := 'Successfully processed ' || :health_row_count || ' health indicator records and ' || :env_row_count || ' environmental health records into curated layer';
  
  -- Update execution log with success
  UPDATE logging.pipeline_execution_log 
  SET 
    execution_end = CURRENT_TIMESTAMP(),
    execution_status = 'SUCCESS',
    rows_processed = :health_row_count + :env_row_count,
    details = :result_msg
  WHERE procedure_name = 'sp_process_curated_data' 
    AND execution_start = :proc_start;
  
  -- Log data quality metrics
  INSERT INTO logging.data_quality_log 
    (table_name, quality_check_name, check_result, check_value, threshold_value, details)
  VALUES 
    ('curated_health_indicators', 'merge_operation_count', 'PASS', :health_row_count, 0, 'Health indicator records processed successfully'),
    ('curated_environmental_data', 'merge_operation_count', 'PASS', :env_row_count, 0, 'Environmental health records processed successfully');
  
  RETURN result_msg;
  
EXCEPTION
  WHEN OTHER THEN
    error_msg := SQLERRM;
    
    -- Update execution log with failure
    UPDATE logging.pipeline_execution_log 
    SET 
      execution_end = CURRENT_TIMESTAMP(),
      execution_status = 'FAILED',
      error_message = :error_msg
    WHERE procedure_name = 'sp_process_curated_data' 
      AND execution_start = :proc_start;
      
    -- Log the error for monitoring
    INSERT INTO logging.data_quality_log 
      (table_name, quality_check_name, check_result, check_value, details)
    VALUES 
      ('sp_process_curated_data', 'procedure_execution', 'FAIL', 0, 'Procedure failed: ' || :error_msg);
    
    RETURN 'ERROR: Curated data processing failed - ' || :error_msg;
END;
$$;