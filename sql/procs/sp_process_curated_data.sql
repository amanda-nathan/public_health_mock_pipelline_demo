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
  
  USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
  USE SCHEMA CURATED;
  
  INSERT INTO logging.pipeline_execution_log 
    (procedure_name, execution_start, execution_status, user_name, warehouse_name)
  VALUES 
    ('sp_process_curated_data', :proc_start, 'RUNNING', CURRENT_USER(), CURRENT_WAREHOUSE());
  
  TRUNCATE TABLE curated_health_indicators;
  
  INSERT INTO curated_health_indicators (
    location_key, state_abbr, county_name, measure_category, measure_name, 
    measure_value, population, latitude, longitude, data_year, data_source, data_quality_flag
  )
  SELECT 
    locationid as location_key,
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
      WHEN data_value IS NOT NULL AND data_value >= 0 THEN 'VALID'
      WHEN data_value IS NULL THEN 'NULL_VALUE'
      ELSE 'INVALID'
    END as data_quality_flag
  FROM landing_raw.raw_cdc_places_data
  WHERE county_name IS NOT NULL;
  
  health_row_count := SQLROWCOUNT;
  
  TRUNCATE TABLE curated_environmental_data;
  
  INSERT INTO curated_environmental_data (
    location_key, county, air_quality_index, pm25_concentration, lead_risk_level,
    water_quality_score, environmental_justice_score, vulnerable_population_pct,
    facility_name, facility_address, last_inspection_date, compliance_status,
    data_year, data_quality_flag
  )
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
      WHEN air_quality_index IS NOT NULL AND air_quality_index > 0 THEN 'VALID'
      WHEN air_quality_index IS NULL THEN 'NULL_VALUE'
      ELSE 'INVALID'
    END as data_quality_flag
  FROM landing_raw.raw_environmental_health_data
  WHERE county IS NOT NULL;
  
  env_row_count := SQLROWCOUNT;
  
  result_msg := 'Successfully processed curated data: ' || :health_row_count || ' health records, ' || :env_row_count || ' environmental records';
  
  INSERT INTO logging.data_quality_log 
    (table_name, quality_check_name, check_result, check_value, threshold_value, details)
  VALUES 
    ('curated_health_indicators', 'row_count_check', 
     CASE WHEN :health_row_count > 0 THEN 'PASS' ELSE 'FAIL' END, 
     :health_row_count, 1, 'Health indicators processing completed'),
    ('curated_environmental_data', 'row_count_check', 
     CASE WHEN :env_row_count > 0 THEN 'PASS' ELSE 'FAIL' END, 
     :env_row_count, 1, 'Environmental data processing completed');
  
  UPDATE logging.pipeline_execution_log 
  SET 
    execution_end = CURRENT_TIMESTAMP(),
    execution_status = 'SUCCESS',
    rows_processed = :health_row_count + :env_row_count
  WHERE procedure_name = 'sp_process_curated_data' 
    AND execution_start = :proc_start;
  
  RETURN result_msg;
  
EXCEPTION
  WHEN OTHER THEN
    error_msg := SQLERRM;
    UPDATE logging.pipeline_execution_log 
    SET 
      execution_end = CURRENT_TIMESTAMP(),
      execution_status = 'FAILED',
      error_message = :error_msg
    WHERE procedure_name = 'sp_process_curated_data' 
      AND execution_start = :proc_start;
    RETURN 'ERROR: ' || :error_msg;
END;
$$;