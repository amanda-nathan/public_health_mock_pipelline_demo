USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA LANDING_RAW;

CREATE OR REPLACE PROCEDURE sp_ingest_raw_data(source_type STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  row_count INTEGER;
  error_msg STRING := '';
  proc_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  result_msg STRING := '';
BEGIN
  
  USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
  USE SCHEMA LANDING_RAW;
  
  INSERT INTO logging.pipeline_execution_log 
    (procedure_name, execution_start, execution_status, user_name, warehouse_name)
  VALUES 
    ('sp_ingest_raw_data', :proc_start, 'RUNNING', CURRENT_USER(), CURRENT_WAREHOUSE());
  
  IF (:source_type = 'CDC_PLACES') THEN
    TRUNCATE TABLE raw_cdc_places_data;
    
    INSERT INTO raw_cdc_places_data (
      state_abbr, county_name, measure_id, data_value, population, 
      latitude, longitude, category, measure, unitofmeasure, 
      data_value_type, geolocation, locationid, locationdesc, 
      datasource, categoryid, measureid, datavaluetypeid, 
      short_question_text, year
    ) VALUES 
    ('MA', 'Middlesex', 'ACCESS2', 15.2, 1628706, 42.3868, -71.2962, 
     'Health Care Access and Quality', 'Current lack of health insurance among adults aged 18–64 years', 
     'Percent', 'Age-adjusted prevalence', '(42.3868, -71.2962)', '25017', 
     'Middlesex County', 'BRFSS', 'ACCESS2', 'ACCESS2', 'AgeAdjPrev', 
     'No health insurance', 2021),
    ('MA', 'Essex', 'ACCESS2', 18.7, 809829, 42.6348, -70.9228, 
     'Health Care Access and Quality', 'Current lack of health insurance among adults aged 18–64 years', 
     'Percent', 'Age-adjusted prevalence', '(42.6348, -70.9228)', '25009', 
     'Essex County', 'BRFSS', 'ACCESS2', 'ACCESS2', 'AgeAdjPrev', 
     'No health insurance', 2021),
    ('MA', 'Worcester', 'CANCER', 456.2, 862618, 42.2553, -71.8973, 
     'Cancer', 'All cancer types age-adjusted incidence rate', 'Per 100000', 
     'Age-adjusted rate', '(42.2553, -71.8973)', '25027', 'Worcester County', 
     'USCS', 'CANCER', 'CANCER', 'AgeAdjRate', 'Cancer incidence', 2021);
    
    row_count := SQLROWCOUNT;
    result_msg := 'Successfully ingested ' || :row_count || ' CDC Places records';
    
  ELSEIF (:source_type = 'ENVIRONMENTAL') THEN
    TRUNCATE TABLE raw_environmental_health_data;
    
    INSERT INTO raw_environmental_health_data (
      location_id, county, air_quality_index, pm25_concentration, 
      lead_exposure_risk, water_quality_score, environmental_justice_score, 
      vulnerable_population_pct, facility_name, facility_address, 
      last_inspection_date, compliance_status, year
    ) VALUES 
    ('MA_25017_001', 'Middlesex', 42, 8.3, 'Low', 87, 0.65, 23.4, 
     'Cambridge Environmental Monitor', '123 Main St, Cambridge, MA 02139', 
     '2024-03-15', 'Compliant', 2024),
    ('MA_25009_002', 'Essex', 48, 9.1, 'Moderate', 82, 0.72, 31.2, 
     'Lynn Environmental Station', '456 Ocean Ave, Lynn, MA 01902', 
     '2024-02-28', 'Non-Compliant', 2024),
    ('MA_25027_003', 'Worcester', 51, 10.7, 'High', 75, 0.85, 38.9, 
     'Worcester Industrial Monitor', '789 Industrial Rd, Worcester, MA 01608', 
     '2024-01-20', 'Under Review', 2024);
    
    row_count := SQLROWCOUNT;
    result_msg := 'Successfully ingested ' || :row_count || ' Environmental records';
    
  ELSE
    error_msg := 'Invalid source_type: ' || :source_type;
    result_msg := :error_msg;
  END IF;
  
  UPDATE logging.pipeline_execution_log 
  SET 
    execution_end = CURRENT_TIMESTAMP(),
    execution_status = CASE WHEN :error_msg = '' THEN 'SUCCESS' ELSE 'FAILED' END,
    rows_processed = :row_count,
    error_message = :error_msg
  WHERE procedure_name = 'sp_ingest_raw_data' 
    AND execution_start = :proc_start;
  
  IF (:source_type = 'CDC_PLACES' AND :row_count > 0) THEN
    INSERT INTO logging.data_quality_log 
      (table_name, quality_check_name, check_result, check_value, threshold_value, details)
    VALUES 
      ('raw_cdc_places_data', 'row_count_check', 'PASS', :row_count, 1, 'Minimum row threshold met');
  END IF;
  
  RETURN result_msg;
  
EXCEPTION
  WHEN OTHER THEN
    error_msg := SQLERRM;
    UPDATE logging.pipeline_execution_log 
    SET 
      execution_end = CURRENT_TIMESTAMP(),
      execution_status = 'FAILED',
      error_message = :error_msg
    WHERE procedure_name = 'sp_ingest_raw_data' 
      AND execution_start = :proc_start;
    RETURN 'ERROR: ' || :error_msg;
END;
$$;