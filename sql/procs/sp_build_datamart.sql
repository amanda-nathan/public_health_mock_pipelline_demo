USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA DATA_MART;

CREATE OR REPLACE PROCEDURE sp_build_datamart()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  dashboard_row_count INTEGER;
  risk_row_count INTEGER;
  error_msg STRING := '';
  proc_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  result_msg STRING := '';
BEGIN
  
  -- Log procedure start
  INSERT INTO logging.pipeline_execution_log 
    (procedure_name, execution_start, execution_status, user_name, warehouse_name)
  VALUES 
    ('sp_build_datamart', :proc_start, 'RUNNING', CURRENT_USER(), CURRENT_WAREHOUSE());
  
  -- Build public health dashboard
  MERGE INTO public_health_dashboard AS target
  USING (
    WITH health_metrics AS (
      SELECT 
        county_name,
        state_abbr,
        MAX(population) as total_population,
        MAX(CASE WHEN measure_category LIKE '%Diabetes%' THEN measure_value END) as diabetes_rate,
        MAX(CASE WHEN measure_category LIKE '%Obesity%' THEN measure_value END) as obesity_rate,
        MAX(CASE WHEN measure_category LIKE '%Cancer%' THEN measure_value END) as cancer_incidence_rate,
        MAX(CASE WHEN measure_name LIKE '%insurance%' THEN measure_value END) as uninsured_rate,
        data_year
      FROM curated.curated_health_indicators
      WHERE data_quality_flag = 'VALID'
      GROUP BY county_name, state_abbr, data_year
    ),
    env_metrics AS (
      SELECT 
        county,
        AVG(air_quality_index) as air_quality_avg,
        AVG(environmental_justice_score) as environmental_justice_score,
        COUNT(CASE WHEN lead_risk_level = 'High' THEN 1 END) as high_risk_facilities_count,
        data_year
      FROM curated.curated_environmental_data
      WHERE data_quality_flag = 'VALID'
      GROUP BY county, data_year
    )
    SELECT 
      h.county_name,
      h.state_abbr,
      h.total_population,
      h.diabetes_rate,
      h.obesity_rate,
      h.cancer_incidence_rate,
      h.uninsured_rate,
      e.air_quality_avg,
      e.environmental_justice_score,
      e.high_risk_facilities_count,
      h.data_year
    FROM health_metrics h
    LEFT JOIN env_metrics e ON h.county_name = e.county AND h.data_year = e.data_year
  ) AS source
  ON target.county_name = source.county_name AND target.data_year = source.data_year
  WHEN MATCHED THEN UPDATE SET
    total_population = source.total_population,
    diabetes_rate = source.diabetes_rate,
    obesity_rate = source.obesity_rate,
    cancer_incidence_rate = source.cancer_incidence_rate,
    uninsured_rate = source.uninsured_rate,
    air_quality_avg = source.air_quality_avg,
    environmental_justice_score = source.environmental_justice_score,
    high_risk_facilities_count = source.high_risk_facilities_count,
    last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    county_name, state_abbr, total_population, diabetes_rate, obesity_rate, 
    cancer_incidence_rate, uninsured_rate, air_quality_avg, environmental_justice_score, 
    high_risk_facilities_count, data_year
  ) VALUES (
    source.county_name, source.state_abbr, source.total_population, source.diabetes_rate, 
    source.obesity_rate, source.cancer_incidence_rate, source.uninsured_rate, 
    source.air_quality_avg, source.environmental_justice_score, source.high_risk_facilities_count, 
    source.data_year
  );
  
  dashboard_row_count := SQLROWCOUNT;
  
  -- Build environmental risk summary
  MERGE INTO environmental_risk_summary AS target
  USING (
    SELECT 
      county,
      lead_risk_level as risk_level,
      COUNT(*) as facility_count,
      AVG(vulnerable_population_pct) as vulnerable_population_pct,
      AVG(air_quality_index) as avg_air_quality,
      AVG(water_quality_score) as avg_water_quality,
      SUM(CASE WHEN compliance_status = 'Compliant' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 as compliance_rate,
      data_year
    FROM curated.curated_environmental_data
    WHERE data_quality_flag = 'VALID'
    GROUP BY county, lead_risk_level, data_year
  ) AS source
  ON target.county_name = source.county 
    AND target.risk_level = source.risk_level 
    AND target.data_year = source.data_year
  WHEN MATCHED THEN UPDATE SET
    facility_count = source.facility_count,
    vulnerable_population_pct = source.vulnerable_population_pct,
    avg_air_quality = source.avg_air_quality,
    avg_water_quality = source.avg_water_quality,
    compliance_rate = source.compliance_rate,
    last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    county_name, risk_level, facility_count, vulnerable_population_pct, 
    avg_air_quality, avg_water_quality, compliance_rate, data_year
  ) VALUES (
    source.county, source.risk_level, source.facility_count, source.vulnerable_population_pct, 
    source.avg_air_quality, source.avg_water_quality, source.compliance_rate, source.data_year
  );
  
  risk_row_count := SQLROWCOUNT;
  
  -- ✅ Fixed: Added : prefix for variable references
  result_msg := 'Successfully built data mart: ' || :dashboard_row_count || ' dashboard records, ' || :risk_row_count || ' risk summary records';
  
  -- ✅ Fixed: Added : prefix for variable references
  -- Update execution log
  UPDATE logging.pipeline_execution_log 
  SET 
    execution_end = CURRENT_TIMESTAMP(),
    execution_status = 'SUCCESS',
    rows_processed = :dashboard_row_count + :risk_row_count
  WHERE procedure_name = 'sp_build_datamart' 
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
    WHERE procedure_name = 'sp_build_datamart' 
      AND execution_start = :proc_start;
    RETURN 'ERROR: ' || :error_msg;
END;
$$;