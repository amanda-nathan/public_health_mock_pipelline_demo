USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA LANDING_RAW;
USE WAREHOUSE DEV_WH;

-- Create task to ingest CDC Places data (runs daily at 2 AM)
CREATE OR REPLACE TASK task_ingest_cdc_places
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
  CALL sp_ingest_raw_data('CDC_PLACES');

-- Create task to ingest Environmental data (runs daily at 2:30 AM)
CREATE OR REPLACE TASK task_ingest_environmental
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 30 2 * * * America/New_York'
AS
  CALL sp_ingest_raw_data('ENVIRONMENTAL');

-- Create task to process curated data (runs after ingestion, at 3 AM)
CREATE OR REPLACE TASK task_process_curated
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
AS
  CALL curated.sp_process_curated_data();

-- Create task to build data mart (runs after curated processing, at 4 AM)
CREATE OR REPLACE TASK task_build_datamart
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 0 4 * * * America/New_York'
AS
  CALL data_mart.sp_build_datamart();

-- Enable all tasks (tasks are created in SUSPENDED state by default)
ALTER TASK task_ingest_cdc_places RESUME;
ALTER TASK task_ingest_environmental RESUME;
ALTER TASK task_process_curated RESUME;
ALTER TASK task_build_datamart RESUME;

-- Create a monitoring task that runs every hour to check for failures
CREATE OR REPLACE TASK task_monitor_pipeline
  WAREHOUSE = DEV_WH
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'
AS
  INSERT INTO logging.data_quality_log (table_name, quality_check_name, check_result, check_value, details)
  SELECT 
    'pipeline_monitoring' as table_name,
    'failed_executions_check' as quality_check_name,
    CASE WHEN failed_count > 0 THEN 'FAIL' ELSE 'PASS' END as check_result,
    failed_count as check_value,
    'Failed executions in last 24 hours: ' || failed_count as details
  FROM (
    SELECT COUNT(*) as failed_count
    FROM logging.pipeline_execution_log
    WHERE execution_status = 'FAILED'
      AND execution_start >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
  );

ALTER TASK task_monitor_pipeline RESUME;