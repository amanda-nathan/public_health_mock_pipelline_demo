-- ✅ Fixed: Let deployment script handle role, just focus on database/schema context
USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA LOGGING;

-- Pipeline execution tracking table
CREATE OR REPLACE TABLE pipeline_execution_log (
  log_id INTEGER AUTOINCREMENT,
  procedure_name VARCHAR(100),
  execution_start TIMESTAMP_NTZ,
  execution_end TIMESTAMP_NTZ,
  execution_status VARCHAR(20), -- SUCCESS, FAILED, RUNNING
  rows_processed INTEGER,
  error_message VARCHAR(5000),
  user_name VARCHAR(100),
  warehouse_name VARCHAR(100),
  details VARCHAR(1000),
  PRIMARY KEY (log_id)
);

-- Data quality monitoring table
CREATE OR REPLACE TABLE data_quality_log (
  log_id INTEGER AUTOINCREMENT,
  table_name VARCHAR(100),
  quality_check_name VARCHAR(100),
  check_result VARCHAR(20), -- PASS, FAIL, WARNING
  check_value FLOAT,
  threshold_value FLOAT,
  check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  details VARCHAR(1000),
  PRIMARY KEY (log_id)
);

-- Create a view for easy monitoring
CREATE OR REPLACE VIEW pipeline_status_dashboard AS
SELECT 
  procedure_name,
  execution_status,
  MAX(execution_start) as last_run_time,
  SUM(rows_processed) as total_rows_processed_today,
  COUNT(*) as runs_today
FROM pipeline_execution_log
WHERE DATE(execution_start) = CURRENT_DATE()
GROUP BY procedure_name, execution_status
ORDER BY last_run_time DESC;

-- Grant permissions to DATA_ENGINEER_ROLE for all logging objects
GRANT ALL ON ALL TABLES IN SCHEMA LOGGING TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON ALL VIEWS IN SCHEMA LOGGING TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA LOGGING TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA LOGGING TO ROLE DATA_ENGINEER_ROLE;