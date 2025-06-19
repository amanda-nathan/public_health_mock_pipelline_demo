USE SCHEMA UTILITY;
 
CREATE OR REPLACE TASK TASK_RUN_PIPELINE
  WAREHOUSE = PH_DEMO_WH
  SCHEDULE = 'USING CRON 0 5 * * * UTC' 
  AS
  SELECT 'Starting pipeline'; ß

-- Task to process curated data, dependent on the parent task
CREATE OR REPLACE TASK TASK_PROCESS_CURATED
  WAREHOUSE = PH_DEMO_WH
  AFTER TASK_RUN_PIPELINE
  AS
  CALL UTILITY.sp_process_curated_data();

-- Task to build the datamart, dependent on the curated task
CREATE OR REPLACE TASK TASK_BUILD_DATAMART
  WAREHOUSE = PH_DEMO_WH
  AFTER TASK_PROCESS_CURATED
  AS
  CALL UTILITY.sp_build_datamart();

-- Initially, all tasks are suspended. The CI/CD pipeline will resume them for a run.
-- To run manually for testing:
EXECUTE TASK TASK_RUN_PIPELINE;
-- To enable the schedule:
-- ALTER TASK TASK_RUN_PIPELINE RESUME;
-- ALTER TASK TASK_PROCESS_CURATED RESUME;
-- ALTER TASK TASK_BUILD_DATAMART RESUME;