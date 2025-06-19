USE ROLE PH_DEMO_DEVELOPER_ROLE;
USE WAREHOUSE PH_DEMO_WH;
USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO; 

USE SCHEMA UTILITY;
CREATE OR REPLACE PROCEDURE sp_ingest_raw_data(SOURCE_NAME VARCHAR, STAGE_NAME VARCHAR, TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    log_procedure_name VARCHAR := 'SP_INGEST_RAW_DATA';
    status VARCHAR := 'SUCCESS';
    message VARCHAR;
    row_count INT := 0;
    sql_command VARCHAR;
BEGIN
    -- This dynamic SQL Stored Procedure ingests data from a specified stage into a specified raw table.
    -- It uses EXECUTE IMMEDIATE to handle dynamic object names, making it a single, reusable
    -- procedure for multiple data sources, directly addressing the challenge of handling 68+ sources.

    sql_command := 'COPY INTO PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING.' || TABLE_NAME || ' (RAW_DATA) ' ||
                   'FROM @PUBLIC_HEALTH_MODERNIZATION_DEMO.LANDING.' || STAGE_NAME || ' ' ||
                   'ON_ERROR = ''CONTINUE'';';

    -- Execute the dynamically constructed COPY command
    EXECUTE IMMEDIATE :sql_command;

    -- Use RESULT_SCAN and LAST_QUERY_ID() to get the number of rows loaded by the COPY command.
    -- This is a key pattern for capturing metadata from commands executed dynamically.
    SELECT "rows_loaded" INTO :row_count FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    message := 'Successfully loaded ' || :row_count || ' rows from stage ' || STAGE_NAME || ' for source ' || SOURCE_NAME || '.';

    -- Log success message
    INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE, ROW_COUNT)
    VALUES (:log_procedure_name, :status, :message, :row_count);

    RETURN 'Success: ' || message;

EXCEPTION
    WHEN OTHER THEN
        status := 'ERROR';
        message := 'Failed to ingest data for ' || SOURCE_NAME || '. SQL Error: ' || SQLERRM;
        
        -- Log the error
        INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE, ROW_COUNT)
        VALUES (:log_procedure_name, :status, :message, 0);

        RETURN 'Error: ' || message;
END;
$$;