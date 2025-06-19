USE SCHEMA UTILITY;
CREATE OR REPLACE PROCEDURE sp_build_datamart()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    log_procedure_name VARCHAR := 'SP_BUILD_DATAMART';
    status VARCHAR := 'SUCCESS';
    message VARCHAR := '';
    row_count INT := 0;
BEGIN
    -- This procedure creates an aggregated table for analysts.
    -- It demonstrates the final transformation layer.
    
    TRUNCATE TABLE DATAMART.COUNTY_HEALTH_METRICS;

    INSERT INTO DATAMART.COUNTY_HEALTH_METRICS (COUNTY, STATE, TOTAL_MEASURES, AVG_VALUE)
    SELECT
        COUNTY,
        STATE,
        COUNT(DISTINCT MEASURE) AS TOTAL_MEASURES,
        AVG(VALUE) AS AVG_VALUE
    FROM CURATED.CDC_PLACES
    GROUP BY COUNTY, STATE;

    row_count := SQLROWCOUNT;
    message := 'Successfully built DATAMART.COUNTY_HEALTH_METRICS with ' || row_count || ' rows.';

    INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE, ROW_COUNT)
    VALUES (:log_procedure_name, :status, :message, :row_count);

    RETURN 'Success: ' || message;

EXCEPTION
    WHEN OTHER THEN
        status := 'ERROR';
        message := 'Error building data mart: ' || SQLERRM;
        INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE)
        VALUES (:log_procedure_name, :status, :message);
        RETURN 'Error: ' || message;
END;
$$;