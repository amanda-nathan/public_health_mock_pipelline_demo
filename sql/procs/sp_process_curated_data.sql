USE SCHEMA UTILITY;
CREATE OR REPLACE PROCEDURE sp_process_curated_data()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    log_procedure_name VARCHAR := 'SP_PROCESS_CURATED_DATA';
    status VARCHAR := 'SUCCESS';
    message VARCHAR := '';
    row_count INT := 0;
BEGIN
    -- PROCESS CDC PLACES DATA
    BEGIN
        INSERT INTO CURATED.CDC_PLACES (LOCATION_ID, COUNTY, STATE, MEASURE, VALUE, CONTACT_EMAIL)
        SELECT
            RAW_DATA:C1::INT,
            RAW_DATA:C2::STRING,
            RAW_DATA:C3::STRING,
            RAW_DATA:C4::STRING,
            RAW_DATA:C5::FLOAT,
            RAW_DATA:C6::STRING
        FROM LANDING.CDC_PLACES_RAW;
        row_count := SQLROWCOUNT;
    END;

    -- PROCESS ENVIRONMENTAL HEALTH DATA
    BEGIN
        INSERT INTO CURATED.ENV_HEALTH (REPORT_ID, LOCATION_NAME, POLLUTANT, LEVEL, REPORTED_BY_EMAIL)
        SELECT
            RAW_DATA:ReportID::STRING,
            RAW_DATA:LocationName::STRING,
            RAW_DATA:Pollutant::STRING,
            RAW_DATA:Level::FLOAT,
            RAW_DATA:ReportedByEmail::STRING
        FROM LANDING.ENV_HEALTH_RAW;
        row_count := row_count + SQLROWCOUNT;
    END;

    message := 'Successfully processed ' || row_count || ' rows into CURATED layer.';

    INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE, ROW_COUNT)
    VALUES (:log_procedure_name, :status, :message, :row_count);

    RETURN 'Success: ' || message;

EXCEPTION
    WHEN OTHER THEN
        status := 'ERROR';
        message := 'Error processing curated data: ' || SQLERRM;
        INSERT INTO UTILITY.PIPELINE_LOGS (PROCEDURE_NAME, STATUS, MESSAGE)
        VALUES (:log_procedure_name, :status, :message);
        RETURN 'Error: ' || message;
END;
$$;