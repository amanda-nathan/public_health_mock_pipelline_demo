USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA CURATED;

-- First, unset existing masking policies from columns if they exist
-- This prevents the "cannot be dropped/replaced as it is associated" error

-- Unset policies from curated schema tables
BEGIN
 
  BEGIN
    ALTER TABLE curated_environmental_data 
    MODIFY COLUMN facility_address 
    UNSET MASKING POLICY;
  EXCEPTION
    WHEN OTHER THEN
   
      NULL;
  END;
  
  -- Try to unset coordinate masking policies  
  BEGIN
    ALTER TABLE curated_health_indicators 
    MODIFY COLUMN latitude 
    UNSET MASKING POLICY;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
  
  BEGIN
    ALTER TABLE curated_health_indicators 
    MODIFY COLUMN longitude 
    UNSET MASKING POLICY;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
END;

-- Switch to data mart schema to unset population policy
USE SCHEMA DATA_MART;

BEGIN
  -- Try to unset population masking policy
  BEGIN
    ALTER TABLE public_health_dashboard 
    MODIFY COLUMN total_population 
    UNSET MASKING POLICY;
  EXCEPTION
    WHEN OTHER THEN
      NULL;
  END;
END;

-- Now drop existing policies if they exist
USE SCHEMA CURATED;

-- Drop policies with error handling
BEGIN
  DROP MASKING POLICY IF EXISTS address_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

BEGIN
  DROP MASKING POLICY IF EXISTS coordinate_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

USE SCHEMA DATA_MART;

BEGIN
  DROP MASKING POLICY IF EXISTS population_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

-- Now create the new masking policies
USE SCHEMA CURATED;

-- Address masking policy for environmental data
CREATE MASKING POLICY address_mask AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE') THEN val
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN 
      CONCAT(LEFT(val, 10), '*** [MASKED] ***')
    ELSE '[REDACTED]'
  END;

-- Coordinate masking policy for location data
CREATE MASKING POLICY coordinate_mask AS (val FLOAT) RETURNS FLOAT ->
  CASE 
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE') THEN val
    WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN ROUND(val, 2)
    ELSE NULL
  END;

USE SCHEMA DATA_MART;

-- Population masking policy for demographic data
CREATE MASKING POLICY population_mask AS (val INTEGER) RETURNS INTEGER ->
  CASE 
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_ANALYST_ROLE') THEN val
    ELSE ROUND(val, -3)  -- Round to nearest thousand for public users
  END;

-- Now apply the masking policies to the appropriate columns
USE SCHEMA CURATED;

-- Apply address masking to environmental facility addresses
BEGIN
  ALTER TABLE curated_environmental_data 
  MODIFY COLUMN facility_address 
  SET MASKING POLICY address_mask;
EXCEPTION
  WHEN OTHER THEN
    -- Log warning but continue - table might not exist yet
    NULL;
END;

 
BEGIN
  ALTER TABLE curated_health_indicators 
  MODIFY COLUMN latitude 
  SET MASKING POLICY coordinate_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

BEGIN
  ALTER TABLE curated_health_indicators 
  MODIFY COLUMN longitude 
  SET MASKING POLICY coordinate_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

 
USE SCHEMA DATA_MART;

BEGIN
  ALTER TABLE public_health_dashboard 
  MODIFY COLUMN total_population 
  SET MASKING POLICY population_mask;
EXCEPTION
  WHEN OTHER THEN
    NULL;
END;

 
USE SCHEMA CURATED;
SHOW MASKING POLICIES;

USE SCHEMA DATA_MART;
SHOW MASKING POLICIES;