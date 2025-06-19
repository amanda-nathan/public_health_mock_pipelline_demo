USE DATABASE PUBLIC_HEALTH_MODERNIZATION_DEMO;
USE SCHEMA LANDING_RAW;
USE WAREHOUSE DEV_WH;

 
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;

 
CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

--Create internal stage (not using external storage right now)
CREATE OR REPLACE STAGE public_health_stage
  FILE_FORMAT = csv_format;

 

CREATE OR REPLACE TABLE raw_cdc_places_data (
  state_abbr VARCHAR(2),
  county_name VARCHAR(100),
  measure_id VARCHAR(50),
  data_value FLOAT,
  population INTEGER,
  latitude FLOAT,
  longitude FLOAT,
  category VARCHAR(200),
  measure VARCHAR(500),
  unitofmeasure VARCHAR(50),
  data_value_type VARCHAR(100),
  geolocation VARCHAR(200),
  locationid VARCHAR(20),
  locationdesc VARCHAR(200),
  datasource VARCHAR(50),
  categoryid VARCHAR(50),
  measureid VARCHAR(50),
  datavaluetypeid VARCHAR(50),
  short_question_text VARCHAR(200),
  year INTEGER,
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw_environmental_health_data (
  location_id VARCHAR(50),
  county VARCHAR(100),
  air_quality_index INTEGER,
  pm25_concentration FLOAT,
  lead_exposure_risk VARCHAR(20),
  water_quality_score INTEGER,
  environmental_justice_score FLOAT,
  vulnerable_population_pct FLOAT,
  facility_name VARCHAR(200),
  facility_address VARCHAR(500),
  last_inspection_date DATE,
  compliance_status VARCHAR(50),
  year INTEGER,
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

 
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE curated_health_indicators (
  location_key VARCHAR(50),
  state_abbr VARCHAR(2),
  county_name VARCHAR(100),
  measure_category VARCHAR(200),
  measure_name VARCHAR(500),
  measure_value FLOAT,
  population INTEGER,
  latitude FLOAT,
  longitude FLOAT,
  data_year INTEGER,
  data_source VARCHAR(50),
  data_quality_flag VARCHAR(10),
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE curated_environmental_data (
  location_key VARCHAR(50),
  county VARCHAR(100),
  air_quality_index INTEGER,
  pm25_concentration FLOAT,
  lead_risk_level VARCHAR(20),
  water_quality_score INTEGER,
  environmental_justice_score FLOAT,
  vulnerable_population_pct FLOAT,
  facility_name VARCHAR(200),
  facility_address VARCHAR(500),
  last_inspection_date DATE,
  compliance_status VARCHAR(50),
  data_year INTEGER,
  data_quality_flag VARCHAR(10),
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

 
USE SCHEMA DATA_MART;

CREATE OR REPLACE TABLE public_health_dashboard (
  county_name VARCHAR(100),
  state_abbr VARCHAR(2),
  total_population INTEGER,
  diabetes_rate FLOAT,
  obesity_rate FLOAT,
  cancer_incidence_rate FLOAT,
  uninsured_rate FLOAT,
  air_quality_avg INTEGER,
  environmental_justice_score FLOAT,
  high_risk_facilities_count INTEGER,
  data_year INTEGER,
  last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE environmental_risk_summary (
  county_name VARCHAR(100),
  risk_level VARCHAR(20),
  facility_count INTEGER,
  vulnerable_population_pct FLOAT,
  avg_air_quality INTEGER,
  avg_water_quality INTEGER,
  compliance_rate FLOAT,
  data_year INTEGER,
  last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);