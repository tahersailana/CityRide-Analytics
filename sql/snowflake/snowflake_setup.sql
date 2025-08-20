-- Create a role for Airflow
CREATE ROLE IF NOT EXISTS AIRFLOW_ROLE;

-- Create a user for Airflow
CREATE USER IF NOT EXISTS AIRFLOW_USER
  PASSWORD = 'StrongPassword123!'  -- replace with strong secret (store in Airflow connection, not in code)
  DEFAULT_ROLE = AIRFLOW_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH   -- replace with your warehouse
  DEFAULT_NAMESPACE = CITYRIDE_ANALYTICS.PUBLIC
  MUST_CHANGE_PASSWORD = FALSE;

-- create warehouse for Airflow
CREATE WAREHOUSE AIRFLOW_WH
WITH WAREHOUSE_SIZE = 'SMALL'      
AUTO_SUSPEND = 60                  
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

--  Grant privileges to role
GRANT USAGE ON WAREHOUSE AIRFLOW_WH TO ROLE AIRFLOW_ROLE;
GRANT OPERATE ON WAREHOUSE AIRFLOW_WH TO ROLE AIRFLOW_ROLE;

-- Assign role to the user
GRANT ROLE AIRFLOW_ROLE TO USER AIRFLOW_USER;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_ROLE;

-- Grant usage on database + schema
Create DATABASE CITYRIDE_ANALYTICS;
Create schema CITYRIDE_ANALYTICS.CITYRIDE_ANALYTICS;
GRANT USAGE ON INTEGRATION MY_S3_INTEGRATION TO ROLE AIRFLOW_ROLE;
GRANT USAGE, ALL PRIVILEGES ON DATABASE CITYRIDE_ANALYTICS TO ROLE AIRFLOW_ROLE;
GRANT USAGE, ALL PRIVILEGES ON SCHEMA CITYRIDE_ANALYTICS.CITYRIDE_METADATA TO ROLE AIRFLOW_ROLE;
GRANT USAGE, ALL PRIVILEGES ON SCHEMA CITYRIDE_ANALYTICS.CITYRIDE_ANALYTICS TO ROLE AIRFLOW_ROLE;

-- create aws-snowflake integration
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::783407116877:role/snowflake_s3_integration_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://cityride-analytics/raw/')
  COMMENT = 'Integration for CityRide raw data in S3';

-- ######################### switch to user AIRFLOW_USER #####################--
use schema CITYRIDE_METADATA

-- create file format 
CREATE FILE FORMAT cityride_parquet_format
    TYPE = PARQUET
    NULL_IF = ('NaN', 'nan', 'NAN', '');

-- create external stage
CREATE OR REPLACE STAGE cityride_stage_raw
URL = 's3://cityride-analytics/raw/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = cityride_parquet_format;
