-- 1. Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake
USE ROLE accountadmin;

CREATE ROLE IF NOT EXISTS admin;
CREATE ROLE IF NOT EXISTS PII;
CREATE ROLE IF NOT EXISTS Developer;

GRANT ROLE IDENTIFIER('"ADMIN"') TO ROLE IDENTIFIER('"ACCOUNTADMIN"');
GRANT ROLE IDENTIFIER('"PII"') TO ROLE IDENTIFIER('"ACCOUNTADMIN"');
GRANT ROLE IDENTIFIER('"DEVELOPER"') TO ROLE IDENTIFIER('"ADMIN"');
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE ADMIN;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE admin;

----------------------------------------------------------------------------------------------------
-- 2. Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries

CREATE WAREHOUSE IF NOT EXISTS assignment_wh
WAREHOUSE_SIZE = 'MEDIUM'
WAREHOUSE_TYPE = 'STANDARD';
USE WAREHOUSE assignment_wh;

----------------------------------------------------------------------------------------------------
-- 3. Switch to the admin role
use role admin;

----------------------------------------------------------------------------------------------------
-- 4. Create a database assignment_db
create database if not exists assignment_db;

use database assignment_db;

----------------------------------------------------------------------------------------------------
-- 5. Create a schema my_schema
create schema if not exists my_schema;
use schema my_schema;

----------------------------------------------------------------------------------------------------
-- 6. Create a table using any sample csv. You can get 1 by googling for
-- sample csv’s. Preferably search for a sample employee dataset so that you have PII related columns else you can consider any column as PII ( 5 ).

CREATE OR REPLACE TABLE employees_internal (
    id INT,
    name VARCHAR,
    phone VARCHAR,
    email VARCHAR,
    hiredate VARCHAR,
    age INT,
    salary FLOAT,
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR DEFAULT 'Local',
    file_name VARCHAR DEFAULT 'empData.csv'
);



CREATE OR REPLACE TABLE employees_external (
    id INT,
    name VARCHAR,
    phone VARCHAR,
    email VARCHAR,
    hiredate VARCHAR,
    age INT,
    salary FLOAT,
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR DEFAULT 'AWS',
    file_name VARCHAR DEFAULT 'empData.csv'
);
----------------------------------------------------------------------------------------------------
-- 7. Also, create a variant version of this dataset

CREATE OR REPLACE TABLE employees_variant (
    id INT,
    name VARCHAR,
    employee_data VARIANT,
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR DEFAULT 'Local',
    file_name VARCHAR DEFAULT 'empData.csv'
);

----------------------------------------------------------------------------------------------------
-- 8. Load the file into an external and internal stage

create stage stage_int;

put file:///Users/ritikpatidar/Desktop/data/empData.csv @stage_int;


CREATE OR REPLACE STORAGE INTEGRATION my_intS3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::538718198607:role/myrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket-ritik/');

create stage ext_stage;

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE ext_stage
  STORAGE_INTEGRATION = my_intS3
  URL = 's3://mybucket-ritik/empData.csv'
  FILE_FORMAT = my_csv_format;

----------------------------------------------------------------------------------------------------
-- 9. Load data into the tables using copy into statements. In one table load
-- from the internal stage and in another from the external

COPY INTO EMPLOYEES_INTERNAL (id, name, phone, email, hiredate, age, salary)
FROM @stage_int
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO employees_external (id, name, phone, email, hiredate, age, salary)
FROM @ext_stage
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';

----------------------------------------------------------------------------------------------------
-- 10. Upload any parquet file to the stage location and infer the schema ofthefile

CREATE OR REPLACE FILE FORMAT my_parquet_file_format
TYPE = parquet;

CREATE OR REPLACE STAGE my_parquet_file_stage
FILE_FORMAT = my_parquet_file_format;

put file:///Users/ritikpatidar/Desktop/data/cities.parquet @my_parquet_file_stage;

----------------------------------------------------------------------------------------------------
-- 11.Run a select query on the staged parquet file without loading it to a snowflake table

Select $1 from @my_parquet_file_stage/cities.parquet;

Select $1:continent::varchar,
$1:country:name::varchar,
$1:country:city::variant
from @my_parquet_file_stage/cities.parquet;

----------------------------------------------------------------------------------------------------
-- 12. Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible
use role admin;
grant usage on database ASSIGNMENT_DB to role DEVELOPER;
grant usage on database ASSIGNMENT_DB to role pii;
grant usage on schema MY_SCHEMA to role DEVELOPER;
grant usage on schema MY_SCHEMA to role pii;
grant select on table EMPLOYEES_INTERNAL to role DEVELOPER;
grant select on table EMPLOYEES_INTERNAL to role PII;

CREATE MASKING POLICY email_masking_policy AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'DEVELOPER' THEN '**masked**'
    ELSE val
  END;

CREATE MASKING POLICY phone_masking_policy AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'DEVELOPER' THEN '**masked**'
    ELSE val
  END;

-- Apply masking policy to email column
ALTER TABLE EMPLOYEES_INTERNAL MODIFY COLUMN email SET MASKING POLICY email_masking_policy;

-- Apply masking policy to phone column
ALTER TABLE EMPLOYEES_INTERNAL MODIFY COLUMN phone SET MASKING POLICY phone_masking_policy;

use role DEVELOPER;
select * from EMPLOYEES_INTERNAL;

use role pii;
SELECT * FROM EMPLOYEES_INTERNAL;
