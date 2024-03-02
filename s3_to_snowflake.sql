-- SQL Worksheet for building Snowpipe - this is a sql file that automates data loading with Snowpipe between AWS S3 bucket and Snowflake database;
-- builds tables in database called Synthea with schema called Stage; creates Snowpipe for each table. 


--** 1. Overview **
--** 2. Setting up Snowflake **
--** 3. Choose the Data Ingestion Method **
--** 4. Configure Cloud Storage Event Notifications **


--** 5. Configure Cloud Storage Permissions **

--5.1)  Create IAM Policy for Snowflake's S3 Access
--5.2)  New IAM Role

--5.3) Integrate IAM user with Snowflake storage
CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = “—“
  STORAGE_ALLOWED_LOCATIONS = (“—);

--5.4) Run storage integration description command
desc integration S3_role_integration;

--5.5) IAM User Permissions


--** 6. Create a pipe in Snowflake **

--6.1)  Create a Database, Table, Stage, and Pipe
--create database
create or replace database Synthea;
 
-- Create table
CREATE SCHEMA Stage;
CREATE OR REPLACE TABLE Stage.Patients (
Id varchar(1000),
BIRTHDATE varchar(1000),
DEATHDATE varchar(1000),
SSN VARCHAR(255),
DRIVERS VARCHAR(1000),
PASSPORT VARCHAR(1000),
PREFIX VARCHAR(255),
FIRST VARCHAR(255),
LAST VARCHAR(255),
SUFFIX VARCHAR(255),
MAIDEN VARCHAR(255),
MARITAL VARCHAR(255),
RACE VARCHAR(255),
ETHNICITY VARCHAR(255),
GENDER VARCHAR(255),
BIRTHPLACE VARCHAR(255),
ADDRESS VARCHAR(1000),
CITY VARCHAR(255),
STATE VARCHAR(255),
COUNTY VARCHAR(255),
FIPS VARCHAR(255),
ZIP VARCHAR(255),
LAT FLOAT,
LON FLOAT,
HEALTHCARE_EXPENSES FLOAT,
HEALTHCARE_COVERAGE FLOAT,
INCOME FLOAT);

CREATE OR REPLACE TABLE Stage.Providers (
    Id varchar(1000),
    ORGANIZATION VARCHAR(1000),
    NAME VARCHAR(255),
    GENDER VARCHAR(255),
    SPECIALITY VARCHAR(1000),
    ADDRESS VARCHAR(1000),
    CITY VARCHAR(255),
    STATE VARCHAR(255),
    ZIP VARCHAR(255),
    LAT FLOAT,
    LON FLOAT,
    ENCOUNTERS varchar(1000),
    PROCEDURES varchar(1000)
);

CREATE OR REPLACE TABLE Stage.Encounters (
    Id varchar(1000),
    "START" varchar(1000),
    STOP varchar(1000),
    PATIENT VARCHAR(1000),
    ORGANIZATION VARCHAR(1000),
    PROVIDER VARCHAR(1000),
    PAYER VARCHAR(1000),
    ENCOUNTERCLASS VARCHAR(255),
    CODE VARCHAR(1000),
    DESCRIPTION VARCHAR(1000),
    BASE_ENCOUNTER_COST FLOAT,
    TOTAL_CLAIM_COST FLOAT,
    PAYER_COVERAGE FLOAT,
    REASONCODE VARCHAR(1000),
    REASONDESCRIPTION VARCHAR(1000)
)

CREATE OR REPLACE TABLE Stage.ClaimsTransactions (
    ID VARCHAR(1000),
    CLAIMID VARCHAR(1000),
    CHARGEID VARCHAR(1000),
    PATIENTID VARCHAR(1000),
    TYPE VARCHAR(1000),
    AMOUNT FLOAT,
    METHOD VARCHAR(1000),
    FROMDATE VARCHAR(1000),
    TODATE VARCHAR(1000),
    PLACEOFSERVICE VARCHAR(1000),
    PROCEDURECODE VARCHAR(1000),
    MODIFIER1 VARCHAR(1000),
    MODIFIER2 VARCHAR(1000),
    DIAGNOSISREF1 VARCHAR(1000),
    DIAGNOSISREF2 VARCHAR(1000),
    DIAGNOSISREF3 VARCHAR(1000),
    DIAGNOSISREF4 VARCHAR(1000),
    UNITS INTEGER,
    DEPARTMENTID VARCHAR(1000),
    NOTES VARCHAR(1000),
    UNITAMOUNT FLOAT,
    TRANSFEROUTID VARCHAR(1000),
    TRANSFERTYPE VARCHAR(1000),
    PAYMENTS FLOAT,
    ADJUSTMENTS FLOAT,
    TRANSFERS FLOAT,
    OUTSTANDING FLOAT,
    APPOINTMENTID VARCHAR(1000),
    LINENOTE VARCHAR(1000),
    PATIENTINSURANCEID VARCHAR(1000),
    FEESCHEDULEID VARCHAR(1000),
    PROVIDERID VARCHAR(1000),
    SUPERVISINGPROVIDERID VARCHAR(1000)
);

CREATE OR REPLACE TABLE Stage.Claims (
    Id VARCHAR(1000),
    PATIENTID VARCHAR(1000),
    PROVIDERID VARCHAR(1000),
    PRIMARYPATIENTINSURANCEID VARCHAR(1000),
    SECONDARYPATIENTINSURANCEID VARCHAR(1000),
    DEPARTMENTID VARCHAR(1000),
    PATIENTDEPARTMENTID VARCHAR(1000),
    DIAGNOSIS1 VARCHAR(1000),
    DIAGNOSIS2 VARCHAR(1000),
    DIAGNOSIS3 VARCHAR(1000),
    DIAGNOSIS4 VARCHAR(1000),
    DIAGNOSIS5 VARCHAR(1000),
    DIAGNOSIS6 VARCHAR(1000),
    DIAGNOSIS7 VARCHAR(1000),
    DIAGNOSIS8 VARCHAR(1000),
    REFERRINGPROVIDERID VARCHAR(1000),
    APPOINTMENTID VARCHAR(1000),
    CURRENTILLNESSDATE VARCHAR(1000),
    SERVICEDATE VARCHAR(1000),
    SUPERVISINGPROVIDERID VARCHAR(1000),
    STATUS1 VARCHAR(1000),
    STATUS2 VARCHAR(1000),
    STATUSP VARCHAR(1000),
    OUTSTANDING1 VARCHAR(1000),
    OUTSTANDING2 VARCHAR(1000),
    OUTSTANDINGP VARCHAR(1000),
    LASTBILLEDDATE1 VARCHAR(1000),
    LASTBILLEDDATE2 VARCHAR(1000),
    LASTBILLEDDATEP VARCHAR(1000),
    HEALTHCARECLAIMTYPEID1 VARCHAR(1000),
    HEALTHCARECLAIMTYPEID2 VARCHAR(1000)
)

CREATE OR REPLACE TABLE Stage.Payers (
    Id VARCHAR(1000),
    NAME VARCHAR(1000),
    OWNERSHIP VARCHAR(1000),
    ADDRESS VARCHAR(1000),
    CITY VARCHAR(1000),
    STATE_HEADQUARTERED VARCHAR(1000),
    ZIP VARCHAR(1000),
    PHONE VARCHAR(1000),
    AMOUNT_COVERED FLOAT,
    AMOUNT_UNCOVERED FLOAT,
    REVENUE FLOAT,
    COVERED_ENCOUNTERS INTEGER,
    UNCOVERED_ENCOUNTERS INTEGER,
    COVERED_MEDICATIONS INTEGER,
    UNCOVERED_MEDICATIONS INTEGER,
    COVERED_PROCEDURES INTEGER,
    UNCOVERED_PROCEDURES INTEGER,
    COVERED_IMMUNIZATIONS INTEGER,
    UNCOVERED_IMMUNIZATIONS INTEGER,
    UNIQUE_CUSTOMERS INTEGER,
    QOLS_AVG FLOAT,
    MEMBER_MONTHS INTEGER
)
-- Create stage
use schema Synthea.Stage;

create or replace stage S3_stage_patients
  url = ('s3://syntheabling/staging/patients/')
  storage_integration = S3_role_integration;
  
create or replace stage S3_stage_providers
  url = ('s3://syntheabling/staging/providers/')
  storage_integration = S3_role_integration;
  
create or replace stage S3_stage_encounters
  url = ('s3://syntheabling/staging/encounters/')
  storage_integration = S3_role_integration;

create or replace stage S3_stage_claims
  url = ('s3://syntheabling/staging/claims/')
  storage_integration = S3_role_integration;
  
create or replace stage S3_stage_claimstransactions
  url = ('s3://syntheabling/staging/claimstransaction/')
  storage_integration = S3_role_integration;

create or replace stage S3_stage_payers
  url = ('s3://syntheabling/staging/payers/')
  storage_integration = S3_role_integration;
  
desc stage s3_stage_patients;
desc stage s3_stage_providers;
desc stage s3_stage_encounters;
desc stage s3_stage_claims;
desc stage s3_stage_claimstransactions;
desc stage s3_stage_payers;

-- Create pipe
create or replace pipe Synthea.Stage.S3_patients_pipe auto_ingest=true as
  copy into Synthea.Stage.PATIENTS
  from @Synthea.Stage.S3_stage_patients;
  
create or replace pipe Synthea.Stage.S3_providers_pipe auto_ingest=true as
  copy into Synthea.Stage.Providers
  from @Synthea.Stage.S3_stage_providers;
  
create or replace pipe Synthea.Stage.S3_encounters_pipe auto_ingest=true as
  copy into Synthea.Stage.Encounters
  from @Synthea.Stage.S3_stage_encounters;
  
create or replace pipe Synthea.Stage.S3_claims_pipe auto_ingest=true as
  copy into Synthea.Stage.Claims
  from @Synthea.Stage.S3_stage_claims;

create or replace pipe Synthea.Stage.S3_claimstransactions_pipe auto_ingest=true as
  copy into Synthea.Stage.ClaimsTransactions
  from @Synthea.Stage.S3_stage_claimstransactions;

create or replace pipe Synthea.Stage.S3_payers_pipe auto_ingest=true as
  copy into Synthea.Stage.payers
  from @Synthea.Stage.S3_stage_payers;

--6.2)  Configure Snowpipe User Permissions

-- Create Role
use role securityadmin;
create or replace role S3_role;
 
-- Grant Object Access and Insert Permission
grant usage on database Synthea to role S3_role;
grant usage on schema Synthea.STAGE to role S3_role;
grant insert, select on Synthea.Stage.Patients to role S3_role;
grant insert, select on Synthea.Stage.Providers to role S3_role;
grant insert, select on Synthea.Stage.Encounters to role S3_role;
grant insert, select on Synthea.Stage.Claims to role S3_role;
grant insert, select on Synthea.Stage.ClaimsTransactions to role S3_role;
grant insert, select on Synthea.Stage.Payers to role S3_role;


grant usage on stage Synthea.Stage.S3_stage_patients to role S3_role;
grant usage on stage Synthea.Stage.S3_stage_providers to role S3_role;
grant usage on stage Synthea.Stage.S3_stage_encounters to role S3_role;
grant usage on stage Synthea.Stage.S3_stage_claims to role S3_role;
grant usage on stage Synthea.Stage.S3_STAGE_CLAIMSTRANSACTIONS to role S3_role;
grant usage on stage Synthea.Stage.S3_stage_payers to role S3_role;



grant usage on warehouse compute_wh to role s3_role;
show grants to role s3_role;

ALTER PIPE Synthea.Stage.S3_PATIENTS_PIPE SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE Synthea.Stage.S3_PROVIDERS_PIPE SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE Synthea.Stage.S3_ENCOUNTERS_PIPE SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE Synthea.Stage.S3_CLAIMS_PIPE SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE Synthea.Stage.S3_CLAIMSTRANSACTIONS_PIPE SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE Synthea.Stage.S3_PAYERS_PIPE SET PIPE_EXECUTION_PAUSED=true;

-- Bestow S3_pipe Ownership
grant ownership on pipe Synthea.Stage.S3_patients_pipe to role S3_role;
grant ownership on pipe Synthea.Stage.S3_providers_pipe to role S3_role;
grant ownership on pipe Synthea.Stage.S3_encounters_pipe to role S3_role;
grant ownership on pipe Synthea.Stage.S3_claims_pipe to role S3_role;
grant ownership on pipe Synthea.Stage.S3_claimstransactions_pipe to role S3_role;
grant ownership on pipe Synthea.Stage.S3_PAYERS_PIPE to role S3_role;


-- Grant S3_role and Set as Default
grant role s3_role to user —;
alter user s01134601 set default_role = S3_role;
 

--** 7. Manage and Remove Pipes **
ALTER PIPE Synthea.Stage.S3_PATIENTS_PIPE SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE Synthea.Stage.S3_PROVIDERS_PIPE SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE Synthea.Stage.S3_ENCOUNTERS_PIPE SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE Synthea.Stage.S3_CLAIMS_PIPE SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE Synthea.Stage.S3_CLAIMSTRANSACTIONS_PIPE SET PIPE_EXECUTION_PAUSED=false;
ALTER PIPE Synthea.Stage.S3_PAYERS_PIPE SET PIPE_EXECUTION_PAUSED=false;

SELECT SYSTEM$PIPE_FORCE_RESUME('S3_PATIENTS_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('S3_Providers_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('S3_Encounters_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('S3_CLAIMS_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('S3_CLAIMSTRANSACTIONS_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('S3_PAYERS_PIPE');
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_PATIENTS_PIPE' );
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_Providers_PIPE' );
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_Encounters_PIPE' );
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_CLAIMS_PIPE' );
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_CLAIMSTRANSACTIONS_PIPE' );
SELECT SYSTEM$PIPE_STATUS( 'Synthea.Stage.S3_PAYERS_PIPE' );


-- Drop Pipe
drop pipe Synthea.Stage.S3_PATIENTS_PIPE;
drop pipe Synthea.Stage.S3_PROVIDERS_PIPE;
drop pipe Synthea.Stage.S3_ENCOUNTERS_PIPE;

-- Show Pipe
show pipes;

create or replace table snomed_ref (
SNOMED varchar(1000), 
DESCRIPTION varchar(10000)
); 
drop table snowmed_ref;

DELETE FROM SNOMED_REF;
