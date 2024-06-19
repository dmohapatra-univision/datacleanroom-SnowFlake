GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE accountadmin;

CREATE APPLICATION PACKAGE native_app_poc;

SHOW APPLICATION PACKAGES;

USE APPLICATION PACKAGE native_app_poc;

CREATE SCHEMA stage_content;

CREATE OR REPLACE STAGE native_app_poc.stage_content.native_app_poc_stage
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1);


LIST @NATIVE_APP_POC.stage_content.NATIVE_APP_POC_stage;

USE WAREHOUSE APP_WH;

CREATE APPLICATION NATIVE_APP_POC_TEST   
  FROM APPLICATION PACKAGE NATIVE_APP_POC
  USING '@native_app_poc.stage_content.native_app_poc_stage';

SHOW APPLICATIONS;  

call NATIVE_APP_POC_TEST.NATIVE_APP_POC.HELLO();

----ADD Data
USE APPLICATION PACKAGE NATIVE_APP_POC;

CREATE or replace SCHEMA shared_data;

---GRANT USAGE ON SCHEMA shared_data TO APPLICATION ROLE app_native;

CREATE or replace TABLE shared_data.accounts (ID INT, NAME VARCHAR, VALUE VARCHAR);

INSERT INTO shared_data.accounts VALUES
  (1, 'Nihar', 'Snowflake'),
  (2, 'Frank', 'Snowflake'),
  (3, 'Benoit', 'Snowflake'),
  (4, 'Steven', 'Acme');


GRANT USAGE ON SCHEMA shared_data TO SHARE IN APPLICATION PACKAGE native_app_poc;
GRANT SELECT ON TABLE shared_data.accounts TO SHARE IN APPLICATION PACKAGE native_app_poc; 

SELECT * FROM shared_data.accounts;


SELECT code_schema.multiply(1,2);


ALTER APPLICATION PACKAGE native_app_poc
  ADD VERSION v1_0 USING '@native_app_poc.stage_content.native_app_poc_stage';


SHOW VERSIONS IN APPLICATION PACKAGE native_app_poc;  


DROP APPLICATION native_app_poc_test;
CREATE APPLICATION native_app_poc_test
  FROM APPLICATION PACKAGE native_app_poc
  USING VERSION V1_0;



ALTER APPLICATION PACKAGE native_app_poc
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = v1_0
  PATCH = 0;

GRANT MANAGE RELEASES ON APPLICATION PACKAGE native_app_poc
  TO ROLE release_mgr;  