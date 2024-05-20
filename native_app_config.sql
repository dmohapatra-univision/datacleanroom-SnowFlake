GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE accountadmin;

CREATE APPLICATION PACKAGE native_app_poc;

SHOW APPLICATION PACKAGES;

USE APPLICATION PACKAGE native_app_poc;

CREATE SCHEMA stage_content;

CREATE OR REPLACE STAGE native_app_poc.stage_content.native_app_poc_stage
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1);



git config --global user.email "dmohapatra@univision.net"
git config --global user.name "dmohapatra"  


