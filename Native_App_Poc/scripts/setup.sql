-- Setup script for the Hello Snowflake! application.

CREATE APPLICATION ROLE app_native;
CREATE SCHEMA IF NOT EXISTS native_app_poc;
GRANT USAGE ON SCHEMA native_app_poc TO APPLICATION ROLE app_native;

CREATE OR REPLACE PROCEDURE native_app_poc.HELLO()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
  AS
  BEGIN
    RETURN 'Hello native_app_poc test';
  END;


GRANT USAGE ON PROCEDURE native_app_poc.HELLO() TO APPLICATION ROLE app_native;