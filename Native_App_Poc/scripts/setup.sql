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

CREATE or REPLACE FUNCTION native_app_poc.multiply(num1 float, num2 float)
  RETURNS float
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.8
  IMPORTS = ('/python/hello_python.py')
  HANDLER='hello_python.multiply';

GRANT USAGE ON FUNCTION native_app_poc.multiply(FLOAT, FLOAT) TO APPLICATION ROLE app_native;

EXECUTE IMMEDIATE FROM './data.sql';    