
CREATE SCHEMA IF NOT EXISTS shared_data;
GRANT USAGE ON SCHEMA shared_data TO APPLICATION ROLE app_native;
CREATE TABLE IF NOT EXISTS shared_data.accounts (ID INT, NAME VARCHAR, VALUE VARCHAR);
INSERT INTO shared_data.accounts VALUES
  (1, 'Nihar', 'Snowflake'),
  (2, 'Frank', 'Snowflake'),
  (3, 'Benoit', 'Snowflake'),
  (4, 'Steven', 'Acme');

GRANT USAGE ON SCHEMA shared_data TO SHARE IN APPLICATION PACKAGE native_app_poc;
GRANT SELECT ON TABLE shared_data.accounts TO SHARE IN APPLICATION PACKAGE native_app_poc;  