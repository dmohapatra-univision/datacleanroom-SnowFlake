CREATE SCHEMA IF NOT EXISTS shared_data;
CREATE TABLE IF NOT EXISTS accounts (ID INT, NAME VARCHAR, VALUE VARCHAR);
INSERT INTO accounts VALUES
  (1, 'Nihar', 'Snowflake'),
  (2, 'Frank', 'Snowflake'),
  (3, 'Benoit', 'Snowflake'),
  (4, 'Steven', 'Acme');

GRANT USAGE ON SCHEMA shared_data TO SHARE IN APPLICATION PACKAGE native_app_poc;
GRANT SELECT ON TABLE accounts TO SHARE IN APPLICATION PACKAGE native_app_poc;  

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_native;

CREATE VIEW IF NOT EXISTS code_schema.accounts_view
  AS SELECT ID, NAME, VALUE
  FROM shared_data.accounts;
GRANT SELECT ON VIEW code_schema.accounts_view TO APPLICATION ROLE app_native;