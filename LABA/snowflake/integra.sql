DROP STORAGE INTEGRATION my_integration;

CREATE STORAGE INTEGRATION IF NOT EXISTS my_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::206563573764:role/test_snow_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sio-materials/');

  DESC STORAGE INTEGRATION my_integration;

  
  create or replace stage my_test_stage
  STORAGE_INTEGRATION=my_integration
  URL="s3://sio-materials/";

 list @my_test_stage;

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SnowflakeTrust",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::042467473907:user/tr292000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "WK53487_SFCRole=6_fM0t5E855YX6mlB5lX16rz6L2QM="
                }
            }
        }
    ]
}
--===========================================
 CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER VARCHAR(100),
    AMOUNT NUMBER(10, 2)
);

select * from ORDERS;

CREATE OR REPLACE FILE FORMAT ORDERS_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE ORDERS_STAGE
    URL = 's3://sio-materials/orders/'
    STORAGE_INTEGRATION = MY_INTEGRATION
    FILE_FORMAT = ORDERS_CSV_FORMAT;

LIST @ORDERS_STAGE;

CREATE  OR REPLACE PIPE ORDERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO ORDERS
FROM @ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = 'ORDERS_CSV_FORMAT');

select * from ORDERS;

SHOW PIPES;  --copy notification channel to S3 event notification

SELECT SYSTEM$PIPE_STATUS('ORDERS_PIPE');

SELECT *
FROM TABLE(
    SNOWFLAKE_LEARNING_DB.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'ORDERS',
        START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    )
);



  