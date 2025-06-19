from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType

def create_test_table(session):
    session.sql("CREATE DATABASE IF NOT EXISTS FROSTVIEW").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS FROSTVIEW.PUBLIC").collect()

    data = [
        (1, 'a@example.com', 30,      'New York'),
        (2, 'b@example.com', 25,      'London'),
        (3, 'c@example.com', None,    'Tel Aviv'),
        (4, None,             40,     'Paris'),
        (5, None,             None,   'Tokyo'),
    ]
    schema = StructType([
        StructField("id",    IntegerType(), False),
        StructField("email", StringType(),  True),
        StructField("age",   IntegerType(), True),
        StructField("city",  StringType(),  False),
    ])
    df = session.create_dataframe(data, schema)
    df.write \
      .mode("overwrite") \
      .save_as_table("frostview.public.sample_table")

def create_log_table(session):
    session.sql("CREATE SCHEMA IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES").collect()
    q = \
    """
    CREATE TABLE IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES.FROSTVIEW_LOG (
        run_id STRING,
        run_time TIMESTAMP,
        database_name STRING,
        schema_name STRING,
        table_name STRING,
        column_name STRING,
        test_type STRING, 
        passed BOOLEAN,
        num_failed INT,
        error_message STRING
    );
    """
    session.sql(q).collect()

def create_config_table(session):
    q = \
    """
    CREATE TABLE IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG (
        database_name STRING,
        schema_name STRING,
        table_name STRING,
        column_name STRING,
        test_type STRING,             -- 'not_null' or 'unique'
        schedule_enabled BOOLEAN,     -- TRUE if scheduled
        schedule_cron STRING,         -- Cron expression, e.g. '24h'
        triggered_by STRING,          -- For future use: e.g. 'task:data_ingest_x'
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    session.sql(q).collect()

def create_config_table_stream(session):
    q = \
    """
    CREATE OR REPLACE STREAM FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG_STREAM
    ON TABLE FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG;
    """
    session.sql(q).collect()

def create_test_definitions_table(session):
    q = f"""
    CREATE TABLE IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES.TEST_DEFINITIONS (
        TEST_NAME STRING PRIMARY KEY,
        DISPLAY_NAME STRING,
        ACTIVE BOOLEAN DEFAULT TRUE
    );
    """
    session.sql(q).collect()

    q = \
    """
    INSERT INTO FROSTVIEW.SYSTEM_TABLES.TEST_DEFINITIONS (TEST_NAME, DISPLAY_NAME, ACTIVE)
    SELECT COLUMN1, COLUMN2, COLUMN3 FROM VALUES
      ('not_null', 'NOT NULL', TRUE),
      ('unique', 'UNIQUE', TRUE)
    AS v(COLUMN1, COLUMN2, COLUMN3)
    WHERE COLUMN1 NOT IN (SELECT TEST_NAME FROM FROSTVIEW.SYSTEM_TABLES.TEST_DEFINITIONS);
    """
    session.sql(q).collect()


def create_tasks_schema(session):
    session.sql("CREATE SCHEMA IF NOT EXISTS FROSTVIEW.TEST_TASKS").collect()


