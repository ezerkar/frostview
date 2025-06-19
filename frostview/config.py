from datetime import datetime

def get_active_tests(session, db, schema, table):
    query = f"""
        SELECT column_name, test_type
        FROM frostview.system_tables.test_config
        WHERE database_name = '{db}'
          AND schema_name = '{schema}'
          AND table_name = '{table}'
    """
    rows = session.sql(query).collect()

    config = {}
    for row in rows:
        col = row["COLUMN_NAME"]
        test = row["TEST_TYPE"]
        config.setdefault(col, {})[test] = True

    return config


def add_test_to_config(session, db, schema, table, column, test_type):
    now = datetime.now()
    session.sql(f"""
        INSERT INTO frostview.system_tables.test_config (
            database_name, schema_name, table_name,
            column_name, test_type,
            schedule_enabled, schedule_cron,
            triggered_by, created_at, updated_at
        )
        VALUES (
            '{db}', '{schema}', '{table}',
            '{column}', '{test_type}',
            TRUE, '24h', NULL,
            '{now}', '{now}'
        )
    """).collect()


def remove_test_from_config(session, db, schema, table, column, test_type):
    session.sql(f"""
        DELETE FROM frostview.system_tables.test_config
        WHERE database_name = '{db}'
          AND schema_name = '{schema}'
          AND table_name = '{table}'
          AND column_name = '{column}'
          AND test_type = '{test_type}'
    """).collect()


def load_test_definitions(session):
    q = \
    """
    SELECT TEST_NAME, DISPLAY_NAME 
    FROM FROSTVIEW.SYSTEM_TABLES.TEST_DEFINITIONS 
    WHERE ACTIVE = TRUE
    """
    rows = session.sql(q).collect()
    return [{"name": row["TEST_NAME"], "display_name": row["DISPLAY_NAME"]} for row in rows]

