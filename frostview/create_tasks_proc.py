def create_tasks_proc(session):
# this task runs every hour 
# and if any changes detected in config creates and deletes tasks accordingly
    q = """
    CREATE PROCEDURE IF NOT EXISTS FROSTVIEW.PUBLIC.SYNC_TEST_TASKS()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    AS
    $$
    def main(session):
        import re
    
        stream_rows = session.sql(\"
            SELECT *
            FROM FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG_STREAM
        \").collect()
    
        if not stream_rows:
            return "No changes detected in config stream. Nothing to do."
    
        config_rows = session.sql(\"
            SELECT DB, SCHEMA, TABLE, COLUMN, TEST_TYPE
            FROM FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG
            WHERE SCHEDULED = TRUE
        \").collect()
    
        def make_task_name(row):
            safe = lambda s: re.sub(r'[^a-zA-Z0-9_]', '_', s)
            return f"{row['TEST_TYPE']}_{safe(row['DB'])}_{safe(row['SCHEMA'])}_{safe(row['TABLE'])}_{safe(row['COLUMN'])}".lower()
        desired_tasks = {make_task_name(r): r for r in config_rows}
    
        task_rows = session.sql(\"
            SHOW TASKS IN SCHEMA FROSTVIEW.TEST_TASKS
        \").collect()
        current_tasks = {row['name'].lower() for row in task_rows}
    
        for task_name, row in desired_tasks.items():
            if task_name not in current_tasks:
                session.sql(f\"
                    CREATE OR REPLACE TASK FROSTVIEW.TEST_TASKS.{task_name}
                        SCHEDULE = '1 DAY'
                    AS
                        CALL FROSTVIEW.PUBLIC.RUN_COLUMN_TEST(
                            '{{row['DB']}}', '{{row['SCHEMA']}}', '{{row['TABLE']}}', '{{row['COLUMN']}}', '{{row['TEST_TYPE']}}'
                        );
                \").collect()
    
        for task_name in current_tasks:
            if task_name not in desired_tasks:
                session.sql(f"DROP TASK IF EXISTS FROSTVIEW.TEST_TASKS.{task_name}").collect()
    
        session.sql("SELECT SYSTEM$STREAM_ADVANCE('FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG_STREAM', 'NEW')").collect()
        return "Test tasks synced: Created missing (serverless), dropped obsolete."
    $$;
    """
    session.sql(q).collect()
