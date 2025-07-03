import inspect
import textwrap
from types import FunctionType

def safe_arg_name_sql(name):
    if name.upper() in {"TABLE", "COLUMN", "SCHEMA", "DB"}:
        return f'"{name.upper()}"'
    return name.upper()

def safe_arg_name_py(name):
    return name.lower()

def get_func_dependencies(func, globalns=None):
    import ast
    if globalns is None:
        globalns = func.__globals__
    seen = set()
    order = []
    def visit(f):
        if f in seen:
            return
        seen.add(f)
        tree = ast.parse(textwrap.dedent(inspect.getsource(f)))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                name = node.func.id
                called = globalns.get(name)
                if isinstance(called, FunctionType):
                    visit(called)
        order.append(f)
    visit(func)
    return list(reversed(order))


import inspect
import textwrap

def generate_snowflake_proc_from_func_with_deps(
    func,
    proc_name=None,
    db="FROSTVIEW",
    schema="TEST_TASKS",
    proc_args=None,
    return_type="STRING"
):
    if proc_name is None:
        proc_name = func.__name__

    if proc_args is None:
        sig = inspect.signature(func)
        proc_args_sql = [
            f'"{k.upper()}" STRING'
            for k in list(sig.parameters.keys())
            if k != "session"
        ]
        proc_args_py = [
            k for k in list(sig.parameters.keys())
            if k != "session"
        ]
    else:
        proc_args_sql = proc_args_py = proc_args

    imports_block = textwrap.dedent("""
    import uuid
    from datetime import datetime
    import operator
    """).strip()

    deps = get_func_dependencies(func)  # Should return a list of all needed functions
    func_source = "\n\n".join([
        textwrap.dedent(inspect.getsource(f)) for f in deps
    ])

    main_args = ", ".join(proc_args_py)
    main_wrapper = f"""
    def main(session, {main_args}):
        return {func.__name__}(session, {main_args})
    """.strip()

    sql = f"""
    CREATE OR REPLACE PROCEDURE {db}.{schema}.{proc_name}(
        {', '.join(proc_args_sql)}
    )
RETURNS {return_type}
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
{imports_block}

{func_source}

{main_wrapper}
$$;
"""
    return "\n".join(line.rstrip() for line in sql.splitlines()).strip()


def create_tasks_proc(session):
    q = \
'''
CREATE OR REPLACE PROCEDURE FROSTVIEW.TEST_TASKS.SYNC_TEST_TASKS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$
import re
def make_task_name(row):
    safe = lambda s: re.sub(r'[^a-zA-Z0-9_]', '_', s)
    return (
        f"{row['TEST_TYPE']}_"
        f"{safe(row['DATABASE_NAME'])}_"
        f"{safe(row['SCHEMA_NAME'])}_"
        f"{safe(row['TABLE_NAME'])}_"
        f"{safe(row['COLUMN_NAME'])}"
    ).lower()

def main(session):
    config_rows = session.sql(
        """SELECT database_name, schema_name, table_name, column_name, test_type 
         FROM FROSTVIEW.SYSTEM_TABLES.TEST_CONFIG where schedule_enabled;"""
    ).collect()

        
    desired_tasks = {make_task_name(r): r for r in config_rows}

    task_rows = session.sql("SHOW TASKS IN SCHEMA FROSTVIEW.TEST_TASKS").collect()
    current_tasks = {row['name'].lower() for row in task_rows}

    for task_name, row in desired_tasks.items():
        if task_name not in current_tasks:
            create_task_q = f"""
                CREATE OR REPLACE TASK FROSTVIEW.TEST_TASKS.{task_name}
                SCHEDULE = '24 hours'
                AS
                CALL FROSTVIEW.TEST_TASKS.RUN_{row['TEST_TYPE']}_TEST(
                    '{row['DATABASE_NAME']}', '{row['SCHEMA_NAME']}', 
                    '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}'
                );"""
            session.sql(create_task_q).collect()
            session.sql(f"ALTER TASK FROSTVIEW.TEST_TASKS.{task_name} RESUME").collect()

    for task_name in current_tasks:
        if task_name not in desired_tasks:
            session.sql(f"DROP TASK IF EXISTS FROSTVIEW.TEST_TASKS.{task_name}").collect()
$$;
'''
    session.sql(q).collect()


def create_sync_test_tasks_scheduler(session):
    """
    Create or replace a serverless task that runs the SYNC_TEST_TASKS proc every hour.
    """
    sql = """
    CREATE OR REPLACE TASK FROSTVIEW.TEST_TASKS.SYNC_TEST_TASKS_SCHEDULER
        SCHEDULE = '1 HOUR'
        ALLOW_OVERLAPPING_EXECUTION = FALSE
    AS
        CALL FROSTVIEW.TEST_TASKS.SYNC_TEST_TASKS();
    """
    session.sql(sql).collect()
    session.sql("ALTER TASK FROSTVIEW.TEST_TASKS.SYNC_TEST_TASKS_SCHEDULER RESUME").collect()


def create_alert_task_scheduler(session):
    session.sql("""
CREATE OR REPLACE PROCEDURE FROSTVIEW.SYSTEM_TABLES.SEND_ALERTS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
def run(session):
    failures = session.sql(
    "SELECT DISTINCT database_name || '.' || schema_name || '.' || table_name AS table_name, "
    "column_name, test_type "
    "FROM frostview.system_tables.frostview_log "
    "WHERE passed = FALSE AND run_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())"
).collect()
    if not failures:
        return '✅ No failed tests in the last 24 hours.'

    body = '❄️ FrostView found the following test failures in the last 24 hours:\\n\\n'
    for row in failures:
        body += '- Table: ' + row['TABLE_NAME'] + ', Column: ' + row['COLUMN_NAME'] + ', Test: ' + row['TEST_TYPE'] + '\\n'

    body = body.replace("'", "''")  # escape single quotes for SQL

    send_email_template = '''
        CALL SYSTEM$SEND_EMAIL(
            'frostview_email_int',
            'email_to_replace',
            '❄️ FrostView Alert: Failed Tests Detected',
            'body_to_replace')
    '''

    recipients = session.sql('SELECT email FROM frostview.system_tables.alert_emails').collect()

    for row in recipients:
        email = row['EMAIL'].replace("'", "''")
        q = send_email_template.replace('email_to_replace', email).replace('body_to_replace', body)
        session.sql(q).collect()

    return '🔔 Alerts sent to ' + str(len(recipients)) + ' recipients.'
$$
""").collect()

    session.sql("""
CREATE TASK IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES.DAILY_ALERTS
SCHEDULE = '24 HOURS'
AS
CALL FROSTVIEW.SYSTEM_TABLES.SEND_ALERTS();
""").collect()

    session.sql("ALTER TASK FROSTVIEW.SYSTEM_TABLES.DAILY_ALERTS RESUME").collect()
