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

def generate_snowflake_proc_from_func_with_deps(
    func,
    proc_name=None,
    db="FROSTVIEW",
    schema="TEST_TASKS",
    proc_args=None,
    return_type="VARIANT"
):
    if proc_name is None:
        proc_name = func.__name__
    if proc_args is None:
        sig = inspect.signature(func)
        proc_args_sql = [
            f"{safe_arg_name_sql(k)} STRING"
            for k in list(sig.parameters.keys())
            if k != "session"
        ]
        proc_args_py = [
            safe_arg_name_py(k)
            for k in list(sig.parameters.keys())
            if k != "session"
        ]
    else:
        proc_args_sql = proc_args_py = proc_args
    # --- Compose imports ---
    imports_block = textwrap.dedent("""
    import uuid
    from datetime import datetime
    import operator
    """).strip()
    # --- Compose all function dependencies ---
    deps = get_func_dependencies(func)
    func_source = "\n\n".join([textwrap.dedent(inspect.getsource(f)) for f in deps])
    # --- Main handler ---
    main_wrapper = f"""
def main(session, {', '.join(proc_args_py)}):
    return {func.__name__}(session, {', '.join(proc_args_py)})
""".strip()
    # --- Build SQL ---
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
    # Final dedent for the entire code block:
    lines = [line.rstrip() for line in sql.splitlines()]
    return "\n".join(lines).strip()



def create_tasks_proc(session):
# this should be tasked to run every hour
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
