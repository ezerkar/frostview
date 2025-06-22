import uuid
from datetime import datetime
import pandas as pd
import operator

def generate_run_id():
    return f"fv_run_{uuid.uuid4().hex[:8]}"

def run_single_numeric_test(session, db, schema, table, column, single_numeric_query, 
                            test_name, pass_if_condition, pass_if_value):
    run_id = generate_run_id()
    run_time = datetime.now()
    try:
        result = session.sql(single_numeric_query).first()[0]
        passed = pass_if_condition(result, pass_if_value)
        error_message = None
    except Exception as e:
        result = None
        passed = None
        error_message = str(e)
    
    log = {
        "run_id": run_id,
        "run_time": run_time,
        "database_name": db,
        "schema_name": schema,
        "table_name": table,
        "column_name": column,
        "test_type": test_name,
        "passed": passed,
        "num_failed": result,
        "error_message": error_message
    }
    
    log_df = session.create_dataframe([log])
    log_df.write.save_as_table("frostview.system_tables.frostview_log", mode="append")
    return passed, log_df

def run_not_null_test(session, db, schema, table, column):
    single_numeric_query = \
    f"""
    SELECT COUNT(*) AS num_failed
    FROM {'.'.join([db, schema, table])}
    WHERE {column} IS NULL
    """
    passed, log = run_single_numeric_test(session, db, schema, table, column, single_numeric_query, 
                            'not_null_test', operator.eq, 0)
    return passed, log

def run_unique_test(session, db, schema, table, column):
    single_numeric_query = \
    f"""
    SELECT COUNT(*) AS num_failed
    FROM {'.'.join([db, schema, table])}
    WHERE {column} IS NULL
    """
    passed, log = run_single_numeric_test(session, db, schema, table, column, single_numeric_query, 
                            'unique_test', operator.eq, 0)
    return passed, log