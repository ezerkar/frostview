import streamlit as st
from frostview.core import *
from frostview.input_boxes import *
from frostview.config import *
from frostview.column_tests import *
from frostview.create_tasks_proc import *
from snowflake.snowpark import Session

test_run_functions = {
    "not_null": run_not_null_test,
    "unique": run_unique_test,
}

@st.cache_data(show_spinner=False)
def ensure_models_exist(_session):
    create_test_table(_session)
    create_log_table(_session)
    create_config_table(_session)
    create_test_definitions_table(_session)
    _session.sql("CREATE SCHEMA IF NOT EXISTS FROSTVIEW.TEST_TASKS").collect()
    _session.sql("CREATE TABLE IF NOT EXISTS FROSTVIEW.SYSTEM_TABLES.ALERT_EMAILS (email STRING PRIMARY KEY)").collect()
    for func in test_run_functions.values():
        q = generate_snowflake_proc_from_func_with_deps(func)
        _session.sql(q).collect()
    create_tasks_proc(_session)
    create_sync_test_tasks_scheduler(_session)
    create_alert_task_scheduler(_session)
    
session = Session.builder.getOrCreate()
ensure_models_exist(session)
test_definitions = load_test_definitions(session)

st.title("FrostView: Table Validator")
table_name = input_table_name(session)
if table_name:
    try:
        table_columns = session.table(table_name).columns
        db, schema, table = table_name.split(".")
        active_config = get_active_tests(session, db, schema, table)
        column_tests_buttons(
            session, table_columns, active_config,
            db, schema, table, test_definitions,
            test_run_functions
        )
    except Exception as e:
        st.error(f"Could not load table info: {e}")
else:
    st.info("Please enter a valid table name to continue.")

st.subheader("📧 Get Notified About Test Failures")
email = st.text_input("Enter your email address to receive daily alerts:")

if email:
    try:
        insert_to_email_table(session, email)
        st.success("You’ll now receive daily alerts for any tests failed in the last 24 hours.")
    except Exception as e:
        st.error(f"Could not save your email: {e}")

