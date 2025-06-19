import streamlit as st
from frostview.core import *
from frostview.input_boxes import *
from frostview.config import *
from frostview.column_tests import *
from snowflake.snowpark import Session

@st.cache_data(show_spinner=False)
def ensure_models_exist(_session):
    create_test_table(_session)
    create_log_table(_session)
    create_config_table(_session)
    create_test_definitions_table(_session)
    create_config_table_stream(_session)
    create_tasks_schema(_session)

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
        test_run_functions = {
            "not_null": run_not_null_test,
            "unique": run_unique_test,
        }
        column_tests_buttons(
            session, table_columns, active_config,
            db, schema, table, test_definitions,
            test_run_functions
        )
    except Exception as e:
        st.error(f"Could not load table info: {e}")
else:
    st.info("Please enter a valid table name to continue.")
