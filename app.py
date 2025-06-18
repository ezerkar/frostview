import streamlit as st
from frostview.core import run_not_null_test, run_unique_test
from frostview.config import get_active_tests, add_test_to_config, remove_test_from_config
from snowflake.snowpark import Session

def create_session():
    return Session.builder.configs({
        "account": st.secrets["snowflake_account"],
        "user": st.secrets["snowflake_user"],
        "password": st.secrets["snowflake_password"],
        "role": st.secrets["snowflake_role"],
        "warehouse": st.secrets["snowflake_warehouse"],
        "database": st.secrets["snowflake_database"],
        "schema": st.secrets["snowflake_schema"]
    }).create()

session = create_session()

st.title("FrostView: Table Validator")

with st.form("table_form"):
    user_input = st.text_input("Enter full table name (format: DB.SCHEMA.TABLE):")
    submitted = st.form_submit_button("Submit")

if not submitted:
    st.stop()

try:
    session.sql(f"SELECT * FROM {user_input.strip()} LIMIT 1").collect()
    st.success(f"Table '{user_input.strip()}' exists and is accessible.")
except Exception as e:
    st.error(f"Table access failed: {e}")
    st.stop()

db, schema, table = user_input.strip().split(".")
columns = session.table(user_input.strip()).columns
active_config = get_active_tests(session, db, schema, table)

for col in columns:
    st.markdown(f"---\n#### Column: `{col}`")
    col_config = active_config.get(col, {})
    is_nn_scheduled = col_config.get("not_null", False)
    is_uq_scheduled = col_config.get("unique", False)

    c1, c2, c3 = st.columns([1, 1, 1.5])
    with c1:
        st.markdown("**NOT NULL**")
    with c2:
        if st.button("▶️ Run now", key=f"run_nn_{col}"):
            passed, log = run_not_null_test(session, db, schema, table, col)
            st.success("✅ Passed" if passed else "❌ Failed")
            st.dataframe(log)
    with c3:
        if not is_nn_scheduled:
            if st.button("📅 Schedule 24h", key=f"sched_nn_{col}"):
                add_test_to_config(session, db, schema, table, col, "not_null")
                st.success("Scheduled every 24h")
        else:
            if st.button("❌ Unschedule", key=f"unsched_nn_{col}"):
                remove_test_from_config(session, db, schema, table, col, "not_null")
                st.warning("Unscheduled")

    c4, c5, c6 = st.columns([1, 1, 1.5])
    with c4:
        st.markdown("**UNIQUE**")
    with c5:
        if st.button("▶️ Run now", key=f"run_uq_{col}"):
            passed, log = run_unique_test(session, db, schema, table, col)
            st.success("✅ Passed" if passed else "❌ Failed")
            st.dataframe(log)
    with c6:
        if not is_uq_scheduled:
            if st.button("📅 Schedule 24h", key=f"sched_uq_{col}"):
                add_test_to_config(session, db, schema, table, col, "unique")
                st.success("Scheduled every 24h")
        else:
            if st.button("❌ Unschedule", key=f"unsched_uq_{col}"):
                remove_test_from_config(session, db, schema, table, col, "unique")
                st.warning("Unscheduled")