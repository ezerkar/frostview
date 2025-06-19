import streamlit as st
from frostview.column_tests import *
from frostview.config import *


def input_table_name(session):
    # Render the form
    with st.form("table_form", clear_on_submit=False):
        user_input = st.text_input(
            "Enter full table name (i.e. frostview.public.sample_table)",
            value=st.session_state.get("table_name", "")
        )
        table_name = user_input.strip()
        submitted = st.form_submit_button("Submit")
        if submitted:
            if not table_name:
                st.error("Please enter a table name.")
                st.stop()
            try:
                session.sql(f"SELECT * FROM {table_name} LIMIT 1").collect()
                st.session_state["table_name"] = table_name
                st.success(f"Table '{table_name}' exists and is accessible.")
            except Exception as e:
                st.error(f"Table access failed: {e}")
                st.stop()
    # After successful submit, or after previous success, return the value from session
    return st.session_state.get("table_name", "")


def test_schedule_callback(session, db, schema, table, col, test_name, key, action):
    if action == "schedule":
        add_test_to_config(session, db, schema, table, col, test_name)
        st.session_state[key] = True
    else:
        remove_test_from_config(session, db, schema, table, col, test_name)
        st.session_state[key] = False

def column_tests_buttons(session, table_columns, active_config, db, schema, table, 
                         test_definitions, test_run_functions):
    for col in table_columns:
        st.markdown(f"---\n#### Column: `{col}`")
        col_config = active_config.get(col, {})
        for test in test_definitions:
            test_name = test["name"]
            display_name = test.get("display_name", test_name)
            run_func = test_run_functions[test_name]
            key = f"{test_name}_sched_{db}_{schema}_{table}_{col}"
            is_scheduled = st.session_state.get(key, col_config.get(test_name, False))
            c0, c1, c2 = st.columns([1, 1, 2]) 
            with c0:
                st.markdown(f"**{display_name}**")
            with c1:
                if st.button("▶️ Run now", key=f"run_{test_name}_{col}"):
                    passed, log = run_func(session, db, schema, table, col)
                    st.success("✅ Passed" if passed else "❌ Failed")
            with c2:
                if not is_scheduled:
                    st.button(
                        "📅 Schedule 24h",
                        key=f"sched_{test_name}_{col}",
                        on_click=test_schedule_callback,
                        args=(session, db, schema, table, col, test_name, key, "schedule"),
                    )
                else:
                    st.button(
                        "❌ Unschedule",
                        key=f"unsched_{test_name}_{col}",
                        on_click=test_schedule_callback,
                        args=(session, db, schema, table, col, test_name, key, "unschedule"),
                    )
