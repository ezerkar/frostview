## FrostView Setup Instructions

1. Run the `sql/create_log_table.sql` to create the logging table.
2. Deploy the `procedures/run_checks.py` stored procedure.
3. Create the task from `sql/create_check_task.sql`.
4. (Optional) Launch the Streamlit UI via `streamlit_ui/dashboard.py`.
