# ❄️ FrostView

**FrostView** is a lightweight, fully Snowflake-native open-source data observability framework.  
No external agents. No Spark. No Docker. Just pure SQL, Snowflake Tasks, and optional Python for anomaly detection.

## ✨ Features
- Scheduled data quality checks using Snowflake Tasks
- Logging of issues in a centralized `frostview_logs` table
- Optional Streamlit dashboard for visualization
- Python stored procedures using Snowpark and sklearn

## 📦 Stack
- Snowflake SQL
- Python (Snowpark 3.10)
- Tasks + Streams (optional)
- Streamlit (optional UI)

## 🛠️ Setup
1. Run `sql/create_log_table.sql`
2. Deploy `procedures/run_checks.py`
3. Schedule with `sql/create_check_task.sql`
4. (Optional) Launch `streamlit_ui/dashboard.py`

## 📄 License
MIT
