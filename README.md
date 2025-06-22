# FrostView

**Snowflake-native data quality & observability with zero external infrastructure.**  
_FrostView lets you define, run, and schedule data tests (uniqueness, not-null, and more to come) — all from inside your Snowflake account, 
 with a visual UI and no setup headaches._

---

## 🚀 Features

- **All-in-Snowflake:** No external infra, no SaaS, everything runs as Snowflake Python procs and tasks.
- **Streamlit UI:** Manage and run tests directly from Snowsight.
- **Built-in checks:** Uniqueness, not-null, and more to come.
- **Scheduling:** Easily schedule tests to run automatically every 24h (or ad hoc).
---

## ⚡ Quickstart 

### 1. **Create a Free Snowflake Account (if needed)**

Sign up at [Snowflake Trial](https://signup.snowflake.com/) if you don’t have one.

### 2. **Log in to Snowsight**

Go to your Snowflake web UI (Snowsight).

### 3. **Set Up the FrostView Database**

In Snowsight Worksheets, run:

```sql
CREATE DATABASE IF NOT EXISTS FROSTVIEW;
CREATE SCHEMA IF NOT EXISTS FROSTVIEW.PUBLIC;
```

### 4. **SET UP GITHUB INTEGRATION (ONE-TIME, SYSADMIN ONLY)**

Only needed once per Snowflake account.

If already created, skip this section.

```sql
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION
  API_PROVIDER = GITHUB
  ENABLED = TRUE;
```

### 5. **CLONE FROSTVIEW FROM GITHUB**
This step is done in Snowsight UI:
1. Go to Projects (or Develop > Projects) in the sidebar.
2. Click "Import from GitHub" or "Git Integration".
3. Connect to GitHub if prompted.
4. Select or search for ezerkar/frostview, and import it.
5. The FrostView code will appear in your Projects list.

### 6. **LAUNCH THE FROSTVIEW STREAMLIT APP**
-- No SQL command, do this in Snowsight UI:
--   1. Go to Projects and open the imported frostview project.
--   2. Open frostview/streamlit/app.py (or the main Streamlit app file).
--   3. Click "Run" (the ▶️ button) to launch the UI.

### 7. **RUN YOUR FIRST TEST!***
-- In the Streamlit app:
--   1. Enter your table (format: DB.SCHEMA.TABLE, you can use the example given) in the input box.
--   2. Click on a column to run or schedule a test.
--   3. Results/status will update live in the UI.

-- Done! FrostView is now live in your account.

