# FrostView

**Snowflake-native data quality & observability with zero external infrastructure.**  
_FrostView lets you define, run, and schedule data tests (uniqueness, not-null, and more to come) — all from inside your Snowflake account, 
 with a visual UI and no setup headaches._

---

## 🚀 Features

- **All-in-Snowflake:** No external infra, no SaaS, everything runs in Snowflake.
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
create or replace api integration github_integration
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com')
    enabled = true;
```

### 5. **CLONE FROSTVIEW FROM GITHUB**

1. Go to Projects-> Streamlit (left side bar).
2. Drop-down -> create from repository (upper right corner).
   ![Screenshot from 2025-06-22 19-00-00](https://github.com/user-attachments/assets/6cbcfb29-6a37-4c43-acb9-1aa5670f6253)
4. Select main File -> Create Git Repository.
   ![Screenshot from 2025-06-22 19-01-12](https://github.com/user-attachments/assets/9d71ea3d-4c75-47f7-bf97-70699f516920)
6. click Create

### 6. **LAUNCH THE FROSTVIEW STREAMLIT APP**
click Run (upeer right corner)

### 7. **RUN YOUR FIRST TEST!**
In the Streamlit app:
1. Enter your table (format: DB.SCHEMA.TABLE, you can use the example given) in the input box.
2. Click on a column to run or schedule a test.
3. Results/status will update live in the UI.

Done! FrostView is now live in your account.
An hourly process will create tasks from from scheduled tests

