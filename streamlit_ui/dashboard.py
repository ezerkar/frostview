import streamlit as st
import pandas as pd
import snowflake.connector

st.title("FrostView: Observability Dashboard")

conn = snowflake.connector.connect(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database="MY_DB",
    schema="PUBLIC"
)

query = "SELECT * FROM frostview_logs ORDER BY created_at DESC LIMIT 100"
df = pd.read_sql(query, conn)
st.dataframe(df)
