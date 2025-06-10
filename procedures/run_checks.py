CREATE OR REPLACE PROCEDURE frostview_run_checks()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
import pandas as pd

def run(session: Session) -> str:
    logs = []
    threshold = 10000

    result = session.sql("SELECT COUNT(*) AS count FROM my_table").collect()
    count = result[0]['COUNT']
    passed = count >= threshold

    logs.append(("row_count_check", "my_table", "TABLE", "volume", "PASS" if passed else "FAIL", count, threshold))

    df = pd.DataFrame(logs, columns=[
        "check_name", "object_name", "object_type", "check_type", "check_result", "check_value", "threshold"
    ])
    session.write_pandas(df, "frostview_logs")
    return f"{len(logs)} checks run"
$$;
