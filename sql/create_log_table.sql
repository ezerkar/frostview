CREATE TABLE IF NOT EXISTS frostview_logs (
    check_name STRING,
    object_name STRING,
    object_type STRING,
    check_type STRING,
    check_result STRING,
    check_value FLOAT,
    threshold FLOAT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
