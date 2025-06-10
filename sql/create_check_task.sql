CREATE OR REPLACE TASK frostview_runner
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '15 MINUTE'
AS
  CALL frostview_run_checks();
