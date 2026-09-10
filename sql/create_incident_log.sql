-- ============================================================
-- REQUIREMENT 02 - STEP 1
-- Raw operational incident log (source for the KPI/ML layer)
--
-- Schema definition only. Incident DATA comes from your uploaded
-- CSV, not from this file - see CSV_UPLOAD_INSTRUCTIONS.md for
-- the exact steps. This file exists only so the table has the
-- right structure if it doesn't already exist when you upload.
--
-- If you upload the CSV via Databricks' "Create table from file"
-- wizard first, Databricks will infer and create the table for
-- you automatically - in that case you don't need to run this
-- file at all, just make sure the column names/types below match
-- what the wizard inferred (rename columns in the wizard if not).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS wmg.dqx_audit;

CREATE TABLE IF NOT EXISTS wmg.dqx_audit.dq_incident_log (
    incident_date          DATE,
    incident_id            STRING,
    incident_description   STRING,
    ds                      STRING,   -- Data Source, e.g. DV360, Gold_Orders
    rev_impact_flag         INT,      -- 1 = revenue impacting, 0 = not
    rev_impact_amount       DOUBLE,
    label                    STRING,   -- Error root cause / category
    detection_type           STRING,   -- 'Automated' or 'Manual'
    event_timestamp           TIMESTAMP,
    detected_timestamp        TIMESTAMP,
    ack_timestamp              TIMESTAMP,
    resolved_timestamp          TIMESTAMP
)
USING DELTA;

-- No INSERT statements here. Load your CSV into this table using
-- one of the methods in CSV_UPLOAD_INSTRUCTIONS.md.