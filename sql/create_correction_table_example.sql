-- ============================================================
-- CORRECTION TABLE PATTERN (Requirement 01 Section 17)
-- One of these per table you want re-ingestion enabled for.
-- Naming convention (must match, reingest.py builds this exact
-- name): <catalog>.<audit_schema>.dq_corrections_<schema>_<table>
--
-- Schema = same columns as the SOURCE table, plus correction_status.
-- A data steward inspects wmg.dqx_quarantine.quarantine_customers_100,
-- writes CORRECTED values (not the original bad ones) into this
-- table with correction_status = 'PENDING'. reingest.py picks up
-- PENDING rows, re-validates them against the live DQ rules, and
-- merges the ones that now pass into Silver.
--
-- EXAMPLE for wmg.default.customers_100 - adjust columns to match
-- your actual source table.
-- ============================================================

CREATE TABLE IF NOT EXISTS wmg.dqx_audit.dq_corrections_default_customers_100 (
    -- mirror customers_100's real columns exactly here, e.g.:
    customer_id     STRING,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    -- ... remaining source columns ...
    correction_status STRING   -- 'PENDING' or 'APPLIED'
)
USING DELTA;

-- Example: steward corrects one bad row (illustrative only -
-- replace with a real corrected record from your quarantine table)
-- INSERT INTO wmg.dqx_audit.dq_corrections_default_customers_100 VALUES
-- ('CUST-042', 'Jane', 'Doe', 'jane.doe@example.com', 'PENDING');

-- To re-run a correction that failed re-validation, just update it
-- and leave correction_status = 'PENDING' - reingest.py will pick
-- it up again on the next pipeline run.