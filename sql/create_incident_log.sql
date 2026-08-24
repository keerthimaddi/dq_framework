-- ============================================================
-- REQUIREMENT 02 - STEP 1
-- Raw operational incident log (Bronze-level source for KPI/ML layer)
-- Adjust catalog/schema to match your environment before running.
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

-- ============================================================
-- Sample rows straight from Requirement 02 (for smoke-testing
-- kpi_metrics.py before real incident data is flowing in).
-- Delete/replace once real logs are ingested.
-- ============================================================
INSERT INTO wmg.dqx_audit.dq_incident_log VALUES
('2026-08-07','INC-101','API DV360 failed','DV360',1,12500.00,
 'Code Failure / Schema Shift','Automated',
 '2026-08-07 08:00:00','2026-08-07 08:05:00','2026-08-07 08:15:00','2026-08-07 09:30:00'),

('2026-08-07','INC-102','Null values in customer_id','Silver_Cust',0,0.00,
 'Data Quality Breach','Automated',
 '2026-08-07 09:30:00','2026-08-07 09:32:00','2026-08-07 09:40:00','2026-08-07 10:10:00'),

('2026-08-08','INC-103','Sigma Dash did not load','DV360',0,0.00,
 'Network / Timeout','Manual',
 '2026-08-08 10:00:00','2026-08-08 11:30:00','2026-08-08 11:45:00','2026-08-08 14:00:00'),

('2026-08-08','INC-104','Duplicate Transaction Keys','Gold_Orders',1,35000.00,
 'Duplicate Check Failure','Automated',
 '2026-08-08 14:00:00','2026-08-08 14:02:00','2026-08-08 14:10:00','2026-08-08 15:00:00'),

('2026-08-09','INC-105','Latency SLA breach on pipeline','Ingest_Stream',1,8200.00,
 'Resource Contention','Automated',
 '2026-08-09 01:00:00','2026-08-09 01:45:00','2026-08-09 02:00:00','2026-08-09 04:30:00'),

('2026-08-09','INC-106','Format Mismatch in Age Column','Bronze_Raw',0,0.00,
 'Type Conversion Error','Automated',
 '2026-08-09 06:00:00','2026-08-09 06:01:00','2026-08-09 06:10:00','2026-08-09 06:40:00');