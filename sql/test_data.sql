-- ============================================================
-- TEST DATA ONLY - NOT PART OF THE PRODUCTION FLOW
--
-- Run this manually, only if you want to smoke-test kpi_metrics.py
-- BEFORE your real CSV is uploaded. Never run this against a table
-- that already has real incident data - it will mix test rows with
-- production rows.
--
-- To remove test rows later:
--   DELETE FROM wmg.dqx_audit.dq_incident_log WHERE incident_id LIKE 'TEST-%';
-- ============================================================

INSERT INTO wmg.dqx_audit.dq_incident_log VALUES
('2026-08-07','TEST-101','API DV360 failed','DV360',1,12500.00,
 'Code Failure / Schema Shift','Automated',
 '2026-08-07 08:00:00','2026-08-07 08:05:00','2026-08-07 08:15:00','2026-08-07 09:30:00'),

('2026-08-07','TEST-102','Null values in customer_id','Silver_Cust',0,0.00,
 'Data Quality Breach','Automated',
 '2026-08-07 09:30:00','2026-08-07 09:32:00','2026-08-07 09:40:00','2026-08-07 10:10:00'),

('2026-08-08','TEST-103','Sigma Dash did not load','DV360',0,0.00,
 'Network / Timeout','Manual',
 '2026-08-08 10:00:00','2026-08-08 11:30:00','2026-08-08 11:45:00','2026-08-08 14:00:00'),

('2026-08-08','TEST-104','Duplicate Transaction Keys','Gold_Orders',1,35000.00,
 'Duplicate Check Failure','Automated',
 '2026-08-08 14:00:00','2026-08-08 14:02:00','2026-08-08 14:10:00','2026-08-08 15:00:00'),

('2026-08-09','TEST-105','Latency SLA breach on pipeline','Ingest_Stream',1,8200.00,
 'Resource Contention','Automated',
 '2026-08-09 01:00:00','2026-08-09 01:45:00','2026-08-09 02:00:00','2026-08-09 04:30:00'),

('2026-08-09','TEST-106','Format Mismatch in Age Column','Bronze_Raw',0,0.00,
 'Type Conversion Error','Automated',
 '2026-08-09 06:00:00','2026-08-09 06:01:00','2026-08-09 06:10:00','2026-08-09 06:40:00');