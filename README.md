
# Campaign DQ Framework

Metadata-driven PySpark DQ framework for Databricks Unity Catalog.

Flow:
GitHub/PyCharm -> Databricks -> Metadata Discovery -> Profiling -> Bronze -> DQ Engine
-> Quarantine -> Silver -> Gold -> DQ Audit/KPIs -> Final Report

The framework supports:
- 16 DQ dimensions
- YAML configuration
- weighted PASS/WARNING/FAIL scoring
- row/column/dataset/cross-table checks
- dynamic Catalog/Schema/Table discovery
- DQ profiling and rule candidates
- Bronze/Silver/Gold
- quarantine-ready architecture
- persistent audit/summary/KPI Delta tables
- optional ML-based dynamic weighting
- Databricks execution

Important:
Business-specific rules must be approved and added to config/dq_rules.yml.
Do not invent business thresholds in Python.


# Campaign Data Quality Framework

## Project Overview
Metadata-driven, PySpark-based Data Quality framework running in Databricks on Unity Catalog `wmg`. Automatically discovers catalog/schema/tables, profiles them, derives DQ rules, runs 16 DQ checks, promotes clean data through Bronze -> Silver -> Gold, quarantines failures, and tracks operational incidents (MTTD/MTTA/MTTR/AIDR) to dynamically re-weight DQ dimensions via ML.

## Architecture
```
Catalog -> Schema -> Table Discovery
     -> Profiling -> Auto Rule Generation (+ manual YAML overrides)
     -> 16 DQ Checks -> Weighted Score -> Quality Gate
     -> Bronze -> Quarantine (if failed) / Silver (if passed) -> Gold
     -> Audit + Summary tables

Incident Log -> KPI Metrics (MTTD/MTTA/MTTR/AIDR/Severity)
     -> incident_history -> ML Stage A (per-metric importance)
     -> KPI Stage B (per-KPI dynamic weight) -> dq_kpis
     -> [PENDING: weight_resolver -> calculate_score]

Quarantine -> Correction table (steward-supplied) -> Re-validation
     -> Silver (re-ingestion)
```

## Requirements Covered
- Requirement 01: metadata-driven discovery, 16 DQ dimensions, YAML config, weighted scoring, Bronze/Silver/Gold, quarantine, audit/summary tables — **complete**
- Requirement 02: KPI metrics (MTTD/MTTA/MTTR/AIDR/severity), ML dynamic weighting — **KPI calculation and Stage A/B complete; feeding weights back into `calculate_score` is built as a standalone, tested module (`weight_resolver.py`) but not yet wired into `dq_engine.py` — see Known Limitations**
- Re-ingestion (Requirement 01 §17) — **minimal practical implementation complete** (`reingest.py`)

## Folder Structure
```
dq_framework/
├── config/
│   └── dq_rules.yml
├── src/
│   ├── main.py
│   ├── metadata_discovery.py
│   ├── profiler.py
│   ├── auto_rules.py
│   ├── rule_loader.py
│   ├── dq_engine.py
│   ├── quarantine_rules.py
│   ├── lakehouse.py
│   ├── ml_weighting.py
│   ├── kpi_metrics.py
│   ├── weight_resolver.py
│   ├── reingest.py
│   └── audit_reporting.py
├── sql/
│   ├── create_incident_log.sql
│   └── create_correction_table_example.sql
├── tests/
│   ├── test_weight_resolver.py
│   └── test_kpi_classification.py
└── README.md
```

## Configuration
All behavior is driven by `config/dq_rules.yml`: `framework` (catalog/schema names, exclusions), `dq_rules` (16 dimensions with weight/severity), `table_rules` (manual overrides merged with auto-derived rules), `ml_weighting` (incident table, feature columns, target), `kpi_metrics` (incident log source, label→KPI classification map).

## How to Deploy
1. Files go under your Databricks Workspace path, e.g. `/Workspace/Users/<you>/dq_framework/src/`.
2. Overwrite `main.py`, add `kpi_metrics.py`, `weight_resolver.py` (new), `reingest.py` (new).
3. `dq_engine.py` and `auto_rules.py` are **not modified by this delivery** — see Known Limitations.
4. Merge `config_diff.yml`'s `kpi_metrics:` block into `dq_rules.yml`, replacing the old one.
5. Run `sql/create_incident_log.sql` once to create/seed the incident log (skip if already done).

## How to Run
Execute `main.py` as a Databricks notebook/job. It runs end-to-end: discovery → profiling → DQ checks → scoring → gate → quarantine → Bronze/Silver/Gold → KPI metrics → ML weighting → KPI weights → incident-level report → weight resolver preview → re-ingestion → audit → final report.

## Tables Created
| Table | Purpose | Created by | First-run required? |
|---|---|---|---|
| `wmg.dqx_bronze.<table>` | Raw ingested copy | `lakehouse.write_bronze` | Yes, every table every run |
| `wmg.dqx_silver.<table>` | Quality-gated clean data | `lakehouse.create_silver` | Only if gate passes |
| `wmg.dqx_gold.<table>` | Curated/aggregated | `lakehouse.create_gold` | Only if Silver exists |
| `wmg.dqx_quarantine.quarantine_<table>` | Failed rows | `lakehouse.create_quarantine` | Only if failures exist |
| `wmg.dqx_audit.dq_audit_results` | Per-check detail history | `audit_reporting.write_audit` | Yes |
| `wmg.dqx_audit.dq_summary` | Per-table run summary | `audit_reporting.write_audit` | Yes |
| `wmg.dqx_profiling.dq_profile` | Column profiling results | `audit_reporting.write_audit` | Yes |
| `wmg.dqx_profiling.dq_rule_candidates` | Suggested new rules | `main.py` | If candidates found |
| `wmg.demo.incident_history` | KPI feature/history table | `kpi_metrics.run_kpi_metrics` | No — cold-start safe |
| `wmg.dqx_audit.dq_dynamic_weights` | Stage A per-metric importance | `ml_weighting.train_dynamic_weights` | No — needs `min_training_rows` history first |
| `wmg.dqx_audit.dq_kpis` | Stage B per-KPI weights (W1..Wn) | `kpi_metrics.compute_kpi_weights` | No — needs Stage A output first |
| `wmg.dqx_audit.dq_kpi_incident_report` | Flat whiteboard-style report | `kpi_metrics.build_incident_level_kpi_report` | No — needs `dq_kpis` first |
| `wmg.dqx_audit.dq_corrections_<schema>_<table>` | Steward-supplied corrections | Manual (steward) | No — re-ingestion no-ops without it |

## DQ Rules
16 dimensions defined in `dq_rules.yml`, each with `enabled`, `default_weight`, `severity`. Per-table rules are auto-derived from live schema (`auto_rules.py`) and merged with optional manual overrides in `table_rules`.

## Scoring
`Score = Σ(check_result × weight) / Σ(weight) × 100`, PASS=1/WARNING=0.5/FAIL=0. **Currently uses static YAML weights only** — dynamic weight consumption is built (`weight_resolver.py`) but not yet wired into `dq_engine.calculate_score` (see Known Limitations).

## KPI / ML Weighting
Raw incidents (`dq_incident_log`) → MTTD/MTTA/MTTR/AIDR/severity computed → aggregated into `incident_history` → Stage A (RandomForest feature importance on the aggregate metrics) → Stage B (per-KPI composite weight, normalized to ~100) → `dq_kpis`. KPI codes are aligned to `dq_rules.yml`'s `DQ01`-`DQ16` ids wherever a real correspondence exists.

## Quality Gate
Silver promotion blocked unless score ≥ `quality_gate.silver_min_score` (currently 90).

## Quarantine
Failing rows (per `build_failure_condition`) written to `dqx_quarantine.quarantine_<table>` with failure metadata.

## Re-ingestion
A steward supplies corrected rows in `dq_corrections_<schema>_<table>` (schema = source table + `correction_status`). `reingest.py` re-validates those rows using the **same** `build_auto_rule`/`merge_rules`/`build_failure_condition` logic already used in the main pipeline — no second validation framework — and merges passers into Silver.

## Bronze/Silver/Gold
Standard medallion architecture; Silver/Gold conditional on quality gate.

## Validation Queries
See `VALIDATION_QUERIES.sql`.

## Known Limitations
1. **Dynamic weights do not yet affect `calculate_score`.** `weight_resolver.py` is built, tested, and demonstrated in every pipeline run (prints the resolved effective weights), but integrating it requires the actual source of `dq_engine.py`, which hasn't been shared. The integration is a ~5 line change: wherever `calculate_score` reads `rule["default_weight"]`, call `weight_resolver.build_effective_weights(spark, cfg)` once and look up by `rule["id"]` instead.
2. **DQ09 over-triggering is unresolved** — needs `auto_rules.py`'s source to patch safely; a likely cause (range rules applied to low-cardinality numeric columns) is documented but not yet fixed.
3. **`auto_rules.py` is unaudited** — works per observed output, internals never reviewed.
4. **Re-ingestion is manual-correction-based**, not automated data repair — by design, given time constraints.
5. **No scheduling or dashboard** — explicitly out of scope for today's delivery per your latest instruction.
6. **`label_kpi_map` is inferred**, not confirmed against your full incident taxonomy — review before relying on KPI classification in a live demo.