
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
