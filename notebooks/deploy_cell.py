import pathlib

base = "/Workspace/Users/keerthi.maddi@mediamint.com/dq_framework/src"

main_content = '''
# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# MAIN PIPELINE
# ============================================================

import uuid
from datetime import datetime

from pyspark.sql import SparkSession

from src.rule_loader import load_config
from src.metadata_discovery import discover_source_tables
from src.dq_engine import (
    run_all_dq,
    calculate_score,
    overall_status,
    get_table_rule,
)

from src.profiler import (
    profile_table,
    create_rule_candidates,
)

from src.audit_reporting import (
    write_audit,
    print_final_report,
)

from src.lakehouse import (
    create_framework_schemas,
    write_bronze,
    create_silver,
    create_gold,
    create_quarantine,
)

from src.quarantine_rules import build_failure_condition

from src.ml_weighting import train_dynamic_weights

from src.auto_rules import build_auto_rule, merge_rules

from src.kpi_metrics import run_kpi_metrics, compute_kpi_weights


# ============================================================
# SPARK SESSION
# ============================================================

def get_spark_session():

    spark = (
        SparkSession
        .builder
        .appName("Campaign Data Quality Framework")
        .getOrCreate()
    )

    return spark


# ============================================================
# PRINT CONFIGURATION
# ============================================================

def print_configuration(cfg):

    framework = cfg["framework"]

    print()
    print("=" * 70)
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("=" * 70)

    print(f"Catalog           : {framework['catalog']}")
    print(f"Audit Schema      : {framework['audit_schema']}")
    print(f"Candidate Schema  : {framework['candidate_schema']}")
    print(f"Quality Gate      : {cfg['quality_gate']}")
    print(f"Overall Thresholds: {cfg['overall_thresholds']}")
    print(f"ML Weighting      : {cfg['ml_weighting']}")
    print()


# ============================================================
# PROCESS ONE TABLE
# ============================================================

def process_table(spark, cfg, catalog, schema, table):

    source = f"{catalog}.{schema}.{table}"
    run_id = str(uuid.uuid4())

    print()
    print("=" * 70)
    print(f"PROCESSING TABLE: {source}")
    print("=" * 70)

    run_timestamp = datetime.now()

    # --------------------------------------------------------
    # LOAD TABLE
    # --------------------------------------------------------
    try:
        df = spark.table(source)
    except Exception as exc:
        print(f"ERROR loading table {source}: {exc}")
        return None, [], [], []

    row_count = df.count()
    column_count = len(df.columns)

    print(f"Rows    : {row_count}")
    print(f"Columns : {column_count}")

    # --------------------------------------------------------
    # PROFILE TABLE
    # --------------------------------------------------------
    print()
    print("Running data profiling...")

    try:
        profile_rows = profile_table(df, catalog, schema, table)
        candidate_rows = create_rule_candidates(profile_rows)

        print(f"Profile rows    : {len(profile_rows)}")
        print(f"Rule candidates : {len(candidate_rows)}")

    except Exception as exc:
        print(f"Profiling error for {source}: {exc}")
        profile_rows = []
        candidate_rows = []

    # --------------------------------------------------------
    # WRITE BRONZE
    # --------------------------------------------------------
    print()
    print("Writing Bronze...")

    bronze_df, column_mapping = write_bronze(
        spark, df, catalog, schema, table, cfg
    )

    # --------------------------------------------------------
    # BUILD EFFECTIVE RULE: AUTO (metadata-derived) + MANUAL
    # (optional table_rules entry in dq_rules.yml, if present)
    # --------------------------------------------------------
    print()
    print("Deriving DQ rules from table metadata...")

    auto_rule = build_auto_rule(spark, bronze_df, catalog, schema, table, cfg)
    manual_rule = get_table_rule(cfg, catalog, schema, table)
    effective_rule = merge_rules(auto_rule, manual_rule)

    print(
        f"Auto-derived: "
        f"{len(auto_rule.get('mandatory_columns', []))} mandatory cols, "
        f"{len(auto_rule.get('unique_keys', []))} key candidate(s), "
        f"{len(auto_rule.get('range_rules', {}))} numeric range rule(s), "
        f"{len(auto_rule.get('pattern_rules', {}))} pattern rule(s)"
        + (
            " | manual overrides applied"
            if manual_rule
            else " | no manual table_rules entry (fully automatic)"
        )
    )

    # cfg is reused across tables in the discovery loop, so scope
    # the injected rule to THIS table only via a shallow copy.
    table_cfg = dict(cfg)
    table_cfg["table_rules"] = {
        **cfg.get("table_rules", {}),
        source: effective_rule,
    }

    # --------------------------------------------------------
    # RUN DQ CHECKS (against Bronze)
    # --------------------------------------------------------
    print()
    print("Running DQ01 - DQ16...")

    results, details = run_all_dq(
        spark, bronze_df, catalog, schema, table, table_cfg
    )

    print()
    print("-" * 70)
    print(f"DQ RESULTS: {source}")
    print("-" * 70)

    for detail in sorted(details, key=lambda d: d["dq_id"]):
        print(
            f"{detail['dq_id']} | "
            f"{detail['status']} | "
            f"Failure %: {detail['failure_percentage']:.2f} | "
            f"Failed: {detail['failed_records']}"
        )

    # --------------------------------------------------------
    # CALCULATE SCORE + STATUS
    # --------------------------------------------------------
    score = calculate_score(cfg, results)
    status = overall_status(score, cfg["overall_thresholds"])

    print()
    print(f"Overall Score : {score:.2f}%")
    print(f"Overall Status: {status}")

    # --------------------------------------------------------
    # QUALITY GATE
    # --------------------------------------------------------
    quality_gate = cfg["quality_gate"]

    gate_threshold = float(
        quality_gate.get("silver_min_score", 90)
    )

    gate_enabled = quality_gate.get("enabled", True)

    if not gate_enabled:
        gate_status = "DISABLED"
    elif score >= gate_threshold:
        gate_status = "PASSED"
    else:
        gate_status = "FAILED"

    print(f"Quality Gate  : {gate_status}")
    print(f"Gate Threshold: {gate_threshold:.2f}%")

    # --------------------------------------------------------
    # QUARANTINE FAILING ROWS
    # --------------------------------------------------------
    print()
    print("Evaluating rows for quarantine...")

    failure_condition = build_failure_condition(bronze_df, effective_rule)

    quarantined_count = create_quarantine(
        spark,
        bronze_df,
        failure_condition,
        catalog,
        schema,
        table,
        cfg,
        run_id,
    )

    # --------------------------------------------------------
    # SILVER (only if quality gate passed)
    # --------------------------------------------------------
    print()
    print("Evaluating Silver promotion...")

    silver_created = create_silver(
        spark, bronze_df, catalog, table, cfg, score
    )

    # --------------------------------------------------------
    # GOLD (only if Silver exists)
    # --------------------------------------------------------
    gold_created = False

    if silver_created:
        print()
        print("Evaluating Gold promotion...")

        gold_created = create_gold(
            spark, catalog, table, cfg, score, status
        )

    # --------------------------------------------------------
    # BUILD SUMMARY ROW
    # --------------------------------------------------------
    summary_row = {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "row_count": int(row_count),
        "column_count": int(column_count),
        "dq01": results.get("DQ01", "WARNING"),
        "dq02": results.get("DQ02", "WARNING"),
        "dq03": results.get("DQ03", "WARNING"),
        "dq04": results.get("DQ04", "WARNING"),
        "dq05": results.get("DQ05", "WARNING"),
        "dq06": results.get("DQ06", "WARNING"),
        "dq07": results.get("DQ07", "WARNING"),
        "dq08": results.get("DQ08", "WARNING"),
        "dq09": results.get("DQ09", "WARNING"),
        "dq10": results.get("DQ10", "WARNING"),
        "dq11": results.get("DQ11", "WARNING"),
        "dq12": results.get("DQ12", "WARNING"),
        "dq13": results.get("DQ13", "WARNING"),
        "dq14": results.get("DQ14", "WARNING"),
        "dq15": results.get("DQ15", "WARNING"),
        "dq16": results.get("DQ16", "WARNING"),
        "dq_score": float(score),
        "total_score": float(score),
        "overall_status": status,
        "quality_gate": gate_status,
        "quarantined_records": int(quarantined_count),
        "silver_created": bool(silver_created),
        "gold_created": bool(gold_created),
    }

    # --------------------------------------------------------
    # BUILD DETAIL ROWS
    # --------------------------------------------------------
    detail_rows = []

    for detail in details:
        detail_rows.append({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "dq_id": detail["dq_id"],
            "status": detail["status"],
            "failure_percentage": float(detail["failure_percentage"]),
            "failed_records": int(detail["failed_records"]),
        })

    return summary_row, detail_rows, profile_rows, candidate_rows


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("STARTING CAMPAIGN DATA QUALITY PIPELINE")
    print("=" * 70)

    # --------------------------------------------------------
    # LOAD CONFIGURATION
    # --------------------------------------------------------
    cfg = load_config()
    print_configuration(cfg)

    # --------------------------------------------------------
    # CREATE SPARK SESSION
    # --------------------------------------------------------
    spark = get_spark_session()

    # --------------------------------------------------------
    # ENSURE BRONZE/SILVER/GOLD/AUDIT/QUARANTINE/PROFILING
    # SCHEMAS EXIST
    # --------------------------------------------------------
    create_framework_schemas(spark, cfg)

    # --------------------------------------------------------
    # KPI METRICS (Requirement 02, Steps 1-3)
    # Runs BEFORE table discovery so wmg.demo.incident_history is
    # freshly merged before the DQ engine discovers and profiles/
    # checks it in the loop below, in this same run. Safe no-op
    # if the raw incident log isn't populated yet.
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("KPI METRICS")
    print("=" * 70)
    try:
        run_kpi_metrics(spark, cfg)
    except Exception as exc:
        print(f"KPI metrics step failed (non-fatal): {exc}")

    # --------------------------------------------------------
    # DISCOVER TABLES
    # --------------------------------------------------------
    tables = discover_source_tables(spark, cfg)

    if not tables:
        print()
        print("No tables found for processing.")
        spark.stop()
        return

    # --------------------------------------------------------
    # PROCESS ALL TABLES
    # --------------------------------------------------------
    summary_rows = []
    detail_rows = []
    profile_rows = []
    candidate_rows = []

    successful_tables = 0
    failed_tables = 0

    for catalog, schema, table in tables:

        try:
            result = process_table(spark, cfg, catalog, schema, table)

            if result is None or result[0] is None:
                failed_tables += 1
                continue

            summary, details, profiles, candidates = result

            if summary:
                summary_rows.append(summary)

            detail_rows.extend(details)
            profile_rows.extend(profiles)
            candidate_rows.extend(candidates)

            successful_tables += 1

        except Exception as exc:
            failed_tables += 1
            print()
            print(f"FAILED TABLE: {catalog}.{schema}.{table}")
            print(f"Reason: {exc}")

    # --------------------------------------------------------
    # WRITE AUDIT RESULTS
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("WRITING AUDIT RESULTS")
    print("=" * 70)

    try:
        write_audit(spark, cfg, summary_rows, detail_rows, profile_rows)
        print("Audit results written successfully.")
    except Exception as exc:
        print(f"Audit write failed: {exc}")

    # --------------------------------------------------------
    # WRITE RULE CANDIDATES
    # --------------------------------------------------------
    if candidate_rows:
        try:
            candidate_schema = cfg["framework"]["candidate_schema"]
            candidate_table = cfg["output_tables"].get("candidates")

            if candidate_table:
                candidate_df = spark.createDataFrame(candidate_rows)

                (
                    candidate_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(
                        f"`{cfg['framework']['catalog']}`."
                        f"`{candidate_schema}`."
                        f"`{candidate_table}`"
                    )
                )

                print("Rule candidates written successfully.")

        except Exception as exc:
            print(f"Candidate write failed: {exc}")

    # --------------------------------------------------------
    # ML DYNAMIC WEIGHTING - Stage A (per-metric importances)
    # (trains against incident_history; safe no-op if disabled
    # or if there isn't enough history yet)
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("ML DYNAMIC WEIGHTING")
    print("=" * 70)
    try:
        train_dynamic_weights(spark, cfg, cfg["framework"]["catalog"])
    except Exception as exc:
        print(f"ML weighting step failed (non-fatal): {exc}")

    # --------------------------------------------------------
    # KPI DYNAMIC WEIGHTS - Stage B (per-KPI W1..Wn, whiteboard shape)
    # Writes final report to output_tables.kpi (wmg.dqx_audit.dq_kpis)
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("KPI DYNAMIC WEIGHTS")
    print("=" * 70)
    try:
        compute_kpi_weights(spark, cfg)
    except Exception as exc:
        print(f"KPI weighting step failed (non-fatal): {exc}")

    # --------------------------------------------------------
    # FINAL REPORT
    # --------------------------------------------------------
    print_final_report(spark, cfg)

    # --------------------------------------------------------
    # PIPELINE SUMMARY
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 70)

    print(f"Tables discovered : {len(tables)}")
    print(f"Tables processed  : {successful_tables}")
    print(f"Tables failed     : {failed_tables}")
    print(f"Summary records   : {len(summary_rows)}")
    print(f"Detail records    : {len(detail_rows)}")
    print(f"Profile records   : {len(profile_rows)}")
    print(f"Rule candidates   : {len(candidate_rows)}")

    print()
    print("=" * 70)

    if failed_tables == 0:
        print("PIPELINE COMPLETED SUCCESSFULLY")
    else:
        print("PIPELINE COMPLETED WITH TABLE ERRORS")

    print("=" * 70)

    spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
'''

kpi_content = '''
# ============================================================
# KPI METRICS MODULE
# Requirement 02 - Steps 1-4
#
# IMPORTANT: this version writes into the SAME table your real
# dq_rules.yml already configures ml_weighting.incident_table to
# read from (wmg.demo.incident_history), using the EXACT column
# names already declared in that table's table_rules block:
#   incident_count, avg_mttd_minutes, avg_mtta_minutes,
#   avg_mttr_minutes, aidr_pct, severity_score, total_rev_impact
#
# You do NOT need to change ml_weighting.py's config wiring -
# it already points at the right place. This module just needs
# to actually populate that table.
#
# Pipeline position (see main_py_patch_notes.md):
#   1. run_kpi_metrics(spark, cfg)   <- run BEFORE the table
#      discovery loop, so wmg.demo.incident_history has fresh
#      data before the DQ engine profiles/checks it like any
#      other discovered source table.
#   2. ml_weighting.train_dynamic_weights(spark, cfg, catalog)
#      (unchanged) - Stage A: learns which metric (MTTD, MTTR,
#      AIDR%, ...) best predicts total_rev_impact.
#   3. compute_kpi_weights(spark, cfg)  <- run AFTER Stage A -
#      Stage B: applies Stage A's metric importances to each
#      KPI's own row, normalizes across KPIs to W1..Wn summing
#      to 100 (matches the whiteboard), writes the final report
#      to output_tables.kpi (wmg.dqx_audit.dq_kpis).
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# HELPERS
# ============================================================

def _tbl(name: str) -> str:
    """Turn 'catalog.schema.table' into `catalog`.`schema`.`table`."""
    return "`" + "`.`".join(name.split(".")) + "`"


def _minutes_between(end_col, start_col):
    return (F.col(end_col).cast("long") - F.col(start_col).cast("long")) / 60.0


def _resolve_incident_table(cfg):
    """The KPI feature/history table = wherever ml_weighting.incident_table points."""
    ml_cfg = cfg.get("ml_weighting", {})
    table = ml_cfg.get("incident_table")
    if not table:
        raise ValueError("cfg['ml_weighting']['incident_table'] is not configured.")
    return table


def _resolve_metric_weights_table(cfg):
    ml_cfg = cfg.get("ml_weighting", {})
    table = ml_cfg.get("output_table")
    if not table:
        raise ValueError("cfg['ml_weighting']['output_table'] is not configured.")
    return table


def _resolve_kpi_report_table(cfg):
    framework = cfg["framework"]
    audit_schema = framework["audit_schema"]
    kpi_table_name = cfg["output_tables"]["kpi"]
    return f"{framework['catalog']}.{audit_schema}.{kpi_table_name}"


# ============================================================
# STEP 2 - PER-INCIDENT METRICS (TTD / TTA / TTR / automated flag)
# ============================================================

def compute_incident_durations(df):
    """
    Adds row-level duration/detection columns to the raw incident log.
    Expects: event_timestamp, detected_timestamp, ack_timestamp,
             resolved_timestamp, detection_type, rev_impact_amount.
    """
    return (
        df
        .withColumn("ttd_min", _minutes_between("detected_timestamp", "event_timestamp"))
        .withColumn("tta_min", _minutes_between("ack_timestamp", "detected_timestamp"))
        .withColumn("ttr_min", _minutes_between("resolved_timestamp", "detected_timestamp"))
        .withColumn(
            "is_automated",
            F.when(F.upper(F.trim(F.col("detection_type"))) == "AUTOMATED", F.lit(1)).otherwise(F.lit(0))
        )
        # guard against bad/missing timestamps producing negative durations
        .withColumn("ttd_min", F.when(F.col("ttd_min") < 0, None).otherwise(F.col("ttd_min")))
        .withColumn("tta_min", F.when(F.col("tta_min") < 0, None).otherwise(F.col("tta_min")))
        .withColumn("ttr_min", F.when(F.col("ttr_min") < 0, None).otherwise(F.col("ttr_min")))
    )


# ============================================================
# CLASSIFY EACH INCIDENT TO A KPI (K01..K16) VIA label_kpi_map
# ============================================================

def classify_kpi(spark, df, kpi_cfg):
    label_map = kpi_cfg.get("label_kpi_map", {})
    default_kpi = kpi_cfg.get(
        "default_kpi", {"kpi": "K99", "kpi_name": "Unclassified", "dimension": "Unknown"}
    )

    rows = [
        {"label": label, "kpi": v["kpi"], "kpi_name": v["kpi_name"], "dimension": v["dimension"]}
        for label, v in label_map.items()
    ]

    if not rows:
        return (
            df
            .withColumn("kpi", F.lit(default_kpi["kpi"]))
            .withColumn("kpi_name", F.lit(default_kpi["kpi_name"]))
            .withColumn("dimension", F.lit(default_kpi["dimension"]))
        )

    map_df = spark.createDataFrame(rows)
    joined = df.join(map_df, on="label", how="left")
    joined = (
        joined
        .withColumn("kpi", F.coalesce(F.col("kpi"), F.lit(default_kpi["kpi"])))
        .withColumn("kpi_name", F.coalesce(F.col("kpi_name"), F.lit(default_kpi["kpi_name"])))
        .withColumn("dimension", F.coalesce(F.col("dimension"), F.lit(default_kpi["dimension"])))
    )
    return joined


# ============================================================
# STEP 3 - AGGREGATE TO incident_history GRAIN
# Grain = (incident_date, kpi, label) so the table accumulates
# real history over time as new days/incidents land, instead of
# collapsing everything into ~10 static rows every run.
# ============================================================

def aggregate_incident_history(df):
    grouped = (
        df.groupBy("incident_date", "kpi", "kpi_name", "dimension", "label")
        .agg(
            F.count(F.lit(1)).cast("int").alias("incident_count"),
            F.avg("ttd_min").alias("avg_mttd_minutes"),
            F.avg("tta_min").alias("avg_mtta_minutes"),
            F.avg("ttr_min").alias("avg_mttr_minutes"),
            (F.avg("is_automated") * F.lit(100.0)).alias("aidr_pct"),
            F.sum("rev_impact_amount").alias("total_rev_impact"),
        )
    )
    return grouped


def add_severity_score(df, severity_cfg):
    """
    severity_score: composite index combining duration (MTTR) and
    revenue impact, each min-max normalized to 0-1 across the
    current batch, blended per configured weights, scaled 0-100.
    Column name matches table_rules.wmg.demo.incident_history exactly.
    """
    duration_weight = float(severity_cfg.get("duration_weight", 0.5))
    revenue_weight = float(severity_cfg.get("revenue_weight", 0.5))

    stats = df.select(
        F.min("avg_mttr_minutes").alias("min_mttr"),
        F.max("avg_mttr_minutes").alias("max_mttr"),
        F.min("total_rev_impact").alias("min_rev"),
        F.max("total_rev_impact").alias("max_rev"),
    ).collect()[0]

    def norm(col_name, lo, hi):
        if lo is None or hi is None or hi == lo:
            return F.lit(0.0)
        return (F.col(col_name) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))

    norm_mttr = norm("avg_mttr_minutes", stats["min_mttr"], stats["max_mttr"])
    norm_rev = norm("total_rev_impact", stats["min_rev"], stats["max_rev"])

    return df.withColumn(
        "severity_score",
        (F.lit(duration_weight) * norm_mttr + F.lit(revenue_weight) * norm_rev) * F.lit(100.0)
    )


def merge_into_incident_history(spark, df, target_table):
    """
    Idempotent upsert keyed on (incident_date, kpi, label) so
    re-running the pipeline on the same day's incidents doesn't
    create duplicate rows, while genuinely new incident-days
    accumulate real history for ml_weighting to train on.
    """
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"incident_history created: {target_table} ({df.count()} rows)")
        return

    delta_tbl = DeltaTable.forName(spark, target_table)
    (
        delta_tbl.alias("t")
        .merge(
            df.alias("s"),
            "t.incident_date = s.incident_date AND t.kpi = s.kpi AND t.label = s.label"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"incident_history merged into: {target_table}")


# ============================================================
# ORCHESTRATOR - STEPS 1-3
# ============================================================

def run_kpi_metrics(spark, cfg):
    """
    Reads the raw incident log, computes MTTD/MTTA/MTTR/AIDR,
    classifies each incident to a KPI, aggregates to
    (incident_date, kpi, label) grain, computes severity_score,
    and MERGEs into wmg.demo.incident_history - the exact table
    ml_weighting.py already trains on.

    Safe no-op if kpi_metrics is disabled or the raw log table
    doesn't exist yet, so it never blocks the rest of the pipeline.
    """
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        print("KPI metrics skipped: disabled in config.")
        return None

    log_table = kpi_cfg.get("incident_log_table")
    if not log_table:
        print("KPI metrics skipped: incident_log_table not configured.")
        return None
    if not spark.catalog.tableExists(log_table):
        print(f"KPI metrics skipped: {log_table} does not exist yet.")
        return None

    print()
    print("=" * 70)
    print("KPI METRICS: MTTD / MTTA / MTTR / AIDR")
    print("=" * 70)

    raw = spark.table(log_table)
    raw_count = raw.count()
    print(f"Raw incident rows: {raw_count}")
    if raw_count == 0:
        print("KPI metrics skipped: incident log is empty.")
        return None

    with_durations = compute_incident_durations(raw)
    classified = classify_kpi(spark, with_durations, kpi_cfg)
    aggregated = aggregate_incident_history(classified)
    with_severity = add_severity_score(aggregated, kpi_cfg.get("severity_weights", {}))

    target_table = _resolve_incident_table(cfg)
    merge_into_incident_history(spark, with_severity, target_table)

    spark.table(target_table).orderBy("incident_date", "kpi").show(truncate=False)
    return with_severity


# ============================================================
# STAGE B - PER-KPI DYNAMIC WEIGHTS (matches whiteboard W1..Wn)
# ============================================================

def _kpi_level_aggregate(history_df):
    """
    Collapse the (incident_date, kpi, label) grain up to one row
    per KPI, count-weighted - this is the row set the whiteboard's
    KPI | Wt | Layer | Dimension table actually represents.
    """
    weighted = (
        history_df
        .withColumn("w_mttd", F.col("avg_mttd_minutes") * F.col("incident_count"))
        .withColumn("w_mtta", F.col("avg_mtta_minutes") * F.col("incident_count"))
        .withColumn("w_mttr", F.col("avg_mttr_minutes") * F.col("incident_count"))
        .withColumn("w_aidr", F.col("aidr_pct") * F.col("incident_count"))
    )
    grouped = (
        weighted.groupBy("kpi", "kpi_name", "dimension")
        .agg(
            F.sum("incident_count").alias("incident_count"),
            (F.sum("w_mttd") / F.sum("incident_count")).alias("avg_mttd_minutes"),
            (F.sum("w_mtta") / F.sum("incident_count")).alias("avg_mtta_minutes"),
            (F.sum("w_mttr") / F.sum("incident_count")).alias("avg_mttr_minutes"),
            (F.sum("w_aidr") / F.sum("incident_count")).alias("aidr_pct"),
            F.sum("total_rev_impact").alias("total_rev_impact"),
        )
    )
    return add_severity_score(grouped, {"duration_weight": 0.5, "revenue_weight": 0.5})


def compute_kpi_weights(spark, cfg):
    """
    Stage B of the two-stage design (see module docstring).
    Reads:
      - wmg.demo.incident_history  (written by run_kpi_metrics)
      - wmg.dqx_audit.dq_dynamic_weights (per-METRIC importances,
        written by ml_weighting.train_dynamic_weights - Stage A)
    Writes:
      - wmg.dqx_audit.dq_kpis (output_tables.kpi) - per-KPI W1..Wn
        normalized to 100, plus the metrics, matching the whiteboard.

    Safe no-op if either input table is missing yet.
    """
    kpi_cfg = cfg.get("kpi_metrics", {})
    ml_cfg = cfg.get("ml_weighting", {})
    if not kpi_cfg.get("enabled", False):
        return None

    history_table = _resolve_incident_table(cfg)
    metric_weights_table = _resolve_metric_weights_table(cfg)

    if not spark.catalog.tableExists(history_table):
        print("KPI weighting skipped: incident_history table not found.")
        return None
    if not spark.catalog.tableExists(metric_weights_table):
        print("KPI weighting skipped: metric-level ML weights not found "
              "(run ml_weighting.train_dynamic_weights first).")
        return None

    history = spark.table(history_table)
    kpi_level = _kpi_level_aggregate(history)

    feature_columns = ml_cfg.get("feature_columns", [])
    metric_weights_rows = (
        spark.table(metric_weights_table)
        .orderBy(F.col("generated_timestamp").desc())
        .limit(max(len(feature_columns), 1))
        .collect()
    )
    metric_weight = {r["kpi"]: float(r["dynamic_weight"]) for r in metric_weights_rows}
    if not metric_weight:
        print("KPI weighting skipped: no metric weights available yet.")
        return None

    usable_features = [c for c in feature_columns if c in metric_weight and c in kpi_level.columns]
    if not usable_features:
        print("KPI weighting skipped: metric weight names don't match feature_columns.")
        return None

    stats_row = kpi_level.select(
        *[F.min(c).alias(f"min_{c}") for c in usable_features],
        *[F.max(c).alias(f"max_{c}") for c in usable_features],
    ).collect()[0]

    scored = kpi_level
    score_expr = F.lit(0.0)
    for c in usable_features:
        lo, hi = stats_row[f"min_{c}"], stats_row[f"max_{c}"]
        norm_col = F.lit(0.0) if (lo is None or hi is None or hi == lo) else (
            (F.col(c) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))
        )
        score_expr = score_expr + (norm_col * F.lit(metric_weight[c]))
    scored = scored.withColumn("raw_score", score_expr)

    total = scored.agg(F.sum("raw_score").alias("t")).collect()[0]["t"] or 0.0
    if total == 0:
        n = scored.count()
        scored = scored.withColumn("weight", F.lit(100.0 / n if n else 0.0))
    else:
        scored = scored.withColumn("weight", F.round(F.col("raw_score") / F.lit(total) * F.lit(100.0), 4))

    result = (
        scored
        .select(
            "kpi", "kpi_name", "dimension", "weight",
            "incident_count", "avg_mttd_minutes", "avg_mtta_minutes",
            "avg_mttr_minutes", "aidr_pct", "total_rev_impact", "severity_score",
        )
        .withColumn("run_timestamp", F.current_timestamp())
    )

    report_table = _resolve_kpi_report_table(cfg)
    (
        result.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(report_table)
    )
    print(f"KPI report written: {report_table}")
    result.orderBy(F.col("weight").desc()).show(truncate=False)
    return result
'''

ml_content = '''
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


def train_dynamic_weights(spark, cfg, catalog):
    ml_cfg = cfg.get("ml_weighting", {})
    if not ml_cfg.get("enabled", False) or not ml_cfg.get("incident_table"):
        print("ML weighting skipped: no incident table configured.")
        return None

    source = ml_cfg["incident_table"]
    if not spark.catalog.tableExists(source):
        print(f"ML weighting skipped: {source} does not exist yet.")
        return None

    df = spark.table("`" + "`.`".join(source.split(".")) + "`")
    features = [c for c in ml_cfg["feature_columns"] if c in df.columns]
    target = ml_cfg["target_column"]
    if len(features) < 2 or target not in df.columns:
        print("ML weighting skipped: insufficient configured incident columns.")
        return None

    # CHANGED: read the minimum-rows threshold from config instead of
    # hardcoding 10, so dq_rules.yml's min_training_rows: 10 actually
    # has an effect (matches the project's "no hardcoding" principle).
    min_rows = int(ml_cfg.get("min_training_rows", 10))

    clean = df.select(*(features + [target])).dropna()
    row_count = clean.count()
    if row_count < min_rows:
        print(f"ML weighting skipped: {row_count} historical rows available, "
              f"{min_rows} required (min_training_rows).")
        return None

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    model_df = assembler.transform(clean)
    model = RandomForestRegressor(
        featuresCol="features", labelCol=target, numTrees=50, seed=42
    ).fit(model_df)

    importances = model.featureImportances.toArray().tolist()
    total = sum(importances) or 1.0
    weights = [{"kpi": k, "raw_importance": float(v),
                "dynamic_weight": round(float(v) / total * 100, 4)}
               for k, v in zip(features, importances)]
    out = spark.createDataFrame(weights).withColumn("generated_timestamp", F.current_timestamp())
    target_table = ml_cfg.get("output_table", f"{catalog}.dqx_audit.dq_dynamic_weights")
    out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "`" + "`.`".join(target_table.split(".")) + "`"
    )
    print("Dynamic ML weights written to:", target_table)
    return weights
'''

pathlib.Path(base + "/main.py").write_text(main_content)
pathlib.Path(base + "/kpi_metrics.py").write_text(kpi_content)
pathlib.Path(base + "/ml_weighting.py").write_text(ml_content)

print("Files written.")
import subprocess
r = subprocess.run(["grep", "-n", "kpi_metrics", base + "/main.py"], capture_output=True, text=True)
print("Verification grep:")
print(r.stdout)