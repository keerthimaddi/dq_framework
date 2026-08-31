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

from src.kpi_metrics import (
    run_kpi_metrics,
    compute_kpi_weights,
    build_incident_level_kpi_report,
)

from src.weight_resolver import build_effective_weights

from src.reingest import run_reingestion


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
    #
    # NOTE: calculate_score currently reads dq_rules.yml
    # default_weight only (see weight_resolver.py + README for
    # the pending integration that will make this dynamic-weight
    # aware). Not changed here to avoid touching dq_engine.py
    # blind - see project status doc.
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
    gate_threshold = float(quality_gate.get("silver_min_score", 90))
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
        spark, bronze_df, failure_condition, catalog, schema, table, cfg, run_id,
    )

    # --------------------------------------------------------
    # SILVER (only if quality gate passed)
    # --------------------------------------------------------
    print()
    print("Evaluating Silver promotion...")

    silver_created = create_silver(spark, bronze_df, catalog, table, cfg, score)

    # --------------------------------------------------------
    # GOLD (only if Silver exists)
    # --------------------------------------------------------
    gold_created = False
    if silver_created:
        print()
        print("Evaluating Gold promotion...")
        gold_created = create_gold(spark, catalog, table, cfg, score, status)

    # --------------------------------------------------------
    # BUILD SUMMARY ROW
    # --------------------------------------------------------
    summary_row = {
        "catalog": catalog, "schema": schema, "table": table,
        "run_id": run_id, "run_timestamp": run_timestamp,
        "row_count": int(row_count), "column_count": int(column_count),
        "dq01": results.get("DQ01", "WARNING"), "dq02": results.get("DQ02", "WARNING"),
        "dq03": results.get("DQ03", "WARNING"), "dq04": results.get("DQ04", "WARNING"),
        "dq05": results.get("DQ05", "WARNING"), "dq06": results.get("DQ06", "WARNING"),
        "dq07": results.get("DQ07", "WARNING"), "dq08": results.get("DQ08", "WARNING"),
        "dq09": results.get("DQ09", "WARNING"), "dq10": results.get("DQ10", "WARNING"),
        "dq11": results.get("DQ11", "WARNING"), "dq12": results.get("DQ12", "WARNING"),
        "dq13": results.get("DQ13", "WARNING"), "dq14": results.get("DQ14", "WARNING"),
        "dq15": results.get("DQ15", "WARNING"), "dq16": results.get("DQ16", "WARNING"),
        "dq_score": float(score), "total_score": float(score),
        "overall_status": status, "quality_gate": gate_status,
        "quarantined_records": int(quarantined_count),
        "silver_created": bool(silver_created), "gold_created": bool(gold_created),
    }

    detail_rows = []
    for detail in details:
        detail_rows.append({
            "catalog": catalog, "schema": schema, "table": table,
            "run_id": run_id, "run_timestamp": run_timestamp,
            "dq_id": detail["dq_id"], "status": detail["status"],
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

    cfg = load_config()
    print_configuration(cfg)

    spark = get_spark_session()

    create_framework_schemas(spark, cfg)

    # --------------------------------------------------------
    # KPI METRICS (Requirement 02, Steps 1-3)
    # Runs BEFORE table discovery so wmg.demo.incident_history is
    # freshly merged before the DQ engine discovers and checks it
    # in the loop below, in this same run.
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
    # KPI DYNAMIC WEIGHTS - Stage B (per-KPI W1..Wn)
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
    # WHITEBOARD-STYLE FLAT REPORT (today's deliverable table)
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("INCIDENT-LEVEL KPI REPORT")
    print("=" * 70)
    try:
        build_incident_level_kpi_report(spark, cfg)
    except Exception as exc:
        print(f"Incident-level KPI report failed (non-fatal): {exc}")

    # --------------------------------------------------------
    # WEIGHT RESOLVER DEMONSTRATION
    # Proves dynamic-weight-with-YAML-fallback resolution works
    # TODAY, even though calculate_score() doesn't consume it yet
    # (that integration needs dq_engine.py's actual source - see
    # README "Known Limitations").
    # --------------------------------------------------------
    print()
    print("=" * 70)
    print("EFFECTIVE WEIGHT RESOLUTION (preview - not yet wired into scoring)")
    print("=" * 70)
    try:
        build_effective_weights(spark, cfg)
    except Exception as exc:
        print(f"Weight resolver preview failed (non-fatal): {exc}")

    # --------------------------------------------------------
    # RE-INGESTION
    # --------------------------------------------------------
    try:
        run_reingestion(spark, cfg, tables)
    except Exception as exc:
        print(f"Re-ingestion step failed (non-fatal): {exc}")

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


if __name__ == "__main__":
    main()
'''

kpi_content = '''
# ============================================================
# KPI METRICS MODULE
# Requirement 02 - Steps 1-4 + whiteboard-style final report
#
# Writes into wmg.demo.incident_history using the EXACT column
# names already declared in table_rules for that table:
#   incident_count, avg_mttd_minutes, avg_mtta_minutes,
#   avg_mttr_minutes, aidr_pct, severity_score, total_rev_impact
#
# NEW in this version: build_incident_level_kpi_report() - the
# single flat table requested for today's demo. One row per
# incident, joined with that incident's KPI code and the current
# ML-derived weight for that KPI. This is NOT a dashboard - it's
# a table you SELECT * FROM and hand to a reviewer.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# HELPERS
# ============================================================

def _tbl(name: str) -> str:
    return "`" + "`.`".join(name.split(".")) + "`"


def _minutes_between(end_col, start_col):
    return (F.col(end_col).cast("long") - F.col(start_col).cast("long")) / 60.0


def _resolve_incident_table(cfg):
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


def _resolve_incident_level_report_table(cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    default = f"{cfg['framework']['catalog']}.{cfg['framework']['audit_schema']}.dq_kpi_incident_report"
    return kpi_cfg.get("incident_report_table", default)


# ============================================================
# STEP 2 - PER-INCIDENT METRICS (TTD / TTA / TTR / automated flag)
# ============================================================

def compute_incident_durations(df):
    return (
        df
        .withColumn("ttd_min", _minutes_between("detected_timestamp", "event_timestamp"))
        .withColumn("tta_min", _minutes_between("ack_timestamp", "detected_timestamp"))
        .withColumn("ttr_min", _minutes_between("resolved_timestamp", "detected_timestamp"))
        .withColumn(
            "is_automated",
            F.when(F.upper(F.trim(F.col("detection_type"))) == "AUTOMATED", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("ttd_min", F.when(F.col("ttd_min") < 0, None).otherwise(F.col("ttd_min")))
        .withColumn("tta_min", F.when(F.col("tta_min") < 0, None).otherwise(F.col("tta_min")))
        .withColumn("ttr_min", F.when(F.col("ttr_min") < 0, None).otherwise(F.col("ttr_min")))
    )


# ============================================================
# CLASSIFY EACH INCIDENT TO A KPI
#
# IMPORTANT CHANGE: kpi codes in label_kpi_map now use the SAME
# ids as dq_rules.yml (DQ01, DQ06, DQ10, DQ03, DQ13, DQ07) for
# every label that genuinely corresponds to one of the 16 DQ
# dimensions. This is what lets weight_resolver.py match a
# dynamic KPI weight directly onto a DQ rule id. Labels that are
# purely operational (not one of the 16 DQ dimensions, e.g.
# "Availability") use an OPS-prefixed code instead - these still
# get tracked and reported, they just won't override a DQ weight,
# which is correct since there's no matching DQ check for them.
# ============================================================

def classify_kpi(spark, df, kpi_cfg):
    label_map = kpi_cfg.get("label_kpi_map", {})
    default_kpi = kpi_cfg.get(
        "default_kpi", {"kpi": "OPS99", "kpi_name": "Unclassified", "dimension": "unclassified"}
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
# ============================================================

def aggregate_incident_history(df):
    return (
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


def add_severity_score(df, severity_cfg):
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
# STAGE B - PER-KPI DYNAMIC WEIGHTS
# ============================================================

def _kpi_level_aggregate(history_df):
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


# ============================================================
# NEW: WHITEBOARD-STYLE FLAT REPORT (one row per incident)
# ============================================================

def build_incident_level_kpi_report(spark, cfg):
    """
    The reviewer-facing table: incident-level rows (date, incident,
    rev impact, DS, label) joined with that incident's KPI code and
    the CURRENT dynamic weight/aggregate stats for that KPI.

    SELECT * FROM <this table> ORDER BY date; is the whole demo.
    """
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        return None

    log_table = kpi_cfg.get("incident_log_table")
    weights_table = _resolve_kpi_report_table(cfg)

    if not spark.catalog.tableExists(log_table):
        print("Incident-level KPI report skipped: incident log not found.")
        return None
    if not spark.catalog.tableExists(weights_table):
        print("Incident-level KPI report skipped: dq_kpis not populated yet "
              "(run compute_kpi_weights first).")
        return None

    raw = spark.table(log_table)
    with_durations = compute_incident_durations(raw)
    classified = classify_kpi(spark, with_durations, kpi_cfg)

    weights_df = spark.table(weights_table)
    latest_ts = weights_df.agg(F.max("run_timestamp")).collect()[0][0]
    if latest_ts is None:
        print("Incident-level KPI report skipped: dq_kpis has no rows yet.")
        return None

    latest_weights = (
        weights_df
        .filter(F.col("run_timestamp") == latest_ts)
        .select(
            F.col("kpi").alias("w_kpi"),
            F.col("weight").alias("ml_weight"),
            F.col("avg_mttd_minutes").alias("kpi_avg_mttd_minutes"),
            F.col("avg_mtta_minutes").alias("kpi_avg_mtta_minutes"),
            F.col("avg_mttr_minutes").alias("kpi_avg_mttr_minutes"),
            F.col("aidr_pct").alias("kpi_aidr_pct"),
            F.col("severity_score").alias("kpi_severity_score"),
        )
    )

    report = (
        classified
        .join(latest_weights, classified["kpi"] == latest_weights["w_kpi"], "left")
        .select(
            F.col("incident_date").alias("date"),
            F.col("incident_id"),
            F.col("incident_description").alias("incident"),
            F.col("rev_impact_flag"),
            F.col("rev_impact_amount"),
            F.col("ds"),
            F.col("label").alias("label_code"),
            F.col("kpi"),
            F.col("kpi_name"),
            F.col("ttd_min").alias("incident_mttd_min"),
            F.col("tta_min").alias("incident_mtta_min"),
            F.col("ttr_min").alias("incident_mttr_min"),
            F.col("is_automated"),
            F.col("ml_weight"),
            F.col("kpi_avg_mttd_minutes"),
            F.col("kpi_avg_mtta_minutes"),
            F.col("kpi_avg_mttr_minutes"),
            F.col("kpi_aidr_pct"),
            F.col("kpi_severity_score"),
        )
        .withColumn("report_generated_timestamp", F.current_timestamp())
    )

    report_table = _resolve_incident_level_report_table(cfg)

    from delta.tables import DeltaTable
    if not spark.catalog.tableExists(report_table):
        (
            report.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(report_table)
        )
    else:
        delta_tbl = DeltaTable.forName(spark, report_table)
        (
            delta_tbl.alias("t")
            .merge(report.alias("s"), "t.incident_id = s.incident_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Incident-level KPI report written: {report_table}")
    spark.table(report_table).orderBy("date").show(truncate=False)
    return report
'''

weight_resolver_content = '''
# ============================================================
# WEIGHT RESOLVER
# Requirement 02, Part 4/5/11: dynamic weight -> YAML fallback
#
# This module is deliberately standalone and reads ONLY from
# Delta tables + cfg - it does not touch dq_engine.py, so it is
# 100% safe to deploy today with zero risk to the working DQ
# engine. It becomes USEFUL the moment calculate_score() is
# patched to call build_effective_weights() instead of reading
# rule["default_weight"] directly (see integration note below).
#
# COLD-START SAFE: every failure mode (table missing, no rows,
# ML hasn't trained yet, a KPI has no weight) falls through to
# the YAML default_weight. This function can NEVER raise in a
# way that blocks the pipeline - worst case it just returns the
# YAML defaults, identical to today's behavior.
# ============================================================

from pyspark.sql import functions as F


def _dimension_key(dimension):
    return dimension.strip().lower() if dimension else None


def load_dynamic_weights(spark, cfg):
    """
    Reads the LATEST run of dq_kpis (per-KPI dynamic weights, Stage B)
    and returns two lookup dicts:
      - by_rule_id:   {"DQ01": 12.4, "DQ06": 8.9, ...}
      - by_dimension: {"completeness": 12.4, "integrity": 8.9, ...}
    Returns ({}, {}) on ANY failure - missing table, empty table,
    schema drift, whatever. Never raises.
    """
    try:
        framework = cfg["framework"]
        kpi_table_name = cfg.get("output_tables", {}).get("kpi")
        if not kpi_table_name:
            return {}, {}

        kpi_table = f"{framework['catalog']}.{framework['audit_schema']}.{kpi_table_name}"

        if not spark.catalog.tableExists(kpi_table):
            print(f"weight_resolver: {kpi_table} does not exist yet - using YAML weights.")
            return {}, {}

        df = spark.table(kpi_table)
        if df.rdd.isEmpty():
            print(f"weight_resolver: {kpi_table} is empty - using YAML weights.")
            return {}, {}

        latest_ts = df.agg(F.max("run_timestamp")).collect()[0][0]
        if latest_ts is None:
            return {}, {}

        rows = (
            df.filter(F.col("run_timestamp") == latest_ts)
            .select("kpi", "dimension", "weight")
            .collect()
        )

        by_rule_id, by_dimension = {}, {}
        for r in rows:
            if r["weight"] is None:
                continue
            by_rule_id[r["kpi"]] = float(r["weight"])
            dim_key = _dimension_key(r["dimension"])
            if dim_key:
                by_dimension[dim_key] = float(r["weight"])

        print(f"weight_resolver: loaded {len(by_rule_id)} dynamic weight(s) from {kpi_table} "
              f"(run_timestamp={latest_ts})")
        return by_rule_id, by_dimension

    except Exception as exc:
        print(f"weight_resolver: dynamic weights unavailable, falling back to YAML ({exc})")
        return {}, {}


def resolve_weight(rule_id, dimension, default_weight, by_rule_id, by_dimension):
    """
    Resolution order:
      1. Dynamic weight matched by DQ rule id (e.g. "DQ01")
      2. Dynamic weight matched by dimension name (e.g. "completeness")
      3. YAML default_weight
    Always returns a float. Never returns None.
    """
    if rule_id in by_rule_id:
        return by_rule_id[rule_id]
    dim_key = _dimension_key(dimension)
    if dim_key and dim_key in by_dimension:
        return by_dimension[dim_key]
    return float(default_weight)


def build_effective_weights(spark, cfg):
    """
    THE function to call from calculate_score(). Returns a dict of
    {dq_id: effective_weight} covering every rule in dq_rules.yml,
    with dynamic weights applied wherever available and YAML defaults
    used everywhere else. Call once per pipeline run (or once per
    table - it's a single small table read, cheap either way).
    """
    by_rule_id, by_dimension = load_dynamic_weights(spark, cfg)

    effective = {}
    sources = {}
    for rule in cfg.get("dq_rules", []):
        rule_id = rule["id"]
        dimension = rule.get("dimension")
        default_weight = rule.get("default_weight", 0)
        weight = resolve_weight(rule_id, dimension, default_weight, by_rule_id, by_dimension)
        effective[rule_id] = weight
        sources[rule_id] = "DYNAMIC" if weight != float(default_weight) else "YAML"

    dynamic_count = sum(1 for s in sources.values() if s == "DYNAMIC")
    print(f"weight_resolver: {dynamic_count}/{len(effective)} dimension weights resolved dynamically, "
          f"{len(effective) - dynamic_count} using YAML defaults.")
    print(f"weight_resolver: effective weights this run: {effective}")

    return effective
'''

reingest_content = '''
# ============================================================
# RE-INGESTION MODULE
# Requirement 01 Section 17: Quarantine -> Correction -> Re-validation -> Silver
#
# DELIBERATELY PRACTICAL, NOT A DATA REPAIR ENGINE:
# a data steward supplies corrected rows in a per-table
# "dq_corrections_<schema>_<table>" Delta table (same schema as
# the source table + a correction_status column). This module
# re-validates ONLY those rows using the EXACT SAME rule-merging
# and failure-condition logic already used in the main pipeline
# (build_auto_rule + merge_rules + build_failure_condition) - so
# there is no second, divergent validation framework. Rows that
# now pass get merged into Silver; rows that still fail are left
# for the steward to correct again.
# ============================================================

from pyspark.sql import functions as F

from src.quarantine_rules import build_failure_condition
from src.dq_engine import get_table_rule
from src.auto_rules import build_auto_rule, merge_rules


def _correction_table_name(cfg, catalog, schema, table):
    audit_schema = cfg["framework"]["audit_schema"]
    return f"{catalog}.{audit_schema}.dq_corrections_{schema}_{table}"


def get_pending_corrections(spark, cfg, catalog, schema, table):
    """
    Returns only PENDING correction rows for this table, or None if
    no correction table exists for it (the common case - most tables
    will never have one, and that's fine, this is a no-op then).
    """
    correction_table = _correction_table_name(cfg, catalog, schema, table)
    if not spark.catalog.tableExists(correction_table):
        return None

    df = spark.table(correction_table)
    if "correction_status" not in df.columns:
        print(f"WARNING: {correction_table} has no correction_status column - "
              f"treating all rows as PENDING.")
        return df
    return df.filter(F.col("correction_status") == "PENDING")


def revalidate_corrections(spark, corrections_df, cfg, catalog, schema, table):
    """
    Re-applies the SAME effective_rule (auto-derived + manual YAML
    override, merged exactly as process_table does it) and the SAME
    build_failure_condition used for the original quarantine. This
    is literally the same DQ logic the rows were quarantined against
    - not a reimplementation.
    """
    auto_rule = build_auto_rule(spark, corrections_df, catalog, schema, table, cfg)
    manual_rule = get_table_rule(cfg, catalog, schema, table)
    effective_rule = merge_rules(auto_rule, manual_rule)

    failure_condition = build_failure_condition(corrections_df, effective_rule)

    still_failing = corrections_df.filter(failure_condition)
    now_passing = corrections_df.filter(~failure_condition)

    return now_passing, still_failing


def promote_to_silver(spark, now_passing_df, cfg, catalog, schema, table):
    """
    Merges corrected+passing rows into the existing Silver table.
    Uses unique_keys from table_rules to merge safely; falls back
    to append if no unique_keys are configured for this table
    (best-effort - configure unique_keys to avoid duplicate risk).
    """
    if now_passing_df.rdd.isEmpty():
        return 0

    silver_schema = cfg["framework"]["silver_schema"]
    silver_table = f"{catalog}.{silver_schema}.{table}"

    if not spark.catalog.tableExists(silver_table):
        print(f"Silver table {silver_table} does not exist yet - "
              f"skipping re-ingestion until a normal run creates it first.")
        return 0

    drop_cols = [c for c in ("correction_status",) if c in now_passing_df.columns]
    clean_df = now_passing_df.drop(*drop_cols) if drop_cols else now_passing_df

    table_rule = cfg.get("table_rules", {}).get(f"{catalog}.{schema}.{table}", {})
    unique_keys = table_rule.get("unique_keys", [])

    count = clean_df.count()

    if unique_keys:
        from delta.tables import DeltaTable
        delta_tbl = DeltaTable.forName(spark, silver_table)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in unique_keys])
        (
            delta_tbl.alias("t")
            .merge(clean_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        print(f"NOTE: {catalog}.{schema}.{table} has no unique_keys configured - "
              f"appending corrected rows (may create duplicates; add unique_keys "
              f"to table_rules to merge safely instead).")
        clean_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(silver_table)

    return count


def run_reingestion(spark, cfg, tables):
    """
    Called once per pipeline run, after the main table loop. Safe
    no-op for every table with no correction table present - this
    only does work where a steward has actually supplied corrections.
    Never raises out of the pipeline; every table is isolated in
    its own try/except.
    """
    print()
    print("=" * 70)
    print("RE-INGESTION")
    print("=" * 70)

    total_promoted = 0
    any_correction_tables_found = False

    for catalog, schema, table in tables:
        try:
            corrections_df = get_pending_corrections(spark, cfg, catalog, schema, table)
            if corrections_df is None:
                continue

            any_correction_tables_found = True
            pending_count = corrections_df.count()
            if pending_count == 0:
                continue

            print(f"{catalog}.{schema}.{table}: {pending_count} pending correction row(s) found.")

            now_passing, still_failing = revalidate_corrections(
                spark, corrections_df, cfg, catalog, schema, table
            )
            promoted = promote_to_silver(spark, now_passing, cfg, catalog, schema, table)
            still_failing_count = still_failing.count()

            print(f"  Promoted to Silver : {promoted}")
            print(f"  Still failing DQ   : {still_failing_count}")

            total_promoted += promoted

        except Exception as exc:
            print(f"Re-ingestion failed for {catalog}.{schema}.{table} (non-fatal): {exc}")

    if not any_correction_tables_found:
        print("No dq_corrections_* tables found for any discovered table - "
              "nothing to re-ingest this run (this is normal until a steward "
              "supplies corrections).")

    print(f"Re-ingestion complete. Total rows promoted to Silver: {total_promoted}")
    return total_promoted
'''

pathlib.Path(base + "/main.py").write_text(main_content)
pathlib.Path(base + "/kpi_metrics.py").write_text(kpi_content)
pathlib.Path(base + "/weight_resolver.py").write_text(weight_resolver_content)
pathlib.Path(base + "/reingest.py").write_text(reingest_content)

print("Files written: main.py, kpi_metrics.py, weight_resolver.py, reingest.py")
import subprocess
r = subprocess.run(["grep", "-n", "weight_resolver\\|reingest", base + "/main.py"], capture_output=True, text=True)
print("Verification grep:")
print(r.stdout)