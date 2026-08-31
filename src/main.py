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