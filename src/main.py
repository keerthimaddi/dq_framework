# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# MAIN PIPELINE
# ============================================================

import uuid
from datetime import datetime

from pyspark.sql import SparkSession

from src.rule_loader import load_config

from src.metadata_discovery import (
    discover_source_tables,
)

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

from src.quarantine_rules import (
    build_failure_condition,
)

from src.ml_weighting import (
    train_dynamic_weights,
)

from src.auto_rules import (
    build_auto_rule,
    merge_rules,
)

from src.kpi_metrics import (
    run_kpi_metrics,
    compute_kpi_weights,
    build_incident_level_kpi_report,
)

from src.weight_resolver import (
    build_effective_weights,
)

from src.reingest import (
    run_reingestion,
)


# ============================================================
# SPARK
# ============================================================

def get_spark_session():

    return (
        SparkSession
        .builder
        .appName(
            "Campaign Data Quality Framework"
        )
        .getOrCreate()
    )


# ============================================================
# CONFIGURATION
# ============================================================

def print_configuration(cfg):

    framework = cfg["framework"]

    print()
    print("=" * 70)
    print(
        "CAMPAIGN DATA QUALITY FRAMEWORK"
    )
    print("=" * 70)

    print(
        f"Catalog           : "
        f"{framework['catalog']}"
    )

    print(
        f"Audit Schema      : "
        f"{framework['audit_schema']}"
    )

    print(
        f"Candidate Schema  : "
        f"{framework['candidate_schema']}"
    )

    print(
        f"Quality Gate      : "
        f"{cfg['quality_gate']}"
    )

    print(
        f"Overall Thresholds: "
        f"{cfg['overall_thresholds']}"
    )

    print(
        f"ML Weighting      : "
        f"{cfg['ml_weighting']}"
    )


# ============================================================
# PROCESS ONE TABLE
# ============================================================

def process_table(
    spark,
    cfg,
    catalog,
    schema,
    table,
    effective_weights,
):

    source = (
        f"{catalog}.{schema}.{table}"
    )

    run_id = str(
        uuid.uuid4()
    )

    print()
    print("=" * 70)
    print(
        f"PROCESSING TABLE: {source}"
    )
    print("=" * 70)

    run_timestamp = datetime.now()

    # --------------------------------------------------------
    # LOAD
    # --------------------------------------------------------

    try:

        df = spark.table(
            source
        )

    except Exception as exc:

        print(
            f"ERROR loading {source}: "
            f"{exc}"
        )

        return None, [], [], []

    row_count = df.count()

    column_count = len(
        df.columns
    )

    print(
        f"Rows    : {row_count}"
    )

    print(
        f"Columns : {column_count}"
    )

    # --------------------------------------------------------
    # PROFILING
    # --------------------------------------------------------

    print()
    print(
        "Running data profiling..."
    )

    try:

        profile_rows = profile_table(
            df,
            catalog,
            schema,
            table,
        )

        candidate_rows = (
            create_rule_candidates(
                profile_rows
            )
        )

        print(
            f"Profile rows    : "
            f"{len(profile_rows)}"
        )

        print(
            f"Rule candidates : "
            f"{len(candidate_rows)}"
        )

    except Exception as exc:

        print(
            f"Profiling error: {exc}"
        )

        profile_rows = []

        candidate_rows = []

    # --------------------------------------------------------
    # BRONZE
    # --------------------------------------------------------

    print()
    print(
        "Writing Bronze..."
    )

    bronze_df, column_mapping = (
        write_bronze(
            spark,
            df,
            catalog,
            schema,
            table,
            cfg,
        )
    )

    # --------------------------------------------------------
    # AUTO RULE + MANUAL RULE
    # --------------------------------------------------------

    print()
    print(
        "Deriving DQ rules from "
        "table metadata..."
    )

    auto_rule = build_auto_rule(
        spark,
        bronze_df,
        catalog,
        schema,
        table,
        cfg,
    )

    manual_rule = get_table_rule(
        cfg,
        catalog,
        schema,
        table,
    )

    effective_rule = merge_rules(
        auto_rule,
        manual_rule,
    )

    print(
        f"Auto-derived: "
        f"{len(auto_rule.get('mandatory_columns', []))} "
        f"mandatory cols, "
        f"{len(auto_rule.get('unique_keys', []))} "
        f"key candidate(s), "
        f"{len(auto_rule.get('range_rules', {}))} "
        f"range rule(s), "
        f"{len(auto_rule.get('pattern_rules', {}))} "
        f"pattern rule(s)"
    )

    table_cfg = dict(cfg)

    table_cfg["table_rules"] = {
        **cfg.get(
            "table_rules",
            {}
        ),
        source: effective_rule,
    }

    # --------------------------------------------------------
    # DQ
    # --------------------------------------------------------

    print()
    print(
        "Running DQ01 - DQ16..."
    )

    results, details = run_all_dq(
        spark,
        bronze_df,
        catalog,
        schema,
        table,
        table_cfg,
    )

    print()
    print("-" * 70)
    print(
        f"DQ RESULTS: {source}"
    )
    print("-" * 70)

    for detail in sorted(
        details,
        key=lambda x: x["dq_id"],
    ):

        print(
            f"{detail['dq_id']} | "
            f"{detail['status']} | "
            f"Failure %: "
            f"{detail['failure_percentage']:.2f} | "
            f"Failed: "
            f"{detail['failed_records']}"
        )

    # --------------------------------------------------------
    # DYNAMIC WEIGHTED SCORE
    # --------------------------------------------------------

    score = calculate_score(
        cfg,
        results,
        effective_weights,
    )

    status = overall_status(
        score,
        cfg["overall_thresholds"],
    )

    print()
    print(
        f"Overall Score : "
        f"{score:.2f}%"
    )

    print(
        f"Overall Status: "
        f"{status}"
    )

    # --------------------------------------------------------
    # QUALITY GATE
    # --------------------------------------------------------

    quality_gate = cfg[
        "quality_gate"
    ]

    gate_threshold = float(
        quality_gate.get(
            "silver_min_score",
            90,
        )
    )

    gate_enabled = quality_gate.get(
        "enabled",
        True,
    )

    if not gate_enabled:

        gate_status = "DISABLED"

    elif score >= gate_threshold:

        gate_status = "PASSED"

    else:

        gate_status = "FAILED"

    print(
        f"Quality Gate  : "
        f"{gate_status}"
    )

    print(
        f"Gate Threshold: "
        f"{gate_threshold:.2f}%"
    )

    # --------------------------------------------------------
    # QUARANTINE
    # --------------------------------------------------------

    print()
    print(
        "Evaluating rows for quarantine..."
    )

    failure_condition = (
        build_failure_condition(
            bronze_df,
            effective_rule,
        )
    )

    quarantined_count = (
        create_quarantine(
            spark,
            bronze_df,
            failure_condition,
            catalog,
            schema,
            table,
            cfg,
            run_id,
        )
    )

    # --------------------------------------------------------
    # SILVER
    # --------------------------------------------------------

    print()
    print(
        "Evaluating Silver promotion..."
    )

    silver_created = create_silver(
        spark,
        bronze_df,
        catalog,
        table,
        cfg,
        score,
    )

    # --------------------------------------------------------
    # GOLD
    # --------------------------------------------------------

    gold_created = False

    if silver_created:

        print()
        print(
            "Evaluating Gold promotion..."
        )

        gold_created = create_gold(
            spark,
            catalog,
            table,
            cfg,
            score,
            status,
        )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    summary_row = {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "run_id": run_id,
        "run_timestamp": run_timestamp,

        "row_count": int(
            row_count
        ),

        "column_count": int(
            column_count
        ),

        "dq01": results.get(
            "DQ01",
            "WARNING",
        ),

        "dq02": results.get(
            "DQ02",
            "WARNING",
        ),

        "dq03": results.get(
            "DQ03",
            "WARNING",
        ),

        "dq04": results.get(
            "DQ04",
            "WARNING",
        ),

        "dq05": results.get(
            "DQ05",
            "WARNING",
        ),

        "dq06": results.get(
            "DQ06",
            "WARNING",
        ),

        "dq07": results.get(
            "DQ07",
            "WARNING",
        ),

        "dq08": results.get(
            "DQ08",
            "WARNING",
        ),

        "dq09": results.get(
            "DQ09",
            "WARNING",
        ),

        "dq10": results.get(
            "DQ10",
            "WARNING",
        ),

        "dq11": results.get(
            "DQ11",
            "WARNING",
        ),

        "dq12": results.get(
            "DQ12",
            "WARNING",
        ),

        "dq13": results.get(
            "DQ13",
            "WARNING",
        ),

        "dq14": results.get(
            "DQ14",
            "WARNING",
        ),

        "dq15": results.get(
            "DQ15",
            "WARNING",
        ),

        "dq16": results.get(
            "DQ16",
            "WARNING",
        ),

        "dq_score": float(
            score
        ),

        "total_score": float(
            score
        ),

        "overall_status": status,

        "quality_gate": gate_status,

        "quarantined_records": int(
            quarantined_count
        ),

        "silver_created": bool(
            silver_created
        ),

        "gold_created": bool(
            gold_created
        ),
    }

    detail_rows = []

    for detail in details:

        detail_rows.append({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "run_id": run_id,
            "run_timestamp": run_timestamp,

            "dq_id": detail[
                "dq_id"
            ],

            "status": detail[
                "status"
            ],

            "failure_percentage": float(
                detail[
                    "failure_percentage"
                ]
            ),

            "failed_records": int(
                detail[
                    "failed_records"
                ]
            ),
        })

    return (
        summary_row,
        detail_rows,
        profile_rows,
        candidate_rows,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print(
        "STARTING CAMPAIGN DATA QUALITY PIPELINE"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # CONFIG
    # --------------------------------------------------------

    cfg = load_config()

    print_configuration(
        cfg
    )

    # --------------------------------------------------------
    # SPARK
    # --------------------------------------------------------

    spark = get_spark_session()

    # --------------------------------------------------------
    # FRAMEWORK SCHEMAS
    # --------------------------------------------------------

    create_framework_schemas(
        spark,
        cfg,
    )

    # ========================================================
    # STEP 1
    # KPI METRICS
    # ========================================================

    print()
    print("=" * 70)
    print(
        "STEP 1 - KPI METRICS"
    )
    print("=" * 70)

    try:

        run_kpi_metrics(
            spark,
            cfg,
        )

    except Exception as exc:

        print(
            f"KPI metrics failed "
            f"(non-fatal): {exc}"
        )

    # ========================================================
    # STEP 2
    # ML STAGE A
    # ========================================================

    print()
    print("=" * 70)
    print(
        "STEP 2 - ML DYNAMIC WEIGHTING"
    )
    print("=" * 70)

    try:

        train_dynamic_weights(
            spark,
            cfg,
            cfg["framework"][
                "catalog"
            ],
        )

    except Exception as exc:

        print(
            f"ML weighting failed "
            f"(non-fatal): {exc}"
        )

    # ========================================================
    # STEP 3
    # KPI STAGE B
    # ========================================================

    print()
    print("=" * 70)
    print(
        "STEP 3 - KPI DYNAMIC WEIGHTS"
    )
    print("=" * 70)

    try:

        compute_kpi_weights(
            spark,
            cfg,
        )

    except Exception as exc:

        print(
            f"KPI weighting failed "
            f"(non-fatal): {exc}"
        )

    # ========================================================
    # STEP 4
    # EFFECTIVE WEIGHTS
    # ========================================================

    print()
    print("=" * 70)
    print(
        "STEP 4 - EFFECTIVE DQ WEIGHTS"
    )
    print("=" * 70)

    try:

        effective_weights = (
            build_effective_weights(
                spark,
                cfg,
            )
        )

    except Exception as exc:

        print(
            f"Dynamic weights unavailable: "
            f"{exc}"
        )

        effective_weights = {
            rule["id"]:
            float(
                rule.get(
                    "default_weight",
                    0,
                )
            )
            for rule in cfg[
                "dq_rules"
            ]
        }

    # ========================================================
    # STEP 5
    # DISCOVERY
    # ========================================================

    print()
    print("=" * 70)
    print(
        "STEP 5 - TABLE DISCOVERY"
    )
    print("=" * 70)

    tables = discover_source_tables(
        spark,
        cfg,
    )

    if not tables:

        print(
            "No tables found."
        )

        spark.stop()

        return

    # ========================================================
    # STEP 6
    # PROCESS TABLES
    # ========================================================

    summary_rows = []

    detail_rows = []

    profile_rows = []

    candidate_rows = []

    successful_tables = 0

    failed_tables = 0

    for (
        catalog,
        schema,
        table,
    ) in tables:

        try:

            result = process_table(
                spark,
                cfg,
                catalog,
                schema,
                table,
                effective_weights,
            )

            if (
                result is None
                or result[0] is None
            ):

                failed_tables += 1

                continue

            (
                summary,
                details,
                profiles,
                candidates,
            ) = result

            if summary:
                summary_rows.append(
                    summary
                )

            detail_rows.extend(
                details
            )

            profile_rows.extend(
                profiles
            )

            candidate_rows.extend(
                candidates
            )

            successful_tables += 1

        except Exception as exc:

            failed_tables += 1

            print()
            print(
                f"FAILED TABLE: "
                f"{catalog}.{schema}.{table}"
            )

            print(
                f"Reason: {exc}"
            )

    # ========================================================
    # WRITE AUDIT
    # ========================================================

    print()
    print("=" * 70)
    print(
        "WRITING AUDIT RESULTS"
    )
    print("=" * 70)

    try:

        write_audit(
            spark,
            cfg,
            summary_rows,
            detail_rows,
            profile_rows,
        )

        print(
            "Audit results written."
        )

    except Exception as exc:

        print(
            f"Audit write failed: "
            f"{exc}"
        )

    # ========================================================
    # WRITE CANDIDATES
    # ========================================================

    if candidate_rows:

        try:

            candidate_schema = cfg[
                "framework"
            ][
                "candidate_schema"
            ]

            candidate_table = (
                cfg["output_tables"]
                .get("candidates")
            )

            if candidate_table:

                candidate_df = (
                    spark.createDataFrame(
                        candidate_rows
                    )
                )

                (
                    candidate_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option(
                        "overwriteSchema",
                        "true",
                    )
                    .saveAsTable(
                        f"`{cfg['framework']['catalog']}`."
                        f"`{candidate_schema}`."
                        f"`{candidate_table}`"
                    )
                )

                print(
                    "Rule candidates written."
                )

        except Exception as exc:

            print(
                f"Candidate write failed: "
                f"{exc}"
            )

    # ========================================================
    # FLAT KPI REPORT
    # ========================================================

    print()
    print("=" * 70)
    print(
        "INCIDENT-LEVEL KPI REPORT"
    )
    print("=" * 70)

    try:

        build_incident_level_kpi_report(
            spark,
            cfg,
        )

    except Exception as exc:

        print(
            f"KPI incident report failed: "
            f"{exc}"
        )

    # ========================================================
    # RE-INGESTION
    # ========================================================

    print()
    print("=" * 70)
    print(
        "RE-INGESTION"
    )
    print("=" * 70)

    try:

        run_reingestion(
            spark,
            cfg,
            tables,
        )

    except Exception as exc:

        print(
            f"Re-ingestion failed: "
            f"{exc}"
        )

    # ========================================================
    # FINAL REPORT
    # ========================================================

    print()
    print("=" * 70)
    print(
        "FINAL REPORT"
    )
    print("=" * 70)

    try:

        print_final_report(
            spark,
            cfg,
        )

    except Exception as exc:

        print(
            f"Final report failed: "
            f"{exc}"
        )

    # ========================================================
    # SUMMARY
    # ========================================================

    print()
    print("=" * 70)
    print(
        "PIPELINE EXECUTION SUMMARY"
    )
    print("=" * 70)

    print(
        f"Tables discovered : "
        f"{len(tables)}"
    )

    print(
        f"Tables processed  : "
        f"{successful_tables}"
    )

    print(
        f"Tables failed     : "
        f"{failed_tables}"
    )

    print(
        f"Summary records   : "
        f"{len(summary_rows)}"
    )

    print(
        f"Detail records    : "
        f"{len(detail_rows)}"
    )

    print(
        f"Profile records   : "
        f"{len(profile_rows)}"
    )

    print(
        f"Rule candidates   : "
        f"{len(candidate_rows)}"
    )

    print()
    print("=" * 70)

    if failed_tables == 0:

        print(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )

    else:

        print(
            "PIPELINE COMPLETED WITH TABLE ERRORS"
        )

    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()