# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# MAIN PIPELINE
# ============================================================

from datetime import datetime

from pyspark.sql import SparkSession

from src.rule_loader import load_config
from src.metadata_discovery import discover_tables
from src.dq_engine import (
    run_all_dq,
    calculate_score,
    overall_status,
)
from src.profiler import (
    profile_table,
    create_rule_candidates,
)
from src.audit_reporting import (
    write_audit,
    print_final_report,
)


# ============================================================
# SPARK SESSION
# ============================================================

def get_spark_session():

    spark = (
        SparkSession
        .builder
        .appName(
            "Campaign Data Quality Framework"
        )
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

    print()


# ============================================================
# PROCESS ONE TABLE
# ============================================================

def process_table(
    spark,
    cfg,
    catalog,
    schema,
    table,
):

    source = (
        f"{catalog}.{schema}.{table}"
    )

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

        print(
            f"ERROR loading table {source}: "
            f"{exc}"
        )

        return None, [], []

    # --------------------------------------------------------
    # BASIC TABLE INFORMATION
    # --------------------------------------------------------

    row_count = df.count()
    column_count = len(df.columns)

    print(
        f"Rows    : {row_count}"
    )

    print(
        f"Columns : {column_count}"
    )

    # --------------------------------------------------------
    # PROFILE TABLE
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
            table
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
            f"Profiling error for "
            f"{source}: {exc}"
        )

        profile_rows = []
        candidate_rows = []

    # --------------------------------------------------------
    # RUN DQ CHECKS
    # --------------------------------------------------------

    print()
    print(
        "Running DQ01 - DQ16..."
    )

    results, details = run_all_dq(
        spark,
        df,
        catalog,
        schema,
        table,
        cfg
    )

    # --------------------------------------------------------
    # PRINT DQ RESULTS
    # --------------------------------------------------------

    print()
    print("-" * 70)
    print(
        f"DQ RESULTS: {source}"
    )
    print("-" * 70)

    for detail in details:

        print(
            f"{detail['dq_id']} | "
            f"{detail['status']} | "
            f"Failure %: "
            f"{detail['failure_percentage']:.2f} | "
            f"Failed: "
            f"{detail['failed_records']}"
        )

    # --------------------------------------------------------
    # CALCULATE SCORE
    # --------------------------------------------------------

    score = calculate_score(
        cfg,
        results
    )

    status = overall_status(
        score,
        cfg["overall_thresholds"]
    )

    print()
    print(
        f"Overall Score : {score:.2f}%"
    )

    print(
        f"Overall Status: {status}"
    )

    # --------------------------------------------------------
    # QUALITY GATE
    # --------------------------------------------------------

    quality_gate = cfg[
        "quality_gate"
    ]

    gate_threshold = float(
        quality_gate.get(
            "threshold",
            0
        )
    )

    gate_enabled = quality_gate.get(
        "enabled",
        True
    )

    if not gate_enabled:

        gate_status = "DISABLED"

    elif score >= gate_threshold:

        gate_status = "PASSED"

    else:

        gate_status = "FAILED"

    print(
        f"Quality Gate  : {gate_status}"
    )

    print(
        f"Gate Threshold: "
        f"{gate_threshold:.2f}%"
    )

    # --------------------------------------------------------
    # BUILD SUMMARY ROW
    # --------------------------------------------------------

    summary_row = {

        "catalog": catalog,

        "schema": schema,

        "table": table,

        "run_timestamp":
            run_timestamp,

        "row_count":
            int(row_count),

        "column_count":
            int(column_count),

        "dq01":
            results.get("DQ01", "WARNING"),

        "dq02":
            results.get("DQ02", "WARNING"),

        "dq03":
            results.get("DQ03", "WARNING"),

        "dq04":
            results.get("DQ04", "WARNING"),

        "dq05":
            results.get("DQ05", "WARNING"),

        "dq06":
            results.get("DQ06", "WARNING"),

        "dq07":
            results.get("DQ07", "WARNING"),

        "dq08":
            results.get("DQ08", "WARNING"),

        "dq09":
            results.get("DQ09", "WARNING"),

        "dq10":
            results.get("DQ10", "WARNING"),

        "dq11":
            results.get("DQ11", "WARNING"),

        "dq12":
            results.get("DQ12", "WARNING"),

        "dq13":
            results.get("DQ13", "WARNING"),

        "dq14":
            results.get("DQ14", "WARNING"),

        "dq15":
            results.get("DQ15", "WARNING"),

        "dq16":
            results.get("DQ16", "WARNING"),

        "dq_score":
            float(score),

        "overall_status":
            status,

        "quality_gate":
            gate_status,
    }

    # --------------------------------------------------------
    # BUILD DETAIL ROWS
    # --------------------------------------------------------

    detail_rows = []

    for detail in details:

        detail_rows.append({

            "catalog":
                catalog,

            "schema":
                schema,

            "table":
                table,

            "run_timestamp":
                run_timestamp,

            "dq_id":
                detail["dq_id"],

            "status":
                detail["status"],

            "failure_percentage":
                float(
                    detail[
                        "failure_percentage"
                    ]
                ),

            "failed_records":
                int(
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
    # LOAD CONFIGURATION
    # --------------------------------------------------------

    cfg = load_config()

    print_configuration(cfg)

    # --------------------------------------------------------
    # CREATE SPARK SESSION
    # --------------------------------------------------------

    spark = get_spark_session()

    # --------------------------------------------------------
    # DISCOVER TABLES
    # --------------------------------------------------------

    tables = discover_tables(
        spark,
        cfg
    )

    if not tables:

        print()
        print(
            "No tables found for processing."
        )

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

    for (
        catalog,
        schema,
        table
    ) in tables:

        try:

            result = process_table(
                spark,
                cfg,
                catalog,
                schema,
                table
            )

            if result is None:

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

    # --------------------------------------------------------
    # WRITE AUDIT RESULTS
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("WRITING AUDIT RESULTS")
    print("=" * 70)

    try:

        write_audit(
            spark,
            cfg,
            summary_rows,
            detail_rows,
            profile_rows
        )

        print(
            "Audit results written successfully."
        )

    except Exception as exc:

        print(
            f"Audit write failed: {exc}"
        )

    # --------------------------------------------------------
    # WRITE RULE CANDIDATES
    # --------------------------------------------------------

    if candidate_rows:

        try:

            candidate_schema = cfg[
                "framework"
            ][
                "candidate_schema"
            ]

            candidate_table = cfg[
                "output_tables"
            ].get(
                "candidates"
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
                        "true"
                    )
                    .saveAsTable(
                        f"`{cfg['framework']['catalog']}`."
                        f"`{candidate_schema}`."
                        f"`{candidate_table}`"
                    )
                )

                print(
                    "Rule candidates written successfully."
                )

        except Exception as exc:

            print(
                f"Candidate write failed: "
                f"{exc}"
            )

    # --------------------------------------------------------
    # FINAL REPORT
    # --------------------------------------------------------

    print_final_report(
        spark,
        cfg
    )

    # --------------------------------------------------------
    # PIPELINE SUMMARY
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
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
    print(
        "=" * 70
    )

    if failed_tables == 0:

        print(
            "PIPELINE COMPLETED SUCCESSFULLY"
        )

    else:

        print(
            "PIPELINE COMPLETED WITH TABLE ERRORS"
        )

    print("=" * 70)

    # --------------------------------------------------------
    # STOP SPARK
    # --------------------------------------------------------

    spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()