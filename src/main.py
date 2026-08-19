# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# MAIN PIPELINE
# ============================================================

import traceback
import uuid
from datetime import datetime

from pyspark.sql import SparkSession

from src.rule_loader import load_config
from src.metadata_discovery import discover_tables
from src.lakehouse import (
    create_framework_schemas,
    read_source_table,
    write_bronze,
    create_silver,
    create_gold,
    create_quarantine,
)
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


DQ_IDS = [
    f"DQ{i:02d}"
    for i in range(1, 17)
]


def print_configuration(cfg):

    print()
    print("=" * 60)
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("=" * 60)

    print(
        f"Catalog: "
        f"{cfg['framework']['catalog']}"
    )

    print()
    print("=" * 60)
    print("DQ RULE CONFIGURATION")
    print("=" * 60)

    for rule in cfg["dq_rules"]:

        print(
            f"{rule['id']} | "
            f"{rule['dimension']} | "
            f"Level: {rule['level']} | "
            f"Enabled: "
            f"{rule.get('enabled', True)} | "
            f"Weight: "
            f"{rule['default_weight']} | "
            f"Severity: "
            f"{rule['severity']}"
        )

    enabled_rules = [
        rule
        for rule in cfg["dq_rules"]
        if rule.get(
            "enabled",
            True
        )
    ]

    total_weight = sum(
        float(rule["default_weight"])
        for rule in enabled_rules
    )

    print()
    print(
        f"Total DQ Rules   : "
        f"{len(cfg['dq_rules'])}"
    )

    print(
        f"Enabled DQ Rules : "
        f"{len(enabled_rules)}"
    )

    print(
        f"Total Weight     : "
        f"{total_weight}"
    )

    thresholds = cfg[
        "overall_thresholds"
    ]

    print()
    print(
        f"PASS    >= {thresholds['pass']}"
    )

    print(
        f"WARNING >= {thresholds['warning']}"
    )

    print(
        f"FAIL    < {thresholds['warning']}"
    )

    gate = cfg[
        "quality_gate"
    ]

    print()
    print(
        f"Silver Quality Gate: "
        f"{gate['silver_min_score']}"
    )


def main():

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    try:

        # ====================================================
        # LOAD CONFIGURATION
        # ====================================================

        cfg = load_config()

        print_configuration(
            cfg
        )

        catalog = cfg[
            "framework"
        ][
            "catalog"
        ]

        # ====================================================
        # CREATE FRAMEWORK SCHEMAS
        # ====================================================

        print()
        print("=" * 60)
        print("CREATING FRAMEWORK SCHEMAS")
        print("=" * 60)

        create_framework_schemas(
            spark,
            cfg
        )

        # ====================================================
        # DISCOVER TABLES
        # ====================================================

        tables = discover_tables(
            spark,
            cfg
        )

        print()
        print(
            f"Tables discovered: "
            f"{len(tables)}"
        )

        if not tables:

            print()
            print(
                "No source tables found."
            )

            print(
                "Check "
                "source_schema_allowlist "
                "in dq_rules.yml."
            )

            return

        # ====================================================
        # PROFILING
        # ====================================================

        profile_rows = []

        if cfg.get(
            "profiling",
            {}
        ).get(
            "enabled",
            True
        ):

            print()
            print("=" * 60)
            print("DQX-STYLE PROFILING")
            print("=" * 60)

            for (
                source_catalog,
                source_schema,
                table
            ) in tables:

                source = (
                    f"{source_catalog}."
                    f"{source_schema}."
                    f"{table}"
                )

                try:

                    df = read_source_table(
                        spark,
                        source_catalog,
                        source_schema,
                        table
                    )

                    rows = profile_table(
                        df,
                        source_catalog,
                        source_schema,
                        table
                    )

                    profile_rows.extend(
                        rows
                    )

                except Exception as exc:

                    print(
                        f"Profiling failed for "
                        f"{source}: {exc}"
                    )

            if profile_rows:

                null_threshold = float(
                    cfg.get(
                        "profiling",
                        {}
                    ).get(
                        "candidate_null_rate_pct",
                        1
                    )
                )

                candidates = (
                    create_rule_candidates(
                        profile_rows,
                        null_threshold
                    )
                )

                if candidates:

                    print()
                    print(
                        f"Rule candidates "
                        f"generated: "
                        f"{len(candidates)}"
                    )

        # ====================================================
        # DQ EXECUTION
        # ====================================================

        print()
        print("=" * 60)
        print(
            "BRONZE -> DQ -> "
            "QUARANTINE -> SILVER -> GOLD"
        )
        print("=" * 60)

        summary_rows = []
        detail_rows = []

        for (
            source_catalog,
            source_schema,
            table
        ) in tables:

            source = (
                f"{source_catalog}."
                f"{source_schema}."
                f"{table}"
            )

            print()
            print("-" * 60)
            print(
                f"Checking: {source}"
            )
            print("-" * 60)

            run_id = str(
                uuid.uuid4()
            )

            try:

                # ============================================
                # SOURCE
                # ============================================

                source_df = read_source_table(
                    spark,
                    source_catalog,
                    source_schema,
                    table
                )

                # ============================================
                # BRONZE
                # ============================================

                bronze_df, mapping = (
                    write_bronze(
                        spark,
                        source_df,
                        source_catalog,
                        source_schema,
                        table,
                        cfg
                    )
                )

                changed_columns = {
                    original: safe
                    for original, safe
                    in mapping.items()
                    if original != safe
                }

                if changed_columns:

                    print(
                        "Column normalization:"
                    )

                    for (
                        original,
                        safe
                    ) in changed_columns.items():

                        print(
                            f"  {original} "
                            f"-> {safe}"
                        )

                # ============================================
                # DQ
                # ============================================

                results, details = (
                    run_all_dq(
                        spark,
                        bronze_df,
                        source_catalog,
                        source_schema,
                        table,
                        cfg
                    )
                )

                # ============================================
                # SCORE
                # ============================================

                score = calculate_score(
                    cfg,
                    results
                )

                status = overall_status(
                    score,
                    cfg[
                        "overall_thresholds"
                    ]
                )

                status_string = " | ".join(
                    f"{dq_id}="
                    f"{results.get(dq_id, 'N/A')}"
                    for dq_id in DQ_IDS
                )

                print()
                print(
                    f"{status_string}"
                )

                print(
                    f"Score={score}% | "
                    f"Overall={status}"
                )

                # ============================================
                # QUARANTINE
                # ============================================

                # The row-level quarantine condition is built
                # from configured row/business/range rules.

                quarantine_condition = None

                table_rule = cfg.get(
                    "table_rules",
                    {}
                ).get(
                    source,
                    {}
                )

                if results.get(
                    "DQ02"
                ) in (
                    "FAIL",
                    "WARNING"
                ):

                    for expression in (
                        table_rule.get(
                            "row_expressions",
                            {}
                        ).values()
                    ):

                        try:

                            invalid = (
                                ~spark
                                .createDataFrame(
                                    [(True,)],
                                    ["dummy"]
                                )
                                .select(
                                    invalid
                                )
                            )

                        except Exception:
                            pass

                # Build quarantine conditions directly
                # against the Bronze DataFrame.

                from pyspark.sql import functions as F

                expressions = []

                if results.get(
                    "DQ02"
                ) in (
                    "FAIL",
                    "WARNING"
                ):

                    expressions.extend(
                        table_rule.get(
                            "row_expressions",
                            {}
                        ).values()
                    )

                if results.get(
                    "DQ05"
                ) in (
                    "FAIL",
                    "WARNING"
                ):

                    expressions.extend(
                        table_rule.get(
                            "consistency_rules",
                            {}
                        ).values()
                    )

                if results.get(
                    "DQ15"
                ) in (
                    "FAIL",
                    "WARNING"
                ):

                    expressions.extend(
                        table_rule.get(
                            "business_rules",
                            {}
                        ).values()
                    )

                if expressions:

                    for expression in expressions:

                        try:

                            invalid = (
                                F.coalesce(
                                    F.expr(
                                        expression
                                    ),
                                    F.lit(False)
                                )
                                == F.lit(False)
                            )

                            quarantine_condition = (
                                invalid
                                if quarantine_condition
                                is None
                                else
                                quarantine_condition
                                | invalid
                            )

                        except Exception as exc:

                            print(
                                "Quarantine expression "
                                f"skipped: {exc}"
                            )

                quarantine_count = (
                    create_quarantine(
                        spark,
                        bronze_df,
                        quarantine_condition,
                        source_catalog,
                        source_schema,
                        table,
                        cfg,
                        run_id
                    )
                )

                # ============================================
                # SILVER
                # ============================================

                silver_created = create_silver(
                    spark,
                    bronze_df,
                    source_catalog,
                    table,
                    cfg,
                    score
                )

                # ============================================
                # GOLD
                # ============================================

                gold_created = False

                if silver_created:

                    gold_created = create_gold(
                        spark,
                        source_catalog,
                        table,
                        cfg,
                        score,
                        status
                    )

                # ============================================
                # SUMMARY
                # ============================================

                row_count = source_df.count()

                summary = {
                    "run_id":
                        run_id,

                    "run_timestamp":
                        datetime.now(),

                    "catalog":
                        source_catalog,

                    "schema":
                        source_schema,

                    "table":
                        table,

                    "row_count":
                        int(row_count),

                    "DQ01":
                        results["DQ01"],

                    "DQ02":
                        results["DQ02"],

                    "DQ03":
                        results["DQ03"],

                    "DQ04":
                        results["DQ04"],

                    "DQ05":
                        results["DQ05"],

                    "DQ06":
                        results["DQ06"],

                    "DQ07":
                        results["DQ07"],

                    "DQ08":
                        results["DQ08"],

                    "DQ09":
                        results["DQ09"],

                    "DQ10":
                        results["DQ10"],

                    "DQ11":
                        results["DQ11"],

                    "DQ12":
                        results["DQ12"],

                    "DQ13":
                        results["DQ13"],

                    "DQ14":
                        results["DQ14"],

                    "DQ15":
                        results["DQ15"],

                    "DQ16":
                        results["DQ16"],

                    "total_score":
                        float(score),

                    "overall_status":
                        status,

                    "silver_created":
                        bool(
                            silver_created
                        ),

                    "gold_created":
                        bool(
                            gold_created
                        ),

                    "quarantined_records":
                        int(
                            quarantine_count
                        ),
                }

                summary_rows.append(
                    summary
                )

                for detail in details:

                    detail_rows.append({

                        "run_id":
                            run_id,

                        "run_timestamp":
                            datetime.now(),

                        "catalog":
                            source_catalog,

                        "schema":
                            source_schema,

                        "table":
                            table,

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
                            )
                    })

            except Exception as exc:

                print()
                print(
                    f"Pipeline failed for "
                    f"{source}"
                )

                print(
                    f"Error: {exc}"
                )

                traceback.print_exc()

                # Continue with the next table.

        # ====================================================
        # AUDIT
        # ====================================================

        print()
        print("=" * 60)
        print("WRITING AUDIT RESULTS")
        print("=" * 60)

        write_audit(
            spark,
            cfg,
            summary_rows,
            detail_rows,
            profile_rows
        )

        # ====================================================
        # FINAL REPORT
        # ====================================================

        print_final_report(
            spark,
            cfg
        )

        print()
        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)

        audit_schema = cfg[
            "framework"
        ][
            "audit_schema"
        ]

        profiling_schema = cfg[
            "framework"
        ][
            "candidate_schema"
        ]

        print(
            f"Summary: "
            f"{catalog}."
            f"{audit_schema}."
            f"{cfg['output_tables']['summary']}"
        )

        print(
            f"Audit: "
            f"{catalog}."
            f"{audit_schema}."
            f"{cfg['output_tables']['audit']}"
        )

        print(
            f"Profile: "
            f"{catalog}."
            f"{profiling_schema}."
            f"{cfg['output_tables']['profiling']}"
        )

        print(
            f"Candidates: "
            f"{catalog}."
            f"{profiling_schema}."
            f"{cfg['output_tables']['candidates']}"
        )

    except Exception as exc:

        print()
        print("=" * 60)
        print("FATAL PIPELINE ERROR")
        print("=" * 60)

        print(
            str(exc)
        )

        traceback.print_exc()

        raise


if __name__ == "__main__":
    main()