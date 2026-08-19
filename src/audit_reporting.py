# ============================================================
# AUDIT REPORTING
# ============================================================

from datetime import datetime

from pyspark.sql import functions as F


def quote_identifier(identifier):

    return (
        "`"
        + str(identifier).replace("`", "``")
        + "`"
    )


def table_name(
    catalog,
    schema,
    table
):

    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(table)}"
    )


def write_audit(
    spark,
    cfg,
    summary_rows,
    detail_rows,
    profile_rows
):

    catalog = cfg[
        "framework"
    ][
        "catalog"
    ]

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

    output_tables = cfg[
        "output_tables"
    ]

    if summary_rows:

        summary_df = spark.createDataFrame(
            summary_rows
        )

        (
            summary_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                table_name(
                    catalog,
                    audit_schema,
                    output_tables[
                        "summary"
                    ]
                )
            )
        )

    if detail_rows:

        detail_df = spark.createDataFrame(
            detail_rows
        )

        (
            detail_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                table_name(
                    catalog,
                    audit_schema,
                    output_tables[
                        "audit"
                    ]
                )
            )
        )

    if profile_rows:

        profile_df = spark.createDataFrame(
            profile_rows
        )

        (
            profile_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .saveAsTable(
                table_name(
                    catalog,
                    profiling_schema,
                    output_tables[
                        "profiling"
                    ]
                )
            )
        )


def print_final_report(
    spark,
    cfg
):

    catalog = cfg[
        "framework"
    ][
        "catalog"
    ]

    audit_schema = cfg[
        "framework"
    ][
        "audit_schema"
    ]

    summary_table = (
        f"{catalog}."
        f"{audit_schema}."
        f"{cfg['output_tables']['summary']}"
    )

    print()
    print("=" * 60)
    print("FINAL DATA QUALITY REPORT")
    print("=" * 60)

    if not spark.catalog.tableExists(
        summary_table
    ):

        print(
            "No summary table found."
        )

        return

    df = (
        spark.table(
            summary_table
        )
        .orderBy(
            F.col(
                "run_timestamp"
            ).desc()
        )
    )

    df.show(
        100,
        truncate=False
    )

    print()
    print(
        f"Summary: {summary_table}"
    )