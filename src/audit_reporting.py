# ============================================================
# AUDIT REPORTING
# ============================================================

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


# ============================================================
# WRITE DELTA TABLE SAFELY
# ============================================================

def write_delta_table(
    df,
    target,
    mode="append"
):

    (
        df.write
        .format("delta")
        .mode(mode)
        .option(
            "mergeSchema",
            "true"
        )
        .saveAsTable(target)
    )


# ============================================================
# WRITE AUDIT
# ============================================================

def write_audit(
    spark,
    cfg,
    summary_rows,
    detail_rows,
    profile_rows
):

    framework = cfg[
        "framework"
    ]

    catalog = framework[
        "catalog"
    ]

    audit_schema = framework[
        "audit_schema"
    ]

    profiling_schema = framework[
        "candidate_schema"
    ]

    output_tables = cfg[
        "output_tables"
    ]

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    if summary_rows:

        summary_df = spark.createDataFrame(
            summary_rows
        )

        target = table_name(
            catalog,
            audit_schema,
            output_tables[
                "summary"
            ]
        )

        write_delta_table(
            summary_df,
            target,
            "append"
        )

        print(
            f"Audit summary written: "
            f"{target}"
        )

    # --------------------------------------------------------
    # DETAIL
    # --------------------------------------------------------

    if detail_rows:

        detail_df = spark.createDataFrame(
            detail_rows
        )

        target = table_name(
            catalog,
            audit_schema,
            output_tables[
                "audit"
            ]
        )

        write_delta_table(
            detail_df,
            target,
            "append"
        )

        print(
            f"Audit detail written: "
            f"{target}"
        )

    # --------------------------------------------------------
    # PROFILING
    # --------------------------------------------------------

    if profile_rows:

        profile_df = spark.createDataFrame(
            profile_rows
        )

        target = table_name(
            catalog,
            profiling_schema,
            output_tables[
                "profiling"
            ]
        )

        (
            profile_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .saveAsTable(target)
        )

        print(
            f"Profiling table written: "
            f"{target}"
        )


# ============================================================
# FINAL REPORT
# ============================================================

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
    print("=" * 70)
    print("FINAL DATA QUALITY REPORT")
    print("=" * 70)

    if not spark.catalog.tableExists(
        summary_table
    ):

        print(
            "No summary table found."
        )

        return

    df = spark.table(
        summary_table
    )

    # Most recent run first
    if "run_timestamp" in df.columns:

        df = df.orderBy(
            F.col(
                "run_timestamp"
            ).desc()
        )

    df.show(
        100,
        truncate=False
    )

    print()
    print(
        f"Summary Table: "
        f"{summary_table}"
    )