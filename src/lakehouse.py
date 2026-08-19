# ============================================================
# LAKEHOUSE
# ============================================================

import re

from pyspark.sql import functions as F


def quote_identifier(identifier):

    return (
        "`"
        + str(identifier).replace("`", "``")
        + "`"
    )


def full_table_name(
    catalog,
    schema,
    table
):

    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(table)}"
    )


def safe_column_name(name):

    name = str(name).strip()

    name = re.sub(
        r"[^A-Za-z0-9_]",
        "_",
        name
    )

    name = re.sub(
        r"_+",
        "_",
        name
    )

    name = name.strip("_")

    if not name:
        name = "column"

    if name[0].isdigit():
        name = "_" + name

    return name.lower()


def normalize_columns(df):

    used = set()
    mapping = {}

    for original in df.columns:

        base = safe_column_name(
            original
        )

        candidate = base
        counter = 1

        while candidate in used:

            candidate = (
                f"{base}_{counter}"
            )

            counter += 1

        used.add(candidate)

        mapping[original] = candidate

    for original, safe in mapping.items():

        if original != safe:

            df = df.withColumnRenamed(
                original,
                safe
            )

    return df, mapping


def read_source_table(
    spark,
    catalog,
    schema,
    table
):

    return spark.table(
        full_table_name(
            catalog,
            schema,
            table
        )
    )


def create_framework_schemas(
    spark,
    cfg
):

    framework = cfg["framework"]

    catalog = framework["catalog"]

    schemas = [
        framework["bronze_schema"],
        framework["silver_schema"],
        framework["gold_schema"],
        framework["audit_schema"],
        framework["quarantine_schema"],
        framework["candidate_schema"],
    ]

    for schema in schemas:

        print(
            f"Checking schema: "
            f"{catalog}.{schema}"
        )

        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{quote_identifier(catalog)}."
            f"{quote_identifier(schema)}"
        )


def write_bronze(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg
):

    safe_df, mapping = normalize_columns(
        df
    )

    bronze_schema = cfg[
        "framework"
    ][
        "bronze_schema"
    ]

    bronze_table = (
        f"{catalog}."
        f"{bronze_schema}."
        f"{table}"
    )

    print(
        f"Writing Bronze: "
        f"{bronze_table}"
    )

    safe_df = (
        safe_df
        .withColumn(
            "_dq_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_dq_source_catalog",
            F.lit(catalog)
        )
        .withColumn(
            "_dq_source_schema",
            F.lit(schema)
        )
        .withColumn(
            "_dq_source_table",
            F.lit(table)
        )
    )

    (
        safe_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            full_table_name(
                catalog,
                bronze_schema,
                table
            )
        )
    )

    return safe_df, mapping


def create_silver(
    spark,
    bronze_df,
    catalog,
    table,
    cfg,
    score
):

    gate = cfg.get(
        "quality_gate",
        {}
    )

    minimum_score = float(
        gate.get(
            "silver_min_score",
            90
        )
    )

    silver_schema = cfg[
        "framework"
    ][
        "silver_schema"
    ]

    silver_table = (
        f"{catalog}."
        f"{silver_schema}."
        f"{table}"
    )

    if (
        gate.get("enabled", True)
        and score < minimum_score
    ):

        print(
            f"Silver BLOCKED | "
            f"Score={score} | "
            f"Required={minimum_score}"
        )

        return False

    metadata_columns = [
        "_dq_ingestion_timestamp",
        "_dq_source_catalog",
        "_dq_source_schema",
        "_dq_source_table",
    ]

    columns_to_drop = [
        column
        for column in metadata_columns
        if column in bronze_df.columns
    ]

    silver_df = bronze_df.drop(
        *columns_to_drop
    )

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            full_table_name(
                catalog,
                silver_schema,
                table
            )
        )
    )

    print(
        f"Silver CREATED: "
        f"{silver_table}"
    )

    return True


def create_gold(
    spark,
    catalog,
    table,
    cfg,
    score,
    status
):

    silver_schema = cfg[
        "framework"
    ][
        "silver_schema"
    ]

    gold_schema = cfg[
        "framework"
    ][
        "gold_schema"
    ]

    silver_table = (
        f"{catalog}."
        f"{silver_schema}."
        f"{table}"
    )

    gold_table = (
        f"{catalog}."
        f"{gold_schema}."
        f"{table}"
    )

    if not spark.catalog.tableExists(
        silver_table
    ):

        print(
            f"Silver does not exist: "
            f"{silver_table}"
        )

        return False

    df = spark.table(
        full_table_name(
            catalog,
            silver_schema,
            table
        )
    )

    gold_df = (
        df
        .withColumn(
            "_dq_quality_score",
            F.lit(float(score))
        )
        .withColumn(
            "_dq_overall_status",
            F.lit(status)
        )
        .withColumn(
            "_dq_gold_timestamp",
            F.current_timestamp()
        )
    )

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            full_table_name(
                catalog,
                gold_schema,
                table
            )
        )
    )

    print(
        f"Gold CREATED: "
        f"{gold_table}"
    )

    return True


def create_quarantine(
    spark,
    df,
    condition,
    catalog,
    schema,
    table,
    cfg,
    run_id
):

    if condition is None:

        print(
            "Quarantined rows: 0"
        )

        return 0

    quarantine_schema = cfg[
        "framework"
    ][
        "quarantine_schema"
    ]

    quarantine_table_name = (
        f"quarantine_"
        f"{safe_column_name(table)}"
    )

    quarantine_df = (
        df
        .filter(condition)
        .withColumn(
            "_dq_run_id",
            F.lit(run_id)
        )
        .withColumn(
            "_dq_source_table",
            F.lit(
                f"{catalog}.{schema}.{table}"
            )
        )
        .withColumn(
            "_dq_quarantine_timestamp",
            F.current_timestamp()
        )
    )

    count = quarantine_df.count()

    if count == 0:

        print(
            "Quarantined rows: 0"
        )

        return 0

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            full_table_name(
                catalog,
                quarantine_schema,
                quarantine_table_name
            )
        )
    )

    print(
        f"Quarantined rows: {count}"
    )

    return count