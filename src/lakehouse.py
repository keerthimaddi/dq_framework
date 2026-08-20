# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# LAKEHOUSE
# DATABRICKS UNITY CATALOG ONLY
# ============================================================

import re

from pyspark.sql import functions as F


# ============================================================
# IDENTIFIER HELPERS
# ============================================================

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


# ============================================================
# UNITY CATALOG VALIDATION
# ============================================================

def validate_unity_catalog(
    spark,
    catalog
):

    try:

        catalogs = {
            row.catalog
            for row in spark.sql(
                "SHOW CATALOGS"
            ).collect()
        }

    except Exception as exc:

        raise RuntimeError(
            "Unable to access Unity Catalog."
        ) from exc

    if catalog not in catalogs:

        raise RuntimeError(
            f"Unity Catalog '{catalog}' "
            f"does not exist or is not accessible."
        )

    return True


# ============================================================
# COLUMN NORMALIZATION
# ============================================================

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

        mapping[
            original
        ] = candidate

    for (
        original,
        safe
    ) in mapping.items():

        if original != safe:

            df = df.withColumnRenamed(
                original,
                safe
            )

    return df, mapping


# ============================================================
# READ SOURCE TABLE
# ============================================================

def read_source_table(
    spark,
    catalog,
    schema,
    table
):

    validate_unity_catalog(
        spark,
        catalog
    )

    table_name = full_table_name(
        catalog,
        schema,
        table
    )

    print(
        f"Reading Unity Catalog table: "
        f"{catalog}.{schema}.{table}"
    )

    try:

        return spark.table(
            table_name
        )

    except Exception as exc:

        raise RuntimeError(
            f"Unable to read Unity Catalog table:\n"
            f"{catalog}.{schema}.{table}\n"
            f"Reason: {exc}"
        ) from exc


# ============================================================
# CREATE FRAMEWORK SCHEMAS
# ============================================================

def create_framework_schemas(
    spark,
    cfg
):

    framework = cfg[
        "framework"
    ]

    catalog = framework[
        "catalog"
    ]

    validate_unity_catalog(
        spark,
        catalog
    )

    schemas = [
        framework["bronze_schema"],
        framework["silver_schema"],
        framework["gold_schema"],
        framework["audit_schema"],
        framework["quarantine_schema"],
        framework["candidate_schema"],
    ]

    print()
    print(
        f"Creating framework schemas "
        f"under Unity Catalog: {catalog}"
    )

    for schema in schemas:

        schema_name = (
            f"{quote_identifier(catalog)}."
            f"{quote_identifier(schema)}"
        )

        print(
            f"Checking: "
            f"{catalog}.{schema}"
        )

        try:

            spark.sql(
                f"""
                CREATE SCHEMA IF NOT EXISTS
                {schema_name}
                """
            )

            print(
                f"READY: "
                f"{catalog}.{schema}"
            )

        except Exception as exc:

            raise RuntimeError(
                f"Unable to create Unity Catalog schema:\n"
                f"{catalog}.{schema}\n"
                f"Reason: {exc}"
            ) from exc


# ============================================================
# WRITE DELTA TABLE
# ============================================================

def write_delta_table(
    df,
    spark,
    catalog,
    schema,
    table,
    mode="overwrite"
):

    validate_unity_catalog(
        spark,
        catalog
    )

    table_name = full_table_name(
        catalog,
        schema,
        table
    )

    print(
        f"Writing Delta table: "
        f"{catalog}.{schema}.{table}"
    )

    try:

        (
            df.write
            .format("delta")
            .mode(mode)
            .option(
                "overwriteSchema",
                "true"
            )
            .saveAsTable(
                table_name
            )
        )

    except Exception as exc:

        raise RuntimeError(
            f"Unable to write Unity Catalog Delta table:\n"
            f"{catalog}.{schema}.{table}\n"
            f"Reason: {exc}"
        ) from exc

    return table_name


# ============================================================
# WRITE BRONZE
# ============================================================

def write_bronze(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg
):

    safe_df, mapping = (
        normalize_columns(df)
    )

    bronze_schema = cfg[
        "framework"
    ][
        "bronze_schema"
    ]

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

    write_delta_table(
        safe_df,
        spark,
        catalog,
        bronze_schema,
        table,
        mode="overwrite"
    )

    return safe_df, mapping


# ============================================================
# CREATE SILVER
# ============================================================

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

    gate_enabled = gate.get(
        "enabled",
        True
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

    if (
        gate_enabled
        and float(score) < minimum_score
    ):

        print()
        print(
            "SILVER QUALITY GATE: BLOCKED"
        )

        print(
            f"Score    : {score}"
        )

        print(
            f"Required : {minimum_score}"
        )

        return False

    print()
    print(
        "SILVER QUALITY GATE: PASSED"
    )

    print(
        f"Score    : {score}"
    )

    print(
        f"Required : {minimum_score}"
    )

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

    write_delta_table(
        silver_df,
        spark,
        catalog,
        silver_schema,
        table,
        mode="overwrite"
    )

    print(
        f"Silver CREATED: "
        f"{catalog}.{silver_schema}.{table}"
    )

    return True


# ============================================================
# CREATE GOLD
# ============================================================

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

    if not table_exists(
        spark,
        catalog,
        silver_schema,
        table
    ):

        print(
            f"Gold BLOCKED: Silver does not exist."
        )

        return False

    silver_name = full_table_name(
        catalog,
        silver_schema,
        table
    )

    df = spark.table(
        silver_name
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

    write_delta_table(
        gold_df,
        spark,
        catalog,
        gold_schema,
        table,
        mode="overwrite"
    )

    print(
        f"Gold CREATED: "
        f"{catalog}.{gold_schema}.{table}"
    )

    return True


# ============================================================
# CREATE QUARANTINE
# ============================================================

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

    quarantine_table = (
        "quarantine_"
        + safe_column_name(table)
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

    write_delta_table(
        quarantine_df,
        spark,
        catalog,
        quarantine_schema,
        quarantine_table,
        mode="append"
    )

    print(
        f"Quarantined rows: {count}"
    )

    return count


# ============================================================
# TABLE EXISTS
# ============================================================

def table_exists(
    spark,
    catalog,
    schema,
    table
):

    validate_unity_catalog(
        spark,
        catalog
    )

    table_name = full_table_name(
        catalog,
        schema,
        table
    )

    try:

        return spark.catalog.tableExists(
            table_name
        )

    except Exception:

        try:

            spark.table(
                table_name
            )

            return True

        except Exception:

            return False