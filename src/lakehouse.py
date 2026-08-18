from pyspark.sql import functions as F


def ensure_schemas(spark, catalog, framework_cfg):
    """
    Create all framework schemas in the configured Unity Catalog.

    framework_cfg is the contents of the YAML 'framework' section.
    """

    schema_keys = [
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "audit_schema",
        "quarantine_schema",
        "candidate_schema",
    ]

    for key in schema_keys:

        schema = framework_cfg.get(key)

        if not schema:
            raise ValueError(
                f"Missing framework schema configuration: {key}"
            )

        print(
            f"Creating/checking schema: "
            f"{catalog}.{schema}"
        )

        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"`{catalog}`.`{schema}`"
        )


def write_bronze(
    spark,
    source_full_name,
    target_full_name
):
    """
    Copy source Unity Catalog table into Bronze
    and add DQ ingestion metadata.
    """

    source_parts = source_full_name.split(".")
    target_parts = target_full_name.split(".")

    source = (
        f"`{source_parts[0]}`."
        f"`{source_parts[1]}`."
        f"`{source_parts[2]}`"
    )

    target = (
        f"`{target_parts[0]}`."
        f"`{target_parts[1]}`."
        f"`{target_parts[2]}`"
    )

    df = spark.table(source)

    bronze_df = (
        df
        .withColumn(
            "_dqx_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_dqx_source_table",
            F.lit(source_full_name)
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(target)
    )

    return bronze_df


def quarantine_rows(
    spark,
    df,
    failed_keys,
    source_full_name,
    failed_rule,
    run_id,
    reason,
    target_full_name
):
    """
    Write failed records to the quarantine table.
    """

    if not failed_keys:
        return 0

    if not failed_keys[0]:
        return 0

    keys = list(failed_keys[0].keys())

    key_df = spark.createDataFrame(
        failed_keys
    )

    bad = df.join(
        key_df,
        keys,
        "inner"
    )

    source_parts = source_full_name.split(".")

    quarantine_df = (
        bad
        .withColumn(
            "_dqx_catalog",
            F.lit(source_parts[0])
        )
        .withColumn(
            "_dqx_schema",
            F.lit(source_parts[1])
        )
        .withColumn(
            "_dqx_table",
            F.lit(source_parts[2])
        )
        .withColumn(
            "_dqx_failed_rule",
            F.lit(failed_rule)
        )
        .withColumn(
            "_dqx_failure_reason",
            F.lit(reason)
        )
        .withColumn(
            "_dqx_pipeline_run_id",
            F.lit(run_id)
        )
        .withColumn(
            "_dqx_detected_timestamp",
            F.current_timestamp()
        )
    )

    target_parts = target_full_name.split(".")

    target = (
        f"`{target_parts[0]}`."
        f"`{target_parts[1]}`."
        f"`{target_parts[2]}`"
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true"
        )
        .saveAsTable(target)
    )

    return quarantine_df.count()


def build_silver(
    spark,
    bronze_full_name,
    quarantine_full_name,
    target_full_name,
    key_columns
):
    """
    Build Silver from Bronze.

    Current generic implementation:
    - Deduplicate using configured unique keys
    - Otherwise deduplicate complete records
    """

    bronze_parts = bronze_full_name.split(".")

    bronze_table = (
        f"`{bronze_parts[0]}`."
        f"`{bronze_parts[1]}`."
        f"`{bronze_parts[2]}`"
    )

    bronze = spark.table(
        bronze_table
    )

    if key_columns:

        valid_keys = [
            key
            for key in key_columns
            if key in bronze.columns
        ]

        if valid_keys:
            clean = bronze.dropDuplicates(
                valid_keys
            )
        else:
            clean = bronze.dropDuplicates()

    else:
        clean = bronze.dropDuplicates()

    target_parts = target_full_name.split(".")

    target = (
        f"`{target_parts[0]}`."
        f"`{target_parts[1]}`."
        f"`{target_parts[2]}`"
    )

    (
        clean.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(target)
    )

    return clean


def build_gold(
    spark,
    silver_full_name,
    target_full_name
):
    """
    Build Gold from Silver.

    Generic implementation currently preserves
    the validated Silver dataset.
    """

    silver_parts = silver_full_name.split(".")

    silver_table = (
        f"`{silver_parts[0]}`."
        f"`{silver_parts[1]}`."
        f"`{silver_parts[2]}`"
    )

    df = spark.table(
        silver_table
    )

    target_parts = target_full_name.split(".")

    target = (
        f"`{target_parts[0]}`."
        f"`{target_parts[1]}`."
        f"`{target_parts[2]}`"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(target)
    )

    return df