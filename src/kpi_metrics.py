# ============================================================
# KPI METRICS
# MTTD / MTTA / MTTR / AIDR / SEVERITY
# ============================================================

from pyspark.sql import functions as F


def _minutes_between(
    end_column,
    start_column,
):

    return (
        F.col(end_column).cast("long")
        -
        F.col(start_column).cast("long")
    ) / 60.0


def _resolve_incident_table(cfg):

    table = (
        cfg.get("ml_weighting", {})
        .get("incident_table")
    )

    if not table:
        raise ValueError(
            "incident_table is not configured."
        )

    return table


def _resolve_metric_weights_table(cfg):

    table = (
        cfg.get("ml_weighting", {})
        .get("output_table")
    )

    if not table:
        raise ValueError(
            "ML output_table is not configured."
        )

    return table


def _resolve_kpi_report_table(cfg):

    framework = cfg["framework"]

    return (
        f"{framework['catalog']}."
        f"{framework['audit_schema']}."
        f"{cfg['output_tables']['kpi']}"
    )


def _resolve_incident_level_report_table(cfg):

    return (
        cfg.get("kpi_metrics", {})
        .get(
            "incident_report_table",
            (
                f"{cfg['framework']['catalog']}."
                f"{cfg['framework']['audit_schema']}."
                "dq_kpi_incident_report"
            ),
        )
    )


# ============================================================
# INCIDENT DURATIONS
# ============================================================

def compute_incident_durations(df):

    result = (
        df
        .withColumn(
            "ttd_min",
            _minutes_between(
                "detected_timestamp",
                "event_timestamp",
            ),
        )
        .withColumn(
            "tta_min",
            _minutes_between(
                "ack_timestamp",
                "detected_timestamp",
            ),
        )
        .withColumn(
            "ttr_min",
            _minutes_between(
                "resolved_timestamp",
                "detected_timestamp",
            ),
        )
        .withColumn(
            "is_automated",
            F.when(
                F.upper(
                    F.trim(
                        F.col("detection_type")
                    )
                ) == "AUTOMATED",
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    )

    for column in [
        "ttd_min",
        "tta_min",
        "ttr_min",
    ]:

        result = result.withColumn(
            column,
            F.when(
                F.col(column) < 0,
                None,
            ).otherwise(
                F.col(column)
            ),
        )

    return result


# ============================================================
# KPI CLASSIFICATION
# ============================================================

def classify_kpi(
    spark,
    df,
    kpi_cfg,
):

    label_map = kpi_cfg.get(
        "label_kpi_map",
        {},
    )

    default_kpi = kpi_cfg.get(
        "default_kpi",
        {
            "kpi": "OPS99",
            "kpi_name": "Unclassified",
            "dimension": "unclassified",
        },
    )

    rows = []

    for label, mapping in label_map.items():

        rows.append({
            "label": label,
            "kpi": mapping["kpi"],
            "kpi_name": mapping["kpi_name"],
            "dimension": mapping["dimension"],
        })

    if not rows:

        return (
            df
            .withColumn(
                "kpi",
                F.lit(default_kpi["kpi"]),
            )
            .withColumn(
                "kpi_name",
                F.lit(default_kpi["kpi_name"]),
            )
            .withColumn(
                "dimension",
                F.lit(default_kpi["dimension"]),
            )
        )

    map_df = spark.createDataFrame(rows)

    result = df.join(
        map_df,
        on="label",
        how="left",
    )

    return (
        result
        .withColumn(
            "kpi",
            F.coalesce(
                F.col("kpi"),
                F.lit(default_kpi["kpi"]),
            ),
        )
        .withColumn(
            "kpi_name",
            F.coalesce(
                F.col("kpi_name"),
                F.lit(default_kpi["kpi_name"]),
            ),
        )
        .withColumn(
            "dimension",
            F.coalesce(
                F.col("dimension"),
                F.lit(default_kpi["dimension"]),
            ),
        )
    )


# ============================================================
# INCIDENT HISTORY
# ============================================================

def aggregate_incident_history(df):

    return (
        df
        .groupBy(
            "incident_date",
            "kpi",
            "kpi_name",
            "dimension",
            "label",
        )
        .agg(
            F.count(
                F.lit(1)
            )
            .cast("int")
            .alias("incident_count"),

            F.avg(
                "ttd_min"
            ).alias(
                "avg_mttd_minutes"
            ),

            F.avg(
                "tta_min"
            ).alias(
                "avg_mtta_minutes"
            ),

            F.avg(
                "ttr_min"
            ).alias(
                "avg_mttr_minutes"
            ),

            (
                F.avg("is_automated")
                * 100.0
            ).alias(
                "aidr_pct"
            ),

            F.sum(
                "rev_impact_amount"
            ).alias(
                "total_rev_impact"
            ),
        )
    )


def add_severity_score(
    df,
    severity_cfg,
):

    duration_weight = float(
        severity_cfg.get(
            "duration_weight",
            0.5,
        )
    )

    revenue_weight = float(
        severity_cfg.get(
            "revenue_weight",
            0.5,
        )
    )

    stats = (
        df.select(
            F.min(
                "avg_mttr_minutes"
            ).alias("min_mttr"),

            F.max(
                "avg_mttr_minutes"
            ).alias("max_mttr"),

            F.min(
                "total_rev_impact"
            ).alias("min_rev"),

            F.max(
                "total_rev_impact"
            ).alias("max_rev"),
        )
        .collect()[0]
    )

    if (
        stats["min_mttr"] is None
        or stats["max_mttr"] is None
    ):

        mttr_expr = F.lit(0.0)

    elif stats["max_mttr"] == stats["min_mttr"]:

        mttr_expr = F.lit(0.0)

    else:

        mttr_expr = (
            F.col("avg_mttr_minutes")
            - F.lit(stats["min_mttr"])
        ) / (
            F.lit(stats["max_mttr"])
            - F.lit(stats["min_mttr"])
        )

    if (
        stats["min_rev"] is None
        or stats["max_rev"] is None
    ):

        revenue_expr = F.lit(0.0)

    elif stats["max_rev"] == stats["min_rev"]:

        revenue_expr = F.lit(0.0)

    else:

        revenue_expr = (
            F.col("total_rev_impact")
            - F.lit(stats["min_rev"])
        ) / (
            F.lit(stats["max_rev"])
            - F.lit(stats["min_rev"])
        )

    return df.withColumn(
        "severity_score",
        (
            F.lit(duration_weight)
            * mttr_expr
            +
            F.lit(revenue_weight)
            * revenue_expr
        )
        * 100.0,
    )


# ============================================================
# MERGE INCIDENT HISTORY
# ============================================================

def merge_into_incident_history(
    spark,
    df,
    target_table,
):

    if not spark.catalog.tableExists(
        target_table
    ):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .saveAsTable(
                target_table
            )
        )

        print(
            f"Created incident history: "
            f"{target_table}"
        )

        return

    # No DeltaTable Python import.
    # Use Spark SQL MERGE instead.

    temp_view = (
        "tmp_incident_history_"
        + str(abs(hash(target_table)))
    )

    df.createOrReplaceTempView(
        temp_view
    )

    target = target_table

    spark.sql(
        f"""
        MERGE INTO `{target.replace('.', '`.`')}` AS t
        USING `{temp_view}` AS s
        ON  t.incident_date = s.incident_date
        AND t.kpi = s.kpi
        AND t.label = s.label

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
        """
    )

    print(
        f"Merged incident history: "
        f"{target_table}"
    )


# ============================================================
# RUN KPI METRICS
# ============================================================

def run_kpi_metrics(
    spark,
    cfg,
):

    kpi_cfg = cfg.get(
        "kpi_metrics",
        {},
    )

    if not kpi_cfg.get(
        "enabled",
        False,
    ):

        print(
            "KPI metrics disabled."
        )

        return None

    log_table = kpi_cfg.get(
        "incident_log_table"
    )

    if not log_table:

        print(
            "KPI metrics skipped: "
            "incident_log_table missing."
        )

        return None

    if not spark.catalog.tableExists(
        log_table
    ):

        print(
            f"KPI metrics skipped: "
            f"{log_table} does not exist."
        )

        return None

    print()
    print("=" * 70)
    print(
        "KPI METRICS: "
        "MTTD / MTTA / MTTR / AIDR / SEVERITY"
    )
    print("=" * 70)

    raw = spark.table(log_table)

    print(
        f"Raw incident rows: "
        f"{raw.count()}"
    )

    if raw.limit(1).count() == 0:

        print(
            "Incident log is empty."
        )

        return None

    with_durations = (
        compute_incident_durations(
            raw
        )
    )

    classified = classify_kpi(
        spark,
        with_durations,
        kpi_cfg,
    )

    aggregated = (
        aggregate_incident_history(
            classified
        )
    )

    with_severity = (
        add_severity_score(
            aggregated,
            kpi_cfg.get(
                "severity_weights",
                {},
            ),
        )
    )

    target_table = (
        _resolve_incident_table(
            cfg
        )
    )

    merge_into_incident_history(
        spark,
        with_severity,
        target_table,
    )

    return with_severity


# ============================================================
# STAGE B - KPI WEIGHTS
# ============================================================

def _kpi_level_aggregate(
    history_df,
):

    weighted = (
        history_df
        .withColumn(
            "w_mttd",
            F.col("avg_mttd_minutes")
            * F.col("incident_count"),
        )
        .withColumn(
            "w_mtta",
            F.col("avg_mtta_minutes")
            * F.col("incident_count"),
        )
        .withColumn(
            "w_mttr",
            F.col("avg_mttr_minutes")
            * F.col("incident_count"),
        )
        .withColumn(
            "w_aidr",
            F.col("aidr_pct")
            * F.col("incident_count"),
        )
    )

    result = (
        weighted
        .groupBy(
            "kpi",
            "kpi_name",
            "dimension",
        )
        .agg(
            F.sum(
                "incident_count"
            ).alias(
                "incident_count"
            ),

            (
                F.sum("w_mttd")
                /
                F.sum("incident_count")
            ).alias(
                "avg_mttd_minutes"
            ),

            (
                F.sum("w_mtta")
                /
                F.sum("incident_count")
            ).alias(
                "avg_mtta_minutes"
            ),

            (
                F.sum("w_mttr")
                /
                F.sum("incident_count")
            ).alias(
                "avg_mttr_minutes"
            ),

            (
                F.sum("w_aidr")
                /
                F.sum("incident_count")
            ).alias(
                "aidr_pct"
            ),

            F.sum(
                "total_rev_impact"
            ).alias(
                "total_rev_impact"
            ),
        )
    )

    return add_severity_score(
        result,
        {
            "duration_weight": 0.5,
            "revenue_weight": 0.5,
        },
    )


# ============================================================
# READ STAGE A ML WEIGHTS
# ============================================================

def _load_stage_a_weights(
    spark,
    table,
):

    if not spark.catalog.tableExists(
        table
    ):
        return {}

    df = spark.table(table)

    required = {
        "kpi",
        "dynamic_weight",
    }

    if not required.issubset(
        set(df.columns)
    ):

        print(
            "Stage A table does not contain "
            "kpi/dynamic_weight."
        )

        return {}

    if "generated_timestamp" in df.columns:

        latest = (
            df.select(
                F.max(
                    "generated_timestamp"
                ).alias("latest")
            )
            .collect()[0]["latest"]
        )

        if latest is not None:

            df = df.filter(
                F.col(
                    "generated_timestamp"
                ) == latest
            )

    return {
        row["kpi"]:
        float(row["dynamic_weight"])
        for row in df.select(
            "kpi",
            "dynamic_weight",
        ).collect()
        if row["dynamic_weight"] is not None
    }


# ============================================================
# COMPUTE KPI WEIGHTS
# ============================================================

def compute_kpi_weights(
    spark,
    cfg,
):

    kpi_cfg = cfg.get(
        "kpi_metrics",
        {}
    )

    if not kpi_cfg.get(
        "enabled",
        False,
    ):
        return None

    history_table = (
        _resolve_incident_table(
            cfg
        )
    )

    stage_a_table = (
        _resolve_metric_weights_table(
            cfg
        )
    )

    if not spark.catalog.tableExists(
        history_table
    ):

        print(
            "KPI weighting skipped: "
            "incident_history missing."
        )

        return None

    history = spark.table(
        history_table
    )

    kpi_level = _kpi_level_aggregate(
        history
    )

    stage_a_weights = (
        _load_stage_a_weights(
            spark,
            stage_a_table,
        )
    )

    if not stage_a_weights:

        print(
            "KPI weighting skipped: "
            "Stage A weights unavailable."
        )

        return None

    feature_columns = (
        cfg.get(
            "ml_weighting",
            {}
        )
        .get(
            "feature_columns",
            [],
        )
    )

    usable_features = [
        column
        for column in feature_columns
        if column in kpi_level.columns
        and column in stage_a_weights
    ]

    if not usable_features:

        print(
            "KPI weighting skipped: "
            "no matching ML feature weights."
        )

        return None

    stats = (
        kpi_level
        .select(
            *[
                F.min(column).alias(
                    f"min_{column}"
                )
                for column in usable_features
            ],
            *[
                F.max(column).alias(
                    f"max_{column}"
                )
                for column in usable_features
            ],
        )
        .collect()[0]
    )

    scored = kpi_level

    score_expression = F.lit(0.0)

    for column in usable_features:

        lo = stats[
            f"min_{column}"
        ]

        hi = stats[
            f"max_{column}"
        ]

        if (
            lo is None
            or hi is None
            or hi == lo
        ):

            normalized = F.lit(0.0)

        else:

            normalized = (
                F.col(column)
                - F.lit(lo)
            ) / (
                F.lit(hi)
                - F.lit(lo)
            )

        score_expression = (
            score_expression
            +
            normalized
            * F.lit(
                stage_a_weights[
                    column
                ]
            )
        )

    scored = scored.withColumn(
        "raw_score",
        score_expression,
    )

    total = (
        scored
        .agg(
            F.sum(
                "raw_score"
            ).alias("total")
        )
        .collect()[0]["total"]
    )

    if not total:

        count = scored.count()

        scored = scored.withColumn(
            "weight",
            F.lit(
                100.0 / count
                if count
                else 0.0
            ),
        )

    else:

        scored = scored.withColumn(
            "weight",
            F.round(
                F.col("raw_score")
                / F.lit(total)
                * 100.0,
                4,
            ),
        )

    result = (
        scored
        .select(
            "kpi",
            "kpi_name",
            "dimension",
            "weight",
            "incident_count",
            "avg_mttd_minutes",
            "avg_mtta_minutes",
            "avg_mttr_minutes",
            "aidr_pct",
            "total_rev_impact",
            "severity_score",
        )
        .withColumn(
            "run_timestamp",
            F.current_timestamp(),
        )
    )

    report_table = (
        _resolve_kpi_report_table(
            cfg
        )
    )

    (
        result.write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true",
        )
        .saveAsTable(
            report_table
        )
    )

    print(
        f"KPI weights written to: "
        f"{report_table}"
    )

    result.orderBy(
        F.col("weight").desc()
    ).show(
        truncate=False
    )

    return result


# ============================================================
# FLAT INCIDENT-LEVEL KPI REPORT
# ============================================================

def build_incident_level_kpi_report(
    spark,
    cfg,
):

    kpi_cfg = cfg.get(
        "kpi_metrics",
        {}
    )

    if not kpi_cfg.get(
        "enabled",
        False,
    ):
        return None

    log_table = kpi_cfg.get(
        "incident_log_table"
    )

    kpi_table = (
        _resolve_kpi_report_table(
            cfg
        )
    )

    report_table = (
        _resolve_incident_level_report_table(
            cfg
        )
    )

    if not spark.catalog.tableExists(
        log_table
    ):

        print(
            "Incident report skipped: "
            "incident log missing."
        )

        return None

    if not spark.catalog.tableExists(
        kpi_table
    ):

        print(
            "Incident report skipped: "
            "dq_kpis missing."
        )

        return None

    raw = spark.table(
        log_table
    )

    classified = classify_kpi(
        spark,
        compute_incident_durations(
            raw
        ),
        kpi_cfg,
    )

    weights = spark.table(
        kpi_table
    )

    latest_ts = (
        weights
        .select(
            F.max(
                "run_timestamp"
            ).alias("latest")
        )
        .collect()[0]["latest"]
    )

    if latest_ts is None:
        return None

    latest_weights = (
        weights
        .filter(
            F.col(
                "run_timestamp"
            ) == latest_ts
        )
        .select(
            F.col("kpi").alias(
                "weight_kpi"
            ),
            F.col("weight").alias(
                "ml_weight"
            ),
        )
    )

    report = (
        classified
        .join(
            latest_weights,
            F.col("kpi")
            == F.col("weight_kpi"),
            "left",
        )
        .select(
            F.col("incident_date").alias(
                "date"
            ),
            F.col("incident_id"),
            F.col(
                "incident_description"
            ).alias(
                "incident"
            ),
            F.col(
                "rev_impact_flag"
            ),
            F.col(
                "rev_impact_amount"
            ),
            F.col("ds"),
            F.col("label").alias(
                "label_code"
            ),
            F.col("kpi"),
            F.col("kpi_name"),
            F.col("ttd_min").alias(
                "incident_mttd_min"
            ),
            F.col("tta_min").alias(
                "incident_mtta_min"
            ),
            F.col("ttr_min").alias(
                "incident_mttr_min"
            ),
            F.col("is_automated"),
            F.col("ml_weight"),
        )
        .withColumn(
            "report_generated_timestamp",
            F.current_timestamp(),
        )
    )

    (
        report.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .saveAsTable(
            report_table
        )
    )

    print(
        f"Incident-level KPI report "
        f"written to: {report_table}"
    )

    report.orderBy(
        "date",
        "incident_id",
    ).show(
        truncate=False
    )

    return report