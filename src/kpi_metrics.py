from pyspark.sql import functions as F


def _tbl(name: str) -> str:
    return "`" + "`.`".join(name.split(".")) + "`"


def _minutes_between(end_col, start_col):
    return (
        F.col(end_col).cast("long")
        - F.col(start_col).cast("long")
    ) / 60.0


def _resolve_incident_table(cfg):
    ml_cfg = cfg.get("ml_weighting", {})
    control_catalog = cfg["framework"]["control_catalog"]
    schema = ml_cfg.get("incident_schema")
    table_name = ml_cfg.get("incident_table_name")
    if not schema or not table_name:
        raise ValueError(
            "cfg['ml_weighting']['incident_schema']/['incident_table_name'] not configured."
        )
    return f"{control_catalog}.{schema}.{table_name}"


def _resolve_metric_weights_table(cfg):
    ml_cfg = cfg.get("ml_weighting", {})
    control_catalog = cfg["framework"]["control_catalog"]
    schema = ml_cfg.get("output_schema")
    table_name = ml_cfg.get("output_table_name")
    if not schema or not table_name:
        raise ValueError(
            "cfg['ml_weighting']['output_schema']/['output_table_name'] not configured."
        )
    return f"{control_catalog}.{schema}.{table_name}"


def _resolve_kpi_report_table(cfg):
    control_catalog = cfg["framework"]["control_catalog"]
    audit_schema = cfg["framework"]["audit_schema"]
    kpi_table_name = cfg["output_tables"]["kpi"]
    return f"{control_catalog}.{audit_schema}.{kpi_table_name}"


def _resolve_incident_level_report_table(cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    control_catalog = cfg["framework"]["control_catalog"]
    schema = kpi_cfg.get("incident_report_schema", cfg["framework"]["audit_schema"])
    table_name = kpi_cfg.get("incident_report_table_name", "dq_kpi_incident_report")
    return f"{control_catalog}.{schema}.{table_name}"


def _resolve_incident_log_table(cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    control_catalog = cfg["framework"]["control_catalog"]
    schema = kpi_cfg.get("incident_log_schema")
    table_name = kpi_cfg.get("incident_log_table_name")
    if not schema or not table_name:
        raise ValueError(
            "cfg['kpi_metrics']['incident_log_schema']/['incident_log_table_name'] not configured."
        )
    return f"{control_catalog}.{schema}.{table_name}"


def compute_incident_durations(df):
    result = (
        df
        .withColumn("ttd_min", _minutes_between("detected_timestamp", "event_timestamp"))
        .withColumn("tta_min", _minutes_between("ack_timestamp", "detected_timestamp"))
        .withColumn("ttr_min", _minutes_between("resolved_timestamp", "detected_timestamp"))
        .withColumn(
            "is_automated",
            F.when(F.upper(F.trim(F.col("detection_type"))) == "AUTOMATED", F.lit(1)).otherwise(F.lit(0))
        )
    )
    result = (
        result
        .withColumn("ttd_min", F.when(F.col("ttd_min") < 0, F.lit(None)).otherwise(F.col("ttd_min")))
        .withColumn("tta_min", F.when(F.col("tta_min") < 0, F.lit(None)).otherwise(F.col("tta_min")))
        .withColumn("ttr_min", F.when(F.col("ttr_min") < 0, F.lit(None)).otherwise(F.col("ttr_min")))
    )
    return result


def classify_kpi(spark, df, kpi_cfg):
    label_map = kpi_cfg.get("label_kpi_map", {})
    default_kpi = kpi_cfg.get("default_kpi", {"kpi": "OPS99", "kpi_name": "Unclassified", "dimension": "unclassified"})
    rows = []
    for label, value in label_map.items():
        rows.append({"label": label, "kpi": value["kpi"], "kpi_name": value["kpi_name"], "dimension": value["dimension"]})
    if not rows:
        return (
            df
            .withColumn("kpi", F.lit(default_kpi["kpi"]))
            .withColumn("kpi_name", F.lit(default_kpi["kpi_name"]))
            .withColumn("dimension", F.lit(default_kpi["dimension"]))
        )
    map_df = spark.createDataFrame(rows)
    joined = df.join(map_df, on="label", how="left")
    return (
        joined
        .withColumn("kpi", F.coalesce(F.col("kpi"), F.lit(default_kpi["kpi"])))
        .withColumn("kpi_name", F.coalesce(F.col("kpi_name"), F.lit(default_kpi["kpi_name"])))
        .withColumn("dimension", F.coalesce(F.col("dimension"), F.lit(default_kpi["dimension"])))
    )


def aggregate_incident_history(df):
    return (
        df
        .groupBy("incident_date", "kpi", "kpi_name", "dimension", "label")
        .agg(
            F.count(F.lit(1)).cast("int").alias("incident_count"),
            F.avg("ttd_min").alias("avg_mttd_minutes"),
            F.avg("tta_min").alias("avg_mtta_minutes"),
            F.avg("ttr_min").alias("avg_mttr_minutes"),
            (F.avg("is_automated") * F.lit(100.0)).alias("aidr_pct"),
            F.sum("rev_impact_amount").alias("total_rev_impact")
        )
    )


def add_severity_score(df, severity_cfg):
    duration_weight = float(severity_cfg.get("duration_weight", 0.5))
    revenue_weight = float(severity_cfg.get("revenue_weight", 0.5))
    stats = (
        df.select(
            F.min("avg_mttr_minutes").alias("min_mttr"),
            F.max("avg_mttr_minutes").alias("max_mttr"),
            F.min("total_rev_impact").alias("min_rev"),
            F.max("total_rev_impact").alias("max_rev")
        ).collect()[0]
    )
    min_mttr, max_mttr, min_rev, max_rev = stats["min_mttr"], stats["max_mttr"], stats["min_rev"], stats["max_rev"]
    if min_mttr is None or max_mttr is None or min_mttr == max_mttr:
        normalized_mttr = F.lit(0.0)
    else:
        normalized_mttr = (F.col("avg_mttr_minutes") - F.lit(min_mttr)) / (F.lit(max_mttr) - F.lit(min_mttr))
    if min_rev is None or max_rev is None or min_rev == max_rev:
        normalized_rev = F.lit(0.0)
    else:
        normalized_rev = (F.col("total_rev_impact") - F.lit(min_rev)) / (F.lit(max_rev) - F.lit(min_rev))
    return df.withColumn(
        "severity_score",
        (F.lit(duration_weight) * normalized_mttr + F.lit(revenue_weight) * normalized_rev) * F.lit(100.0)
    )


def merge_into_incident_history(spark, df, target_table):
    required_columns = [
        "incident_date", "kpi", "kpi_name", "dimension", "label",
        "incident_count", "avg_mttd_minutes", "avg_mtta_minutes",
        "avg_mttr_minutes", "aidr_pct", "total_rev_impact", "severity_score"
    ]

    if not spark.catalog.tableExists(target_table):
        (
            df.select(*required_columns)
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"incident_history created: {target_table}")
        return

    target_df = spark.table(target_table)
    target_columns = set(target_df.columns)
    missing_columns = [c for c in required_columns if c not in target_columns]

    if missing_columns:
        print("Existing incident_history table has an old/incomplete schema.")
        print(f"Missing columns: {missing_columns}")
        print("Rebuilding incident_history with the corrected KPI schema...")
        (
            df.select(*required_columns)
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"incident_history rebuilt: {target_table}")
        return

    from delta.tables import DeltaTable
    delta_tbl = DeltaTable.forName(spark, target_table)
    merge_condition = "t.incident_date = s.incident_date AND t.kpi = s.kpi AND t.label = s.label"
    (
        delta_tbl.alias("t")
        .merge(df.select(*required_columns).alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"incident_history merged into: {target_table}")


def run_kpi_metrics(spark, cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        print("KPI metrics skipped: disabled in config.")
        return None
    log_table = _resolve_incident_log_table(cfg)
    if not spark.catalog.tableExists(log_table):
        print(f"KPI metrics skipped: {log_table} does not exist yet.")
        return None
    print()
    print("=" * 70)
    print("KPI METRICS: MTTD / MTTA / MTTR / AIDR")
    print("=" * 70)
    raw = spark.table(log_table)
    raw_count = raw.count()
    print(f"Raw incident rows: {raw_count}")
    if raw_count == 0:
        print("KPI metrics skipped: incident log is empty.")
        return None
    with_durations = compute_incident_durations(raw)
    classified = classify_kpi(spark, with_durations, kpi_cfg)
    aggregated = aggregate_incident_history(classified)
    with_severity = add_severity_score(aggregated, kpi_cfg.get("severity_weights", {}))
    target_table = _resolve_incident_table(cfg)
    merge_into_incident_history(spark, with_severity, target_table)
    print()
    print(f"Incident history table: {target_table}")
    spark.table(target_table).orderBy("incident_date", "kpi").show(truncate=False)
    return with_severity


def _kpi_level_aggregate(history_df):
    weighted = (
        history_df
        .withColumn("w_mttd", F.col("avg_mttd_minutes") * F.col("incident_count"))
        .withColumn("w_mtta", F.col("avg_mtta_minutes") * F.col("incident_count"))
        .withColumn("w_mttr", F.col("avg_mttr_minutes") * F.col("incident_count"))
        .withColumn("w_aidr", F.col("aidr_pct") * F.col("incident_count"))
    )
    grouped = (
        weighted.groupBy("kpi", "kpi_name", "dimension")
        .agg(
            F.sum("incident_count").alias("incident_count"),
            (F.sum("w_mttd") / F.sum("incident_count")).alias("avg_mttd_minutes"),
            (F.sum("w_mtta") / F.sum("incident_count")).alias("avg_mtta_minutes"),
            (F.sum("w_mttr") / F.sum("incident_count")).alias("avg_mttr_minutes"),
            (F.sum("w_aidr") / F.sum("incident_count")).alias("aidr_pct"),
            F.sum("total_rev_impact").alias("total_rev_impact")
        )
    )
    return add_severity_score(grouped, {"duration_weight": 0.5, "revenue_weight": 0.5})


def compute_kpi_weights(spark, cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    ml_cfg = cfg.get("ml_weighting", {})
    if not kpi_cfg.get("enabled", False):
        print("KPI weighting skipped: KPI metrics disabled.")
        return None
    history_table = _resolve_incident_table(cfg)
    metric_weights_table = _resolve_metric_weights_table(cfg)
    if not spark.catalog.tableExists(history_table):
        print("KPI weighting skipped: incident_history table not found.")
        return None
    if not spark.catalog.tableExists(metric_weights_table):
        print("KPI weighting skipped: metric-level ML weights not found.")
        return None
    history = spark.table(history_table)
    if history.limit(1).count() == 0:
        print("KPI weighting skipped: incident_history is empty.")
        return None
    kpi_level = _kpi_level_aggregate(history)
    metric_df = spark.table(metric_weights_table)
    if "generated_timestamp" in metric_df.columns:
        latest_timestamp = metric_df.agg(F.max("generated_timestamp").alias("latest_timestamp")).collect()[0]["latest_timestamp"]
        if latest_timestamp is not None:
            metric_df = metric_df.filter(F.col("generated_timestamp") == latest_timestamp)
    if "kpi" not in metric_df.columns or "dynamic_weight" not in metric_df.columns:
        print("KPI weighting skipped: ML weight table does not contain kpi/dynamic_weight columns.")
        return None
    metric_rows = metric_df.select("kpi", "dynamic_weight").collect()
    metric_weight = {}
    for row in metric_rows:
        if row["kpi"] is None or row["dynamic_weight"] is None:
            continue
        metric_weight[row["kpi"]] = float(row["dynamic_weight"])
    if not metric_weight:
        print("KPI weighting skipped: no metric weights available.")
        return None
    feature_columns = ml_cfg.get("feature_columns", [])
    usable_features = [c for c in feature_columns if c in metric_weight and c in kpi_level.columns]
    if not usable_features:
        print("KPI weighting skipped: ML metric names do not match KPI feature columns.")
        return None
    stats = kpi_level.select(
        *[F.min(c).alias(f"min_{c}") for c in usable_features],
        *[F.max(c).alias(f"max_{c}") for c in usable_features]
    ).collect()[0]
    score_expr = F.lit(0.0)
    for c in usable_features:
        lo, hi = stats[f"min_{c}"], stats[f"max_{c}"]
        normalized = F.lit(0.0) if (lo is None or hi is None or lo == hi) else (F.col(c) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))
        score_expr = score_expr + normalized * F.lit(metric_weight[c])
    scored = kpi_level.withColumn("raw_score", score_expr)
    total = scored.agg(F.sum("raw_score").alias("total")).collect()[0]["total"]
    if total is None:
        total = 0.0
    if total == 0:
        n = scored.count()
        if n == 0:
            return None
        scored = scored.withColumn("weight", F.lit(100.0 / n))
    else:
        scored = scored.withColumn("weight", F.round((F.col("raw_score") / F.lit(total) * F.lit(100.0)), 4))
    result = (
        scored.select(
            "kpi", "kpi_name", "dimension", "weight", "incident_count",
            "avg_mttd_minutes", "avg_mtta_minutes", "avg_mttr_minutes",
            "aidr_pct", "total_rev_impact", "severity_score"
        ).withColumn("run_timestamp", F.current_timestamp())
    )
    report_table = _resolve_kpi_report_table(cfg)
    result.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(report_table)
    print()
    print(f"KPI dynamic weights written: {report_table}")
    print()
    print("KPI DYNAMIC WEIGHTS")
    result.orderBy(F.col("weight").desc()).show(truncate=False)
    weight_sum = result.agg(F.sum("weight").alias("weight_sum")).collect()[0]["weight_sum"]
    print(f"Total KPI weight: {weight_sum}")
    return result


def build_incident_level_kpi_report(spark, cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        return None
    log_table = _resolve_incident_log_table(cfg)
    weights_table = _resolve_kpi_report_table(cfg)
    if not spark.catalog.tableExists(log_table):
        print("Incident-level KPI report skipped: incident log not found.")
        return None
    if not spark.catalog.tableExists(weights_table):
        print("Incident-level KPI report skipped: dq_kpis not populated yet.")
        return None
    raw = spark.table(log_table)
    with_durations = compute_incident_durations(raw)
    classified = classify_kpi(spark, with_durations, kpi_cfg)
    weights_df = spark.table(weights_table)
    latest_ts = weights_df.agg(F.max("run_timestamp").alias("latest_ts")).collect()[0]["latest_ts"]
    if latest_ts is None:
        print("Incident-level KPI report skipped: dq_kpis has no valid run_timestamp.")
        return None
    latest_weights = (
        weights_df.filter(F.col("run_timestamp") == latest_ts)
        .select(
            F.col("kpi").alias("w_kpi"),
            F.col("weight").alias("ml_weight"),
            F.col("avg_mttd_minutes").alias("kpi_avg_mttd_minutes"),
            F.col("avg_mtta_minutes").alias("kpi_avg_mtta_minutes"),
            F.col("avg_mttr_minutes").alias("kpi_avg_mttr_minutes"),
            F.col("aidr_pct").alias("kpi_aidr_pct"),
            F.col("severity_score").alias("kpi_severity_score")
        )
    )
    report = (
        classified.join(latest_weights, classified["kpi"] == latest_weights["w_kpi"], "left")
        .select(
            F.col("incident_date").alias("date"),
            F.col("incident_id"),
            F.col("incident_description").alias("incident"),
            F.col("rev_impact_flag"),
            F.col("rev_impact_amount"),
            F.col("ds"),
            F.col("label").alias("label_code"),
            F.col("kpi"),
            F.col("kpi_name"),
            F.col("dimension"),
            F.col("ttd_min").alias("incident_mttd_min"),
            F.col("tta_min").alias("incident_mtta_min"),
            F.col("ttr_min").alias("incident_mttr_min"),
            F.col("is_automated"),
            F.col("ml_weight"),
            F.col("kpi_avg_mttd_minutes"),
            F.col("kpi_avg_mtta_minutes"),
            F.col("kpi_avg_mttr_minutes"),
            F.col("kpi_aidr_pct"),
            F.col("kpi_severity_score")
        )
        .withColumn("report_generated_timestamp", F.current_timestamp())
    )
    report_table = _resolve_incident_level_report_table(cfg)
    report.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(report_table)
    print()
    print(f"Incident-level KPI report written: {report_table}")
    spark.table(report_table).orderBy("date").show(truncate=False)
    return report