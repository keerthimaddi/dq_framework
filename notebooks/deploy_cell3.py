import pathlib

base = "/Workspace/Users/keerthi.maddi@mediamint.com/dq_framework/src"

kpi_content = '''
# ============================================================
# KPI METRICS MODULE
# Requirement 02 - Steps 1-4 + whiteboard-style final report
#
# Writes into wmg.demo.incident_history using the EXACT column
# names already declared in table_rules for that table:
#   incident_count, avg_mttd_minutes, avg_mtta_minutes,
#   avg_mttr_minutes, aidr_pct, severity_score, total_rev_impact
#
# NEW in this version: build_incident_level_kpi_report() - the
# single flat table requested for today's demo. One row per
# incident, joined with that incident's KPI code and the current
# ML-derived weight for that KPI. This is NOT a dashboard - it's
# a table you SELECT * FROM and hand to a reviewer.
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# HELPERS
# ============================================================

def _tbl(name: str) -> str:
    return "`" + "`.`".join(name.split(".")) + "`"


def _minutes_between(end_col, start_col):
    return (F.col(end_col).cast("long") - F.col(start_col).cast("long")) / 60.0


def _resolve_incident_table(cfg):
    ml_cfg = cfg.get("ml_weighting", {})
    table = ml_cfg.get("incident_table")
    if not table:
        raise ValueError("cfg['ml_weighting']['incident_table'] is not configured.")
    return table


def _resolve_metric_weights_table(cfg):
    ml_cfg = cfg.get("ml_weighting", {})
    table = ml_cfg.get("output_table")
    if not table:
        raise ValueError("cfg['ml_weighting']['output_table'] is not configured.")
    return table


def _resolve_kpi_report_table(cfg):
    framework = cfg["framework"]
    audit_schema = framework["audit_schema"]
    kpi_table_name = cfg["output_tables"]["kpi"]
    return f"{framework['catalog']}.{audit_schema}.{kpi_table_name}"


def _resolve_incident_level_report_table(cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    default = f"{cfg['framework']['catalog']}.{cfg['framework']['audit_schema']}.dq_kpi_incident_report"
    return kpi_cfg.get("incident_report_table", default)


# ============================================================
# STEP 2 - PER-INCIDENT METRICS (TTD / TTA / TTR / automated flag)
# ============================================================

def compute_incident_durations(df):
    return (
        df
        .withColumn("ttd_min", _minutes_between("detected_timestamp", "event_timestamp"))
        .withColumn("tta_min", _minutes_between("ack_timestamp", "detected_timestamp"))
        .withColumn("ttr_min", _minutes_between("resolved_timestamp", "detected_timestamp"))
        .withColumn(
            "is_automated",
            F.when(F.upper(F.trim(F.col("detection_type"))) == "AUTOMATED", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("ttd_min", F.when(F.col("ttd_min") < 0, None).otherwise(F.col("ttd_min")))
        .withColumn("tta_min", F.when(F.col("tta_min") < 0, None).otherwise(F.col("tta_min")))
        .withColumn("ttr_min", F.when(F.col("ttr_min") < 0, None).otherwise(F.col("ttr_min")))
    )


# ============================================================
# CLASSIFY EACH INCIDENT TO A KPI
#
# IMPORTANT CHANGE: kpi codes in label_kpi_map now use the SAME
# ids as dq_rules.yml (DQ01, DQ06, DQ10, DQ03, DQ13, DQ07) for
# every label that genuinely corresponds to one of the 16 DQ
# dimensions. This is what lets weight_resolver.py match a
# dynamic KPI weight directly onto a DQ rule id. Labels that are
# purely operational (not one of the 16 DQ dimensions, e.g.
# "Availability") use an OPS-prefixed code instead - these still
# get tracked and reported, they just won't override a DQ weight,
# which is correct since there's no matching DQ check for them.
# ============================================================

def classify_kpi(spark, df, kpi_cfg):
    label_map = kpi_cfg.get("label_kpi_map", {})
    default_kpi = kpi_cfg.get(
        "default_kpi", {"kpi": "OPS99", "kpi_name": "Unclassified", "dimension": "unclassified"}
    )

    rows = [
        {"label": label, "kpi": v["kpi"], "kpi_name": v["kpi_name"], "dimension": v["dimension"]}
        for label, v in label_map.items()
    ]

    if not rows:
        return (
            df
            .withColumn("kpi", F.lit(default_kpi["kpi"]))
            .withColumn("kpi_name", F.lit(default_kpi["kpi_name"]))
            .withColumn("dimension", F.lit(default_kpi["dimension"]))
        )

    map_df = spark.createDataFrame(rows)
    joined = df.join(map_df, on="label", how="left")
    joined = (
        joined
        .withColumn("kpi", F.coalesce(F.col("kpi"), F.lit(default_kpi["kpi"])))
        .withColumn("kpi_name", F.coalesce(F.col("kpi_name"), F.lit(default_kpi["kpi_name"])))
        .withColumn("dimension", F.coalesce(F.col("dimension"), F.lit(default_kpi["dimension"])))
    )
    return joined


# ============================================================
# STEP 3 - AGGREGATE TO incident_history GRAIN
# ============================================================

def aggregate_incident_history(df):
    return (
        df.groupBy("incident_date", "kpi", "kpi_name", "dimension", "label")
        .agg(
            F.count(F.lit(1)).cast("int").alias("incident_count"),
            F.avg("ttd_min").alias("avg_mttd_minutes"),
            F.avg("tta_min").alias("avg_mtta_minutes"),
            F.avg("ttr_min").alias("avg_mttr_minutes"),
            (F.avg("is_automated") * F.lit(100.0)).alias("aidr_pct"),
            F.sum("rev_impact_amount").alias("total_rev_impact"),
        )
    )


def add_severity_score(df, severity_cfg):
    duration_weight = float(severity_cfg.get("duration_weight", 0.5))
    revenue_weight = float(severity_cfg.get("revenue_weight", 0.5))

    stats = df.select(
        F.min("avg_mttr_minutes").alias("min_mttr"),
        F.max("avg_mttr_minutes").alias("max_mttr"),
        F.min("total_rev_impact").alias("min_rev"),
        F.max("total_rev_impact").alias("max_rev"),
    ).collect()[0]

    def norm(col_name, lo, hi):
        if lo is None or hi is None or hi == lo:
            return F.lit(0.0)
        return (F.col(col_name) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))

    norm_mttr = norm("avg_mttr_minutes", stats["min_mttr"], stats["max_mttr"])
    norm_rev = norm("total_rev_impact", stats["min_rev"], stats["max_rev"])

    return df.withColumn(
        "severity_score",
        (F.lit(duration_weight) * norm_mttr + F.lit(revenue_weight) * norm_rev) * F.lit(100.0)
    )


def merge_into_incident_history(spark, df, target_table):
    from delta.tables import DeltaTable

    required_merge_cols = {"incident_date", "kpi", "label"}
    needs_overwrite = True

    if spark.catalog.tableExists(target_table):
        existing_cols = set(spark.table(target_table).columns)
        if required_merge_cols.issubset(existing_cols):
            needs_overwrite = False
        else:
            missing = required_merge_cols - existing_cols
            print(
                f"incident_history schema mismatch: {target_table} is missing "
                f"{missing} - this table predates kpi_metrics.py's schema (it was "
                f"seeded some other way). A MERGE can't target columns that don't "
                f"exist, so overwriting once with the correct schema "
                f"(incident_date, kpi, kpi_name, dimension, label + 7 metric "
                f"columns). Future runs will merge normally from here on."
            )

    if needs_overwrite:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"incident_history (re)created with correct schema: {target_table} ({df.count()} rows)")
        return

    delta_tbl = DeltaTable.forName(spark, target_table)
    (
        delta_tbl.alias("t")
        .merge(
            df.alias("s"),
            "t.incident_date = s.incident_date AND t.kpi = s.kpi AND t.label = s.label"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"incident_history merged into: {target_table}")


# ============================================================
# ORCHESTRATOR - STEPS 1-3
# ============================================================

def run_kpi_metrics(spark, cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        print("KPI metrics skipped: disabled in config.")
        return None

    log_table = kpi_cfg.get("incident_log_table")
    if not log_table:
        print("KPI metrics skipped: incident_log_table not configured.")
        return None
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

    spark.table(target_table).orderBy("incident_date", "kpi").show(truncate=False)
    return with_severity


# ============================================================
# STAGE B - PER-KPI DYNAMIC WEIGHTS
# ============================================================

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
            F.sum("total_rev_impact").alias("total_rev_impact"),
        )
    )
    return add_severity_score(grouped, {"duration_weight": 0.5, "revenue_weight": 0.5})


def compute_kpi_weights(spark, cfg):
    kpi_cfg = cfg.get("kpi_metrics", {})
    ml_cfg = cfg.get("ml_weighting", {})
    if not kpi_cfg.get("enabled", False):
        return None

    history_table = _resolve_incident_table(cfg)
    metric_weights_table = _resolve_metric_weights_table(cfg)

    if not spark.catalog.tableExists(history_table):
        print("KPI weighting skipped: incident_history table not found.")
        return None
    if not spark.catalog.tableExists(metric_weights_table):
        print("KPI weighting skipped: metric-level ML weights not found "
              "(run ml_weighting.train_dynamic_weights first).")
        return None

    history = spark.table(history_table)
    kpi_level = _kpi_level_aggregate(history)

    feature_columns = ml_cfg.get("feature_columns", [])
    metric_weights_rows = (
        spark.table(metric_weights_table)
        .orderBy(F.col("generated_timestamp").desc())
        .limit(max(len(feature_columns), 1))
        .collect()
    )
    metric_weight = {r["kpi"]: float(r["dynamic_weight"]) for r in metric_weights_rows}
    if not metric_weight:
        print("KPI weighting skipped: no metric weights available yet.")
        return None

    usable_features = [c for c in feature_columns if c in metric_weight and c in kpi_level.columns]
    if not usable_features:
        print("KPI weighting skipped: metric weight names don't match feature_columns.")
        return None

    stats_row = kpi_level.select(
        *[F.min(c).alias(f"min_{c}") for c in usable_features],
        *[F.max(c).alias(f"max_{c}") for c in usable_features],
    ).collect()[0]

    scored = kpi_level
    score_expr = F.lit(0.0)
    for c in usable_features:
        lo, hi = stats_row[f"min_{c}"], stats_row[f"max_{c}"]
        norm_col = F.lit(0.0) if (lo is None or hi is None or hi == lo) else (
            (F.col(c) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))
        )
        score_expr = score_expr + (norm_col * F.lit(metric_weight[c]))
    scored = scored.withColumn("raw_score", score_expr)

    total = scored.agg(F.sum("raw_score").alias("t")).collect()[0]["t"] or 0.0
    if total == 0:
        n = scored.count()
        scored = scored.withColumn("weight", F.lit(100.0 / n if n else 0.0))
    else:
        scored = scored.withColumn("weight", F.round(F.col("raw_score") / F.lit(total) * F.lit(100.0), 4))

    result = (
        scored
        .select(
            "kpi", "kpi_name", "dimension", "weight",
            "incident_count", "avg_mttd_minutes", "avg_mtta_minutes",
            "avg_mttr_minutes", "aidr_pct", "total_rev_impact", "severity_score",
        )
        .withColumn("run_timestamp", F.current_timestamp())
    )

    report_table = _resolve_kpi_report_table(cfg)
    (
        result.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(report_table)
    )
    print(f"KPI report written: {report_table}")
    result.orderBy(F.col("weight").desc()).show(truncate=False)
    return result


# ============================================================
# NEW: WHITEBOARD-STYLE FLAT REPORT (one row per incident)
# ============================================================

def build_incident_level_kpi_report(spark, cfg):
    """
    The reviewer-facing table: incident-level rows (date, incident,
    rev impact, DS, label) joined with that incident's KPI code and
    the CURRENT dynamic weight/aggregate stats for that KPI.

    SELECT * FROM <this table> ORDER BY date; is the whole demo.
    """
    kpi_cfg = cfg.get("kpi_metrics", {})
    if not kpi_cfg.get("enabled", False):
        return None

    log_table = kpi_cfg.get("incident_log_table")
    weights_table = _resolve_kpi_report_table(cfg)

    if not spark.catalog.tableExists(log_table):
        print("Incident-level KPI report skipped: incident log not found.")
        return None
    if not spark.catalog.tableExists(weights_table):
        print("Incident-level KPI report skipped: dq_kpis not populated yet "
              "(run compute_kpi_weights first).")
        return None

    raw = spark.table(log_table)
    with_durations = compute_incident_durations(raw)
    classified = classify_kpi(spark, with_durations, kpi_cfg)

    weights_df = spark.table(weights_table)
    latest_ts = weights_df.agg(F.max("run_timestamp")).collect()[0][0]
    if latest_ts is None:
        print("Incident-level KPI report skipped: dq_kpis has no rows yet.")
        return None

    latest_weights = (
        weights_df
        .filter(F.col("run_timestamp") == latest_ts)
        .select(
            F.col("kpi").alias("w_kpi"),
            F.col("weight").alias("ml_weight"),
            F.col("avg_mttd_minutes").alias("kpi_avg_mttd_minutes"),
            F.col("avg_mtta_minutes").alias("kpi_avg_mtta_minutes"),
            F.col("avg_mttr_minutes").alias("kpi_avg_mttr_minutes"),
            F.col("aidr_pct").alias("kpi_aidr_pct"),
            F.col("severity_score").alias("kpi_severity_score"),
        )
    )

    report = (
        classified
        .join(latest_weights, classified["kpi"] == latest_weights["w_kpi"], "left")
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
            F.col("ttd_min").alias("incident_mttd_min"),
            F.col("tta_min").alias("incident_mtta_min"),
            F.col("ttr_min").alias("incident_mttr_min"),
            F.col("is_automated"),
            F.col("ml_weight"),
            F.col("kpi_avg_mttd_minutes"),
            F.col("kpi_avg_mtta_minutes"),
            F.col("kpi_avg_mttr_minutes"),
            F.col("kpi_aidr_pct"),
            F.col("kpi_severity_score"),
        )
        .withColumn("report_generated_timestamp", F.current_timestamp())
    )

    report_table = _resolve_incident_level_report_table(cfg)

    from delta.tables import DeltaTable
    if not spark.catalog.tableExists(report_table):
        (
            report.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(report_table)
        )
    else:
        delta_tbl = DeltaTable.forName(spark, report_table)
        (
            delta_tbl.alias("t")
            .merge(report.alias("s"), "t.incident_id = s.incident_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Incident-level KPI report written: {report_table}")
    spark.table(report_table).orderBy("date").show(truncate=False)
    return report
'''

pathlib.Path(base + "/kpi_metrics.py").write_text(kpi_content)
print("kpi_metrics.py written.")
import subprocess
r = subprocess.run(["grep", "-n", "needs_overwrite"], input=open(base + "/kpi_metrics.py").read(), capture_output=True, text=True)
print("Verification grep:")
print(r.stdout)