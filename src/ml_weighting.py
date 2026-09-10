from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


def _resolve_incident_table(cfg, ml_cfg):
    control_catalog = cfg["framework"]["control_catalog"]
    schema = ml_cfg.get("incident_schema")
    table_name = ml_cfg.get("incident_table_name")
    if not schema or not table_name:
        return None
    return f"{control_catalog}.{schema}.{table_name}"


def _resolve_output_table(cfg, ml_cfg):
    control_catalog = cfg["framework"]["control_catalog"]
    schema = ml_cfg.get("output_schema")
    table_name = ml_cfg.get("output_table_name")
    if not schema or not table_name:
        return f"{control_catalog}.dqx_audit.dq_dynamic_weights"
    return f"{control_catalog}.{schema}.{table_name}"


def train_dynamic_weights(spark, cfg, catalog=None):
    """
    NOTE: the `catalog` parameter is kept for backward compatibility
    with main.py's existing call site (`train_dynamic_weights(spark,
    cfg, cfg["framework"]["catalog"])`), but is no longer used to
    build table paths - control_catalog from cfg is used instead,
    since framework.catalog was removed as part of the multi-catalog
    architecture change. Safe to pass anything here, or update the
    call site to drop the third argument entirely.
    """
    ml_cfg = cfg.get("ml_weighting", {})
    if not ml_cfg.get("enabled", False):
        print("ML weighting skipped: disabled in config.")
        return None

    source = _resolve_incident_table(cfg, ml_cfg)
    if not source:
        print("ML weighting skipped: incident_schema/incident_table_name not configured.")
        return None

    if not spark.catalog.tableExists(source):
        print(f"ML weighting skipped: {source} does not exist yet.")
        return None

    df = spark.table("`" + "`.`".join(source.split(".")) + "`")
    features = [c for c in ml_cfg.get("feature_columns", []) if c in df.columns]
    target = ml_cfg.get("target_column")
    if len(features) < 2 or not target or target not in df.columns:
        print("ML weighting skipped: insufficient configured incident columns.")
        return None

    min_rows = int(ml_cfg.get("min_training_rows", 10))

    clean = df.select(*(features + [target])).dropna()
    row_count = clean.count()
    if row_count < min_rows:
        print(f"ML weighting skipped: {row_count} historical rows available, "
              f"{min_rows} required (min_training_rows).")
        return None

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    model_df = assembler.transform(clean)
    model = RandomForestRegressor(
        featuresCol="features", labelCol=target, numTrees=50, seed=42
    ).fit(model_df)

    importances = model.featureImportances.toArray().tolist()
    total = sum(importances) or 1.0
    weights = [{"kpi": k, "raw_importance": float(v),
                "dynamic_weight": round(float(v) / total * 100, 4)}
               for k, v in zip(features, importances)]
    out = spark.createDataFrame(weights).withColumn("generated_timestamp", F.current_timestamp())

    target_table = _resolve_output_table(cfg, ml_cfg)
    out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "`" + "`.`".join(target_table.split(".")) + "`"
    )
    print("Dynamic ML weights written to:", target_table)
    return weights