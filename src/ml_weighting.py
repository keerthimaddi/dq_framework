from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


def train_dynamic_weights(spark, cfg, catalog):
    ml_cfg = cfg.get("ml_weighting", {})
    if not ml_cfg.get("enabled", False) or not ml_cfg.get("incident_table"):
        print("ML weighting skipped: no incident table configured.")
        return None

    source = ml_cfg["incident_table"]
    if not spark.catalog.tableExists(source):
        print(f"ML weighting skipped: {source} does not exist yet.")
        return None

    df = spark.table("`" + "`.`".join(source.split(".")) + "`")
    features = [c for c in ml_cfg["feature_columns"] if c in df.columns]
    target = ml_cfg["target_column"]
    if len(features) < 2 or target not in df.columns:
        print("ML weighting skipped: insufficient configured incident columns.")
        return None

    # CHANGED: read the minimum-rows threshold from config instead of
    # hardcoding 10, so dq_rules.yml's min_training_rows: 10 actually
    # has an effect (matches the project's "no hardcoding" principle).
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
    target_table = ml_cfg.get("output_table", f"{catalog}.dqx_audit.dq_dynamic_weights")
    out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "`" + "`.`".join(target_table.split(".")) + "`"
    )
    print("Dynamic ML weights written to:", target_table)
    return weights