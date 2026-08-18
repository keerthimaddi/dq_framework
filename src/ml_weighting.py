
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

def train_dynamic_weights(spark, cfg, catalog):
    ml_cfg = cfg.get("ml_weighting", {})
    if not ml_cfg.get("enabled", False) or not ml_cfg.get("incident_table"):
        print("ML weighting skipped: no incident table configured.")
        return None

    source = ml_cfg["incident_table"]
    df = spark.table("`" + "`.`".join(source.split(".")) + "`")
    features = [c for c in ml_cfg["feature_columns"] if c in df.columns]
    target = ml_cfg["target_column"]
    if len(features) < 2 or target not in df.columns:
        print("ML weighting skipped: insufficient configured incident columns.")
        return None

    clean = df.select(*(features + [target])).dropna()
    if clean.count() < 10:
        print("ML weighting skipped: at least 10 historical rows are recommended.")
        return None

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    model_df = assembler.transform(clean)
    model = RandomForestRegressor(
        featuresCol="features", labelCol=target, numTrees=50, seed=42
    ).fit(model_df)

    importances = model.featureImportances.toArray().tolist()
    total = sum(importances) or 1.0
    weights = [{"kpi": k, "raw_importance": float(v),
                "dynamic_weight": round(float(v)/total*100, 4)}
               for k, v in zip(features, importances)]
    out = spark.createDataFrame(weights).withColumn("generated_timestamp", F.current_timestamp())
    target_table = ml_cfg.get("output_table", f"{catalog}.dqx_audit.dq_dynamic_weights")
    out.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(
        "`" + "`.`".join(target_table.split(".")) + "`"
    )
    print("Dynamic ML weights written to:", target_table)
    return weights
