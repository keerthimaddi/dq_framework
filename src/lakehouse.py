
from pyspark.sql import functions as F

def ensure_schemas(spark, catalog, cfg):
    for key in ["bronze_schema", "silver_schema", "gold_schema", "audit_schema", "quarantine_schema", "candidate_schema"]:
        schema = cfg["framework"][key]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

def write_bronze(spark, source_full_name, target_full_name):
    df = spark.table("`" + "`.`".join(source_full_name.split(".")) + "`")
    out = df.withColumn("_dqx_ingestion_timestamp", F.current_timestamp()) \
           .withColumn("_dqx_source_table", F.lit(source_full_name))
    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "`" + "`.`".join(target_full_name.split(".")) + "`"
    )
    return out

def quarantine_rows(spark, df, failed_keys, source_full_name, failed_rule, run_id, reason, target_full_name):
    if not failed_keys:
        return 0
    keys = set(failed_keys[0].keys())
    key_df = spark.createDataFrame(failed_keys)
    bad = df.join(key_df, list(keys), "inner")
    q = bad.withColumn("_dqx_catalog", F.lit(source_full_name.split(".")[0])) \
          .withColumn("_dqx_schema", F.lit(source_full_name.split(".")[1])) \
          .withColumn("_dqx_table", F.lit(source_full_name.split(".")[2])) \
          .withColumn("_dqx_failed_rule", F.lit(failed_rule)) \
          .withColumn("_dqx_failure_reason", F.lit(reason)) \
          .withColumn("_dqx_pipeline_run_id", F.lit(run_id)) \
          .withColumn("_dqx_detected_timestamp", F.current_timestamp())
    q.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "`" + "`.`".join(target_full_name.split(".")) + "`"
    )
    return bad.count()

def build_silver(spark, bronze_full_name, quarantine_full_name, target_full_name, key_columns):
    bronze = spark.table("`" + "`.`".join(bronze_full_name.split(".")) + "`")
    clean = bronze.dropDuplicates(key_columns) if key_columns else bronze.dropDuplicates()
    clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "`" + "`.`".join(target_full_name.split(".")) + "`"
    )
    return clean

def build_gold(spark, silver_full_name, target_full_name):
    df = spark.table("`" + "`.`".join(silver_full_name.split(".")) + "`")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "`" + "`.`".join(target_full_name.split(".")) + "`"
    )
    return df
