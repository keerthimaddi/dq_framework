
from pyspark.sql import functions as F
from datetime import datetime

def persist_results(spark, cfg, results, details, kpis, catalog):
    audit_schema = cfg["framework"]["audit_schema"]
    q_schema = cfg["framework"]["quarantine_schema"]
    cand_schema = cfg["framework"]["candidate_schema"]
    summary_table = f"`{catalog}`.`{audit_schema}`.`{cfg['output_tables']['summary']}`"
    audit_table = f"`{catalog}`.`{audit_schema}`.`{cfg['output_tables']['audit']}`"
    kpi_table = f"`{catalog}`.`{audit_schema}`.`{cfg['output_tables']['kpi']}`"

    if details:
        spark.createDataFrame(details).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(audit_table)

    if results:
        spark.createDataFrame(results).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(summary_table)

    if kpis:
        spark.createDataFrame(kpis).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(kpi_table)

def make_kpis(result):
    out = []
    ts = result["run_timestamp"]
    out.append({
        "run_id": result["run_id"], "run_timestamp": ts,
        "catalog": result["catalog"], "schema": result["schema"], "table": result["table"],
        "layer": "DQ", "kpi": "Null Rate",
        "value": next((d["failure_percentage"] for d in result["details"] if d["dq_check"]=="DQ11"), 0.0)
    })
    out.append({
        "run_id": result["run_id"], "run_timestamp": ts,
        "catalog": result["catalog"], "schema": result["schema"], "table": result["table"],
        "layer": "DQ", "kpi": "Duplicate Key Rate",
        "value": next((d["failure_percentage"] for d in result["details"] if d["dq_check"]=="DQ04"), 0.0)
    })
    out.append({
        "run_id": result["run_id"], "run_timestamp": ts,
        "catalog": result["catalog"], "schema": result["schema"], "table": result["table"],
        "layer": "DQ", "kpi": "Data Latency",
        "value": next((d["failure_percentage"] for d in result["details"] if d["dq_check"]=="DQ07"), 0.0)
    })
    return out

def create_reporting_views(spark, catalog, cfg):
    schema = cfg["framework"]["audit_schema"]
    summary = cfg["output_tables"]["summary"]
    audit = cfg["output_tables"]["audit"]
    kpi = cfg["output_tables"]["kpi"]
    spark.sql(f"""
      CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_dq_final_report AS
      SELECT * FROM `{catalog}`.`{schema}`.`{summary}`
    """)
    spark.sql(f"""
      CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_dq_history AS
      SELECT * FROM `{catalog}`.`{schema}`.`{audit}`
    """)
    spark.sql(f"""
      CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_dq_kpis AS
      SELECT * FROM `{catalog}`.`{schema}`.`{kpi}`
    """)
