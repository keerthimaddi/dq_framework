
from pyspark.sql import functions as F
from src.metadata_discovery import get_spark_session, discover_tables
from src.rule_loader import load_config, load_dq_rules, get_table_rule
from src.profiler import profile_table, generate_rule_candidates
from src.dq_engine import run_dq_checks, run_integrity_checks, calculate_overall_score
from src.lakehouse import ensure_schemas, write_bronze, build_silver, build_gold
from src.audit_reporting import persist_results, make_kpis, create_reporting_views
from src.ml_weighting import train_dynamic_weights

def main():
    spark = get_spark_session()
    cfg = load_config()
    catalog = cfg["framework"]["catalog"]

    print("\n======================================")
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("======================================")
    print("Catalog:", catalog)

    rules, overall_thresholds = load_dq_rules()
    enabled = [r for r in rules if r.get("enabled", False)]
    print("\n======================================")
    print("DQ RULE CONFIGURATION")
    print("======================================")
    for r in rules:
        print(f"{r['id']} | {r['dimension']} | Level: {r['level']} | "
              f"Enabled: {r['enabled']} | Weight: {r['default_weight']} | Severity: {r['severity']}")
    print(f"\nTotal DQ Rules    : {len(rules)}")
    print(f"Enabled DQ Rules  : {len(enabled)}")
    print(f"Total Weight      : {sum(float(r['default_weight']) for r in enabled)}")
    print(f"PASS    >= {overall_thresholds['pass']}")
    print(f"WARNING >= {overall_thresholds['warning']}")
    print(f"FAIL    < {overall_thresholds['warning']}")

    ensure_schemas(spark, catalog, cfg["framework"])

    tables = discover_tables(
        spark, catalog,
        cfg["framework"].get("excluded_schemas", ["information_schema"])
    )
    print("\nTables discovered:", len(tables))

    # Profiling and candidate generation.
    profile_rows, candidate_rows = [], []
    if cfg.get("profiling", {}).get("enabled", True):
        print("\n======================================")
        print("DQX-STYLE PROFILING")
        print("======================================")
        for c, s, t in tables:
            try:
                p = profile_table(spark, c, s, t, cfg)
                profile_rows.extend(p)
                candidate_rows.extend(generate_rule_candidates(p, cfg))
            except Exception as e:
                print(f"Profiling failed for {c}.{s}.{t}: {e}")

    if profile_rows:
        spark.createDataFrame(profile_rows).write.format("delta").mode("overwrite").option(
            "overwriteSchema","true"
        ).saveAsTable(f"`{catalog}`.`{cfg['framework']['candidate_schema']}`.`{cfg['output_tables']['profiling']}`")
    if candidate_rows:
        spark.createDataFrame(candidate_rows).write.format("delta").mode("overwrite").option(
            "overwriteSchema","true"
        ).saveAsTable(f"`{catalog}`.`{cfg['framework']['candidate_schema']}`.`{cfg['output_tables']['candidates']}`")

    summary_results, audit_details, kpis = [], [], []

    print("\n======================================")
    print("BRONZE -> DQ -> QUARANTINE -> SILVER -> GOLD")
    print("======================================")

    for c, s, t in tables:
        source = f"{c}.{s}.{t}"
        print(f"\nChecking: {source}")
        try:
            table_cfg = get_table_rule(cfg, source)
            bronze = f"{c}.{cfg['framework']['bronze_schema']}.{t}"
            silver = f"{c}.{cfg['framework']['silver_schema']}.{t}"
            gold = f"{c}.{cfg['framework']['gold_schema']}.{t}"

            write_bronze(spark, source, bronze)

            result = run_dq_checks(spark, c, s, t, cfg)
            integrity_status, ibad, itotal, ireason, irows = run_integrity_checks(
                spark, cfg, result
            )
            result["dq_results"]["DQ06"] = integrity_status

            for d in result["details"]:
                if d["dq_check"] == "DQ06":
                    d["status"] = integrity_status
                    d["failed_records"] = ibad
                    d["total_records"] = itotal
                    d["failure_percentage"] = (ibad / itotal * 100) if itotal else 0
                    d["failure_reason"] = ireason
                    d["failed_row_keys"] = irows

            score, status = calculate_overall_score(cfg, result["dq_results"])
            result["total_score"] = score
            result["overall_status"] = status

            summary_results.append({
                "run_id": result["run_id"], "run_timestamp": result["run_timestamp"],
                "catalog": c, "schema": s, "table": t,
                **{k: v for k, v in result["dq_results"].items()},
                "total_score": score, "overall_status": status,
                "row_count": result["row_count"], "layer": "BRONZE"
            })
            audit_details.extend(result["details"])
            kpis.extend(make_kpis(result))

            dq_output = " | ".join(
                f"{r['id']}={result['dq_results'].get(r['id'],'N/A')}" for r in enabled
            )
            print(f"{dq_output} | Score={score}% | Overall={status}")

            # Silver is the cleaned/validated layer. For generic datasets,
            # use approved unique keys if present, otherwise complete-row dedupe.
            key_columns = [k for k in table_cfg.get("unique_keys", []) if k in spark.table(
                f"`{c}`.`{cfg['framework']['bronze_schema']}`.`{t}`"
            ).columns]
            build_silver(
                spark, bronze,
                f"{c}.{cfg['framework']['quarantine_schema']}.{cfg['output_tables']['quarantine']}_{t}",
                silver, key_columns
            )

            # Gold preserves the validated Silver dataset unless a business aggregation
            # is explicitly configured.
            build_gold(spark, silver, gold)

        except Exception as e:
            print(f"Pipeline failed for {source}: {e}")

    persist_results(spark, cfg, summary_results, audit_details, kpis, catalog)
    create_reporting_views(spark, catalog, cfg["framework"] | {"output_tables": cfg["output_tables"]})
    train_dynamic_weights(spark, cfg, catalog)

    print("\n======================================")
    print("FINAL DQ REPORT")
    print("======================================")
    spark.table(f"`{catalog}`.`{cfg['framework']['audit_schema']}`.`{cfg['output_tables']['summary']}`") \
        .orderBy(F.col("run_timestamp").desc()) \
        .show(truncate=False)

    print("\n======================================")
    print("PIPELINE COMPLETE")
    print("======================================")
    print("Audit table :", f"{catalog}.{cfg['framework']['audit_schema']}.{cfg['output_tables']['audit']}")
    print("Summary table:", f"{catalog}.{cfg['framework']['audit_schema']}.{cfg['output_tables']['summary']}")
    print("KPI table   :", f"{catalog}.{cfg['framework']['audit_schema']}.{cfg['output_tables']['kpi']}")
    print("Profiling   :", f"{catalog}.{cfg['framework']['candidate_schema']}.{cfg['output_tables']['profiling']}")
    print("Candidates  :", f"{catalog}.{cfg['framework']['candidate_schema']}.{cfg['output_tables']['candidates']}")

if __name__ == "__main__":
    main()
