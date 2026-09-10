from pyspark.sql import functions as F

from src.quarantine_rules import build_failure_condition
from src.dq_engine import get_table_rule
from src.auto_rules import build_auto_rule, merge_rules


def _correction_table_name(cfg, catalog, schema, table):
    # Correction/audit tables are always centralized in
    # control_catalog, regardless of which catalog the SOURCE
    # table lives in - this keeps all steward-facing correction
    # tables in one predictable place across a multi-catalog
    # discovery run, instead of scattering dq_corrections_* tables
    # across every catalog that happens to have a failing table.
    control_catalog = cfg["framework"]["control_catalog"]
    audit_schema = cfg["framework"]["audit_schema"]
    return f"{control_catalog}.{audit_schema}.dq_corrections_{catalog}_{schema}_{table}"


def get_pending_corrections(spark, cfg, catalog, schema, table):
    correction_table = _correction_table_name(cfg, catalog, schema, table)
    if not spark.catalog.tableExists(correction_table):
        return None

    df = spark.table(correction_table)
    if "correction_status" not in df.columns:
        print(f"WARNING: {correction_table} has no correction_status column - "
              f"treating all rows as PENDING.")
        return df
    return df.filter(F.col("correction_status") == "PENDING")


def revalidate_corrections(spark, corrections_df, cfg, catalog, schema, table):
    auto_rule = build_auto_rule(spark, corrections_df, catalog, schema, table, cfg)
    manual_rule = get_table_rule(cfg, catalog, schema, table)
    effective_rule = merge_rules(auto_rule, manual_rule)

    failure_condition = build_failure_condition(corrections_df, effective_rule)

    still_failing = corrections_df.filter(failure_condition)
    now_passing = corrections_df.filter(~failure_condition)

    return now_passing, still_failing


def promote_to_silver(spark, now_passing_df, cfg, catalog, schema, table):
    # Silver/Gold stay namespaced by SOURCE catalog+schema, since
    # this is where the corrected data actually belongs, unlike
    # the audit/correction bookkeeping tables above.
    if now_passing_df.rdd.isEmpty():
        return 0

    silver_schema = cfg["framework"]["silver_schema"]
    silver_table = f"{catalog}.{silver_schema}.{table}"

    if not spark.catalog.tableExists(silver_table):
        print(f"Silver table {silver_table} does not exist yet - "
              f"skipping re-ingestion until a normal run creates it first.")
        return 0

    drop_cols = [c for c in ("correction_status",) if c in now_passing_df.columns]
    clean_df = now_passing_df.drop(*drop_cols) if drop_cols else now_passing_df

    table_rule = cfg.get("table_rules", {}).get(f"{catalog}.{schema}.{table}", {})
    unique_keys = table_rule.get("unique_keys", [])

    count = clean_df.count()

    if unique_keys:
        from delta.tables import DeltaTable
        delta_tbl = DeltaTable.forName(spark, silver_table)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in unique_keys])
        (
            delta_tbl.alias("t")
            .merge(clean_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        print(f"NOTE: {catalog}.{schema}.{table} has no unique_keys configured - "
              f"appending corrected rows (may create duplicates; add unique_keys "
              f"to table_rules to merge safely instead).")
        clean_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(silver_table)

    return count


def run_reingestion(spark, cfg, tables):
    print()
    print("=" * 70)
    print("RE-INGESTION")
    print("=" * 70)

    total_promoted = 0
    any_correction_tables_found = False

    for catalog, schema, table in tables:
        try:
            corrections_df = get_pending_corrections(spark, cfg, catalog, schema, table)
            if corrections_df is None:
                continue

            any_correction_tables_found = True
            pending_count = corrections_df.count()
            if pending_count == 0:
                continue

            print(f"{catalog}.{schema}.{table}: {pending_count} pending correction row(s) found.")

            now_passing, still_failing = revalidate_corrections(
                spark, corrections_df, cfg, catalog, schema, table
            )
            promoted = promote_to_silver(spark, now_passing, cfg, catalog, schema, table)
            still_failing_count = still_failing.count()

            print(f"  Promoted to Silver : {promoted}")
            print(f"  Still failing DQ   : {still_failing_count}")

            total_promoted += promoted

        except Exception as exc:
            print(f"Re-ingestion failed for {catalog}.{schema}.{table} (non-fatal): {exc}")

    if not any_correction_tables_found:
        print("No dq_corrections_* tables found for any discovered table - "
              "nothing to re-ingest this run (this is normal until a steward "
              "supplies corrections).")

    print(f"Re-ingestion complete. Total rows promoted to Silver: {total_promoted}")
    return total_promoted