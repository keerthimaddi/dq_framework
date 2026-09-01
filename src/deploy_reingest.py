import pathlib

base = "/Workspace/Users/keerthi.maddi@mediamint.com/dq_framework/src"

reingest_content = '''
# ============================================================
# RE-INGESTION MODULE
# Requirement 01 Section 17: Quarantine -> Correction -> Re-validation -> Silver
#
# DELIBERATELY PRACTICAL, NOT A DATA REPAIR ENGINE:
# a data steward supplies corrected rows in a per-table
# "dq_corrections_<schema>_<table>" Delta table (same schema as
# the source table + a correction_status column). This module
# re-validates ONLY those rows using the EXACT SAME rule-merging
# and failure-condition logic already used in the main pipeline
# (build_auto_rule + merge_rules + build_failure_condition) - so
# there is no second, divergent validation framework. Rows that
# now pass get merged into Silver; rows that still fail are left
# for the steward to correct again.
# ============================================================

from pyspark.sql import functions as F

from src.quarantine_rules import build_failure_condition
from src.dq_engine import get_table_rule
from src.auto_rules import build_auto_rule, merge_rules


def _correction_table_name(cfg, catalog, schema, table):
    audit_schema = cfg["framework"]["audit_schema"]
    return f"{catalog}.{audit_schema}.dq_corrections_{schema}_{table}"


def get_pending_corrections(spark, cfg, catalog, schema, table):
    """
    Returns only PENDING correction rows for this table, or None if
    no correction table exists for it (the common case - most tables
    will never have one, and that's fine, this is a no-op then).
    """
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
    """
    Re-applies the SAME effective_rule (auto-derived + manual YAML
    override, merged exactly as process_table does it) and the SAME
    build_failure_condition used for the original quarantine. This
    is literally the same DQ logic the rows were quarantined against
    - not a reimplementation.
    """
    auto_rule = build_auto_rule(spark, corrections_df, catalog, schema, table, cfg)
    manual_rule = get_table_rule(cfg, catalog, schema, table)
    effective_rule = merge_rules(auto_rule, manual_rule)

    failure_condition = build_failure_condition(corrections_df, effective_rule)

    still_failing = corrections_df.filter(failure_condition)
    now_passing = corrections_df.filter(~failure_condition)

    return now_passing, still_failing


def promote_to_silver(spark, now_passing_df, cfg, catalog, schema, table):
    """
    Merges corrected+passing rows into the existing Silver table.
    Uses unique_keys from table_rules to merge safely; falls back
    to append if no unique_keys are configured for this table
    (best-effort - configure unique_keys to avoid duplicate risk).
    """
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
    """
    Called once per pipeline run, after the main table loop. Safe
    no-op for every table with no correction table present - this
    only does work where a steward has actually supplied corrections.
    Never raises out of the pipeline; every table is isolated in
    its own try/except.
    """
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
'''

target = pathlib.Path(base + "/reingest.py")
print("BEFORE - current deployed file:")
try:
    print(target.read_text())
except Exception as e:
    print(f"Could not read existing file: {e}")
print("=" * 70)

target.write_text(reingest_content)
print("reingest.py (re)written.")

import subprocess
r = subprocess.run(["grep", "-n", "^def ", str(target)], capture_output=True, text=True)
print("Functions now in deployed reingest.py:")
print(r.stdout)




