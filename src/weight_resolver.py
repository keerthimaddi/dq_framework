# ============================================================
# WEIGHT RESOLVER
# Requirement 02, Part 4/5/11: dynamic weight -> YAML fallback
#
# This module is deliberately standalone and reads ONLY from
# Delta tables + cfg - it does not touch dq_engine.py, so it is
# 100% safe to deploy today with zero risk to the working DQ
# engine. It becomes USEFUL the moment calculate_score() is
# patched to call build_effective_weights() instead of reading
# rule["default_weight"] directly (see integration note below).
#
# COLD-START SAFE: every failure mode (table missing, no rows,
# ML hasn't trained yet, a KPI has no weight) falls through to
# the YAML default_weight. This function can NEVER raise in a
# way that blocks the pipeline - worst case it just returns the
# YAML defaults, identical to today's behavior.
# ============================================================

from pyspark.sql import functions as F


def _dimension_key(dimension):
    return dimension.strip().lower() if dimension else None


def load_dynamic_weights(spark, cfg):
    """
    Reads the LATEST run of dq_kpis (per-KPI dynamic weights, Stage B)
    and returns two lookup dicts:
      - by_rule_id:   {"DQ01": 12.4, "DQ06": 8.9, ...}
      - by_dimension: {"completeness": 12.4, "integrity": 8.9, ...}
    Returns ({}, {}) on ANY failure - missing table, empty table,
    schema drift, whatever. Never raises.
    """
    try:
        framework = cfg["framework"]
        kpi_table_name = cfg.get("output_tables", {}).get("kpi")
        if not kpi_table_name:
            return {}, {}

        kpi_table = f"{framework['catalog']}.{framework['audit_schema']}.{kpi_table_name}"

        if not spark.catalog.tableExists(kpi_table):
            print(f"weight_resolver: {kpi_table} does not exist yet - using YAML weights.")
            return {}, {}

        df = spark.table(kpi_table)
        if df.rdd.isEmpty():
            print(f"weight_resolver: {kpi_table} is empty - using YAML weights.")
            return {}, {}

        latest_ts = df.agg(F.max("run_timestamp")).collect()[0][0]
        if latest_ts is None:
            return {}, {}

        rows = (
            df.filter(F.col("run_timestamp") == latest_ts)
            .select("kpi", "dimension", "weight")
            .collect()
        )

        by_rule_id, by_dimension = {}, {}
        for r in rows:
            if r["weight"] is None:
                continue
            by_rule_id[r["kpi"]] = float(r["weight"])
            dim_key = _dimension_key(r["dimension"])
            if dim_key:
                by_dimension[dim_key] = float(r["weight"])

        print(f"weight_resolver: loaded {len(by_rule_id)} dynamic weight(s) from {kpi_table} "
              f"(run_timestamp={latest_ts})")
        return by_rule_id, by_dimension

    except Exception as exc:
        print(f"weight_resolver: dynamic weights unavailable, falling back to YAML ({exc})")
        return {}, {}


def resolve_weight(rule_id, dimension, default_weight, by_rule_id, by_dimension):
    """
    Resolution order:
      1. Dynamic weight matched by DQ rule id (e.g. "DQ01")
      2. Dynamic weight matched by dimension name (e.g. "completeness")
      3. YAML default_weight
    Always returns a float. Never returns None.
    """
    if rule_id in by_rule_id:
        return by_rule_id[rule_id]
    dim_key = _dimension_key(dimension)
    if dim_key and dim_key in by_dimension:
        return by_dimension[dim_key]
    return float(default_weight)


def build_effective_weights(spark, cfg):
    """
    THE function to call from calculate_score(). Returns a dict of
    {dq_id: effective_weight} covering every rule in dq_rules.yml,
    with dynamic weights applied wherever available and YAML defaults
    used everywhere else. Call once per pipeline run (or once per
    table - it's a single small table read, cheap either way).
    """
    by_rule_id, by_dimension = load_dynamic_weights(spark, cfg)

    effective = {}
    sources = {}
    for rule in cfg.get("dq_rules", []):
        rule_id = rule["id"]
        dimension = rule.get("dimension")
        default_weight = rule.get("default_weight", 0)
        weight = resolve_weight(rule_id, dimension, default_weight, by_rule_id, by_dimension)
        effective[rule_id] = weight
        sources[rule_id] = "DYNAMIC" if weight != float(default_weight) else "YAML"

    dynamic_count = sum(1 for s in sources.values() if s == "DYNAMIC")
    print(f"weight_resolver: {dynamic_count}/{len(effective)} dimension weights resolved dynamically, "
          f"{len(effective) - dynamic_count} using YAML defaults.")
    print(f"weight_resolver: effective weights this run: {effective}")

    return effective