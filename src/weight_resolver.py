from pyspark.sql import functions as F


def _dimension_key(dimension):
    return dimension.strip().lower() if dimension else None


def load_dynamic_weights(spark, cfg):
    try:
        framework = cfg["framework"]
        control_catalog = framework["control_catalog"]
        kpi_table_name = cfg.get("output_tables", {}).get("kpi")
        if not kpi_table_name:
            return {}, {}

        kpi_table = f"{control_catalog}.{framework['audit_schema']}.{kpi_table_name}"

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
    if rule_id in by_rule_id:
        return by_rule_id[rule_id]
    dim_key = _dimension_key(dimension)
    if dim_key and dim_key in by_dimension:
        return by_dimension[dim_key]
    return float(default_weight)


def build_effective_weights(spark, cfg):
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