# ============================================================
# DYNAMIC WEIGHT RESOLVER
# ============================================================

from pyspark.sql import functions as F


def _dimension_key(dimension):

    if not dimension:
        return None

    return str(dimension).strip().lower()


def load_dynamic_weights(spark, cfg):

    try:

        framework = cfg["framework"]

        kpi_table_name = (
            cfg.get("output_tables", {})
            .get("kpi")
        )

        if not kpi_table_name:
            return {}, {}

        kpi_table = (
            f"{framework['catalog']}."
            f"{framework['audit_schema']}."
            f"{kpi_table_name}"
        )

        if not spark.catalog.tableExists(
            kpi_table
        ):

            print(
                "weight_resolver: dq_kpis "
                "does not exist. "
                "Using YAML weights."
            )

            return {}, {}

        df = spark.table(kpi_table)

        if df.limit(1).count() == 0:

            print(
                "weight_resolver: dq_kpis "
                "is empty. Using YAML weights."
            )

            return {}, {}

        required = {
            "kpi",
            "dimension",
            "weight",
            "run_timestamp",
        }

        if not required.issubset(
            set(df.columns)
        ):

            print(
                "weight_resolver: dq_kpis "
                "schema is incomplete. "
                "Using YAML weights."
            )

            return {}, {}

        latest_ts = (
            df.select(
                F.max("run_timestamp")
                .alias("latest")
            )
            .collect()[0]["latest"]
        )

        if latest_ts is None:
            return {}, {}

        rows = (
            df
            .filter(
                F.col("run_timestamp")
                == latest_ts
            )
            .select(
                "kpi",
                "dimension",
                "weight",
            )
            .collect()
        )

        by_rule_id = {}
        by_dimension = {}

        for row in rows:

            if row["weight"] is None:
                continue

            weight = float(row["weight"])

            kpi = row["kpi"]

            dimension = _dimension_key(
                row["dimension"]
            )

            if kpi:
                by_rule_id[str(kpi)] = weight

            if dimension:
                by_dimension[dimension] = weight

        print(
            f"weight_resolver: loaded "
            f"{len(by_rule_id)} dynamic KPI weights."
        )

        return by_rule_id, by_dimension

    except Exception as exc:

        print(
            "weight_resolver: dynamic weights "
            f"unavailable: {exc}"
        )

        return {}, {}


def resolve_weight(
    rule_id,
    dimension,
    default_weight,
    by_rule_id,
    by_dimension,
):

    if rule_id in by_rule_id:
        return float(
            by_rule_id[rule_id]
        )

    dimension_key = _dimension_key(
        dimension
    )

    if (
        dimension_key
        and dimension_key in by_dimension
    ):

        return float(
            by_dimension[dimension_key]
        )

    return float(default_weight)


def build_effective_weights(
    spark,
    cfg,
):

    by_rule_id, by_dimension = (
        load_dynamic_weights(
            spark,
            cfg,
        )
    )

    effective = {}

    for rule in cfg.get(
        "dq_rules",
        [],
    ):

        rule_id = rule["id"]

        dimension = rule.get(
            "dimension"
        )

        default_weight = float(
            rule.get(
                "default_weight",
                0,
            )
        )

        effective[rule_id] = (
            resolve_weight(
                rule_id,
                dimension,
                default_weight,
                by_rule_id,
                by_dimension,
            )
        )

    print()
    print(
        "EFFECTIVE DQ WEIGHTS"
    )

    total = 0.0

    for rule in cfg.get(
        "dq_rules",
        [],
    ):

        dq_id = rule["id"]

        weight = effective[dq_id]

        total += weight

        print(
            f"{dq_id:<5} | "
            f"{weight:>8.4f}"
        )

    print(
        f"TOTAL EFFECTIVE WEIGHT: "
        f"{total:.4f}"
    )

    return effective