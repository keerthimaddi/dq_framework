# ============================================================
# AUTO RULES
# Derives DQ01/03/04/09/10/11/13/14 rules automatically from a
# table's own schema and current statistics - no per-table YAML
# entry required. Anything that genuinely needs business
# knowledge (DQ02 row_expressions, DQ05 consistency_rules,
# DQ06 relationships, DQ07 date_columns, DQ08 conformity_rules,
# DQ12 length_rules, DQ15 business_rules) is left to optional
# config in table_rules and simply merged in on top when present.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T


NUMERIC_TYPES = (
    T.ByteType, T.ShortType, T.IntegerType, T.LongType,
    T.FloatType, T.DoubleType, T.DecimalType,
)

STRING_TYPES = (T.StringType,)

# name fragments that suggest a column is an identifier/key
KEY_NAME_HINTS = ("id", "key", "code", "uuid", "number")

PATTERN_LIBRARY = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "phone": r"^\+?[0-9()\-\s]{7,15}$",
    "url": r"^https?://\S+$",
}


def _looks_like_key(column_name):

    lowered = column_name.lower()

    return any(hint in lowered for hint in KEY_NAME_HINTS)


def _get_or_create_schema_baseline(spark, df, catalog, schema, table, cfg):
    """
    DQ13 needs an "expected type" per column. Rather than hand-typing
    that per table, the first time a table is seen its current dtypes
    become the baseline. On every later run, DQ13 checks current
    dtypes against that stored baseline - i.e. it becomes a schema
    drift detector, fully metadata-driven.
    """

    framework = cfg["framework"]
    audit_schema = framework["audit_schema"]
    baseline_table = f"`{catalog}`.`{audit_schema}`.`dq_schema_baseline`"

    current_types = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    try:
        baseline_exists = spark.catalog.tableExists(
            f"{catalog}.{audit_schema}.dq_schema_baseline"
        )
    except Exception:
        baseline_exists = False

    if not baseline_exists:

        rows = [
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "column_name": column_name,
                "expected_type": expected_type,
            }
            for column_name, expected_type in current_types.items()
        ]

        spark.createDataFrame(rows).write.format("delta").mode(
            "append"
        ).option("mergeSchema", "true").saveAsTable(baseline_table)

        # First time seen: baseline == current, so DQ13 passes this run.
        return current_types

    existing = (
        spark.table(baseline_table)
        .filter(
            (F.col("catalog") == catalog)
            & (F.col("schema") == schema)
            & (F.col("table") == table)
        )
        .collect()
    )

    if not existing:

        rows = [
            {
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "column_name": column_name,
                "expected_type": expected_type,
            }
            for column_name, expected_type in current_types.items()
        ]

        spark.createDataFrame(rows).write.format("delta").mode(
            "append"
        ).option("mergeSchema", "true").saveAsTable(baseline_table)

        return current_types

    baseline_types = {
        row["column_name"]: row["expected_type"]
        for row in existing
    }

    # New columns not yet in the baseline are added silently
    # (schema growth is normal); only columns present in the
    # baseline are enforced going forward.
    for column_name, expected_type in current_types.items():
        baseline_types.setdefault(column_name, expected_type)

    return baseline_types


def build_auto_rule(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg,
    null_threshold_pct=0.0,
    pattern_match_threshold=0.95,
    range_std_multiplier=4.0,
):
    """
    Builds a table_rules-shaped dict purely from the table's own
    schema + current statistics. Safe to call on ANY table with
    zero configuration.
    """

    total = df.count()

    auto_rule = {
        "mandatory_columns": [],
        "null_columns": [],
        "unique_keys": [],
        "pattern_rules": {},
        "range_rules": {},
        "expected_types": {},
    }

    if total == 0:
        return auto_rule

    fields = df.schema.fields

    # --------------------------------------------------------
    # SINGLE-PASS STATS: null counts, distinct counts, numeric
    # mean/stddev, and pattern-match counts for string columns.
    # --------------------------------------------------------
    agg_exprs = []

    for field in fields:
        c = field.name

        agg_exprs.append(
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(
                f"null__{c}"
            )
        )
        agg_exprs.append(
            F.approx_count_distinct(F.col(c)).alias(f"distinct__{c}")
        )

        if isinstance(field.dataType, NUMERIC_TYPES):
            agg_exprs.append(F.mean(F.col(c)).alias(f"mean__{c}"))
            agg_exprs.append(F.stddev(F.col(c)).alias(f"std__{c}"))

        if isinstance(field.dataType, STRING_TYPES):
            for pattern_name, pattern in PATTERN_LIBRARY.items():
                agg_exprs.append(
                    F.sum(
                        F.when(
                            F.col(c).isNotNull()
                            & F.col(c).rlike(pattern),
                            1,
                        ).otherwise(0)
                    ).alias(f"pat_{pattern_name}__{c}")
                )

    stats = df.agg(*agg_exprs).collect()[0].asDict()

    # --------------------------------------------------------
    # DERIVE RULES PER COLUMN
    # --------------------------------------------------------
    for field in fields:
        c = field.name

        null_count = stats.get(f"null__{c}") or 0
        distinct_count = stats.get(f"distinct__{c}") or 0
        non_null_count = total - null_count
        null_pct = (null_count / total) * 100.0

        # DQ01 / DQ11: near-zero observed null rate -> treat as mandatory
        if null_pct <= null_threshold_pct:
            auto_rule["mandatory_columns"].append(c)
            auto_rule["null_columns"].append(c)

        # DQ04 / DQ10: fully-distinct, non-null, id-like name -> key candidate
        if (
            non_null_count > 0
            and distinct_count >= non_null_count
            and _looks_like_key(c)
        ):
            auto_rule["unique_keys"].append(c)

        # DQ09: numeric columns -> statistical outlier bounds
        if isinstance(field.dataType, NUMERIC_TYPES):
            mean_v = stats.get(f"mean__{c}")
            std_v = stats.get(f"std__{c}")

            if mean_v is not None and std_v is not None and std_v > 0:
                auto_rule["range_rules"][c] = {
                    "min": mean_v - (range_std_multiplier * std_v),
                    "max": mean_v + (range_std_multiplier * std_v),
                }

        # DQ14: string columns -> known pattern if match rate is high enough
        if isinstance(field.dataType, STRING_TYPES) and non_null_count > 0:
            for pattern_name, pattern in PATTERN_LIBRARY.items():
                match_count = stats.get(f"pat_{pattern_name}__{c}") or 0
                match_rate = match_count / non_null_count

                if match_rate >= pattern_match_threshold:
                    auto_rule["pattern_rules"][c] = pattern
                    break

    # Keep only the single strongest key candidate to avoid
    # flooding DQ04/DQ10 with every id-like column.
    if auto_rule["unique_keys"]:
        auto_rule["unique_keys"] = [auto_rule["unique_keys"][0]]

    # DQ13: schema-drift baseline (metadata-only, no hand typing)
    auto_rule["expected_types"] = _get_or_create_schema_baseline(
        spark, df, catalog, schema, table, cfg
    )

    return auto_rule


def merge_rules(auto_rule, manual_rule):
    """
    Manual (table_rules in YAML) rules are optional and additive:
      - list values are unioned (deduplicated)
      - dict values are updated (manual entries override matching
        auto keys, e.g. a manual range_rules.age overrides the
        auto-computed statistical bound for that one column)
      - anything else, manual fully replaces auto
    """

    if not manual_rule:
        return auto_rule

    merged = {}

    for key, value in auto_rule.items():
        if isinstance(value, list):
            merged[key] = list(value)
        elif isinstance(value, dict):
            merged[key] = dict(value)
        else:
            merged[key] = value

    for key, value in manual_rule.items():

        if key not in merged:
            merged[key] = value
            continue

        existing = merged[key]

        if isinstance(existing, list) and isinstance(value, list):
            merged[key] = list(dict.fromkeys(existing + value))
        elif isinstance(existing, dict) and isinstance(value, dict):
            existing.update(value)
        else:
            merged[key] = value

    return merged