
from pyspark.sql.functions import col, min, max, count, countDistinct, avg, sum as spark_sum
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType

def profile_table(spark, catalog, schema, table, cfg):
    full = f"`{catalog}`.`{schema}`.`{table}`"
    df = spark.table(full)
    total = df.count()
    top_n = int(cfg.get("profiling", {}).get("top_values", 10))
    rows = []

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType.simpleString()
        nulls = df.filter(col(name).isNull()).count()
        distinct = df.select(name).distinct().count()
        row = {
            "catalog": catalog, "schema": schema, "table": table,
            "column_name": name, "data_type": dtype,
            "total_rows": total, "null_count": nulls,
            "null_rate_pct": round((nulls / total * 100) if total else 100, 4),
            "distinct_count": distinct,
            "min_value": None, "max_value": None,
            "avg_value": None, "top_values": None
        }

        if isinstance(field.dataType, NumericType):
            agg = df.select(
                min(col(name)).alias("min_value"),
                max(col(name)).alias("max_value"),
                avg(col(name)).alias("avg_value")
            ).first()
            row["min_value"] = str(agg["min_value"]) if agg["min_value"] is not None else None
            row["max_value"] = str(agg["max_value"]) if agg["max_value"] is not None else None
            row["avg_value"] = float(agg["avg_value"]) if agg["avg_value"] is not None else None

        if isinstance(field.dataType, (StringType, DateType, TimestampType)):
            vals = [
                r[name] for r in df.groupBy(name).count()
                .orderBy(col("count").desc())
                .limit(top_n).collect()
                if r[name] is not None
            ]
            row["top_values"] = ", ".join(map(str, vals))

        rows.append(row)
    return rows

def generate_rule_candidates(profile_rows, cfg):
    candidates = []
    p_cfg = cfg.get("profiling", {})
    null_threshold = float(p_cfg.get("candidate_null_rate_pct", 1))

    for r in profile_rows:
        if r["null_rate_pct"] <= null_threshold:
            candidates.append({
                "catalog": r["catalog"], "schema": r["schema"], "table": r["table"],
                "column_name": r["column_name"], "candidate_dimension": "completeness",
                "candidate_rule": f"{r['column_name']} should be non-null",
                "observed_value": f"null_rate={r['null_rate_pct']}%",
                "status": "PENDING_REVIEW"
            })

        if r["data_type"] in ("int", "bigint", "double", "float", "decimal", "long", "short"):
            if r["min_value"] is not None and r["max_value"] is not None:
                candidates.append({
                    "catalog": r["catalog"], "schema": r["schema"], "table": r["table"],
                    "column_name": r["column_name"], "candidate_dimension": "range",
                    "candidate_rule": f"observed range {r['min_value']} to {r['max_value']}",
                    "observed_value": f"min={r['min_value']};max={r['max_value']}",
                    "status": "PENDING_REVIEW"
                })

        if r["data_type"] == "string":
            candidates.append({
                "catalog": r["catalog"], "schema": r["schema"], "table": r["table"],
                "column_name": r["column_name"], "candidate_dimension": "pattern",
                "candidate_rule": "Review frequent values and derive approved pattern",
                "observed_value": r["top_values"],
                "status": "PENDING_REVIEW"
            })
    return candidates
