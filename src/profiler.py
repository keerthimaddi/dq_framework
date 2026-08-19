# ============================================================
# PROFILER
# ============================================================

from datetime import datetime

from pyspark.sql import functions as F


def profile_table(
    df,
    catalog,
    schema,
    table
):

    rows = []

    total = df.count()

    for field in df.schema.fields:

        column = field.name

        null_count = (
            df.filter(
                F.col(column).isNull()
            ).count()
        )

        distinct_count = (
            df.select(column)
            .distinct()
            .count()
        )

        null_percentage = (
            null_count / total * 100
            if total
            else 0
        )

        rows.append({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "column_name": column,
            "data_type":
                field.dataType.simpleString(),
            "row_count": int(total),
            "null_count": int(null_count),
            "null_percentage":
                float(null_percentage),
            "distinct_count":
                int(distinct_count),
            "profile_timestamp":
                datetime.now()
        })

    return rows


def create_rule_candidates(
    profile_rows,
    null_threshold=1.0
):

    candidates = []

    for row in profile_rows:

        if (
            row["null_percentage"]
            > null_threshold
        ):

            candidates.append({
                "catalog":
                    row["catalog"],
                "schema":
                    row["schema"],
                "table":
                    row["table"],
                "column_name":
                    row["column_name"],
                "candidate_rule":
                    "NULL_CHECK",
                "reason":
                    f"Null percentage exceeds "
                    f"{null_threshold}%",
                "created_timestamp":
                    datetime.now()
            })

        if (
            row["distinct_count"] == 1
            and row["row_count"] > 1
        ):

            candidates.append({
                "catalog":
                    row["catalog"],
                "schema":
                    row["schema"],
                "table":
                    row["table"],
                "column_name":
                    row["column_name"],
                "candidate_rule":
                    "LOW_CARDINALITY",
                "reason":
                    "Column contains one "
                    "distinct value",
                "created_timestamp":
                    datetime.now()
            })

    return candidates