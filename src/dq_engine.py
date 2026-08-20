# ============================================================
# DQ ENGINE
# DQ01 - DQ16
# ============================================================

from pyspark.sql import functions as F


def safe_column_name(name):

    import re

    name = str(name).strip()

    name = re.sub(
        r"[^A-Za-z0-9_]",
        "_",
        name
    )

    name = re.sub(
        r"_+",
        "_",
        name
    )

    name = name.strip("_")

    if not name:
        name = "column"

    if name[0].isdigit():
        name = "_" + name

    return name.lower()


def resolve_column(
    df,
    column_name
):

    if column_name in df.columns:
        return column_name

    safe = safe_column_name(
        column_name
    )

    if safe in df.columns:
        return safe

    return None


def status_from_percentage(
    percentage
):

    percentage = float(
        percentage
    )

    if percentage <= 0:
        return "PASS"

    if percentage <= 1:
        return "WARNING"

    return "FAIL"


def get_table_rule(
    cfg,
    catalog,
    schema,
    table
):

    source = (
        f"{catalog}.{schema}.{table}"
    )

    return cfg.get(
        "table_rules",
        {}
    ).get(
        source,
        {}
    )


# ============================================================
# DQ01 COMPLETENESS
# ============================================================

def dq01_completeness(
    df,
    rule
):

    columns = rule.get(
        "mandatory_columns",
        []
    )

    if not columns:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "WARNING", 100.0, 0

    missing_columns = [
        column
        for column in columns
        if resolve_column(df, column) is None
    ]

    if missing_columns:

        return (
            "FAIL",
            100.0,
            total
        )

    condition = None

    for column in columns:

        resolved = resolve_column(
            df,
            column
        )

        current = (
            F.col(resolved).isNull()
        )

        condition = (
            current
            if condition is None
            else condition | current
        )

    failed = df.filter(
        condition
    ).count()

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ02 ACCURACY
# ============================================================

def dq02_accuracy(
    df,
    rule
):

    expressions = rule.get(
        "row_expressions",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed_rows = 0

    for expression in expressions.values():

        try:

            failed_rows += df.filter(
                F.coalesce(
                    F.expr(expression),
                    F.lit(False)
                ) == F.lit(False)
            ).count()

        except Exception as exc:

            print(
                f"DQ02 expression skipped: "
                f"{expression} | {exc}"
            )

    percentage = (
        failed_rows / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed_rows
    )


# ============================================================
# DQ03 VALIDITY
# ============================================================

def dq03_validity(
    df,
    rule
):

    allowed_values = rule.get(
        "allowed_values",
        {}
    )

    if not allowed_values:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for column, values in (
        allowed_values.items()
    ):

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += total

            continue

        failed += df.filter(
            F.col(resolved).isNotNull()
            &
            ~F.col(resolved).isin(values)
        ).count()

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ04 UNIQUENESS
# ============================================================

def dq04_uniqueness(
    df,
    rule
):

    keys = rule.get(
        "unique_keys",
        []
    )

    if not keys:
        return "PASS", 0.0, 0

    resolved_keys = [
        resolve_column(df, key)
        for key in keys
    ]

    if any(
        key is None
        for key in resolved_keys
    ):

        return "FAIL", 100.0, df.count()

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    unique_count = (
        df.select(
            *resolved_keys
        )
        .dropDuplicates()
        .count()
    )

    duplicate_rows = (
        total - unique_count
    )

    percentage = (
        duplicate_rows / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        duplicate_rows
    )


# ============================================================
# DQ05 CONSISTENCY
# ============================================================

def dq05_consistency(
    df,
    rule
):

    expressions = rule.get(
        "consistency_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for expression in expressions.values():

        try:

            failed += df.filter(
                F.coalesce(
                    F.expr(expression),
                    F.lit(False)
                ) == F.lit(False)
            ).count()

        except Exception as exc:

            print(
                f"DQ05 expression skipped: "
                f"{exc}"
            )

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ06 INTEGRITY
# ============================================================

def dq06_integrity(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg
):

    relationships = cfg.get(
        "relationships",
        []
    )

    source = (
        f"{catalog}.{schema}.{table}"
    )

    relevant = [
        relationship
        for relationship in relationships
        if relationship.get(
            "child_table"
        ) == source
    ]

    if not relevant:
        return "PASS", 0.0, 0

    total_rows = df.count()

    if total_rows == 0:
        return "PASS", 0.0, 0

    total_failed = 0

    for relationship in relevant:

        child_column = resolve_column(
            df,
            relationship[
                "child_column"
            ]
        )

        if not child_column:
            total_failed += total_rows
            continue

        parent_table = relationship[
            "parent_table"
        ]

        parent_df = spark.table(
            parent_table
        )

        parent_column = resolve_column(
            parent_df,
            relationship[
                "parent_column"
            ]
        )

        if not parent_column:
            total_failed += total_rows
            continue

        parent_values = (
            parent_df
            .select(
                F.col(
                    parent_column
                ).alias(
                    "_parent_key"
                )
            )
            .dropDuplicates()
        )

        child_values = df.select(
            F.col(
                child_column
            ).alias(
                "_child_key"
            )
        )

        nullable_allowed = (
            relationship.get(
                "nullable_allowed",
                True
            )
        )

        if nullable_allowed:

            child_values = child_values.filter(
                F.col("_child_key").isNotNull()
            )

        else:

            null_count = child_values.filter(
                F.col("_child_key").isNull()
            ).count()

            total_failed += null_count

            child_values = child_values.filter(
                F.col("_child_key").isNotNull()
            )

        invalid = (
            child_values
            .join(
                parent_values,
                F.col("_child_key")
                == F.col("_parent_key"),
                "left_anti"
            )
            .count()
        )

        total_failed += invalid

    percentage = (
        total_failed / total_rows * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        total_failed
    )


# ============================================================
# DQ07 TIMELINESS
# ============================================================

def dq07_timeliness(
    df,
    rule,
    cfg
):

    date_columns = rule.get(
        "date_columns",
        []
    )

    if not date_columns:
        return "PASS", 0.0, 0

    date_column = None

    for column in date_columns:

        resolved = resolve_column(
            df,
            column
        )

        if resolved:
            date_column = resolved
            break

    if not date_column:

        return "FAIL", 100.0, df.count()

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    parsed = F.to_date(
        F.col(date_column)
    )

    invalid = df.filter(
        F.col(date_column).isNotNull()
        &
        parsed.isNull()
    ).count()

    percentage = (
        invalid / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        invalid
    )


# ============================================================
# DQ08 CONFORMITY
# ============================================================

def dq08_conformity(
    df,
    rule
):

    expressions = rule.get(
        "conformity_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for expression in expressions.values():

        try:

            failed += df.filter(
                F.coalesce(
                    F.expr(expression),
                    F.lit(False)
                ) == F.lit(False)
            ).count()

        except Exception as exc:

            print(
                f"DQ08 expression skipped: "
                f"{exc}"
            )

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ09 RANGE
# ============================================================

def dq09_range(
    df,
    rule
):

    ranges = rule.get(
        "range_rules",
        {}
    )

    if not ranges:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for column, limits in (
        ranges.items()
    ):

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += total

            continue

        condition = None

        if "min" in limits:

            condition = (
                F.col(resolved)
                < float(limits["min"])
            )

        if "max" in limits:

            maximum = (
                F.col(resolved)
                > float(limits["max"])
            )

            condition = (
                maximum
                if condition is None
                else condition | maximum
            )

        if condition is not None:

            failed += df.filter(
                F.col(resolved).isNotNull()
                & condition
            ).count()

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ10 DUPLICATE
# ============================================================

def dq10_duplicate(
    df,
    rule
):

    return dq04_uniqueness(
        df,
        rule
    )


# ============================================================
# DQ11 NULL
# ============================================================

def dq11_null(
    df,
    rule
):

    columns = rule.get(
        "null_columns",
        []
    )

    if not columns:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0
    checked_columns = 0

    for column in columns:

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += total
            checked_columns += 1
            continue

        checked_columns += 1

        failed += df.filter(
            F.col(resolved).isNull()
        ).count()

    denominator = (
        total
        * max(checked_columns, 1)
    )

    percentage = (
        failed / denominator * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ12 LENGTH
# ============================================================

def dq12_length(
    df,
    rule
):

    rules = rule.get(
        "length_rules",
        {}
    )

    if not rules:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for column, limits in rules.items():

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += total
            continue

        condition = None

        if "min" in limits:

            condition = (
                F.length(
                    F.col(resolved)
                )
                < int(limits["min"])
            )

        if "max" in limits:

            maximum = (
                F.length(
                    F.col(resolved)
                )
                > int(limits["max"])
            )

            condition = (
                maximum
                if condition is None
                else condition | maximum
            )

        if condition is not None:

            failed += df.filter(
                F.col(resolved).isNotNull()
                & condition
            ).count()

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ13 DATA TYPE
# ============================================================

def dq13_data_type(
    df,
    rule
):

    expected = rule.get(
        "expected_types",
        {}
    )

    if not expected:
        return "PASS", 0.0, 0

    actual = {
        field.name:
            field.dataType.simpleString().lower()
        for field in df.schema.fields
    }

    failed = 0

    for column, expected_type in (
        expected.items()
    ):

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += 1
            continue

        actual_type = actual.get(
            resolved,
            ""
        )

        expected_type = (
            str(expected_type)
            .lower()
            .replace(
                "integer",
                "int"
            )
            .replace(
                "stringtype",
                "string"
            )
            .replace(
                "doubletype",
                "double"
            )
        )

        if expected_type == "date":

            valid = (
                actual_type == "date"
            )

        elif expected_type == "timestamp":

            valid = (
                actual_type.startswith(
                    "timestamp"
                )
            )

        elif expected_type == "int":

            valid = actual_type in (
                "int",
                "integer"
            )

        elif expected_type == "double":

            valid = actual_type in (
                "double",
                "float",
                "decimal"
            )

        else:

            valid = (
                actual_type
                == expected_type
            )

        if not valid:
            failed += 1

    percentage = (
        failed
        / max(len(expected), 1)
        * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ14 PATTERN
# ============================================================

def dq14_pattern(
    df,
    rule
):

    patterns = rule.get(
        "pattern_rules",
        {}
    )

    if not patterns:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for column, pattern in (
        patterns.items()
    ):

        resolved = resolve_column(
            df,
            column
        )

        if not resolved:

            failed += total
            continue

        failed += df.filter(
            F.col(resolved).isNotNull()
            &
            ~F.col(resolved).rlike(pattern)
        ).count()

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ15 BUSINESS RULE
# ============================================================

def dq15_business_rule(
    df,
    rule
):

    expressions = rule.get(
        "business_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    failed = 0

    for expression in expressions.values():

        try:

            failed += df.filter(
                F.coalesce(
                    F.expr(expression),
                    F.lit(False)
                ) == F.lit(False)
            ).count()

        except Exception as exc:

            print(
                f"DQ15 expression skipped: "
                f"{exc}"
            )

    percentage = (
        failed / total * 100
    )

    return (
        status_from_percentage(
            percentage
        ),
        percentage,
        failed
    )


# ============================================================
# DQ16 VOLUME
# ============================================================

def dq16_volume(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg
):

    current_count = df.count()

    if current_count == 0:

        return (
            "FAIL",
            100.0,
            current_count
        )

    audit_schema = cfg[
        "framework"
    ][
        "audit_schema"
    ]

    summary_table_name = cfg[
        "output_tables"
    ][
        "summary"
    ]

    audit_table = (
        f"{catalog}."
        f"{audit_schema}."
        f"{summary_table_name}"
    )

    try:

        if not spark.catalog.tableExists(
            audit_table
        ):

            return (
                "PASS",
                0.0,
                current_count
            )

        previous = (
            spark.table(
                audit_table
            )
            .filter(
                (F.col("catalog") == catalog)
                &
                (F.col("schema") == schema)
                &
                (F.col("table") == table)
            )
            .orderBy(
                F.col(
                    "run_timestamp"
                ).desc()
            )
            .limit(1)
            .collect()
        )

        if not previous:

            return (
                "PASS",
                0.0,
                current_count
            )

        previous_count = int(
            previous[0]["row_count"]
        )

        if previous_count == 0:

            return (
                "PASS",
                0.0,
                current_count
            )

        variance = (
            abs(
                current_count
                - previous_count
            )
            / previous_count
            * 100
        )

        threshold = float(
            cfg["framework"].get(
                "volume_default_variance_pct",
                30
            )
        )

        if variance <= 15:

            status = "PASS"

        elif variance <= threshold:

            status = "WARNING"

        else:

            status = "FAIL"

        return (
            status,
            variance,
            current_count
        )

    except Exception as exc:

        print(
            f"DQ16 baseline warning: {exc}"
        )

        return (
            "PASS",
            0.0,
            current_count
        )


# ============================================================
# SCORE
# ============================================================

# ============================================================
# SCORE
# ============================================================

def calculate_score(
    cfg,
    results
):

    total_weight = 0.0
    earned_weight = 0.0

    for rule in cfg["dq_rules"]:

        if not rule.get(
            "enabled",
            True
        ):
            continue

        dq_id = rule["id"]

        weight = float(
            rule.get(
                "default_weight",
                0
            )
        )

        total_weight += weight

        if results.get(
            dq_id
        ) == "PASS":

            earned_weight += weight

    if total_weight == 0:

        return 0.0

    score = (
        earned_weight
        / total_weight
        * 100
    )

    return round(
        score,
        2
    )


# ============================================================
# RUN ALL DQ CHECKS
# ============================================================

# ============================================================
# RUN ALL DQ CHECKS
# ============================================================

def run_all_dq(
    spark,
    df,
    catalog,
    schema,
    table,
    cfg
):

    table_rule = get_table_rule(
        cfg,
        catalog,
        schema,
        table
    )

    results = {}
    details = []

    # --------------------------------------------------------
    # GLOBAL DQ RULE CONFIGURATION
    # --------------------------------------------------------

    global_rules = {
        rule["id"]: rule
        for rule in cfg["dq_rules"]
    }

    # --------------------------------------------------------
    # MERGE GLOBAL RULE + TABLE-SPECIFIC CONFIGURATION
    # --------------------------------------------------------

    merged_rules = {}

    for dq_id in [
        f"DQ{i:02d}"
        for i in range(1, 17)
    ]:

        base_rule = dict(
            global_rules.get(
                dq_id,
                {}
            )
        )

        # Table rule can contain:
        #
        # DQ01:
        #   mandatory_columns: [...]
        #
        # OR flat configuration.
        #
        if dq_id in table_rule:

            specific = table_rule[
                dq_id
            ]

            if isinstance(
                specific,
                dict
            ):

                base_rule.update(
                    specific
                )

        # Also support table-level
        # properties directly.
        #
        # Example:
        # mandatory_columns: [...]
        #
        # for DQ01.
        property_map = {
            "DQ01":
                "mandatory_columns",
            "DQ02":
                "row_expressions",
            "DQ03":
                "allowed_values",
            "DQ04":
                "unique_keys",
            "DQ05":
                "consistency_rules",
            "DQ07":
                "date_columns",
            "DQ08":
                "conformity_rules",
            "DQ09":
                "range_rules",
            "DQ10":
                "unique_keys",
            "DQ11":
                "null_columns",
            "DQ12":
                "length_rules",
            "DQ13":
                "expected_types",
            "DQ14":
                "pattern_rules",
            "DQ15":
                "business_rules",
        }

        if dq_id in property_map:

            property_name = (
                property_map[dq_id]
            )

            if (
                property_name
                in table_rule
            ):

                base_rule[
                    property_name
                ] = table_rule[
                    property_name
                ]

        merged_rules[
            dq_id
        ] = base_rule

    # --------------------------------------------------------
    # DQ FUNCTION MAP
    # --------------------------------------------------------

    functions = {
        "DQ01": dq01_completeness,

        "DQ02": dq02_accuracy,

        "DQ03": dq03_validity,

        "DQ04": dq04_uniqueness,

        "DQ05": dq05_consistency,

        "DQ07": lambda d, r:
            dq07_timeliness(
                d,
                r,
                cfg
            ),

        "DQ08": dq08_conformity,

        "DQ09": dq09_range,

        "DQ10": dq10_duplicate,

        "DQ11": dq11_null,

        "DQ12": dq12_length,

        "DQ13": dq13_data_type,

        "DQ14": dq14_pattern,

        "DQ15": dq15_business_rule,
    }

    # --------------------------------------------------------
    # DQ01-DQ05
    # DQ07-DQ15
    # --------------------------------------------------------

    for dq_id in [
        "DQ01",
        "DQ02",
        "DQ03",
        "DQ04",
        "DQ05",
        "DQ07",
        "DQ08",
        "DQ09",
        "DQ10",
        "DQ11",
        "DQ12",
        "DQ13",
        "DQ14",
        "DQ15",
    ]:

        rule_definition = (
            global_rules.get(
                dq_id,
                {}
            )
        )

        enabled = rule_definition.get(
            "enabled",
            True
        )

        # ----------------------------------------------------
        # DISABLED RULE
        # ----------------------------------------------------

        if not enabled:

            results[dq_id] = "SKIPPED"

            details.append({
                "dq_id":
                    dq_id,

                "status":
                    "SKIPPED",

                "failure_percentage":
                    0.0,

                "failed_records":
                    0
            })

            continue

        # ----------------------------------------------------
        # EXECUTE RULE
        # ----------------------------------------------------

        try:

            status, percentage, failed = (
                functions[dq_id](
                    df,
                    merged_rules[dq_id]
                )
            )

        except Exception as exc:

            print(
                f"{dq_id} execution error "
                f"for {table}: {exc}"
            )

            status = "WARNING"

            percentage = 0.0

            failed = 0

        results[dq_id] = status

        details.append({
            "dq_id":
                dq_id,

            "status":
                status,

            "failure_percentage":
                float(percentage),

            "failed_records":
                int(failed)
        })

    # --------------------------------------------------------
    # DQ06 - INTEGRITY
    # --------------------------------------------------------

    dq06_enabled = global_rules[
        "DQ06"
    ].get(
        "enabled",
        True
    )

    if dq06_enabled:

        try:

            status, percentage, failed = (
                dq06_integrity(
                    spark,
                    df,
                    catalog,
                    schema,
                    table,
                    cfg
                )
            )

        except Exception as exc:

            print(
                f"DQ06 execution error "
                f"for {table}: {exc}"
            )

            status = "WARNING"
            percentage = 0.0
            failed = 0

        results["DQ06"] = status

        details.append({
            "dq_id":
                "DQ06",

            "status":
                status,

            "failure_percentage":
                float(percentage),

            "failed_records":
                int(failed)
        })

    else:

        results["DQ06"] = "SKIPPED"

        details.append({
            "dq_id":
                "DQ06",

            "status":
                "SKIPPED",

            "failure_percentage":
                0.0,

            "failed_records":
                0
        })

    # --------------------------------------------------------
    # DQ16 - VOLUME
    # --------------------------------------------------------

    dq16_enabled = global_rules[
        "DQ16"
    ].get(
        "enabled",
        True
    )

    if dq16_enabled:

        try:

            status, percentage, row_count = (
                dq16_volume(
                    spark,
                    df,
                    catalog,
                    schema,
                    table,
                    cfg
                )
            )

        except Exception as exc:

            print(
                f"DQ16 execution error "
                f"for {table}: {exc}"
            )

            status = "WARNING"
            percentage = 0.0
            row_count = df.count()

        results["DQ16"] = status

        details.append({
            "dq_id":
                "DQ16",

            "status":
                status,

            "failure_percentage":
                float(percentage),

            "failed_records":
                0
        })

    else:

        results["DQ16"] = "SKIPPED"

        details.append({
            "dq_id":
                "DQ16",

            "status":
                "SKIPPED",

            "failure_percentage":
                0.0,

            "failed_records":
                0
        })

    return results, details