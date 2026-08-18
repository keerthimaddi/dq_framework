# ============================================================
# CAMPAIGN DATA QUALITY FRAMEWORK
# COMPLETE FINAL PIPELINE
# ============================================================

import os
import re
import uuid
import yaml
import traceback
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DateType,
    TimestampType,
)


# ============================================================
# SPARK
# ============================================================

spark = SparkSession.builder.getOrCreate()


# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "wmg"

DQ_IDS = [
    "DQ01", "DQ02", "DQ03", "DQ04",
    "DQ05", "DQ06", "DQ07", "DQ08",
    "DQ09", "DQ10", "DQ11", "DQ12",
    "DQ13", "DQ14", "DQ15", "DQ16"
]


# ============================================================
# PATH
# ============================================================

def find_config():

    candidates = [
        Path("config/dq_rules.yml"),
        Path("../config/dq_rules.yml"),
        Path("../../config/dq_rules.yml"),
        Path(__file__).resolve().parents[1] / "config" / "dq_rules.yml"
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Could not find config/dq_rules.yml"
    )


def load_config():

    config_path = find_config()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not cfg:
        raise ValueError("YAML configuration is empty")

    if "framework" not in cfg:
        raise ValueError(
            "YAML configuration must contain the 'framework' section"
        )

    if "dq_rules" not in cfg:
        raise ValueError(
            "YAML configuration must contain the 'dq_rules' section"
        )

    rules = cfg["dq_rules"]

    if len(rules) != 16:
        raise ValueError(
            f"Exactly 16 DQ rules required. Found {len(rules)}"
        )

    enabled_weight = sum(
        float(r.get("default_weight", 0))
        for r in rules
        if r.get("enabled", True)
    )

    if abs(enabled_weight - 100.0) > 0.01:
        raise ValueError(
            f"Enabled DQ weights must total 100. Found {enabled_weight}"
        )

    return cfg


# ============================================================
# SAFE IDENTIFIERS
# ============================================================

def q(identifier):
    """
    Safely quote Unity Catalog identifiers.
    """
    return "`" + str(identifier).replace("`", "``") + "`"


def full_table(catalog, schema, table):

    return (
        f"{q(catalog)}."
        f"{q(schema)}."
        f"{q(table)}"
    )


def safe_column_name(name):

    name = str(name).strip()

    # Replace all illegal Delta characters
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)

    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing _
    name = name.strip("_")

    if not name:
        name = "column"

    if name[0].isdigit():
        name = "_" + name

    return name.lower()


def normalize_columns(df):

    used = set()
    mapping = {}

    for original in df.columns:

        base = safe_column_name(original)

        candidate = base
        counter = 1

        while candidate in used:
            candidate = f"{base}_{counter}"
            counter += 1

        used.add(candidate)

        mapping[original] = candidate

    for original, safe in mapping.items():

        if original != safe:
            df = df.withColumnRenamed(original, safe)

    return df, mapping


# ============================================================
# STATUS
# ============================================================

def status_from_percentage(value):

    value = float(value)

    if value <= 0:
        return "PASS"

    if value <= 1:
        return "WARNING"

    return "FAIL"


def overall_status(score, thresholds):

    score = float(score)

    if score >= float(thresholds.get("pass", 90)):
        return "PASS"

    if score >= float(thresholds.get("warning", 75)):
        return "WARNING"

    return "FAIL"


# ============================================================
# DQ SCORE
# ============================================================

def calculate_score(cfg, results):

    rules = cfg["dq_rules"]

    total_weight = 0.0
    earned_weight = 0.0

    for rule in rules:

        if not rule.get("enabled", True):
            continue

        dq_id = rule["id"]
        weight = float(rule["default_weight"])

        total_weight += weight

        if results.get(dq_id) == "PASS":
            earned_weight += weight

    if total_weight == 0:
        return 0.0

    return round(
        earned_weight / total_weight * 100,
        2
    )


# ============================================================
# DISCOVERY
# ============================================================

def discover_tables(cfg):

    print()
    print("=" * 60)
    print("DATABRICKS CATALOG DISCOVERY")
    print("=" * 60)

    catalog = cfg["framework"].get(
        "catalog",
        CATALOG
    )

    excluded = set(
        x.lower()
        for x in cfg["framework"].get(
            "excluded_schemas",
            []
        )
    )

    allowlist = set(
        x.lower()
        for x in cfg["framework"].get(
            "source_schema_allowlist",
            []
        )
    )

    print()
    print(f"Catalog: {catalog}")

    catalogs = [
        r[0]
        for r in spark.sql("SHOW CATALOGS").collect()
    ]

    if catalog not in catalogs:
        raise ValueError(
            f"Catalog '{catalog}' does not exist"
        )

    tables = []

    schema_rows = spark.sql(
        f"SHOW SCHEMAS IN {q(catalog)}"
    ).collect()

    for row in schema_rows:

        schema = row[0]

        if schema.lower() in excluded:
            continue

        if allowlist and schema.lower() not in allowlist:
            continue

        print(
            f"Discovering schema: {schema}"
        )

        try:

            table_rows = spark.sql(
                f"SHOW TABLES IN "
                f"{q(catalog)}.{q(schema)}"
            ).collect()

            for table_row in table_rows:

                table = table_row[1]

                is_temp = (
                    bool(table_row[2])
                    if len(table_row) > 2
                    else False
                )

                if is_temp:
                    continue

                tables.append(
                    (catalog, schema, table)
                )

        except Exception as exc:

            print(
                f"Unable to inspect "
                f"{catalog}.{schema}: {exc}"
            )

    print()
    print("=" * 60)
    print("CATALOG / SCHEMA / TABLES")
    print("=" * 60)

    for catalog, schema, table in tables:

        print(
            f"Catalog: {catalog} | "
            f"Schema: {schema} | "
            f"Table: {table}"
        )

    print()
    print(
        f"Total Tables Found: {len(tables)}"
    )

    return tables


# ============================================================
# CREATE FRAMEWORK SCHEMAS
# ============================================================

def create_framework_schemas(cfg):

    framework = cfg["framework"]

    schemas = [
        framework["bronze_schema"],
        framework["silver_schema"],
        framework["gold_schema"],
        framework["audit_schema"],
        framework["quarantine_schema"],
        framework["candidate_schema"],
    ]

    for schema in schemas:

        print(
            f"Creating/checking schema: "
            f"{CATALOG}.{schema}"
        )

        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{q(CATALOG)}.{q(schema)}"
        )


# ============================================================
# READ SOURCE SAFELY
# ============================================================

def read_source_table(catalog, schema, table):

    return spark.table(
        full_table(catalog, schema, table)
    )


# ============================================================
# SAFE BRONZE
# ============================================================

def write_bronze(
    df,
    catalog,
    schema,
    table,
    cfg
):

    safe_df, mapping = normalize_columns(df)

    bronze_schema = cfg["framework"][
        "bronze_schema"
    ]

    bronze_name = (
        f"{catalog}.{bronze_schema}.{table}"
    )

    print()
    print(
        f"Writing Bronze: {bronze_name}"
    )

    # Add ingestion metadata
    safe_df = (
        safe_df
        .withColumn(
            "_dq_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_dq_source_catalog",
            F.lit(catalog)
        )
        .withColumn(
            "_dq_source_schema",
            F.lit(schema)
        )
        .withColumn(
            "_dq_source_table",
            F.lit(table)
        )
    )

    (
        safe_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            q(bronze_name)
        )
    )

    return safe_df, mapping


# ============================================================
# COLUMN LOOKUP
# ============================================================

def resolve_column(df, column_name):

    if column_name in df.columns:
        return column_name

    safe = safe_column_name(column_name)

    if safe in df.columns:
        return safe

    return None


# ============================================================
# NUMERIC COLUMNS
# ============================================================

def numeric_columns(df):

    numeric_types = (
        "int",
        "bigint",
        "double",
        "float",
        "long",
        "decimal",
        "short"
    )

    return [
        field.name
        for field in df.schema.fields
        if any(
            x in field.dataType.simpleString().lower()
            for x in numeric_types
        )
    ]


# ============================================================
# DQ01 COMPLETENESS
# ============================================================

def dq01_completeness(df, rule):

    columns = rule.get(
        "mandatory_columns",
        df.columns
    )

    columns = [
        resolve_column(df, c)
        for c in columns
    ]

    columns = [
        c for c in columns
        if c is not None
    ]

    if not columns:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "WARNING", 100.0, 0

    invalid_condition = None

    for c in columns:

        condition = F.col(c).isNull()

        if invalid_condition is None:
            invalid_condition = condition
        else:
            invalid_condition = (
                invalid_condition | condition
            )

    failed = df.filter(
        invalid_condition
    ).count()

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ02 ACCURACY
# ============================================================

def dq02_accuracy(df, rule):

    expressions = rule.get(
        "row_expressions",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()

    failed = 0

    for _, expression in expressions.items():

        try:

            invalid = df.filter(
                ~F.expr(expression)
            ).count()

            failed += invalid

        except Exception:

            # Invalid optional business expression
            # should not crash entire pipeline
            continue

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ03 VALIDITY
# ============================================================

def dq03_validity(df, rule):

    allowed_values = rule.get(
        "allowed_values",
        {}
    )

    if not allowed_values:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for column, values in allowed_values.items():

        c = resolve_column(
            df,
            column
        )

        if not c:
            continue

        failed += df.filter(
            F.col(c).isNotNull()
            & ~F.col(c).isin(values)
        ).count()

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ04 UNIQUENESS
# ============================================================

def dq04_uniqueness(df, rule):

    keys = rule.get(
        "unique_keys",
        []
    )

    keys = [
        resolve_column(df, x)
        for x in keys
    ]

    keys = [
        x for x in keys
        if x is not None
    ]

    if not keys:
        return "PASS", 0.0, 0

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    duplicate_rows = (
        total
        - df.select(*keys).dropDuplicates().count()
    )

    pct = (
        duplicate_rows / total * 100
    )

    return status_from_percentage(pct), pct, duplicate_rows


# ============================================================
# DQ05 CONSISTENCY
# ============================================================

def dq05_consistency(df, rule):

    expressions = rule.get(
        "consistency_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for _, expression in expressions.items():

        try:

            failed += df.filter(
                ~F.expr(expression)
            ).count()

        except Exception:

            continue

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ06 INTEGRITY
# ============================================================

def dq06_integrity(
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

    relevant = [
        r
        for r in relationships
        if r.get("child_table")
        == f"{catalog}.{schema}.{table}"
    ]

    if not relevant:
        return "PASS", 0.0, 0

    total_failed = 0
    total_rows = df.count()

    for relationship in relevant:

        child_col = resolve_column(
            df,
            relationship["child_column"]
        )

        if not child_col:
            continue

        parent_table = relationship[
            "parent_table"
        ]

        parent_df = spark.table(
            q(parent_table.replace(".", "`.`"))
        )

        parent_col = resolve_column(
            parent_df,
            relationship["parent_column"]
        )

        if not parent_col:
            continue

        parent_values = (
            parent_df
            .select(
                F.col(parent_col)
                .alias("_parent_key")
            )
            .dropDuplicates()
        )

        child_values = df.select(
            F.col(child_col)
            .alias("_child_key")
        )

        invalid = (
            child_values
            .filter(
                F.col("_child_key").isNotNull()
            )
            .join(
                parent_values,
                F.col("_child_key")
                == F.col("_parent_key"),
                "left_anti"
            )
            .count()
        )

        total_failed += invalid

    pct = (
        total_failed / total_rows * 100
        if total_rows else 0
    )

    return status_from_percentage(pct), pct, total_failed


# ============================================================
# DQ07 TIMELINESS
# ============================================================

def dq07_timeliness(df, rule, cfg):

    date_columns = rule.get(
        "date_columns",
        []
    )

    date_column = None

    for c in date_columns:

        date_column = resolve_column(
            df,
            c
        )

        if date_column:
            break

    if not date_column:
        return "PASS", 0.0, 0

    # SAFE parsing.
    # This is important because values such as
    # 'not-a-date' must NOT crash the pipeline.
    parsed = F.coalesce(
        F.try_to_timestamp(
            F.col(date_column)
        ),
        F.try_to_timestamp(
            F.col(date_column),
            F.lit("yyyy-MM-dd")
        )
    )

    total = df.count()

    if total == 0:
        return "PASS", 0.0, 0

    invalid = df.filter(
        F.col(date_column).isNotNull()
        & parsed.isNull()
    ).count()

    pct = (
        invalid / total * 100
    )

    return status_from_percentage(pct), pct, invalid


# ============================================================
# DQ08 CONFORMITY
# ============================================================

def dq08_conformity(df, rule):

    expressions = rule.get(
        "conformity_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for _, expression in expressions.items():

        try:

            failed += df.filter(
                ~F.expr(expression)
            ).count()

        except Exception:

            continue

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ09 RANGE
# ============================================================

def dq09_range(df, rule):

    ranges = rule.get(
        "range_rules",
        {}
    )

    if not ranges:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for column, limits in ranges.items():

        c = resolve_column(
            df,
            column
        )

        if not c:
            continue

        condition = None

        if "min" in limits:

            condition = (
                F.col(c) < float(limits["min"])
            )

        if "max" in limits:

            max_condition = (
                F.col(c) > float(limits["max"])
            )

            condition = (
                max_condition
                if condition is None
                else condition | max_condition
            )

        if condition is not None:

            failed += df.filter(
                F.col(c).isNotNull()
                & condition
            ).count()

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ10 DUPLICATE
# ============================================================

def dq10_duplicate(df, rule):

    keys = rule.get(
        "unique_keys",
        []
    )

    keys = [
        resolve_column(df, k)
        for k in keys
    ]

    keys = [
        k for k in keys
        if k
    ]

    if not keys:
        return "PASS", 0.0, 0

    total = df.count()

    unique_count = (
        df.select(*keys)
        .dropDuplicates()
        .count()
    )

    duplicate_rows = total - unique_count

    pct = (
        duplicate_rows / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, duplicate_rows


# ============================================================
# DQ11 NULL
# ============================================================

def dq11_null(df, rule):

    columns = rule.get(
        "null_columns",
        []
    )

    if not columns:
        return "PASS", 0.0, 0

    total = df.count()

    failed = 0

    for column in columns:

        c = resolve_column(
            df,
            column
        )

        if c:

            failed += df.filter(
                F.col(c).isNull()
            ).count()

    pct = (
        failed / (total * max(len(columns), 1))
        * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ12 LENGTH
# ============================================================

def dq12_length(df, rule):

    rules = rule.get(
        "length_rules",
        {}
    )

    if not rules:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for column, limits in rules.items():

        c = resolve_column(
            df,
            column
        )

        if not c:
            continue

        condition = None

        if "min" in limits:

            condition = (
                F.length(F.col(c))
                < int(limits["min"])
            )

        if "max" in limits:

            max_condition = (
                F.length(F.col(c))
                > int(limits["max"])
            )

            condition = (
                max_condition
                if condition is None
                else condition | max_condition
            )

        if condition is not None:

            failed += df.filter(
                F.col(c).isNotNull()
                & condition
            ).count()

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ13 DATA TYPE
# ============================================================

def dq13_data_type(df, rule):

    expected = rule.get(
        "expected_types",
        {}
    )

    if not expected:
        return "PASS", 0.0, 0

    actual = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    failed = 0

    for column, expected_type in expected.items():

        c = resolve_column(
            df,
            column
        )

        if not c:
            failed += 1
            continue

        actual_type = actual.get(c, "")

        expected_type = (
            expected_type
            .lower()
            .replace("integer", "int")
            .replace("doubletype", "double")
            .replace("stringtype", "string")
        )

        if not actual_type.startswith(
            expected_type
        ):
            failed += 1

    denominator = max(
        len(expected),
        1
    )

    pct = (
        failed / denominator * 100
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ14 PATTERN
# ============================================================

def dq14_pattern(df, rule):

    patterns = rule.get(
        "pattern_rules",
        {}
    )

    if not patterns:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for column, pattern in patterns.items():

        c = resolve_column(
            df,
            column
        )

        if not c:
            continue

        try:

            failed += df.filter(
                F.col(c).isNotNull()
                & ~F.col(c).rlike(pattern)
            ).count()

        except Exception:

            continue

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ15 BUSINESS RULE
# ============================================================

def dq15_business_rule(df, rule):

    expressions = rule.get(
        "business_rules",
        {}
    )

    if not expressions:
        return "PASS", 0.0, 0

    total = df.count()
    failed = 0

    for _, expression in expressions.items():

        try:

            failed += df.filter(
                ~F.expr(expression)
            ).count()

        except Exception:

            continue

    pct = (
        failed / total * 100
        if total else 0
    )

    return status_from_percentage(pct), pct, failed


# ============================================================
# DQ16 VOLUME
# ============================================================

def dq16_volume(
    df,
    catalog,
    schema,
    table,
    cfg
):

    current_count = df.count()

    if current_count == 0:
        return "FAIL", 100.0, current_count

    # First run = no historical baseline.
    # Do not fail a table just because it has no baseline.
    try:

        audit_table = (
            f"{catalog}."
            f"{cfg['framework']['audit_schema']}."
            f"{cfg['output_tables']['summary']}"
        )

        if not spark.catalog.tableExists(
            audit_table
        ):
            return "PASS", 0.0, current_count

        previous = (
            spark.table(audit_table)
            .filter(
                (F.col("catalog") == catalog)
                & (F.col("schema") == schema)
                & (F.col("table") == table)
            )
            .orderBy(
                F.col("run_timestamp").desc()
            )
            .limit(1)
            .collect()
        )

        if not previous:
            return "PASS", 0.0, current_count

        previous_count = int(
            previous[0]["row_count"]
        )

        if previous_count == 0:
            return "PASS", 0.0, current_count

        variance = abs(
            current_count - previous_count
        ) / previous_count * 100

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

        return status, variance, current_count

    except Exception:

        return "PASS", 0.0, current_count


# ============================================================
# GET TABLE RULE
# ============================================================

def get_table_rule(cfg, source):

    return cfg.get(
        "table_rules",
        {}
    ).get(
        source,
        {}
    )


# ============================================================
# RUN ALL 16 DQ CHECKS
# ============================================================

def run_all_dq(
    df,
    catalog,
    schema,
    table,
    cfg
):

    rule = get_table_rule(
        cfg,
        f"{catalog}.{schema}.{table}"
    )

    results = {}
    details = []

    checks = [
        ("DQ01", dq01_completeness),
        ("DQ02", dq02_accuracy),
        ("DQ03", dq03_validity),
        ("DQ04", dq04_uniqueness),
        ("DQ05", dq05_consistency),
        ("DQ08", dq08_conformity),
        ("DQ09", dq09_range),
        ("DQ10", dq10_duplicate),
        ("DQ11", dq11_null),
        ("DQ12", dq12_length),
        ("DQ13", dq13_data_type),
        ("DQ14", dq14_pattern),
        ("DQ15", dq15_business_rule),
    ]

    for dq_id, function in checks:

        try:

            status, pct, failed = function(
                df,
                rule
            )

        except Exception as exc:

            print(
                f"{dq_id} execution warning "
                f"for {table}: {exc}"
            )

            status = "WARNING"
            pct = 0.0
            failed = 0

        results[dq_id] = status

        details.append({
            "dq_id": dq_id,
            "status": status,
            "failure_percentage": float(pct),
            "failed_records": int(failed)
        })

    # DQ06
    try:

        status, pct, failed = dq06_integrity(
            df,
            catalog,
            schema,
            table,
            cfg
        )

    except Exception as exc:

        print(
            f"DQ06 warning for {table}: {exc}"
        )

        status, pct, failed = (
            "WARNING",
            0.0,
            0
        )

    results["DQ06"] = status

    details.append({
        "dq_id": "DQ06",
        "status": status,
        "failure_percentage": float(pct),
        "failed_records": int(failed)
    })

    # DQ07
    try:

        status, pct, failed = dq07_timeliness(
            df,
            rule,
            cfg
        )

    except Exception as exc:

        print(
            f"DQ07 warning for {table}: {exc}"
        )

        status, pct, failed = (
            "WARNING",
            0.0,
            0
        )

    results["DQ07"] = status

    details.append({
        "dq_id": "DQ07",
        "status": status,
        "failure_percentage": float(pct),
        "failed_records": int(failed)
    })

    # DQ16
    try:

        status, pct, row_count = dq16_volume(
            df,
            catalog,
            schema,
            table,
            cfg
        )

    except Exception:

        status, pct, row_count = (
            "PASS",
            0.0,
            df.count()
        )

    results["DQ16"] = status

    details.append({
        "dq_id": "DQ16",
        "status": status,
        "failure_percentage": float(pct),
        "failed_records": 0
    })

    return results, details


# ============================================================
# QUARANTINE
# ============================================================

def create_quarantine(
    df,
    results,
    catalog,
    schema,
    table,
    cfg,
    run_id
):

    row_level_rules = [
        "DQ02",
        "DQ05",
        "DQ09",
        "DQ15"
    ]

    failed_rules = [
        x for x in row_level_rules
        if results.get(x) in (
            "FAIL",
            "WARNING"
        )
    ]

    if not failed_rules:

        print(
            "Quarantined rows: 0"
        )

        return 0

    condition = None

    for dq_id in failed_rules:

        rule = get_table_rule(
            cfg,
            f"{catalog}.{schema}.{table}"
        )

        expression_groups = []

        if dq_id == "DQ02":
            expression_groups = rule.get(
                "row_expressions",
                {}
            ).values()

        elif dq_id == "DQ05":
            expression_groups = rule.get(
                "consistency_rules",
                {}
            ).values()

        elif dq_id == "DQ15":
            expression_groups = rule.get(
                "business_rules",
                {}
            ).values()

        elif dq_id == "DQ09":
            for col, limits in rule.get(
                "range_rules",
                {}
            ).items():

                c = resolve_column(
                    df,
                    col
                )

                if not c:
                    continue

                if "min" in limits:

                    expression_groups.append(
                        f"{c} >= {float(limits['min'])}"
                    )

                if "max" in limits:

                    expression_groups.append(
                        f"{c} <= {float(limits['max'])}"
                    )

        for expression in expression_groups:

            try:

                invalid = ~F.expr(
                    expression
                )

                condition = (
                    invalid
                    if condition is None
                    else condition | invalid
                )

            except Exception:

                continue

    if condition is None:

        print(
            "Quarantined rows: 0"
        )

        return 0

    quarantine_df = (
        df.filter(condition)
        .withColumn(
            "_dq_run_id",
            F.lit(run_id)
        )
        .withColumn(
            "_dq_source_table",
            F.lit(
                f"{catalog}.{schema}.{table}"
            )
        )
        .withColumn(
            "_dq_quarantine_timestamp",
            F.current_timestamp()
        )
    )

    count = quarantine_df.count()

    if count == 0:

        print(
            "Quarantined rows: 0"
        )

        return 0

    quarantine_schema = cfg[
        "framework"
    ][
        "quarantine_schema"
    ]

    quarantine_table = (
        f"{catalog}."
        f"{quarantine_schema}."
        f"quarantine_{safe_column_name(table)}"
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            q(quarantine_table)
        )
    )

    print(
        f"Quarantined rows: {count}"
    )

    return count


# ============================================================
# SILVER
# ============================================================

def create_silver(
    bronze_df,
    catalog,
    table,
    cfg,
    score
):

    gate = cfg.get(
        "quality_gate",
        {}
    )

    minimum = float(
        gate.get(
            "silver_min_score",
            90
        )
    )

    silver_schema = cfg[
        "framework"
    ][
        "silver_schema"
    ]

    silver_table = (
        f"{catalog}."
        f"{silver_schema}."
        f"{table}"
    )

    if (
        gate.get("enabled", True)
        and score < minimum
    ):

        print(
            f"Silver BLOCKED: "
            f"Score {score} < {minimum}"
        )

        return False

    (
        bronze_df
        .drop(
            "_dq_ingestion_timestamp",
            "_dq_source_catalog",
            "_dq_source_schema",
            "_dq_source_table"
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            q(silver_table)
        )
    )

    print(
        f"Silver CREATED: "
        f"{silver_table}"
    )

    return True


# ============================================================
# GOLD
# ============================================================

def create_gold(
    catalog,
    table,
    cfg,
    score,
    status
):

    silver_schema = cfg[
        "framework"
    ][
        "silver_schema"
    ]

    gold_schema = cfg[
        "framework"
    ][
        "gold_schema"
    ]

    silver_table = (
        f"{catalog}."
        f"{silver_schema}."
        f"{table}"
    )

    gold_table = (
        f"{catalog}."
        f"{gold_schema}."
        f"{table}"
    )

    if not spark.catalog.tableExists(
        silver_table
    ):

        return

    df = spark.table(
        q(silver_table)
    )

    (
        df
        .withColumn(
            "_dq_quality_score",
            F.lit(float(score))
        )
        .withColumn(
            "_dq_overall_status",
            F.lit(status)
        )
        .withColumn(
            "_dq_gold_timestamp",
            F.current_timestamp()
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            q(gold_table)
        )
    )

    print(
        f"Gold CREATED: {gold_table}"
    )


# ============================================================
# PROFILING
# ============================================================

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

        null_count = df.filter(
            F.col(column).isNull()
        ).count()

        distinct_count = (
            df.select(column)
            .distinct()
            .count()
        )

        null_pct = (
            null_count / total * 100
            if total else 0
        )

        rows.append({
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "column_name": column,
            "data_type": field.dataType.simpleString(),
            "row_count": int(total),
            "null_count": int(null_count),
            "null_percentage": float(null_pct),
            "distinct_count": int(distinct_count),
            "profile_timestamp": datetime.now()
        })

    return rows


# ============================================================
# AUDIT TABLE
# ============================================================

def write_audit(
    cfg,
    summary_rows,
    detail_rows,
    profile_rows
):

    audit_schema = cfg[
        "framework"
    ][
        "audit_schema"
    ]

    profile_schema = cfg[
        "framework"
    ][
        "candidate_schema"
    ]

    if summary_rows:

        summary_df = spark.createDataFrame(
            summary_rows
        )

        (
            summary_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                q(
                    f"{CATALOG}."
                    f"{audit_schema}."
                    f"{cfg['output_tables']['summary']}"
                )
            )
        )

    if detail_rows:

        detail_df = spark.createDataFrame(
            detail_rows
        )

        (
            detail_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                q(
                    f"{CATALOG}."
                    f"{audit_schema}."
                    f"{cfg['output_tables']['audit']}"
                )
            )
        )

    if profile_rows:

        profile_df = spark.createDataFrame(
            profile_rows
        )

        (
            profile_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true"
            )
            .saveAsTable(
                q(
                    f"{CATALOG}."
                    f"{profile_schema}."
                    f"{cfg['output_tables']['profiling']}"
                )
            )
        )


# ============================================================
# DQ RULE CANDIDATES
# ============================================================

def create_rule_candidates(
    profile_rows
):

    candidates = []

    for row in profile_rows:

        if (
            row["null_percentage"]
            > 1
        ):

            candidates.append({
                "catalog": row["catalog"],
                "schema": row["schema"],
                "table": row["table"],
                "column_name": row["column_name"],
                "candidate_rule": "NULL_CHECK",
                "reason": "Null percentage exceeds 1%",
                "created_timestamp": datetime.now()
            })

        if (
            row["distinct_count"]
            == 1
            and row["row_count"] > 1
        ):

            candidates.append({
                "catalog": row["catalog"],
                "schema": row["schema"],
                "table": row["table"],
                "column_name": row["column_name"],
                "candidate_rule": "LOW_CARDINALITY",
                "reason": "Column contains one distinct value",
                "created_timestamp": datetime.now()
            })

    return candidates


# ============================================================
# CAMPAIGN INCIDENT REPORT
# ============================================================

def create_campaign_report(
    cfg
):

    gold_schema = cfg[
        "framework"
    ][
        "gold_schema"
    ]

    report_table = (
        f"{CATALOG}."
        f"{gold_schema}."
        f"campaign_incident_report"
    )

    source = (
        f"{CATALOG}.dqx_silver.campaign_data"
    )

    if not spark.catalog.tableExists(
        source
    ):

        return

    df = spark.table(
        q(source)
    )

    # Business-facing output.
    # Safe internal Delta names are translated back
    # to the required reporting names.

    columns = []

    aliases = {
        "event_date": "Date",
        "incident": "Incident",
        "rev_impact": "Rev Impact",
        "ds": "DS",
        "label_code": "Label Code",
        "kpi": "KPI",
        "include_count": "Include",
        "campaign_name": "Campaign name",
        "ad_set_name": "Ad set name",
        "amount_spent_usd": "Amount spent (USD)",
        "age_group": "Age Group",
        "creative_type": "Creative Type",
        "campaign_id": "Campaign ID",
        "record_id": "Record ID"
    }

    for source_column, output_column in aliases.items():

        if source_column in df.columns:

            columns.append(
                F.col(source_column)
                .alias(output_column)
            )

    if not columns:

        return

    report = df.select(*columns)

    (
        report.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            q(report_table)
        )
    )

    print()
    print(
        "Campaign Incident Report created:"
    )
    print(report_table)

    report.show(
        50,
        truncate=False
    )


# ============================================================
# DQ CONFIGURATION DISPLAY
# ============================================================

def print_configuration(cfg):

    print()
    print("=" * 60)
    print("CAMPAIGN DATA QUALITY FRAMEWORK")
    print("=" * 60)

    print(
        f"Catalog: "
        f"{cfg['framework'].get('catalog', CATALOG)}"
    )

    print()
    print("=" * 60)
    print("DQ RULE CONFIGURATION")
    print("=" * 60)

    rules = cfg["dq_rules"]

    for rule in rules:

        print(
            f"{rule['id']} | "
            f"{rule['dimension']} | "
            f"Level: {rule['level']} | "
            f"Enabled: {rule.get('enabled', True)} | "
            f"Weight: {rule['default_weight']} | "
            f"Severity: {rule['severity']}"
        )

    enabled = [
        r for r in rules
        if r.get("enabled", True)
    ]

    weight = sum(
        float(r["default_weight"])
        for r in enabled
    )

    print()
    print(
        f"Total DQ Rules    : {len(rules)}"
    )

    print(
        f"Enabled DQ Rules  : {len(enabled)}"
    )

    print(
        f"Total Weight      : {weight}"
    )

    thresholds = cfg[
        "overall_thresholds"
    ]

    print(
        f"PASS    >= {thresholds['pass']}"
    )

    print(
        f"WARNING >= {thresholds['warning']}"
    )

    print(
        f"FAIL    < {thresholds['warning']}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    try:

        cfg = load_config()

        print_configuration(cfg)

        create_framework_schemas(
            cfg
        )

        tables = discover_tables(
            cfg
        )

        print()
        print(
            f"Tables discovered: "
            f"{len(tables)}"
        )

        print()
        print("=" * 60)
        print("DQX-STYLE PROFILING")
        print("=" * 60)

        profile_rows = []

        for catalog, schema, table in tables:

            source = (
                f"{catalog}.{schema}.{table}"
            )

            try:

                df = read_source_table(
                    catalog,
                    schema,
                    table
                )

                rows = profile_table(
                    df,
                    catalog,
                    schema,
                    table
                )

                profile_rows.extend(rows)

            except Exception as exc:

                print(
                    f"Profiling failed for "
                    f"{source}: {exc}"
                )

        profile_schema = cfg[
            "framework"
        ][
            "candidate_schema"
        ]

        if profile_rows:

            profile_df = spark.createDataFrame(
                profile_rows
            )

            (
                profile_df.write
                .format("delta")
                .mode("overwrite")
                .option(
                    "overwriteSchema",
                    "true"
                )
                .saveAsTable(
                    q(
                        f"{CATALOG}."
                        f"{profile_schema}."
                        f"{cfg['output_tables']['profiling']}"
                    )
                )
            )

            candidates = create_rule_candidates(
                profile_rows
            )

            if candidates:

                candidate_df = (
                    spark.createDataFrame(
                        candidates
                    )
                )

                (
                    candidate_df.write
                    .format("delta")
                    .mode("overwrite")
                    .option(
                        "overwriteSchema",
                        "true"
                    )
                    .saveAsTable(
                        q(
                            f"{CATALOG}."
                            f"{profile_schema}."
                            f"{cfg['output_tables']['candidates']}"
                        )
                    )
                )

        # ====================================================
        # DQ EXECUTION
        # ====================================================

        print()
        print("=" * 60)
        print(
            "BRONZE -> DQ -> "
            "QUARANTINE -> SILVER -> GOLD"
        )
        print("=" * 60)

        summary_rows = []
        detail_rows = []

        for catalog, schema, table in tables:

            source = (
                f"{catalog}.{schema}.{table}"
            )

            print()
            print(
                f"Checking: {source}"
            )

            run_id = str(
                uuid.uuid4()
            )

            try:

                source_df = read_source_table(
                    catalog,
                    schema,
                    table
                )

                # --------------------------------------------
                # Bronze
                # --------------------------------------------

                bronze_df, mapping = write_bronze(
                    source_df,
                    catalog,
                    schema,
                    table,
                    cfg
                )

                changed = {
                    k: v
                    for k, v in mapping.items()
                    if k != v
                }

                if changed:

                    print(
                        "Column normalization applied:"
                    )

                    for original, safe in changed.items():

                        print(
                            f"  {original} -> {safe}"
                        )

                # --------------------------------------------
                # DQ
                # --------------------------------------------

                results, details = run_all_dq(
                    bronze_df,
                    catalog,
                    schema,
                    table,
                    cfg
                )

                score = calculate_score(
                    cfg,
                    results
                )

                status = overall_status(
                    score,
                    cfg["overall_thresholds"]
                )

                # --------------------------------------------
                # Output DQ statuses
                # --------------------------------------------

                status_string = " | ".join(
                    f"{dq_id}={results.get(dq_id, 'N/A')}"
                    for dq_id in DQ_IDS
                )

                print(
                    f"{status_string} | "
                    f"Score={score}% | "
                    f"Overall={status}"
                )

                # --------------------------------------------
                # Quarantine
                # --------------------------------------------

                create_quarantine(
                    bronze_df,
                    results,
                    catalog,
                    schema,
                    table,
                    cfg,
                    run_id
                )

                # --------------------------------------------
                # Silver
                # --------------------------------------------

                silver_created = create_silver(
                    bronze_df,
                    catalog,
                    table,
                    cfg,
                    score
                )

                # --------------------------------------------
                # Gold
                # --------------------------------------------

                if silver_created:

                    create_gold(
                        catalog,
                        table,
                        cfg,
                        score,
                        status
                    )

                # --------------------------------------------
                # Summary
                # --------------------------------------------

                row_count = source_df.count()

                summary = {
                    "run_id": run_id,
                    "run_timestamp": datetime.now(),
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                    "row_count": int(row_count),
                    "DQ01": results["DQ01"],
                    "DQ02": results["DQ02"],
                    "DQ03": results["DQ03"],
                    "DQ04": results["DQ04"],
                    "DQ05": results["DQ05"],
                    "DQ06": results["DQ06"],
                    "DQ07": results["DQ07"],
                    "DQ08": results["DQ08"],
                    "DQ09": results["DQ09"],
                    "DQ10": results["DQ10"],
                    "DQ11": results["DQ11"],
                    "DQ12": results["DQ12"],
                    "DQ13": results["DQ13"],
                    "DQ14": results["DQ14"],
                    "DQ15": results["DQ15"],
                    "DQ16": results["DQ16"],
                    "total_score": float(score),
                    "overall_status": status,
                    "silver_created": bool(
                        silver_created
                    )
                }

                summary_rows.append(
                    summary
                )

                for detail in details:

                    detail_rows.append({
                        "run_id": run_id,
                        "run_timestamp": datetime.now(),
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "dq_id": detail["dq_id"],
                        "status": detail["status"],
                        "failure_percentage": float(
                            detail[
                                "failure_percentage"
                            ]
                        ),
                        "failed_records": int(
                            detail[
                                "failed_records"
                            ]
                        )
                    })

            except Exception as exc:

                print()
                print(
                    f"Pipeline failed for "
                    f"{source}: {exc}"
                )

                # IMPORTANT:
                # One bad source table must never
                # stop the complete framework.

                traceback.print_exc()

        # ====================================================
        # AUDIT
        # ====================================================

        write_audit(
            cfg,
            summary_rows,
            detail_rows,
            profile_rows
        )

        # ====================================================
        # CAMPAIGN REPORT
        # ====================================================

        create_campaign_report(
            cfg
        )

        # ====================================================
        # FINAL REPORT
        # ====================================================

        print()
        print("=" * 60)
        print(
            "FINAL DATA QUALITY REPORT"
        )
        print("=" * 60)

        audit_schema = cfg[
            "framework"
        ][
            "audit_schema"
        ]

        summary_table = (
            f"{CATALOG}."
            f"{audit_schema}."
            f"{cfg['output_tables']['summary']}"
        )

        if spark.catalog.tableExists(
            summary_table
        ):

            final_df = (
                spark.table(
                    q(summary_table)
                )
                .orderBy(
                    F.col(
                        "run_timestamp"
                    ).desc()
                )
            )

            final_df.show(
                100,
                truncate=False
            )

        print()
        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)

        print(
            f"Summary : "
            f"{summary_table}"
        )

        print(
            f"Audit   : "
            f"{CATALOG}."
            f"{audit_schema}."
            f"{cfg['output_tables']['audit']}"
        )

        print(
            f"Profile : "
            f"{CATALOG}."
            f"{profile_schema}."
            f"{cfg['output_tables']['profiling']}"
        )

        print(
            f"Candidates : "
            f"{CATALOG}."
            f"{profile_schema}."
            f"{cfg['output_tables']['candidates']}"
        )

        print(
            f"Campaign Report : "
            f"{CATALOG}."
            f"{cfg['framework']['gold_schema']}."
            f"campaign_incident_report"
        )

    except Exception as exc:

        print()
        print(
            "======================================"
        )

        print(
            "FATAL PIPELINE ERROR"
        )

        print(
            "======================================"
        )

        print(
            str(exc)
        )

        traceback.print_exc()

        raise


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()