
import uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

STATUS_ORDER = {"PASS": 0, "WARNING": 1, "FAIL": 2}

def status_from_failure_pct(pct, threshold):
    threshold = threshold or {}
    p = float(threshold.get("pass", 0))
    w = float(threshold.get("warning", 5))
    f = float(threshold.get("fail", 10))
    if pct <= p: return "PASS"
    if pct <= w: return "WARNING"
    return "FAIL"

def _table_rule(cfg, full_name):
    return cfg.get("table_rules", {}).get(full_name, {})

def _rule(cfg, dq_id):
    return next(r for r in cfg["dq_rules"] if r["id"] == dq_id)

def _safe_pct(bad, total):
    return (float(bad) / float(total) * 100.0) if total else 0.0

def _candidate_key(df, keys):
    return df.select(*[F.col(k) for k in keys if k in df.columns])

def _failed_row_ids(df, condition, keys):
    valid_keys = [k for k in keys if k in df.columns]
    if not valid_keys:
        return []
    return [r.asDict() for r in df.filter(condition).select(*valid_keys).limit(10000).collect()]

def run_dq_checks(spark, catalog, schema, table, cfg, baseline_row_count=None):
    full_name = f"{catalog}.{schema}.{table}"
    df = spark.table(f"`{catalog}`.`{schema}`.`{table}`")
    total = df.count()
    tr = _table_rule(cfg, full_name)
    results = {}
    details = []
    run_id = str(uuid.uuid4())
    run_ts = datetime.utcnow()

    def add(dq_id, bad, denom, reason, failed_rows=None):
        rule = _rule(cfg, dq_id)
        pct = _safe_pct(bad, denom)
        status = status_from_failure_pct(pct, rule.get("threshold"))
        results[dq_id] = status
        details.append({
            "run_id": run_id, "run_timestamp": run_ts,
            "catalog": catalog, "schema": schema, "table": table,
            "dq_check": dq_id, "dq_dimension": rule["dimension"],
            "level": rule["level"], "status": status,
            "failure_percentage": pct,
            "weight": float(rule.get("default_weight", 0)),
            "failed_records": int(bad), "total_records": int(denom),
            "failure_reason": reason,
            "failed_row_keys": failed_rows or []
        })

    # DQ01 Completeness - configured mandatory columns; otherwise PASS.
    mandatory = [c for c in tr.get("mandatory_columns", []) if c in df.columns]
    bad = sum(df.filter(F.col(c).isNull()).count() for c in mandatory)
    denom = total * len(mandatory)
    add("DQ01", bad, denom, "Mandatory column NULL rate", _failed_row_ids(
        df, sum([F.col(c).isNull().cast("int") for c in mandatory]) > 0, tr.get("unique_keys", [])
    ) if mandatory else [])

    # DQ02 Accuracy - configured row expressions, otherwise PASS.
    exprs = list((tr.get("row_expressions") or {}).values())
    if exprs:
        bad_condition = ~F.expr(" AND ".join(f"({e})" for e in exprs))
        bad_count = df.filter(bad_condition).count()
        add("DQ02", bad_count, total, "Configured accuracy expressions",
            _failed_row_ids(df, bad_condition, tr.get("unique_keys", [])))
    else:
        add("DQ02", 0, total, "No approved accuracy rule configured")

    # DQ03 Validity - allowed values.
    allowed = tr.get("allowed_values", {}) or {}
    invalid = 0
    invalid_cond = None
    for c, vals in allowed.items():
        if c in df.columns:
            cond = F.col(c).isNotNull() & ~F.col(c).isin(vals)
            invalid += df.filter(cond).count()
            invalid_cond = cond if invalid_cond is None else (invalid_cond | cond)
    add("DQ03", invalid, total * max(len(allowed), 1) if allowed else total,
        "Configured allowed-value rules",
        _failed_row_ids(df, invalid_cond, tr.get("unique_keys", [])) if invalid_cond is not None else [])

    # DQ04 Uniqueness.
    keys = [k for k in tr.get("unique_keys", []) if k in df.columns]
    if keys:
        dup_rows = total - df.select(*keys).dropDuplicates().count()
        add("DQ04", dup_rows, total, f"Duplicate unique-key rows for {keys}",
            _failed_row_ids(df, F.lit(True), keys) if dup_rows else [])
    else:
        add("DQ04", 0, total, "No approved unique key configured")

    # DQ05 Consistency.
    consistency = tr.get("consistency_rules", {}) or {}
    bad = 0
    bad_cond = None
    for _, expr in consistency.items():
        cond = ~F.expr(expr)
        bad += df.filter(cond).count()
        bad_cond = cond if bad_cond is None else (bad_cond | cond)
    add("DQ05", bad, total * max(len(consistency), 1) if consistency else total,
        "Configured consistency rules",
        _failed_row_ids(df, bad_cond, keys) if bad_cond is not None else [])

    # DQ06 Integrity is evaluated in run_integrity_checks.
    add("DQ06", 0, 1, "Cross-table integrity evaluated separately")

    # DQ07 Timeliness.
    date_cols = [c for c in tr.get("date_columns", []) if c in df.columns]
    if date_cols:
        bad = sum(df.filter(F.col(c) > F.current_timestamp()).count() for c in date_cols)
        add("DQ07", bad, total * len(date_cols), "Future date/timestamp values")
    else:
        add("DQ07", 0, total, "No approved date column configured")

    # DQ08 Conformity.
    conformity = tr.get("conformity_rules", {}) or {}
    bad = 0
    for c, expr in conformity.items():
        if c in df.columns:
            bad += df.filter(~F.expr(expr)).count()
    add("DQ08", bad, total * max(len(conformity), 1) if conformity else total,
        "Configured conformity expressions")

    # DQ09 Range.
    ranges = tr.get("range_rules", {}) or {}
    bad = 0
    bad_cond = None
    for c, spec in ranges.items():
        if c not in df.columns: continue
        lo, hi = spec.get("min"), spec.get("max")
        cond = F.lit(False)
        if lo is not None: cond = cond | (F.col(c) < F.lit(lo))
        if hi is not None: cond = cond | (F.col(c) > F.lit(hi))
        bad += df.filter(cond).count()
        bad_cond = cond if bad_cond is None else (bad_cond | cond)
    add("DQ09", bad, total * max(len(ranges), 1) if ranges else total,
        "Configured range rules",
        _failed_row_ids(df, bad_cond, keys) if bad_cond is not None else [])

    # DQ10 Complete-row duplicates.
    duplicate_rows = total - df.dropDuplicates().count()
    add("DQ10", duplicate_rows, total, "Duplicate complete records")

    # DQ11 Null - same concept but explicitly configurable.
    null_columns = [c for c in tr.get("null_columns", mandatory) if c in df.columns]
    bad = sum(df.filter(F.col(c).isNull()).count() for c in null_columns)
    add("DQ11", bad, total * len(null_columns) if null_columns else total,
        "Configured unexpected NULL columns")

    # DQ12 Length.
    lengths = tr.get("length_rules", {}) or {}
    bad = 0
    for c, spec in lengths.items():
        if c in df.columns:
            max_len = spec.get("max")
            min_len = spec.get("min")
            cond = F.lit(False)
            if max_len is not None: cond = cond | (F.length(F.col(c)) > F.lit(max_len))
            if min_len is not None: cond = cond | (F.length(F.col(c)) < F.lit(min_len))
            bad += df.filter(F.col(c).isNotNull() & cond).count()
    add("DQ12", bad, total * max(len(lengths), 1) if lengths else total,
        "Configured string-length rules")

    # DQ13 Data type.
    expected = tr.get("expected_types", {}) or {}
    bad = 0
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    for c, expected_type in expected.items():
        if c in actual and actual[c].lower() != str(expected_type).lower():
            bad += 1
    add("DQ13", bad, max(len(expected), 1), "Configured expected Spark data types")

    # DQ14 Pattern.
    patterns = tr.get("pattern_rules", {}) or {}
    bad = 0
    bad_cond = None
    for c, pattern in patterns.items():
        if c in df.columns:
            cond = F.col(c).isNotNull() & ~F.col(c).rlike(pattern)
            bad += df.filter(cond).count()
            bad_cond = cond if bad_cond is None else (bad_cond | cond)
    add("DQ14", bad, total * max(len(patterns), 1) if patterns else total,
        "Configured regular-expression rules",
        _failed_row_ids(df, bad_cond, keys) if bad_cond is not None else [])

    # DQ15 Business rules.
    business = tr.get("business_rules", {}) or {}
    bad = 0
    bad_cond = None
    for _, expr in business.items():
        cond = ~F.expr(expr)
        bad += df.filter(cond).count()
        bad_cond = cond if bad_cond is None else (bad_cond | cond)
    add("DQ15", bad, total * max(len(business), 1) if business else total,
        "Configured business rules",
        _failed_row_ids(df, bad_cond, keys) if bad_cond is not None else [])

    # DQ16 Volume.
    if baseline_row_count is None:
        add("DQ16", 0, 1, "No historical baseline; non-empty dataset accepted")
    else:
        variance = abs(total - baseline_row_count) / baseline_row_count * 100 if baseline_row_count else 100
        add("DQ16", variance, 100, "Row-count variance percentage")

    return {
        "run_id": run_id,
        "run_timestamp": run_ts,
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "dq_results": results,
        "details": details,
        "row_count": total
    }

def run_integrity_checks(spark, cfg, current_result):
    full_name = f"{current_result['catalog']}.{current_result['schema']}.{current_result['table']}"
    relevant = [r for r in cfg.get("relationships", []) if r.get("child_table") == full_name]
    if not relevant:
        return "PASS", 0, 0, "No cross-table relationship configured", []

    df = spark.table(f"`{current_result['catalog']}`.`{current_result['schema']}`.`{current_result['table']}`")
    total_bad = 0
    total = 0
    reasons = []
    row_keys = []

    for rel in relevant:
        child = rel["child_column"]
        parent_table = rel["parent_table"]
        parent_col = rel["parent_column"]
        child_df = df
        if child not in child_df.columns:
            continue
        parent = spark.table("`" + "`.`".join(parent_table.split(".")) + "`").select(parent_col).where(F.col(parent_col).isNotNull()).distinct()
        if rel.get("nullable_allowed", False):
            child_df = child_df.where(F.col(child).isNotNull())
        total += child_df.count()
        bad_df = child_df.join(parent, child_df[child] == parent[parent_col], "left_anti")
        bad = bad_df.count()
        total_bad += bad
        reasons.append(f"{child} -> {parent_table}.{parent_col}: {bad} orphan rows")
        keys = _table_rule(cfg, full_name).get("unique_keys", [])
        if keys:
            row_keys.extend([r.asDict() for r in bad_df.select(*[k for k in keys if k in bad_df.columns]).limit(10000).collect()])

    rule = _rule(cfg, "DQ06")
    pct = _safe_pct(total_bad, total)
    status = status_from_failure_pct(pct, rule.get("threshold"))
    return status, total_bad, total, "; ".join(reasons), row_keys

def calculate_overall_score(cfg, dq_results):
    enabled = [r for r in cfg["dq_rules"] if r.get("enabled")]
    total_weight = sum(float(r.get("default_weight", 0)) for r in enabled)
    earned = 0.0
    factors = cfg.get("scoring", {})
    for r in enabled:
        s = dq_results.get(r["id"], "FAIL")
        factor = factors.get(f"{s.lower()}_factor", 0)
        earned += float(r.get("default_weight", 0)) * float(factor)
    score = round(earned / total_weight * 100, 2) if total_weight else 0.0
    th = cfg["overall_thresholds"]
    status = "PASS" if score >= float(th["pass"]) else ("WARNING" if score >= float(th["warning"]) else "FAIL")
    return score, status
