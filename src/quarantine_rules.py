# ============================================================
# QUARANTINE RULES
# Builds a single row-level boolean condition from table_rules
# so lakehouse.create_quarantine() knows which rows to isolate.
# ============================================================

from pyspark.sql import functions as F

from src.dq_engine import resolve_column


def build_failure_condition(df, table_rule):
    """
    Returns a Spark Column that is True for any row that fails
    at least one configured row/column-level rule for this table.
    Returns None if no relevant rules are configured (nothing to
    quarantine on - matches lakehouse.create_quarantine's contract).
    """

    condition = None

    def combine(new_cond):
        nonlocal condition
        condition = new_cond if condition is None else (condition | new_cond)

    # ---- Mandatory / null columns -> completeness + null failures
    null_check_columns = set(
        table_rule.get("mandatory_columns", [])
        + table_rule.get("null_columns", [])
    )

    for col_name in null_check_columns:
        resolved = resolve_column(df, col_name)
        if resolved:
            combine(F.col(resolved).isNull())

    # ---- Range rules -> range / accuracy failures
    for col_name, bounds in table_rule.get("range_rules", {}).items():
        resolved = resolve_column(df, col_name)
        if not resolved:
            continue
        c = F.col(resolved)
        if "min" in bounds:
            combine(c < F.lit(bounds["min"]))
        if "max" in bounds:
            combine(c > F.lit(bounds["max"]))

    # ---- Length rules
    for col_name, bounds in table_rule.get("length_rules", {}).items():
        resolved = resolve_column(df, col_name)
        if not resolved:
            continue
        length_col = F.length(F.col(resolved).cast("string"))
        if "min" in bounds:
            combine(length_col < F.lit(bounds["min"]))
        if "max" in bounds:
            combine(length_col > F.lit(bounds["max"]))

    # ---- Pattern rules (regex)
    for col_name, pattern in table_rule.get("pattern_rules", {}).items():
        resolved = resolve_column(df, col_name)
        if not resolved:
            continue
        c = F.col(resolved).cast("string")
        combine(c.isNotNull() & (~c.rlike(pattern)))

    # ---- Allowed values (conformity)
    for col_name, values in table_rule.get("allowed_values", {}).items():
        if not values:
            continue
        resolved = resolve_column(df, col_name)
        if not resolved:
            continue
        c = F.col(resolved)
        combine(c.isNotNull() & (~c.isin(values)))

    # ---- Row expressions + business rules
    # A row that does NOT satisfy the expression is a failure.
    combined_expressions = {
        **table_rule.get("row_expressions", {}),
        **table_rule.get("business_rules", {}),
        **table_rule.get("consistency_rules", {}),
    }

    for rule_name, expr in combined_expressions.items():
        try:
            combine(~F.expr(expr))
        except Exception as exc:
            print(
                f"WARNING: could not evaluate row expression "
                f"'{rule_name}' ({expr}): {exc}"
            )

    return condition