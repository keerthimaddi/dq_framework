# ============================================================
# tests/test_weight_resolver.py
# Pure-logic tests for resolve_weight() - no Spark session needed
# for this function since it takes plain dicts, not DataFrames.
# Run with: pytest tests/test_weight_resolver.py -v
# ============================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.weight_resolver import resolve_weight


def test_uses_dynamic_weight_by_rule_id_when_available():
    by_rule_id = {"DQ01": 15.5}
    by_dimension = {}
    result = resolve_weight("DQ01", "completeness", 10, by_rule_id, by_dimension)
    assert result == 15.5


def test_falls_back_to_dimension_match_when_no_rule_id_match():
    by_rule_id = {}
    by_dimension = {"completeness": 22.0}
    result = resolve_weight("DQ01", "completeness", 10, by_rule_id, by_dimension)
    assert result == 22.0


def test_falls_back_to_yaml_default_when_no_dynamic_weight_at_all():
    result = resolve_weight("DQ01", "completeness", 10, {}, {})
    assert result == 10.0


def test_yaml_fallback_is_cold_start_safe():
    # Simulates first-run: no dq_kpis table exists yet, so both
    # dynamic dicts are empty. Every rule must still resolve.
    rules = [
        {"id": "DQ01", "dimension": "completeness", "default_weight": 10},
        {"id": "DQ04", "dimension": "uniqueness", "default_weight": 8},
        {"id": "DQ16", "dimension": "volume", "default_weight": 1},
    ]
    for rule in rules:
        w = resolve_weight(rule["id"], rule["dimension"], rule["default_weight"], {}, {})
        assert w == float(rule["default_weight"])


def test_rule_id_match_takes_priority_over_dimension_match():
    by_rule_id = {"DQ01": 99.0}
    by_dimension = {"completeness": 1.0}
    result = resolve_weight("DQ01", "completeness", 10, by_rule_id, by_dimension)
    assert result == 99.0


def test_never_returns_none():
    assert resolve_weight("DQ99", None, 5, {}, {}) is not None
    assert resolve_weight("DQ99", None, 5, {}, {}) == 5.0


if __name__ == "__main__":
    test_uses_dynamic_weight_by_rule_id_when_available()
    test_falls_back_to_dimension_match_when_no_rule_id_match()
    test_falls_back_to_yaml_default_when_no_dynamic_weight_at_all()
    test_yaml_fallback_is_cold_start_safe()
    test_rule_id_match_takes_priority_over_dimension_match()
    test_never_returns_none()
    print("All weight_resolver tests passed.")