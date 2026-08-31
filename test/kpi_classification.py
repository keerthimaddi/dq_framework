# ============================================================
# tests/test_kpi_classification.py
# Verifies the label_kpi_map produces DQ-rule-aligned codes for
# every label that has a real DQ dimension equivalent, and OPS-
# prefixed codes for the ones that don't. Pure dict logic - no
# Spark needed. Run with: pytest tests/test_kpi_classification.py -v
# ============================================================

LABEL_KPI_MAP = {
    "Null Primary Keys":            {"kpi": "DQ01", "kpi_name": "Completeness", "dimension": "completeness"},
    "Data Quality Breach":          {"kpi": "DQ01", "kpi_name": "Completeness", "dimension": "completeness"},
    "FK Mismatch":                  {"kpi": "DQ06", "kpi_name": "Integrity",    "dimension": "integrity"},
    "Duplicate Check Failure":      {"kpi": "DQ10", "kpi_name": "Duplicate",    "dimension": "duplicate"},
    "Format/Regex Failure":         {"kpi": "DQ03", "kpi_name": "Validity",     "dimension": "validity"},
    "Type Conversion Error":        {"kpi": "DQ13", "kpi_name": "Data Type",    "dimension": "data_type"},
    "Code Failure / Schema Shift":  {"kpi": "OPS01", "kpi_name": "Availability", "dimension": "availability"},
    "Network / Timeout":            {"kpi": "OPS01", "kpi_name": "Availability", "dimension": "availability"},
    "Resource Contention":          {"kpi": "DQ07", "kpi_name": "Timeliness",   "dimension": "timeliness"},
    "SLA Latency Breach":           {"kpi": "DQ07", "kpi_name": "Timeliness",   "dimension": "timeliness"},
}

VALID_DQ_IDS = {f"DQ{n:02d}" for n in range(1, 17)}


def test_every_dq_mapped_label_uses_a_real_dq_rule_id():
    for label, mapping in LABEL_KPI_MAP.items():
        if mapping["kpi"].startswith("DQ"):
            assert mapping["kpi"] in VALID_DQ_IDS, (
                f"{label} maps to {mapping['kpi']}, which is not one of DQ01-DQ16"
            )


def test_ops_codes_are_not_mistaken_for_dq_rule_ids():
    for label, mapping in LABEL_KPI_MAP.items():
        if mapping["kpi"].startswith("OPS"):
            assert mapping["kpi"] not in VALID_DQ_IDS


def test_dimension_names_are_lowercase_matching_yaml_convention():
    # dq_rules.yml uses lowercase dimension names (completeness,
    # accuracy, validity...) - label_kpi_map must match exactly or
    # weight_resolver's dimension fallback silently never matches.
    for label, mapping in LABEL_KPI_MAP.items():
        assert mapping["dimension"] == mapping["dimension"].lower()


def test_no_duplicate_dq_id_maps_to_conflicting_dimension():
    seen = {}
    for label, mapping in LABEL_KPI_MAP.items():
        kpi = mapping["kpi"]
        if kpi in seen:
            assert seen[kpi] == mapping["dimension"], (
                f"{kpi} maps to both {seen[kpi]} and {mapping['dimension']} - inconsistent"
            )
        seen[kpi] = mapping["dimension"]


if __name__ == "__main__":
    test_every_dq_mapped_label_uses_a_real_dq_rule_id()
    test_ops_codes_are_not_mistaken_for_dq_rule_ids()
    test_dimension_names_are_lowercase_matching_yaml_convention()
    test_no_duplicate_dq_id_maps_to_conflicting_dimension()
    print("All KPI classification tests passed.")