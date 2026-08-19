# ============================================================
# RULE LOADER
# ============================================================

from pathlib import Path
import yaml


def find_config():

    candidates = [
        Path("dq_rules.yml"),
        Path("config/dq_rules.yml"),
        Path("../dq_rules.yml"),
        Path("../config/dq_rules.yml"),
        Path("../../dq_rules.yml"),
        Path(__file__).resolve().parents[1] / "dq_rules.yml",
    ]

    for path in candidates:
        if path.exists():
            return path.resolve()

    raise FileNotFoundError(
        "Could not find dq_rules.yml"
    )


def load_config():

    config_path = find_config()

    print()
    print("=" * 60)
    print("LOADING DQ RULES")
    print("=" * 60)

    print(f"Configuration: {config_path}")

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as file:

        cfg = yaml.safe_load(file)

    if not cfg:
        raise ValueError(
            "dq_rules.yml is empty"
        )

    required_sections = [
        "framework",
        "overall_thresholds",
        "quality_gate",
        "dq_rules",
        "table_rules",
        "relationships",
        "profiling",
        "ml_weighting",
        "output_tables",
    ]

    for section in required_sections:

        if section not in cfg:

            raise ValueError(
                f"Missing YAML section: {section}"
            )

    rules = cfg["dq_rules"]

    if len(rules) != 16:

        raise ValueError(
            f"Exactly 16 DQ rules are required. "
            f"Found {len(rules)}"
        )

    expected_ids = [
        f"DQ{i:02d}"
        for i in range(1, 17)
    ]

    actual_ids = [
        rule.get("id")
        for rule in rules
    ]

    if actual_ids != expected_ids:

        raise ValueError(
            "DQ rule IDs must be exactly: "
            + ", ".join(expected_ids)
        )

    enabled_weight = sum(
        float(rule.get("default_weight", 0))
        for rule in rules
        if rule.get("enabled", True)
    )

    if abs(enabled_weight - 100.0) > 0.01:

        raise ValueError(
            f"Enabled DQ weights must total 100. "
            f"Found {enabled_weight}"
        )

    print(
        f"Loaded {len(rules)} DQ rules"
    )

    print(
        f"Enabled weight: {enabled_weight}"
    )

    return cfg