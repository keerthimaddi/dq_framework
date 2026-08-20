# ============================================================
# RULE LOADER
# ============================================================

from pathlib import Path
import yaml


EXPECTED_DQ_IDS = [
    f"DQ{i:02d}"
    for i in range(1, 17)
]


def find_config():

    candidates = [

        Path("dq_rules.yml"),

        Path("config/dq_rules.yml"),

        Path("../dq_rules.yml"),

        Path("../config/dq_rules.yml"),

        Path("../../dq_rules.yml"),

        Path(__file__).resolve().parents[1]
        / "dq_rules.yml",

    ]

    for path in candidates:

        if path.exists():

            return path.resolve()

    raise FileNotFoundError(
        "Could not find dq_rules.yml. "
        "Expected it in project root or config/."
    )


def load_config():

    config_path = find_config()

    print()
    print("=" * 70)
    print("LOADING DQ RULES")
    print("=" * 70)

    print(
        f"Configuration: {config_path}"
    )

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as file:

        cfg = yaml.safe_load(file)

    if not cfg:

        raise ValueError(
            "dq_rules.yml is empty."
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
                f"Missing YAML section: "
                f"{section}"
            )

    # --------------------------------------------------------
    # DQ RULES
    # --------------------------------------------------------

    rules = cfg[
        "dq_rules"
    ]

    if not isinstance(
        rules,
        list
    ):

        raise ValueError(
            "dq_rules must be a list."
        )

    if len(rules) != 16:

        raise ValueError(
            "Exactly 16 DQ rules are required. "
            f"Found {len(rules)}."
        )

    actual_ids = [
        rule.get("id")
        for rule in rules
    ]

    if actual_ids != EXPECTED_DQ_IDS:

        raise ValueError(
            "DQ rule IDs must be exactly: "
            + ", ".join(
                EXPECTED_DQ_IDS
            )
        )

    # --------------------------------------------------------
    # WEIGHTS
    # --------------------------------------------------------

    enabled_weight = 0.0

    for rule in rules:

        dq_id = rule["id"]

        enabled = rule.get(
            "enabled",
            True
        )

        weight = float(
            rule.get(
                "default_weight",
                0
            )
        )

        if weight < 0:

            raise ValueError(
                f"{dq_id} has negative weight."
            )

        if enabled:

            enabled_weight += weight

    if abs(
        enabled_weight - 100.0
    ) > 0.01:

        raise ValueError(
            "Enabled DQ weights must total 100. "
            f"Found {enabled_weight}."
        )

    # --------------------------------------------------------
    # THRESHOLDS
    # --------------------------------------------------------

    thresholds = cfg[
        "overall_thresholds"
    ]

    if "pass" not in thresholds:

        raise ValueError(
            "overall_thresholds.pass is required."
        )

    if "warning" not in thresholds:

        raise ValueError(
            "overall_thresholds.warning is required."
        )

    if float(
        thresholds["pass"]
    ) <= float(
        thresholds["warning"]
    ):

        raise ValueError(
            "PASS threshold must be greater "
            "than WARNING threshold."
        )

    # --------------------------------------------------------
    # FRAMEWORK
    # --------------------------------------------------------

    framework = cfg[
        "framework"
    ]

    required_framework = [
        "catalog",
        "audit_schema",
        "candidate_schema",
    ]

    for key in required_framework:

        if key not in framework:

            raise ValueError(
                f"framework.{key} is required."
            )

    print(
        f"Loaded {len(rules)} DQ rules"
    )

    print(
        f"Enabled weight: "
        f"{enabled_weight}"
    )

    print(
        f"Overall PASS threshold: "
        f"{thresholds['pass']}"
    )

    print(
        f"Overall WARNING threshold: "
        f"{thresholds['warning']}"
    )

    return cfg