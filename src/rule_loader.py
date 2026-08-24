# ============================================================
# DYNAMIC RULE CONFIGURATION LOADER
# ============================================================

from pathlib import Path
from typing import Dict, Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
RULE_FILE = CONFIG_DIR / "dq_rules.yml"


def load_config() -> Dict[str, Any]:
    """
    Load the DQ framework configuration.

    The configuration is optional for table discovery.
    Discovery itself remains dynamic.
    """

    if not RULE_FILE.exists():
        raise FileNotFoundError(
            f"DQ configuration not found: {RULE_FILE}"
        )

    with open(
        RULE_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        config = yaml.safe_load(file) or {}

    return config


def get_framework_config(
    config: Dict[str, Any]
) -> Dict[str, Any]:

    return config.get(
        "framework",
        {}
    )


def get_dq_rules(
    config: Dict[str, Any]
):

    return config.get(
        "dq_rules",
        []
    )


def get_quality_gate(
    config: Dict[str, Any]
):

    return config.get(
        "quality_gate",
        {}
    )


def get_overall_thresholds(
    config: Dict[str, Any]
):

    return config.get(
        "overall_thresholds",
        {
            "pass": 90,
            "warning": 75,
            "fail": 0,
        }
    )


def get_table_rules(
    config: Dict[str, Any]
):

    return config.get(
        "table_rules",
        {}
    )


def get_table_rule(
    config: Dict[str, Any],
    full_table_name: str
) -> Dict[str, Any]:

    table_rules = get_table_rules(config)

    return table_rules.get(
        full_table_name,
        {}
    )