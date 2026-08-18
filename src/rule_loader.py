
from pathlib import Path
import yaml

REQUIRED_DIMENSIONS = [
    "completeness", "accuracy", "validity", "uniqueness",
    "consistency", "integrity", "timeliness", "conformity",
    "range", "duplicate", "null", "length", "data_type",
    "pattern", "business_rule", "volume"
]

def _find_config():
    candidates = [
        Path("config/dq_rules.yml"),
        Path(__file__).resolve().parents[1] / "config" / "dq_rules.yml",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("config/dq_rules.yml was not found")

def load_config():
    with open(_find_config(), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    rules = cfg.get("dq_rules", [])
    dimensions = [r.get("dimension") for r in rules]

    missing = [d for d in REQUIRED_DIMENSIONS if d not in dimensions]
    if missing:
        raise ValueError(f"Missing DQ dimensions: {missing}")

    ids = [r.get("id") for r in rules]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate DQ rule IDs found")

    if len(rules) != 16:
        raise ValueError(f"Exactly 16 DQ rules are required; found {len(rules)}")

    total_weight = sum(
        float(r.get("default_weight", 0))
        for r in rules
        if r.get("enabled", False)
    )
    if total_weight <= 0:
        raise ValueError("Enabled DQ rule weight must be greater than zero")

    return cfg

def load_dq_rules():
    cfg = load_config()
    return cfg["dq_rules"], cfg["overall_thresholds"]

def get_table_rule(cfg, full_name):
    return cfg.get("table_rules", {}).get(full_name, {})

def get_rule(cfg, dq_id):
    for r in cfg["dq_rules"]:
        if r["id"] == dq_id:
            return r
    return None
