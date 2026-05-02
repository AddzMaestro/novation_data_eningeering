"""
DQ rules loader. Reads config/dq_rules.yaml once and exposes typed accessors
for the six issue categories. The pipeline uses these accessors to:
  - drive the dq_flag priority ordering
  - emit handling_action values into dq_report.json (cross-referenced by scorer)
  - locate quarantine paths

A rule is intentionally consumed read-only — the file is a contract with
the evaluation system, not pipeline state.
"""

import os
import yaml


_DEFAULT_RULES_PATH = "/data/config/dq_rules.yaml"

# Local-dev fallback paths searched if the container path is absent.
_FALLBACK_PATHS = [
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "dq_rules.yaml"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "dq_rules.yaml"),
]


def load_dq_rules(path=None):
    """Load dq_rules.yaml. Path overridable via DQ_RULES_PATH env var."""
    if path is None:
        path = os.environ.get("DQ_RULES_PATH", _DEFAULT_RULES_PATH)

    if not os.path.exists(path):
        for fallback in _FALLBACK_PATHS:
            if os.path.exists(fallback):
                path = fallback
                break
        else:
            raise FileNotFoundError(f"dq_rules.yaml not found at {path} or fallbacks {_FALLBACK_PATHS}")

    with open(path) as f:
        return yaml.safe_load(f)


def issue_keys():
    """Canonical issue_type strings as they must appear in dq_report.json."""
    return [
        "duplicate_transactions",
        "orphaned_transactions",
        "amount_type_mismatch",
        "date_format_inconsistency",
        "currency_variants",
        "null_account_id",
    ]


def handling_action(rules, issue_type):
    """Return the handling.action string for a given issue_type."""
    return rules["issues"][issue_type]["handling"]["action"]


def dq_flag_priority(rules):
    """List of dq_flag values in priority order for fact_transactions."""
    return list(rules.get("dq_flag_priority", ["TYPE_MISMATCH", "DATE_FORMAT", "CURRENCY_VARIANT"]))


def quarantine_path(rules, issue_type):
    """Relative quarantine path (under output root) for issues whose handling
    action is QUARANTINED or EXCLUDED_NULL_PK. Returns None if not applicable."""
    return rules["issues"][issue_type]["handling"].get("quarantine_path")
