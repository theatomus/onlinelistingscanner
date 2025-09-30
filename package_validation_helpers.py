import os
import json
import re
from typing import Tuple, Dict, Any, Optional

def load_typical_box_sizes() -> Dict[str, list]:
    """Load typical box sizes from typical_box_sizes.json.
    Returns a dict mapping box name to [L, W, H] lists (in inches).
    Returns empty dict if file not found or on error."""
    box_file = "typical_box_sizes.json"
    try:
        if os.path.exists(box_file):
            with open(box_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def find_model_override_rule(title_dict: Dict[str, Any], rules: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first model-specific rule whose match_text appears in the listing title.

    title_dict – Listing title dictionary (expects keys like 'title_text_key' or 'title_key').
    rules      – The root package_validation_rules object.
    Returns the matched rule dict if found, else None.
    """
    model_check = rules.get("model_check", {})
    if not model_check.get("enabled"):
        return None

    # Use whichever title variant is present.  The log-files sometimes contain only
    # a `[Full Title:]` line which ends up mapped to `title_title_key`, so we need to
    # look at that as well when searching for model-override matches.
    title_text_full = (
        title_dict.get("title_text_key")
        or title_dict.get("title_key")
        or title_dict.get("title_title_key")
        or ""
    ).lower()

    for rule in model_check.get("rules", []):
        match_text = str(rule.get("match_text", "")).strip().lower()
        if match_text and match_text in title_text_full:
            return rule
    return None


def dimensions_match_typical(
    length: float,
    width: float,
    height: float,
    typical_boxes: Dict[str, list],
    tolerance: float = 0.5,
) -> Tuple[bool, Optional[str]]:
    """Return (True, box_name) if the (L,W,H) matches any typical box within tolerance inches."""
    pkg_sorted = sorted([length, width, height])
    for name, dims in typical_boxes.items():
        if len(dims) != 3:
            continue
        box_sorted = sorted(dims)
        if all(abs(a - b) <= tolerance for a, b in zip(pkg_sorted, box_sorted)):
            return True, name
    return False, None 