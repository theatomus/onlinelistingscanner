import re
from typing import Any, Dict

def parse_battery(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # Presence
    if re.search(r"\b(no\s+battery|battery\s+not\s+included)\b", tl):
        result.setdefault("battery_presence", "Not Included")
    elif "battery" in tl:
        result.setdefault("battery_presence", "Included")

    # Condition
    if re.search(r"\b(as\s*is|bad|weak|degraded)\b", tl):
        result.setdefault("battery_condition", "AS IS")
    elif re.search(r"\b(good|healthy|passes)\b", tl):
        result.setdefault("battery_condition", "Good")

    # Health percentage
    m = re.search(r"\b(\d{2,3})\s*%\b", tl)
    if m and 0 <= int(m.group(1)) <= 100:
        result.setdefault("battery_health", m.group(1) + "%")

    if logger:
        try:
            logger.debug(f"Description Battery extraction: {result}")
        except Exception:
            pass
    return result



