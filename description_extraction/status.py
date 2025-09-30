import re
from typing import Any, Dict

def parse_status(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # Storage status
    if re.search(r"\b(no\s+(ssd|hdd|storage|drive|drives|hard\s*drive))\b", tl):
        result.setdefault("storage_status", "No Storage")

    # OS status (complement to os.parse)
    if re.search(r"\b(no\s+os|no\s+operating\s+system)\b", tl):
        result.setdefault("os_status", "No OS")

    # Battery status
    # Detect "Bad" conditions first (def/defective/bad + battery/batt/bat variants)
    if re.search(r"\b(def(?:ective)?\s*(?:battery|batt|bat)|bad\s*(?:battery|batt|bat))\b", tl):
        result.setdefault("battery_status", "Bad")
    # Detect "AS IS" phrasing
    elif re.search(r"\b(battery\s+as\s*is|as\s*is\s*battery)\b", tl):
        result.setdefault("battery_status", "AS IS")
    # Detect explicit missing battery
    elif re.search(r"\bno\s+[^/]*/?[^/]*battery\b", tl):
        result.setdefault("battery_status", "Not Included")

    # BIOS status
    if re.search(r"\b(bios\s+locked|password\s+locked|bios\s+password)\b", tl):
        result.setdefault("bios_status", "Locked")

    # Testing status
    if re.search(r"\b(untested|for\s*parts|not\s*working|does\s*not\s*power)\b", tl):
        result.setdefault("test_status", "For Parts or Untested")
    elif re.search(r"\b(tested|working|pass(?:ed)?)\b", tl):
        result.setdefault("test_status", "Tested Working")

    if logger:
        try:
            logger.debug(f"Description Status extraction: {result}")
        except Exception:
            pass
    return result



