import re
from typing import Any, Dict

def parse_lot(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract lot/quantity information from description text.
    Canonical key: lot
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # Patterns: "Lot of 9", "Lot: 9", "Qty: 9", "Quantity: 9", "9x <item>"
    m = re.search(r"\bLot\s*(?:of|:)\s*(\d+)\b", t, re.IGNORECASE)
    if m:
        result.setdefault("lot", m.group(1))
        return result

    m = re.search(r"\b(Qty|Quantity)\s*[:\-]?\s*(\d+)\b", t, re.IGNORECASE)
    if m:
        result.setdefault("lot", m.group(2))
        return result

    m = re.search(r"\b(\d+)\s*x\b", t)
    if m:
        result.setdefault("lot", m.group(1))

    if logger:
        try:
            logger.debug(f"Description Lot extraction: {result}")
        except Exception:
            pass
    return result



