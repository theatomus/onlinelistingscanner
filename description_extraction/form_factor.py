import re
from typing import Any, Dict

def parse_form_factor(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # Common system form factors
    mapping = {
        "small form factor": "SFF",
        "sff": "SFF",
        "ultra small form factor": "USFF",
        "usff": "USFF",
        "mini tower": "Mini Tower",
        "tower": "Tower",
        "micro": "Micro",
        "mini": "Mini",
        "rack": "Rack",
        "desktop": "Desktop",
    }
    for k, v in mapping.items():
        if k in tl:
            result.setdefault("form_factor", v)
            break

    if logger:
        try:
            logger.debug(f"Description Form Factor extraction: {result}")
        except Exception:
            pass
    return result



