import re
from typing import Any, Dict, List

PHONE_COLORS = [
    "Black", "White", "Silver", "Gold", "Blue", "Red", "Green", "Purple", "Yellow",
    "Space Gray", "Space Grey", "Graphite", "Midnight Black", "Midnight", "Starlight", "Deep Purple",
    "Product Red", "Rose Gold", "Alpine Green", "Coral", "Titanium", "Gray", "Grey"
]

def parse_phone(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # Apple A#### model numbers
    models: List[str] = []
    for m in re.finditer(r"\bA\d{4}\b", t):
        models.append(m.group(0))
    if models:
        if len(models) == 1:
            result.setdefault("phone_model", models[0])
        else:
            for i, mval in enumerate(models, 1):
                result.setdefault(f"phone_model{i}", mval)

    # colors
    found_colors: List[str] = []
    for color in PHONE_COLORS:
        if color.lower() in tl:
            found_colors.append(color)
    if found_colors:
        if len(found_colors) == 1:
            result.setdefault("color", found_colors[0])
        else:
            for i, c in enumerate(found_colors, 1):
                result.setdefault(f"color{i}", c)

    # iPhone models by name (e.g., iPhone 7, iPhone XR, iPhone 12 Pro Max, iPhone SE 2nd Gen)
    iphone_pat = re.compile(
        r"\biPhone\s+(SE\s*(?:2nd|3rd)\s*Gen|XR|XS\s*Max|XS|X|\d+\s*(?:Pro\s*Max|Pro|Plus|Mini)?)\b",
        re.IGNORECASE,
    )
    m = iphone_pat.search(desc_text)
    if m:
        model_name = f"iPhone {m.group(1)}".replace("  ", " ").strip()
        result.setdefault("phone_model_name", model_name)

    # iPad models by name
    ipad_pat = re.compile(r"\biPad\s+(Air|Mini|Pro)\b", re.IGNORECASE)
    m = ipad_pat.search(desc_text)
    if m:
        model_name = f"iPad {m.group(1)}".strip()
        result.setdefault("phone_model_name", model_name)

    # storage sizes in phone context (>= 8GB reasonable)
    caps: List[str] = []
    for m in re.finditer(r"\b(\d+(?:\.\d+)?)\s*(GB|TB)\b", t, re.IGNORECASE):
        num = float(m.group(1))
        unit = m.group(2).upper()
        if unit == "TB" or (unit == "GB" and num >= 8):
            caps.append(f"{int(num) if num.is_integer() else num}{unit}")
    if caps:
        if len(caps) == 1:
            result.setdefault("storage_size", caps[0])
        else:
            for i, c in enumerate(caps, 1):
                result.setdefault(f"storage_size{i}", c)

    # network status basic
    if re.search(r"\b(wifi|wi[- ]?fi\s*only)\b", tl):
        result.setdefault("network_status", "WiFi Only")
    if re.search(r"\b(network\s+unlocked|net\s+unlocked|carrier\s+unlocked)\b", tl):
        result.setdefault("network_status", "Network Unlocked")
    elif re.search(r"\bunlocked\b", tl):
        result.setdefault("network_status", "Network Unlocked")
    # Carriers
    carriers = ["Verizon", "AT&T", "T-Mobile", "Sprint", "Cricket", "Metro", "Boost", "Xfinity", "US Cellular"]
    for carr in carriers:
        if carr.lower() in tl:
            result.setdefault("network_carrier", carr)

    if logger:
        try:
            logger.debug(f"Description Phone extraction: {result}")
        except Exception:
            pass
    return result



