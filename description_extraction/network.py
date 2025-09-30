import re
from typing import Any, Dict, List

try:
    from configs.extractor_phone import network_carriers as PHONE_NETWORK_CARRIERS
except Exception:
    PHONE_NETWORK_CARRIERS = [
        "Verizon", "AT&T", "T-Mobile", "US Cellular", "Cricket", "Metro",
        "Boost Mobile", "Mint Mobile", "Google Fi", "Xfinity Mobile", "Spectrum Mobile",
        "Straight Talk", "Total by Verizon", "Simple Mobile"
    ]

SYNONYMS = {
    "vzw": "Verizon",
    "att": "AT&T",
    "tmobile": "T-Mobile",
}

def parse_network(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract network status and carriers from description text.
    Returns canonical keys: network_status, network_statusN, network_carrier, network_carrierN.
    Non-destructive; caller will merge only missing keys.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text.lower()

    # Status detection (collect multiple)
    statuses: List[str] = []
    if re.search(r"\b(wifi|wi-fi)\b", t):
        statuses.append("WiFi Only")
    unlocked = bool(re.search(r"\b(network\s+unlocked|net\s+unlocked|carrier\s+unlocked|unlocked)\b", t))
    locked = bool(re.search(r"\blocked\b", t))
    if unlocked:
        statuses.append("Network Unlocked")
    elif locked:
        statuses.append("Locked")
    if statuses:
        result.setdefault("network_status", statuses[0])
        for idx, s in enumerate(statuses[1:], start=2):
            result.setdefault(f"network_status{idx}", s)

    # Carrier detection (word/token boundaries only)
    carriers_found: List[str] = []
    # AT&T: single token only (no spaced variants)
    if re.search(r"(?<!\w)at&t(?!\w)", t):
        carriers_found.append("AT&T")
    # Other carriers exact-word
    for carrier in PHONE_NETWORK_CARRIERS:
        cl = carrier.lower()
        # special-case at&t handled above
        if cl == "at&t":
            continue
        if re.search(rf"\b{re.escape(cl)}\b", t):
            carriers_found.append(carrier)
    # Synonyms strictly as tokens (avoid matching inside 'battery')
    for syn, canon in SYNONYMS.items():
        if syn == "att":
            continue
        if re.search(rf"\b{re.escape(syn)}\b", t) and canon not in carriers_found:
            carriers_found.append(canon)

    if carriers_found:
        # single preferred
        result.setdefault("network_carrier", carriers_found[0])
        # also enumerate if multiple
        for idx, c in enumerate(carriers_found, 1):
            result.setdefault(f"network_carrier{idx}", c)

    if logger:
        try:
            logger.debug(f"Description Network extraction: {result}")
        except Exception:
            pass

    return result



