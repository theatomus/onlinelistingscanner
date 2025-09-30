import re
from typing import Any, Dict

def parse_switch(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # Brand/series (heuristic; prefer to fill when missing)
    m = re.search(r"\b(cisco|juniper|arista|brocade|hp|hpe|netgear|ubiquiti|mikrotik|dell)\b", tl)
    if m:
        result.setdefault("switch_brand", m.group(1).title())

    # Port counts
    m = re.search(r"\b(\d{2,3})\s*(ports?)\b", tl)
    if m:
        result.setdefault("switch_ports", m.group(1))

    # Speed keywords
    if any(k in tl for k in ["10/100", "10/100/1000", "gigabit", "1g", "10g", "25g", "40g", "100g"]):
        result.setdefault("switch_speed", next((k.upper() for k in ["100g","40g","25g","10g","1g","gigabit","10/100/1000","10/100"] if k in tl), None))

    # Interface (SFP/SFP+)
    if "sfp+" in tl:
        result.setdefault("switch_interface", "SFP+")
    elif "sfp" in tl:
        result.setdefault("switch_interface", "SFP")

    return result

def parse_adapter(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # Brand
    m = re.search(r"\b(intel|broadcom|mellanox|qlogic|chelsio|aquantia|marvell|realtek)\b", tl)
    if m:
        result.setdefault("adapter_brand", m.group(1).title())

    # Speed
    if any(k in tl for k in ["1g", "10g", "25g", "40g", "100g"]) or "gigabit" in tl:
        result.setdefault("adapter_speed", next((k.upper() for k in ["100g","40g","25g","10g","1g","gigabit"] if k in tl), None))

    # Ports
    m = re.search(r"\b(\d+)\s*(ports?)\b", tl)
    if m:
        result.setdefault("adapter_ports", m.group(1))

    # Form factor / slot
    if re.search(r"\b(pcie|pci-e|pci express)\b", tl):
        result.setdefault("adapter_form_factor", "PCIe")

    return result



