import re
from typing import Any, Dict

def parse_hdd(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract HDD/drive-specific fields from description text.
    Canonical keys: hdd_interface, hdd_form_factor, hdd_rpm, hdd_transfer_rate,
    hdd_model_number, hdd_part_number, hdd_usage_hours.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # Interface
    if re.search(r"\b(sata|sas|scsi|ide|pata|nvme)\b", tl):
        result.setdefault("hdd_interface", re.search(r"\b(sata|sas|scsi|ide|pata|nvme)\b", tl).group(1).upper())

    # Form factor
    if re.search(r"\b(2\.5in|3\.5in)\b", t, re.IGNORECASE):
        result.setdefault("hdd_form_factor", re.search(r"\b(2\.5in|3\.5in)\b", t, re.IGNORECASE).group(1))

    # RPM
    if re.search(r"\b(\d{3,5})\s*RPM\b", t, re.IGNORECASE):
        result.setdefault("hdd_rpm", re.search(r"\b(\d{3,5})\s*RPM\b", t, re.IGNORECASE).group(1))

    # Transfer rate
    if re.search(r"\b(\d+(?:\.\d+)?)\s*(Gbps|MB/s)\b", t, re.IGNORECASE):
        m = re.search(r"\b(\d+(?:\.\d+)?)\s*(Gbps|MB/s)\b", t, re.IGNORECASE)
        result.setdefault("hdd_transfer_rate", f"{m.group(1)} {m.group(2)}")

    # Model / Part numbers (heuristic)
    # Common HDD model styles: ST1000DM010, WD10EZEX, HUS726T6TALE6L4, etc.
    m = re.search(r"\b([A-Z]{2,4}[0-9A-Z]{4,})\b", t)
    if m:
        result.setdefault("hdd_model_number", m.group(1))
    # Part numbers with labels
    m = re.search(r"\b(PN|P/N|Part\s*Number)[:\s]+([A-Z0-9\-]+)\b", t, re.IGNORECASE)
    if m:
        result.setdefault("hdd_part_number", m.group(2).upper())

    # Usage hours
    m = re.search(r"\b(\d{2,6})\s*(power[- ]on\s*)?hours?\b", tl)
    if m:
        result.setdefault("hdd_usage_hours", m.group(1))

    if logger:
        try:
            logger.debug(f"Description HDD extraction: {result}")
        except Exception:
            pass
    return result



