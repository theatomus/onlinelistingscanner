import re
from typing import Any, Dict, List, Tuple

from .common import iter_lines, merge_if_empty, has_storage_negation, STORAGE_TERMS, looks_like_capacity, normalize_size

def _collect_storage_sizes(text: str) -> List[str]:
    sizes: List[str] = []
    for line in iter_lines(text):
        # Exclude likely RAM sizes by checking nearby RAM context tokens
        for m in re.finditer(r"\b(\d+(?:\.\d+)?(MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(normalize_size(m.group(1)))
        # slash duals: 256GB/1TB, 240GB/240GB
        for m in re.finditer(r"\b(\d+(?:\.\d+)?(MB|GB|TB))\s*/\s*(\d+(?:\.\d+)?(MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(f"{normalize_size(m.group(1))}/{normalize_size(m.group(3))}")
        # ranges for storage too
        for m in re.finditer(r"\b(\d+(?:\.\d+)?(MB|GB|TB))\s*-\s*(\d+(?:\.\d+)?(MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(f"{normalize_size(m.group(1))}-{normalize_size(m.group(3))}")
    return list(dict.fromkeys(sizes))

def parse_storage(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract storage-related fields, non-destructively.
    Canonical keys: storage_capacity, storage_capacity2, storage_type, storage_type1, storage_type2,
    storage_drive_count, storage_individual_capacity, storage_drive_size, storage_range.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text.lower()
    storage_not_included = has_storage_negation(desc_text)

    # 1) Storage types
    type_hits: List[str] = []
    for term in ["ssd", "hdd", "nvme", "emmc", "m.2", "msata", "sata", "sas", "scsi"]:
        if term in t:
            type_hits.append(term.upper() if term in ("ssd", "hdd", "nvme", "emmc") else ("M.2" if term == "m.2" else term))
    # Keep type even if negated (e.g., "No SSD" still indicates supported type)
    if type_hits:
        result.setdefault("storage_type", type_hits[0])

    # 2) Sizes with adjacency preference: SIZE followed by storage keyword (e.g., "256GB SSD")
    sizes = _collect_storage_sizes(desc_text)
    adj_matches: List[Tuple[str, str]] = []
    for m in re.finditer(r"\b(\d+(?:\.\d+)?(?:MB|GB|TB))\s*(SSD|HDD|NVME|EMMC|M\.2|MSATA|SATA|SAS|SCSI)\b", desc_text, flags=re.IGNORECASE):
        size = normalize_size(m.group(1))
        stype_raw = m.group(2).upper()
        stype = "M.2" if stype_raw in {"M.2", "M2"} else stype_raw
        adj_matches.append((size, stype))

    # Apply adjacency-based capacities first
    if adj_matches:
        # Set primary type from adjacency if not already set
        if "storage_type" not in result and adj_matches:
            result["storage_type"] = adj_matches[0][1]
        # Populate up to two capacities from adjacent matches
        result.setdefault("storage_capacity", adj_matches[0][0])
        if len(adj_matches) > 1:
            result.setdefault("storage_capacity2", adj_matches[1][0])

    # Phone/tablet context: allow typical phone storage sizes (>= 8GB)
    if "storage_capacity" not in result:
        try:
            from description_extraction.phone import parse_phone
            ph = parse_phone(desc_text)
            phone_storage = ph.get("storage_size") or ph.get("storage_size1")
            if phone_storage:
                result["storage_capacity"] = phone_storage
        except Exception:
            pass

    # 3) Drive size (form factor)
    if re.search(r"\b(2\.5in|3\.5in)\b", desc_text, re.IGNORECASE):
        result.setdefault("storage_drive_size", re.search(r"\b(2\.5in|3\.5in)\b", desc_text, re.IGNORECASE).group(1))

    # 4) Drive counts like "[2] 1TB" or "2 x 1TB"
    if not storage_not_included:
        if re.search(r"\[(\d+)\]\s*\d", desc_text):
            result.setdefault("storage_drive_count", re.search(r"\[(\d+)\]\s*\d", desc_text).group(1))
        elif re.search(r"\b(\d+)\s*x\s*\d", desc_text, re.IGNORECASE):
            result.setdefault("storage_drive_count", re.search(r"\b(\d+)\s*x\s*\d", desc_text, re.IGNORECASE).group(1))

    if logger:
        try:
            logger.debug(f"Description Storage extraction: {result}")
        except Exception:
            pass

    return result



