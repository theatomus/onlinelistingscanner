import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

def iter_lines(text: str) -> Iterable[str]:
    if not text:
        return []
    for line in text.split('\n'):
        line = line.strip()
        if line:
            yield line

def merge_if_empty(base: Dict[str, Any], updates: Dict[str, Any]) -> None:
    for k, v in updates.items():
        if v is None or v == "":
            continue
        if k not in base:
            base[k] = v

NEGATIONS = {"no", "none", "n/a", "without"}
STORAGE_TERMS = {
    "ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives",
    "harddrive", "hard", "hd", "os/ssd", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks",
    "local", "locstorage"
}
RAM_TERMS = {"ram", "memory", "dimm", "sodimm", "so-dimm", "ddr", "ddr2", "ddr3", "ddr4", "ddr5", "ecc", "reg", "registered", "rdimm", "lrdimm", "udimm", "sdram"}

def has_storage_negation(text: str) -> bool:
    t = text.lower()
    if re.search(r"\bno\s+(ssd|ssds|hdd|hdds|storage|drive|drives|hard\s*drive|hd|locstorage|ssd/os|m\.2|m2|msata|sata|sas|scsi|disk|disks)\b", t):
        return True
    # composite forms like NoSSD
    return any(f"no{term}" in t for term in STORAGE_TERMS)

def looks_like_capacity(token: str) -> bool:
    return bool(re.match(r"^\d+(?:\.\d+)?(mb|gb|tb)$", token, re.IGNORECASE))

def normalize_size(token: str) -> str:
    m = re.match(r"^(\d+(?:\.\d+)?)(mb|gb|tb)$", token.strip(), re.IGNORECASE)
    if not m:
        return token
    num, unit = m.groups()
    return f"{num}{unit.upper()}"



