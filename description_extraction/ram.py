import re
from typing import Any, Dict, List, Tuple

from .common import iter_lines, merge_if_empty, has_storage_negation, RAM_TERMS, STORAGE_TERMS, looks_like_capacity, normalize_size

def _collect_sizes(text: str) -> List[str]:
    sizes: List[str] = []
    for line in iter_lines(text):
        for tok in re.findall(r"\b\d+(?:\.\d+)?(MB|GB|TB)\b", line, flags=re.IGNORECASE):
            pass
        for m in re.finditer(r"\b(\d+(?:\.\d+)?(MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(normalize_size(m.group(1)))
        for m in re.finditer(r"\b(\d+)x(\d+(?:\.\d+)?(MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(f"{m.group(1)}x{normalize_size(m.group(2))}")
        for m in re.finditer(r"\((\d+)\s*x\s*(\d+(?:\.\d+)?(MB|GB|TB))\)", line, flags=re.IGNORECASE):
            sizes.append(f"{m.group(1)}x{normalize_size(m.group(2))}")
        # ranges like 16GB-32GB or 4-16GB
        for m in re.finditer(r"\b(\d+(?:\.\d+)?(?:MB|GB|TB))\s*-\s*(\d+(?:\.\d+)?(?:MB|GB|TB))\b", line, flags=re.IGNORECASE):
            sizes.append(f"{normalize_size(m.group(1))}-{normalize_size(m.group(2))}")
        for m in re.finditer(r"\b(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)(MB|GB|TB)\b", line, flags=re.IGNORECASE):
            first = f"{m.group(1)}{m.group(3)}"
            second = f"{m.group(2)}{m.group(3)}"
            sizes.append(f"{normalize_size(first)}-{normalize_size(second)}")
    return list(dict.fromkeys(sizes))

def _has_ram_context(window: str) -> bool:
    t = window.lower()
    # Word-boundary RAM context to avoid matching 'gddr'
    return bool(re.search(r"\b(?:ram|memory|dimm|so-?dimm|lpddr[0-5]x?|ddr[0-5])\b", t))

def _near_storage_terms(window: str) -> bool:
    t = window.lower()
    return any(term in t for term in STORAGE_TERMS)

def _has_gpu_context(window: str) -> bool:
    t = window.lower()
    return any(k in t for k in ["rtx", "gtx", "quadro", "radeon", "gddr", "hbm", "geforce", "tesla"]) 

def parse_ram(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract RAM-related fields from description text, non-destructively.
    Returns canonical keys: ram_size, ram_config, ram_type, ram_speed_grade, ram_modules, ram_rank, ram_brand,
    ram_ecc, ram_registered, ram_unbuffered, ram_details, ram_range.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    storage_not_included = has_storage_negation(desc_text)

    # 1) RAM types and flags
    # RAM types (exclude GPU VRAM types like GDDR/HBM)
    if re.search(r"\b(ddr|lpddr)[0-5]x?\b", desc_text, re.IGNORECASE):
        m = re.search(r"\b(ddr|lpddr)[0-5]x?\b", desc_text, re.IGNORECASE)
        result.setdefault("ram_type", m.group(0))
    # Form factor recognition: SoDIMM
    if re.search(r"\bsodimm\b", desc_text, re.IGNORECASE):
        # Prefer form factor token when present to align with title expectations
        result["ram_type"] = "Sodimm"
    if re.search(r"\becc\b", desc_text, re.IGNORECASE):
        result.setdefault("ram_ecc", "ecc")
    if re.search(r"\b(reg|registered|rdimm|lrdimm)\b", desc_text, re.IGNORECASE):
        result.setdefault("ram_registered", "registered")
    if re.search(r"\b(unbuffered|udimm)\b", desc_text, re.IGNORECASE):
        result.setdefault("ram_unbuffered", "unbuffered")

    # 2) Speed grades
    mhz = re.search(r"\b(\d+(?:\.\d+)?)\s*mhz\b", desc_text, re.IGNORECASE)
    if mhz:
        # Only accept MHz if RAM context is present to avoid CPU 1.70 GHz -> 1700 MHz confusion
        if _has_ram_context(desc_text) and not _near_storage_terms(desc_text):
            # Keep integer MHz if possible
            val = mhz.group(1)
            if val.endswith('.0'):
                val = val[:-2]
            result.setdefault("ram_speed_grade", f"{val}MHz")
    pc_grade = re.search(r"\bPC-?\d{2,4}[A-Z]?\b", desc_text, re.IGNORECASE)
    if pc_grade:
        result.setdefault("ram_speed_grade", pc_grade.group(0))

    # 3) Sizes and configs
    sizes = _collect_sizes(desc_text)
    configs = [s for s in sizes if re.match(r"^\d+x\d+(MB|GB|TB)$", s, re.IGNORECASE)]
    ranges = [s for s in sizes if re.match(r"^\d+(?:\.\d+)?(MB|GB|TB)\-\d+(?:\.\d+)?(MB|GB|TB)$", s, re.IGNORECASE)]
    singles = [s for s in sizes if re.match(r"^\d+(?:\.\d+)?(MB|GB|TB)$", s, re.IGNORECASE)]

    if configs:
        result.setdefault("ram_config", ", ".join(configs))
        # try to pick a plausible total size if a single standalone size appears
        if singles and "ram_size" not in result:
            result["ram_size"] = singles[0]

    if not configs and singles:
        # Bias rules: if storage is explicitly excluded, we can assign reasonable RAM sizes
        # Else, require RAM context nearby in the text
        if storage_not_included and not _has_gpu_context(desc_text):
            result.setdefault("ram_size", singles[0])
        elif _has_ram_context(desc_text) and not _near_storage_terms(desc_text) and not _has_gpu_context(desc_text):
            result.setdefault("ram_size", singles[0])

    if ranges and _has_ram_context(desc_text) and not _near_storage_terms(desc_text):
        result.setdefault("ram_range", ranges[0])

    # 4) Modules or ranks
    m = re.search(r"\(\s*\d+\s*x\s*\d+\s*(MB|GB|TB)\s*\)", desc_text, re.IGNORECASE)
    if m:
        # Already captured as config; expose a generic modules flag
        result.setdefault("ram_modules", "yes")
    if re.search(r"\d+Rx\d+", desc_text, re.IGNORECASE):
        result.setdefault("ram_rank", re.search(r"\d+Rx\d+", desc_text, re.IGNORECASE).group(0))

    # 5) Brand hints
    if re.search(r"\b(samsung|kingston|hynix|micron|crucial|corsair|g\.skill)\b", desc_text, re.IGNORECASE):
        result.setdefault("ram_brand", re.search(r"\b(samsung|kingston|hynix|micron|crucial|corsair|g\.skill)\b", desc_text, re.IGNORECASE).group(1))

    if logger:
        try:
            logger.debug(f"Description RAM extraction: {result}")
        except Exception:
            pass

    return result



