import re
from typing import Any, Dict

GPU_BRANDS = ["NVIDIA", "AMD", "Intel"]
GPU_INDICATORS = [
    "geforce", "rtx", "gtx", "gt", "quadro", "tesla", "radeon", "rx", "firepro", "arc", "iris", "uhd", "hd"
]

def _is_vram_context(window: str) -> bool:
    t = window.lower()
    return any(ind in t for ind in GPU_INDICATORS) or "vram" in t

def parse_graphics(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract GPU fields from description text.
    Canonical keys: gpu, gpu_memory_type, gpu_type, gpu_spec.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # Model/spec (basic heuristics)
    # NVIDIA/AMD names like RTX 3080, GTX 1060, RX 580
    m = re.search(r"\b((rtx|gtx)\s*\d{3,4}[a-z]*)\b", tl)
    if m:
        result.setdefault("gpu", m.group(1).upper().replace(" ", ""))
    m = re.search(r"\b(rx\s*\d{3,4}[a-z]*)\b", tl)
    if m and "gpu" not in result:
        result.setdefault("gpu", m.group(1).upper().replace(" ", ""))
    # Quadro/Tesla/FirePro
    m = re.search(r"\b(quadro\s*[a-z0-9]+)\b", tl)
    if m and "gpu" not in result:
        result.setdefault("gpu", m.group(1).title())
    m = re.search(r"\b(tesla\s*[a-z0-9]+)\b", tl)
    if m and "gpu" not in result:
        result.setdefault("gpu", m.group(1).title())
    m = re.search(r"\b(firepro\s*[a-z0-9]+)\b", tl)
    if m and "gpu" not in result:
        result.setdefault("gpu", m.group(1).title())

    # GPU memory type
    m = re.search(r"\b(gddr[56]|gddr|hbm2?)\b", tl)
    if m:
        result.setdefault("gpu_memory_type", m.group(1).upper())

    # VRAM sizes (ensure GPU context to avoid RAM)
    for m in re.finditer(r"\b(\d+)(?:\s*)?(gb|mb)\b", tl, re.IGNORECASE):
        span = max(0, m.start() - 20)
        window = tl[span:m.end()+20]
        if _is_vram_context(window):
            size = f"{m.group(1)}{m.group(2).upper()}"
            result.setdefault("gpu_spec", size)
            break

    # GPU type heuristic
    if "integrated" in tl or "igpu" in tl or "intel hd" in tl or "iris" in tl or "uhd" in tl:
        result.setdefault("gpu_type", "Integrated")
    elif any(k in tl for k in ["discrete", "dedicated", "rtx", "gtx", "rx", "quadro", "tesla", "radeon"]):
        result.setdefault("gpu_type", "Discrete")

    if logger:
        try:
            logger.debug(f"Description Graphics extraction: {result}")
        except Exception:
            pass
    return result



