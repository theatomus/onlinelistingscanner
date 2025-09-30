import re
from typing import Any, Dict

def parse_os(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    tl = desc_text.lower()

    # status
    if re.search(r"\b(no\s+os|no\s+operating\s+system)\b", tl):
        result.setdefault("os_status", "No OS")
    elif re.search(r"\b(activated|installed|fresh\s*install)\b", tl):
        result.setdefault("os_status", "Installed")

    # type
    if "windows" in tl:
        result.setdefault("os_type", "Windows")
    elif "macos" in tl or "os x" in tl or "mac os" in tl:
        result.setdefault("os_type", "macOS")
    elif "linux" in tl or "ubuntu" in tl or "debian" in tl or "centos" in tl:
        result.setdefault("os_type", "Linux")
    elif re.search(r"\bandroid\b", tl):
        result.setdefault("os_type", "Android")
    elif re.search(r"\b(?:ios|iphone\s+os|ipad\s+os|ipados)\b", tl):
        # word-boundary match prevents false match on 'BIOS'
        result.setdefault("os_type", "iOS")

    # edition + version
    m = re.search(r"\bWindows\s+(\d+|XP|Vista|7|8(?:\.1)?|10|11)(?:\s*(Home|Pro|Enterprise|Education|Server))?\b", desc_text, re.IGNORECASE)
    if m:
        result.setdefault("os_version", m.group(1))
        if m.group(2):
            result.setdefault("os_edition", m.group(2))

    if logger:
        try:
            logger.debug(f"Description OS extraction: {result}")
        except Exception:
            pass
    return result



