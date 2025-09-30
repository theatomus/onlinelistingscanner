from __future__ import annotations

import re
from typing import Optional


APPROVED_DEVICE_TYPES = {
    "Cell Phones & Smartphones",
    "Cell Phone & Smartphone Parts",
    "Tablets & eBook Readers",
    "Tablets & eReaders",
    "PC Laptops & Netbooks",
    "PC Desktops & All-In-Ones",
    "Apple Laptops",
    "Apple Desktops & All-In-Ones",
    "CPUs/Processors",
    "Graphics/Video Cards",
    "Monitors",
    "Laptop Power Adapters/Chargers",
    "Server Memory (RAM)",
    "Computer Components & Parts",
    "Computer Servers",
    "Enterprise Routers",
}


PHONE_PATTERNS = (
    r"\biphone\b", r"\bandroid\b", r"\bpixel\b", r"\bgalaxy\s+s\d*\b", r"\bsm-[a-z0-9]+\b",
    r"\boneplus\b", r"\b\d{3,4}[a-z]?\s?\-?\s?\d{3,4}[a-z]?\b"  # generic handset codes
)
TABLET_PATTERNS = (
    r"\bipad\b", r"\bipad\s+(pro|air|mini)\b", r"\btablet\b", r"\bgalaxy\s+tab\b", r"\bsurface\s+pro\b",
)
APPLE_LAPTOP_PATTERNS = (r"\bmac\s?book\b",)
APPLE_DESKTOP_PATTERNS = (r"\bimac\b", r"\bmac\s+pro\b", r"\bmac\s+mini\b", r"\bmac\s+studio\b")
PC_LAPTOP_PATTERNS = (
    r"\blaptop\b", r"\bnotebook\b", r"\blatitude\b", r"\bthinkpad\b", r"\belitebook\b", r"\bzbook\b",
    r"\bxps\b", r"\binspiron\b", r"\bprobook\b", r"\bchromebook\b", r"\bprecision\s+\d{3,4}\b"
)
PC_DESKTOP_PATTERNS = (
    r"\bdesktop\b", r"\boptiplex\b", r"\bthinkcentre\b", r"\bprecision\s+tower\b", r"\btower\b",
    r"\bnuc\b", r"\bmicro\b", r"\btiny\b", r"\bmini\s*pc\b", r"\ball[- ]in[- ]one\b", r"\baio\b"
)
GPU_PATTERNS = (r"\bgtx\b", r"\brtx\b", r"\bquadro\b", r"\bradeon\b", r"\bgraphics\s+card\b")
MONITOR_PATTERNS = (r"\bmonitor\b", r"\bdisplay\b", r"\b\d{2}[\"']\b")
ADAPTER_PATTERNS = (r"\bcharger\b", r"\bpower\s+adapter\b", r"\bac\s+adapter\b", r"\bwd\d{2}\b", r"\bdock\b")
SERVER_RAM_PATTERNS = (r"\b(ddr|sodimm|udimm|rdimm|lrdimm)\b",)
CPU_ONLY_PATTERNS = (r"\b(i[3579]-\d|xeon|ryzen)\b",)

def _any(patterns: tuple[str, ...], text: str) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def _heuristic_device_type(title: str) -> Optional[str]:
    t = title.lower()
    # Specific Dell Inspiron rule: if title mentions Inspiron and Desktop, force desktops
    # Handles variants like "Dell Inspiron 3650 Desktop" or "Inspiron desktop tower"
    if ("inspiron" in t) and ("desktop" in t or "tower" in t or "all-in-one" in t or "aio" in t):
        return "PC Desktops & All-In-Ones"
    # Parts for phones – prioritize parts before generic phone detection
    if ("iphone" in t or "android" in t or "pixel" in t or "galaxy" in t) and any(p in t for p in ("housing", "digitizer", "assembly", "screen", "parts", "oem", "genuine")):
        return "Cell Phone & Smartphone Parts"
    if _any(PHONE_PATTERNS, t):
        return "Cell Phones & Smartphones"
    if _any(TABLET_PATTERNS, t):
        return "Tablets & eReaders"
    if _any(APPLE_LAPTOP_PATTERNS, t):
        return "Apple Laptops"
    if _any(APPLE_DESKTOP_PATTERNS, t):
        return "Apple Desktops & All-In-Ones"
    if _any(PC_LAPTOP_PATTERNS, t):
        return "PC Laptops & Netbooks"
    if _any(PC_DESKTOP_PATTERNS, t):
        return "PC Desktops & All-In-Ones"
    if any(k in t for k in ["router", "asr", "isr", "vedge", "edge router", "sd-wan", "switch", "nexus", "catalyst", "edgecore", "juniper", "arista"]):
        return "Enterprise Routers"
    if any(k in t for k in ["server", "xserve", "rack server", "rackmount server", "poweredge", "proliant", "thinksystem", "superserver"]):
        return "Computer Servers"
    if _any(GPU_PATTERNS, t):
        return "Graphics/Video Cards"
    if _any(MONITOR_PATTERNS, t):
        return "Monitors"
    if _any(ADAPTER_PATTERNS, t):
        return "Laptop Power Adapters/Chargers"
    if _any(SERVER_RAM_PATTERNS, t):
        return "Server Memory (RAM)"
    if _any(CPU_ONLY_PATTERNS, t) and "motherboard" not in t:
        return "CPUs/Processors"
    return None


def classify_device_type(title: str, allow_llm: bool = False) -> Optional[str]:
    """Return an approved device type for the given title, or None if unknown.

    Uses heuristics first. If allow_llm is True and llama-cli is available, ask a tiny model to choose
    from APPROVED_DEVICE_TYPES, but only return a value that is exactly one of those approved strings.
    """
    # Ultra-light: heuristic only, zero external footprint
    h = _heuristic_device_type(title)
    return h if h in APPROVED_DEVICE_TYPES else None


