import re
from typing import Any, Dict, List, Tuple

try:
    # Pyparsing is optional; we use it when available for tolerant KV/bullet parsing
    from pyparsing import (
        Word,
        alphanums,
        OneOrMore,
        Optional,
        Regex,
        StringStart,
        StringEnd,
        Suppress,
        nums,
        Literal,
        Or,
    )
    PYPARSING_AVAILABLE = True
except Exception:
    PYPARSING_AVAILABLE = False


def _normalize_key(raw_key: str) -> str:
    """Map common key variants to canonical keys expected downstream.

    Only returns known canonical keys; unknown keys return an empty string so caller can ignore.
    """
    key = (raw_key or "").strip().lower()
    key = re.sub(r"\s+", " ", key)

    key_map = {
        "cosmetic condition": "Cosmetic Condition",
        "cosmetic": "Cosmetic Condition",
        "functional condition": "Functional Condition",
        "functional": "Functional Condition",
        "data sanitization": "Data Sanitization",
        "data sanitation": "Data Sanitization",
        "data wipe": "Data Sanitization",
        "sanitization": "Data Sanitization",
        "sanitized": "Data Sanitization",
    }

    return key_map.get(key, "")


def _parse_r2_certification(line: str) -> Dict[str, str]:
    """Parse R2 certification lines like:
    "R2v3 Certification: F4 - Hardware Functional, C6 - Used Excellent"
    Returns a dict with possible keys 'Functional Condition' and 'Cosmetic Condition'.
    """
    result: Dict[str, str] = {}
    if not re.search(r"\bR2\w*\s*Certification\b", line, re.IGNORECASE):
        return result

    # Capture pairs like F4 - text, C6 - text, allowing colon as well
    pairs = re.findall(r"\b([FC]\d+)\s*[-:]+\s*([^,;]+)", line, re.IGNORECASE)
    for code, text in pairs:
        code = code.upper().strip()
        value_text = text.strip()
        value = f"{code}-{value_text}" if value_text else code
        key = "Functional Condition" if code.startswith("F") else "Cosmetic Condition"
        # Do not overwrite if already present in caller's dict; merging policy handled by caller
        if key not in result:
            result[key] = value
    return result


def _parse_kv_and_bullets_pyparsing(desc_text: str) -> Tuple[Dict[str, str], List[str]]:
    """Use pyparsing to capture tolerant Key/Value lines and bullet/numbered items.
    Returns (kv_dict, bullets).
    Only recognized keys are normalized and returned in kv_dict.
    """
    kv: Dict[str, str] = {}
    bullets: List[str] = []

    # Grammar for Key: Value (allowing separators :, -, — and optional surrounding spaces)
    key_token = OneOrMore(Word(alphanums + " /&().-"))
    sep = Suppress(Regex(r"\s*[:\-—]\s*"))
    value_token = Regex(r".*")
    kv_line = StringStart() + key_token("k") + sep + value_token("v") + StringEnd()

    # Grammar for bullets: "- text", "* text", "• text", or "1. text"
    bullet_sym = Or([Literal("-"), Literal("*"), Literal("•")])
    numbered = Word(nums) + Literal(".")
    bullet_prefix = Or([bullet_sym, numbered])
    bullet_line = StringStart() + bullet_prefix.suppress() + Regex(r"\s*(.*)")("b") + StringEnd()

    for raw_line in desc_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Try KV first
        try:
            parsed = kv_line.parse_string(line)
            raw_key = " ".join(parsed.get("k", []))
            value = parsed.get("v", "").strip()
            canon = _normalize_key(raw_key)
            if canon and value:
                kv.setdefault(canon, value)
                continue
        except Exception:
            pass

        # Then bullets
        try:
            parsed_b = bullet_line.parse_string(line)
            btxt = parsed_b.get("b", "").strip()
            if btxt:
                bullets.append(btxt)
                continue
        except Exception:
            pass

    return kv, bullets


def _parse_kv_and_bullets_regex(desc_text: str) -> Tuple[Dict[str, str], List[str]]:
    """Fallback regex-based KV and bullet parsing if pyparsing is unavailable."""
    kv: Dict[str, str] = {}
    bullets: List[str] = []

    kv_pattern = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 /&().-]+?)\s*(?::|\-|—)\s*(.+)$")
    bullet_pattern = re.compile(r"^\s*(?:[\-\*\u2022]|\d+\.)\s+(.*)$")

    for raw_line in desc_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = kv_pattern.match(line)
        if m:
            raw_key = m.group(1)
            value = m.group(2).strip()
            canon = _normalize_key(raw_key)
            if canon and value:
                kv.setdefault(canon, value)
            continue
        b = bullet_pattern.match(line)
        if b:
            bullets.append(b.group(1).strip())

    return kv, bullets


def parse_description_structured(desc_text: str, logger=None) -> Dict[str, Any]:
    """Parse description text to extract normalized fields and useful structures.

    Current scope (Phase 1):
    - R2 certification lines → Functional/Cosmetic Condition
    - Key/Value tolerant lines for canonical keys
    - Bullets/numbered items collected (returned under 'bullets')

    Returns a dict that may include canonical keys and 'bullets'.
    Caller is responsible for non-destructive merging into existing structures.
    """
    if not desc_text:
        return {}

    result: Dict[str, Any] = {}

    # First pass: line-wise R2 detection
    for raw_line in desc_text.splitlines():
        r2 = _parse_r2_certification(raw_line)
        for k, v in r2.items():
            # Do not overwrite here; caller defines merge policy
            result.setdefault(k, v)

    # KV + bullets using pyparsing when available; else regex
    try:
        if PYPARSING_AVAILABLE:
            kv, bullets = _parse_kv_and_bullets_pyparsing(desc_text)
        else:
            kv, bullets = _parse_kv_and_bullets_regex(desc_text)
    except Exception:
        kv, bullets = _parse_kv_and_bullets_regex(desc_text)

    # Merge only recognized canonical keys
    for k, v in kv.items():
        if v and k not in result:
            result[k] = v

    if bullets:
        result["bullets"] = bullets

    # Phase 2: Domain-specific enrichers (RAM, Storage). Non-destructive.
    try:
        from description_extraction.ram import parse_ram
        from description_extraction.storage import parse_storage
        from description_extraction.network import parse_network
        from description_extraction.cpu import parse_cpu
        from description_extraction.graphics import parse_graphics
        from description_extraction.screen import parse_screen
        from description_extraction.os import parse_os
        from description_extraction.hdd import parse_hdd
        from description_extraction.network_device import parse_switch, parse_adapter
        from description_extraction.battery import parse_battery
        from description_extraction.status import parse_status
        from description_extraction.lot import parse_lot
        from description_extraction.form_factor import parse_form_factor
        from description_extraction.phone import parse_phone
        ram_data = parse_ram(desc_text, logger)
        storage_data = parse_storage(desc_text, logger)
        network_data = parse_network(desc_text, logger)
        cpu_data = parse_cpu(desc_text, logger)
        gpu_data = parse_graphics(desc_text, logger)
        screen_data = parse_screen(desc_text, logger)
        os_data = parse_os(desc_text, logger)
        hdd_data = parse_hdd(desc_text, logger)
        switch_data = parse_switch(desc_text, logger)
        adapter_data = parse_adapter(desc_text, logger)
        battery_data = parse_battery(desc_text, logger)
        status_data = parse_status(desc_text, logger)
        lot_data = parse_lot(desc_text, logger)
        form_factor_data = parse_form_factor(desc_text, logger)
        phone_data = parse_phone(desc_text, logger)
        for k, v in ram_data.items():
            result.setdefault(k, v)
        for k, v in storage_data.items():
            result.setdefault(k, v)
        for k, v in network_data.items():
            result.setdefault(k, v)
        for k, v in cpu_data.items():
            result.setdefault(k, v)
        for k, v in gpu_data.items():
            result.setdefault(k, v)
        for k, v in screen_data.items():
            result.setdefault(k, v)
        for k, v in os_data.items():
            result.setdefault(k, v)
        for k, v in hdd_data.items():
            result.setdefault(k, v)
        for k, v in switch_data.items():
            result.setdefault(k, v)
        for k, v in adapter_data.items():
            result.setdefault(k, v)
        for k, v in battery_data.items():
            result.setdefault(k, v)
        for k, v in status_data.items():
            result.setdefault(k, v)
        for k, v in lot_data.items():
            result.setdefault(k, v)
        for k, v in form_factor_data.items():
            result.setdefault(k, v)
        for k, v in phone_data.items():
            result.setdefault(k, v)
    except Exception as e:
        if logger:
            logger.debug(f"Domain enrichers (RAM/Storage) skipped due to error: {e}")

    if logger:
        try:
            found_keys = ", ".join(result.keys())
            logger.debug(f"Structured description parsing extracted keys: {found_keys}")
        except Exception:
            pass

    return result


