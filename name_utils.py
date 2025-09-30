import json
import os
import re
from typing import Dict, Optional


_MAPPING_CACHE: Optional[Dict[str, str]] = None
_MAPPING_PATH: Optional[str] = None


def _default_mapping_path() -> str:
    base_dir = os.path.dirname(__file__)
    return os.path.join(base_dir, 'configs', 'initials_mapping.json')


def load_initials_mapping(mapping_path: Optional[str] = None) -> Dict[str, str]:
    global _MAPPING_CACHE, _MAPPING_PATH
    if mapping_path is None:
        mapping_path = _default_mapping_path()
    # If cache is valid for same path, reuse
    if _MAPPING_CACHE is not None and _MAPPING_PATH == mapping_path:
        return _MAPPING_CACHE

    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Normalize keys to uppercase for robust lookups
            _MAPPING_CACHE = {str(k).upper(): str(v) for k, v in data.items()}
            _MAPPING_PATH = mapping_path
            return _MAPPING_CACHE
    except Exception:
        # On any error, fall back to empty mapping
        _MAPPING_CACHE = {}
        _MAPPING_PATH = mapping_path
        return _MAPPING_CACHE


def get_name_for_initials(initials: str, mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
    if not initials:
        return None
    if mapping is None:
        mapping = load_initials_mapping()
    return mapping.get(initials.upper())


def format_initial_with_name(initials: str, mapping: Optional[Dict[str, str]] = None) -> str:
    """
    Return a display string for initials, appending the real name in parentheses when known.
    Example: "SF" -> "SF (Shawn Ford)"; unknown returns "SF".
    """
    name = get_name_for_initials(initials, mapping)
    if name:
        return f"{initials} ({name})"
    return initials


_SKU_PREFIX_REGEX = re.compile(r'^([A-Z]{2})(\s*-\s*|\s+)')


def annotate_sku_with_name(sku: str, mapping: Optional[Dict[str, str]] = None) -> str:
    """
    Insert the real name immediately after the SKU prefix initials when known.
    Examples:
      "SF-72873-M9" -> "SF (Shawn Ford)-72873-M9"
      "JW 12345"    -> "JW (Jacob Watson) 12345"
    When the initials are not in the mapping, returns the sku unchanged.
    """
    if not isinstance(sku, str) or not sku:
        return sku
    match = _SKU_PREFIX_REGEX.match(sku.strip())
    if not match:
        return sku
    initials = match.group(1)
    sep = match.group(2)
    name = get_name_for_initials(initials, mapping)
    if not name:
        return sku
    # Rebuild with name inserted after initials
    start, end = match.span()
    annotated_prefix = f"{initials} ({name}){sep}"
    return annotated_prefix + sku[end:]


