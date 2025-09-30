from __future__ import annotations

import re
from typing import Dict, Callable


def normalize_storage_units(value: str) -> str:
    if not isinstance(value, str):
        return value
    # Convert like '500 gb', '1.5tb' -> '500GB', '1.50TB'
    def fmt(m):
        num = m.group(1)
        dec = m.group(2) or ''
        unit = m.group(3).upper()
        if dec == '':
            num_fmt = f"{int(num)}"
        else:
            num_fmt = f"{num}.{dec[:2]}"
        return f"{num_fmt}{unit}"
    return re.sub(r"(\d+)(?:\.(\d+))?\s*(gb|tb|mb)", fmt, value, flags=re.IGNORECASE)


def normalize_cpu_speed(value: str) -> str:
    if not isinstance(value, str):
        return value
    def fmt(m):
        whole = m.group(1)
        dec = m.group(2) or '00'
        unit_raw = m.group(3)
        unit = 'GHz' if unit_raw.lower() == 'ghz' else ('MHz' if unit_raw.lower() == 'mhz' else unit_raw)
        if len(dec) == 1:
            dec += '0'
        return f"{whole}.{dec}{unit}"
    return re.sub(r"(\d+)(?:\.(\d+))?\s*(ghz|mhz)", fmt, value, flags=re.IGNORECASE)


def normalize_list_separators(value: str) -> str:
    if not isinstance(value, str):
        return value
    # Replace slashes with commas and normalize spacing
    return ', '.join([p.strip() for p in value.replace('/', ',').split(',') if p.strip()])


def fix_cpu_family(value: str) -> str:
    if not isinstance(value, str):
        return value
    if re.fullmatch(r"i[3579]", value.strip(), flags=re.IGNORECASE):
        return f"Core {value.strip()}"
    return value


def normalize_common(value: str) -> str:
    value = normalize_storage_units(value)
    value = normalize_cpu_speed(value)
    value = normalize_list_separators(value)
    return value


SECTION_KEY_SPECIFIC: Dict[str, Dict[str, Callable[[str], str]]] = {
    'title': {
        'title_cpu_family_key': fix_cpu_family,
    },
    'specifics': {
        'specs_cpu_family_key': fix_cpu_family,
    },
    'table': {
        'table_cpu_family_key': fix_cpu_family,
    },
}


def normalize_section_values(section_name: str, section: Dict[str, str]) -> Dict[str, str]:
    normalizers = SECTION_KEY_SPECIFIC.get(section_name, {})
    out: Dict[str, str] = {}
    for k, v in section.items():
        nv = normalize_common(v) if isinstance(v, str) else v
        if k in normalizers:
            nv = normalizers[k](nv)
        out[k] = nv
    return out


