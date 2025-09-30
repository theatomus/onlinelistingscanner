from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Optional, List, Tuple

from .value_normalizer import normalize_section_values


def load_instructions(path: Path = Path('training/config/instructions.yaml')) -> Dict:
    try:
        import yaml  # type: ignore
    except Exception:
        import subprocess, sys
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyyaml'])
        import yaml  # type: ignore
    try:
        if path.exists():
            return yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    except Exception:
        return {}
    return {}

def ensure_requests():
    try:
        import requests  # type: ignore
        return requests
    except Exception:
        import subprocess, sys
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
        import requests  # type: ignore
        return requests


def read_description_file(item_or_path: str, items_dir: Path) -> tuple[str, str]:
    p = Path(item_or_path)
    if p.exists():
        item = p.stem.replace('_description','')
        text = p.read_text(encoding='utf-8', errors='replace')
        return item, text
    # treat as item number
    item = item_or_path
    desc_path = items_dir / f"{item}_description.txt"
    text = desc_path.read_text(encoding='utf-8', errors='replace')
    # Strip BOM if present
    if text and text[0] == '\ufeff':
        text = text.lstrip('\ufeff')
    return item, text


def extract_conditions(description_text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not description_text:
        return out
    # Standalone codes like F3 - Key Functions Working, C4 - Used Good
    standalone_pattern = r'(?:^|\s)([FC]\d+)\s*[-:]\s*([^,\n\r\?]+?)(?=\s*[,\n\r\?\*\u2022]|\s+\?\?|$)'
    for code, cond in re.findall(standalone_pattern, description_text, flags=re.IGNORECASE|re.MULTILINE):
        code = code.upper().strip()
        cond = re.sub(r'\s+', ' ', cond).rstrip('.,;?!').strip()
        if not cond:
            continue
        key = 'desc_functional_condition_key' if code.startswith('F') else 'desc_cosmetic_condition_key'
        if key not in out:
            out[key] = f"{code}: {cond}"
    # Explicit lines like 'Functional Condition: F3 Key Functions Working'
    line_patterns = [
        (r'(?:Functional\s*Condition)\s*:?\s*([CF]\d+)?\s*[-:]?\s*(.+)$','desc_functional_condition_key'),
        (r'(?:Cosmetic\s*Condition)\s*:?\s*([CF]\d+)?\s*[-:]?\s*(.+)$','desc_cosmetic_condition_key')
    ]
    for line in description_text.splitlines():
        ll = line.strip()
        if not ll:
            continue
        for pat, key in line_patterns:
            m = re.search(pat, ll, flags=re.IGNORECASE)
            if m:
                code = (m.group(1) or '').upper().strip()
                val = re.sub(r'\s+',' ', (m.group(2) or '').strip()).rstrip('.,;')
                if val and key not in out:
                    out[key] = f"{code}: {val}" if code else val
    return out


# --- Deterministic extractors to mimic python_parsed layout ---

DEFAULT_TABLE_LABEL_MAP = {
    # Table / device block
    'make': 'table_brand_key',
    'model': 'table_model_key',
    'diagnostic': 'table_diagnostic_key',
    'processor (cpu)': 'table_cpu_full_key',
    'processor': 'table_cpu_full_key',
    'memory (ram)': 'table_ram_full_key',
    'memory': 'table_ram_full_key',
    'hard drive': 'table_storage_type_key',
    'optical drive': 'table_optical_drive_key',
    'video card': 'table_videocard_key',
    'screen size': 'table_screen_composite_key',
    'battery': 'table_battery_key',
    'webcam': 'table_webcam_key',
    'ethernet': 'table_ethernet_key',
    'wifi': 'table_wifi_key',
    'bluetooth': 'table_bluetooth_key',
    'charger': 'table_charger_key',
    'os': 'table_os_key',
    'additional components': 'table_additional_components_key',
    'notes': 'table_notes_key',
    'defects': 'table_defects_key',
    'missing components': 'table_missing_components_key',
}
DEFAULT_SPECIFICS_LABEL_MAP = {
    'brand': 'specs_brand_key',
    'processor': 'specs_cpu_full_key',
    'screen size': 'specs_screen_size_key',
}

LABEL_SYNONYMS_DEFAULT = {
    # Table
    'gpu': 'video card',
    'graphics': 'video card',
    'graphics card': 'video card',
    'cpu': 'processor',
    'operating system': 'os',
    'wifi/ bluetooth': 'wifi',
    # Specifics common
    'processor cpu': 'processor',
}



TITLE_DERIVED_KEYS = [
    'title_brand_key', 'title_model_key', 'title_device_type_key', 'title_battery_status_key',
    'title_cpu_brand_key', 'title_cpu_family_key', 'title_cpu_model_key', 'title_cpu_generation_key',
    'title_cpu_speed_key', 'title_cpu_suffix_key', 'title_ram_size_key', 'title_storage_type_key',
    'title_storage_status_key', 'title_additional_info_key',
]


def _clean_label(s: str) -> str:
    s = s.strip().strip(':').strip()
    s = s.lstrip('.').strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _kv_lines(text: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if ':' in line:
            lab, val = line.split(':', 1)
            out.append((_clean_label(lab).lower(), val.strip()))
    return out


def parse_category_block(text: str) -> Dict[str, str]:
    # Expect path lines under a CATEGORY PATH header
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return {}
    path = ' > '.join(lines)
    leaf = lines[-1]
    return {
        'category_path_key': f'Category Path: {path}',
        'leaf_category_key': f'Category: {leaf}',
    }


def parse_metadata_block(text: str) -> Dict[str, str]:
    # Convert lines to meta_* keys. Store value only; printing will generate the display label.
    meta: Dict[str, str] = {}
    for lab, val in _kv_lines(text):
        if re.fullmatch(r"[a-z0-9_]+_key", lab):
            key = lab
        else:
            key = f'meta_{lab.replace(" ", "_")}_key'
        meta[key] = val
    return meta


def parse_specifics_block(text: str, label_map: Dict[str, str]) -> Dict[str, str]:
    specs: Dict[str, str] = {}
    for lab, val in _kv_lines(text):
        norm_lab = LABEL_SYNONYMS_DEFAULT.get(lab, lab)
        k = label_map.get(norm_lab, label_map.get(lab))
        if k:
            specs[k] = val
    return specs


def _split_wifi_bluetooth(pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for lab, val in pairs:
        if 'wifi:bluetooth' in f'{lab}:{val}'.lower():
            # Cases like 'WiFi:Bluetooth: Yes' on one line
            out.append(('wifi', 'Yes'))
            out.append(('bluetooth', 'Yes'))
        else:
            out.append((lab, val))
    return out


def parse_table_shared(text: str, label_map: Dict[str, str]) -> Dict[str, str]:
    # Parse label/value pairs where a label may be alone on a line and the value appears on the next line.
    lines = [l.rstrip() for l in text.splitlines()]
    pairs: List[Tuple[str, str]] = []
    pending_label: Optional[str] = None

    def commit(label: Optional[str], value: str):
        if not label:
            return
        lab = _clean_label(label).replace('\u00A0', ' ').strip('.')
        pairs.append((lab, value.strip()))

    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.strip()
        i += 1
        if not s:
            continue
        # Combined WiFi:Bluetooth: line
        if re.search(r"(?i)wifi\s*:\s*bluetooth\s*:", s.replace('.', '')):
            pairs.append(('WiFi', 'Yes'))
            pairs.append(('Bluetooth', 'Yes'))
            # consume possible trailing Yes on next line
            if i < len(lines) and lines[i].strip().lower() in ('yes', 'no', '-'):
                i += 1
            continue

        # Label with colon and inline value
        if ':' in s:
            lab, val = s.split(':', 1)
            lab = _clean_label(lab)
            val = val.strip()
            if val == '':
                # Lookahead for value on next non-empty line
                j = i
                while j < len(lines) and lines[j].strip() == '':
                    j += 1
                if j < len(lines):
                    val = lines[j].strip()
                    i = j + 1
            commit(lab, val)
            pending_label = None
            continue

        # Standalone label ending with ':' (already stripped), or a bare value following a pending label
        if pending_label is None and re.fullmatch(r"[A-Za-z][A-Za-z0-9 \-/().]*", s):
            # Might be a bare value without explicit label; ignore (cannot map reliably)
            continue
        else:
            if pending_label is not None:
                commit(pending_label, s)
                pending_label = None

    # Map to table_* keys
    shared: Dict[str, str] = {}
    for lab, val in pairs:
        labn = _clean_label(lab).lower()
        syn = LABEL_SYNONYMS_DEFAULT.get(labn, labn)
        key = label_map.get(syn, label_map.get(labn))
        if not key:
            continue
        shared[key] = val.strip()
    return shared


def derive_title_keys(full_title: str, category_leaf: Optional[str]) -> Dict[str, str]:
    if not full_title:
        return {}
    t = full_title
    out: Dict[str, str] = {
        'meta_title_key': f'Title: {t}',
    }
    # Brand: first token if uppercase letters? Fallback to known brands
    m = re.search(r'\b(HP|Dell|Lenovo|Apple|Acer|ASUS|Microsoft|Samsung|Toshiba)\b', t, flags=re.I)
    if m:
        out['title_brand_key'] = f'brand: {m.group(1).upper() if m.group(1).upper()=="HP" else m.group(1).title()}'
    # Model: sequences like ZBook 15u G6, ThinkPad T480
    m = re.search(r'\b([A-Za-z]+\s?(?:Book|Pad|MacBook|EliteBook)\s?[A-Za-z0-9\- ]+)\b', t)
    if m:
        out['title_model_key'] = f'model: {m.group(1).strip()}'
    # CPU brand/family/model/suffix
    if re.search(r'\bIntel\b', t, flags=re.I):
        out['title_cpu_brand_key'] = 'cpu_brand: Intel'
    fam = re.search(r'\b(Core\s+i[3579])\b', t, flags=re.I)
    if fam:
        out['title_cpu_family_key'] = f'cpu_family: {fam.group(1).replace(" ", " ").title()}'
    model = re.search(r'i[3579]-?(\d{3,5})([A-Z]{1,2})?', t, flags=re.I)
    if model:
        out['title_cpu_model_key'] = f'cpu_model: {model.group(1)}'
        if model.group(2):
            out['title_cpu_suffix_key'] = f'cpu_suffix: {model.group(2)}'
    gen = re.search(r'\b(\d{1,2})(?:th)?\s*Gen\b', t, flags=re.I)
    if gen:
        out['title_cpu_generation_key'] = f'cpu_generation: {gen.group(1)}th Gen'
    spd = re.search(r'(\d+(?:\.\d+)?)\s*GHz', t, flags=re.I)
    if spd:
        out['title_cpu_speed_key'] = f'cpu_speed: {spd.group(1)}GHz'
    ram = re.search(r'(\d+\s?GB)\s*RAM', t, flags=re.I)
    if ram:
        out['title_ram_size_key'] = f'ram_size: {ram.group(1).replace(" ", "").upper()}'
    if 'NO BATTERY' in t.upper():
        out['title_battery_status_key'] = 'battery_status: Not Included'
    if 'NO CHARGER' in t.upper():
        out['title_additional_info_key'] = 'additional_info: Core NO CHARGER'
    if 'HDD' in t.upper():
        out['title_storage_type_key'] = 'storage_type: HDD'
    if 'NO HDD' in t.upper() or 'NO SSD' in t.upper():
        out['title_storage_status_key'] = 'storage_status: Not Included'
    if category_leaf:
        out['title_device_type_key'] = f'device_type: {category_leaf}'
    return out


def llm_extract_specs_from_description(description_text: str, llm_url: Optional[str]) -> Dict[str, str]:
    if not llm_url:
        return {}
    requests = ensure_requests()
    sys_prompt = (
        "You are a JSON-only assistant for eBay listing descriptions. "
        "Extract normalized keys from the provided description. Return JSON with any of these if present: "
        "storage_capacity, ram_size, cpu_family, operating_system, screen_size, battery, device_type. No prose."
    )
    body = {
        'model': 'local',
        'messages': [
            {'role': 'system', 'content': sys_prompt},
            {'role': 'user', 'content': description_text[:6000]},
        ],
        'response_format': {'type': 'json_object'},
        'temperature': 0.1,
        'max_tokens': 256,
    }
    try:
        r = requests.post(llm_url.rstrip('/') + '/v1/chat/completions', json=body, timeout=30)
        if not r.ok:
            return {}
        content = r.json().get('choices',[{}])[0].get('message',{}).get('content','{}')
        data = json.loads(content)
        # Normalize to specs_*_key naming
        out: Dict[str, str] = {}
        mapping = {
            'storage_capacity':'specs_storage_capacity_key',
            'ram_size':'specs_ram_size_key',
            'cpu_family':'specs_cpu_family_key',
            'operating_system':'specs_operating_system_key',
            'screen_size':'specs_screen_size_key',
            'battery':'specs_battery_key',
            'device_type':'title_device_type_key',
        }
        for k,v in (data.items() if isinstance(data, dict) else []):
            if v:
                key = mapping.get(k)
                if key:
                    out[key] = str(v).strip()
        return out
    except Exception:
        return {}


def format_ai_python_parsed(
    item: str,
    description_text: str,
    title: Dict[str, str],
    metadata: Dict[str, str],
    category: Dict[str, str],
    specifics: Dict[str, str],
    table_shared: Dict[str, str],
    table_entries: List[Dict[str, str]],
    desc_fields: Dict[str, str],
    include_description_text: bool,
) -> str:
    lines: List[str] = []
    # Title Data
    if title:
        lines.append('====== TITLE DATA ======')
        if 'meta_title_key' in title:
            lines.append(f"[meta_title_key] {title['meta_title_key'].split(':',1)[0]}: {title['meta_title_key'].split(':',1)[1].strip()}")
        # Write remaining title_* keys
        for k in sorted(k for k in title.keys() if k.startswith('title_')):
            field = k.replace('title_', '').replace('_key', '')
            lines.append(f"[{k}] {field}: {title[k].split(':',1)[1].strip() if ':' in title[k] else title[k]}")
        lines.append('')

    # Metadata
    if metadata:
        lines.append('====== METADATA ======')
        for k in sorted(metadata.keys()):
            field = k.replace('meta_', '').replace('_key', '').replace('_', ' ').title()
            val = metadata[k]
            lines.append(f"[{k}] {field}: {val}")
        lines.append('')

    # Category
    if category:
        lines.append('====== CATEGORY ======')
        for k in ['category_path_key', 'leaf_category_key']:
            if k in category:
                field = 'Category Path' if k == 'category_path_key' else 'Category'
                value = category[k].split(':', 1)[1].strip() if ':' in category[k] else category[k]
                lines.append(f"[{k}] {field}: {value}")
        lines.append('')

    # Specifics
    if specifics:
        lines.append('====== SPECIFICS ======')
        for k in sorted(specifics.keys()):
            field = k.replace('specs_', '').replace('_key', '').replace('_', ' ').title()
            val = specifics[k]
            lines.append(f"[{k}] {field}: {val}")
        lines.append('')

    # Table Data
    lines.append('====== TABLE DATA ======')
    lines.append('[table_entry_count_key] Total Entries: ' + str(max(1, len(table_entries))))
    lines.append('')
    if table_shared:
        lines.append('Shared Values:')
        for k in sorted(table_shared.keys()):
            field = k.replace('table_', '').replace('_key', '').replace('_', ' ').title()
            val = table_shared[k]
            lines.append(f"[{k}] {field}: {val}")
        lines.append('')
    # Entries (write at least one)
    if not table_entries:
        table_entries = [{}]
    for idx, entry in enumerate(table_entries, start=1):
        lines.append(f'Entry {idx}:')
        for k in sorted(entry.keys()):
            field = k.replace('table_', '').replace('_key', '').replace('_', ' ').title()
            val = entry[k]
            if ':' not in val:
                val = f'{field}: {val}'
            lines.append(f"[{k}] {val}")
        lines.append('')

    # Description
    lines.append('====== DESCRIPTION ======')
    for key in ['desc_cosmetic_condition_key', 'desc_functional_condition_key', 'desc_datasanitization_key']:
        if key in desc_fields:
            field = key.replace('desc_', '').replace('_key', '').replace('_', ' ').title()
            lines.append(f"[{key}] {field}: {desc_fields[key]}")
    if include_description_text:
        lines.append(f"[desc_description_text_key] Description Text: {description_text.strip()}")
    lines.append('')
    return "\n".join(lines)


def write_ai_parsed(item: str, content: str, items_dir: Path) -> Path:
    # Ensure target directory exists even if caller provided a relative path
    try:
        items_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    out_path = items_dir / f"ai_python_parsed_{item}.txt"
    # Ensure parent exists
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding='utf-8')
    return out_path


def main():
    ap = argparse.ArgumentParser(description='Generate AI-based python_parsed-like file from description')
    ap.add_argument('item_or_path', help='Item number or path to *_description.txt')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--llm-url', default=None, help='Optional local llama.cpp URL (e.g., http://127.0.0.1:8080)')
    ap.add_argument('--full', action='store_true', help='Force full extraction of Title/Meta/Category/Specifics/Table/Description')
    args = ap.parse_args()

    items_dir = Path(args.items_dir)
    instructions = load_instructions(Path('training/config/instructions.yaml'))
    item, desc_text = read_description_file(args.item_or_path, items_dir)

    # 1) Parse key blocks directly from description text
    # METADATA
    meta_block = re.search(r"={3,}\s*METADATA\s*={3,}(.*?)(?=={3,}|\Z)", desc_text, flags=re.IGNORECASE | re.DOTALL)
    metadata = parse_metadata_block(meta_block.group(1)) if meta_block else {}
    # CATEGORY PATH
    cat_block = re.search(r"={3,}\s*CATEGOR[YI]\s*PATH\s*={3,}(.*?)(?=={3,}|\Z)", desc_text, flags=re.IGNORECASE | re.DOTALL)
    category = parse_category_block(cat_block.group(1)) if cat_block else {}
    # ITEM SPECIFICS
    specs_block = re.search(r"={3,}\s*ITEM\s*SPECIFICS\s*={3,}(.*?)(?=={3,}|\Z)", desc_text, flags=re.IGNORECASE | re.DOTALL)
    specifics_label_map = instructions.get('ai_python_parsed', {}).get('specifics_label_map', DEFAULT_SPECIFICS_LABEL_MAP)
    specifics = parse_specifics_block(specs_block.group(1), specifics_label_map) if specs_block else {}
    # TABLE-like block under "==== TABLE DATA ===" or "=== ITEM DESCRIPTION ===" list of labels
    table_block = re.search(r"={3,}\s*ITEM\s*DESCRIPTION\s*={3,}(.*?)(?=={3,}|\Z)", desc_text, flags=re.IGNORECASE | re.DOTALL)
    # Pre-fix common combined markers before parsing
    tb_text = table_block.group(1) if table_block else ''
    tb_text = tb_text.replace('WiFi:Bluetooth:', 'WiFi: Yes\nBluetooth: Yes')
    table_label_map = instructions.get('ai_python_parsed', {}).get('table_label_map', DEFAULT_TABLE_LABEL_MAP)
    table_shared = parse_table_shared(tb_text, table_label_map) if tb_text else {}

    # 2) Conditions (cosmetic/functional/data)
    desc_fields = {}
    conds = extract_conditions(desc_text)
    if 'desc_cosmetic_condition_key' in conds:
        desc_fields['desc_cosmetic_condition_key'] = conds['desc_cosmetic_condition_key']
    if 'desc_functional_condition_key' in conds:
        desc_fields['desc_functional_condition_key'] = conds['desc_functional_condition_key']
    # Data sanitization code (ND, etc.)
    m = re.search(r"Data\s*Sanitization\s*:\s*([^\n\r]+)", desc_text, flags=re.IGNORECASE)
    if m:
        desc_fields['desc_datasanitization_key'] = m.group(1).strip()

    # 3) Title-derived keys
    ft = re.search(r"^\s*(?:Full\s+)?Title\s*:\s*(.+)$", desc_text, flags=re.IGNORECASE | re.MULTILINE)
    full_title = ft.group(1).strip() if ft else ''
    title = derive_title_keys(full_title, category.get('leaf_category_key', '').split(':')[-1].strip() if category.get('leaf_category_key') else None)

    # Fallbacks: populate title keys from parsed specifics/table when missing
    if 'title_brand_key' not in title and 'specs_brand_key' in specifics:
        title['title_brand_key'] = f"brand: {specifics['specs_brand_key']}"
    if 'title_model_key' not in title and 'table_model_key' in table_shared:
        # table_model_key value is like 'Model: ZBook 15u G6'
        v = table_shared['table_model_key']
        vv = v.split(':', 1)[1].strip() if ':' in v else v
        title['title_model_key'] = f'model: {vv}'
    if 'title_device_type_key' not in title and category.get('leaf_category_key'):
        title['title_device_type_key'] = f"device_type: {category['leaf_category_key'].split(':',1)[1].strip()}"

    # 4) LLM assist for SPECIFICS and device type if provided
    if instructions.get('ai_python_parsed', {}).get('enable_llm_spec_extraction', True):
        llm_specs = llm_extract_specs_from_description(desc_text, args.llm_url)
    else:
        llm_specs = {}
    # Merge LLM specifics without overwriting parsed ones
    for k, v in llm_specs.items():
        if k.startswith('specs_') and k not in specifics:
            specifics[k] = f"{k.replace('specs_','').replace('_key','').replace('_',' ').title()}: {v}"
        if k == 'title_device_type_key' and 'title_device_type_key' not in title:
            title['title_device_type_key'] = f'device_type: {v}'

    # 5) Derive richer fields from cpu/ram/storage when possible
    cpu_full = table_shared.get('table_cpu_full_key') or specifics.get('specs_cpu_full_key', '')
    if cpu_full:
        m = re.search(r'(Intel|AMD)', cpu_full, flags=re.I)
        if m and 'title_cpu_brand_key' not in title:
            title['title_cpu_brand_key'] = f'cpu_brand: {m.group(1).title()}'
        fam = re.search(r'(Core\s+i[3579])', cpu_full, flags=re.I)
        if fam and 'title_cpu_family_key' not in title:
            title['title_cpu_family_key'] = f'cpu_family: {fam.group(1).title()}'
        mdl = re.search(r'i[3579]-?(\d{3,5})([A-Z]{1,2})?', cpu_full, flags=re.I)
        if mdl and 'title_cpu_model_key' not in title:
            title['title_cpu_model_key'] = f'cpu_model: {mdl.group(1)}'
            if mdl.group(2) and 'title_cpu_suffix_key' not in title:
                title['title_cpu_suffix_key'] = f'cpu_suffix: {mdl.group(2)}'
        spd = re.search(r'(\d+(?:\.\d+)?)\s*GHz', cpu_full, flags=re.I)
        if spd and 'title_cpu_speed_key' not in title:
            title['title_cpu_speed_key'] = f'cpu_speed: {spd.group(1)}GHz'
    # RAM derivation
    ram_full = table_shared.get('table_ram_full_key', '')
    if ram_full:
        ram = re.search(r'(\d+)\s*GB', ram_full, flags=re.I)
        if ram and 'title_ram_size_key' not in title:
            title['title_ram_size_key'] = f'ram_size: {ram.group(1)}GB'
    # Storage interpretation
    storage = table_shared.get('table_storage_type_key', '')
    if storage:
        if re.search(r'\bNo\b', storage, flags=re.I):
            title.setdefault('title_storage_status_key', 'storage_status: Not Included')
        if re.search(r'\bSSD\b', storage, flags=re.I):
            title.setdefault('title_storage_type_key', 'storage_type: SSD')
        elif re.search(r'\bHDD\b', storage, flags=re.I):
            title.setdefault('title_storage_type_key', 'storage_type: HDD')

    # OS type/status derivation
    os_val = table_shared.get('table_os_key', '') or specifics.get('specs_operating_system_key', '')
    if os_val:
        if re.search(r'\bNo\b', os_val, flags=re.I):
            title.setdefault('title_os_status_key', 'os_status: Not Included')
        else:
            # type extraction
            if re.search(r'Windows\s*(\d+|10|11|7|8\.?1?)', os_val, flags=re.I):
                title.setdefault('title_os_type_key', 'os_type: Windows')
            elif re.search(r'mac\s?os|macos|os\s?x', os_val, flags=re.I):
                title.setdefault('title_os_type_key', 'os_type: macOS')
            elif re.search(r'Chrome\s?OS', os_val, flags=re.I):
                title.setdefault('title_os_type_key', 'os_type: Chrome OS')
            elif re.search(r'Ubuntu|Linux', os_val, flags=re.I):
                title.setdefault('title_os_type_key', 'os_type: Linux')

    # Additional info consolidation from title text
    t_upper = full_title.upper()
    add_info: List[str] = []
    if 'NO CHARGER' in t_upper or re.search(r'\bNO\s*AC\s*ADAPTER\b', t_upper):
        add_info.append('NO CHARGER')
    if 'NO BATTERY' in t_upper:
        add_info.append('NO BATTERY')
    if re.search(r'\bNO\s*(HDD|SSD|DRIVE)\b', t_upper):
        add_info.append('NO DRIVE')
    if add_info and 'title_additional_info_key' not in title:
        title['title_additional_info_key'] = f"additional_info: {' '.join(sorted(set(add_info)))}"

    # Lot detection
    mlot = re.search(r'\bLot\s*(?:of\s*)?(\d+)\b', full_title, flags=re.I) or re.search(r'\b(\d+)x\b', full_title)
    if mlot and 'title_lot_key' not in title:
        title['title_lot_key'] = f'lot: {mlot.group(1)}'

    # Form factor from title
    if re.search(r'\bSFF\b|Small\s*Form\s*Factor', full_title, flags=re.I):
        title.setdefault('title_form_factor_key', 'form_factor: SFF')
    elif re.search(r'\bUSFF\b|Ultra\s*Small\s*Form\s*Factor', full_title, flags=re.I):
        title.setdefault('title_form_factor_key', 'form_factor: USFF')
    elif re.search(r'\bMicro(?!soft)\b', full_title, flags=re.I):
        title.setdefault('title_form_factor_key', 'form_factor: Micro')
    elif re.search(r'\bMini\b', full_title, flags=re.I):
        title.setdefault('title_form_factor_key', 'form_factor: Mini')
    elif re.search(r'\bTower\b', full_title, flags=re.I):
        title.setdefault('title_form_factor_key', 'form_factor: Tower')

    # Color from title (basic)
    colors = ['Black','Silver','Gray','Grey','White','Blue','Red']
    for c in colors:
        if re.search(rf'\b{c}\b', full_title, flags=re.I):
            title.setdefault('title_color_key', f'color: {c}')
            break

    # Network from table
    if 'table_wifi_key' in table_shared:
        wifi_val = table_shared['table_wifi_key']
        if re.search(r'\bYes\b', wifi_val, flags=re.I):
            title.setdefault('title_network_status1_key', 'network_status1: WiFi Yes')
        elif re.search(r'\bNo\b', wifi_val, flags=re.I):
            title.setdefault('title_network_status1_key', 'network_status1: WiFi No')

    # GPU from table videocard
    vc = table_shared.get('table_videocard_key', '')
    if vc:
        mbrand = re.search(r'(NVIDIA|AMD|Intel)', vc, flags=re.I)
        if mbrand:
            title.setdefault('title_gpu_brand_key', f'gpu_brand: {mbrand.group(1).upper()}')
        # Series/model heuristics
        mseries = re.search(r'(GeForce\s+(GTX|RTX)|Radeon\s+(RX|Pro))\s*([A-Za-z0-9 ]+)?', vc, flags=re.I)
        if mseries:
            series = mseries.group(1)
            rest = (mseries.group(4) or '').strip()
            title.setdefault('title_gpu_series_key', f'gpu_series: {series}')
            if rest:
                title.setdefault('title_gpu_model_key', f'gpu_model: {rest}')

    # Storage capacities from title (up to 3)
    caps = re.findall(r'(\d+(?:\.\d+)?)\s*(TB|GB)\b', full_title, flags=re.I)
    if caps:
        seen = []
        idx = 1
        for num, unit in caps:
            val = f"{num}{unit.upper()}"
            if val in seen:
                continue
            seen.append(val)
            if idx <= 3:
                title.setdefault(f'title_storage_capacity{idx}_key', f'storage_capacity{idx}: {val}')
                idx += 1

    # 6) Normalize sections
    title_norm = normalize_section_values('title', title)
    metadata_norm = normalize_section_values('metadata', metadata)
    category_norm = category  # simple
    specifics_norm = normalize_section_values('specifics', specifics)
    table_shared_norm = normalize_section_values('table', table_shared)

    include_desc = bool(instructions.get('ai_python_parsed', {}).get('include_description_text', False))
    content = format_ai_python_parsed(
        item=item,
        description_text=desc_text,
        title=title_norm,
        metadata=metadata_norm,
        category=category_norm,
        specifics=specifics_norm,
        table_shared=table_shared_norm,
        table_entries=[],
        desc_fields=desc_fields,
        include_description_text=include_desc,
    )

    out_path = write_ai_parsed(item, content, items_dir)
    print(f"Wrote {out_path}")


if __name__ == '__main__':
    raise SystemExit(main())


