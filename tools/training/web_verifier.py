from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


DEFAULT_SOURCES = Path('training/config/web_sources.json')
INSTRUCTIONS_PATH = Path('training/config/instructions.yaml')
DEFAULT_CACHE = Path('training/web_cache.json')


@dataclass
class DeviceIdentity:
    brand: str = ''
    model: str = ''
    device_type: str = ''


def load_sources(config_path: Path = DEFAULT_SOURCES) -> Dict[str, List[str]]:
    if config_path.exists():
        try:
            return json.loads(config_path.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def _load_instructions(path: Path = INSTRUCTIONS_PATH) -> Dict:
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


def save_cache(cache: Dict[str, Dict], cache_path: Path = DEFAULT_CACHE) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2), encoding='utf-8')


def load_cache(cache_path: Path = DEFAULT_CACHE) -> Dict[str, Dict]:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def derive_identity(listing: Dict[str, Dict[str, str]]) -> DeviceIdentity:
    def first_nonempty(keys: List[Tuple[str, str]]) -> str:
        for section, key in keys:
            sec = listing.get(section, {})
            if key in sec and sec[key]:
                return str(sec[key]).strip()
        return ''

    brand = first_nonempty([
        ('title', 'title_brand_key'),
        ('specifics', 'specs_brand_key'),
        ('table_shared', 'table_brand_key'),
    ])
    model = first_nonempty([
        ('title', 'title_model_key'),
        ('specifics', 'specs_model_key'),
        ('table_shared', 'table_model_key'),
    ])
    device_type = first_nonempty([
        ('title', 'device_type_key'),
        ('title', 'title_device_type_key'),
        ('specifics', 'specs_device_type_key'),
    ])
    return DeviceIdentity(brand=brand, model=model, device_type=device_type)


def build_urls(identity: DeviceIdentity, sources: Dict[str, List[str]]) -> List[str]:
    urls: List[str] = []
    brand_key = identity.brand.strip()
    if not brand_key:
        return urls
    templates = sources.get(brand_key) or sources.get(brand_key.title()) or sources.get(brand_key.upper()) or []
    for tpl in templates:
        url = tpl.replace('{brand}', identity.brand).replace('{model}', identity.model).replace('{device_type}', identity.device_type)
        urls.append(url)
    return urls


def fetch_url(url: str, timeout: float = 8.0) -> Optional[str]:
    try:
        resp = requests.get(url, headers={'User-Agent': 'SpecVerifier/1.0'}, timeout=timeout)
        if resp.ok and resp.text:
            return resp.text[:6000]  # Keep token cost low
    except Exception:
        return None
    return None


def parse_simple_specs(text: str) -> Dict[str, str]:
    """
    Very conservative regex-based extraction for common specs.
    """
    out: Dict[str, str] = {}
    # Storage capacities like 256GB/1TB
    m = re.findall(r'(\d+(?:\.\d+)?)\s*(GB|TB)\b', text, flags=re.IGNORECASE)
    if m:
        # Return unique, normalized units
        vals = []
        seen = set()
        for num, unit in m:
            unit = unit.upper()
            val = f"{num}{unit}"
            if val not in seen:
                seen.add(val)
                vals.append(val)
        out['storage_capacity'] = ', '.join(vals[:5])
    # RAM sizes
    r = re.findall(r'(\d+)\s*GB\s*(?:RAM|Memory)\b', text, flags=re.IGNORECASE)
    if r:
        out['ram_size'] = ', '.join(sorted({f"{x}GB" for x in r}))
    # CPU family: Core i3/i5/i7/i9
    c = re.findall(r'(Core\s+i[3579])', text, flags=re.IGNORECASE)
    if c:
        out['cpu_family'] = sorted({x.title() for x in c})[0]
    return out


def llm_extract_specs(page_text: str, llm_url: str, timeout: float = 12.0) -> Dict[str, str]:
    instr = _load_instructions()
    temperature = float(instr.get('validator', {}).get('llm', {}).get('temperature', 0.1))
    max_tokens = int(instr.get('validator', {}).get('llm', {}).get('max_tokens', 256))
    body = {
        "model": "local",
        "messages": [
            {"role": "system", "content": "You are a JSON-only assistant. Extract normalized specs keys from the provided page text. Return JSON with any of these keys if present: storage_capacity, ram_size, cpu_family. No prose."},
            {"role": "user", "content": page_text[:6000]}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"}
    }
    try:
        resp = requests.post(f"{llm_url.rstrip('/')}/v1/chat/completions", json=body, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "{}")
        return json.loads(content)
    except Exception:
        return {}


def verify_with_web(listing: Dict[str, Dict[str, str]], llm_url: str, sources_path: Path = DEFAULT_SOURCES) -> List[str]:
    issues: List[str] = []
    sources = load_sources(sources_path)
    if not sources:
        return issues

    identity = derive_identity(listing)
    urls = build_urls(identity, sources)
    if not urls:
        return issues

    cache = load_cache()
    cache_key = f"{identity.brand}|{identity.model}|{identity.device_type}"
    if cache_key in cache:
        web_specs = cache[cache_key]
    else:
        web_specs = {}
        for url in urls:
            html = fetch_url(url)
            if not html:
                continue
            # Combine simple regex with LLM extraction for robustness
            extracted = parse_simple_specs(html)
            if llm_url:
                llm_specs = llm_extract_specs(html, llm_url)
                extracted.update({k: v for k, v in llm_specs.items() if v})
            # Merge into web_specs (first wins to keep deterministic)
            for k, v in extracted.items():
                web_specs.setdefault(k, v)
        cache[cache_key] = web_specs
        save_cache(cache)

    # Compare against listing values (across all sections)
    def get_current_value(key_basename: str) -> Tuple[str, str]:
        # Return (section.key, value) for first match
        prefixed = [
            ('title', f'title_{key_basename}_key'),
            ('specifics', f'specs_{key_basename}_key'),
            ('table_shared', f'table_{key_basename}_key'),
        ]
        for sec_name, k in prefixed:
            sec = listing.get(sec_name, {})
            if k in sec and sec[k]:
                return f"{sec_name}.{k}", str(sec[k])
        return '', ''

    for web_key, web_val in web_specs.items():
        sec_dot_key, current_val = get_current_value(web_key)
        if not sec_dot_key:
            issues.append(f"WEB Suggest {web_key}: '{web_val}' (no local value)")
        elif current_val and web_val and current_val.lower().strip() != web_val.lower().strip():
            issues.append(f"WEB Verify {sec_dot_key}: '{current_val}' -> '{web_val}'")

    return issues


