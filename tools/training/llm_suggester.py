from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional

import requests


DEFAULT_CACHE = Path('training/llm_suggestions_cache.json')
INSTRUCTIONS_PATH = Path('training/config/instructions.yaml')


def _cache_key(section: str, key: str, value: str) -> str:
    raw = f"{section}||{key}||{value}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _load_cache(cache_path: Path = DEFAULT_CACHE) -> Dict[str, Any]:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def _save_cache(data: Dict[str, Any], cache_path: Path = DEFAULT_CACHE) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, indent=2), encoding='utf-8')


def _load_instructions(path: Path = INSTRUCTIONS_PATH) -> Dict[str, Any]:
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


def suggest_with_llm(section: str, key: str, value: str, llm_url: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    """
    Query a local llama.cpp server (OpenAI-compatible /v1/chat/completions) to get JSON suggestions.
    Returns a dict {normalize: str, suggest: str, notes: str} or None.
    Caches by (section, key, value).
    """
    cache = _load_cache()
    ck = _cache_key(section, key, value)
    if ck in cache:
        return cache[ck]

    system_prompt = (
        "You are a JSON-only assistant for eBay listing normalization. "
        "Always respond with strict JSON: {\"normalize\": string|null, \"suggest\": string|null, \"notes\": string|null}. "
        "Do not include any text outside JSON."
    )
    user_prompt = (
        "Given section, key and value, return normalized and suggested canonical value if applicable.\n"
        f"section: {section}\n"
        f"key: {key}\n"
        f"value: {value}\n"
        "Rules: prefer compact units (GB/TB), fix CPU family (Core i7), standardize GHz formats, return null where no change."
    )

    instr = _load_instructions()
    temperature = float(instr.get('llm_suggester', {}).get('temperature', 0.1))
    max_tokens = int(instr.get('llm_suggester', {}).get('max_tokens', 128))

    body = {
        "model": "local",  # llama.cpp ignores/uses loaded model
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
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
        parsed = json.loads(content)
        result = {
            "normalize": parsed.get("normalize"),
            "suggest": parsed.get("suggest"),
            "notes": parsed.get("notes"),
        }
        cache[ck] = result
        _save_cache(cache)
        return result
    except Exception:
        return None


