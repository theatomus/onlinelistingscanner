from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Set, Tuple


PREFIX_TO_SECTION = {
    'title_': 'title',
    'specs_': 'specifics',
    'meta_': 'metadata',
    'desc_': 'description',
    'table_': 'table_shared',  # union of shared/entry keys for schema purposes
    'category_': 'category',
}


def scan_python_parsed(item_dir: Path) -> Dict[str, Set[str]]:
    """
    Extract all bracketed keys from python_parsed_*.txt files under item_dir.
    Returns a mapping section -> set(keys) where keys are bracket keys like 'title_brand_key'.
    """
    found: Dict[str, Set[str]] = {s: set() for s in ['title', 'specifics', 'metadata', 'description', 'table_shared', 'category']}
    pattern = re.compile(r"^\s*\[(?P<key>[^\]]+)\]")
    for fp in sorted(item_dir.glob('python_parsed_*.txt')):
        try:
            for line in fp.read_text(encoding='utf-8', errors='replace').splitlines():
                m = pattern.match(line)
                if not m:
                    continue
                key = m.group('key').strip()
                section = None
                for prefix, sec in PREFIX_TO_SECTION.items():
                    if key.startswith(prefix):
                        section = sec
                        break
                # Special cases
                if not section and key == 'leaf_category_key':
                    section = 'category'
                if section:
                    found.setdefault(section, set()).add(key)
        except Exception:
            continue
    return found


def scan_process_description(src: Path) -> Dict[str, Set[str]]:
    """
    Find key-like tokens in process_description.py such as title_*_key, specs_*_key, etc.
    """
    found: Dict[str, Set[str]] = {s: set() for s in ['title', 'specifics', 'metadata', 'description', 'table_shared', 'category']}
    try:
        text = src.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return found

    # Generic pattern for prefixed keys
    for prefix, sec in PREFIX_TO_SECTION.items():
        for m in re.finditer(rf"\b{re.escape(prefix)}[a-z0-9_]+_key\b", text, flags=re.IGNORECASE):
            found[sec].add(m.group(0))

    # Also catch meta_*_key that may be constructed dynamically (already covered by above)
    # Category leaf key
    if 'leaf_category_key' in text:
        found['category'].add('leaf_category_key')
    return found


def union_keysets(a: Dict[str, Set[str]], b: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for sec in set(a.keys()) | set(b.keys()):
        out[sec] = set(a.get(sec, set())) | set(b.get(sec, set()))
    return out


def save_all_keys(all_keys: Dict[str, Set[str]], out_path: Path) -> None:
    serial = {sec: sorted(keys) for sec, keys in all_keys.items()}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(serial, indent=2), encoding='utf-8')


def merge_into_schema(all_keys: Dict[str, Set[str]], schema_path: Path) -> None:
    existing = {}
    if schema_path.exists():
        try:
            existing = json.loads(schema_path.read_text(encoding='utf-8'))
        except Exception:
            existing = {}
    allowed = existing.get('allowed_keys', {}) if isinstance(existing, dict) else {}

    for sec, keys in all_keys.items():
        allowed.setdefault(sec, [])
        merged = set(allowed.get(sec, [])) | set(sorted(keys))
        allowed[sec] = sorted(merged)

    schema = {'allowed_keys': allowed}
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, indent=2), encoding='utf-8')


def main():
    ap = argparse.ArgumentParser(description='Build keyspace from python_parsed files and process_description.py and merge into schema')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--process-src', default='process_description.py')
    ap.add_argument('--out-keys', default='training/all_keys.json')
    ap.add_argument('--schema', default='training/schema.json')
    args = ap.parse_args()

    items_dir = Path(args.items_dir)
    process_src = Path(args.process_src)

    keys_from_files = scan_python_parsed(items_dir)
    keys_from_src = scan_process_description(process_src)
    all_keys = union_keysets(keys_from_files, keys_from_src)

    save_all_keys(all_keys, Path(args.out_keys))
    merge_into_schema(all_keys, Path(args.schema))
    print(f"Wrote {args.out_keys} and merged keys into {args.schema}")


if __name__ == '__main__':
    raise SystemExit(main())


