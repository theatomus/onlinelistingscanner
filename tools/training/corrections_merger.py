from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from .mini_lm import NgramLM


PREFIX_TO_SECTION = {
    'title_': 'title',
    'specs_': 'specifics',
    'meta_': 'metadata',
    'desc_': 'description',
    'table_': 'table_shared',
    'category_': 'category',
}


def extract_kv_from_text(text: str) -> List[Tuple[str, str]]:
    """
    Extract list of (key, value) from lines like:
      [title_brand_key] brand: Dell
      [specs_ram_size_key] ram_size: 16GB
    label is optional; value may be empty.
    """
    out: List[Tuple[str, str]] = []
    pat = re.compile(r"^\s*\[(?P<key>[^\]]+)\]\s*(?:(?P<label>[^:]+)\s*:\s*)?(?P<val>.*)$")
    for line in text.splitlines():
        m = pat.match(line)
        if not m:
            continue
        k = m.group('key').strip()
        v = (m.group('val') or '').strip()
        out.append((k, v))
    return out


def read_python_parsed_files(items_dir: Path, backups_root: Path | None) -> Dict[str, str]:
    """
    Return mapping item_number -> full text content from python_parsed_*.txt
    Searches items_dir (shallow) and backups_root recursively if provided.
    If duplicates are found, prefer current items_dir.
    """
    results: Dict[str, str] = {}
    # current
    for fp in sorted(items_dir.glob('python_parsed_*.txt')):
        item = fp.stem.replace('python_parsed_', '')
        try:
            results[item] = fp.read_text(encoding='utf-8', errors='replace')
        except Exception:
            continue
    # backups
    if backups_root and backups_root.exists():
        for fp in sorted(backups_root.rglob('python_parsed_*.txt')):
            item = fp.stem.replace('python_parsed_', '')
            if item in results:
                continue
            try:
                results[item] = fp.read_text(encoding='utf-8', errors='replace')
            except Exception:
                continue
    return results


def import_as_corrections(items_dir: Path, backups_root: Path | None, corrections_dir: Path, overwrite: bool = False) -> Tuple[int, int]:
    """
    Write python_parsed_*.txt contents into training/corrections/<item>.txt
    Returns (written, skipped)
    """
    corrections_dir.mkdir(parents=True, exist_ok=True)
    data = read_python_parsed_files(items_dir, backups_root)
    written = 0
    skipped = 0
    for item, text in data.items():
        out = corrections_dir / f"{item}.txt"
        if out.exists() and not overwrite:
            skipped += 1
            continue
        try:
            out.write_text(text, encoding='utf-8')
            written += 1
        except Exception:
            skipped += 1
    return written, skipped


def scan_corrections_keys_and_values(corrections_dir: Path) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
    """
    Returns (section_to_keys, key_to_values_set) extracted from corrections/*.txt
    """
    section_to_keys: Dict[str, Set[str]] = {s: set() for s in ['title', 'specifics', 'metadata', 'description', 'table_shared', 'category']}
    key_to_values: Dict[str, Set[str]] = {}
    for fp in sorted(corrections_dir.glob('*.txt')):
        try:
            text = fp.read_text(encoding='utf-8', errors='replace')
        except Exception:
            continue
        for key, val in extract_kv_from_text(text):
            sec = None
            for prefix, sname in PREFIX_TO_SECTION.items():
                if key.startswith(prefix):
                    sec = sname
                    break
            if not sec:
                continue
            section_to_keys.setdefault(sec, set()).add(key)
            key_to_values.setdefault(key, set()).add(val)
    return section_to_keys, key_to_values


def merge_into_schema(section_to_keys: Dict[str, Set[str]], schema_path: Path) -> None:
    existing: Dict[str, List[str]] = {}
    if schema_path.exists():
        try:
            data = json.loads(schema_path.read_text(encoding='utf-8'))
            existing = data.get('allowed_keys', {}) or {}
        except Exception:
            existing = {}
    for sec, keys in section_to_keys.items():
        prev = set(existing.get(sec, []))
        existing[sec] = sorted(prev | set(keys))
    schema = {'allowed_keys': existing}
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, indent=2), encoding='utf-8')


def write_gold_values(key_to_values: Dict[str, Set[str]], out_path: Path, limit_per_key: int = 1000) -> None:
    serial = {k: sorted(list(v))[:limit_per_key] for k, v in key_to_values.items()}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(serial, indent=2), encoding='utf-8')


def rebuild_mini_lm(dataset_path: Path, gold_values_path: Path, out_path: Path) -> None:
    lm = NgramLM(n=3)
    # Fit from existing dataset if present
    if dataset_path.exists():
        try:
            lm.fit(str(dataset_path))
        except Exception:
            pass
    # Also fit from gold values
    if gold_values_path.exists():
        try:
            gold = json.loads(gold_values_path.read_text(encoding='utf-8'))
            for key, values in gold.items():
                for v in values:
                    lm.fit_value(key, v)
        except Exception:
            pass
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lm.save(out_path)


def main():
    ap = argparse.ArgumentParser(description='Import corrections from python_parsed and merge into schema and gold set')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    ap.add_argument('--corrections-dir', default='training/corrections')
    ap.add_argument('--schema', default='training/schema.json')
    ap.add_argument('--gold-values', default='training/gold_values.json')
    ap.add_argument('--dataset', default='training/training_dataset.json')
    ap.add_argument('--mini-lm-out', default='training/mini_lm.json')
    ap.add_argument('--no-import', action='store_true', help='Skip importing from python_parsed; only merge existing corrections')
    ap.add_argument('--overwrite', action='store_true', help='Overwrite existing corrections when importing')
    args = ap.parse_args()

    items_dir = Path(args.items_dir)
    backups_root = Path(args.backups_root)
    corrections_dir = Path(args.corrections_dir)
    schema_path = Path(args.schema)
    gold_values_path = Path(args.gold_values)
    dataset_path = Path(args.dataset)
    mini_lm_out = Path(args.mini_lm_out)

    if not args.no_import:
        w, s = import_as_corrections(items_dir, backups_root, corrections_dir, overwrite=args.overwrite)
        print(f"Imported corrections from python_parsed: written={w}, skipped={s}")

    section_to_keys, key_to_values = scan_corrections_keys_and_values(corrections_dir)
    merge_into_schema(section_to_keys, schema_path)
    print(f"Merged keys into {schema_path}")

    write_gold_values(key_to_values, gold_values_path)
    print(f"Wrote gold values to {gold_values_path}")

    rebuild_mini_lm(dataset_path, gold_values_path, mini_lm_out)
    print(f"Rebuilt mini-LM at {mini_lm_out}")


if __name__ == '__main__':
    raise SystemExit(main())


