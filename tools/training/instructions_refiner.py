from __future__ import annotations

import argparse
import re
import json
from pathlib import Path
from typing import Dict, Tuple


def ensure_yaml():
    try:
        import yaml  # type: ignore
        return yaml
    except Exception:
        import subprocess, sys
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyyaml'])
        import yaml  # type: ignore
        return yaml


def iter_parsed_lines(root: Path):
    for fp in sorted(root.glob('python_parsed_*.txt')):
        try:
            for line in fp.read_text(encoding='utf-8', errors='replace').splitlines():
                yield line
        except Exception:
            continue


def harvest_label_map(items_dir: Path, backups_root: Path | None) -> Tuple[Dict[Tuple[str, str], int], Dict[Tuple[str, str], int]]:
    """
    Returns (specifics_counts, table_counts) as dicts mapping (label_lower, key) -> count
    """
    pat = re.compile(r"^\s*\[(?P<key>[^\]]+)\]\s*(?P<label>[^:]+)\s*:\s*(?P<val>.*)$")
    spec_counts: Dict[Tuple[str, str], int] = {}
    table_counts: Dict[Tuple[str, str], int] = {}

    def add_counts(path: Path):
        for line in iter_parsed_lines(path):
            m = pat.match(line)
            if not m:
                continue
            key = m.group('key').strip()
            label = m.group('label').strip().strip('.').lower()
            pair = (label, key)
            if key.startswith('specs_'):
                spec_counts[pair] = spec_counts.get(pair, 0) + 1
            elif key.startswith('table_'):
                table_counts[pair] = table_counts.get(pair, 0) + 1

    add_counts(items_dir)
    if backups_root and backups_root.exists():
        for sub in backups_root.rglob('item_contents'):
            add_counts(sub)
        # also accept files directly under backups_root
        add_counts(backups_root)
    return spec_counts, table_counts


def reduce_to_best(counts: Dict[Tuple[str, str], int]) -> Dict[str, str]:
    """For each label, pick the key with the highest count."""
    best: Dict[str, Tuple[str, int]] = {}
    for (label, key), c in counts.items():
        if label not in best or c > best[label][1]:
            best[label] = (key, c)
    return {label: key for label, (key, _c) in best.items()}


def merge_maps(base: Dict[str, str], suggested: Dict[str, str], overwrite: bool = False, limit: int | None = None) -> Tuple[Dict[str, str], int]:
    merged = dict(base)
    added = 0
    for label, key in suggested.items():
        if (label in merged) and not overwrite:
            continue
        merged[label] = key
        added += 1
        if limit and added >= limit:
            break
    return merged, added


def main():
    ap = argparse.ArgumentParser(description='Suggest/update instructions label maps from corpus')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    ap.add_argument('--instructions', default='training/config/instructions.yaml')
    ap.add_argument('--overwrite', action='store_true')
    ap.add_argument('--limit', type=int, default=None, help='Limit number of new entries added per map')
    args = ap.parse_args()

    yaml = ensure_yaml()
    instr_path = Path(args.instructions)
    base = {}
    if instr_path.exists():
        try:
            base = yaml.safe_load(instr_path.read_text(encoding='utf-8')) or {}
        except Exception:
            base = {}
    api = base.setdefault('ai_python_parsed', {})
    cur_specs = api.get('specifics_label_map', {}) or {}
    cur_table = api.get('table_label_map', {}) or {}

    spec_counts, table_counts = harvest_label_map(Path(args.items_dir), Path(args.backups_root))
    spec_best = reduce_to_best(spec_counts)
    table_best = reduce_to_best(table_counts)

    # Only keep entries matching the right prefixes
    spec_best = {lab: key for lab, key in spec_best.items() if key.startswith('specs_')}
    table_best = {lab: key for lab, key in table_best.items() if key.startswith('table_')}

    new_specs, specs_added = merge_maps(cur_specs, spec_best, overwrite=args.overwrite, limit=args.limit)
    new_table, table_added = merge_maps(cur_table, table_best, overwrite=args.overwrite, limit=args.limit)

    api['specifics_label_map'] = dict(sorted(new_specs.items()))
    api['table_label_map'] = dict(sorted(new_table.items()))

    instr_path.parent.mkdir(parents=True, exist_ok=True)
    instr_path.write_text(yaml.safe_dump(base, sort_keys=False), encoding='utf-8')
    out = {
        'specifics_added': specs_added,
        'table_added': table_added,
        'instructions': str(instr_path),
    }
    print(json.dumps(out))


if __name__ == '__main__':
    raise SystemExit(main())


