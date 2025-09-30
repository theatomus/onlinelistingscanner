from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

from .parsed_reader import read_many_from_dir, read_many_from_backups, ListingData


@dataclass
class TrainingExample:
    item_number: str
    keys: Dict[str, List[str]]  # per section keys
    values: Dict[str, Dict[str, str]]  # flattened representative values per section


def normalize_section_keys(keys: List[str]) -> List[str]:
    # Keep exact keys as they appear (e.g., 'title_brand_key') to preserve existing conventions
    return sorted(keys)


def representative_values(section: Dict[str, str]) -> Dict[str, str]:
    # Keep short values only for compactness; training consumers can fetch full files if needed
    result = {}
    for k, v in section.items():
        if not isinstance(v, str):
            continue
        trimmed = v.strip()
        if len(trimmed) > 200:
            trimmed = trimmed[:200] + '…'
        result[k] = trimmed
    return result


def build_training_examples(item_dir: str | Path, backups_root: str | Path | None = None) -> List[TrainingExample]:
    # Load current items first
    data = read_many_from_dir(item_dir)
    # Merge with backups, without overwriting existing items
    if backups_root:
        backup_data = read_many_from_backups(backups_root)
        for item, val in backup_data.items():
            data.setdefault(item, val)
    examples: List[TrainingExample] = []
    for item_number, (listing, _sections) in data.items():
        ex = TrainingExample(
            item_number=item_number,
            keys={
                'title': normalize_section_keys(list(listing.title.keys())),
                'specifics': normalize_section_keys(list(listing.specifics.keys())),
                'metadata': normalize_section_keys(list(listing.metadata.keys())),
                'description': normalize_section_keys(list(listing.description.keys())),
                'table_shared': normalize_section_keys(list(listing.table_shared.keys())),
                'table_entry_union': normalize_section_keys(sorted({k for e in listing.table_data for k in e.keys()})),
            },
            values={
                'title': representative_values(listing.title),
                'specifics': representative_values(listing.specifics),
                'metadata': representative_values(listing.metadata),
                'description': representative_values(listing.description),
                'table_shared': representative_values(listing.table_shared),
                # For table entries, keep only the first entry as representative for training compactness
                'table_entry_1': representative_values(listing.table_data[0]) if listing.table_data else {},
            }
        )
        examples.append(ex)
    return examples


def save_training_dataset(examples: List[TrainingExample], out_path: str | Path) -> None:
    payload = [
        {
            'item_number': ex.item_number,
            'keys': ex.keys,
            'values': ex.values,
        }
        for ex in examples
    ]
    Path(out_path).write_text(json.dumps(payload, indent=2), encoding='utf-8')


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Build training dataset from python_parsed_*.txt directory')
    ap.add_argument('--items-dir', default='item_contents', help='Directory with python_parsed_*.txt files')
    ap.add_argument('--backups-root', default='backups/itemcontents', help='Backups root that contains historical item_contents backups')
    ap.add_argument('--out', default='training/training_dataset.json', help='Output JSON path')
    args = ap.parse_args()

    examples = build_training_examples(args.items_dir, args.backups_root)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    save_training_dataset(examples, args.out)
    print(f"Wrote {len(examples)} examples to {args.out}")


if __name__ == '__main__':
    main()


