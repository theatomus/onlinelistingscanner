from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple, Optional


@dataclass
class ParsedSections:
    sections: Dict[str, List[str]] = field(default_factory=lambda: {
        'TITLE DATA': [],
        'METADATA': [],
        'CATEGORY': [],
        'SPECIFICS': [],
        'TABLE DATA': [],
        'DESCRIPTION': [],
    })


@dataclass
class ListingData:
    title: Dict[str, str] = field(default_factory=dict)
    specifics: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, str] = field(default_factory=dict)
    category: Dict[str, str] = field(default_factory=dict)
    description: Dict[str, str] = field(default_factory=dict)
    table_shared: Dict[str, str] = field(default_factory=dict)
    table_data: List[Dict[str, str]] = field(default_factory=list)


def _parse_bracket_kv(line: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse a line like: "[title_brand_key] brand: Dell"
    Returns (bracket_key, raw_key, value) or None if not matching.
    """
    if not line.startswith('['):
        return None
    try:
        end_idx = line.index(']')
    except ValueError:
        return None

    bracket_key = line[1:end_idx].strip()
    remainder = line[end_idx + 1:].strip()

    # Expect format: "raw_key: value"; tolerate missing colon
    if ': ' in remainder:
        raw_key, value = remainder.split(': ', 1)
        raw_key = raw_key.strip()
        value = value.strip()
    else:
        raw_key = remainder.strip()
        value = ''

    return bracket_key, raw_key, value


def read_parsed_txt(file_path: str | Path) -> Tuple[ListingData, ParsedSections]:
    """
    Lightweight reader for python_parsed_*.txt files produced by process_description.py.

    Avoids importing large UI-heavy modules. Parses into a normalized ListingData-like dict.
    """
    path = Path(file_path)
    listing = ListingData()
    sections = ParsedSections()

    current_section: Optional[str] = None
    in_shared_values = False
    current_entry: Optional[Dict[str, str]] = None

    if not path.exists():
        raise FileNotFoundError(f"Parsed file not found: {path}")

    content = path.read_text(encoding='utf-8', errors='replace')
    for raw_line in content.split('\n'):
        line = raw_line.strip()
        if not line:
            if current_section:
                sections.sections.setdefault(current_section, []).append(line)
            continue

        if line.startswith('======') and line.endswith('======'):
            current_section = line[6:-6].strip().upper()
            sections.sections.setdefault(current_section, []).append(line)
            in_shared_values = False
            current_entry = None
            continue

        if current_section:
            sections.sections[current_section].append(line)

        # Handle TABLE DATA entry boundaries and shared values
        if current_section == 'TABLE DATA':
            if line.startswith('Shared Values:'):
                in_shared_values = True
                current_entry = None
                continue
            if re.match(r'^Entry\s+\d+\s*:', line) and not line.startswith('['):
                in_shared_values = False
                current_entry = {}
                listing.table_data.append(current_entry)
                continue

        kv = _parse_bracket_kv(line)
        if not kv:
            continue

        bracket_key, raw_key, value = kv

        # Route to the appropriate dict based on prefix in bracket_key
        target_dict: Optional[Dict[str, str]] = None
        if bracket_key.startswith('title_'):
            target_dict = listing.title
        elif bracket_key.startswith('specs_'):
            target_dict = listing.specifics
        elif bracket_key.startswith('meta_'):
            target_dict = listing.metadata
        elif bracket_key.startswith('desc_'):
            target_dict = listing.description
        elif bracket_key.startswith('category_'):
            target_dict = listing.category
        elif bracket_key.startswith('table_'):
            if current_section == 'TABLE DATA':
                if in_shared_values:
                    target_dict = listing.table_shared
                else:
                    if current_entry is None:
                        current_entry = {}
                        listing.table_data.append(current_entry)
                    target_dict = current_entry
            else:
                # In case table lines appear outside TABLE DATA, store as shared
                target_dict = listing.table_shared
        else:
            # Fallback: try to infer from current section
            section_map = {
                'TITLE DATA': listing.title,
                'SPECIFICS': listing.specifics,
                'METADATA': listing.metadata,
                'DESCRIPTION': listing.description,
                'CATEGORY': listing.category,
                'TABLE DATA': listing.table_shared if in_shared_values else (current_entry or listing.table_shared),
            }
            target_dict = section_map.get(current_section)

        if target_dict is not None:
            # Preserve the original bracket key to match how other modules expect keys
            target_dict[bracket_key] = value

    return listing, sections


def read_many_from_dir(directory: str | Path) -> Dict[str, Tuple[ListingData, ParsedSections]]:
    """
    Load all python_parsed_*.txt files in a directory.
    Returns a dict keyed by item number (string) to (ListingData, Sections).
    """
    base = Path(directory)
    results: Dict[str, Tuple[ListingData, ParsedSections]] = {}
    for fp in sorted(base.glob('python_parsed_*.txt')):
        item_number = fp.name.replace('python_parsed_', '').replace('.txt', '')
        try:
            listing, sections = read_parsed_txt(fp)
            results[item_number] = (listing, sections)
        except Exception:
            # Skip unreadable files; callers can log details
            continue
    return results


def read_many_from_backups(backups_root: str | Path) -> Dict[str, Tuple[ListingData, ParsedSections]]:
    """
    Recursively scan backups/itemcontents/** directories for python_parsed_*.txt files
    and load them. Later (current dir) data should take precedence, so callers can merge
    with current first then fill missing from backups.
    """
    root = Path(backups_root)
    results: Dict[str, Tuple[ListingData, ParsedSections]] = {}
    if not root.exists():
        return results

    # Common layout used by scan_monitor: backups/itemcontents/item_contents_backup_YYYYMMDD/
    for fp in sorted(root.rglob('python_parsed_*.txt')):
        item_number = fp.name.replace('python_parsed_', '').replace('.txt', '')
        if item_number in results:
            # Keep first encountered (older or earlier in sort) to reduce churn; caller can override
            continue
        try:
            listing, sections = read_parsed_txt(fp)
            results[item_number] = (listing, sections)
        except Exception:
            continue
    return results


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Parsed TXT reader utility')
    ap.add_argument('path', nargs='?', default='item_contents', help='Directory or file path to read')
    ap.add_argument('--json', dest='as_json', action='store_true', help='Print JSON summary')
    args = ap.parse_args()

    p = Path(args.path)
    if p.is_file():
        listing, _ = read_parsed_txt(p)
        summary = {
            'title_keys': list(listing.title.keys()),
            'specifics_keys': list(listing.specifics.keys()),
            'metadata_keys': list(listing.metadata.keys()),
            'description_keys': list(listing.description.keys()),
            'table_shared_keys': list(listing.table_shared.keys()),
            'table_entries': len(listing.table_data),
        }
        print(json.dumps(summary, indent=2) if args.as_json else summary)
    else:
        data = read_many_from_dir(p)
        print(json.dumps({k: {'entries': len(v[0].table_data)} for k, v in data.items()}, indent=2) if args.as_json else f"Loaded {len(data)} items")


if __name__ == '__main__':
    main()


