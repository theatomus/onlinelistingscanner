from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List

from .common import atomic_write_text, backup_file, find_paths, json_print


def replace_in_text(text: str, pattern: str, replacement: str, regex: bool) -> tuple[str, int]:
    if regex:
        new_text, count = re.subn(pattern, replacement, text)
    else:
        count = text.count(pattern)
        new_text = text.replace(pattern, replacement)
    return new_text, count


def ensure_line(text: str, line: str) -> tuple[str, int]:
    if line in text:
        return text, 0
    if text and not text.endswith('\n'):
        text += '\n'
    return text + line + '\n', 1


def process_files(root: Path, include: List[str], pattern: str | None, replacement: str | None, regex: bool, ensure: str | None, dry_run: bool) -> dict:
    targets = find_paths(root, include)
    modified = 0
    mods = 0
    details = []
    for fp in targets:
        if not fp.is_file():
            continue
        text = fp.read_text(encoding='utf-8', errors='ignore')
        before = text
        file_mods = 0
        if pattern is not None and replacement is not None:
            text, c = replace_in_text(text, pattern, replacement, regex)
            file_mods += c
        if ensure is not None:
            text, c = ensure_line(text, ensure)
            file_mods += c
        if file_mods > 0 and text != before:
            modified += 1
            mods += file_mods
            if not dry_run:
                backup_file(fp)
                atomic_write_text(fp, text)
        if file_mods:
            details.append({'file': str(fp), 'changes': file_mods})
    return {
        'files_examined': len(targets),
        'files_modified': modified,
        'modifications': mods,
        'details': details,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description='File Edit Agent')
    ap.add_argument('--root', default='.')
    ap.add_argument('--include', action='append', required=True, help='Glob(s) to include (repeatable)')
    ap.add_argument('--pattern')
    ap.add_argument('--replacement')
    ap.add_argument('--regex', action='store_true')
    ap.add_argument('--ensure', help='Ensure a line exists in each file')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if (args.pattern is None) ^ (args.replacement is None):
        ap.error('--pattern and --replacement must be used together')

    result = process_files(Path(args.root), args.include, args.pattern, args.replacement, args.regex, args.ensure, args.dry_run)
    json_print(result)


if __name__ == '__main__':
    main()


