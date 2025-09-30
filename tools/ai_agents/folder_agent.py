from __future__ import annotations

import argparse
import os
import re
import shutil
from pathlib import Path
from typing import List

from .common import find_paths, json_print


def rename_paths(root: Path, include: List[str], pattern: str, replacement: str, dry_run: bool) -> dict:
    rx = re.compile(pattern)
    targets = find_paths(root, include)
    details = []
    moved = 0
    for fp in targets:
        new_name = rx.sub(replacement, fp.name)
        if new_name != fp.name:
            dest = fp.with_name(new_name)
            details.append({'from': str(fp), 'to': str(dest)})
            moved += 1
            if not dry_run:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(fp), str(dest))
    return {'examined': len(targets), 'moved': moved, 'details': details}


def move_by_glob(root: Path, include: List[str], destination: str, dry_run: bool) -> dict:
    dest = Path(destination)
    targets = find_paths(root, include)
    details = []
    moved = 0
    for fp in targets:
        rel = fp.relative_to(root)
        dest_path = dest / rel.name
        details.append({'from': str(fp), 'to': str(dest_path)})
        moved += 1
        if not dry_run:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(fp), str(dest_path))
    return {'examined': len(targets), 'moved': moved, 'details': details}


def main() -> None:
    ap = argparse.ArgumentParser(description='Folder Management Agent')
    ap.add_argument('--root', default='.')
    ap.add_argument('--include', action='append', required=True)
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--rename', action='store_true', help='Rename by regex')
    grp.add_argument('--move', metavar='DEST', help='Move matched files to DEST')
    ap.add_argument('--pattern')
    ap.add_argument('--replacement')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if args.rename:
        if not args.pattern or args.replacement is None:
            ap.error('--rename requires --pattern and --replacement')
        result = rename_paths(Path(args.root), args.include, args.pattern, args.replacement, args.dry_run)
    else:
        result = move_by_glob(Path(args.root), args.include, args.move, args.dry_run)
    json_print(result)


if __name__ == '__main__':
    main()


