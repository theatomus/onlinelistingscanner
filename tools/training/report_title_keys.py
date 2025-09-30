from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path


def collect_files(items_dir: Path, backups_root: Path | None) -> list[Path]:
    files: list[Path] = []
    if items_dir.exists():
        files.extend(sorted(items_dir.rglob('python_parsed_*.txt')))
    if backups_root and backups_root.exists():
        files.extend(sorted(backups_root.rglob('python_parsed_*.txt')))
    return files


def main():
    ap = argparse.ArgumentParser(description='Report frequency of title_* keys in python_parsed files')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    ap.add_argument('--out', default='training/reports/title_keys_counts.txt')
    args = ap.parse_args()

    items_dir = Path(args.items_dir)
    backups_root = Path(args.backups_root)
    rx = re.compile(r"^\s*\[(?P<key>(title_[^\]]+_key|meta_title_key))\]\s*")
    counts: Counter[str] = Counter()
    files = collect_files(items_dir, backups_root)
    for fp in files:
        try:
            for line in fp.read_text(encoding='utf-8', errors='replace').splitlines():
                m = rx.match(line)
                if m:
                    counts[m.group('key')] += 1
        except Exception:
            continue

    lines = [f"{counts[k]:6}  {k}" for k in sorted(counts, key=lambda k: counts[k], reverse=True)]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding='utf-8')
    print("\n".join(lines))


if __name__ == '__main__':
    raise SystemExit(main())


