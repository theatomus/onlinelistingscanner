from __future__ import annotations

import sys
from pathlib import Path


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return path.read_text(errors='replace')


def print_item(root: Path, item: str) -> None:
    desc = root / f"{item}_description.txt"
    py = root / f"python_parsed_{item}.txt"
    ai = root / f"ai_parsed_{item}.txt"
    ai_legacy = root / f"ai_python_parsed_{item}.txt"
    print(f"===== PRINT ITEM {item} =====")
    for p, label in [
        (desc, 'DESCRIPTION'),
        (py, 'PYTHON_PARSED'),
        (ai if ai.exists() else ai_legacy, 'AI_PARSED'),
    ]:
        print(f"----- {label}: {p}")
        if p.exists():
            print(read_text(p))
        else:
            print(f"[missing] {p}")
    print(f"===== END ITEM {item} =====")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python tools/training/print_items.py <item> [<item2> ...] [--items-dir item_contents]")
        return 1
    # Default items dir
    items_dir = Path('item_contents')
    items: list[str] = []
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--items-dir' and (i + 1) < len(args):
            items_dir = Path(args[i + 1])
            i += 2
            continue
        items.append(args[i])
        i += 1
    for item in items:
        print_item(items_dir, item)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


