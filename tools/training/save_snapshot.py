from __future__ import annotations

import sys
from pathlib import Path


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return path.read_text(errors='replace')


def write_snapshot(items_dir: Path, out_dir: Path, item: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"item_{item}_snapshot.txt"
    desc = items_dir / f"{item}_description.txt"
    py = items_dir / f"python_parsed_{item}.txt"
    ai = items_dir / f"ai_parsed_{item}.txt"
    if not ai.exists():
        ai = items_dir / f"ai_python_parsed_{item}.txt"
    lines = [f"===== PRINT ITEM {item} =====\n"]
    for p, label in [
        (desc, 'DESCRIPTION'),
        (py, 'PYTHON_PARSED'),
        (ai, 'AI_PARSED'),
    ]:
        lines.append(f"----- {label}: {p}\n")
        if p.exists():
            lines.append(read_text(p) + "\n")
        else:
            lines.append(f"[missing] {p}\n")
    lines.append(f"===== END ITEM {item} =====\n")
    out_path.write_text(''.join(lines), encoding='utf-8')
    return out_path


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python tools/training/save_snapshot.py <item> [<item2> ...] [--items-dir item_contents] [--out-dir training/diagnostics]")
        return 1
    items_dir = Path('item_contents')
    out_dir = Path('training/diagnostics')
    items: list[str] = []
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--items-dir' and (i + 1) < len(args):
            items_dir = Path(args[i + 1])
            i += 2
            continue
        if args[i] == '--out-dir' and (i + 1) < len(args):
            out_dir = Path(args[i + 1])
            i += 2
            continue
        items.append(args[i])
        i += 1
    for item in items:
        p = write_snapshot(items_dir, out_dir, item)
        print(str(p))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


