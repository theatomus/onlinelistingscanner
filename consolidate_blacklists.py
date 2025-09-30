import os
from pathlib import Path

# Prefer state-based canonical path; ensure parent exists at write time
CANONICAL = Path('state') / 'processed_items_blacklist.txt'
SEARCH_NAMES = {
    'processed_items_blacklist.txt',
    'blacklist.txt',
    'blacklist.json',
}

def find_blacklist_files(root: Path) -> list[Path]:
    candidates: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            lname = name.lower()
            if 'blacklist' in lname and lname.endswith(('.txt', '.json')):
                p = Path(dirpath) / name
                # Skip our canonical target temp files
                if p.name.endswith('.tmp'):
                    continue
                candidates.append(p)
                # Also include legacy root canonical explicitly if present
                legacy = Path('processed_items_blacklist.txt')
                if legacy.exists():
                    candidates.append(legacy)
    return candidates

def read_items_from_file(path: Path) -> set[str]:
    items: set[str] = set()
    try:
        if path.suffix.lower() == '.json':
            import json
            with path.open('r', encoding='utf-8') as f:
                data = json.load(f)
            # Accept lists or sets of strings
            if isinstance(data, dict) and 'items' in data and isinstance(data['items'], (list, set)):
                items.update(str(x).strip() for x in data['items'] if str(x).strip())
            elif isinstance(data, (list, set)):
                items.update(str(x).strip() for x in data if str(x).strip())
        else:
            with path.open('r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    v = line.strip()
                    if v:
                        items.add(v)
    except Exception:
        pass
    return items

def atomic_write_lines(path: Path, lines: list[str]) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open('w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    os.replace(tmp, path)

def main() -> None:
    root = Path('.')
    all_files = find_blacklist_files(root)
    # Ensure we include canonical if present
    if CANONICAL.exists():
        all_files.append(CANONICAL)

    merged: set[str] = set()
    for fp in all_files:
        if not fp.exists():
            continue
        merged.update(read_items_from_file(fp))

    if not merged:
        print('No blacklist entries found; leaving canonical file unchanged.')
        return

    items_sorted = sorted(merged)
    atomic_write_lines(CANONICAL, items_sorted)
    print(f'Merged {len(all_files)} files into {CANONICAL} with {len(items_sorted)} unique items.')

if __name__ == '__main__':
    main()


