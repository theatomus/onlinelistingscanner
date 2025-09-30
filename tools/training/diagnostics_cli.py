from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Tuple


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return path.read_text(errors='replace')


def print_item(files_root: Path, item: str) -> int:
    desc = files_root / f"{item}_description.txt"
    py = files_root / f"python_parsed_{item}.txt"
    ai = files_root / f"ai_parsed_{item}.txt"
    ai_legacy = files_root / f"ai_python_parsed_{item}.txt"
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
    return 0


def run(cmd: List[str], cwd: Path) -> int:
    print(f"[run] {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line.rstrip())
    rc = proc.wait()
    print(f"[done] rc={rc}")
    return rc


def rinse_once(
    repo_cwd: Path,
    items_dir: Path,
    backups_root: Path,
    training_dir: Path,
    item: str,
    llm_url: str | None,
    overwrite: bool,
) -> int:
    rc = 0
    # 1) instructions_refiner
    rc |= run([sys.executable, '-m', 'tools.training.instructions_refiner', '--items-dir', str(items_dir), '--backups-root', str(backups_root), '--instructions', str(training_dir / 'config' / 'instructions.yaml')], repo_cwd)
    # 2) corrections_merger
    merge = [
        sys.executable, '-m', 'tools.training.corrections_merger',
        '--items-dir', str(items_dir), '--backups-root', str(backups_root),
        '--corrections-dir', str(training_dir / 'corrections'),
        '--schema', str(training_dir / 'schema.json'),
        '--gold-values', str(training_dir / 'gold_values.json'),
        '--dataset', str(training_dir / 'training_dataset.json'),
        '--mini-lm-out', str(training_dir / 'mini_lm.json'),
    ]
    if overwrite:
        merge.append('--overwrite')
    rc |= run(merge, repo_cwd)
    # 3) run_workflow
    rc |= run([sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', str(items_dir), '--backups-root', str(backups_root), '--out-dir', str(training_dir)], repo_cwd)
    # 4) generate ai_parsed for item
    gen = [sys.executable, '-m', 'tools.training.ai_python_parsed', item, '--items-dir', str(items_dir)]
    if llm_url:
        gen += ['--llm-url', llm_url]
    rc |= run(gen, repo_cwd)
    # 5) print files
    print_item(items_dir, item)
    return rc


def title_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for line in read_text(path).splitlines():
        m = re.match(r"^\[(title_[^\]]+)\]", line)
        if m:
            keys.add(m.group(1).lower())
    return keys


def diff_title_keys(items_dir: Path, limit: int = 10) -> int:
    missing_counts: Dict[str, int] = {}
    item_missing: Dict[str, List[str]] = {}
    for pp in sorted(items_dir.glob('python_parsed_*.txt')):
        item = pp.stem.replace('python_parsed_', '')
        ap = items_dir / f"ai_parsed_{item}.txt"
        if not ap.exists():
            continue
        miss = sorted(title_keys(pp) - title_keys(ap))
        if miss:
            item_missing[item] = miss
            for k in miss:
                missing_counts[k] = missing_counts.get(k, 0) + 1
    print('=== Top missing title keys (count) ===')
    for k, c in sorted(missing_counts.items(), key=lambda kv: kv[1], reverse=True)[:15]:
        print(f'{k} = {c}')
    print('\n=== Sample items with missing title keys ===')
    for i, (iid, miss) in enumerate(item_missing.items()):
        if i >= limit:
            break
        print(f"{iid}: {', '.join(miss)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description='Diagnostics CLI for printing items and iterative retraining')
    ap.add_argument('--cwd', default='.', help='Repository root working directory')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--training-dir', default='training')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    ap.add_argument('--llm-url', default=None)
    sub = ap.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('print-item')
    p.add_argument('item')

    g = sub.add_parser('gen-item')
    g.add_argument('item')

    r = sub.add_parser('rinse')
    r.add_argument('item')
    r.add_argument('--iterations', type=int, default=1)
    r.add_argument('--overwrite', action='store_true')

    d = sub.add_parser('diff-title-keys')
    d.add_argument('--limit', type=int, default=10)

    args = ap.parse_args()
    repo_cwd = Path(args.cwd).resolve()
    items_dir = (repo_cwd / args.items_dir).resolve()
    training_dir = (repo_cwd / args.training_dir).resolve()
    backups_root = (repo_cwd / args.backups_root).resolve()

    if args.cmd == 'print-item':
        return print_item(items_dir, args.item)
    if args.cmd == 'gen-item':
        cmd = [sys.executable, '-m', 'tools.training.ai_python_parsed', args.item, '--items-dir', str(items_dir)]
        if args.llm_url:
            cmd += ['--llm-url', args.llm_url]
        return run(cmd, repo_cwd)
    if args.cmd == 'rinse':
        rc = 0
        for _i in range(int(args.iterations)):
            rc |= rinse_once(repo_cwd, items_dir, backups_root, training_dir, args.item, args.llm_url, bool(args.overwrite))
        return rc
    if args.cmd == 'diff-title-keys':
        return diff_title_keys(items_dir, args.limit)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


