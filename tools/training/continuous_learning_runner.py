from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
import time


def run(cmd: list[str]) -> int:
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line.rstrip())
        return proc.wait()
    except Exception as e:
        print(f"[error] {e}")
        return 1


def main():
    ap = argparse.ArgumentParser(description='Run continuous learning steps in background with a lock')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    ap.add_argument('--training-dir', default='training')
    ap.add_argument('--instructions', default='training/config/instructions.yaml')
    args = ap.parse_args()

    training_dir = Path(args.training_dir)
    locks = training_dir / 'locks'
    locks.mkdir(parents=True, exist_ok=True)
    lock_file = locks / 'learning.lock'

    if lock_file.exists():
        print('[skip] continuous learning already running')
        return 0

    try:
        lock_file.write_text(str(time.time()))
    except Exception:
        pass

    try:
        # 1) Refine instructions label maps from corpus
        rc = run([sys.executable, '-m', 'tools.training.instructions_refiner', '--items-dir', args.items_dir, '--backups-root', args.backups_root, '--instructions', args.instructions])
        print(f"[refiner] rc={rc}")
        # 2) Merge corrections and rebuild mini-LM
        rc = run([sys.executable, '-m', 'tools.training.corrections_merger', '--items-dir', args.items_dir, '--backups-root', args.backups_root, '--corrections-dir', str(Path(args.training_dir) / 'corrections'), '--schema', str(Path(args.training_dir) / 'schema.json'), '--gold-values', str(Path(args.training_dir) / 'gold_values.json'), '--dataset', str(Path(args.training_dir) / 'training_dataset.json'), '--mini-lm-out', str(Path(args.training_dir) / 'mini_lm.json')])
        print(f"[merge] rc={rc}")
        # 3) Run workflow (schema + mini‑LM + validation)
        rc = run([sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', args.items_dir, '--backups-root', args.backups_root, '--out-dir', args.training_dir])
        print(f"[workflow] rc={rc}")
    finally:
        try:
            lock_file.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


