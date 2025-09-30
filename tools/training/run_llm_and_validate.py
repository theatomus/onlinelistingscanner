from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Iterable

def ensure_requests():
    try:
        import requests  # type: ignore
        return requests
    except Exception:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
        import requests  # type: ignore
        return requests


def tcp_wait(host: str, port: int, timeout_s: int) -> bool:
    end = time.time() + timeout_s
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def find_llama_server(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    # Try PATH
    from shutil import which
    w = which('llama-server.exe') or which('llama-server')
    if w:
        return Path(w)
    # Try common relative spots
    candidates = [
        Path.cwd() / 'llama-server.exe',
        Path.cwd() / 'llama-server',
        Path.cwd().parent / 'llama.cpp' / 'llama-server.exe',
        Path.cwd().parent / 'llama.cpp' / 'llama-server',
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def find_model(prefer: Optional[str], only_phi: bool) -> tuple[Optional[Path], int]:
    models = Path('models')
    choices: list[Path] = []
    # Always try Phi-3 first
    choices.append(models / 'phi-3-mini-4k-instruct-q4_k_m.gguf')
    if not only_phi:
        # Then configured preference
        if prefer == 'llama1b':
            choices.append(models / 'Llama-3.2-1B-Instruct-Q4_K_M.gguf')
        # Reasonable fallbacks
        choices.extend([
            models / 'Llama-3.2-1B-Instruct-Q4_K_M.gguf',
            models / 'Qwen2.5-0.5B-Instruct-Q4_K_M.gguf',
            models / 'tinyllama-1.1b-chat-v1.0-q4_k_m.gguf',
        ])
    seen = set()
    ordered = [c for c in choices if not (str(c) in seen or seen.add(str(c)))]
    for c in ordered:
        if c.exists():
            # Port convention: Phi3=8080, Llama1B=8081, Qwen0.5B=8082, Tiny=8083
            name = c.name.lower()
            port = 8080 if 'phi-3' in name else 8081 if 'llama-3.2-1b' in name else 8082 if 'qwen2.5-0.5b' in name else 8083
            return c, port
    return None, 0


def start_server(server: Path, model: Path, port: int, ctx: int = 2048, threads: Optional[int] = None) -> subprocess.Popen:
    if threads is None:
        try:
            threads = os.cpu_count() or 8
        except Exception:
            threads = 8
    args = [
        str(server),
        '-m', str(model),
        '-c', str(ctx),
        '-t', str(threads),
        '-ngl', '0',
        '-p', str(port),
    ]
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def health_check(base_url: str) -> bool:
    requests = ensure_requests()
    try:
        body = {
            'model': 'local',
            'messages': [
                {'role': 'system', 'content': 'Return JSON only: {"ok":true}'},
                {'role': 'user', 'content': 'ping'}
            ],
            'response_format': {'type': 'json_object'},
            'temperature': 0.1,
            'max_tokens': 16
        }
        r = requests.post(base_url.rstrip('/') + '/v1/chat/completions', json=body, timeout=10)
        if not r.ok:
            return False
        return '"ok":true' in r.text or '"ok": true' in r.text
    except Exception:
        return False


def ensure_training(items_dir: Path, backups_root: Path, training_dir: Path) -> None:
    schema = training_dir / 'schema.json'
    if schema.exists():
        return
    print('[setup] Building training dataset + schema + mini-LM...')
    subprocess.check_call([sys.executable, '-m', 'tools.training.training_data_builder', '--items-dir', str(items_dir), '--backups-root', str(backups_root), '--out', str(training_dir / 'training_dataset.json')])
    subprocess.check_call([sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', str(items_dir), '--backups-root', str(backups_root), '--out-dir', str(training_dir)])


def pick_item(items_dir: Path, explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    for p in sorted(items_dir.glob('python_parsed_*.txt')):
        return p
    return None


def iter_items(items_dir: Path) -> Iterable[Path]:
    return sorted(items_dir.glob('python_parsed_*.txt'))


def find_open_port(preferred: int) -> int:
    # Try preferred, then next 20 ports
    for p in range(preferred, preferred + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', p))
                return p
            except OSError:
                continue
    return preferred


def main():
    ap = argparse.ArgumentParser(description='Start local llama.cpp server (prefers Phi-3), health-check, ensure training, validate items, write AI issues.')
    ap.add_argument('--server-path', default=None, help='Path to llama-server.exe if not on PATH')
    ap.add_argument('--prefer', choices=['phi3','llama1b'], default='phi3', help='Prefer a specific small model if present')
    ap.add_argument('--port', type=int, default=None, help='Override port')
    ap.add_argument('--item', default=None, help='Path to python_parsed_*.txt (defaults to first in item_contents)')
    ap.add_argument('--write-issues', action='store_true', help='Also write training/live_issues/ITEM.txt')
    ap.add_argument('--no-web', action='store_true', help='Disable web verification step')
    ap.add_argument('--all', action='store_true', help='Validate all items in item_contents and write issues for each')
    ap.add_argument('--only-phi', action='store_true', help='Use only Phi-3 model (no fallback)')
    args = ap.parse_args()

    items_dir = Path('item_contents')
    backups_root = Path('backups') / 'itemcontents'
    training_dir = Path('training')

    server = find_llama_server(args.server_path)
    if not server:
        print('ERROR: llama-server(.exe) not found. Install llama.cpp and provide --server-path if needed.')
        return 1
    model, default_port = find_model(args.prefer, args.only_phi)
    if not model:
        print('ERROR: No model found in models/. Expected one of: phi-3-mini-4k-instruct-q4_k_m.gguf, Llama-3.2-1B-Instruct-Q4_K_M.gguf, Qwen2.5-0.5B-Instruct-Q4_K_M.gguf, tinyllama-1.1b-chat-v1.0-q4_k_m.gguf')
        return 1

    preferred_port = args.port or default_port
    port = find_open_port(preferred_port)
    print(f'[info] Using server: {server}')
    print(f'[info] Using model:  {model}')
    print(f'[info] Port:         {port}')

    # Start server
    proc = start_server(server, model, port)
    if not tcp_wait('127.0.0.1', port, timeout_s=60):
        print('ERROR: LLM server not ready after 60s')
        try:
            proc.terminate()
        except Exception:
            pass
        return 1
    base_url = f'http://127.0.0.1:{port}'
    print('[ok] Server listening')

    if not health_check(base_url):
        print('ERROR: Health check failed')
        return 1
    print('[ok] Health check passed')

    # Ensure training artifacts exist
    ensure_training(items_dir, backups_root, training_dir)

    if args.all:
        wrote = 0
        for item in iter_items(items_dir):
            print(f'[info] Validating: {item.name}')
            cmd = [
                sys.executable, '-m', 'tools.training.ai_validator', 'validate', str(item),
                '--schema', str(training_dir / 'schema.json'),
                '--lm', str(training_dir / 'mini_lm.json'),
                '--llm-url', base_url,
            ]
            if not args.no_web:
                # Web verify disabled globally
            out = subprocess.check_output(cmd, text=True, errors='ignore')
            for line in out.splitlines():
                if any(k in line for k in ['Unexpected key','Normalize','Suggest','WEB','Device type','Category mismatch']):
                    print('  -', line)
            if args.write_issues:
                item_num = item.stem.replace('python_parsed_','')
                cmd2 = [
                    sys.executable, '-m', 'tools.training.write_ai_issues', item_num,
                    '--items-dir', str(items_dir),
                    '--schema', str(training_dir / 'schema.json'),
                    '--lm', str(training_dir / 'mini_lm.json'),
                    '--llm-url', base_url,
                ]
                if not args.no_web:
                    # Web verify disabled globally
                subprocess.check_call(cmd2)
                wrote += 1
        if args.write_issues:
            print(f'[ok] Wrote {wrote} issue files to training/live_issues')
    else:
        # Single item
        item = pick_item(items_dir, args.item)
        if not item:
            print('ERROR: No python_parsed_*.txt found in item_contents (or provided item not found).')
            return 1
        print(f'[info] Validating: {item.name}')
        cmd = [
            sys.executable, '-m', 'tools.training.ai_validator', 'validate', str(item),
            '--schema', str(training_dir / 'schema.json'),
            '--lm', str(training_dir / 'mini_lm.json'),
            '--llm-url', base_url,
        ]
        if not args.no_web:
            # Web verify disabled globally
        out = subprocess.check_output(cmd, text=True, errors='ignore')
        print('\n=== VALIDATION OUTPUT (filtered) ===')
        for line in out.splitlines():
            if any(k in line for k in ['Unexpected key','Normalize','Suggest','WEB','Device type','Category mismatch']) or 'No schema issues' in line:
                print(line)
        if args.write_issues:
            item_num = item.stem.replace('python_parsed_','')
            cmd2 = [
                sys.executable, '-m', 'tools.training.write_ai_issues', item_num,
                '--items-dir', str(items_dir),
                '--schema', str(training_dir / 'schema.json'),
                '--lm', str(training_dir / 'mini_lm.json'),
                '--llm-url', base_url,
            ]
            if not args.no_web:
                cmd2.append('--web-verify')
            subprocess.check_call(cmd2)
            out_file = Path('training') / 'live_issues' / f'{item_num}.txt'
            print(f'\n[ok] Wrote: {out_file}')

    print('\n[done] All steps completed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


