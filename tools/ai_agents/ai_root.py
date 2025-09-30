from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from collections import Counter
from datetime import datetime

from .common import read_json, write_json, atomic_write_text


def run_agent(cmd: list[str], dry_run: bool) -> dict:
    if dry_run:
        return {'cmd': cmd, 'dry_run': True}
    out = subprocess.run(cmd, capture_output=True, text=True)
    return {
        'cmd': cmd,
        'returncode': out.returncode,
        'stdout': out.stdout.strip(),
        'stderr': out.stderr.strip(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description='Universal AI Orchestrator')
    ap.add_argument('--plan', required=False, help='Path to JSON plan file')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--print-prefs', action='store_true', help='Print learned preferences and exit')
    ap.add_argument('--reset-prefs', action='store_true', help='Reset learned preferences and exit')
    ap.add_argument('--print-absorbed', action='store_true', help='Print absorbed summary and exit')
    args = ap.parse_args()

    state_dir = Path(__file__).with_name('state')
    prefs_path = state_dir / 'preferences.json'
    history_path = state_dir / 'history.json'
    state_dir.mkdir(parents=True, exist_ok=True)

    # Preferences schema: { "file_edit": {"include": ["**/*.py"], ... }, "folder": {...} }
    prefs = read_json(prefs_path, default={})
    history = read_json(history_path, default={'runs': []})

    if args.print_prefs:
        print(json.dumps({'preferences': prefs}, ensure_ascii=False, indent=2))
        return
    if args.reset_prefs:
        write_json(prefs_path, {})
        write_json(history_path, {'runs': []})
        print(json.dumps({'reset': True}, ensure_ascii=False))
        return
    if args.print_absorbed:
        summary_path = state_dir / 'absorbed_summary.txt'
        if summary_path.exists():
            print(summary_path.read_text(encoding='utf-8'))
        else:
            analytics_path = state_dir / 'analytics.json'
            analytics = read_json(analytics_path, default={})
            print(json.dumps({'analytics': analytics}, ensure_ascii=False, indent=2))
        return

    if not args.plan:
        print(json.dumps({'error': 'No plan provided. Use --plan or an info flag like --print-prefs/--print-absorbed.'}, ensure_ascii=False))
        return

    plan_path = Path(args.plan)
    plan = json.loads(plan_path.read_text(encoding='utf-8'))

    results = []
    for step in plan.get('steps', []):
        agent = step['agent']  # 'file_edit' | 'folder' | 'shell'
        if agent == 'file_edit':
            cmd = [sys.executable, str(Path(__file__).with_name('file_edit_agent.py'))]
            cmd += ['--root', step.get('root', '.')]
            include = step.get('include') or prefs.get('file_edit', {}).get('include', [])
            for inc in include:
                cmd += ['--include', inc]
            if 'pattern' in step and 'replacement' in step:
                cmd += ['--pattern', step['pattern'], '--replacement', step['replacement']]
            if step.get('regex'):
                cmd += ['--regex']
            if 'ensure' in step:
                cmd += ['--ensure', step['ensure']]
            if args.dry_run or step.get('dry_run'):
                cmd += ['--dry-run']
            results.append(run_agent(cmd, dry_run=False))
        elif agent == 'folder':
            cmd = [sys.executable, str(Path(__file__).with_name('folder_agent.py'))]
            cmd += ['--root', step.get('root', '.')]
            include = step.get('include') or prefs.get('folder', {}).get('include', [])
            for inc in include:
                cmd += ['--include', inc]
            if 'move' in step:
                cmd += ['--move', step['move']]
            elif 'pattern' in step and 'replacement' in step:
                cmd += ['--rename', '--pattern', step['pattern'], '--replacement', step['replacement']]
            if args.dry_run or step.get('dry_run'):
                cmd += ['--dry-run']
            results.append(run_agent(cmd, dry_run=False))
        elif agent == 'python_ast':
            cmd = [sys.executable, str(Path(__file__).with_name('python_ast_agent.py'))]
            cmd += ['--root', step.get('root', '.')]
            include = step.get('include') or prefs.get('python_ast', {}).get('include', ['**/*.py'])
            for inc in include:
                cmd += ['--include', inc]
            if 'rename_symbol' in step:
                old, new = step['rename_symbol']
                cmd += ['--rename-symbol', old, new]
            if args.dry_run or step.get('dry_run'):
                cmd += ['--dry-run']
            results.append(run_agent(cmd, dry_run=False))
        elif agent == 'shell':
            cmd = step['cmd']
            results.append(run_agent(cmd, dry_run=args.dry_run or step.get('dry_run', False)))
        else:
            results.append({'error': f'Unknown agent: {agent}'})

    # Persist simple learning: last include globs per agent and run history
    for step in plan.get('steps', []):
        agent = step['agent']
        if agent in ('file_edit', 'folder'):
            prefs.setdefault(agent, {})
            if 'include' in step and step['include']:
                prefs[agent]['include'] = step['include']
        if agent == 'python_ast':
            prefs.setdefault(agent, {})
            if 'include' in step and step['include']:
                prefs[agent]['include'] = step['include']
    write_json(prefs_path, prefs)

    history['runs'].append({
        'plan': str(plan_path),
        'results': results,
    })
    write_json(history_path, history)

    # Update absorbed analytics summary
    analytics_path = state_dir / 'analytics.json'
    analytics = read_json(analytics_path, default={'file_types_edits': {}, 'patterns': {}, 'includes': {}, 'runs': 0, 'last_updated': ''})

    file_types_counter = Counter(analytics.get('file_types_edits', {}))
    patterns_counter = Counter(analytics.get('patterns', {}))
    includes_counter = Counter(analytics.get('includes', {}))

    for step, res in zip(plan.get('steps', []), results):
        if step.get('agent') != 'file_edit':
            continue
        # Count includes used
        used_includes = step.get('include') or prefs.get('file_edit', {}).get('include', [])
        for inc in used_includes:
            includes_counter[inc] += 1
        # Count patterns
        if 'pattern' in step and step['pattern']:
            patterns_counter[step['pattern']] += 1
        # Parse agent stdout JSON
        try:
            payload = json.loads(res.get('stdout') or '{}')
            for d in payload.get('details', []):
                file_path = d.get('file')
                changes = int(d.get('changes', 0))
                if file_path and changes:
                    ext = Path(file_path).suffix.lower().lstrip('.') or 'noext'
                    file_types_counter[ext] += changes
        except Exception:
            pass

    analytics['file_types_edits'] = dict(file_types_counter)
    analytics['patterns'] = dict(patterns_counter)
    analytics['includes'] = dict(includes_counter)
    analytics['runs'] = int(analytics.get('runs', 0)) + 1
    analytics['last_updated'] = datetime.utcnow().isoformat() + 'Z'
    write_json(analytics_path, analytics)

    # Write compact human summary
    lines = []
    lines.append('Absorbed Data Summary')
    lines.append(f"Runs: {analytics['runs']}")
    # Top 10 file types
    top_types = Counter(analytics['file_types_edits']).most_common(10)
    if top_types:
        lines.append('Top file types (edits):')
        for ext, cnt in top_types:
            lines.append(f"- {ext}: {cnt}")
    # Top 10 patterns
    top_patterns = Counter(analytics['patterns']).most_common(10)
    if top_patterns:
        lines.append('Top patterns:')
        for pat, cnt in top_patterns:
            # Truncate long patterns for readability
            disp = pat if len(pat) <= 80 else pat[:77] + '...'
            lines.append(f"- {disp}: {cnt}")
    lines.append(f"Last updated: {analytics['last_updated']}")
    summary_path = state_dir / 'absorbed_summary.txt'
    atomic_write_text(summary_path, '\n'.join(lines) + '\n')

    print(json.dumps({'results': results}, ensure_ascii=False))


if __name__ == '__main__':
    main()


