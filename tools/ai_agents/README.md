Local AI Agents Toolkit
=======================

A small suite of localized file/folder/script editing agents and an orchestrator for safe, auditable workspace maintenance.

- Designed to be run via PowerShell with non-interactive flags
- Atomic writes, dry-run mode, JSON summaries
- Lives under `tools/ai_agents/` to avoid cluttering the root

Components
----------

- `common.py`: Shared utilities (atomic write, backups, file finding)
- `file_edit_agent.py`: Search/replace, ensure line, multi-file edits
- `folder_agent.py`: Move/rename files by glob/regex, dry-run first
- `ai_root.py`: Orchestrator that runs agent plans from JSON
- `cursor_rules.md`: How Cursor should invoke these agents
 - `state/`: Stores learned preferences, run history, absorbed analytics

Quick start (PowerShell)
------------------------

Dry-run a replace across Python files, printing a JSON summary:

```powershell
python .\tools\ai_agents\file_edit_agent.py --root . --include "**/*.py" --replace "foo" "bar" --dry-run | cat
```

Apply a folder rename by regex:

```powershell
python .\tools\ai_agents\folder_agent.py --root . --rename --pattern "^(.*)old(.*)$" --replacement "$1new$2" --include "**/*" | cat
```

Run a multi-step plan:

```powershell
python .\tools\ai_agents\ai_root.py --plan .\tools\ai_agents\plans\example_plan.json --dry-run | cat
```

Each agent supports `--dry-run` and prints a concise JSON result to stdout.

Insights and preferences
------------------------

Print preferences or absorbed analytics without a plan:

```powershell
python .\tools\ai_agents\ai_root.py --print-prefs | cat
python .\tools\ai_agents\ai_root.py --print-absorbed | cat
python .\tools\ai_agents\ai_root.py --reset-prefs | cat
```


