Cursor Invocation Rules for Local Agents
=======================================

- Prefer dry-run first. Always pass non-interactive flags.
- When editing multiple files, use `file_edit_agent.py` with `--include` globs.
- For moves/renames, use `folder_agent.py` with `--rename` or `--move`.
- For orchestrated multi-step tasks, create a plan JSON and invoke `ai_root.py`.

Examples (PowerShell):

```powershell
# Dry-run replace across Python files
python .\tools\ai_agents\file_edit_agent.py --root . --include "**/*.py" --pattern "foo" --replacement "bar" --dry-run | cat

# Apply a planned multi-step operation
python .\tools\ai_agents\ai_root.py --plan .\tools\ai_agents\plans\example_plan.json | cat

# Print learned prefs and absorbed data
python .\tools\ai_agents\ai_root.py --print-prefs | cat
python .\tools\ai_agents\ai_root.py --print-absorbed | cat
```


