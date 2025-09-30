"""
Directive: Isolate all files used by zscrape / scan monitor

This utility scans the repository to find everything required by:
- scan_monitor.py (Python GUI/daemon)
- zscrape_process_new_auto_shutdown_at_350pm_new.ahk (AHK launcher)

It will:
- Recursively collect local Python dependencies via static import analysis (AST)
- Heuristically collect resource directories/files referenced via os.path.join(BASE_DIR, ...)
- Include the AHK zscrape script and any directly referenced resources
- Copy the discovered files and folders into a destination folder, preserving paths

Notes:
- Default action is copy-only (non-destructive). Use --move to move instead of copy
- You can add extra includes/excludes via CLI flags
- Designed to run from anywhere inside the repo; it finds repo root automatically
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


# -------- Configuration (sweepable) --------
# You can adjust these constants to tune behavior.

ENTRYPOINT_CANDIDATES: Tuple[str, ...] = (
    "scan_monitor.py",
    "zscrape_process_new_auto_shutdown_at_350pm_new.ahk",
    # Seed these because scan monitor monitors/mentions them often
    "runit.py",
    "process_description.py",
)

# Consider these string suffixes as data files worth copying when found as literals
DATA_FILE_SUFFIXES: Tuple[str, ...] = (
    ".txt",
    ".json",
    ".csv",
    ".tsv",
    ".yaml",
    ".yml",
    ".ini",
    ".db",
    ".sqlite",
    ".gz",
    ".tar",
    ".zip",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".ahk",
)

# Directory names commonly defined in scan monitor; always include if discovered
KNOWN_RESOURCE_DIR_NAMES: Tuple[str, ...] = (
    "eBayListingData",
    "reports",
    "backups",
    "logs",
    "state",
)

# Ignore patterns when copying entire folders
DEFAULT_COPY_IGNORES: Tuple[str, ...] = (
    "__pycache__",
    ".git",
    ".hg",
    ".svn",
    "node_modules",
)


def find_repo_root(start: Path) -> Path:
    """Ascend directories until a known repo marker or root-level files are found.

    Heuristic: prefer a directory containing scan_monitor.py.
    Fallback: stop at filesystem root.
    """
    current = start.resolve()
    while True:
        if (current / "scan_monitor.py").exists():
            return current
        parent = current.parent
        if parent == current:
            return current
        current = parent


def normalize_path(path: Path) -> Path:
    try:
        return path.resolve()
    except Exception:
        return path


def resolve_local_module(module_name: str, repo_root: Path) -> Optional[Path]:
    """Resolve a module name to a file within repo_root if possible.

    - Supports dotted modules (e.g., pkg.sub.module)
    - Tries <name>.py and <name>/__init__.py
    """
    parts = module_name.split(".")
    candidate = repo_root.joinpath(*parts)
    py_file = candidate.with_suffix(".py")
    if py_file.exists():
        return normalize_path(py_file)
    init_file = candidate / "__init__.py"
    if init_file.exists():
        return normalize_path(init_file)
    return None


def parse_imports(py_path: Path) -> Set[str]:
    """Parse import statements from a Python file using AST.

    Returns a set of top-level module names (not resolved to paths).
    """
    try:
        text = py_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return set()
    try:
        tree = ast.parse(text)
    except Exception:
        return set()

    modules: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.add(node.module)
    return modules


_OS_JOIN_RE = re.compile(
    r"os\.path\.join\(\s*(?P<base>[A-Za-z_][A-Za-z0-9_]*)\s*,\s*(?P<parts>[^\)]*?)\)",
    re.DOTALL,
)

_STRING_LITERAL_RE = re.compile(r"(['\"])((?:\\.|(?!\1).)*)\1")


def _extract_string_literals(arglist: str) -> List[str]:
    matches = list(_STRING_LITERAL_RE.finditer(arglist))
    values: List[str] = []
    for m in matches:
        values.append(m.group(2))
    return values


def discover_base_vars(py_text: str) -> Set[str]:
    """Find variable names representing the file's base directory.

    Heuristics: look for assignments like:
    - BASE_DIR = os.path.dirname(__file__)
    - BASE_DIR = Path(__file__).resolve().parent
    - ROOT = Path(__file__).parent
    """
    base_vars: Set[str] = set()
    patterns = [
        re.compile(r"^(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*os\.path\.dirname\(\s*__file__\s*\)", re.MULTILINE),
        re.compile(r"^(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*Path\(\s*__file__\s*\)\.resolve\(\)\.parent", re.MULTILINE),
        re.compile(r"^(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*Path\(\s*__file__\s*\)\.parent", re.MULTILINE),
    ]
    for pat in patterns:
        for m in pat.finditer(py_text):
            base_vars.add(m.group("var"))
    # Always include common names for robustness
    base_vars.update({"BASE_DIR"})
    return base_vars


def discover_resource_paths(py_file: Path, repo_root: Path) -> Tuple[Set[Path], Set[Path]]:
    """Discover resource directories and files referenced from a Python file.

    Returns (directories, files) as absolute Paths within repo_root when resolvable.
    """
    try:
        text = py_file.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return set(), set()

    base_vars = discover_base_vars(text)
    dirs: Set[Path] = set()
    files: Set[Path] = set()

    # Find os.path.join(BASE_VAR, 'seg', 'seg2', ...)
    for m in _OS_JOIN_RE.finditer(text):
        base = m.group("base")
        if base not in base_vars:
            continue
        segs = _extract_string_literals(m.group("parts"))
        if not segs:
            continue
        abs_path = repo_root.joinpath(*segs)
        if any(p == ".." for p in segs):
            # ignore upward paths for safety; resolve within repo root
            abs_path = (repo_root / Path(os.path.join(*segs))).resolve()
        if abs_path.exists():
            if abs_path.is_dir():
                dirs.add(normalize_path(abs_path))
            else:
                files.add(normalize_path(abs_path))
        else:
            # If first segment is a known resource dir, include it optimistically
            if segs[0] in KNOWN_RESOURCE_DIR_NAMES:
                candidate = repo_root.joinpath(segs[0])
                if candidate.exists() and candidate.is_dir():
                    dirs.add(normalize_path(candidate))

    # Also capture literal filenames
    for lit in _extract_string_literals(text):
        lower = lit.lower()
        if lower.endswith(DATA_FILE_SUFFIXES):
            # Try resolve relative to file dir then repo root
            for base in (py_file.parent, repo_root):
                candidate = normalize_path(base / lit)
                if candidate.exists() and candidate.is_file():
                    files.add(candidate)
                    break

    return dirs, files


def walk_python_deps(entry_py_files: Iterable[Path], repo_root: Path) -> Set[Path]:
    """Recursively discover local Python module dependencies starting from entry files."""
    to_visit: List[Path] = [normalize_path(p) for p in entry_py_files if p.exists()]
    visited: Set[Path] = set()
    discovered: Set[Path] = set()

    while to_visit:
        current = normalize_path(to_visit.pop())
        if current in visited:
            continue
        visited.add(current)
        discovered.add(current)
        for mod in parse_imports(current):
            resolved = resolve_local_module(mod, repo_root)
            if resolved and resolved not in visited:
                to_visit.append(resolved)
    return discovered


def collect_everything(repo_root: Path) -> Tuple[Set[Path], Set[Path], Set[Path]]:
    """Collect (python_files, data_dirs, data_files)."""
    # Resolve entrypoints
    entry_py: List[Path] = []
    ahk_files: Set[Path] = set()
    for rel in ENTRYPOINT_CANDIDATES:
        p = repo_root / rel
        if p.suffix.lower() == ".py" and p.exists():
            entry_py.append(p)
        elif p.suffix.lower() == ".ahk" and p.exists():
            ahk_files.add(normalize_path(p))

    # Discover additional python/ahk references from AHK files
    ahk_ref_files: Set[Path] = set()
    ahk_ref_py: Set[Path] = set()
    for ahk in ahk_files:
        try:
            text = ahk.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = ""
        # Find concatenations like A_ScriptDir . "\\path\\to\\file"
        # and A_ScriptDir . '\\path\\to\\file'
        for m in re.finditer(r"A_ScriptDir\s*\.\s*([\'\"])\\\\([^\'\"]+)\1", text):
            rel_str = m.group(2)
            # Clean trailing quotes embedded in constructed strings (e.g., test.py")
            rel_clean = rel_str.rstrip('"')
            candidate = normalize_path(repo_root / Path(rel_clean))
            if candidate.exists():
                ahk_ref_files.add(candidate)
                if candidate.suffix.lower() == ".py":
                    ahk_ref_py.add(candidate)

        # Also catch explicit Run/RunWait paths like RunWait(A_ScriptDir . "\\file.ahk")
        for m in re.finditer(r"Run(?:Wait)?\(\s*A_ScriptDir\s*\.\s*([\'\"])\\\\([^\'\"]+)\1", text):
            rel_str = m.group(2)
            rel_clean = rel_str.rstrip('"')
            candidate = normalize_path(repo_root / Path(rel_clean))
            if candidate.exists():
                ahk_ref_files.add(candidate)
                if candidate.suffix.lower() == ".py":
                    ahk_ref_py.add(candidate)

    # Discover python graph (include any .py referenced by AHK as roots)
    all_roots: List[Path] = list(entry_py) + sorted(ahk_ref_py)
    python_files = walk_python_deps(all_roots, repo_root)

    # Heuristically discover resource paths from all discovered python files
    data_dirs: Set[Path] = set()
    data_files: Set[Path] = set()
    for py in python_files:
        dirs, files = discover_resource_paths(py, repo_root)
        data_dirs.update(dirs)
        data_files.update(files)

    # Always include known resource dirs if present at root
    for name in KNOWN_RESOURCE_DIR_NAMES:
        candidate = repo_root / name
        if candidate.exists() and candidate.is_dir():
            data_dirs.add(normalize_path(candidate))

    # Also include the AHK files explicitly and any direct file refs found in AHK
    data_files.update(ahk_files)
    data_files.update(ahk_ref_files)

    return python_files, data_dirs, data_files


def copy_file_preserve_tree(src: Path, repo_root: Path, dest_root: Path) -> None:
    rel = src.relative_to(repo_root)
    dest_path = dest_root / rel
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest_path)


def copy_dir_preserve_tree(src_dir: Path, repo_root: Path, dest_root: Path, ignores: Tuple[str, ...]) -> None:
    # Copy recursively while skipping ignored folder names
    for root, dirs, files in os.walk(src_dir):
        root_path = Path(root)
        # Filter ignored directories in-place for os.walk
        dirs[:] = [d for d in dirs if d not in ignores]
        for f in files:
            src_file = root_path / f
            copy_file_preserve_tree(src_file, repo_root, dest_root)


def move_paths(paths: Iterable[Path], repo_root: Path, dest_root: Path) -> None:
    for p in paths:
        rel = p.relative_to(repo_root)
        dest_path = dest_root / rel
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), str(dest_path))


def run_isolation(dest: Path, move_instead_of_copy: bool = False, extra_include: List[str] | None = None, extra_exclude: List[str] | None = None) -> Dict[str, List[str]]:
    script_dir = Path(__file__).resolve().parent
    repo_root = find_repo_root(script_dir)
    # Anchor destination inside the repo root for relative paths (friendlier for double-click usage)
    dest_root = dest if dest.is_absolute() else (repo_root / dest)
    dest_root = dest_root.resolve()
    dest_root.mkdir(parents=True, exist_ok=True)

    python_files, data_dirs, data_files = collect_everything(repo_root)

    # Apply extra includes/excludes
    extras: Set[Path] = set()
    if extra_include:
        for item in extra_include:
            p = (repo_root / item).resolve()
            if p.exists():
                extras.add(p)

    excludes: Set[Path] = set()
    if extra_exclude:
        for item in extra_exclude:
            excludes.add((repo_root / item).resolve())

    # Combine paths
    file_set: Set[Path] = set(python_files) | set(data_files) | extras
    dir_set: Set[Path] = set(data_dirs)

    # Filter excludes
    file_set = {p for p in file_set if p not in excludes and not any(str(p).startswith(str(e) + os.sep) for e in excludes)}
    dir_set = {d for d in dir_set if d not in excludes and not any(str(d).startswith(str(e) + os.sep) for e in excludes)}

    # Execute copy/move
    copied: List[str] = []
    moved: List[str] = []
    skipped: List[str] = []

    if move_instead_of_copy:
        # Move directories first to preserve content, then individual files not already in moved dirs
        for d in sorted(dir_set):
            try:
                move_paths([d], repo_root, dest_root)
                moved.append(str(d.relative_to(repo_root)))
            except Exception as e:
                skipped.append(f"DIR {d}: {e}")
        for f in sorted(file_set):
            # Skip files that were already moved as part of a directory
            if any(str(f).startswith(str(d) + os.sep) for d in dir_set):
                continue
            try:
                move_paths([f], repo_root, dest_root)
                moved.append(str(f.relative_to(repo_root)))
            except Exception as e:
                skipped.append(f"FILE {f}: {e}")
    else:
        # Copy directories, then copy remaining files
        for d in sorted(dir_set):
            try:
                copy_dir_preserve_tree(d, repo_root, dest_root, DEFAULT_COPY_IGNORES)
                copied.append(str(d.relative_to(repo_root)) + os.sep)
            except Exception as e:
                skipped.append(f"DIR {d}: {e}")
        for f in sorted(file_set):
            # Skip files already covered by a copied directory
            if any(str(f).startswith(str(d) + os.sep) for d in dir_set):
                continue
            try:
                copy_file_preserve_tree(f, repo_root, dest_root)
                copied.append(str(f.relative_to(repo_root)))
            except Exception as e:
                skipped.append(f"FILE {f}: {e}")

    return {
        "copied": copied,
        "moved": moved,
        "skipped": skipped,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Isolate zscrape/scan_monitor dependencies and resources")
    ap.add_argument("--dest", default="isolated_zscrape", help="Destination directory (created if missing)")
    ap.add_argument("--move", action="store_true", help="Move instead of copy (destructive)")
    ap.add_argument("--include", action="append", help="Extra paths (relative to repo root) to include; repeatable")
    ap.add_argument("--exclude", action="append", help="Paths (relative to repo root) to exclude; repeatable")
    args = ap.parse_args()

    result = run_isolation(dest=Path(args.dest), move_instead_of_copy=bool(args.move), extra_include=args.include, extra_exclude=args.exclude)

    print("=== Isolation Complete ===")
    for key in ("copied", "moved", "skipped"):
        items = result.get(key) or []
        print(f"{key.capitalize()} ({len(items)}):")
        for it in items:
            print(f"  - {it}")


if __name__ == "__main__":
    main()


