"""
Z-Sync GUI — Simple two-way merge for project files and flash drive

DIRECTIVES (edit these to tweak behavior):

- PROJECT_ROOT: Defaults to the folder this script is in. Change if needed.
- DEFAULT_MIRROR_FOLDER_NAME: Folder name created on the flash drive for the mirror (e.g., 'newBranch1').
- INCLUDED_EXTENSIONS: File extensions to include by default (scripts + text/config types).
- ALWAYS_INCLUDE_DIRS: Directories to always include entirely (recursive), regardless of file extension.
- ALWAYS_INCLUDE_BASENAMES: Specific filenames to always include anywhere in the tree.
- DEFAULT_EXCLUDED_DIRS: Directories to skip (not searched), typically large/binary or transient.
- USE_HASH_WHEN_TIMES_EQUAL: If True, when mtimes are equal but sizes differ, use SHA256 hash to detect changes.
- CONFLICT_POLICY: 'newer_wins' (default). Future options could be 'prefer_left' or 'prefer_right'.

User intent notes (memory):
- One file per entity with directives at the top, so these are here for easy sweeping.
- Comments capture intent. This tool focuses on merging updated files and specific content trees to/from a flash drive.
"""

from __future__ import annotations

import os
import sys
import time
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# ====== Directives ======
PROJECT_ROOT = Path(__file__).resolve().parent

# The folder name to create on the flash drive as the mirror root of this project.
DEFAULT_MIRROR_FOLDER_NAME = PROJECT_ROOT.name  # e.g., 'newBranch1'

# File types we consider for syncing by default (scripts + text/config)
INCLUDED_EXTENSIONS: Set[str] = {
    ".py", ".pyw", ".ahk", ".ps1", ".bat",
    ".txt", ".json", ".csv", ".tsv", ".ini", ".cfg", ".conf",
    ".md", ".sql", ".yaml", ".yml",
}

# Script-only extensions used for safe auto-push from host -> flash
SCRIPT_EXTENSIONS: Set[str] = {
    ".py", ".pyw", ".ahk", ".ps1", ".bat",
}

# Text-like extensions for auto pull from flash -> project
TEXTLIKE_EXTENSIONS: Set[str] = {
    ".txt", ".json",
}

# Directories to always include entirely (recursive), even if files within have other extensions.
ALWAYS_INCLUDE_DIRS: Set[str] = {
    "backups",
    "item_contents",
    "state",  # include runtime state files like baselines/blacklists
}

# Specific filenames to always include, anywhere in the tree.
ALWAYS_INCLUDE_BASENAMES: Set[str] = {
    # Legacy basenames (still picked up if found anywhere)
    "_ignore_list_active.txt",
    "_ignore_list_scheduled.txt",
    "processed_items_blacklist.txt",
    "highest_sku_number.txt",
}

# Directories to skip while scanning (not searched at all)
DEFAULT_EXCLUDED_DIRS: Set[str] = {
    ".git",
    "__pycache__",
    "models",  # large binary model files
    "llama-b6121-bin-win-cpu-x64",  # large binaries/tooling
    "logs",  # usually not needed for mirrors; user can un-exclude later
}

# If mtimes are equal but sizes differ, optionally hash to decide if different
USE_HASH_WHEN_TIMES_EQUAL: bool = True

# Conflict policy for bi-directional sync
CONFLICT_POLICY: str = "newer_wins"  # future: 'prefer_left', 'prefer_right'

# Auto-sync (project -> flash) settings
AUTO_SYNC_ENABLED_DEFAULT: bool = False
AUTO_SYNC_INTERVAL_MS: int = 5000  # 5 seconds


# ====== Core logic ======

@dataclass
class SyncAction:
    direction: str  # 'push' or 'pull'
    src: Path
    dst: Path
    reason: str  # e.g., 'src newer', 'dst missing', 'hash diff'


def is_under_any_directory(path: Path, dir_names: Set[str], stop_at: Path) -> bool:
    """Return True if `path` is within any directory named in `dir_names` between its parent and stop_at.

    Used to force-include files if they are within ALWAYS_INCLUDE_DIRS trees.
    """
    try:
        relative = path.resolve().relative_to(stop_at.resolve())
    except Exception:
        return False
    for part in relative.parts:
        if part in dir_names:
            return True
    return False


def should_consider_file(path: Path, base_root: Path) -> bool:
    """Decide whether this file is a candidate for syncing based on directives."""
    if not path.is_file():
        return False

    # Always include if basename matches
    if path.name in ALWAYS_INCLUDE_BASENAMES:
        return True

    # If the file is inside any of the always-include directories, include it.
    if is_under_any_directory(path, ALWAYS_INCLUDE_DIRS, base_root):
        return True

    # Otherwise, include only by extension
    return path.suffix.lower() in INCLUDED_EXTENSIONS


def iter_project_files(project_root: Path, excluded_dirs: Set[str]) -> Iterable[Path]:
    """Yield files from project_root, skipping excluded directories by name."""
    for root, dirnames, filenames in os.walk(project_root):
        # Filter dirnames in-place to skip traversal into excluded dirs
        dirnames[:] = [d for d in dirnames if d not in excluded_dirs]
        for filename in filenames:
            yield Path(root) / filename


def sha256_of_file(path: Path, block_size: int = 65536) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def newer_than(a: Path, b: Path) -> bool:
    return a.stat().st_mtime > b.stat().st_mtime


def equal_time_diff_size(a: Path, b: Path) -> bool:
    return (int(a.stat().st_mtime) == int(b.stat().st_mtime)) and (a.stat().st_size != b.stat().st_size)


def plan_one_way(
    left_root: Path,
    right_root: Path,
    base_for_rel: Path,
    excluded_dirs: Set[str],
    direction_label: str,  # 'push' if left->right, 'pull' if right->left
) -> List[SyncAction]:
    """Compute a list of actions to mirror from left_root to right_root for candidate files.

    The directory structure under left_root relative to project_root is mirrored under right_root.
    """
    actions: List[SyncAction] = []
    for file_path in iter_project_files(left_root, excluded_dirs):
        try:
            rel = file_path.resolve().relative_to(base_for_rel.resolve())
        except Exception:
            # If not under the expected base, skip
            continue
        if not should_consider_file(file_path, base_for_rel):
            continue

        dst = right_root / rel
        if not dst.exists():
            actions.append(SyncAction(direction_label, file_path, dst, reason="dst missing"))
            continue

        # Both exist: decide if copy is needed
        try:
            if newer_than(file_path, dst):
                actions.append(SyncAction(direction_label, file_path, dst, reason="src newer"))
            elif equal_time_diff_size(file_path, dst) and USE_HASH_WHEN_TIMES_EQUAL:
                # Hash compare to detect difference
                try:
                    if sha256_of_file(file_path) != sha256_of_file(dst):
                        actions.append(SyncAction(direction_label, file_path, dst, reason="hash diff"))
                except Exception:
                    # If hashing fails, be conservative: do nothing.
                    pass
        except FileNotFoundError:
            # If either side vanished during planning, skip; user can re-run
            continue
    return actions


def plan_two_way(
    project_root: Path,
    flash_root: Path,
    mirror_folder_name: str,
    excluded_dirs: Set[str],
) -> Tuple[List[SyncAction], List[SyncAction]]:
    """Return (push_actions, pull_actions) based on newer-wins policy.

    - push: from project_root -> flash_root/mirror_folder_name
    - pull: from flash_root/mirror_folder_name -> project_root
    """
    right_root = flash_root / mirror_folder_name

    push = plan_one_way(project_root, right_root, project_root, excluded_dirs, direction_label="push")
    pull = plan_one_way(right_root, project_root, right_root, excluded_dirs, direction_label="pull") if right_root.exists() else []

    if CONFLICT_POLICY == "newer_wins":
        # Remove opposing actions where the opposite side is also newer (to avoid double-copy). We'll resolve by time.
        # Build quick lookup by relative path
        def rel_push(a: SyncAction) -> Path:
            # push action src is under project_root; match by project-relative path
            return a.src.resolve().relative_to(project_root.resolve())

        def rel_pull(a: SyncAction) -> Path:
            # pull action dst is under project_root; match by project-relative path
            return a.dst.resolve().relative_to(project_root.resolve())

        pull_by_rel = {rel_pull(a): a for a in pull if a.dst.exists()}
        filtered_push: List[SyncAction] = []
        for a in push:
            r = rel_push(a)
            opposing = pull_by_rel.get(r)
            if opposing is None:
                filtered_push.append(a)
                continue
            # Both sides suggest updates; keep only the direction where src is newer
            try:
                # Compare times at opposing sides
                if newer_than(a.src, opposing.src):
                    filtered_push.append(a)
                else:
                    # keep opposing (pull) and drop this push
                    pass
            except FileNotFoundError:
                filtered_push.append(a)
        # Now also drop pull actions that are older than existing push for same file
        push_rel = {rel_push(a) for a in filtered_push}
        filtered_pull = [a for a in pull if rel_pull(a) not in push_rel]
        return filtered_push, filtered_pull

    # Default: no special conflict resolution
    return push, pull


def ensure_parent_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def execute_actions(actions: Sequence[SyncAction], log_fn) -> Tuple[int, int]:
    """Execute sync actions. Returns (num_copied, num_errors)."""
    import shutil

    copied = 0
    errors = 0
    for a in actions:
        try:
            ensure_parent_directory(a.dst)
            shutil.copy2(a.src, a.dst)
            log_fn(f"{a.direction.upper()}: {a.src} -> {a.dst} [{a.reason}]")
            copied += 1
        except Exception as e:
            log_fn(f"ERROR copying {a.src} -> {a.dst}: {e}")
            errors += 1
    return copied, errors


# ====== GUI ======

class ZSyncApp(ttk.Frame):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master)
        self.master.title("Z-Sync — Project ⇄ Flash Merge")
        self.master.geometry("980x640")

        self.project_root_var = tk.StringVar(value=str(PROJECT_ROOT))
        self.flash_root_var = tk.StringVar(value=self._default_flash_drive_guess())
        self.mirror_name_var = tk.StringVar(value=DEFAULT_MIRROR_FOLDER_NAME)

        self.include_ext_var = tk.StringVar(value=", ".join(sorted(INCLUDED_EXTENSIONS)))
        self.always_dirs_var = tk.StringVar(value=", ".join(sorted(ALWAYS_INCLUDE_DIRS)))
        self.always_files_var = tk.StringVar(value=", ".join(sorted(ALWAYS_INCLUDE_BASENAMES)))
        self.exclude_dirs_var = tk.StringVar(value=", ".join(sorted(DEFAULT_EXCLUDED_DIRS)))

        self.use_hash_var = tk.BooleanVar(value=USE_HASH_WHEN_TIMES_EQUAL)
        self.preview_only_var = tk.BooleanVar(value=True)

        # Auto-sync state
        self.auto_enabled_var = tk.BooleanVar(value=AUTO_SYNC_ENABLED_DEFAULT)  # scripts-only push
        self.auto_two_way_var = tk.BooleanVar(value=False)  # full two-way mirror
        self.auto_interval_ms_var = tk.IntVar(value=AUTO_SYNC_INTERVAL_MS)
        self._auto_tick_scheduled = False

        # Additional auto mode: flash → project for text-like and item_contents
        self.auto_pull_texts_var = tk.BooleanVar(value=False)

        # Monitoring filters (relative subfolders)
        self.host_subpaths_var = tk.StringVar(value="")
        self.flash_subpaths_var = tk.StringVar(value="")
        self.monitor_host_subpaths: List[str] = []
        self.monitor_flash_subpaths: List[str] = []

        self.push_actions: List[SyncAction] = []
        self.pull_actions: List[SyncAction] = []

        self._build_ui()

    # ---- UI helpers ----
    def _build_ui(self) -> None:
        pad = {"padx": 8, "pady": 4}

        # Paths frame
        paths = ttk.LabelFrame(self, text="Paths")
        paths.grid(row=0, column=0, sticky="nsew", **pad)

        ttk.Label(paths, text="Project root:").grid(row=0, column=0, sticky="w")
        ttk.Entry(paths, textvariable=self.project_root_var, width=80).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(paths, text="Browse", command=self._browse_project_root).grid(row=0, column=2)

        ttk.Label(paths, text="Flash drive root:").grid(row=1, column=0, sticky="w")
        ttk.Entry(paths, textvariable=self.flash_root_var, width=80).grid(row=1, column=1, sticky="we", padx=6)
        ttk.Button(paths, text="Browse", command=self._browse_flash_root).grid(row=1, column=2)

        ttk.Label(paths, text="Mirror folder name on flash:").grid(row=2, column=0, sticky="w")
        ttk.Entry(paths, textvariable=self.mirror_name_var, width=40).grid(row=2, column=1, sticky="w", padx=6)

        # Options frame
        opts = ttk.LabelFrame(self, text="Options (edit directives below for persistent defaults)")
        opts.grid(row=1, column=0, sticky="nsew", **pad)

        ttk.Checkbutton(opts, text="Use SHA256 when times equal but sizes differ", variable=self.use_hash_var).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(opts, text="Preview only (don’t copy)", variable=self.preview_only_var).grid(row=0, column=1, sticky="w")

        ttk.Label(opts, text="Included extensions (comma-separated):").grid(row=1, column=0, sticky="w")
        ttk.Entry(opts, textvariable=self.include_ext_var, width=80).grid(row=1, column=1, sticky="we", padx=6)

        ttk.Label(opts, text="Always-include directories:").grid(row=2, column=0, sticky="w")
        ttk.Entry(opts, textvariable=self.always_dirs_var, width=80).grid(row=2, column=1, sticky="we", padx=6)

        ttk.Label(opts, text="Always-include filenames:").grid(row=3, column=0, sticky="w")
        ttk.Entry(opts, textvariable=self.always_files_var, width=80).grid(row=3, column=1, sticky="we", padx=6)

        ttk.Label(opts, text="Excluded directories:").grid(row=4, column=0, sticky="w")
        ttk.Entry(opts, textvariable=self.exclude_dirs_var, width=80).grid(row=4, column=1, sticky="we", padx=6)

        # Buttons
        btns = ttk.Frame(self)
        btns.grid(row=2, column=0, sticky="we", **pad)
        ttk.Button(btns, text="Analyze", command=self.on_analyze).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Sync (Run)", command=self.on_sync).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Clear Log", command=self.on_clear_log).grid(row=0, column=2, padx=4)

        # Auto-sync controls
        auto = ttk.LabelFrame(self, text="Auto-sync options")
        auto.grid(row=3, column=0, sticky="we", **pad)
        ttk.Checkbutton(auto, text="Enable auto-sync (Project → Flash, scripts only)", variable=self.auto_enabled_var, command=self._on_toggle_auto).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Checkbutton(auto, text="Enable auto pull (Flash → Project, texts + item_contents)", variable=self.auto_pull_texts_var, command=self._on_toggle_auto).grid(row=1, column=0, columnspan=3, sticky="w")
        ttk.Checkbutton(auto, text="Enable auto two-way mirror (Project ↔ Flash, all included types)", variable=self.auto_two_way_var, command=self._on_toggle_auto).grid(row=2, column=0, columnspan=3, sticky="w")
        ttk.Label(auto, text="Interval (ms):").grid(row=3, column=0, sticky="e", padx=(0, 4))
        ttk.Spinbox(auto, from_=500, to=60000, increment=500, textvariable=self.auto_interval_ms_var, width=8, command=self._on_auto_interval_changed).grid(row=3, column=1, sticky="w")

        # Monitoring filters
        mon = ttk.LabelFrame(self, text="Monitoring filters (relative subfolders)")
        mon.grid(row=4, column=0, sticky="we", **pad)
        ttk.Label(mon, text="Host subfolders (comma-separated):").grid(row=0, column=0, sticky="w")
        ttk.Entry(mon, textvariable=self.host_subpaths_var, width=60).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Label(mon, text="Flash subfolders (comma-separated, under mirror root):").grid(row=1, column=0, sticky="w")
        ttk.Entry(mon, textvariable=self.flash_subpaths_var, width=60).grid(row=1, column=1, sticky="we", padx=6)

        # Results
        results = ttk.LabelFrame(self, text="Plan / Log")
        results.grid(row=5, column=0, sticky="nsew", **pad)

        self.tree = ttk.Treeview(results, columns=("direction", "reason", "src", "dst"), show="headings")
        self.tree.heading("direction", text="Direction")
        self.tree.heading("reason", text="Reason")
        self.tree.heading("src", text="Source")
        self.tree.heading("dst", text="Destination")
        self.tree.column("direction", width=90, anchor="w")
        self.tree.column("reason", width=120, anchor="w")
        self.tree.column("src", width=420, anchor="w")
        self.tree.column("dst", width=420, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")

        vsb = ttk.Scrollbar(results, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=vsb.set)
        vsb.grid(row=0, column=1, sticky="ns")

        # Status bar
        self.status_var = tk.StringVar(value="Ready.")
        status = ttk.Label(self, textvariable=self.status_var, anchor="w")
        status.grid(row=6, column=0, sticky="we", **pad)

        # Grid weights
        self.grid(row=0, column=0, sticky="nsew")
        self.master.grid_rowconfigure(0, weight=1)
        self.master.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(5, weight=1)
        self.grid_columnconfigure(0, weight=1)
        results.grid_rowconfigure(0, weight=1)
        results.grid_columnconfigure(0, weight=1)

    def _browse_project_root(self) -> None:
        p = filedialog.askdirectory(initialdir=self.project_root_var.get(), title="Select Project Root")
        if p:
            self.project_root_var.set(p)

    def _browse_flash_root(self) -> None:
        p = filedialog.askdirectory(initialdir=self.flash_root_var.get() or "C:/", title="Select Flash Drive Root")
        if p:
            self.flash_root_var.set(p)

    def _default_flash_drive_guess(self) -> str:
        # Simple heuristic: look for common removable letters; fallback empty
        for letter in ["E", "F", "G", "H", "D"]:
            path = f"{letter}:/"
            try:
                if os.path.exists(path):
                    return path
            except Exception:
                pass
        return ""

    # ---- Planning & execution ----
    def _read_options_into_directives(self) -> Tuple[Path, Path, str, Set[str]]:
        global INCLUDED_EXTENSIONS, ALWAYS_INCLUDE_DIRS, ALWAYS_INCLUDE_BASENAMES, DEFAULT_EXCLUDED_DIRS, USE_HASH_WHEN_TIMES_EQUAL

        INCLUDED_EXTENSIONS = {e.strip().lower() if e.strip().startswith(".") else f".{e.strip().lower()}" for e in self.include_ext_var.get().split(",") if e.strip()}
        ALWAYS_INCLUDE_DIRS = {d.strip() for d in self.always_dirs_var.get().split(",") if d.strip()}
        ALWAYS_INCLUDE_BASENAMES = {b.strip() for b in self.always_files_var.get().split(",") if b.strip()}
        DEFAULT_EXCLUDED_DIRS = {d.strip() for d in self.exclude_dirs_var.get().split(",") if d.strip()}
        USE_HASH_WHEN_TIMES_EQUAL = bool(self.use_hash_var.get())

        project_root = Path(self.project_root_var.get()).resolve()
        flash_root = Path(self.flash_root_var.get()).resolve()
        mirror_name = self.mirror_name_var.get().strip() or project_root.name

        # Monitoring filters
        self.monitor_host_subpaths = [s.strip().replace("\\", "/").lstrip("/") for s in self.host_subpaths_var.get().split(",") if s.strip()]
        self.monitor_flash_subpaths = [s.strip().replace("\\", "/").lstrip("/") for s in self.flash_subpaths_var.get().split(",") if s.strip()]
        return project_root, flash_root, mirror_name, DEFAULT_EXCLUDED_DIRS

    def _log(self, msg: str) -> None:
        self.status_var.set(msg)
        self.master.update_idletasks()

    def _populate_tree(self, actions: Sequence[SyncAction]) -> None:
        self.tree.delete(*self.tree.get_children())
        for a in actions:
            self.tree.insert("", "end", values=(a.direction, a.reason, str(a.src), str(a.dst)))

    def on_analyze(self) -> None:
        try:
            project_root, flash_root, mirror_name, excluded = self._read_options_into_directives()
        except Exception as e:
            messagebox.showerror("Error", f"Invalid options: {e}")
            return

        if not project_root.exists():
            messagebox.showerror("Error", f"Project root does not exist: {project_root}")
            return
        if not flash_root.exists():
            messagebox.showerror("Error", f"Flash drive root does not exist: {flash_root}")
            return

        self._log("Analyzing…")
        push, pull = plan_two_way(project_root, flash_root, mirror_name, excluded)
        self.push_actions = push
        self.pull_actions = pull

        combined = push + pull
        self._populate_tree(combined)
        self._log(f"Plan ready: {len(push)} push, {len(pull)} pull, total {len(combined)} actions.")

    def on_sync(self) -> None:
        if self.preview_only_var.get():
            messagebox.showinfo("Preview only", "Preview mode is ON. Uncheck 'Preview only' to perform copies.")
            return

        actions = self.push_actions + self.pull_actions
        if not actions:
            messagebox.showinfo("Nothing to do", "No actions to perform. Click Analyze first.")
            return

        if not messagebox.askyesno("Confirm", f"Execute {len(actions)} copy operations?"):
            return

        self._log("Copying…")
        copied, errors = execute_actions(actions, log_fn=lambda m: self._log(m))
        self._log(f"Done. Copied {copied} files with {errors} errors.")

    def on_clear_log(self) -> None:
        self._populate_tree([])
        self._log("Ready.")

    # ---- Auto-sync logic ----
    def _on_toggle_auto(self) -> None:
        if self.auto_enabled_var.get() or self.auto_pull_texts_var.get() or self.auto_two_way_var.get():
            self._schedule_auto_tick()
            self._log("Auto-sync enabled.")
        else:
            self._log("Auto-sync disabled.")

    def _on_auto_interval_changed(self) -> None:
        # Reschedule next tick with new interval
        if self.auto_enabled_var.get() or self.auto_pull_texts_var.get() or self.auto_two_way_var.get():
            self._schedule_auto_tick(reschedule=True)

    def _schedule_auto_tick(self, reschedule: bool = False) -> None:
        # We use after() for periodic polling; avoid double-scheduling
        if reschedule or not self._auto_tick_scheduled:
            interval = max(200, int(self.auto_interval_ms_var.get() or AUTO_SYNC_INTERVAL_MS))
            self.master.after(interval, self._auto_sync_tick)
            self._auto_tick_scheduled = True

    def _auto_sync_tick(self) -> None:
        self._auto_tick_scheduled = False
        if not (self.auto_enabled_var.get() or self.auto_pull_texts_var.get() or self.auto_two_way_var.get()):
            return

        try:
            project_root, flash_root, mirror_name, excluded = self._read_options_into_directives()
        except Exception:
            self._schedule_auto_tick()
            return

        if not project_root.exists() or not flash_root.exists():
            self._schedule_auto_tick()
            return

        flash_mirror_root = flash_root / mirror_name

        if self.auto_two_way_var.get():
            # Classic mirror: plan two-way and execute
            push, pull = plan_two_way(project_root, flash_root, mirror_name, excluded)
            total = 0
            errors = 0
            if push:
                c, e = execute_actions(push, log_fn=lambda m: None)
                total += c
                errors += e
            if pull:
                c, e = execute_actions(pull, log_fn=lambda m: None)
                total += c
                errors += e
            if total or errors:
                self._log(f"Auto two-way mirror: copied {total} files ({errors} errors)")
        else:
            # Independent modes can run together
            if self.auto_enabled_var.get():
                actions = self._plan_auto_push_scripts(project_root, flash_mirror_root, excluded, self.monitor_host_subpaths)
                if actions:
                    copied, errors = execute_actions(actions, log_fn=lambda m: None)
                    if copied or errors:
                        self._log(f"Auto-sync: copied {copied} script files ({errors} errors)")
            if self.auto_pull_texts_var.get():
                pull_actions = self._plan_auto_pull_texts(project_root, flash_mirror_root, excluded, self.monitor_flash_subpaths)
                if pull_actions:
                    copied, errors = execute_actions(pull_actions, log_fn=lambda m: None)
                    if copied or errors:
                        self._log(f"Auto-pull: copied {copied} text/item files ({errors} errors)")

        self._schedule_auto_tick()

    def _plan_auto_push_scripts(self, project_root: Path, flash_mirror_root: Path, excluded_dirs: Set[str], monitor_host_subpaths: List[str]) -> List[SyncAction]:
        # Ensure mirror root exists for planning; actual copy will mkdir parents
        actions: List[SyncAction] = []
        for file_path in iter_project_files(project_root, excluded_dirs):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in SCRIPT_EXTENSIONS:
                continue
            try:
                rel = file_path.resolve().relative_to(project_root.resolve())
            except Exception:
                continue
            # Filter by monitored host subpaths (if any)
            if not _rel_matches_subpaths(rel, monitor_host_subpaths):
                continue
            dst = flash_mirror_root / rel
            if not dst.exists():
                actions.append(SyncAction("push", file_path, dst, reason="dst missing (auto)"))
                continue
            try:
                if newer_than(file_path, dst):
                    actions.append(SyncAction("push", file_path, dst, reason="src newer (auto)"))
                elif equal_time_diff_size(file_path, dst) and USE_HASH_WHEN_TIMES_EQUAL:
                    try:
                        if sha256_of_file(file_path) != sha256_of_file(dst):
                            actions.append(SyncAction("push", file_path, dst, reason="hash diff (auto)"))
                    except Exception:
                        pass
            except FileNotFoundError:
                continue
        return actions

    def _plan_auto_pull_texts(self, project_root: Path, flash_mirror_root: Path, excluded_dirs: Set[str], monitor_flash_subpaths: List[str]) -> List[SyncAction]:
        actions: List[SyncAction] = []
        for file_path in iter_project_files(flash_mirror_root, excluded_dirs):
            if not file_path.is_file():
                continue
            try:
                rel = file_path.resolve().relative_to(flash_mirror_root.resolve())
            except Exception:
                continue
            # Filter by monitored flash subpaths (if any)
            if not _rel_matches_subpaths(rel, monitor_flash_subpaths):
                continue
            # Allow item_contents subtree always; plus text-like files and always-include basenames
            include = False
            if file_path.name in ALWAYS_INCLUDE_BASENAMES:
                include = True
            elif is_under_any_directory(file_path, {"item_contents"}, flash_mirror_root):
                include = True
            elif file_path.suffix.lower() in TEXTLIKE_EXTENSIONS:
                include = True
            if not include:
                continue
            dst = project_root / rel
            if not dst.exists():
                actions.append(SyncAction("pull", file_path, dst, reason="dst missing (auto)"))
                continue
            try:
                if newer_than(file_path, dst):
                    actions.append(SyncAction("pull", file_path, dst, reason="src newer (auto)"))
                elif equal_time_diff_size(file_path, dst) and USE_HASH_WHEN_TIMES_EQUAL:
                    try:
                        if sha256_of_file(file_path) != sha256_of_file(dst):
                            actions.append(SyncAction("pull", file_path, dst, reason="hash diff (auto)"))
                    except Exception:
                        pass
            except FileNotFoundError:
                continue
        return actions


def _rel_matches_subpaths(rel_path: Path, subpaths: List[str]) -> bool:
    if not subpaths:
        return True
    rel_str = rel_path.as_posix()
    for sp in subpaths:
        sp_norm = sp.rstrip("/")
        if not sp_norm:
            continue
        if rel_str.startswith(sp_norm):
            return True
    return False


def main() -> None:
    root = tk.Tk()
    app = ZSyncApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()


