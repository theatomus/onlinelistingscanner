from __future__ import annotations

import os
import re
import sys
import threading
import time
import ctypes
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext


class SearchAndProcessGUI(tk.Tk):
    """
    A lightweight GUI to:
    - Index items from `item_contents/`
    - Search by item number or title words
    - Run process_description.py then runit.py for selected items

    It prefers existing `python_parsed_<item>.txt` to read titles. If not present, it falls
    back to parsing `<item>.html` or `<item>_description.txt` heuristically.
    """

    def __init__(self, items_dir: str = 'item_contents') -> None:
        super().__init__()
        self.title('Search & Process: process_description → runit')
        self.geometry('1100x720')

        self.items_dir = Path(items_dir)
        self.items_dir.mkdir(exist_ok=True)

        # Data: item_number -> title
        self.item_to_title: Dict[str, str] = {}
        self.filtered_items: List[Tuple[str, str]] = []
        self._search_after_id: Optional[str] = None

        self._build_ui()
        self._index_items_background()

    # --- UI ---
    def _build_ui(self) -> None:
        top = ttk.Frame(self)
        top.pack(fill='x', padx=8, pady=8)

        ttk.Label(top, text='Items dir:').pack(side='left')
        self.items_var = tk.StringVar(value=str(self.items_dir))
        ttk.Entry(top, textvariable=self.items_var, width=60).pack(side='left', padx=6)
        ttk.Button(top, text='Browse', command=self._browse_items_dir).pack(side='left')
        ttk.Button(top, text='Reload Index', command=self._index_items_background).pack(side='left', padx=6)

        mode_frame = ttk.Frame(self)
        mode_frame.pack(fill='x', padx=8)
        self.search_mode = tk.StringVar(value='auto')
        ttk.Radiobutton(mode_frame, text='Item # or Title (auto)', value='auto', variable=self.search_mode).pack(side='left')
        ttk.Radiobutton(mode_frame, text='Item #', value='item', variable=self.search_mode).pack(side='left', padx=8)
        ttk.Radiobutton(mode_frame, text='Title words (AND)', value='title', variable=self.search_mode).pack(side='left')
        # Active search on mode change
        self.search_mode.trace_add('write', lambda *_: self._do_search())

        search_frame = ttk.Frame(self)
        search_frame.pack(fill='x', padx=8, pady=6)
        self.query_var = tk.StringVar()
        entry = ttk.Entry(search_frame, textvariable=self.query_var)
        entry.pack(side='left', fill='x', expand=True)
        entry.bind('<Return>', lambda _e: self._on_enter())
        ttk.Button(search_frame, text='Search', command=self._do_search).pack(side='left', padx=6)
        entry.focus_set()
        # Active search as-you-type (debounced)
        self.query_var.trace_add('write', lambda *_: self._on_query_changed())

        main = ttk.PanedWindow(self, orient='horizontal')
        main.pack(fill='both', expand=True, padx=8, pady=6)

        left = ttk.Frame(main)
        main.add(left, weight=1)
        ttk.Label(left, text='Results (select one or more):').pack(anchor='w')
        self.results = tk.Listbox(left, selectmode='extended')
        self.results.pack(fill='both', expand=True)
        self.results.bind('<Double-Button-1>', lambda _e: self._process_selected())

        right = ttk.Frame(main)
        main.add(right, weight=1)
        run_frame = ttk.LabelFrame(right, text='Run')
        run_frame.pack(fill='x')
        ttk.Button(run_frame, text='Process + Run Selected', command=self._process_selected).pack(side='left', padx=6, pady=6)
        self.skip_runit_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(run_frame, text='Skip runit (only process_description)', variable=self.skip_runit_var).pack(side='left', padx=6)

        log_frame = ttk.LabelFrame(right, text='Log')
        log_frame.pack(fill='both', expand=True, pady=8)
        self.log = scrolledtext.ScrolledText(log_frame, wrap='word', height=20)
        self.log.pack(fill='both', expand=True)

    # --- Actions ---
    def _browse_items_dir(self) -> None:
        from tkinter import filedialog
        chosen = filedialog.askdirectory(initialdir=str(self.items_dir), title='Pick item_contents dir')
        if chosen:
            self.items_dir = Path(chosen)
            self.items_var.set(chosen)
            self._index_items_background()

    def _index_items_background(self) -> None:
        def work() -> None:
            try:
                self._append_log('Indexing items...')
                self.item_to_title = self._build_index(self.items_dir)
                self._append_log(f'Indexed {len(self.item_to_title)} items.')
                # Refresh results according to current query/mode immediately
                self.after(0, self._do_search)
            except Exception as exc:
                self._append_log(f'Indexing error: {exc}')

        threading.Thread(target=work, daemon=True).start()

    def _on_query_changed(self) -> None:
        # Debounce rapid typing to avoid excessive filtering
        if self._search_after_id is not None:
            try:
                self.after_cancel(self._search_after_id)
            except Exception:
                pass
        self._search_after_id = self.after(200, self._do_search)

    def _do_search(self) -> None:
        results = self._compute_search_results()
        self._refresh_results(results)

    def _compute_search_results(self) -> List[Tuple[str, str]]:
        q = (self.query_var.get() or '').strip()
        mode = self.search_mode.get()
        if not q:
            return list(self.item_to_title.items())

        if mode == 'item':
            return [(item, title) for item, title in self.item_to_title.items() if q in item]
        elif mode == 'title':
            words = [w for w in re.split(r"\s+", q) if w]
            results: List[Tuple[str, str]] = []
            for item, title in self.item_to_title.items():
                title_low = (title or '').lower()
                if all(w.lower() in title_low for w in words):
                    results.append((item, title))
            return results
        else:
            words = [w for w in re.split(r"\s+", q) if w]
            results_map: Dict[str, str] = {}
            for item, title in self.item_to_title.items():
                if q in item:
                    results_map[item] = title
            for item, title in self.item_to_title.items():
                title_low = (title or '').lower()
                if all(w.lower() in title_low for w in words):
                    results_map[item] = title
            return list(results_map.items())

    def _on_enter(self) -> None:
        results = self._compute_search_results()
        self._refresh_results(results)
        if len(results) == 1:
            # Run the only match
            single_item = results[0][0]
            self._start_processing([single_item], self.skip_runit_var.get())

    def _refresh_results(self, items: List[Tuple[str, str]]) -> None:
        self.filtered_items = sorted(items, key=lambda it: it[0])
        self.results.delete(0, tk.END)
        for item, title in self.filtered_items:
            display = f"{item}  |  {title or '(no title)'}"
            self.results.insert(tk.END, display)

    def _process_selected(self) -> None:
        sel = self.results.curselection()
        if not sel:
            messagebox.showinfo('No selection', 'Select one or more items in the results list.')
            return
        items = [self.filtered_items[i][0] for i in sel]
        skip_runit = self.skip_runit_var.get()
        self._start_processing(items, skip_runit)

    def _start_processing(self, items: List[str], skip_runit: bool) -> None:
        def work() -> None:
            for item in items:
                self._append_log(f"Processing item {item} ...")
                ok = self._run_process_description(item)
                if not ok:
                    self._append_log(f"process_description FAILED for {item}")
                    continue
                self._append_log(f"process_description OK for {item}")
                if not skip_runit:
                    self._append_log(f"Starting runit for {item} ...")
                    ok2 = self._run_runit(item)
                    if ok2:
                        self._append_log(f"runit finished for {item}")
                    else:
                        self._append_log(f"runit FAILED for {item}")
            self._append_log('Done.')
        threading.Thread(target=work, daemon=True).start()

    # --- Helpers ---
    def _append_log(self, text: str) -> None:
        def do_append() -> None:
            self.log.insert(tk.END, text + '\n')
            self.log.see(tk.END)
        # Always marshal to main thread
        self.after(0, do_append)

    def _build_index(self, items_dir: Path) -> Dict[str, str]:
        index: Dict[str, str] = {}

        # Discover items by `_description.txt`
        for path in sorted(items_dir.glob('*_description.txt')):
            item = path.name.replace('_description.txt', '')
            title = self._find_title_for_item(items_dir, item)
            index[item] = title

        # Include items that only have HTML (no _description.txt)
        for path in sorted(items_dir.glob('*.html')):
            item = path.stem
            if item not in index:
                index[item] = self._find_title_for_item(items_dir, item)

        return index

    def _find_title_for_item(self, items_dir: Path, item: str) -> str:
        # 1) Prefer python_parsed
        parsed = items_dir / f'python_parsed_{item}.txt'
        if parsed.exists():
            try:
                title = self._read_title_from_parsed(parsed)
                if title:
                    return title
            except Exception:
                pass

        # 2) Try HTML
        html_path = items_dir / f'{item}.html'
        if html_path.exists():
            try:
                with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                    html = f.read()
                # Look for <input name="title" value="...">
                m = re.search(r'name=["\']title["\'][^>]*\bvalue=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
                if m:
                    t = m.group(1).strip()
                    if t:
                        return t
            except Exception:
                pass

        # 3) Try description text
        txt_path = items_dir / f'{item}_description.txt'
        if txt_path.exists():
            try:
                with open(txt_path, 'r', encoding='utf-8', errors='ignore') as f:
                    txt = f.read()
                # Heuristic: look for an "Item title" label and take next line
                m = re.search(r'(?im)^Item\s+title[^\r\n]*\r?\n(.+)$', txt)
                if m:
                    t = m.group(1).strip()
                    if t:
                        return t
            except Exception:
                pass

        return ''

    def _read_title_from_parsed(self, parsed_path: Path) -> str:
        # The parsed format is sectioned; keys are shown in brackets on one line, value on next line.
        # Prefer [Full Title] if present, else the first [title_*] we find that looks like full title.
        title: Optional[str] = None
        try:
            with open(parsed_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.read().splitlines()
        except Exception:
            return ''

        for i, line in enumerate(lines):
            if line.strip() == '[Full Title]':
                # Next non-empty line is the title
                for j in range(i + 1, min(i + 6, len(lines))):
                    val = lines[j].strip()
                    if val:
                        return val
        # Fallback: try to detect a likely full title key
        for i, line in enumerate(lines):
            if line.strip().startswith('[title_') and 'full' in line.lower():
                for j in range(i + 1, min(i + 6, len(lines))):
                    val = lines[j].strip()
                    if val:
                        title = val
                        break
            if title:
                break
        return title or ''

    def _run_process_description(self, item: str) -> bool:
        exe = sys.executable or 'python'
        cmd = [exe, 'process_description.py', item, '--skip-runit']
        try:
            self._append_log('  $ ' + ' '.join(cmd))
            rc = subprocess.run(
                cmd,
                cwd=str(self._project_root()),
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
            )
            if rc.stdout:
                self._append_log(rc.stdout)
            if rc.stderr:
                self._append_log(rc.stderr)
            return rc.returncode == 0
        except Exception as exc:
            self._append_log(f'  ERROR: {exc}')
            return False

    def _run_runit(self, item: str) -> bool:
        exe = sys.executable or 'python'
        # Pass the item arg; runit.py will open GUI if Caps Lock is ON
        cmd = [exe, 'runit.py', item]
        try:
            self._append_log('  > ' + ' '.join(cmd) + '  [new console, CapsLock trick]')
            creation_flags = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0

            # Simulate Caps Lock ON so runit opens its GUI even when launched programmatically
            original_caps_on = None
            if os.name == 'nt':
                try:
                    original_caps_on = bool(ctypes.windll.user32.GetKeyState(0x14) & 1)
                    if not original_caps_on:
                        # Toggle Caps Lock ON
                        VK_CAPITAL = 0x14
                        KEYEVENTF_KEYUP = 0x2
                        ctypes.windll.user32.keybd_event(VK_CAPITAL, 0, 0, 0)
                        ctypes.windll.user32.keybd_event(VK_CAPITAL, 0, KEYEVENTF_KEYUP, 0)
                except Exception:
                    original_caps_on = None

            subprocess.Popen(cmd, cwd=str(self._project_root()), creationflags=creation_flags)

            # Restore Caps Lock to original state shortly after spawn
            if os.name == 'nt' and original_caps_on is not None and original_caps_on is False:
                def restore_caps() -> None:
                    try:
                        time.sleep(1.0)
                        VK_CAPITAL = 0x14
                        KEYEVENTF_KEYUP = 0x2
                        ctypes.windll.user32.keybd_event(VK_CAPITAL, 0, 0, 0)
                        ctypes.windll.user32.keybd_event(VK_CAPITAL, 0, KEYEVENTF_KEYUP, 0)
                    except Exception:
                        pass
                threading.Thread(target=restore_caps, daemon=True).start()

            # Do not wait; GUI should continue immediately
            return True
        except Exception as exc:
            self._append_log(f'  ERROR: {exc}')
            return False

    def _project_root(self) -> Path:
        # Assume this script resides in project root (same as process_description.py)
        return Path(__file__).resolve().parent

    def _refresh_results(self, items: List[Tuple[str, str]]) -> None:
        def do_refresh() -> None:
            self.filtered_items = sorted(items, key=lambda it: it[0])
            self.results.delete(0, tk.END)
            for item, title in self.filtered_items:
                display = f"{item}  |  {title or '(no title)'}"
                self.results.insert(tk.END, display)
        # Always marshal to main thread
        self.after(0, do_refresh)


def main() -> None:
    app = SearchAndProcessGUI(items_dir='item_contents')
    app.mainloop()


if __name__ == '__main__':
    main()


