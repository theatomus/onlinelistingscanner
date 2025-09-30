from __future__ import annotations

import os
import sys
import threading
import queue
import subprocess
import json
import time
from pathlib import Path
from typing import Optional, List

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox


def ensure_requests():
    try:
        import requests  # type: ignore
        return requests
    except Exception:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
        import requests  # type: ignore
        return requests


class MasterGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('AI Backend Master Console')
        self.geometry('1200x800')
        self.server_proc: Optional[subprocess.Popen] = None
        self.output_q: queue.Queue[str] = queue.Queue()
        self.poll_output()

        # Defaults
        self.settings_file = Path('training') / 'master_gui_settings.json'
        self.cwd = tk.StringVar(value=str(Path.cwd()))
        self.items_dir = tk.StringVar(value=str(Path('item_contents')))
        self.backups_root = tk.StringVar(value=str(Path('backups') / 'itemcontents'))
        self.training_dir = tk.StringVar(value=str(Path('training')))
        self.server_path = tk.StringVar(value='')
        self.model_path = tk.StringVar(value=str(Path('models') / 'phi-3-mini-4k-instruct-q4_k_m.gguf'))
        self.port = tk.IntVar(value=8080)
        self.ctx = tk.IntVar(value=2048)
        self.threads = tk.IntVar(value=max(1, (os.cpu_count() or 8)))
        self.ngl = tk.IntVar(value=0)
        self.use_web = tk.BooleanVar(value=True)
        self.overwrite_corrections = tk.BooleanVar(value=False)
        # Instructions tuning
        self.instructions_path = tk.StringVar(value=str(Path('training') / 'config' / 'instructions.yaml'))
        self.ai_enable_llm_spec = tk.BooleanVar(value=True)
        self.ai_llm_temp = tk.DoubleVar(value=0.1)
        self.ai_llm_max = tk.IntVar(value=256)
        self.val_enable_llm = tk.BooleanVar(value=True)
        self.val_enable_web = tk.BooleanVar(value=True)
        self.val_llm_temp = tk.DoubleVar(value=0.1)
        self.val_llm_max = tk.IntVar(value=128)
        self.sugg_temp = tk.DoubleVar(value=0.1)
        self.sugg_max = tk.IntVar(value=128)
        self.web_sources_path = tk.StringVar(value=str(Path('training') / 'config' / 'web_sources.json'))

        # Load previous state if available
        self.load_state()

        self.build_ui()
        # Autodetect only if not set
        if not self.server_path.get() or not Path(self.server_path.get()).exists():
            self.autodetect_server()
        if not self.model_path.get() or not Path(self.model_path.get()).exists():
            self.autodetect_model()

        # Save on close
        self.protocol('WM_DELETE_WINDOW', self.on_close)

    # --- UI ---
    def build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill='x')
        ttk.Label(top, text='CWD:').pack(side='left')
        ttk.Entry(top, textvariable=self.cwd, width=80).pack(side='left', padx=4)
        ttk.Button(top, text='Browse', command=self.pick_cwd).pack(side='left')

        # Workflow banner
        banner = ttk.Label(self, text='1) Paths  →  2) LLM Server  →  3) Training  →  4) AI Parsed  →  5) Validation  →  6) Review/Correct',
                           anchor='center', padding=6)
        banner.pack(fill='x')

        nb = ttk.Notebook(self)
        nb.pack(fill='both', expand=True)

        # Reordered tabs to match end-to-end flow
        nb.add(self.build_paths_tab(nb), text='1) Paths')
        nb.add(self.build_server_tab(nb), text='2) LLM Server')
        nb.add(self.build_training_tab(nb), text='3) Training')
        nb.add(self.build_ai_parsed_tab(nb), text='4) AI Parsed')
        nb.add(self.build_validation_tab(nb), text='5) Validation')
        nb.add(self.build_review_tab(nb), text='6) Review/Correct')
        nb.add(self.build_instructions_tab(nb), text='7) Instructions')
        nb.add(self.build_diagnostics_tab(nb), text='8) Diagnostics')

        # Output
        out_frame = ttk.LabelFrame(self, text='Console Output')
        out_frame.pack(fill='both', expand=True, padx=6, pady=6)
        self.out_text = scrolledtext.ScrolledText(out_frame, wrap='word', height=16)
        self.out_text.pack(fill='both', expand=True)

        # Try to autodetect server/model once on startup
        self.autodetect_server()
        self.autodetect_model()

    def build_server_tab(self, parent):
        f = ttk.Frame(parent)
        row = 0
        ttk.Label(f, text='Server EXE:').grid(row=row, column=0, sticky='w')
        ttk.Entry(f, textvariable=self.server_path, width=80).grid(row=row, column=1, sticky='we', padx=4)
        ttk.Button(f, text='Browse', command=self.pick_server).grid(row=row, column=2)
        ttk.Button(f, text='Autodetect', command=self.autodetect_server).grid(row=row, column=3)
        row += 1
        ttk.Label(f, text='Model GGUF:').grid(row=row, column=0, sticky='w')
        ttk.Entry(f, textvariable=self.model_path, width=80).grid(row=row, column=1, sticky='we', padx=4)
        ttk.Button(f, text='Browse', command=self.pick_model).grid(row=row, column=2)
        ttk.Button(f, text='Autodetect', command=self.autodetect_model).grid(row=row, column=3)
        row += 1
        ttk.Label(f, text='Port:').grid(row=row, column=0, sticky='w')
        ttk.Spinbox(f, from_=8000, to=9000, textvariable=self.port, width=8).grid(row=row, column=1, sticky='w')
        ttk.Label(f, text='Ctx:').grid(row=row, column=1, sticky='e')
        ttk.Spinbox(f, from_=512, to=8192, increment=256, textvariable=self.ctx, width=8).grid(row=row, column=1)
        ttk.Label(f, text='Threads:').grid(row=row, column=1, sticky='e', padx=(120,0))
        ttk.Spinbox(f, from_=1, to=128, textvariable=self.threads, width=8).grid(row=row, column=1, sticky='e', padx=(200,0))
        row += 1
        ttk.Label(f, text='ngl (GPU layers):').grid(row=row, column=0, sticky='w')
        ttk.Spinbox(f, from_=0, to=64, textvariable=self.ngl, width=8).grid(row=row, column=1, sticky='w')
        row += 1
        ttk.Button(f, text='Start Server', command=self.start_server).grid(row=row, column=0, pady=6)
        ttk.Button(f, text='Health Check', command=self.health_check).grid(row=row, column=1, pady=6, sticky='w')
        ttk.Button(f, text='Stop Server', command=self.stop_server).grid(row=row, column=2, pady=6)
        f.grid_columnconfigure(1, weight=1)
        return f

    def build_training_tab(self, parent):
        f = ttk.Frame(parent)
        ttk.Label(f, text='Build and refine training artifacts used by validation and AI suggestions.', foreground='#555').pack(anchor='w', padx=6, pady=(0,6))
        b1 = ttk.Button(f, text='Build Keyspace → Schema (parse python_parsed + process_description)', command=self.run_keyspace)
        b1.pack(anchor='w', padx=6, pady=4)
        b2 = ttk.Button(f, text='Build Dataset (item_contents + backups)', command=self.run_dataset)
        b2.pack(anchor='w', padx=6, pady=4)
        b3 = ttk.Button(f, text='Run Workflow (schema + mini‑LM + validate report)', command=self.run_workflow)
        b3.pack(anchor='w', padx=6, pady=4)
        cb_over = ttk.Checkbutton(f, text='Overwrite corrections from python_parsed when importing', variable=self.overwrite_corrections)
        cb_over.pack(anchor='w', padx=10, pady=(2,2))
        b4 = ttk.Button(
            f,
            text='Import python_parsed as Corrections → Merge Schema/Gold → Rebuild mini‑LM',
            command=self.run_corrections_merge,
        )
        b4.pack(anchor='w', padx=6, pady=4)
        # One-click pipeline: start server → retrain (refine+merge+workflow) → generate AI Parsed
        b5 = ttk.Button(
            f,
            text='One‑Click: Start Server → Retrain → Generate AI Parsed',
            command=self.run_one_click_pipeline,
        )
        b5.pack(anchor='w', padx=6, pady=(8,6))
        self.add_tip(b1, 'Harvest all bracketed keys from python_parsed files and source code; merge into training/schema.json')
        self.add_tip(b2, 'Create training/training_dataset.json from all parsed files, including backups')
        self.add_tip(b3, 'Rebuild schema + train mini‑LM and validate all items, producing training/reports')
        self.add_tip(cb_over, 'If checked, existing files in training/corrections will be overwritten with current python_parsed contents')
        self.add_tip(b4, 'Auto-import current/backed-up python_parsed files as corrections, expand schema, write gold_values, and rebuild mini‑LM')
        self.add_tip(b5, 'Runs: start llama server (if needed) → instructions refiner → corrections merge → workflow → generate ai_parsed_* files for all descriptions')
        return f

    def build_validation_tab(self, parent):
        f = ttk.Frame(parent)
        ttk.Label(f, text='Validate items and write AI issues for RunIt to display.', foreground='#555').pack(anchor='w', padx=6, pady=(0,6))
        cb = ttk.Checkbutton(f, text='Use web verification (fetch vendor pages, parse with LLM)', variable=self.use_web)
        cb.pack(anchor='w', padx=6, pady=4)
        b1 = ttk.Button(f, text='Generate AI Issues for ALL items (writes training/live_issues)', command=self.batch_write_issues)
        b1.pack(anchor='w', padx=6, pady=4)
        b2 = ttk.Button(f, text='Validate first item (console only)', command=self.validate_one)
        b2.pack(anchor='w', padx=6, pady=4)
        self.add_tip(cb, 'If on, validator will attempt web verification for missing/wrong keys')
        self.add_tip(b1, 'Run validator on all items and write per‑item issues for RunIt’s “AI Detected Issues”')
        self.add_tip(b2, 'Quick check for the first available item; results printed in the console below')
        return f

    def build_ai_parsed_tab(self, parent):
        f = ttk.Frame(parent)
        ttk.Button(f, text='Generate AI Parsed for ALL descriptions', command=self.batch_ai_parsed).pack(anchor='w', padx=6, pady=6)
        ttk.Label(
            f,
            text='Creates item_contents/ai_parsed_<item>.txt (and legacy ai_python_parsed_<item>.txt) for each *_description.txt'
        ).pack(anchor='w', padx=6)
        return f

    def build_review_tab(self, parent):
        f = ttk.Frame(parent)
        ttk.Label(f, text='Review and correct AI Parsed outputs; saved corrections improve training over time.', foreground='#555').pack(anchor='w', padx=6, pady=(6,6))
        b = ttk.Button(f, text='Open Review & Correction GUI', command=self.open_review_gui)
        b.pack(anchor='w', padx=6, pady=6)
        self.add_tip(b, 'Launches a tool to edit ai_python_parsed_<item>.txt and save corrections to training/corrections')
        return f

    def build_paths_tab(self, parent):
        f = ttk.Frame(parent)
        self._add_path_row(f, 'Items dir:', self.items_dir, 0, self.pick_items)
        self._add_path_row(f, 'Backups root:', self.backups_root, 1, self.pick_backups)
        self._add_path_row(f, 'Training dir:', self.training_dir, 2, self.pick_training)
        return f

    def build_instructions_tab(self, parent):
        f = ttk.Frame(parent)
        row = 0
        ttk.Label(f, text='Instructions YAML:').grid(row=row, column=0, sticky='w')
        ttk.Entry(f, textvariable=self.instructions_path, width=70).grid(row=row, column=1, sticky='we', padx=4)
        ttk.Button(f, text='Browse', command=self.pick_instructions).grid(row=row, column=2)
        ttk.Button(f, text='Reload', command=self.load_instructions_yaml).grid(row=row, column=3)
        row += 1

        ai = ttk.LabelFrame(f, text='AI Parsed')
        ai.grid(row=row, column=0, columnspan=4, sticky='we', padx=6, pady=6)
        ttk.Checkbutton(ai, text='Enable LLM spec extraction', variable=self.ai_enable_llm_spec).grid(row=0, column=0, sticky='w')
        ttk.Label(ai, text='LLM temp:').grid(row=0, column=1, sticky='e')
        ttk.Spinbox(ai, from_=0.0, to=2.0, increment=0.05, textvariable=self.ai_llm_temp, width=6).grid(row=0, column=2, sticky='w')
        ttk.Label(ai, text='max_tokens:').grid(row=0, column=3, sticky='e')
        ttk.Spinbox(ai, from_=32, to=4096, increment=32, textvariable=self.ai_llm_max, width=8).grid(row=0, column=4, sticky='w')

        val = ttk.LabelFrame(f, text='Validator')
        val.grid(row=row+1, column=0, columnspan=4, sticky='we', padx=6, pady=6)
        ttk.Checkbutton(val, text='Enable LLM suggestions (no web verify here)', variable=self.val_enable_llm).grid(row=0, column=0, sticky='w')
        ttk.Checkbutton(val, text='Enable Web verification', variable=self.val_enable_web).grid(row=0, column=1, sticky='w')
        ttk.Label(val, text='LLM temp:').grid(row=0, column=2, sticky='e')
        ttk.Spinbox(val, from_=0.0, to=2.0, increment=0.05, textvariable=self.val_llm_temp, width=6).grid(row=0, column=3, sticky='w')
        ttk.Label(val, text='max_tokens:').grid(row=0, column=4, sticky='e')
        ttk.Spinbox(val, from_=32, to=4096, increment=32, textvariable=self.val_llm_max, width=8).grid(row=0, column=5, sticky='w')

        sug = ttk.LabelFrame(f, text='LLM Suggester')
        sug.grid(row=row+2, column=0, columnspan=4, sticky='we', padx=6, pady=6)
        ttk.Label(sug, text='Temperature:').grid(row=0, column=0, sticky='e')
        ttk.Spinbox(sug, from_=0.0, to=2.0, increment=0.05, textvariable=self.sugg_temp, width=6).grid(row=0, column=1, sticky='w')
        ttk.Label(sug, text='max_tokens:').grid(row=0, column=2, sticky='e')
        ttk.Spinbox(sug, from_=32, to=4096, increment=32, textvariable=self.sugg_max, width=8).grid(row=0, column=3, sticky='w')

        web = ttk.LabelFrame(f, text='Web Verifier')
        web.grid(row=row+3, column=0, columnspan=4, sticky='we', padx=6, pady=6)
        ttk.Label(web, text='Sources JSON:').grid(row=0, column=0, sticky='w')
        ttk.Entry(web, textvariable=self.web_sources_path, width=60).grid(row=0, column=1, sticky='we', padx=4)
        ttk.Button(web, text='Browse', command=self.pick_web_sources).grid(row=0, column=2)

        act = ttk.Frame(f)
        act.grid(row=row+4, column=0, columnspan=4, sticky='we')
        ttk.Button(act, text='Save Instructions', command=self.save_instructions_yaml).pack(side='left', padx=6, pady=6)
        ttk.Button(act, text='Open YAML', command=self.open_instructions_yaml).pack(side='left', padx=6, pady=6)
        ttk.Button(act, text='Suggest label maps from corpus', command=self.suggest_label_maps).pack(side='left', padx=6, pady=6)

        f.grid_columnconfigure(1, weight=1)
        return f

    def build_diagnostics_tab(self, parent):
        f = ttk.Frame(parent)
        self.diag_item = tk.StringVar(value='')
        row = 0
        ttk.Label(f, text='Item number:').grid(row=row, column=0, sticky='w')
        ttk.Entry(f, textvariable=self.diag_item, width=24).grid(row=row, column=1, sticky='w')
        ttk.Button(f, text='Print Item Files', command=self.print_item_files).grid(row=row, column=2, padx=6)
        row += 1
        ttk.Button(f, text='Generate AI Parsed (Item)', command=self.generate_ai_parsed_for_item).grid(row=row, column=0, pady=6, sticky='w')
        ttk.Button(f, text='Rinse & Repeat (Retrain → Generate → Print)', command=self.rinse_repeat_item).grid(row=row, column=1, pady=6, sticky='w')
        ttk.Button(f, text='Save Item Snapshot to File', command=self.save_item_snapshot).grid(row=row, column=2, pady=6, sticky='w')
        self.add_tip(f, 'Utilities to quickly inspect description/python_parsed/ai_parsed for an item and run the retrain cycle.')
        return f

    def _add_path_row(self, parent, label, var, row, browse_cmd):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky='w')
        ttk.Entry(parent, textvariable=var, width=80).grid(row=row, column=1, sticky='we', padx=4)
        btn = ttk.Button(parent, text='Browse', command=browse_cmd)
        btn.grid(row=row, column=2)
        self.add_tip(btn, 'Select a directory; relative paths are resolved against the repo root')
        parent.grid_columnconfigure(1, weight=1)

    # --- Actions ---
    def pick_cwd(self):
        d = filedialog.askdirectory(initialdir=self.cwd.get())
        if d:
            self.cwd.set(d)

    def pick_server(self):
        p = filedialog.askopenfilename(initialdir=self.cwd.get(), title='Select llama-server.exe')
        if p:
            self.server_path.set(p)

    def autodetect_server(self):
        # If already valid, keep it
        cur = self.server_path.get().strip()
        if cur and Path(cur).exists():
            self.append(f'[autodetect] server (kept): {cur}')
            return
        # Try PATH first
        from shutil import which
        w = which('llama-server.exe') or which('llama-server')
        if w:
            self.server_path.set(w)
            self.append(f'[autodetect] server: {w}')
            return
        # Try common locations under current workspace
        candidates = [
            Path(self.cwd.get()) / 'llama-server.exe',
            Path(self.cwd.get()) / 'llama-server',
        ]
        # Scan subfolders shallowly
        base = Path(self.cwd.get())
        for sub in base.glob('*'):
            if sub.is_dir():
                candidates.append(sub / 'llama-server.exe')
                candidates.append(sub / 'llama-server')
        for c in candidates:
            if c.exists():
                self.server_path.set(str(c))
                self.append(f'[autodetect] server: {c}')
                return
        # Recursive search as last resort
        try:
            hit = next(base.rglob('llama-server*.exe'))
            self.server_path.set(str(hit))
            self.append(f'[autodetect] server: {hit}')
            return
        except StopIteration:
            pass
        self.append('[autodetect] server not found')

    def pick_model(self):
        p = filedialog.askopenfilename(initialdir=self.cwd.get(), title='Select model gguf')
        if p:
            self.model_path.set(p)

    def autodetect_model(self):
        # If already valid, keep it
        cur = self.model_path.get().strip()
        if cur and Path(cur).exists():
            self.append(f'[autodetect] model (kept): {cur}')
            return
        # Prefer Phi-3; then Llama-1B; then Qwen-0.5B; then TinyLlama
        models_dir = Path(self.cwd.get()) / 'models'
        candidates = [
            models_dir / 'phi-3-mini-4k-instruct-q4_k_m.gguf',
            models_dir / 'Llama-3.2-1B-Instruct-Q4_K_M.gguf',
            models_dir / 'Qwen2.5-0.5B-Instruct-Q4_K_M.gguf',
            models_dir / 'tinyllama-1.1b-chat-v1.0-q4_k_m.gguf',
        ]
        for c in candidates:
            if c.exists():
                self.model_path.set(str(c))
                self.append(f'[autodetect] model: {c}')
                return
        # Fallback: any gguf under models
        if models_dir.exists():
            any_gguf = list(models_dir.glob('*.gguf')) or list(models_dir.rglob('*.gguf'))
            if any_gguf:
                self.model_path.set(str(any_gguf[0]))
                self.append(f'[autodetect] model: {any_gguf[0]}')
                return
        self.append('[autodetect] model not found')

    # --- Persistence ---
    def load_state(self):
        try:
            if self.settings_file.exists():
                data = json.loads(self.settings_file.read_text(encoding='utf-8'))
                self.cwd.set(data.get('cwd', self.cwd.get()))
                self.items_dir.set(data.get('items_dir', self.items_dir.get()))
                self.backups_root.set(data.get('backups_root', self.backups_root.get()))
                self.training_dir.set(data.get('training_dir', self.training_dir.get()))
                self.server_path.set(data.get('server_path', self.server_path.get()))
                self.model_path.set(data.get('model_path', self.model_path.get()))
                self.port.set(int(data.get('port', self.port.get())))
                self.ctx.set(int(data.get('ctx', self.ctx.get())))
                self.threads.set(int(data.get('threads', self.threads.get())))
                self.ngl.set(int(data.get('ngl', self.ngl.get())))
                self.use_web.set(bool(data.get('use_web', self.use_web.get())))
                self.overwrite_corrections.set(bool(data.get('overwrite_corrections', self.overwrite_corrections.get())))
                # tuning
                self.instructions_path.set(data.get('instructions_path', self.instructions_path.get()))
                self.ai_enable_llm_spec.set(bool(data.get('ai_enable_llm_spec', self.ai_enable_llm_spec.get())))
                self.ai_llm_temp.set(float(data.get('ai_llm_temp', self.ai_llm_temp.get())))
                self.ai_llm_max.set(int(data.get('ai_llm_max', self.ai_llm_max.get())))
                self.val_enable_llm.set(bool(data.get('val_enable_llm', self.val_enable_llm.get())))
                self.val_enable_web.set(bool(data.get('val_enable_web', self.val_enable_web.get())))
                self.val_llm_temp.set(float(data.get('val_llm_temp', self.val_llm_temp.get())))
                self.val_llm_max.set(int(data.get('val_llm_max', self.val_llm_max.get())))
                self.sugg_temp.set(float(data.get('sugg_temp', self.sugg_temp.get())))
                self.sugg_max.set(int(data.get('sugg_max', self.sugg_max.get())))
                self.web_sources_path.set(data.get('web_sources_path', self.web_sources_path.get()))
        except Exception:
            pass

    def save_state(self):
        try:
            self.settings_file.parent.mkdir(parents=True, exist_ok=True)
            data = {
                'cwd': self.cwd.get(),
                'items_dir': self.items_dir.get(),
                'backups_root': self.backups_root.get(),
                'training_dir': self.training_dir.get(),
                'server_path': self.server_path.get(),
                'model_path': self.model_path.get(),
                'port': self.port.get(),
                'ctx': self.ctx.get(),
                'threads': self.threads.get(),
                'ngl': self.ngl.get(),
                'use_web': self.use_web.get(),
                'overwrite_corrections': self.overwrite_corrections.get(),
                'instructions_path': self.instructions_path.get(),
                'ai_enable_llm_spec': self.ai_enable_llm_spec.get(),
                'ai_llm_temp': self.ai_llm_temp.get(),
                'ai_llm_max': self.ai_llm_max.get(),
                'val_enable_llm': self.val_enable_llm.get(),
                'val_enable_web': self.val_enable_web.get(),
                'val_llm_temp': self.val_llm_temp.get(),
                'val_llm_max': self.val_llm_max.get(),
                'sugg_temp': self.sugg_temp.get(),
                'sugg_max': self.sugg_max.get(),
                'web_sources_path': self.web_sources_path.get(),
            }
            self.settings_file.write_text(json.dumps(data, indent=2), encoding='utf-8')
            self.append(f"[saved] {self.settings_file}")
        except Exception as e:
            self.append(f"[save error] {e}")

    def on_close(self):
        self.save_state()
        self.destroy()

    def pick_items(self):
        d = filedialog.askdirectory(initialdir=self.cwd.get(), title='Select items directory')
        if d:
            self.items_dir.set(d)

    def pick_backups(self):
        d = filedialog.askdirectory(initialdir=self.cwd.get(), title='Select backups root')
        if d:
            self.backups_root.set(d)

    def pick_training(self):
        d = filedialog.askdirectory(initialdir=self.cwd.get(), title='Select training directory')
        if d:
            self.training_dir.set(d)

    def append(self, text: str):
        self.out_text.insert('end', text + '\n')
        self.out_text.see('end')

    # --- Tooltips ---
    def add_tip(self, widget, text: str):
        tip = _ToolTip(widget, text)
        widget.bind('<Enter>', tip.show)
        widget.bind('<Leave>', tip.hide)

    def enqueue(self, text: str):
        self.output_q.put(text)

    def poll_output(self):
        try:
            while True:
                line = self.output_q.get_nowait()
                self.append(line)
        except queue.Empty:
            pass
        self.after(100, self.poll_output)

    def get_repo_root(self) -> Path:
        """Find the repository root that contains the 'tools/training' package and process_description.py.
        Searches current CWD and up to two parent directories.
        """
        start = Path(self.cwd.get()).resolve()
        candidates = [start, start.parent, start.parent.parent]
        for p in candidates:
            if (p / 'tools' / 'training' / '__init__.py').exists():
                return p
        # Fallback to current
        return start

    def resolve_path(self, p: str) -> Path:
        path = Path(p)
        if path.is_absolute():
            return path
        return self.get_repo_root() / path

    def run_async(self, args: List[str]):
        # Run a Python process with given args in selected CWD
        cwd = Path(self.cwd.get())
        repo_root = self.get_repo_root()
        def _runner():
            try:
                self.enqueue(f"[run] {' '.join(args)}")
                # Ensure module imports resolve even if Python resolves cwd differently
                env = os.environ.copy()
                # Prepend repo root so 'tools.training' resolves
                env['PYTHONPATH'] = str(repo_root) + (os.pathsep + env.get('PYTHONPATH','') if env.get('PYTHONPATH') else '')
                proc = subprocess.Popen(args, cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                assert proc.stdout is not None
                for line in proc.stdout:
                    self.enqueue(line.rstrip())
                rc = proc.wait()
                self.enqueue(f"[done] rc={rc}")
            except Exception as e:
                self.enqueue(f"[error] {e}")
        threading.Thread(target=_runner, daemon=True).start()

    def run_sequence(self, commands: List[List[str]], on_done: Optional[callable] = None):
        cwd = Path(self.cwd.get())
        repo_root = self.get_repo_root()
        def _runner():
            try:
                env = os.environ.copy()
                env['PYTHONPATH'] = str(repo_root) + (os.pathsep + env.get('PYTHONPATH','') if env.get('PYTHONPATH') else '')
                for args in commands:
                    self.enqueue(f"[run] {' '.join(args)}")
                    proc = subprocess.Popen(args, cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        self.enqueue(line.rstrip())
                    rc = proc.wait()
                    self.enqueue(f"[done] rc={rc}")
                if on_done:
                    on_done()
            except Exception as e:
                self.enqueue(f"[error] {e}")
        threading.Thread(target=_runner, daemon=True).start()

    def start_server(self):
        if self.server_proc and self.server_proc.poll() is None:
            messagebox.showinfo('Info', 'Server already running')
            return
        server = self.server_path.get().strip()
        model = self.model_path.get().strip()
        if not server or not Path(server).exists():
            messagebox.showerror('Error', 'Invalid server path')
            return
        if not model or not Path(model).exists():
            messagebox.showerror('Error', 'Invalid model path')
            return
        args = [server, '-m', model, '-c', str(self.ctx.get()), '-t', str(self.threads.get()), '-ngl', str(self.ngl.get()), '--port', str(self.port.get())]
        self.append('[start] ' + ' '.join(args))
        try:
            self.server_proc = subprocess.Popen(args, cwd=str(Path(self.cwd.get())), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.append('[ok] server started')
        except Exception as e:
            messagebox.showerror('Error', f'Failed to start server: {e}')

    def run_one_click_pipeline(self):
        # 1) Ensure server is running
        if not (self.server_proc and self.server_proc.poll() is None):
            self.start_server()
        else:
            self.append('[info] server already running')
        # 2) Refine instructions
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        instr = str(self.resolve_path(self.instructions_path.get()))
        self.run_async([sys.executable, '-m', 'tools.training.instructions_refiner', '--items-dir', items_dir, '--backups-root', backups, '--instructions', instr])
        # 3) Corrections merge (optionally overwrite)
        training = self.resolve_path(self.training_dir.get())
        args = [
            sys.executable, '-m', 'tools.training.corrections_merger',
            '--items-dir', items_dir,
            '--backups-root', backups,
            '--corrections-dir', str(training / 'corrections'),
            '--schema', str(training / 'schema.json'),
            '--gold-values', str(training / 'gold_values.json'),
            '--dataset', str(training / 'training_dataset.json'),
            '--mini-lm-out', str(training / 'mini_lm.json'),
        ]
        if self.overwrite_corrections.get():
            args.append('--overwrite')
        self.run_async(args)
        # 4) Full workflow (dataset+schema+mini-LM+validate)
        self.run_async([sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', items_dir, '--backups-root', backups, '--out-dir', str(training)])
        # 5) Generate AI Parsed for all descriptions
        self.batch_ai_parsed()

    def stop_server(self):
        if self.server_proc and self.server_proc.poll() is None:
            try:
                self.server_proc.terminate()
                self.append('[ok] server terminated')
            except Exception as e:
                self.append(f'[error] {e}')
        else:
            self.append('[info] no server running')

    def health_check(self):
        requests = ensure_requests()
        base = f"http://127.0.0.1:{self.port.get()}"
        body = {
            'model': 'local',
            'messages': [
                {'role': 'system', 'content': 'Return JSON only: {"ok":true}'},
                {'role': 'user', 'content': 'ping'},
            ],
            'response_format': {'type': 'json_object'},
            'temperature': 0.1,
            'max_tokens': 16,
        }
        try:
            r = requests.post(base + '/v1/chat/completions', json=body, timeout=10)
            self.append(f'[health] {r.status_code} {r.text[:200]}')
        except Exception as e:
            self.append(f'[health] error: {e}')

    def run_keyspace(self):
        proc_src = self.get_repo_root() / 'process_description.py'
        items_dir = str(self.resolve_path(self.items_dir.get()))
        out_keys = str(self.resolve_path(self.training_dir.get()) / 'all_keys.json')
        schema = str(self.resolve_path(self.training_dir.get()) / 'schema.json')
        self.run_async([sys.executable, '-m', 'tools.training.keyspace_builder', '--items-dir', items_dir, '--process-src', str(proc_src), '--out-keys', out_keys, '--schema', schema])

    def run_dataset(self):
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        out = str(self.resolve_path(self.training_dir.get()) / 'training_dataset.json')
        self.run_async([sys.executable, '-m', 'tools.training.training_data_builder', '--items-dir', items_dir, '--backups-root', backups, '--out', out])

    def run_workflow(self):
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        outdir = str(self.resolve_path(self.training_dir.get()))
        self.run_async([sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', items_dir, '--backups-root', backups, '--out-dir', outdir])

    def run_corrections_merge(self):
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        training = self.resolve_path(self.training_dir.get())
        args = [
            sys.executable, '-m', 'tools.training.corrections_merger',
            '--items-dir', items_dir,
            '--backups-root', backups,
            '--corrections-dir', str(training / 'corrections'),
            '--schema', str(training / 'schema.json'),
            '--gold-values', str(training / 'gold_values.json'),
            '--dataset', str(training / 'training_dataset.json'),
            '--mini-lm-out', str(training / 'mini_lm.json'),
        ]
        if self.overwrite_corrections.get():
            args.append('--overwrite')
        self.run_async(args)

    def validate_one(self):
        # Validate first file in items_dir
        items = sorted(self.resolve_path(self.items_dir.get()).glob('python_parsed_*.txt'))
        if not items:
            messagebox.showerror('Error', 'No python_parsed_*.txt found')
            return
        item = str(items[0])
        args = [sys.executable, '-m', 'tools.training.ai_validator', 'validate', item, '--schema', str(self.resolve_path(self.training_dir.get()) / 'schema.json'), '--lm', str(self.resolve_path(self.training_dir.get()) / 'mini_lm.json'), '--llm-url', f'http://127.0.0.1:{self.port.get()}']
        # Web verify disabled
        self.run_async(args)

    def batch_write_issues(self):
        # Iterate items and write AI issues
        items = sorted(self.resolve_path(self.items_dir.get()).glob('python_parsed_*.txt'))
        if not items:
            messagebox.showerror('Error', 'No python_parsed_*.txt found')
            return
        for fp in items:
            item_num = fp.stem.replace('python_parsed_','')
            args = [sys.executable, '-m', 'tools.training.write_ai_issues', item_num, '--items-dir', str(self.resolve_path(self.items_dir.get())), '--schema', str(self.resolve_path(self.training_dir.get()) / 'schema.json'), '--lm', str(self.resolve_path(self.training_dir.get()) / 'mini_lm.json'), '--llm-url', f'http://127.0.0.1:{self.port.get()}']
            # Web verify disabled
            self.run_async(args)

    def batch_ai_parsed(self):
        # Iterate *_description.txt in items_dir (shallow + recursive) and backups_root recursively
        items_base = self.resolve_path(self.items_dir.get())
        backups_base = self.resolve_path(self.backups_root.get())
        descs = set()
        # Shallow
        descs.update(items_base.glob('*_description.txt'))
        # Recursive under items_dir
        descs.update(items_base.rglob('*_description.txt'))
        # Recursive under backups root (if exists)
        if backups_base.exists():
            descs.update(backups_base.rglob('*_description.txt'))
        descs = sorted(descs)
        if not descs:
            self.append(f"[warn] No *_description.txt found under {items_base} or {backups_base}")
            messagebox.showerror('Error', 'No *_description.txt found in items dir or backups root')
            return
        for dp in descs:
            args = [sys.executable, '-m', 'tools.training.ai_python_parsed', str(dp), '--items-dir', self.items_dir.get(), '--llm-url', f'http://127.0.0.1:{self.port.get()}']
            self.run_async(args)

    def open_review_gui(self):
        # Launch the separate review GUI
        items_dir = str(self.resolve_path(self.items_dir.get()))
        corrections_dir = str(self.resolve_path(self.training_dir.get()) / 'corrections')
        self.run_async([sys.executable, '-m', 'tools.training.ai_review_gui', '--items-dir', items_dir, '--corrections-dir', corrections_dir])

    # --- Diagnostics helpers ---
    def _read_file_safely(self, path: Path) -> str:
        try:
            return path.read_text(encoding='utf-8', errors='replace')
        except Exception:
            try:
                return path.read_text(errors='replace')
            except Exception as e:
                return f"[error] {e}"

    def print_item_files(self):
        item = self.diag_item.get().strip()
        if not item:
            messagebox.showerror('Error', 'Enter an item number')
            return
        base = self.resolve_path(self.items_dir.get())
        paths = [
            (base / f"{item}_description.txt", 'DESCRIPTION'),
            (base / f"python_parsed_{item}.txt", 'PYTHON_PARSED'),
            (base / f"ai_parsed_{item}.txt", 'AI_PARSED'),
        ]
        # Fallback to legacy ai filename
        if not paths[2][0].exists():
            paths[2] = (base / f"ai_python_parsed_{item}.txt", 'AI_PARSED (legacy)')
        self.append(f"===== PRINT ITEM {item} =====")
        for p, label in paths:
            self.append(f"----- {label}: {p}")
            if p.exists():
                self.append(self._read_file_safely(p))
            else:
                self.append(f"[missing] {p}")
        self.append(f"===== END ITEM {item} =====")

    def save_item_snapshot(self):
        item = self.diag_item.get().strip()
        if not item:
            messagebox.showerror('Error', 'Enter an item number')
            return
        base = self.resolve_path(self.items_dir.get())
        paths = [
            (base / f"{item}_description.txt", 'DESCRIPTION'),
            (base / f"python_parsed_{item}.txt", 'PYTHON_PARSED'),
            (base / f"ai_parsed_{item}.txt", 'AI_PARSED'),
        ]
        if not paths[2][0].exists():
            paths[2] = (base / f"ai_python_parsed_{item}.txt", 'AI_PARSED (legacy)')
        out_dir = self.resolve_path(self.training_dir.get()) / 'diagnostics'
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"item_{item}_snapshot.txt"
        try:
            lines = [f"===== PRINT ITEM {item} =====\n"]
            for p, label in paths:
                lines.append(f"----- {label}: {p}\n")
                if p.exists():
                    lines.append(self._read_file_safely(p) + "\n")
                else:
                    lines.append(f"[missing] {p}\n")
            lines.append(f"===== END ITEM {item} =====\n")
            out_path.write_text(''.join(lines), encoding='utf-8')
            self.append(f"[saved] {out_path}")
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def generate_ai_parsed_for_item(self):
        item = self.diag_item.get().strip()
        if not item:
            messagebox.showerror('Error', 'Enter an item number')
            return
        args = [sys.executable, '-m', 'tools.training.ai_python_parsed', item, '--items-dir', self.items_dir.get(), '--llm-url', f'http://127.0.0.1:{self.port.get()}']
        self.run_async(args)

    def rinse_repeat_item(self):
        item = self.diag_item.get().strip()
        if not item:
            messagebox.showerror('Error', 'Enter an item number')
            return
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        instr = str(self.resolve_path(self.instructions_path.get()))
        training = self.resolve_path(self.training_dir.get())
        seq = [
            [sys.executable, '-m', 'tools.training.instructions_refiner', '--items-dir', items_dir, '--backups-root', backups, '--instructions', instr],
            [sys.executable, '-m', 'tools.training.corrections_merger', '--items-dir', items_dir, '--backups-root', backups, '--corrections-dir', str(training / 'corrections'), '--schema', str(training / 'schema.json'), '--gold-values', str(training / 'gold_values.json'), '--dataset', str(training / 'training_dataset.json'), '--mini-lm-out', str(training / 'mini_lm.json')] + (['--overwrite'] if self.overwrite_corrections.get() else []),
            [sys.executable, '-m', 'tools.training.run_workflow', '--items-dir', items_dir, '--backups-root', backups, '--out-dir', str(training)],
            [sys.executable, '-m', 'tools.training.ai_python_parsed', item, '--items-dir', items_dir, '--llm-url', f'http://127.0.0.1:{self.port.get()}'],
        ]
        def _on_done():
            try:
                self.print_item_files()
            except Exception:
                pass
        self.run_sequence(seq, on_done=_on_done)

    # --- Instructions helpers ---
    def ensure_yaml(self):
        try:
            import yaml  # type: ignore
            return yaml
        except Exception:
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyyaml'])
                import yaml  # type: ignore
                return yaml
            except Exception as e:
                messagebox.showerror('Error', f'Failed to install pyyaml: {e}')
                raise

    def pick_instructions(self):
        p = filedialog.askopenfilename(initialdir=self.cwd.get(), title='Select instructions.yaml', filetypes=[('YAML','*.yaml;*.yml'),('All','*.*')])
        if p:
            self.instructions_path.set(p)

    def pick_web_sources(self):
        p = filedialog.askopenfilename(initialdir=self.cwd.get(), title='Select web_sources.json', filetypes=[('JSON','*.json'),('All','*.*')])
        if p:
            self.web_sources_path.set(p)

    def load_instructions_yaml(self):
        yaml = self.ensure_yaml()
        path = Path(self.instructions_path.get())
        if not path.exists():
            messagebox.showwarning('Warning', f'Instructions file not found: {path}')
            return
        try:
            data = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
            api = data.get('ai_python_parsed', {})
            val = data.get('validator', {})
            sug = data.get('llm_suggester', {})
            web = data.get('web_verifier', {})
            self.ai_enable_llm_spec.set(bool(api.get('enable_llm_spec_extraction', True)))
            self.ai_llm_temp.set(float(api.get('llm', {}).get('temperature', 0.1)))
            self.ai_llm_max.set(int(api.get('llm', {}).get('max_tokens', 256)))
            self.val_enable_llm.set(bool(val.get('enable_llm_suggest', True)))
            self.val_enable_web.set(bool(val.get('enable_web_verify', True)))
            self.val_llm_temp.set(float(val.get('llm', {}).get('temperature', 0.1)))
            self.val_llm_max.set(int(val.get('llm', {}).get('max_tokens', 128)))
            self.sugg_temp.set(float(sug.get('temperature', 0.1)))
            self.sugg_max.set(int(sug.get('max_tokens', 128)))
            if web.get('sources_config'):
                self.web_sources_path.set(str(web.get('sources_config')))
            self.append(f"[instructions] loaded {path}")
        except Exception as e:
            messagebox.showerror('Error', f'Failed to load instructions: {e}')

    def save_instructions_yaml(self):
        yaml = self.ensure_yaml()
        path = Path(self.instructions_path.get())
        try:
            base = {}
            if path.exists():
                try:
                    base = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
                except Exception:
                    base = {}
            base.setdefault('ai_python_parsed', {})
            base['ai_python_parsed']['enable_llm_spec_extraction'] = bool(self.ai_enable_llm_spec.get())
            base['ai_python_parsed'].setdefault('llm', {})
            base['ai_python_parsed']['llm']['temperature'] = float(self.ai_llm_temp.get())
            base['ai_python_parsed']['llm']['max_tokens'] = int(self.ai_llm_max.get())

            base.setdefault('validator', {})
            base['validator']['enable_llm_suggest'] = bool(self.val_enable_llm.get())
            base['validator']['enable_web_verify'] = bool(self.val_enable_web.get())
            base['validator'].setdefault('llm', {})
            base['validator']['llm']['temperature'] = float(self.val_llm_temp.get())
            base['validator']['llm']['max_tokens'] = int(self.val_llm_max.get())

            base.setdefault('llm_suggester', {})
            base['llm_suggester']['temperature'] = float(self.sugg_temp.get())
            base['llm_suggester']['max_tokens'] = int(self.sugg_max.get())

            base.setdefault('web_verifier', {})
            base['web_verifier']['sources_config'] = self.web_sources_path.get()

            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(yaml.safe_dump(base, sort_keys=False), encoding='utf-8')
            self.append(f"[instructions] saved {path}")
        except Exception as e:
            messagebox.showerror('Error', f'Failed to save instructions: {e}')

    def open_instructions_yaml(self):
        try:
            os.startfile(self.instructions_path.get())
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def suggest_label_maps(self):
        items_dir = str(self.resolve_path(self.items_dir.get()))
        backups = str(self.resolve_path(self.backups_root.get()))
        instr = str(self.resolve_path(self.instructions_path.get()))
        args = [
            sys.executable, '-m', 'tools.training.instructions_refiner',
            '--items-dir', items_dir,
            '--backups-root', backups,
            '--instructions', instr,
        ]
        self.run_async(args)


class _ToolTip:
    def __init__(self, widget, text: str):
        self.widget = widget
        self.text = text
        self.tip = None

    def show(self, _evt=None):
        if self.tip or not self.text:
            return
        # Position near mouse pointer
        x = self.widget.winfo_pointerx() + 12
        y = self.widget.winfo_pointery() + 12
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f'+{x}+{y}')
        lbl = tk.Label(self.tip, text=self.text, justify='left', background='#FFFFE0', relief='solid', borderwidth=1, padx=6, pady=4)
        lbl.pack()

    def hide(self, _evt=None):
        if self.tip:
            self.tip.destroy()
            self.tip = None


def main():
    app = MasterGUI()
    app.mainloop()


if __name__ == '__main__':
    main()


