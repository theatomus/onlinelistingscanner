from __future__ import annotations

import json
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Dict, List


class TrainingViewer(tk.Tk):
    def __init__(self, base_dir: str = 'training'):
        super().__init__()
        self.title('Training Dataset & Schema Viewer')
        self.geometry('1200x800')
        self.base_dir = Path(base_dir)
        self.dataset_path = self.base_dir / 'training_dataset.json'
        self.schema_path = self.base_dir / 'schema.json'
        self.report_path = self.base_dir / 'reports' / 'validation_report.json'
        self.lm_path = self.base_dir / 'mini_lm.json'

        self.dataset = []
        self.schema: Dict[str, List[str]] = {}
        self.issues: Dict[str, List[str]] = {}
        self.lm_meta = {}

        self._build_ui()
        self._load_all()

    def _build_ui(self):
        frm = ttk.Frame(self)
        frm.pack(fill='both', expand=True)

        top = ttk.Frame(frm)
        top.pack(fill='x')

        ttk.Button(top, text='Load', command=self._load_all).pack(side='left', padx=4, pady=4)
        ttk.Button(top, text='Save Schema', command=self._save_schema).pack(side='left', padx=4, pady=4)
        ttk.Button(top, text='Export Report (txt)', command=self._export_report_txt).pack(side='left', padx=4, pady=4)
        ttk.Button(top, text='LM Info', command=self._show_lm_info).pack(side='left', padx=4, pady=4)

        paned = ttk.PanedWindow(frm, orient='horizontal')
        paned.pack(fill='both', expand=True)

        # Left: Items list (from dataset)
        left = ttk.Frame(paned)
        self.items_list = tk.Listbox(left)
        self.items_list.pack(fill='both', expand=True)
        self.items_list.bind('<<ListboxSelect>>', self._on_item_selected)
        paned.add(left, weight=1)

        # Center: Schema editor
        center = ttk.Frame(paned)
        center_paned = ttk.PanedWindow(center, orient='vertical')
        center_paned.pack(fill='both', expand=True)

        self.schema_section_combo = ttk.Combobox(center, values=['title', 'specifics', 'metadata', 'description', 'table_shared', 'table_entry_union'])
        self.schema_section_combo.set('title')
        self.schema_section_combo.pack(fill='x', padx=4, pady=4)
        self.schema_section_combo.bind('<<ComboboxSelected>>', lambda e: self._refresh_schema_keys())

        self.schema_keys = tk.Text(center, height=20)
        center_paned.add(self.schema_keys, weight=1)
        paned.add(center, weight=2)

        # Right: Issues for selected item
        right = ttk.Frame(paned)
        self.issues_text = tk.Text(right)
        self.issues_text.pack(fill='both', expand=True)
        paned.add(right, weight=2)

        # Bottom: Item detail (representative values)
        bottom = ttk.Frame(frm)
        bottom.pack(fill='both', expand=True)
        self.item_values = tk.Text(bottom, height=16)
        self.item_values.pack(fill='both', expand=True)

    def _load_all(self):
        try:
            if self.dataset_path.exists():
                self.dataset = json.loads(self.dataset_path.read_text(encoding='utf-8'))
            else:
                self.dataset = []
            if self.schema_path.exists():
                self.schema = json.loads(self.schema_path.read_text(encoding='utf-8')).get('allowed_keys', {})
            else:
                self.schema = {}
            if self.report_path.exists():
                self.issues = json.loads(self.report_path.read_text(encoding='utf-8'))
            else:
                self.issues = {}
            if self.lm_path.exists():
                try:
                    lm_raw = json.loads(self.lm_path.read_text(encoding='utf-8'))
                    self.lm_meta = {k: (len(v) if isinstance(v, dict) else v) for k, v in lm_raw.items() if k in ('n', 'counts', 'context_counts', 'known_values')}
                except Exception:
                    self.lm_meta = {}
            self._refresh_items()
            self._refresh_schema_keys()
        except Exception as e:
            messagebox.showerror('Load Error', str(e))

    def _refresh_items(self):
        self.items_list.delete(0, tk.END)
        for ex in self.dataset:
            self.items_list.insert(tk.END, ex.get('item_number', 'UNKNOWN'))

    def _refresh_schema_keys(self):
        section = self.schema_section_combo.get()
        keys = self.schema.get(section, [])
        self.schema_keys.delete('1.0', tk.END)
        self.schema_keys.insert('1.0', '\n'.join(keys))

    def _on_item_selected(self, _evt=None):
        idxs = self.items_list.curselection()
        if not idxs:
            return
        idx = idxs[0]
        ex = self.dataset[idx]
        item = ex.get('item_number', 'UNKNOWN')
        # Show issues
        self.issues_text.delete('1.0', tk.END)
        for issue in self.issues.get(item, []):
            self.issues_text.insert(tk.END, f"- {issue}\n")
        # Show representative values for quick inspection
        values = ex.get('values', {})
        pretty = json.dumps(values, indent=2)
        self.item_values.delete('1.0', tk.END)
        self.item_values.insert(tk.END, pretty)

    def _save_schema(self):
        try:
            section = self.schema_section_combo.get()
            edited = self.schema_keys.get('1.0', tk.END).splitlines()
            cleaned = [k.strip() for k in edited if k.strip()]
            self.schema.setdefault(section, [])
            self.schema[section] = sorted(set(cleaned))
            payload = {'allowed_keys': self.schema}
            self.schema_path.parent.mkdir(parents=True, exist_ok=True)
            self.schema_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
            messagebox.showinfo('Saved', f'Schema updated at {self.schema_path}')
        except Exception as e:
            messagebox.showerror('Save Error', str(e))

    def _export_report_txt(self):
        try:
            rpt_txt = self.report_path.with_suffix('.txt')
            rpt_txt.parent.mkdir(parents=True, exist_ok=True)
            if not self.issues:
                rpt_txt.write_text('No schema issues found across all items.\n', encoding='utf-8')
            else:
                lines: List[str] = []
                for item, issues in self.issues.items():
                    lines.append(f'ITEM={item}')
                    lines.extend([f'  - {i}' for i in issues])
                    lines.append('')
                rpt_txt.write_text('\n'.join(lines), encoding='utf-8')
            messagebox.showinfo('Exported', f'Wrote {rpt_txt}')
        except Exception as e:
            messagebox.showerror('Export Error', str(e))

    def _show_lm_info(self):
        if not self.lm_meta:
            messagebox.showinfo('Mini-LM', 'No mini-LM found. Run the workflow to build one.')
            return
        info = [
            f"n-gram: {self.lm_meta.get('n', '?')}",
            f"#contexts: {self.lm_meta.get('context_counts', 0)}",  # shows dict size, not sum
            f"#counts: {self.lm_meta.get('counts', 0)}",
            f"#keys with known values: {self.lm_meta.get('known_values', 0)}",
        ]
        messagebox.showinfo('Mini-LM', '\n'.join(info))


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Training data/schema viewer')
    ap.add_argument('--training-dir', default='training')
    args = ap.parse_args()
    app = TrainingViewer(base_dir=args.training_dir)
    app.mainloop()


if __name__ == '__main__':
    main()


