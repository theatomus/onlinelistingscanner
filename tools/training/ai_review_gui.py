from __future__ import annotations

import json
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext


class AIReviewApp(tk.Tk):
    def __init__(self, items_dir: str = 'item_contents', corrections_dir: str = 'training/corrections'):
        super().__init__()
        self.title('AI Parsed Review & Correction')
        self.geometry('1100x750')
        self.items_dir = Path(items_dir)
        self.corrections_dir = Path(corrections_dir)
        self.corrections_dir.mkdir(parents=True, exist_ok=True)

        top = ttk.Frame(self)
        top.pack(fill='x', padx=6, pady=6)
        ttk.Label(top, text='Items dir:').pack(side='left')
        self.items_var = tk.StringVar(value=str(self.items_dir))
        ttk.Entry(top, textvariable=self.items_var, width=60).pack(side='left', padx=4)
        ttk.Button(top, text='Browse', command=self.browse_items).pack(side='left')
        ttk.Button(top, text='Reload', command=self.load_list).pack(side='left', padx=6)

        main = ttk.PanedWindow(self, orient='horizontal')
        main.pack(fill='both', expand=True)

        left = ttk.Frame(main)
        self.listbox = tk.Listbox(left)
        self.listbox.pack(fill='both', expand=True)
        self.listbox.bind('<<ListboxSelect>>', self.on_select)
        main.add(left, weight=1)

        right = ttk.Frame(main)
        right_pane = ttk.PanedWindow(right, orient='vertical')
        right_pane.pack(fill='both', expand=True)

        # Description text
        desc_frame = ttk.LabelFrame(right_pane, text='Description Text')
        self.desc_text = scrolledtext.ScrolledText(desc_frame, wrap='word', height=18)
        self.desc_text.pack(fill='both', expand=True)
        right_pane.add(desc_frame, weight=2)

        # AI Parsed (editable)
        ai_frame = ttk.LabelFrame(right_pane, text='AI Parsed (editable)')
        self.ai_text = scrolledtext.ScrolledText(ai_frame, wrap='word', height=14)
        self.ai_text.pack(fill='both', expand=True)
        right_pane.add(ai_frame, weight=2)

        # Buttons
        btns = ttk.Frame(right)
        btns.pack(fill='x')
        ttk.Button(btns, text='Save Correction', command=self.save_correction).pack(side='left', padx=4, pady=6)
        ttk.Button(btns, text='Load Correction', command=self.load_correction).pack(side='left', padx=4, pady=6)
        ttk.Button(btns, text='Open AI Parsed File', command=self.open_ai_file).pack(side='left', padx=4, pady=6)
        main.add(right, weight=3)

        self.current_item: str | None = None
        self.load_list()

    def browse_items(self):
        d = filedialog.askdirectory(initialdir=self.items_var.get())
        if d:
            self.items_var.set(d)
            self.items_dir = Path(d)
            self.load_list()

    def load_list(self):
        self.listbox.delete(0, tk.END)
        items = []
        for p in sorted(Path(self.items_var.get()).glob('*_description.txt')):
            items.append(p.stem.replace('_description',''))
        for it in items:
            self.listbox.insert(tk.END, it)
        if not items:
            messagebox.showwarning('No files', 'No *_description.txt found in selected directory')

    def on_select(self, _evt=None):
        sel = self.listbox.curselection()
        if not sel:
            return
        item = self.listbox.get(sel[0])
        self.current_item = item
        # Load description
        desc_path = Path(self.items_var.get()) / f'{item}_description.txt'
        try:
            self.desc_text.delete('1.0', tk.END)
            self.desc_text.insert('1.0', desc_path.read_text(encoding='utf-8', errors='replace'))
        except Exception as e:
            self.desc_text.delete('1.0', tk.END)
            self.desc_text.insert('1.0', f'[error] {e}')
        # Load AI parsed (prefer new name, fallback to legacy)
        ai_path_new = Path(self.items_var.get()) / f'ai_parsed_{item}.txt'
        ai_path_legacy = Path(self.items_var.get()) / f'ai_python_parsed_{item}.txt'
        try:
            self.ai_text.delete('1.0', tk.END)
            if ai_path_new.exists():
                self.ai_text.insert('1.0', ai_path_new.read_text(encoding='utf-8', errors='replace'))
            elif ai_path_legacy.exists():
                self.ai_text.insert('1.0', ai_path_legacy.read_text(encoding='utf-8', errors='replace'))
            else:
                self.ai_text.insert('1.0', '# No ai_parsed file yet. Use the Master GUI to generate or paste content here and Save Correction.')
        except Exception as e:
            self.ai_text.delete('1.0', tk.END)
            self.ai_text.insert('1.0', f'[error] {e}')

    def save_correction(self):
        if not self.current_item:
            messagebox.showerror('Error', 'No item selected')
            return
        txt = self.ai_text.get('1.0', tk.END)
        # Keep as plain text; store under corrections with .txt
        corr_path = self.corrections_dir / f'{self.current_item}.txt'
        try:
            self.corrections_dir.mkdir(parents=True, exist_ok=True)
            corr_path.write_text(txt, encoding='utf-8')
            messagebox.showinfo('Saved', f'Saved correction: {corr_path}')
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def load_correction(self):
        if not self.current_item:
            messagebox.showerror('Error', 'No item selected')
            return
        corr_path = self.corrections_dir / f'{self.current_item}.txt'
        if corr_path.exists():
            self.ai_text.delete('1.0', tk.END)
            self.ai_text.insert('1.0', corr_path.read_text(encoding='utf-8', errors='replace'))
        else:
            messagebox.showinfo('Info', 'No saved correction for this item')

    def open_ai_file(self):
        if not self.current_item:
            return
        ai_path = Path(self.items_var.get()) / f'ai_python_parsed_{self.current_item}.txt'
        try:
            os.startfile(ai_path)
        except Exception:
            messagebox.showinfo('Info', 'AI file does not exist or cannot be opened')


def main():
    import argparse
    ap = argparse.ArgumentParser(description='AI Parsed Review & Correction GUI')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--corrections-dir', default='training/corrections')
    args = ap.parse_args()
    app = AIReviewApp(items_dir=args.items_dir, corrections_dir=args.corrections_dir)
    app.mainloop()


if __name__ == '__main__':
    main()


