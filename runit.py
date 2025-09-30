import os
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
PROCESSING_LOGS_DIR = os.path.join(LOGS_DIR, "processing")
BACKUPS_DIR = os.path.join(os.path.dirname(__file__), "backups")
CONFIGS_DIR = os.path.join(os.path.dirname(__file__), "configs")
# Ensure a consistent, script-relative reports directory (to align with scan_monitor.py)
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(PROCESSING_LOGS_DIR, exist_ok=True)
os.makedirs(BACKUPS_DIR, exist_ok=True)
os.makedirs(CONFIGS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

def str_to_bool(value: str) -> bool:
    """Best-effort string-to-bool parser for env/CLI values."""
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "t", "yes", "y", "on"}

def is_zscrape_context() -> bool:
    """Best-effort detection whether this process was launched by a zscrape runner."""
    # Explicit env from launchers
    if str_to_bool(os.getenv('ZSCRAPE', '')) or str_to_bool(os.getenv('ZSCRAPE_CONTEXT', '')) or str_to_bool(os.getenv('ZSCRAPE_ACTIVE', '')):
        return True
    # Process ancestry heuristic (Windows): look for 'zscrape' in parent cmdlines
    try:
        import psutil  # type: ignore
        current = psutil.Process(os.getpid())
        for parent in current.parents():
            try:
                name = (parent.name() or '').lower()
            except Exception:
                name = ''
            try:
                cmdline = ' '.join(parent.cmdline() or []).lower()
            except Exception:
                cmdline = ''
            if 'zscrape' in name or 'zscrape' in cmdline:
                return True
            if 'autohotkey' in name and 'zscrape' in cmdline:
                return True
    except Exception:
        # psutil not available or access denied; fall back to False
        pass
    return False

# Import standardized SKU utilities if available
try:
    from sku_utils import (extract_sku_parts as std_extract_sku_parts, 
                          format_sku as std_format_sku,
                          ENABLE_STANDARDIZED_SKU_HANDLING)
    USE_STANDARDIZED_SKU_HANDLING = ENABLE_STANDARDIZED_SKU_HANDLING
except ImportError:
    USE_STANDARDIZED_SKU_HANDLING = False
import tkinter as tk
from tkinter import ttk, scrolledtext, font, messagebox, simpledialog
from pathlib import Path
from collections import Counter
import tkinterdnd2 as tkinterdnd
import textwrap
import re
import logging
import webbrowser
import platform
import subprocess
import json
import uuid
import ast
from datetime import datetime, timedelta
import shutil
from importlib.metadata import version
import time
import sys
import subprocess
import ctypes
import difflib
import winsound
from collections import defaultdict
from itertools import product
import pyperclip
import urllib.parse  # For URL-encoding search keywords

from package_validation_helpers import (
    load_typical_box_sizes,
    find_model_override_rule,
    dimensions_match_typical,
)

from package_validation import check_package_validation, set_validation_logger_session
# Optional annotation of SKUs with real names
try:
    from name_utils import annotate_sku_with_name
except ImportError:
    def annotate_sku_with_name(sku: str):
        return sku

# Database imports for SQLite integration
try:
    from listing_database import get_database, ListingDatabase
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("Warning: Database module not available, running in file-only mode")

if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')      # type: ignore[attr-defined]
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')      # type: ignore[attr-defined]

from comparisons import (
    compare_title_vs_specifics,
    compare_title_vs_table,
    compare_specifics_vs_table,
    compare_title_vs_metadata,
    compare_specifics_vs_metadata,
    compare_multi_item_lists
)

sound_played = False
duplicate_titles = {}
duplicate_skus = {}
preload_session_ids = {}
is_argument_mode = True
send_message_flag = True
RULES_FILE = "equivalence_rules.json"
BLACKLIST_FILE = "blacklist.json"
WHITELIST_FILE = "whitelist.json"

# Database configuration flags
ENABLE_DATABASE_MODE = False and DATABASE_AVAILABLE  # Enable database reading
FALLBACK_TO_FILES = True  # Re-enabled - Smart fallback for maximum reliability

# Database-specific global variables
parsed_data_db = {}        # Cache database records by item_number
comparison_cache_db = {}   # Cache comparison results from database
database_stats = {}        # Cache database statistics
db_connection = None       # Global database connection

SUPPRESS_INVALID_START_BYTE = True  # Set to False to allow 'invalid start byte' errors to raise normally

# Define RuleType class for predefined rules
class RuleType:
    def __init__(self, name, param_names=None):
        self.name = name
        self.param_names = param_names or []
    def generate_func(self, params):
        return f"lambda v1, v2: v1 == v2  # {self.name} with {params}"

# Predefined rule types
rule_types = [
    RuleType("Exact Match"),
    RuleType("Case-Insensitive Match"),
    RuleType("Substring Match", ["substring"]),
    RuleType("Custom Rule")
]

def extract_pull_log_content(item_number):
    pull_log_file = Path(PROCESSING_LOGS_DIR) / 'pull_logs' / f"{item_number}_pull_log.txt"
    if not pull_log_file.exists():
        return "Pull log file not found.\n"
    try:
        with open(pull_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        content = ""
        for line in lines:
            if any(keyword in line for keyword in ["Cosmetic Condition", "Functional Condition", "Data Sanitization"]):
                break
            content += line
        return content
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {pull_log_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return "Error reading pull log file: invalid start byte\n"
        else:
            raise

def get_new_duplicates():
    # Define file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ebay_data_dir = os.path.join(script_dir, "eBayListingData")
    state_dir = os.path.join(script_dir, "state")
    os.makedirs(state_dir, exist_ok=True)
    active_file = os.path.join(ebay_data_dir, "duplicate_titles_active.txt")
    scheduled_file = os.path.join(ebay_data_dir, "duplicate_titles_scheduled.txt")
    # Prefer state/ paths; migrate legacy files if present
    ignore_active_file = os.path.join(state_dir, "_ignore_list_active.txt")
    ignore_scheduled_file = os.path.join(state_dir, "_ignore_list_scheduled.txt")
    legacy_ignore_active = os.path.join(script_dir, "_ignore_list_active.txt")
    legacy_ignore_scheduled = os.path.join(script_dir, "_ignore_list_scheduled.txt")
    try:
        if os.path.exists(legacy_ignore_active) and not os.path.exists(ignore_active_file):
            with open(legacy_ignore_active, 'r', encoding='utf-8') as fsrc, open(ignore_active_file, 'w', encoding='utf-8') as fdst:
                fdst.write(fsrc.read())
        if os.path.exists(legacy_ignore_scheduled) and not os.path.exists(ignore_scheduled_file):
            with open(legacy_ignore_scheduled, 'r', encoding='utf-8') as fsrc, open(ignore_scheduled_file, 'w', encoding='utf-8') as fdst:
                fdst.write(fsrc.read())
    except Exception:
        pass

    # Load existing ignore lists
    ignore_active = load_ignore_list(ignore_active_file)
    ignore_scheduled = load_ignore_list(ignore_scheduled_file)

    # Read duplicates
    active_duplicates = []
    if os.path.exists(active_file):
        with open(active_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.lower() not in ignore_active:
                    active_duplicates.append(line)

    scheduled_duplicates = []
    if os.path.exists(scheduled_file):
        with open(scheduled_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.lower() not in ignore_scheduled:
                    scheduled_duplicates.append(line)

    return active_duplicates, scheduled_duplicates, ignore_active_file, ignore_scheduled_file, active_file, scheduled_file

def handle_duplicates():
    active_duplicates, scheduled_duplicates, ignore_active_file, ignore_scheduled_file, active_file, scheduled_file = get_new_duplicates()
    
    if active_duplicates or scheduled_duplicates:
        def _build_keyword(dup_line: str) -> str:
            """Extract a meaningful keyword from a duplicate line for use in eBay search URL."""
            dup_line = dup_line.strip()
            # 1) Handle duplicate title headers (e.g., "Duplicate Title: Awesome Laptop i7 256GB")
            m = re.match(r"Duplicate\s+Title:\s*(.+)", dup_line, re.IGNORECASE)
            if m:
                return urllib.parse.quote_plus(m.group(1).strip())

            # 2) Handle duplicate SKU headers that may or may not use parentheses
            #    Examples: "Duplicate SKU Number (123456):" or "Duplicate SKU Number 123456"
            m = re.match(r"Duplicate\s+SKU\s+Number[^0-9]*(\d+)", dup_line, re.IGNORECASE)
            if m:
                return urllib.parse.quote_plus(m.group(1))

            # If it's a full listing line, use the title before " - SKU: "
            if " - SKU: " in dup_line:
                return urllib.parse.quote_plus(dup_line.split(" - SKU: ")[0])  # title only

            # 3) Check for numbers or terms wrapped in parentheses as a fallback
            m = re.search(r"\(([^)]+)\)", dup_line)
            if m:
                return urllib.parse.quote_plus(m.group(1))
            # Fallback – entire line encoded
            return urllib.parse.quote_plus(dup_line)

        parts = []
        if active_duplicates:
            parts.append("Active duplicate titles:\n" + "\n".join(active_duplicates))
            kw = _build_keyword(active_duplicates[0])
            parts.append(f"View Active Listings: https://www.ebay.com/sh/lst/active?keyword={kw}&source=filterbar&action=search\n")
        if scheduled_duplicates:
            parts.append("Scheduled duplicate titles:\n" + "\n".join(scheduled_duplicates))
            kw = _build_keyword(scheduled_duplicates[0])
            parts.append(f"View Scheduled Listings: https://www.ebay.com/sh/lst/scheduled?keyword={kw}&source=filterbar&action=search\n")

        message = "\n\n".join(parts)

        # Always send the message to Mattermost regardless of context
        try:
            mm_script = os.path.join(os.path.dirname(__file__), 'testmattermostmsg.py')
            subprocess.run([sys.executable, mm_script, message])
        except Exception as e:
            logger.error(f"Failed to invoke testmattermostmsg.py: {e}", extra={'session_id': current_session_id})
        
        # Add to ignore lists
        if active_duplicates:
            save_to_ignore_list(ignore_active_file, active_duplicates)
        if scheduled_duplicates:
            save_to_ignore_list(ignore_scheduled_file, scheduled_duplicates)
        
        # Clear the duplicate files
        with open(active_file, 'w', encoding='utf-8') as f:
            pass
        with open(scheduled_file, 'w', encoding='utf-8') as f:
            pass
            
def load_list(file_path):
    """Load a list from a JSON file."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {file_path}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return {}
        else:
            raise
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}", extra={'session_id': current_session_id})
        return {}

def save_list(file_path, data):
    """Save a list to a JSON file."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"Saved {file_path}")
    except Exception as e:
        print(f"Error saving {file_path}: {str(e)}")

def create_rule_management_interface(frame):
    global tab_contents  # Ensure we can modify tab_contents
    # Initialize TkinterDnD
    root = frame.winfo_toplevel()
    if not isinstance(root, TkinterDnD.Tk):
        root = TkinterDnD.Tk()
        frame = tk.Frame(root)
        frame.pack(fill='both', expand=True)

    # Main layout
    main_frame = tk.Frame(frame)
    main_frame.pack(fill='both', expand=True, padx=5, pady=5)

    # Left panel: Rule Library
    rule_frame = tk.LabelFrame(main_frame, text="Rule Library", font=("Arial", 12, "bold"))
    rule_frame.pack(side='left', fill='y', padx=5, pady=5)
    
    rule_listbox = tk.Listbox(rule_frame, bg='white', font=("Arial", 12), width=20, height=15)
    rule_listbox.pack(fill='y', padx=5, pady=5)
    for rt in rule_types:
        rule_listbox.insert('end', rt.name)
    
    # New Rule button
    new_rule_btn = ttk.Button(rule_frame, text="New Rule", command=lambda: add_new_rule(rule_listbox))
    new_rule_btn.pack(pady=5)

    # Right panel: Comparison Types & Rules
    tree_frame = tk.LabelFrame(main_frame, text="Comparison Types & Rules", font=("Arial", 12, "bold"))
    tree_frame.pack(side='right', fill='both', expand=True, padx=5, pady=5)

    tree = ttk.Treeview(tree_frame, columns=("Rule", "UUID"), show="tree headings")
    tree.heading("#0", text="Comparison/Key", anchor='w')
    tree.heading("Rule", text="Rule Definition", anchor='w')
    tree.heading("UUID", text="Rule ID", anchor='w')
    tree.column("#0", width=200)
    tree.column("Rule", width=400)
    tree.column("UUID", width=100)
    tree.pack(fill='both', expand=True, padx=5, pady=5)

    scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    scrollbar.pack(side='right', fill='y')
    tree.configure(yscrollcommand=scrollbar.set)

    # Store all widgets in a dictionary
    widgets = {
        'rule_listbox': rule_listbox,
        'new_rule_btn': new_rule_btn,
        'tree': tree,
        'scrollbar': scrollbar
    }
    tab_contents["---"] = widgets

    # Populate Treeview with comparison types and keys
    def populate_tree():
        tree.delete(*tree.get_children())
        comparison_types = ['title', 'specifics', 'metadata', 'description']
        keys = set()
        for file_path in all_files:
            if file_path in parsed_data:
                listing_data, _ = parsed_data[file_path]
                for section in comparison_types:
                    for key in listing_data.get(section, {}):
                        if '_' in key:
                            keys.add((section, '_'.join(key.split('_')[1:])))
        
        for ctype in comparison_types:
            parent = tree.insert("", "end", text=ctype.capitalize(), open=True)
            for section, key in keys:
                if section == ctype:
                    key_node = tree.insert(parent, "end", text=key)
                    if key in equivalence_rules:
                        for rule_str in equivalence_rules[key]:
                            rule_id = str(uuid.uuid4())[:8]
                            tree.insert(key_node, "end", values=(rule_str, rule_id))
        logger.debug("Treeview populated with keys and rules")

    # TkinterDnD setup
    rule_listbox.drop_target_register("text")
    tree.drop_target_register("text")
    rule_listbox.drag_source_register(1, "text")

    def dnd_start(event):
        index = rule_listbox.nearest(event.y)
        if index >= 0:
            rule_name = rule_listbox.get(index)
            return ("text", rule_name)
        return None

    def drop_on_tree(event):
        item = tree.identify_row(event.y)
        if item and tree.parent(item) and not tree.get_children(item):  # Key node
            key = tree.item(item, "text")
            assign_rule(key, event.data)
        return "break"

    rule_listbox.dnd_bind('<<DragStart>>', dnd_start)
    tree.dnd_bind('<<Drop>>', drop_on_tree)

    # Assign rule to key
    def assign_rule(key, rule_name):
        rule_type = next((rt for rt in rule_types if rt.name == rule_name), None)
        if rule_type:
            if rule_type.name == "Custom Rule":
                rule_str = simpledialog.askstring("Custom Rule", "Enter rule (e.g., lambda v1, v2: v1 == v2):", parent=frame)
            else:
                params = {p: simpledialog.askstring("Input", f"Enter {p}:", parent=frame) or "" for p in rule_type.param_names}
                rule_str = rule_type.generate_func(params) if all(params.values()) else None
            if rule_str:
                if key not in equivalence_rules:
                    equivalence_rules[key] = []
                equivalence_rules[key].append(rule_str)
                save_rules()
                populate_tree()
                logger.debug(f"Assigned rule '{rule_str}' to key '{key}'")

    # Add new rule
    def add_new_rule(listbox):
        rule_str = simpledialog.askstring("New Rule", "Enter custom rule (e.g., lambda v1, v2: v1 == v2):", parent=frame)
        if rule_str:
            custom_rule = RuleType(f"Custom: {rule_str[:10]}...")
            rule_types.append(custom_rule)
            listbox.insert('end', custom_rule.name)
            logger.debug(f"Added new custom rule: {custom_rule.name}")

    # Edit rule
    def edit_rule(event):
        item = tree.focus()
        if item and tree.get_children(item) == ():  # Rule node
            key = tree.item(tree.parent(item), "text")
            rule_str = tree.item(item, "values")[0]
            new_rule = simpledialog.askstring("Edit Rule", "Edit rule:", initialvalue=rule_str, parent=frame)
            if new_rule:
                equivalence_rules[key] = [new_rule if r == rule_str else r for r in equivalence_rules[key]]
                save_rules()
                populate_tree()
                logger.debug(f"Edited rule for key '{key}': '{rule_str}' -> '{new_rule}'")

    # Delete rule
    def delete_rule(event):
        item = tree.focus()
        if item and tree.get_children(item) == ():
            key = tree.item(tree.parent(item), "text")
            rule_str = tree.item(item, "values")[0]
            if messagebox.askyesno("Confirm", f"Delete '{rule_str}' from '{key}'?"):
                equivalence_rules[key].remove(rule_str)
                if not equivalence_rules[key]:
                    del equivalence_rules[key]
                save_rules()
                populate_tree()
                logger.debug(f"Deleted rule '{rule_str}' from key '{key}'")

    # Save to JSON
    def save_rules():
        try:
            with open(RULES_FILE, 'w', encoding='utf-8') as f:
                json.dump(equivalence_rules, f, indent=4)
            logger.debug("Saved equivalence rules to JSON")
        except Exception as e:
            logger.error(f"Error saving rules: {str(e)}")

    # Bind events
    tree.bind("<Double-1>", edit_rule)
    tree.bind("<Delete>", delete_rule)

    # Initialize
    load_data()
    load_rules()
    populate_tree()
    return widgets

def edit_rule_dialog(parent, rule_str, key, callback):
    dialog = tk.Toplevel(parent)
    dialog.title("Edit Rule")
    dialog.geometry("500x300")
    tk.Label(dialog, text="Enter rule string (e.g., lambda v1, v2: v1 == v2):").pack(pady=5)
    text_area = tk.Text(dialog, width=60, height=10)
    text_area.insert(tk.END, rule_str)
    text_area.pack(pady=10)

    def save():
        new_rule_str = text_area.get("1.0", tk.END).strip()
        if new_rule_str:
            callback(new_rule_str)
        dialog.destroy()

    tk.Button(dialog, text="Save", command=save).pack(pady=5)
    tk.Button(dialog, text="Cancel", command=dialog.destroy).pack(pady=5)

# Platform-specific Caps Lock check
if sys.platform == 'win32':
    def is_caps_lock_on():
        """Check Caps Lock state on Windows using ctypes."""
        return (ctypes.windll.user32.GetKeyState(0x14) & 1) == 1
elif sys.platform == 'linux':
    def is_caps_lock_on():
        """Check Caps Lock state on Linux using xset."""
        try:
            output = subprocess.check_output(['xset', 'q']).decode()
            return 'Caps Lock:   on' in output
        except Exception:
            return False
elif sys.platform == 'darwin':
    def is_caps_lock_on():
        """Check Caps Lock state on macOS using AppleScript."""
        try:
            script = 'tell application "System Events" to get (keyboard layout contains "Caps Lock")'
            result = subprocess.check_output(['osascript', '-e', script]).decode().strip()
            return result == 'true'
        except Exception:
            return False
else:
    def is_caps_lock_on():
        """Default for unsupported platforms: assume Caps Lock is off."""
        return False

# --- Global Constants ---
condition_mappings = {
    "Used": ["Used", "Pre-owned", "Refurbished"],
    "For parts or not working": ["For parts or not working", "Bad"],
    "New": ["New", "Brand New", "Open Box"]
}

laptop_pc_leaf_categories = {
    'pc laptops & netbooks',
    'laptops & netbooks',
    'desktops & all-in-one pcs',
    'apple laptops',
    'pc desktops & all-in-ones'
}

laptop_leaf_categories = {
    'pc laptops & netbooks',
    'laptops & netbooks',
    'apple laptops',
}

component_categories = {
    'memory (ram)',
    'computer components & parts',
    'hard drives (hdd, ssd & nas)',
    'cpus/processors',
    'graphics/video cards',
    'motherboards',
    'power supplies',
    'laptop replacement parts',
    'desktop replacement parts',
}

# --- Global Variables ---
root = None
all_files = []
files = []
files_with_issues = []
parsed_data = {}
comparisons_cache = {}
looked_at_files = set()
search_var = None
search_timer = None
tabs = {}
tab_contents = {}  # Add this line
right_panel = None
notebook = None
show_all_var = None
show_unseen_issues_var = None
category_filter_var = None
file_label = None
description_label = None
ebay_link = None
listing = None
is_power_adapter = False
logger = logging.getLogger('listing_analyzer')
current_session_id = 'INIT'
current_file_index = 0
settings_file = "settings.json"
item_number = None
show_all = False
show_unseen_issues = False
category_filter = 0
theme = 'light'
frame_bg = None
pause_on_exit = True

# --- Logging setup with default session_id to avoid formatter KeyError ---
class SessionIdFilter(logging.Filter):
    def filter(self, record):
        # Ensure every log record has a session_id, to satisfy formatters that expect it
        if not hasattr(record, 'session_id'):
            try:
                record.session_id = current_session_id
            except Exception:
                record.session_id = 'INIT'
        return True

def set_validation_logger_session(session_id: str) -> None:
    """Initialize logger filters/handlers and set default session id."""
    global current_session_id, logger
    current_session_id = session_id or 'INIT'

    # Add filter to both this logger and root so any handler sees session_id
    session_filter = SessionIdFilter()
    logger.addFilter(session_filter)
    logging.getLogger().addFilter(session_filter)

    # Ensure at least a console handler exists once with session id in format
    has_console = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    if not has_console:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] [%(session_id)s] - %(message)s'))
        # Mark as main so item-specific rotation doesn't remove it
        console.is_main = True  # type: ignore[attr-defined]
        logger.addHandler(console)

set_validation_logger_session(current_session_id)

# Windows XP Media Center Black Edition Theme
xp_media_center_black_styles = {
    "TFrame": {"background": "#003366"},
    "TLabel": {"background": "#003366", "foreground": "#E0E0E0", "font": ("Arial", 10)},
    "TButton": {"font": ("Arial", 10), "padding": 5, "background": "#002244", "foreground": "#E0E0E0"},
    "Treeview": {"background": "#004488", "fieldbackground": "#004488", "foreground": "#E0E0E0", "font": ("Arial", 8)},
}

xp_media_center_black_style_maps = {
    "Treeview": {"background": [('selected', '#336699')]},
    "TButton": {"background": [('active', '#004488')]}
}

light_mode_styles = {
    "TFrame": {"background": "#f0f0f0"},
    "TLabel": {"background": "#f0f0f0", "foreground": "black", "font": ("Arial", 10)},
    "TButton": {"font": ("Arial", 10), "padding": 5, "background": "#e0e0e0", "foreground": "black"},
    "Treeview": {"background": "#ffffff", "fieldbackground": "#ffffff", "foreground": "black", "font": ("Arial", 8)},
}
light_mode_style_maps = {
    "Treeview": {"background": [('selected', '#e0e0e0')]},
    "TButton": {"background": [('active', '#d0d0d0')]}
}
dark_mode_styles = {
    "TFrame": {"background": "#333333"},
    "TLabel": {"background": "#333333", "foreground": "white", "font": ("Arial", 10)},
    "TButton": {"font": ("Arial", 10), "padding": 5, "background": "#555555", "foreground": "white"},
    "Treeview": {"background": "#444444", "fieldbackground": "#444444", "foreground": "white", "font": ("Arial", 8)},
}
dark_mode_style_maps = {
    "Treeview": {"background": [('selected', '#666666')]},
    "TButton": {"background": [('active', '#5a5a5a')]}
}
neutral_gray_blue_styles = {
    "TFrame": {"background": "#A3BFFA"},
    "TLabel": {"background": "#A3BFFA", "foreground": "#2D3748", "font": ("Arial", 10)},
    "TButton": {"font": ("Arial", 10), "padding": 5, "background": "#CBD5E0", "foreground": "#2D3748"},
    "Treeview": {"background": "#CBD5E0", "fieldbackground": "#CBD5E0", "foreground": "#2D3748", "font": ("Arial", 8)},
}
neutral_gray_blue_style_maps = {
    "Treeview": {"background": [('selected', '#718096')]},
    "TButton": {"background": [('active', '#718096')]}
}
dark_neutral_blue_styles = {
    "TFrame": {"background": "#2C3E50"},
    "TLabel": {"background": "#2C3E50", "foreground": "#E2E8F0", "font": ("Arial", 10)},
    "TButton": {"font": ("Arial", 10), "padding": 5, "background": "#4A5568", "foreground": "#E2E8F0"},
    "Treeview": {"background": "#4A5568", "fieldbackground": "#4A5568", "foreground": "#E2E8F0", "font": ("Arial", 8)},
}
dark_neutral_blue_style_maps = {
    "Treeview": {"background": [('selected', '#718096')]},
    "TButton": {"background": [('active', '#718096')]}
}

# --- Utility Functions ---

def set_item_log_file(item_number, session_id):
    log_dir = Path(PROCESSING_LOGS_DIR) / 'compare_logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{item_number}.log"
    
    # Remove existing item-specific handlers
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler) and not getattr(handler, 'is_main', False):
            logger.removeHandler(handler)
            handler.close()
    
    # Add new item-specific handler
    item_handler = logging.FileHandler(str(log_file), mode='a', encoding='utf-8')
    item_handler.setFormatter(logging.Formatter('%(asctime)s - [%(session_id)s] - %(message)s'))
    logger.addHandler(item_handler)
    
    # Set the current session_id
    global current_session_id
    current_session_id = session_id

def load_looked_at_files():
    looked_at_file = "looked_at.txt"
    looked_at_files_set = set()
    try:
        if os.path.exists(looked_at_file):
            with open(looked_at_file, 'r', encoding='utf-8') as f:
                for line in f:
                    item_num = line.strip()
                    if item_num:
                        file_path = Path(f"item_contents/python_parsed_{item_num}.txt")
                        looked_at_files_set.add(str(file_path))
        logger.debug(f"Loaded {len(looked_at_files_set)} looked-at files", extra={'session_id': current_session_id})
    except Exception as e:
        logger.error(f"Error loading looked-at files: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
    return looked_at_files_set

def save_looked_at_files():
    looked_at_file = "looked_at.txt"
    try:
        item_numbers = {str(file).replace('item_contents\\python_parsed_', '').replace('.txt', '')
                        for file in looked_at_files}
        with open(looked_at_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted(item_numbers)) + '\n')
        logger.debug(f"Saved {len(item_numbers)} looked-at files", extra={'session_id': current_session_id})
    except Exception as e:
        logger.error(f"Error saving looked-at files: {str(e)}", exc_info=True, extra={'session_id': current_session_id})

def load_settings():
    global theme, show_all, show_unseen_issues, category_filter
    try:
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                theme = settings.get('theme', 'light')
                show_all = settings.get('show_all', False)
                show_unseen_issues = settings.get('show_unseen_issues', False)
                category_filter = settings.get('category_filter', 0)
        else:
            theme = 'light'
            show_all = False
            show_unseen_issues = False
            category_filter = 0
    except Exception as e:
        logger.error(f"Error loading settings: {str(e)}", extra={'session_id': current_session_id})
        theme = 'light'
        show_all = False
        show_unseen_issues = False
        category_filter = 0

def save_settings():
    try:
        settings = {
            'theme': theme,
            'show_all': show_all,
            'show_unseen_issues': show_unseen_issues,
            'category_filter': category_filter
        }
        with open(settings_file, 'w', encoding='utf-8') as f:
            json.dump(settings, f)
    except Exception as e:
        logger.error(f"Error saving settings: {str(e)}", extra={'session_id': current_session_id})

# --- Category Check ---
def is_laptop_pc_category(file_path):
    try:
        if file_path in parsed_data:
            _, sections = parsed_data[file_path]
            category_section = sections.get('CATEGORY', [])
        else:
            listing_data, sections = parse_file(file_path)
            category_section = sections.get('CATEGORY', [])

        # for line in category_section:
        #     leaf_match = re.search(r'\[leaf_category_key\]\s*Category:\s*(.+)', line)
        #     if leaf_match:
        #         leaf_category = leaf_match.group(1).strip().lower()
        #         if leaf_category in laptop_pc_leaf_categories:
        #             logger.debug(f"File {file_path.name} is laptop/PC via leaf_category_key: {leaf_category}", extra={'session_id': current_session_id})
        #             return True
        #         logger.debug(f"File {file_path.name} not laptop/PC; leaf_category_key: {leaf_category}", extra={'session_id': current_session_id})
        #         return False

        # for line in category_section:
        #     path_match = re.search(r'\[category_path_key\]\s*Category:\s*(.+)', line)
        #     if path_match:
        #         path = path_match.group(1).strip()
        #         categories = [cat.strip() for cat in path.split('>')]
        #         if categories and categories[-1].lower() in laptop_pc_leaf_categories:
        #             logger.debug(f"File {file_path.name} is laptop/PC via category_path_key: {categories[-1]}", extra={'session_id': current_session_id})
        #             return True
        # logger.debug(f"File {file_path.name} not laptop/PC; no matching leaf category", extra={'session_id': current_session_id})
        # return False
        logger.debug(f"File {file_path.name} category check disabled; returning False", extra={'session_id': current_session_id})
        return False
    except Exception as e:
        logger.error(f"Error checking category for {file_path}: {str(e)}", extra={'session_id': current_session_id})
        return False

logger = logging.getLogger(__name__)
current_session_id = "your_session_id_here"  # Replace with actual session ID logic

# =====================================================
# DATABASE INTEGRATION FUNCTIONS
# =====================================================

def initialize_database():
    """Initialize database connection and retrieve basic stats"""
    global db_connection, database_stats
    
    if not ENABLE_DATABASE_MODE:
        logger.info("Database mode disabled, using file-only mode", extra={'session_id': current_session_id})
        return False
    
    try:
        db_connection = get_database()
        database_stats = db_connection.get_database_stats()
        
        if database_stats:
            total_records = sum(s.get('count', 0) for s in database_stats.get('status_breakdown', []))
            logger.info(f"💾 Database connected - {total_records} records available", extra={'session_id': current_session_id})
            return True
        else:
            logger.warning("Database connected but no stats available", extra={'session_id': current_session_id})
            return True
            
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}", extra={'session_id': current_session_id})
        if FALLBACK_TO_FILES:
            logger.info("📁 Falling back to file-only mode", extra={'session_id': current_session_id})
        return False

def load_listing_from_database(item_number: str) -> tuple:
    """
    Load listing data from database and convert to runit.py format
    Returns: (listing_data, sections) in the same format as parse_file()
    """
    try:
        # Get database connection (don't rely on global variable)
        db = get_database()
        if not db:
            logger.error(f"❌ Could not get database connection for {item_number}", extra={'session_id': current_session_id})
            return None, None
        
        # Get data from database
        db_record = db.get_listing(item_number, decompress=True)
        
        if not db_record:
            logger.warning(f"📊 Item {item_number} not found in database", extra={'session_id': current_session_id})
            return None, None
        
        # Helper: post-process title keys for compact/spaced negation groups like
        # "NoPowerCord/HardDrive/SSD(a)" so storage doesn't claim RAM-sized GB.
        def _postprocess_title_negations(title_dict: dict) -> dict:
            try:
                import re
                if not isinstance(title_dict, dict):
                    return title_dict
                full_title = str(title_dict.get('Full Title') or title_dict.get('title_title_key') or '')
                addl = str(title_dict.get('title_additional_info_key') or '')
                text = (full_title + ' ' + addl).lower()
                # Detect spaced or compact power-cord negation grouped with HDD/SSD
                pat = re.compile(r"\bno(?:\s*power\s*(?:cord|adapter)|power\s*cord|power\s*adapter|powercord|poweradapter)\b[^,;:\n]{0,160}(?:/|\|)\s*(?:hard\s*drive|hdd|ssd)\b", re.IGNORECASE)
                if pat.search(text):
                    # Force storage status and remove any title storage capacities
                    title_dict['title_storage_status_key'] = 'Not Included'
                    for k in list(title_dict.keys()):
                        if k.startswith('title_storage_capacity'):
                            title_dict.pop(k, None)
                    # Opportunistically promote a single GB token to RAM when present
                    if 'title_ram_size_key' not in title_dict:
                        gbs = re.findall(r"\b(\d{1,3})\s*GB\b", full_title, flags=re.IGNORECASE)
                        uniq = []
                        for g in gbs:
                            val = f"{g}GB"
                            if val not in uniq:
                                uniq.append(val)
                        if len(uniq) == 1:
                            title_dict['title_ram_size_key'] = uniq[0]
                return title_dict
            except Exception:
                return title_dict

        # Convert database record to runit.py format
        listing_data = {
            'title': db_record.get('title_json', {}),
            'specifics': db_record.get('specifics_json', {}), 
            'table_shared': {},  # Will be populated from table_data if available
            'table_data': db_record.get('table_data', []),
            'table_metadata': {},  # Can be derived from table_data count
            'metadata': db_record.get('metadata_json', {}),
            'description': db_record.get('description_json', {})
        }

        # Apply title post-process before formatting sections
        listing_data['title'] = _postprocess_title_negations(listing_data['title'])
        
        # Create sections to match EXACT file format
        sections = {
            'TITLE DATA': [],
            'METADATA': [], 
            'CATEGORY': [],
            'SPECIFICS': [],
            'TABLE DATA': [],
            'DESCRIPTION': []
        }
        
        # Convert database JSON to EXACT file format
        if listing_data['title']:
            sections['TITLE DATA'].append('====== TITLE DATA ======')
            
            # Handle Full Title specially (no brackets)
            if 'Full Title' in listing_data['title']:
                sections['TITLE DATA'].append(f"Full Title: {listing_data['title']['Full Title']}")
            
            # Convert title fields to file format - keys should already be correct
            for k, v in listing_data['title'].items():
                if k == 'Full Title':
                    continue  # Already handled above
                elif k.endswith('_key'):
                    # Already properly formatted key - just add brackets and field name
                    field_name = k.replace('title_', '').replace('_key', '')
                    sections['TITLE DATA'].append(f"[{k}] {field_name}: {v}")
                else:
                    # Legacy fallback for old format
                    sections['TITLE DATA'].append(f"[title_{k}_key] {k}: {v}")
        
        if listing_data['metadata']:
            sections['METADATA'].append('====== METADATA ======')
            for k, v in listing_data['metadata'].items():
                # Keys should now be properly formatted with meta_ prefix
                if k.endswith('_key'):
                    # Already properly formatted key - just add brackets and field name
                    field_name = k.replace('meta_', '').replace('_key', '')
                    # Convert key back to display format
                    display_name = {
                        'title': 'Title',
                        'customlabel': 'Custom Label',
                        'listinginfo': 'Listing Info',
                        'itemnumber': 'Item Number'
                    }.get(field_name, field_name.replace('_', ' ').title())
                    sections['METADATA'].append(f"[{k}] {display_name}: {v}")
                else:
                    # Legacy fallback
                    safe_key = k.lower().replace(' ', '_').replace('-', '_')
                    sections['METADATA'].append(f"[meta_{safe_key}_key] {k}: {v}")
        
        if db_record.get('category'):
            sections['CATEGORY'].append('====== CATEGORY ======')
            # Category needs proper formatting too
            category = db_record.get('category', '')
            if category:
                sections['CATEGORY'].append(f"[leaf_category_key] Category: {category}")
            
        if listing_data['specifics']:
            sections['SPECIFICS'].append('====== SPECIFICS ======')
            for k, v in listing_data['specifics'].items():
                # Keys should now be properly formatted with specs_ prefix
                if k.endswith('_key'):
                    # Already properly formatted key - just add brackets and field name
                    field_name = k.replace('specs_', '').replace('_key', '')
                    # Map common field names to proper display names
                    display_name = {
                        'brand': 'Brand',
                        'cpu': 'Processor', 
                        'screen_size': 'Screen Size',
                        'cpu_brand': 'CPU Brand',
                        'cpu_family': 'CPU Family'
                    }.get(field_name, field_name.replace('_', ' ').title())
                    sections['SPECIFICS'].append(f"[{k}] {display_name}: {v}")
                else:
                    # Legacy fallback
                    safe_key = k.lower().replace(' ', '_').replace('-', '_')
                    display_name = k.replace('_', ' ').title()
                    sections['SPECIFICS'].append(f"[specs_{safe_key}_key] {display_name}: {v}")
                
        if listing_data['description']:
            sections['DESCRIPTION'].append('====== DESCRIPTION ======')
            for k, v in listing_data['description'].items():
                # Keys should now be properly formatted with desc_ prefix
                if k.endswith('_key'):
                    # Already properly formatted key - just add brackets and field name
                    field_name = k.replace('desc_', '').replace('_key', '')
                    display_name = {
                        'description_text': 'Description Text',
                        'cosmetic_condition': 'Cosmetic Condition',
                        'functional_condition': 'Functional Condition', 
                        'datasanitization': 'Data Sanitization'
                    }.get(field_name, field_name.replace('_', ' ').title())
                    sections['DESCRIPTION'].append(f"[{k}] {display_name}: {v}")
                else:
                    # Legacy fallback
                    safe_key = k.lower().replace(' ', '_').replace('-', '_')
                    display_name = k.replace('_', ' ').title()
                    sections['DESCRIPTION'].append(f"[desc_{safe_key}_key] {display_name}: {v}")
        
        # Handle table data formatting
        if listing_data['table_data']:
            listing_data['table_metadata']['table_entry_count_key'] = str(len(listing_data['table_data']))
            
            for i, entry in enumerate(listing_data['table_data'], 1):
                sections['TABLE DATA'].append(f"Entry {i}:")
                for key, value in entry.items():
                    sections['TABLE DATA'].append(f"[{key}]: {value}")
        
        logger.debug(f"💾 Successfully loaded item {item_number} from database", extra={'session_id': current_session_id})
        return listing_data, sections
        
    except Exception as e:
        logger.error(f"❌ Error loading item {item_number} from database: {e}", extra={'session_id': current_session_id})
        return None, None

def get_all_item_numbers_from_database() -> list:
    """Get all available item numbers from database"""
    try:
        # Get database connection (don't rely on global variable)
        db = get_database()
        if not db:
            logger.error("❌ Could not get database connection", extra={'session_id': current_session_id})
            return []
        
        # Query database for all item numbers
        results = db.search_listings("", limit=10000)  # Large limit to get all
        
        item_numbers = []
        for result in results:
            item_number = result.get('item_number')
            if item_number:
                item_numbers.append(item_number)
        
        logger.info(f"💾 Found {len(item_numbers)} items in database", extra={'session_id': current_session_id})
        return sorted(item_numbers)
        
    except Exception as e:
        logger.error(f"❌ Error retrieving item numbers from database: {e}", extra={'session_id': current_session_id})
        return []

def enhanced_parse_file(file_path_or_item_number):
    """
    Enhanced parse function that tries database first, then falls back to files
    Accepts either a file path (Path object) or item number (string)
    """
    # Determine if input is item number or file path
    if isinstance(file_path_or_item_number, str):
        # It's an item number
        item_number = file_path_or_item_number
        file_path = Path(f'item_contents/python_parsed_{item_number}.txt')
    else:
        # It's a file path
        file_path = file_path_or_item_number
        item_number = file_path.name.replace('python_parsed_', '').replace('.txt', '')
    
    # Try database first (if enabled)
    if ENABLE_DATABASE_MODE:
        try:
            listing_data, sections = load_listing_from_database(item_number)
            if listing_data is not None:
                logger.debug(f"💾 Loaded {item_number} from DATABASE", extra={'session_id': current_session_id})
                return listing_data, sections
        except Exception as e:
            logger.warning(f"⚠️ Database load failed for {item_number}: {e}", extra={'session_id': current_session_id})
    
    # Fallback to file parsing
    if FALLBACK_TO_FILES and file_path.exists():
        try:
            listing_data, sections = parse_file(file_path)
            logger.debug(f"📁 Loaded {item_number} from FILE", extra={'session_id': current_session_id})
            return listing_data, sections
        except Exception as e:
            logger.error(f"❌ File load failed for {item_number}: {e}", extra={'session_id': current_session_id})
    
    # Both methods failed
    logger.error(f"❌ Could not load {item_number} from database OR file", extra={'session_id': current_session_id})
    return None, None

def parse_file(file_path):
    # Initialize data structure with an additional 'table_shared' dictionary for shared values
    listing_data = {
        'title': {}, 
        'specifics': {}, 
        'table_shared': {},  # Added to store shared values from TABLE DATA
        'table_data': [], 
        'table_metadata': {}, 
        'metadata': {}, 
        'description': {}
    }
    sections = {
        'TITLE DATA': [], 
        'METADATA': [], 
        'CATEGORY': [], 
        'SPECIFICS': [], 
        'TABLE DATA': [], 
        'DESCRIPTION': []
    }
    current_section = None
    current_entry = None
    in_shared_values = False  # Flag to track when parsing shared values in TABLE DATA

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file.read().split('\n'):
                line = line.strip()
                # Identify section headers
                if line.startswith('======') and line.endswith('======'):
                    current_section = line[6:-6].strip().upper()
                    if current_section in sections:
                        sections[current_section].append(line)
                    in_shared_values = False  # Reset flag when a new section begins
                    continue
                if current_section:
                    sections[current_section].append(line)
                    # Handle TITLE DATA section
                    if current_section == 'TITLE DATA' and line.startswith('Full Title:'):
                        if 'title_title_key' not in listing_data['title']:
                            listing_data['title']['title_title_key'] = line.replace('Full Title:', '').strip()
                    # Handle TABLE DATA section
                    if current_section == 'TABLE DATA':
                        if line.startswith('Shared Values:'):
                            in_shared_values = True  # Start capturing shared values
                            continue
                        elif line.startswith('Entry') and ':' in line and not re.match(r'\[.*\]', line):
                            in_shared_values = False  # End shared values, start new entry
                            current_entry = {}
                            listing_data['table_data'].append(current_entry)
                            continue
                    # Parse key-value pairs in [key] value format
                    key_match = re.match(r'\[(.*?)\]\s*(.*)', line)
                    if key_match:
                        full_key = key_match.group(1).lower().replace(' ', '_')
                        value = key_match.group(2).split(':', 1)[1].strip() if ':' in key_match.group(2) else key_match.group(2).strip()
                        if full_key.startswith('title_'):
                            listing_data['title'][full_key] = value
                        elif full_key.startswith('specs_'):
                            listing_data['specifics'][full_key] = value
                        elif current_section == 'TABLE DATA':
                            if full_key == 'table_entry_count_key':
                                listing_data['table_metadata'][full_key] = value
                            elif in_shared_values:
                                listing_data['table_shared'][full_key] = value  # Store shared values
                            elif current_entry is not None:
                                current_entry[full_key] = value  # Store entry-specific values
                        elif full_key.startswith('meta_'):
                            listing_data['metadata'][full_key] = value
                        elif full_key.startswith('desc_'):
                            listing_data['description'][full_key] = value
                    # Handle condition fields in DESCRIPTION section
                    elif current_section == 'DESCRIPTION':
                        condition_match = re.match(r'(Cosmetic Condition|Functional Condition|Data Sanitization):\s*([^\n]+)', line)
                        if condition_match:
                            sections[current_section].append(line)
            
            # Filter out empty table entries
            listing_data['table_data'] = [entry for entry in listing_data['table_data'] if entry]
            
            # Log table entry keys for verification
            for idx, entry in enumerate(listing_data['table_data'], start=1):
                logger.debug(f"Table Entry {idx} keys: {list(entry.keys())}", extra={'session_id': current_session_id})
            
            # Log shared table values keys for verification
            if 'table_shared' in listing_data:
                logger.debug(f"Shared Table Values keys: {list(listing_data['table_shared'].keys())}", extra={'session_id': current_session_id})
                        
            return listing_data, sections
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:  # Assuming this is a global or defined constant
            logger.error(f"Failed to load {file_path}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return listing_data, sections
        else:
            raise
            
def compare_ram(title_ram, table_rams):
    """Compare the RAM configuration from title and table entries."""
    title_counter = parse_ram_details(title_ram)
    table_counter = Counter()
    for ram in table_rams:
        table_counter[ram.upper()] += 1
    return title_counter == table_counter, title_counter, table_counter

def compare_ram_modules(v1, v2):
    """Special comparison function for RAM modules handling different formats."""
    def extract_module_count(val):
        if not val:
            return None
        count_match = re.search(r'(\d+)\s*x', val)
        if count_match:
            return count_match.group(1)
        if re.match(r'^\d+$', val):
            return val
        return None
    
    count1 = extract_module_count(v1)
    count2 = extract_module_count(v2)
    
    if count1 and count2:
        return count1 == count2
    
    if (count1 == '1' and '1x' in v2) or (count2 == '1' and '1x' in v1):
        return True
    
    return v1 == v2

def parse_composite_family(family):
    """Parse a composite CPU family string into a list of possible families."""
    if family.startswith("Core i") and '/' in family:
        base = "Core i"
        parts = family[len(base):].split('/')
        full_families = []
        for part in parts:
            if part.startswith('i'):
                full_families.append(base + part[1:])
            else:
                full_families.append(base + part)
        return full_families
    return [family]
        
def enhance_cpu_model_comparison(model1, model2, title=None, specs=None, table=None, is_recursive_call=False):
    """Enhanced comparison for CPU models."""
    logger.debug(f"Enhanced CPU comparison: '{model1}' vs '{model2}'", extra={'session_id': current_session_id})
    
    if not model1 or not model2:
        return False
    
    # Convert inputs to lowercase for case-insensitive comparison
    model1 = str(model1).lower().strip()
    model2 = str(model2).lower().strip()
    
    # Split values into options for partial matching
    options1 = [item.strip() for item in model1.split(',')]
    options2 = [item.strip() for item in model2.split(',')]
    
    # Check if any option from model1 matches any option from model2
    for opt1 in options1:
        for opt2 in options2:
            # Perform the comparison logic on each pair
            model1_lower = opt1
            model2_lower = opt2
            
            for brand in ['intel', 'amd', 'apple']:
                model1_lower = model1_lower.replace(brand, '').strip()
                model2_lower = model2_lower.replace(brand, '').strip()
            match1 = re.search(r"(i[3579])-(\d+)([a-z]{0,2})", model1_lower)
            match2 = re.search(r"(i[3579])-(\d+)([a-z]{0,2})", model2_lower)
            
            if match1 and match2:
                base1 = f"{match1.group(1)}-{match1.group(2)}"
                suffix1 = match1.group(3)
                base2 = f"{match2.group(1)}-{match2.group(2)}"
                suffix2 = match2.group(3)
                if base1 == base2 and suffix1 and suffix2 and suffix1 == suffix2:
                    logger.debug(f"Specific CPU models match: '{opt1}' vs '{opt2}'", extra={'session_id': current_session_id})
                    return True
                logger.debug(f"Specific CPU models do not match: '{opt1}' vs '{opt2}'", extra={'session_id': current_session_id})
                # Continue checking other pairs if no match
            
            family1 = None
            family2 = None
            
            family_match1 = re.search(r'i([3579])', model1_lower)
            if family_match1:
                family1 = family_match1.group(1)
            
            family_match2 = re.search(r'i([3579])', model2_lower)
            if family_match2:
                family2 = family_match2.group(1)
            
            if family1 and family2 and family1 != family2:
                logger.debug(f"CPU families don't match: i{family1} vs i{family2}", extra={'session_id': current_session_id})
                continue
            
            gen1 = None
            gen2 = None
            
            gen_pattern1 = re.search(r'(\d+)(?:th|st|nd|rd)?\s*gen', model1_lower, re.IGNORECASE)
            if gen_pattern1:
                gen1 = int(gen_pattern1.group(1))
            
            gen_pattern2 = re.search(r'(\d+)(?:th|st|nd|rd)?\s*gen', model2_lower, re.IGNORECASE)
            if gen_pattern2:
                gen2 = int(gen_pattern2.group(1))
            
            if not gen1 and match1:
                model_num = match1.group(2)
                if model_num:
                    if model_num.startswith(('10', '11', '12', '13', '14', '15', '16', '17', '18', '19')):
                        gen1 = int(model_num[:2])
                    else:
                        gen1 = int(model_num[0])
            
            if not gen2 and match2:
                model_num = match2.group(2)
                if model_num:
                    if model_num.startswith(('10', '11', '12', '13', '14', '15', '16', '17', '18', '19')):
                        gen2 = int(model_num[:2])
                    else:
                        gen2 = int(model_num[0])
            
            if gen1 and gen2 and gen1 != gen2:
                logger.debug(f"CPU generations don't match: {gen1} vs {gen2}", extra={'session_id': current_session_id})
                continue
            
            if (family1 and family2 and family1 == family2) and (gen1 and gen2 and gen1 == gen2):
                logger.debug(f"CPU models match based on family i{family1} and generation {gen1}", extra={'session_id': current_session_id})
                return True
            
            if cpu_model_partial_match(opt1, opt2):
                logger.debug(f"Partial CPU model match found: '{opt1}' vs '{opt2}'", extra={'session_id': current_session_id})
                return True
    
    return False  # No matches found after checking all pairs

def cpu_model_partial_match(v1, v2):
    """Compare CPU models, handling suffix variations."""
    v1_lower = v1.lower()
    v2_lower = v2.lower()
    suffixes = [
        't', 'u', 'h', 'k', 'kf', 'ks', 'm', 'hq', 'hk', 'p', 'f', 'e', 'y', 'g', 's', 'r', 'b', 'c',
        'er', 'te', 'he', 'hl', 'le', 'qe', 'ue', 're', 'se',
        'x', 'xt', 'g', 'ge', 'h', 'hs', 'u', 'hx', 's', 'e', 'af', 'pro', 'wx', 'tdp', 'p', 't'
    ]
    v1_base = re.sub(r'(' + '|'.join(suffixes) + ')$', '', v1_lower, flags=re.IGNORECASE).strip()
    v2_base = re.sub(r'(' + '|'.join(suffixes) + ')$', '', v2_lower, flags=re.IGNORECASE).strip()
    if v1_base == v2_base:
        return True
    
    v1_norm = re.sub(r'\(.*?\)', '', v1_lower).strip()
    v2_norm = re.sub(r'\(.*?\)', '', v2_lower).strip()
    parts1 = set(re.split(r'[ -]+', v1_norm)) - {''}
    parts2 = set(re.split(r'[ -]+', v2_norm)) - {''}
    
    if not parts1 or not parts2:
        return True
    
    def strip_brand(model):
        common_brands = ['intel', 'amd', 'apple']
        for brand in common_brands:
            model = model.replace(brand.lower(), '').strip()
        return model
    
    v1_stripped = strip_brand(v1_norm)
    v2_stripped = strip_brand(v2_norm)
    parts1_stripped = set(re.split(r'[ -]+', v1_stripped)) - {''}
    parts2_stripped = set(re.split(r'[ -]+', v2_stripped)) - {''}
    
    common = parts1_stripped & parts2_stripped
    significant_common = any(len(part) > 2 for part in common)
    
    if not significant_common:
        return False
    
    allowed_extras = {'mini', 'laptop', 'pc', 'type', 'notebook', 'desktop', 'sff', 'small', 'form', 'factor', 'gen2', 'yoga', 'thinkpad', 'x1', 'latitude', 'dm'}
    unique1 = parts1_stripped - parts2_stripped
    unique2 = parts2_stripped - parts1_stripped
    
    if (unique1 - allowed_extras) and (unique2 - allowed_extras):
        return False
    
    return True

def normalize_model(model_str):
    """Normalize generation indicators in the model string to a standard format."""
    def replace_gen(match):
        if match.group(1):
            return f"Gen {match.group(1)}"
        elif match.group(2):
            return f"Gen {match.group(2)}"
        return match.group(0)
    
    pattern = r'\b(?:G|Gen|Generation)[-\s]*(\d+)|(\d+)(?:st|nd|rd|th)\s*gen\b'
    return re.sub(pattern, replace_gen, model_str, flags=re.IGNORECASE)

def extract_generation_numbers(gen_str):
    if not gen_str:
        return set()
    gen_str = gen_str.lower().strip()
    numbers = set()
    for part in gen_str.split('/'):
        part = part.strip()
        match = re.search(r'(\d+)(?:th|st|nd|rd)?\s*(gen|generation)?', part)
        if match:
            numbers.add(int(match.group(1)))
        elif part.isdigit():
            numbers.add(int(part))
    return numbers if numbers else {gen_str}

def compare_ram_size(val1, val2):
    """Compare RAM sizes with unit normalization and sensible tolerance.

    - Supports decimals (e.g., '1.5TB')
    - Normalizes to GB
    - If one value is TB and the other is GB, accept GB in [TB*1000, TB*1024] to account for seller rounding
    - Otherwise require exact numeric equality (after normalization), with a tiny epsilon for float comparisons
    """

    def parse_ram(val):
        # Remove commas first, then match
        val_clean = val.replace(',', '') if val else ''
        match = re.match(r'^(\d+(?:\.\d+)?)\s*(gb|mb|tb)$', val_clean.lower().strip())
        if not match:
            return None, None
        num_str, unit = match.groups()
        num = float(num_str)
        # Return size in GB and original unit
        to_gb = {'mb': num / 1024.0, 'gb': num, 'tb': num * 1024.0}
        return to_gb[unit], unit

    size1_gb, unit1 = parse_ram(val1)
    size2_gb, unit2 = parse_ram(val2)

    if size1_gb is not None and size2_gb is not None:
        # Exact match first (with small epsilon)
        if abs(size1_gb - size2_gb) <= 0.5:  # allow sub-GB rounding noise
            return True

        # Special TB vs GB tolerance: accept GB within [TB*1000, TB*1024]
        if unit1 == 'tb' and unit2 == 'gb':
            min_expected = (size1_gb / 1024.0) * 1000.0  # convert TB to TB number, then to 1000-based GB
            max_expected = size1_gb  # already TB*1024 -> GB
            return min_expected - 0.5 <= size2_gb <= max_expected + 0.5
        if unit2 == 'tb' and unit1 == 'gb':
            min_expected = (size2_gb / 1024.0) * 1000.0
            max_expected = size2_gb
            return min_expected - 0.5 <= size1_gb <= max_expected + 0.5

        return False

    # Fallback: remove commas and spaces for comparison
    v1 = (val1 or '').replace(',', '').replace(' ', '').lower()
    v2 = (val2 or '').replace(',', '').replace(' ', '').lower()
    return v1 == v2
    
def multi_value_partial_match(val1, val2, context="general"):
    """
    Check if two values match when one or both may contain multiple values separated by delimiters.
    Returns True if there's any overlap between the values.
    Designed for title vs specs comparisons where specs usually has single values.
    """
    def split_value(val):
        """Split a value by common delimiters and return a set of cleaned parts."""
        if not val:
            return set()
        # Split by common delimiters used in eBay listings
        parts = re.split(r'[\/,;|]', val)
        cleaned_parts = set()
        for part in parts:
            cleaned = part.strip().lower()
            if cleaned:
                cleaned_parts.add(cleaned)
        return cleaned_parts
    
    parts1 = split_value(val1)
    parts2 = split_value(val2)
    
    # If either value doesn't split into multiple parts, fall back to exact comparison
    if len(parts1) <= 1 and len(parts2) <= 1:
        return val1.strip().lower() == val2.strip().lower()
    
    # Check if there's any overlap
    overlap = bool(parts1 & parts2)
    logger.debug(f"Multi-value comparison ({context}): '{val1}' -> {parts1}, '{val2}' -> {parts2}, overlap: {overlap}", extra={'session_id': current_session_id})
    return overlap

def compare_cpu_generation(val1, val2):
    """Compare CPU generations by extracting numerical values, handling multiple generations in one value."""
    def parse_generations(val):
        """Extract all generation numbers from a value like '10th / 11th Gen' or '11th Gen'"""
        generations = set()
        # Find all patterns like "10th", "11th", "10 Gen", etc.
        matches = re.findall(r'(\d+)(?:th|st|nd|rd)?\s*(?:gen|generation)?', val.lower().strip())
        for match in matches:
            generations.add(int(match))
        return generations

    gens1 = parse_generations(val1)
    gens2 = parse_generations(val2)
    
    if gens1 and gens2:
        # If both have generation numbers, check if there's any overlap
        overlap = bool(gens1 & gens2)
        logger.debug(f"CPU generation comparison: '{val1}' -> {gens1}, '{val2}' -> {gens2}, overlap: {overlap}", extra={'session_id': current_session_id})
        return overlap
    
    # Fallback to exact match if no generations found
    return val1.strip().lower() == val2.strip().lower()
    
def compare_screen_size(val1, val2):
    """Compare screen sizes by extracting numerical values (e.g., '12.6in' vs '14in')."""
    def parse_size(val):
        # Remove resolution (e.g., '1920x1080') and refresh rate (e.g., '60Hz')
        val_clean = re.sub(r'\b\d{3,4}x\d{3,4}\b|\b\d{1,3}Hz\b', '', val).strip()
        # Extract numerical value with optional decimal and optional 'in' or '"'
        match = re.search(r'(\d+\.?\d*)\s*(in|"|inch|inches)?', val_clean, re.IGNORECASE)
        return float(match.group(1)) if match else None

    size1 = parse_size(val1)
    size2 = parse_size(val2)
    if size1 is not None and size2 is not None:
        return size1 == size2
    return val1.strip() == val2.strip()

def compare_cpu_speed(val1, val2):
    """Compare CPU speeds by extracting and comparing numerical values (e.g., '3.10GHz' vs '3.20GHz')."""
    def parse_speed(val):
        match = re.search(r'(\d+\.?\d*)', val.strip())
        return float(match.group(1)) if match else None

    speed1 = parse_speed(val1)
    speed2 = parse_speed(val2)
    if speed1 is not None and speed2 is not None:
        return speed1 == speed2
    return val1.strip() == val2.strip()

def compare_screen_resolution(val1, val2):
    """Compare screen resolutions by normalizing separators and stripping spaces.

    Examples considered equal:
    - '1920x1080' vs '1920 x 1080'
    - '1920X1080' vs '1920×1080'
    """
    def normalize_resolution(value: str) -> str:
        if value is None:
            return ''
        text = str(value).lower().strip()
        # Normalize multiplication/separator symbol and remove spaces around it
        text = re.sub(r'[x×]','x', text)
        text = re.sub(r'\s*x\s*', 'x', text)
        # Remove any non-digit/non-'x' characters (e.g., quotes)
        text = re.sub(r'[^0-9x]', '', text)
        return text

    return normalize_resolution(val1) == normalize_resolution(val2)

# Utility Functions
def strip_brand(model, title=None, specs=None, table=None):
    brand_keys = ['brand_key', 'make_key']
    data_dicts = [title, specs, table]
    model_lower = model.lower()
    brands = set()
    for data in data_dicts:
        if data:
            for bk in brand_keys:
                if bk in data:
                    brands.add(data[bk].lower())
    for brand in brands:
        model_lower = model_lower.replace(brand, '').strip()
    return model_lower

def has_no_storage(title=None, specs=None, table=None):
    data_dicts = [title, specs, table]
    for data in data_dicts:
        if data and 'storage_key' in data:
            storage_val = data['storage_key'].lower()
            if storage_val in ["no storage", "none", "no", "n/a", "no (m.2)"]:
                return True
    return False

def partial_match(val1, val2, title=None, specs=None, table=None):
    v1_norm = re.sub(r'\(.*?\)', '', val1.lower(), flags=re.IGNORECASE).strip()
    v2_norm = re.sub(r'\(.*?\)', '', val2.lower(), flags=re.IGNORECASE).strip()
    parts1 = set(re.split(r'[ -]+', v1_norm)) - {''}
    parts2 = set(re.split(r'[ -]+', v2_norm)) - {''}
    if not parts1 or not parts2:
        return True
    v1_stripped = strip_brand(v1_norm, title, specs, table)
    v2_stripped = strip_brand(v2_norm, title, specs, table)
    parts1_stripped = set(re.split(r'[ -]+', v1_stripped)) - {''}
    parts2_stripped = set(re.split(r'[ -]+', v2_stripped)) - {''}
    common = parts1_stripped & parts2_stripped
    significant_common = any(len(part) > 2 for part in common)
    if not significant_common:
        return False
    allowed_extras = {'mini', 'laptop', 'pc', 'type', 'notebook', 'desktop', 'sff', 'small', 'form', 'factor', 'gen2', 'yoga', 'thinkpad', 'x1', 'latitude', 'dm'}
    unique1 = parts1_stripped - parts2_stripped
    unique2 = parts2_stripped - parts1_stripped
    if (unique1 - allowed_extras) and (unique2 - allowed_extras):
        return False
    return True

def expand_abbreviations(v1, v2):
    v1_lower = v1.lower()
    v2_lower = v2.lower()
    abbreviations = {
        'sff': 'small form factor (sff)',
        'nvme': 'nvme (non-volatile memory express)',
        'dm': 'desktop mini',  # Changed from 'mini desktop' to 'desktop mini'
    }
    for abbr, full in abbreviations.items():
        if v1_lower == abbr and (full in v2_lower or abbr in v2_lower):
            return True
        if v2_lower == abbr and (full in v1_lower or abbr in v1_lower):
            return True
        if (abbr in v1_lower or full in v1_lower) and (abbr in v2_lower or full in v2_lower):
            return True
        # Add specific check for 'dm' being equivalent to 'desktop'
        if abbr == 'dm' and v1_lower == abbr and v2_lower == 'desktop':
            return True
        if abbr == 'dm' and v2_lower == abbr and v1_lower == 'desktop':
            return True
    return v1_lower in v2_lower or v2_lower in v1_lower
    
def cpu_model_partial_match_local(v1, v2, title=None, specs=None, table=None):
    v1_lower = v1.lower()
    v2_lower = v2.lower()
    suffixes = [
        't', 'u', 'h', 'k', 'kf', 'ks', 'm', 'hq', 'hk', 'p', 'f', 'e', 'y', 'g', 's', 'r', 'b', 'c',
        'er', 'te', 'he', 'hl', 'le', 'qe', 'ue', 're', 'se',
        'x', 'xt', 'g', 'ge', 'h', 'hs', 'u', 'hx', 's', 'e', 'af', 'pro', 'wx', 'tdp', 'p', 't'
    ]
    v1_base = re.sub(r'(' + '|'.join(suffixes) + ')$', '', v1_lower).strip()
    v2_base = re.sub(r'(' + '|'.join(suffixes) + ')$', '', v2_lower).strip()
    if v1_base == v2_base:
        return True
    return partial_match(v1, v2, title, specs, table)

def get_capacity(data_dict, key_prefix):
    capacity_key = f"{key_prefix}storage_capacity_key"
    return data_dict.get(capacity_key, "").lower() if data_dict else ""

def gpu_equivalence(v1, v2):
    v1_lower = v1.lower()
    v2_lower = v2.lower()
    dedicated_brands = {"nvidia", "amd", "quadro", "firepro", "radeon", "geforce"}
    v1_lower = "integrated" if v1_lower == "no" else v1_lower
    v2_lower = "integrated" if v2_lower == "no" else v2_lower

    # Intel integrated graphics patterns – all Intel graphics are integrated
    intel_integrated_patterns = [
        "intel hd graphics", "intel uhd graphics", "intel iris graphics",
        "intel iris plus graphics", "intel iris xe graphics", "intel iris pro graphics",
        "hd graphics", "uhd graphics", "iris graphics", "iris plus graphics",
        "iris xe graphics", "iris pro graphics",
        # Stripped versions (without "graphics")
        "intel hd", "intel uhd", "intel iris", "intel iris plus",
        "intel iris xe", "intel iris pro",
        "hd", "uhd", "iris", "iris plus", "iris xe", "iris pro"
    ]

    # AMD integrated graphics patterns – treat generic Radeon names as integrated
    amd_integrated_patterns = [
        "amd radeon graphics", "radeon graphics",
        "amd radeon(tm) graphics", "amd radeon", "radeon"
    ]
    if (
        any(p in v1_lower for p in amd_integrated_patterns) and
        ("integrated" in v2_lower or "on-board" in v2_lower)
    ) or (
        any(p in v2_lower for p in amd_integrated_patterns) and
        ("integrated" in v1_lower or "on-board" in v1_lower)
    ):
        logger.debug(
            f"Matched AMD integrated graphics: '{v1_lower}' with '{v2_lower}'",
            extra={'session_id': current_session_id}
        )
        return True

    # Check if one value is Intel-integrated and the other literally "integrated"
    v1_is_intel_integrated = any(p in v1_lower for p in intel_integrated_patterns)
    v2_is_intel_integrated = any(p in v2_lower for p in intel_integrated_patterns)
    if (
        v1_is_intel_integrated and ("integrated" in v2_lower or "on-board" in v2_lower)
    ) or (
        v2_is_intel_integrated and ("integrated" in v1_lower or "on-board" in v1_lower)
    ):
        logger.debug(
            f"Matched Intel integrated graphics: '{v1_lower}' with '{v2_lower}'",
            extra={'session_id': current_session_id}
        )
        return True

    # Special-case Radeon Vega integrated variants
    vega_variants = {"radeon vega graphics", "vega graphics", "radeon vega", "vega"}
    if (
        any(v in v1_lower for v in vega_variants) and
        ("integrated" in v2_lower or "on-board" in v2_lower or "intel" in v2_lower)
    ):
        logger.debug(
            f"Matched '{v1_lower}' with '{v2_lower}' as Radeon Vega (integrated) equivalence",
            extra={'session_id': current_session_id}
        )
        return True
    if (
        any(v in v2_lower for v in vega_variants) and
        ("integrated" in v1_lower or "on-board" in v1_lower or "intel" in v2_lower)
    ):
        logger.debug(
            f"Matched '{v2_lower}' with '{v1_lower}' as Radeon Vega (integrated) equivalence",
            extra={'session_id': current_session_id}
        )
        return True

    # -------- generic/fallback handling below --------
    def normalize_gpu_name(gpu_str):
        # Remove memory size / type, extra spaces, parentheses
        gpu_str = re.sub(r'\s*\(\s*\d+\s*[gm]b\s*\)|\s+\d+\s*[gm]b\s*(gddr\d)?', '', gpu_str).strip()
        gpu_str = re.sub(r'\s+', ' ', gpu_str).strip('() ')
        return gpu_str

    v1_normalized = normalize_gpu_name(v1_lower)
    v2_normalized = normalize_gpu_name(v2_lower)
    if v1_normalized == v2_normalized:
        logger.debug(
            f"Matched '{v1_lower}' with '{v2_lower}' after normalization: '{v1_normalized}'",
            extra={'session_id': current_session_id}
        )
        return True

    result = (
        ("integrated" in v1_lower or "on-board" in v1_lower) and
        ("integrated" in v2_lower or "intel" in v2_lower)
    ) or (
        "dedicated" in v1_lower and "dedicated" in v2_lower
    ) or (
        ("dedicated" in v1_lower and any(b in v2_lower for b in dedicated_brands)) or
        ("dedicated" in v2_lower and any(b in v1_lower for b in dedicated_brands))
    ) or (
        (
            ("uhd graphics" in v1_lower and "uhd graphics" in v2_lower) or
            ("uhd graphics" in v1_lower and re.search(r'uhd graphics \\d+', v2_lower)) or
            ("uhd graphics" in v2_lower and re.search(r'uhd graphics \\d+', v1_lower)) or
            v1_lower in v2_lower or v2_lower in v1_lower
        )
    ) or (
        ("integrated" in v1_lower or "on-board" in v1_lower) and
        v2_lower in {"n/a", "none", "not applicable", "no"}
    ) or (
        ("integrated" in v2_lower or "on-board" in v2_lower) and
        v1_lower in {"n/a", "none", "not applicable", "no"}
    ) or partial_match(v1, v2)

    return result
    
def partial_storage_match(v1, v2, include_capacity=False, is_mobile_device=False):
    v1_lower = v1.lower().strip()
    v2_lower = v2.lower().strip()
    storage_types = {"emmc", "ssd", "hdd", "nvme", "sata"}
    v1_types = {t for t in storage_types if t in v1_lower}
    v2_types = {t for t in storage_types if t in v2_lower}
    v1_has_type = bool(v1_types)
    v2_has_type = bool(v2_types)
    if include_capacity:
        v1_has_capacity = bool(re.search(r'\d+(gb|tb)', v1_lower.replace(" ", "")))
        v2_has_capacity = bool(re.search(r'\d+(gb|tb)', v2_lower.replace(" ", "")))
        v1_capacity = re.search(r'(\d+(gb|tb))', v1_lower.replace(" ", ""))
        v2_capacity = re.search(r'(\d+(gb|tb))', v2_lower.replace(" ", ""))
        capacity_match = v1_capacity and v2_capacity and v1_capacity.group(1) == v2_capacity.group(1)
    else:
        v1_has_capacity = v2_has_capacity = capacity_match = False

    result = (
        is_mobile_device and
        (
            (v1_has_type or v2_has_type or (include_capacity and (v1_has_capacity or v2_has_capacity))) and
            (not v1_has_type or not v2_has_type or v1_types & v2_types) and
            (not include_capacity or not v1_has_capacity or not v2_has_capacity or capacity_match) and
            not (
                ("no" in v1_lower and (any(t in v2_lower for t in storage_types) or (include_capacity and v2_has_capacity))) or
                ("no" in v2_lower and (any(t in v1_lower for t in storage_types) or (include_capacity and v1_has_capacity)))
            )
        )
    )
    if result:
        logger.debug(f"Partial storage match for mobile device: '{v1_lower}' vs '{v2_lower}'", extra={'session_id': current_session_id})
    return result

def edit_rule(tree, event):
    """Edit a rule or key in the Treeview and save changes."""
    item = tree.identify_row(event.y)
    if not item:
        return
    column = tree.identify_column(event.x)
    if column != '#2':  # Only edit rule column
        return
    key_item = tree.parent(item)
    if not key_item:
        # Editing key
        current_key = tree.item(item, "values")[0]
        new_key = simpledialog.askstring("Edit Key", "Enter new key:", initialvalue=current_key)
        if new_key and new_key != current_key:
            if current_key in equivalence_rules:
                equivalence_rules[new_key] = equivalence_rules.pop(current_key)
            tree.item(item, values=(new_key, ""))
    else:
        # Editing rule
        key = tree.item(key_item, "values")[0]
        current_rule = tree.item(item, "values")[1]
        new_rule = simpledialog.askstring("Edit Rule", "Enter new rule:", initialvalue=current_rule)
        if new_rule:
            index = tree.index(item)
            equivalence_rules[key][index] = new_rule
            tree.item(item, values=("", new_rule))
    save_equivalence_rules(equivalence_rules)

def add_new_rule(tree):
    """Add a new rule to the Treeview and equivalence_rules, then save."""
    key = simpledialog.askstring("Add New Rule", "Enter key for new rule:")
    if not key:
        return
    rule_str = simpledialog.askstring("Add New Rule", "Enter new rule string:")
    if not rule_str:
        return
    if key not in equivalence_rules:
        equivalence_rules[key] = []
    equivalence_rules[key].append(rule_str)
    parent = tree.insert("", "end", values=(key, ""), tags=("key",))
    tree.insert(parent, "end", values=("", rule_str), tags=("rule",))
    save_equivalence_rules(equivalence_rules)

# Global equivalence rules dictionary
equivalence_rules = {}
    
def load_equivalence_rules():
    """Load equivalence rules from JSON file."""
    global equivalence_rules
    if os.path.exists(RULES_FILE):
        try:
            with open(RULES_FILE, 'r', encoding='utf-8') as f:
                equivalence_rules = json.load(f)
                logger.debug(f"Loaded equivalence rules from {RULES_FILE}", extra={'session_id': current_session_id})
        except UnicodeDecodeError as e:
            if SUPPRESS_INVALID_START_BYTE:
                logger.error(f"Failed to load {RULES_FILE}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
                equivalence_rules = {}
            else:
                raise
        except json.JSONDecodeError as e:
            logger.error(f"Error loading rules from {RULES_FILE}: {str(e)}", extra={'session_id': current_session_id})
            equivalence_rules = {}
    else:
        logger.warning(f"Equivalence rules file not found: {RULES_FILE}", extra={'session_id': current_session_id})
        equivalence_rules = {}
    
    logger.debug(f"Loaded {len(equivalence_rules)} equivalence rule categories", extra={'session_id': current_session_id})
    
def save_equivalence_rules(rules):
    """Save equivalence rules to JSON file."""
    try:
        with open(RULES_FILE, 'w', encoding='utf-8') as f:
            json.dump(rules, f, indent=4)
        logger.debug(f"Saved equivalence rules to {RULES_FILE}", extra={'session_id': current_session_id})
    except Exception as e:
        logger.error(f"Error saving rules to {RULES_FILE}: {str(e)}", extra={'session_id': current_session_id})

# === NEW HELPER FUNCTION ===
# Detect common placeholder strings (e.g. "See details", "Notes", "In table", "Table") that should
# act as wildcards when they appear in Specs values.

def is_specs_placeholder(value):
    """Return True if *value* is a generic placeholder that should not trigger mismatches."""
    if not value:
        return False
    value_clean = value.lower().strip()
    # Matches if the string starts with any of the allowed keywords.
    #  - see ...   (e.g., "see details", "see description")
    #  - note / notes ...
    #  - in table ...
    #  - table
    return bool(re.match(r'^(see\b|notes?\b|in\s+table\b|table\b)', value_clean))

def check_equivalence(key, val1, val2, title=None, specs=None, table=None, is_mobile_device=False):
    logger.debug(f"Checking equivalence for {key}: '{val1}' vs '{val2}'", extra={'session_id': current_session_id})

    # Handle placeholder values (e.g., "see details", "notes", "in table", "table") as wildcards.
    if is_specs_placeholder(val1) or is_specs_placeholder(val2):
        logger.debug(
            f"Equivalence found for {key}: Placeholder detected ('{val1}' vs '{val2}'), acting as wildcard",
            extra={'session_id': current_session_id}
        )
        return True

    # Handle "see notes" as a universal wildcard for specs keys (case-insensitive)
    if key.startswith('specs_') and val1 and val1.lower().strip() == 'see notes':
        logger.debug(f"Equivalence found for {key}: '{val1}' is 'see notes' in specs, acting as wildcard", extra={'session_id': current_session_id})
        return True

    # Handle range comparisons for RAM and storage
    if key in ['ram_range_key', 'ram_size_range_key', 'storage_range_key', 'storage_capacity_range_key']:
        # Check if one value is a range and the other is within that range
        def is_range(value):
            return bool(re.match(r'^\d+(gb|mb|tb)-\d+(gb|mb|tb)$', value.lower().strip()))
        
        def is_within_range(range_str, value_str):
            range_match = re.match(r'^(\d+)(gb|mb|tb)-(\d+)(gb|mb|tb)$', range_str.lower().strip())
            value_match = re.match(r'^(\d+)(gb|mb|tb)$', value_str.lower().strip())
            
            if not range_match or not value_match:
                return False
            
            min_size, min_unit, max_size, max_unit = range_match.groups()
            value_size, value_unit = value_match.groups()
            
            unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
            min_size_gb = int(min_size) * unit_multipliers[min_unit]
            max_size_gb = int(max_size) * unit_multipliers[max_unit]
            value_size_gb = int(value_size) * unit_multipliers[value_unit]
            
            return min_size_gb <= value_size_gb <= max_size_gb
        
        val1_is_range = is_range(val1)
        val2_is_range = is_range(val2)
        
        if val1_is_range and not val2_is_range:
            result = is_within_range(val1, val2)
            logger.debug(f"Range check for {key}: '{val2}' {'is' if result else 'is not'} within range '{val1}'", extra={'session_id': current_session_id})
            return result
        elif val2_is_range and not val1_is_range:
            result = is_within_range(val2, val1)
            logger.debug(f"Range check for {key}: '{val1}' {'is' if result else 'is not'} within range '{val2}'", extra={'session_id': current_session_id})
            return result
        elif val1_is_range and val2_is_range:
            result = val1.lower().strip() == val2.lower().strip()
            logger.debug(f"Range vs range check for {key}: '{val1}' and '{val2}' are {'same' if result else 'different'}", extra={'session_id': current_session_id})
            return result

    numerical_comparisons = {
        'ram_size_key': compare_ram_size,
        'ram_total': compare_ram_size,
        'ram_total_key': compare_ram_size,
        'total_capacity_key': compare_ram_size,
        'cpu_generation_key': compare_cpu_generation,
        'screen_size_key': compare_screen_size,
        'cpu_speed_key': compare_cpu_speed,
        'screen_resolution_key': compare_screen_resolution,
        'maximum_resolution_key': compare_screen_resolution,
    }

    if key in numerical_comparisons:
        logger.debug(f"Using numerical comparison for {key}", extra={'session_id': current_session_id})
        return numerical_comparisons[key](val1, val2)

    # >>>>>>>>>>>> NEW LOGIC FOR CPU FAMILY <<<<<<<<<<<<
    # Treat a generic 'Core' family value as compatible with any specific 'Core iX' value (e.g., 'Core i5', 'Core i7').
    if key in ['cpu_family_key', 'processor_family_key', 'family_key', 'cpu_family']:
        if not val1 or not val2:
            return val1 == val2  # If either is missing, fall back to strict equality

        v1 = val1.lower().strip()
        v2 = val2.lower().strip()

        def is_generic_core(value: str) -> bool:
            """Return True if *value* is a non-specific Intel Core family identifier (e.g., 'core' or 'intel core')."""
            return value in {'core', 'intel core'}

        # NEW: Detect a generic Xeon family identifier (simply 'xeon')
        def is_generic_xeon(value: str) -> bool:
            """Return True if *value* represents a generic Intel Xeon family without further qualifiers."""
            return value == 'xeon'

        # Split composite values like "Core i5/Core i7" into individual options for flexible comparison
        def split_options(value: str):
            return [opt.strip() for opt in re.split(r'[\\/,]', value.lower()) if opt.strip()]

        opts1 = split_options(v1)
        opts2 = split_options(v2)

        # If either side is a generic 'core' and the other contains any 'core' option, regard as equivalent
        if any(is_generic_core(opt) for opt in opts1) and any(opt.startswith('core') for opt in opts2):
            logger.debug(f"Equivalence found for CPU family: generic '{val1}' vs specific '{val2}'", extra={'session_id': current_session_id})
            return True
        if any(is_generic_core(opt) for opt in opts2) and any(opt.startswith('core') for opt in opts1):
            logger.debug(f"Equivalence found for CPU family: generic '{val2}' vs specific '{val1}'", extra={'session_id': current_session_id})
            return True

        # NEW: If either side is a generic 'Xeon' and the other is a more specific Xeon variant (e.g., 'Xeon E'), treat as equivalent
        if any(is_generic_xeon(opt) for opt in opts1) and any(opt.startswith('xeon') and not is_generic_xeon(opt) for opt in opts2):
            logger.debug(f"Equivalence found for CPU family: generic '{val1}' vs specific '{val2}' (Xeon)", extra={'session_id': current_session_id})
            return True
        if any(is_generic_xeon(opt) for opt in opts2) and any(opt.startswith('xeon') and not is_generic_xeon(opt) for opt in opts1):
            logger.debug(f"Equivalence found for CPU family: generic '{val2}' vs specific '{val1}' (Xeon)", extra={'session_id': current_session_id})
            return True
        # Otherwise, require exact textual match
        return v1 == v2
    # <<<<<<<<<<<< END CPU FAMILY LOGIC >>>>>>>>>>>>

    # Explicitly use gpu_equivalence for gpu_spec_key to handle GPU specification comparisons
    if key == 'gpu_spec_key':
        return gpu_equivalence(val1, val2)

    if key == 'type_key' and is_power_adapter:
        logger.debug("Skipping 'type' check for power adapter category", extra={'session_id': current_session_id})
        return True

    if key in ['model_key', 'mpn_key']:
        model_mpn_result = check_model_mpn_equivalence(key, val1, val2, title, specs, table)
        if model_mpn_result is not None:
            return model_mpn_result

    # Special handling for videocard_key - strip "Graphics" before comparison
    if key == 'videocard_key':
        def strip_graphics(value):
            if not value:
                return value
            # Remove "Graphics" word (case-insensitive) and clean up extra spaces
            cleaned = re.sub(r'\bgraphics\b', '', value, flags=re.IGNORECASE).strip()
            # Clean up multiple spaces
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        
        val1_cleaned = strip_graphics(val1)
        val2_cleaned = strip_graphics(val2)
        logger.debug(f"Videocard comparison after stripping 'Graphics': '{val1_cleaned}' vs '{val2_cleaned}'", extra={'session_id': current_session_id})
        return gpu_equivalence(val1_cleaned, val2_cleaned)

    # New: Handle GPU model comparisons (e.g., 'RTX 7300' vs '7300') using gpu_equivalence for partial matches
    if key in ['gpu_model_key', 'gpu_model']:
        logger.debug(f"GPU model comparison using gpu_equivalence: '{val1}' vs '{val2}'", extra={'session_id': current_session_id})
        return gpu_equivalence(val1, val2)

    # Enhanced GPU brand/series comparison: Check videocard_key when individual GPU fields don't match
    if key in ['gpu_brand_key', 'gpu_series_key', 'gpu_brand', 'gpu_series']:
        # First try direct comparison
        if gpu_equivalence(val1, val2):
            logger.debug(f"GPU {key} direct match: '{val1}' vs '{val2}'", extra={'session_id': current_session_id})
            return True
        
        # If direct comparison fails, look for a videocard field in the table data
        videocard_value = None
        if table:
            # Check for exact match first
            if 'videocard_key' in table:
                videocard_value = table['videocard_key']
            else:
                # Search for key variants that end with 'videocard_key' or equal 'videocard'
                for tbl_key, tbl_val in table.items():
                    k_lower = tbl_key.lower()
                    if k_lower.endswith('videocard_key') or k_lower == 'videocard':
                        videocard_value = tbl_val
                        logger.debug(f"GPU comparison found videocard key variant: '{tbl_key}' = '{tbl_val}'", extra={'session_id': current_session_id})
                        break
        
        if videocard_value and val1:
            # Check if specs value appears in the videocard description
            val1_lower = val1.lower().strip()
            videocard_lower = videocard_value.lower()
            logger.debug(f"GPU {key} checking videocard fallback: '{val1}' in '{videocard_value}'", extra={'session_id': current_session_id})
            
            # For brand matching, check common brand names
            if key in ['gpu_brand_key', 'gpu_brand']:
                brand_variants = {
                    'nvidia': ['nvidia', 'quadro', 'geforce', 'gtx', 'rtx'],
                    'intel': ['intel', 'uhd', 'hd graphics', 'iris'],
                    'amd': ['amd', 'radeon', 'firepro']
                }
                
                # Find which brand val1 represents
                val1_brand = None
                for brand, variants in brand_variants.items():
                    if val1_lower in variants or any(variant in val1_lower for variant in variants):
                        val1_brand = brand
                        break
                
                if val1_brand:
                    # Check if any variant of this brand appears in videocard
                    brand_found = any(variant in videocard_lower for variant in brand_variants[val1_brand])
                    if brand_found:
                        logger.debug(f"GPU brand equivalence found via videocard: '{val1}' found in videocard '{videocard_value}'", extra={'session_id': current_session_id})
                        return True
            
            # For series matching, check if the series name appears in videocard
            elif key in ['gpu_series_key', 'gpu_series']:
                # Check if the series name appears in the videocard description
                if val1_lower in videocard_lower:
                    logger.debug(f"GPU series equivalence found via videocard: '{val1}' found in videocard '{videocard_value}'", extra={'session_id': current_session_id})
                    return True
                
                # Special handling for common series mappings
                series_mappings = {
                    'quadro': ['quadro'],
                    'geforce': ['geforce', 'gtx', 'rtx'],
                    'uhd': ['uhd graphics', 'uhd'],
                    'hd': ['hd graphics', 'hd'],
                    'iris': ['iris']
                }
                
                if val1_lower in series_mappings:
                    series_found = any(variant in videocard_lower for variant in series_mappings[val1_lower])
                    if series_found:
                        logger.debug(f"GPU series equivalence found via videocard mapping: '{val1}' mapped variants found in videocard '{videocard_value}'", extra={'session_id': current_session_id})
                        return True

        # If no videocard match found, fall back to original comparison result
        logger.debug(f"GPU {key} no equivalence found: '{val1}' vs '{val2}' (videocard fallback also failed)", extra={'session_id': current_session_id})
        return False

    if key == 'ram_features_key':
        try:
            features1 = ast.literal_eval(val1)
            features2 = ast.literal_eval(val2)
            if not (isinstance(features1, list) and isinstance(features2, list)):
                return val1 == val2
            features1 = [f.strip().lower() for f in features1]
            features2 = [f.strip().lower() for f in features2]
            if ('ecc' in features1 and 'non-ecc' in features2) or ('non-ecc' in features1 and 'ecc' in features2):
                logger.debug(f"Mismatch {key}: ECC vs non-ECC contradiction", extra={'session_id': current_session_id})
                return False
            logger.debug(f"Equivalence found for {key}: No ECC contradiction", extra={'session_id': current_session_id})
            return True
        except (ValueError, SyntaxError, AttributeError) as e:
            logger.debug(f"Parsing failed for {key}: {str(e)}, falling back to strict equality", extra={'session_id': current_session_id})
            return val1 == val2

    if key == 'cpu_model_key':
        # Enhanced CPU model comparison to handle different separator formats
        logger.debug(f"CPU model comparison: '{val1}' vs '{val2}'", extra={'session_id': current_session_id})
        
        # Normalize both values by splitting on separators and creating sets
        def normalize_cpu_models(value):
            if not value:
                return set()
            # Split by both comma and slash, then clean and normalize
            items = re.split(r'[\/,]', value)
            normalized_items = set()
            for item in items:
                cleaned = item.strip().lower()
                if cleaned:
                    normalized_items.add(cleaned)
            return normalized_items
        
        val1_set = normalize_cpu_models(val1)
        val2_set = normalize_cpu_models(val2)
        
        logger.debug(f"Normalized CPU models: '{val1}' -> {val1_set}, '{val2}' -> {val2_set}", extra={'session_id': current_session_id})
        
        # Check if the sets are equivalent
        is_equivalent = val1_set == val2_set
        
        if is_equivalent:
            logger.debug(f"CPU model equivalence found: {val1_set} == {val2_set}", extra={'session_id': current_session_id})
        else:
            logger.debug(f"CPU model mismatch: {val1_set} != {val2_set}", extra={'session_id': current_session_id})
        
        return is_equivalent

    if key == 'color_key':
        if not val1 or not val2:
            return val1 == val2
        if val1.lower().strip() == 'multicolor' or val2.lower().strip() == 'multicolor':
            logger.debug(f"Equivalence found for {key}: 'Multicolor' acts as wildcard", extra={'session_id': current_session_id})
            return True
        title_words = set(re.split(r'\W+', val1.lower())) - {''}
        specs_words = set(re.split(r'\W+', val2.lower())) - {''}
        return bool(title_words & specs_words)

    val1_clean = val1.strip().lower() if val1 else ""
    val2_clean = val2.strip().lower() if val2 else ""

    is_mobile_device = False
    if listing and 'CATEGORY' in listing.get('sections', {}):
        for line in listing['sections']['CATEGORY']:
            leaf_match = re.search(r'\[leaf_category_key\]\s*Category:\s*(.+)', line)
            if leaf_match:
                leaf_category = leaf_match.group(1).strip().lower()
                if leaf_category in laptop_pc_leaf_categories:
                    is_mobile_device = True
                    break

    rules = equivalence_rules.get(key, [])
    if rules:
        for rule_str in rules:
            try:
                rule_func = eval(rule_str, globals())
                result = rule_func(val1, val2, title=title, specs=specs, table=table, is_mobile_device=is_mobile_device)
                if result:
                    logger.debug(f"Equivalence found via rule for {key}: '{val1}' == '{val2}'", extra={'session_id': current_session_id})
                    return True
            except Exception as e:
                logger.debug(f"Rule evaluation failed for {key}: {str(e)}, skipping rule", extra={'session_id': current_session_id})
                continue

    if key in ['cpu_generation_key', 'processor_key']:
        return val1 == val2
    # Note: videocard_key is handled separately above to strip "Graphics"
    if key in ['model_key', 'series_key', 'mpn_key', 'motherboard_model_key', 'videocard_key', 'socket_type_key']:
        return partial_match(val1, val2, title, specs, table)

    if key in ['password_key', 'os_password_key']:
        pattern = r'^12345\b'
        return bool(re.match(pattern, val1.strip())) and bool(re.match(pattern, val2.strip()))

    return val1.lower().strip() == val2.lower().strip()
    
def check_model_mpn_equivalence(key, val1, val2, title=None, specs=None, table=None):
    logger.debug(f"Cross-checking model/MPN: {key} '{val1}' vs '{val2}'", extra={'session_id': current_session_id})
    
    if val1 == val2:
        return True
    
    title_model = title.get('model_key', '') if title else ''
    title_mpn = title.get('mpn_key', '') if title else ''
    specs_model = specs.get('model_key', '') if specs else ''
    specs_mpn = specs.get('mpn_key', '') if specs else ''
    table_model = table.get('model_key', '') if table else ''
    table_mpn = table.get('mpn_key', '') if table else ''
    
    val1_norm = val1.lower().strip()
    val2_norm = val2.lower().strip()
    title_model_norm = title_model.lower().strip()
    title_mpn_norm = title_mpn.lower().strip()
    specs_model_norm = specs_model.lower().strip()
    specs_mpn_norm = specs_mpn.lower().strip()
    table_model_norm = table_model.lower().strip()
    table_mpn_norm = table_mpn.lower().strip()
    
    if (key == 'model_key' and title and specs and 
        ((val1_norm == title_model_norm and val2_norm == specs_model_norm) or 
         (val1_norm == specs_model_norm and val2_norm == title_model_norm))):
        if specs_mpn_norm:
            model_parts1 = set(re.split(r'[\s-]+', val1_norm)) - {''}
            model_parts2 = set(re.split(r'[\s-]+', val2_norm)) - {''}
            mpn_parts = set(re.split(r'[\s-]+', specs_mpn_norm)) - {''}
            if (any(part in specs_mpn_norm for part in model_parts1 if len(part) > 2) or
                any(part in specs_mpn_norm for part in model_parts2 if len(part) > 2) or
                any(part in val1_norm for part in mpn_parts if len(part) > 2) or
                any(part in val2_norm for part in mpn_parts if len(part) > 2)):
                logger.debug(f"Model/MPN cross-check success: '{val1_norm}' and '{val2_norm}' connected via MPN '{specs_mpn_norm}'", extra={'session_id': current_session_id})
                return True
    
    if key == 'model_key' and (specs_mpn_norm or table_mpn_norm):
        other_mpn = specs_mpn_norm or table_mpn_norm
        val1_parts = set(re.split(r'[\s-]+', val1_norm)) - {''}
        val2_parts = set(re.split(r'[\s-]+', val2_norm)) - {''}
        mpn_parts = set(re.split(r'[\s-]+', other_mpn)) - {''}
        significant_parts = [part for part in val1_parts & val2_parts if len(part) > 2]
        if significant_parts:
            logger.debug(f"Model values share significant parts: {significant_parts}", extra={'session_id': current_session_id})
            return True
        if (any(part in other_mpn for part in val1_parts if len(part) > 2) and
            any(part in other_mpn for part in val2_parts if len(part) > 2)):
            logger.debug(f"Both models contain parts of MPN '{other_mpn}'", extra={'session_id': current_session_id})
            return True
    
    if key in ['model_key', 'mpn_key']:
        model_values = [val1_norm, val2_norm, specs_model_norm, specs_mpn_norm, table_model_norm, table_mpn_norm]
        model_values = [v for v in model_values if v]
        model_number_pattern = re.compile(r'([a-z]+)(\d+[\w-]+)', re.IGNORECASE)
        model_series_pattern = re.compile(r'([a-z]+)\s*(\d+)(?:\s|$)', re.IGNORECASE)
        model_matches = []
        series_matches = []
        for value in model_values:
            model_match = model_number_pattern.search(value)
            series_match = model_series_pattern.search(value)
            if model_match:
                prefix, model_num = model_match.groups()
                model_matches.append((prefix.lower(), model_num.lower()))
            if series_match:
                prefix, series_num = series_match.groups()
                series_matches.append((prefix.lower(), series_num))
        if model_matches and series_matches:
            for model_prefix, model_num in model_matches:
                for series_prefix, series_num in series_matches:
                    if model_prefix == series_prefix and series_num in model_num:
                        logger.debug(f"Model/series match: '{model_prefix} {model_num}' matches series '{series_prefix} {series_num}'", extra={'session_id': current_session_id})
                        return True
    
    return None

def check_seller_notes_typos(specs, misc_issues):
    if 'seller_notes_key' in specs and specs['seller_notes_key']:
        seller_notes = specs['seller_notes_key']
        logger.debug(f"Checking seller_notes_key for typos: '{seller_notes}'", extra={'session_id': current_session_id})
        standard_phrases = [
            "This is a tested",
            "This unit",
            "This item",
            "This laptop",
            "This desktop",
            "This computer"
        ]
        for phrase in standard_phrases:
            pattern = r'[a-zA-Z0-9](' + re.escape(phrase) + r')'
            matches = re.finditer(pattern, seller_notes)
            for match in matches:
                start_pos = max(0, match.start() - 10)
                end_pos = min(len(seller_notes), match.end() + 10)
                context = seller_notes[start_pos:end_pos]
                problem_char = seller_notes[match.start()]
                issue_text = f"Typo in seller notes: ...{context}..."
                misc_issues.append((issue_text,))
                logger.debug(f"Found missing space: '{problem_char}{phrase}'", extra={'session_id': current_session_id})
                return

def check_ram_range(title, table_entries):
    """
    Check if all table RAM sizes are within the title's RAM size range.
    
    Args:
        title (dict): Normalized title data with keys like 'ram_size_range_key' or 'ram_range_key'.
        table_entries (list): List of table entry dictionaries containing 'table_ram_size_key'.
    
    Returns:
        tuple: (is_match, issue_str)
            - is_match (bool): True if all RAM sizes are within range, False otherwise.
            - issue_str (str): Description of any mismatch or empty string if no issue.
    """
    # Check for both possible range key names
    range_key = None
    if 'ram_size_range_key' in title:
        range_key = 'ram_size_range_key'
    elif 'ram_range_key' in title:
        range_key = 'ram_range_key'
    
    if not range_key or not table_entries:
        return True, ""
    
    # Parse the RAM size range (e.g., "4GB-16GB")
    range_str = title[range_key].lower().strip()
    range_match = re.match(r'^(\d+)(gb|mb|tb)-(\d+)(gb|mb|tb)$', range_str)
    if not range_match:
        return False, f"Invalid RAM size range format: {range_str}"
    
    min_size, min_unit, max_size, max_unit = range_match.groups()
    min_size, max_size = int(min_size), int(max_size)
    
    # Convert sizes to GB for comparison
    unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
    min_size_gb = min_size * unit_multipliers[min_unit]
    max_size_gb = max_size * unit_multipliers[max_unit]
    
    # Check each table entry
    for idx, entry in enumerate(table_entries, start=1):
        ram_size = entry.get('table_ram_size_key', '').lower().strip()
        if not ram_size:
            return False, f"Missing RAM size in Table Entry {idx}"
        
        ram_match = re.match(r'^(\d+)(gb|mb|tb)$', ram_size)
        if not ram_match:
            return False, f"Invalid RAM size format in Table Entry {idx}: {ram_size}"
        
        size, unit = ram_match.groups()
        size = int(size)
        size_gb = size * unit_multipliers[unit]
        
        if not (min_size_gb <= size_gb <= max_size_gb):
            return False, f"RAM size out of range in Table Entry {idx}: {ram_size} (Range: {range_str})"
    
    return True, ""
    
def parse_ram_details(details_str):
    """Parse the RAM details string into a Counter of RAM sizes, handling NxSIZEunit with comma or slash separators."""
    # Remove trailing "RAM" if present and strip whitespace
    details_str = details_str.replace(" RAM", "").strip().upper()
    
    # Handle both comma and slash separators
    # Split by comma first, then by slash within each part
    parts = []
    for main_part in details_str.split(','):
        parts.extend(main_part.split('/'))
    
    ram_counter = Counter()
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        # Match pattern like "1x8GB" or "2x4GB"
        nx_match = re.match(r'(\d+)\s*x\s*(\d+)\s*(gb|mb|tb)', part, re.IGNORECASE)
        if nx_match:
            count = int(nx_match.group(1))
            size = nx_match.group(2)
            unit = nx_match.group(3).upper()
            ram_counter[f"{size}{unit}"] += count
            continue
        
        # Match pattern like "8GB" (assume 1 module)
        size_match = re.match(r'(\d+)\s*(gb|mb|tb)', part, re.IGNORECASE)
        if size_match:
            size = size_match.group(1)
            unit = size_match.group(2).upper()
            ram_counter[f"{size}{unit}"] += 1

def calculate_total_ram_gb(ram_config_str):
    """Calculate total RAM in GB from a configuration string like '1x8GB, 1x16GB'."""
    if not ram_config_str:
        return 0
    
    # Parse the configuration
    ram_counter = parse_ram_details(ram_config_str)
    
    # Calculate total in GB
    total_gb = 0
    unit_multipliers = {'MB': 0.001, 'GB': 1, 'TB': 1000}
    
    for ram_spec, count in ram_counter.items():
        # Extract size and unit from specs like "8GB", "16GB"
        match = re.match(r'(\d+)(GB|MB|TB)', ram_spec)
        if match:
            size = int(match.group(1))
            unit = match.group(2)
            size_gb = size * unit_multipliers.get(unit, 1)
            total_gb += size_gb * count
    
    return total_gb

def format_ram_breakdown(ram_config_str):
    """Format RAM configuration into a readable breakdown with total."""
    if not ram_config_str:
        return "No RAM configuration"
    
    ram_counter = parse_ram_details(ram_config_str)
    total_gb = calculate_total_ram_gb(ram_config_str)
    
    if not ram_counter:
        return f"Invalid RAM format: {ram_config_str}"
    
    # Format individual components
    parts = []
    for ram_spec, count in sorted(ram_counter.items()):
        if count > 1:
            parts.append(f"{count}x {ram_spec}")
        else:
            parts.append(ram_spec)
    
    breakdown = " + ".join(parts)
    return f"{breakdown} = {total_gb}GB total"

# Enhanced RAM configuration validation
def validate_ram_configuration(ram_config_str, expected_total_str):
    """Validate that RAM configuration matches expected total."""
    try:
        calculated_total = calculate_total_ram_gb(ram_config_str)
        
        # Parse expected total
        expected_match = re.match(r'(\d+)\s*(gb|mb|tb)', expected_total_str.lower().strip())
        if not expected_match:
            return False, f"Invalid expected total format: {expected_total_str}"
        
        expected_size = int(expected_match.group(1))
        expected_unit = expected_match.group(2).upper()
        unit_multipliers = {'MB': 0.001, 'GB': 1, 'TB': 1000}
        expected_total_gb = expected_size * unit_multipliers.get(expected_unit, 1)
        
        # Allow small rounding differences
        if abs(calculated_total - expected_total_gb) <= 0.1:
            return True, f"RAM configuration valid: {format_ram_breakdown(ram_config_str)}"
        else:
            return False, f"RAM mismatch: {ram_config_str} calculates to {calculated_total}GB, expected {expected_total_gb}GB"
            
    except Exception as e:
        return False, f"Error validating RAM configuration: {str(e)}"
        
def normalize_value(key, value):
    if key == 'cpu_speed_key':
        return format_cpu_speed(value)
    elif key in ['cpu_family_key', 'cpu_model_key', 'storage_capacity_key', 'ram_size_key']:
        return value.lower()
    else:
        return value

def format_cpu_speed(speed):
    if not speed:
        return speed
    # Standardize by removing trailing zeros and ensuring 'GHz', then convert to uppercase
    if speed.endswith("GHz") or speed.endswith("Ghz"):
        speed = speed[:-3].rstrip('0').rstrip('.') + "GHz"
    return speed  # Remove .upper() call to preserve case
    
def format_ram_counter(counter):
    """Format RAM counter into a string like '3x 8GB/1x 16GB', sorted by size (smallest to largest)."""
    # Extract parts and sort by numerical value of the size
    def get_size_value(size_str):
        # Extract the numerical part (e.g., '8' from '8GB')
        match = re.match(r'(\d+)(?:GB|MB|TB)', size_str, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 0  # Fallback for invalid formats
    
    # Create parts and sort by size value
    parts = [f"{count}x {size}" for size, count in counter.items() if count > 0]
    parts.sort(key=lambda x: get_size_value(x.split(' ')[1]))  # Sort by the size part (e.g., '8GB' in '3x 8GB')
    return '/'.join(parts)

def highlight_differences(str1, str2):
    """Highlight differing parts of two strings with double asterisks."""
    sm = difflib.SequenceMatcher(None, str1, str2)
    highlighted1 = []
    highlighted2 = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == 'equal':
            highlighted1.append(str1[i1:i2])
            highlighted2.append(str2[j1:j2])
        elif tag == 'replace':
            highlighted1.append(f"**{str1[i1:i2]}**")
            highlighted2.append(f"**{str2[j1:j2]}**")
        elif tag == 'delete':
            highlighted1.append(f"**{str1[i1:i2]}**")
        elif tag == 'insert':
            highlighted2.append(f"**{str2[j1:j2]}**")
    return ''.join(highlighted1), ''.join(highlighted2)

def highlight_differing_parts(ram_str, differing_sizes):
    """Highlight counts in RAM configuration strings where sizes differ."""
    parts = ram_str.split('/')
    highlighted_parts = []
    for part in parts:
        part = part.strip()
        if 'x' in part:
            count, size = part.split('x', 1)
            size = size.strip()
            if size in differing_sizes:
                highlighted_parts.append(f"**{count}**x {size}")
            else:
                highlighted_parts.append(part)
        else:
            highlighted_parts.append(part)
    return ' / '.join(highlighted_parts)

def highlight_summary_issues(issue_strings, misc_issues, listing_data):
    """Apply highlighting to issue strings for the copyable summary."""
    highlighted_issues = []
    title = {k.replace('title_', ''): v for k, v in listing_data['title'].items()}
    table_data = listing_data['table_data']

    for issue in issue_strings:
        # Handle RAM configuration mismatch specifically
        if "RAM" in issue:
            title_ram = title.get('ram_details_key', '')
            title_counter = parse_ram_details(title_ram)
            table_ram_sizes = [entry.get('table_ram_size_key', 'Unknown').upper() for entry in table_data]
            table_counter = Counter(table_ram_sizes)
            formatted_title = format_ram_counter(title_counter)
            formatted_table = format_ram_counter(table_counter)
            differing_sizes = [size for size in set(title_counter) | set(table_counter) if title_counter[size] != table_counter.get(size, 0)]
            highlighted_title = highlight_differing_parts(formatted_title, differing_sizes)
            highlighted_table = highlight_differing_parts(formatted_table, differing_sizes)
            highlighted_issue = f"RAM: Title has {highlighted_title}, Table has {highlighted_table}"
            highlighted_issues.append(highlighted_issue)
        else:
            # Split only if the issue is a comparison with a comma
            parts = issue.split(' in ', 2)
            if len(parts) == 3 and ', ' in parts[0]:
                val_part, loc1, loc2 = parts
                val1, val2 = val_part.split(', ', 1)
                h_val1, h_val2 = highlight_differences(val1.strip(), val2.strip())
                highlighted_issue = f"{h_val1} in {loc1}, {h_val2} in {loc2}"
                highlighted_issues.append(highlighted_issue)
            else:
                # Pass through non-comparison issues unchanged
                highlighted_issues.append(issue)

    # Process misc_issues (these are typically not comparisons)
    for misc_issue_tuple in misc_issues:
        misc_issue = misc_issue_tuple[0].replace('  - ', '').strip()
        highlighted_issues.append(misc_issue)  # Add as-is

    return highlighted_issues

def format_discrepancy(discrepancy_dict):
    key = discrepancy_dict["key"]
    parts = []
    if discrepancy_dict["title"]:
        parts.append(f"'{discrepancy_dict['title']}' in Title")
    if discrepancy_dict["specs"]:
        parts.append(f"'{discrepancy_dict['specs']}' in Specs")
    if discrepancy_dict["table_values"]:
        if len(discrepancy_dict["table_values"]) == 1:
            val, count = discrepancy_dict["table_values"][0]
            parts.append(f"'{val}' in Table")
        else:
            table_str = ', '.join(f"'{val}' ({count} entries)" for val, count in discrepancy_dict["table_values"])
            parts.append(f"{table_str} in Table")
    return f"{key}: " + ', '.join(parts)

def normalize_key(key):
    original_key = key
    
    # Remove known prefixes
    for prefix in ['title_', 'table_', 'desc_', 'meta_', 'specs_']:
        if key.startswith(prefix):
            key = key[len(prefix):]
            break  # Remove only the first matching prefix
    
    # Remove '_key' suffix if present
    if key.endswith('_key'):
        key = key[:-4]
    
    # Standardize certain keys
    standardization_map = {
        'make': 'brand',
        'videocard': 'graphics_card',
        'cpu': 'processor',
        'ram': 'memory',
        'os': 'operating_system',  # Map 'os' to 'operating_system'
        'operating system': 'operating_system',  # Handle keys with spaces
        'operating_system': 'operating_system',  # Ensure consistency
        # Add more mappings as needed
    }
    
    # Apply the standardization map
    if key in standardization_map:
        key = standardization_map[key]
    
    # Log the normalization for debugging
    logger.debug(f"Normalized key: '{original_key}' -> '{key}'", extra={'session_id': current_session_id})
    
    return key

def format_unmatched_for_comparison(source, target, key, value):
    display_key = key.replace('_key', '').replace('_', ' ').title()
    key_clean = f"{source.lower()}_{key}_key".replace('_key', '')
    return (f"    {display_key}", f"    {value}\n    ({key_clean})", "∅", f"    [Missing in {target}]")
    
def format_comparison(key, full_key1, val1, full_key2, val2, is_match, multiple_entries=False, entry_str=None):
    """
    Formats a comparison entry for display, handling matches and mismatches.
    
    Args:
        key (str): The key being compared (e.g., 'brand_key').
        full_key1 (str): Full key from first source (e.g., 'title_brand_key').
        val1: Value from the first source.
        full_key2 (str): Full key from second source (e.g., 'specs_brand_key').
        val2: Value from the second source.
        is_match (bool): Whether the values match.
        multiple_entries (bool, optional): Indicates if table data has multiple entries. Defaults to False.
        entry_str (str, optional): Additional entry string for table data. Defaults to None.
    
    Returns:
        tuple: A tuple containing:
            - A 5-element tuple (padded_key, padded_val1, symbol, padded_val2, table_entry).
            - issue_str (str): A string describing the comparison.
    """
    # Clean and format the display key
    display_key = key.replace('_key', '').replace('_', ' ').title()
    key1_clean = full_key1.replace('_key', '') if full_key1 else 'unknown_source1'
    key2_clean = full_key2.replace('_key', '') if full_key2 else 'unknown_source2'
    
    # Set symbol based on match status or missing values
    if val1 is None or val2 is None:
        symbol = "∅"  # Symbol for missing value
    else:
        symbol = "==" if is_match else "≠"

    # Determine source labels
    location1 = 'Table' if 'table' in full_key1 and not multiple_entries else (
        'Table Entry' if 'table' in full_key1 else ('Title' if 'title' in full_key1 else 'Specs'))
    location2 = 'Table' if 'table' in full_key2 and not multiple_entries else (
        'Table Entry' if 'table' in full_key2 else ('Title' if 'title' in full_key2 else 'Specs'))

    # Format values with fallbacks
    val1_display = format_cpu_speed(val1) if key == 'cpu_speed_key' and val1 else (str(val1) if val1 is not None else "N/A")
    val2_display = format_cpu_speed(val2) if key == 'cpu_speed_key' and val2 else (str(val2) if val2 is not None else "N/A")

    # Adjust location for table entries (used only for issue_str)
    if multiple_entries and entry_str and 'table' in full_key1:
        location1 = f"Table {entry_str}"
    if multiple_entries and entry_str and 'table' in full_key2:
        location2 = f"Table {entry_str}"

    # Construct the comparison elements
    padded_key = f"    {display_key}"
    padded_val1 = f"    {val1_display}\n    ({key1_clean})"
    padded_val2 = f"    {val2_display}\n    ({key2_clean})"
    # Set table_entry to the entry string only if multiple entries exist, otherwise empty
    table_entry = entry_str if multiple_entries and entry_str else ""
    issue_str = f"{display_key}: '{val1_display}' in {location1}, '{val2_display}' in {location2}"

    # Log the comparison
    logger.debug(f"Formatted comparison: {issue_str}, is_match: {is_match}", extra={'session_id': current_session_id})

    # Return a 5-element tuple to support the new Table Entry column
    comparison_tuple = (padded_key, padded_val1, symbol, padded_val2, table_entry)
    return comparison_tuple, issue_str

def format_non_matched(section, key, value):
    display_key = key.replace('_', ' ').title()
    key_clean = f"{section.lower()}_{key}_key".replace('_key', '')
    section_label = ("Title" if section.lower() == "title" else 
                     "Specifics" if section.lower() == "specifics" else "Table")
    
    if section.lower() == "title":
        return (f"    {display_key}", f"    {value}\n    ({key_clean})", "∅", "    [Missing in Specifics/Table]")
    elif section.lower() == "specifics":
        return (f"    {display_key}", f"    {value}\n    ({key_clean})", "∅", "    [Missing in Title/Table]")
    else:
        return (f"    {display_key}", f"    {value}\n    ({key_clean})", "∅", "    [Missing in Title/Specifics]")

def parse_combined_issue(issue_str, num_table_entries):
    """
    Parse an issue string like 'Key: Title has val, Specs has val, Table has val' into key and sources,
    excluding any source with 'N/A' values.

    Args:
        issue_str (str): The issue string to parse.
        num_table_entries (int): Number of table entries for formatting Table data.

    Returns:
        str or None: Formatted issue string if valid sources exist, otherwise None.
    """
    if ': ' not in issue_str:
        return None
    key, values_str = issue_str.split(': ', 1)
    parts = values_str.split(', ')
    sources = {}
    for part in parts:
        if ' has ' in part:
            source, val = part.split(' has ', 1)
            if val.strip().lower() != 'n/a':
                sources[source] = val
    if sources:
        formatted_sources = []
        for source, val in sources.items():
            if source == 'Table' and num_table_entries > 1:
                formatted_sources.append(f"{source} '{val}' (all {num_table_entries} entries)")
            else:
                formatted_sources.append(f"{source} '{val}'")
        formatted_issue = f"{key}: {', '.join(formatted_sources)}"
        return formatted_issue
    return None

def get_title_values_for_spec_key(title, spec_key):
    if not spec_key.endswith('_key'):
        return []
    base = spec_key[:-4]  # e.g., 'cpu_model'
    pattern = re.compile(f"^{base}(\\d+)?_key$")
    values = [title[key] for key in title if pattern.match(key)]
    return values
            
def get_terms_for_category(terms_dict, category):
    all_terms = terms_dict.get('all', [])
    category_terms = terms_dict.get(category.lower(), []) if category else []
    terms_to_check = []
    for term in all_terms + category_terms:
        if term.startswith('^'):
            parts = term.split(' ', 1)
            if len(parts) == 2:
                condition, actual_term = parts
                condition = condition[1:].lower()
                if category and condition in category.lower():
                    terms_to_check.append(actual_term)
        else:
            terms_to_check.append(term)
    logger.debug(f"Category: '{category}', Terms to check: {terms_to_check}", extra={'session_id': current_session_id})
    return terms_to_check

def load_correct_phrases_ci():
    phrases_file = "correct_phrases_case_insensitive.txt"
    phrases_ci = {'all': []}
    current_section = 'all'
    try:
        if os.path.exists(phrases_file):
            with open(phrases_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('[') and line.endswith(']'):
                        current_section = line[1:-1].strip().lower()
                        if current_section not in phrases_ci:
                            phrases_ci[current_section] = []
                    elif line:
                        phrases_ci[current_section].append(line)
            logger.debug(f"Loaded case-insensitive correct phrases: {phrases_ci}", extra={'session_id': current_session_id})
        else:
            logger.warning(f"Case-insensitive correct phrases file not found: {phrases_file}", extra={'session_id': current_session_id})
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {phrases_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return phrases_ci
        else:
            raise
    except Exception as e:
        logger.error(f"Error loading case-insensitive correct phrases: {str(e)}", extra={'session_id': current_session_id})
        return phrases_ci
    return phrases_ci

def load_correct_phrases():
    phrases_file = "correct_phrases.txt"
    phrases = {'all': []}
    current_section = 'all'
    try:
        if not os.path.exists(phrases_file):
            logger.warning(f"Correct phrases file does not exist: {os.path.abspath(phrases_file)}", extra={'session_id': current_session_id})
            return phrases
        logger.debug(f"Found correct phrases file: {os.path.abspath(phrases_file)}", extra={'session_id': current_session_id})
        
        with open(phrases_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            logger.debug(f"Read {len(lines)} lines from correct phrases file", extra={'session_id': current_session_id})
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1].strip().lower()
                    if current_section not in phrases:
                        phrases[current_section] = []
                    logger.debug(f"Parsing section: {current_section}", extra={'session_id': current_session_id})
                else:
                    phrases[current_section].append(line)
                    logger.debug(f"Added phrase to {current_section}: {line}", extra={'session_id': current_session_id})
        # Remove empty sections
        phrases = {k: v for k, v in phrases.items() if v}
        logger.debug(f"Loaded correct phrases: {phrases}", extra={'session_id': current_session_id})
        return phrases
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {phrases_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return phrases
        else:
            raise
    except PermissionError:
        logger.error(f"Permission denied accessing {phrases_file}", extra={'session_id': current_session_id})
        return phrases
    except Exception as e:
        logger.error(f"Unexpected error reading {phrases_file}: {str(e)}", extra={'session_id': current_session_id})
        return phrases

def load_preferred_spellings():
    preferred_file = "preferred_spellings.txt"
    preferred_spellings = {'all': []}
    current_section = 'all'
    try:
        if not os.path.exists(preferred_file):
            logger.warning(f"Preferred spellings file does not exist: {os.path.abspath(preferred_file)}", extra={'session_id': current_session_id})
            return preferred_spellings
        logger.debug(f"Found preferred spellings file: {os.path.abspath(preferred_file)}", extra={'session_id': current_session_id})
        
        with open(preferred_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            logger.debug(f"Read {len(lines)} lines from preferred spellings file", extra={'session_id': current_session_id})
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1].strip().lower()
                    if current_section not in preferred_spellings:
                        preferred_spellings[current_section] = []
                    logger.debug(f"Parsing section: {current_section}", extra={'session_id': current_session_id})
                else:
                    preferred_spellings[current_section].append(line)
                    logger.debug(f"Added term to {current_section}: {line}", extra={'session_id': current_session_id})
        # Remove empty sections
        preferred_spellings = {k: v for k, v in preferred_spellings.items() if v}
        logger.debug(f"Loaded preferred spellings: {preferred_spellings}", extra={'session_id': current_session_id})
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {preferred_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return preferred_spellings
        else:
            raise
    except PermissionError:
        logger.error(f"Permission denied accessing {preferred_file}", extra={'session_id': current_session_id})
        return preferred_spellings
    except Exception as e:
        logger.error(f"Unexpected error reading {preferred_file}: {str(e)}", extra={'session_id': current_session_id})
        return preferred_spellings
    return preferred_spellings

def get_terms_for_category(terms_dict, category, case_insensitive=False):
    all_terms = terms_dict.get('all', [])
    category_terms = terms_dict.get(category.lower(), []) if category else []
    terms_to_check = []
    for term in all_terms + category_terms:
        if term.startswith('^'):
            parts = term.split(' ', 1)
            if len(parts) == 2:
                condition, actual_term = parts
                condition = condition[1:].lower()
                if category and condition in category.lower():
                    terms_to_check.append(actual_term)
        else:
            terms_to_check.append(term)
    if case_insensitive:
        terms_to_check = [term.lower() for term in terms_to_check]  # Convert to lowercase for case-insensitive checks
    return terms_to_check

def check_words_in_order(text, words, case_sensitive=True):
    """Check if words appear in order in the text, with optional case sensitivity.
    
    Args:
        text (str): The text to search (e.g., title).
        words (list): List of words to check.
        case_sensitive (bool): If True, match case; if False, ignore case.
    
    Returns:
        tuple: (bool, list) - True if words are in order, False otherwise; list of positions.
    """
    pattern_flags = 0 if case_sensitive else re.IGNORECASE
    positions = []
    for word in words:
        pattern = r'\b' + re.escape(word) + r'\b'
        matches = [m.start() for m in re.finditer(pattern, text, pattern_flags)]
        if not matches:
            return False, []
        positions.append(matches)
    for pos_tuple in product(*positions):
        if all(pos_tuple[i] < pos_tuple[i+1] for i in range(len(pos_tuple)-1)):
            return True, pos_tuple
    return False, []

def normalize_category_string(text):
    """Normalize category strings for comparison by handling case, whitespace, and punctuation."""
    if not text:
        return ""
    # Convert to lowercase, normalize whitespace, and strip
    normalized = ' '.join(text.lower().split())
    # Normalize common punctuation variations
    normalized = normalized.replace('&', 'and').replace(',', ' ')
    # Remove extra spaces that might have been created
    normalized = ' '.join(normalized.split())
    return normalized

def calculate_total_ram_gb_helper(ram_config_str):
    """Calculate total RAM in GB from a configuration string like '1x8GB, 1x16GB' or '(1x4GB)(1x8GB)'."""
    if not ram_config_str:
        return 0
    
    # Handle both comma and slash separators, and also parentheses separators
    parts = []
    for main_part in ram_config_str.split(','):
        for sub_part in main_part.split('/'):
            # Split by parentheses groups like "(1x4GB)(1x8GB)"
            paren_parts = re.findall(r'\([^)]+\)|[^()]+', sub_part)
            for paren_part in paren_parts:
                if paren_part.strip():
                    parts.append(paren_part.strip())
    
    total_gb = 0
    unit_multipliers = {'MB': 0.001, 'GB': 1, 'TB': 1000}
    
    for part in parts:
        part = part.strip().upper()
        if not part:
            continue
            
        # Remove parentheses if present
        part = part.strip('()')
        
        # Match pattern like "1x8GB" or "2x4GB"
        nx_match = re.match(r'(\d+)\s*[Xx]\s*(\d+)\s*(GB|MB|TB)', part)
        if nx_match:
            count = int(nx_match.group(1))
            size = int(nx_match.group(2))
            unit = nx_match.group(3)
            size_gb = size * unit_multipliers.get(unit, 1)
            total_gb += size_gb * count
            continue
        
        # Match pattern like "8GB" (assume 1 module)
        size_match = re.match(r'(\d+)\s*(GB|MB|TB)', part)
        if size_match:
            size = int(size_match.group(1))
            unit = size_match.group(2)
            size_gb = size * unit_multipliers.get(unit, 1)
            total_gb += size_gb
    
    return total_gb
    
def validate_ram_configuration_helper(ram_config_str, expected_total_str):
    """Validate that RAM configuration matches expected total."""
    try:
        calculated_total = calculate_total_ram_gb_helper(ram_config_str)
        
        # Parse expected total
        expected_match = re.match(r'(\d+(?:\.\d+)?)\s*(gb|mb|tb)', expected_total_str.lower().strip())
        if not expected_match:
            return False, f"Invalid expected total format: {expected_total_str}"
        
        expected_size = float(expected_match.group(1))
        expected_unit = expected_match.group(2).upper()
        unit_multipliers = {'MB': 0.001, 'GB': 1, 'TB': 1000}
        expected_total_gb = expected_size * unit_multipliers.get(expected_unit, 1)
        
        # Allow small rounding differences
        if abs(calculated_total - expected_total_gb) <= 0.1:
            return True, f"RAM configuration valid: {ram_config_str} = {calculated_total}GB"
        else:
            return False, f"RAM mismatch: {ram_config_str} = {calculated_total}GB, but total displayed: {expected_total_str}"
            
    except Exception as e:
        return False, f"Error validating RAM configuration: {str(e)}"

def check_for_actual_untested_helper(text):
    """Check for actual untested issues (not CD-ROM or adapters)."""
    if not text or not isinstance(text, str):
        return False
        
    # Check if all untested mentions are part of CD-ROM or adapter context
    untested_pattern = r'\b(untested|has not been tested|not been power tested|unable to test)\b'
    
    # Look for untested phrases directly
    untested_matches = list(re.finditer(untested_pattern, text, re.IGNORECASE))
    if not untested_matches:
        return False
        
    # Check each match to see if it's in an ignored context
    for match in untested_matches:
        start_pos = match.start()
        end_pos = match.end()
        # Get some context around the match (30 chars should be enough)
        surrounding_text = text[max(0, start_pos-15):min(len(text), end_pos+15)].lower()
        
        # Skip if related to CD-ROM
        if "cd-rom" in surrounding_text or "cdrom" in surrounding_text:
            continue
            
        # Skip if related to adapter
        if "adapter untested" in surrounding_text or "adapter untested" in surrounding_text:
            continue
        
        # If we get here, we found an untested mention that's not in an ignored context
        return True
            
    # If all mentions were skipped, no actual untested items found
    return False

# Category validation functions
def load_category_mapping():
    """Load the category mapping from category_mapping.txt"""
    mapping_file = "category_mapping.txt"
    category_mapping = {}
    
    try:
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                current_store_category = None
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Check if this is a store category header (ends with colon)
                    if line.endswith(':'):
                        current_store_category = line[:-1].strip()
                        category_mapping[current_store_category] = []
                    # Check if this is an eBay category (starts with dash)
                    elif line.startswith('- ') and current_store_category:
                        ebay_category = line[2:].strip()
                        category_mapping[current_store_category].append(ebay_category)
            
            logger.debug(f"Loaded category mapping with {len(category_mapping)} store categories", extra={'session_id': current_session_id})
            return category_mapping
        else:
            logger.warning(f"Category mapping file not found: {mapping_file}", extra={'session_id': current_session_id})
            return {}
    except Exception as e:
        logger.error(f"Error loading category mapping: {str(e)}", extra={'session_id': current_session_id})
        return {}

def extract_store_category_info(metadata, title):
    """Extract store category information from metadata and title"""
    store_category = None
    sub_category = None
    
    # Debug: Log all available metadata keys
    logger.debug(f"Available metadata keys: {list(metadata.keys())}", extra={'session_id': current_session_id})
    
    # Look for store category in various metadata fields
    possible_store_keys = [
        'listing_storecategory_key', 'store_category_key', 'storecategory_key', 'category_key', 
        'store_cat_key', 'primary_category_key', 'storecategory', 'listing_storecategory'
    ]
    
    for key in possible_store_keys:
        if key in metadata and metadata[key]:
            store_category = metadata[key].strip()
            logger.debug(f"Found store category '{store_category}' using key '{key}'", extra={'session_id': current_session_id})
            break
    
    # Look for store subcategory
    possible_subcat_keys = [
        'listing_storesubcategory_key', 'store_subcategory_key', 'storesubcategory_key', 'subcategory_key',
        'store_subcat_key', 'storesubcategory', 'listing_storesubcategory'
    ]
    
    for key in possible_subcat_keys:
        if key in metadata and metadata[key]:
            sub_category = metadata[key].strip()
            logger.debug(f"Found store subcategory '{sub_category}' using key '{key}'", extra={'session_id': current_session_id})
            break
    
    # If still not found, try partial key matching
    if not sub_category:
        for key, value in metadata.items():
            if 'subcategory' in key.lower() or 'subcat' in key.lower():
                if value and value.strip():
                    sub_category = value.strip()
                    logger.debug(f"Found store subcategory '{sub_category}' using partial match key '{key}'", extra={'session_id': current_session_id})
                    break
    
    # Also check title for category information if not found in metadata
    if not store_category:
        # Look for category information in title data
        title_category_keys = [
            'category_key', 'store_category_key', 'type_key'
        ]
        for key in title_category_keys:
            if key in title and title[key]:
                potential_category = title[key].strip()
                # Only use if it looks like a store category name
                if potential_category and not potential_category.lower() in ['laptop', 'desktop', 'tablet']:
                    store_category = potential_category
                    logger.debug(f"Found store category '{store_category}' from title using key '{key}'", extra={'session_id': current_session_id})
                    break
    
    logger.debug(f"Final extraction result: store_category='{store_category}', sub_category='{sub_category}'", extra={'session_id': current_session_id})
    return store_category, sub_category
    
def normalize_category_name_for_mapping(category_name):
    """Normalize category names for comparison with mapping"""
    if not category_name:
        return ""
    
    # Convert to lowercase and normalize common variations
    normalized = category_name.lower().strip()
    
    # Handle common variations
    replacements = {
        'pc laptops & netbooks': 'laptops & netbooks',
        'pc desktops & all-in-one pcs': 'desktops & all-in-ones',
        'pc desktops & all-in-ones': 'desktops & all-in-ones',
        'apple desktops & all-in-ones': 'apple desktops & all-in-ones',
        'cell phones & smartphones': 'cell phones & smartphones',
        'tablets & ebook readers': 'tablets & ebook readers',
        'computer components & parts': 'computer components & parts'
    }
    
    return replacements.get(normalized, normalized)

def is_gray_area_2in1_case(title_dict, leaf_category, device_type=None):
    """Return True for gray-area 2-in-1/Surface Pro style hybrids to suppress category mismatch issues.

    Minimal safe condition:
    - Leaf category includes 'PC Laptops & Netbooks'
    - Title indicates a convertible/detachable (2-in-1 phrases or known hybrid series/models)
    Device type is optional and not required for the exemption.
    """
    try:
        lc = (leaf_category or '').lower().strip()
        if not lc:
            return False
        if 'pc laptops & netbooks' not in lc:
            return False

        # Prefer Full Title, fallback to title_title_key
        title_text = ''
        if isinstance(title_dict, dict):
            title_text = title_dict.get('Full Title') or title_dict.get('title_title_key') or ''
        tl = str(title_text).lower()

        # Phrase-based detection
        if (
            '2in1' in tl
            or re.search(r"\b2\s*in\s*1\b", tl)
            or re.search(r"\b2\s*-\s*in\s*-\s*1\b", tl)
            or re.search(r"\b2\W*in\W*1\b", tl)  # robust against unicode dashes/punctuations
            or 'surface pro' in tl or 'surface book' in tl or 'surface go' in tl
            or 'spectre x360' in tl or 'envy x360' in tl or re.search(r"\bx360\b", tl)
            or 'x1 yoga' in tl or re.search(r"\byoga\b", tl)
            or 'convertible' in tl or 'detachable' in tl
        ):
            return True

        # Model-based detection (Dell known 2-in-1s)
        try:
            from configs.dell_models import dell_2in1_models
        except Exception:
            dell_2in1_models = set()

        # Collect 4-digit model numbers from title and optional model key
        model_candidates = set(re.findall(r"\b\d{4}\b", tl))
        if isinstance(title_dict, dict):
            model_field = title_dict.get('title_model_key') or ''
            if model_field:
                model_candidates.update(re.findall(r"\b\d{4}\b", str(model_field).lower()))

        brand_hint = ''
        if isinstance(title_dict, dict):
            brand_hint = str(title_dict.get('title_brand_key', '')).lower()
        if ('dell' in tl or 'dell' in brand_hint) and any(m in dell_2in1_models for m in model_candidates):
            return True
        return False
    except Exception:
        return False

def suggest_store_categories_for_leaf(leaf_category, category_mapping):
    """Return store categories whose allowed eBay categories match the given leaf category."""
    suggestions = []
    if not leaf_category or not category_mapping:
        return suggestions
    leaf_normalized = normalize_category_name_for_mapping(leaf_category)
    for store_category_name, allowed_ebay_categories in category_mapping.items():
        for allowed in allowed_ebay_categories:
            allowed_normalized = normalize_category_name_for_mapping(allowed)
            if (
                leaf_normalized == allowed_normalized or
                leaf_normalized in allowed_normalized or
                allowed_normalized in leaf_normalized
            ):
                suggestions.append(store_category_name)
                break
    # De-duplicate while preserving order
    seen = set()
    unique_suggestions = []
    for name in suggestions:
        if name not in seen:
            seen.add(name)
            unique_suggestions.append(name)
    # Prefer specific categories over 'Other' when alternatives exist
    if len(unique_suggestions) > 1:
        filtered = [s for s in unique_suggestions if s.strip().lower() != 'other']
        unique_suggestions = filtered or unique_suggestions
    return unique_suggestions

def is_leaf_category_in_mapping(leaf_category, category_mapping):
    """Return True if the given leaf category appears anywhere in the mapping."""
    if not leaf_category or not category_mapping:
        return False
    leaf_normalized = normalize_category_name_for_mapping(leaf_category)
    for allowed_ebay_categories in category_mapping.values():
        for allowed in allowed_ebay_categories:
            allowed_normalized = normalize_category_name_for_mapping(allowed)
            if (
                leaf_normalized == allowed_normalized
                or leaf_normalized in allowed_normalized
                or allowed_normalized in leaf_normalized
            ):
                return True
    return False

def log_unmapped_leaf_category(meta, store_category, sub_category, leaf_category):
    """Append details about an unmapped eBay category to a processing log file."""
    try:
        from pathlib import Path
        from datetime import datetime
        log_path = Path(PROCESSING_LOGS_DIR) / 'unmapped_categories.txt'
        log_path.parent.mkdir(parents=True, exist_ok=True)
        item_num = 'Unknown'
        try:
            if isinstance(meta, dict):
                item_num = meta.get('meta_itemnumber_key', 'Unknown')
        except Exception:
            item_num = 'Unknown'
        timestamp = datetime.now().isoformat(timespec='seconds')
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(
                f"{timestamp} | item {item_num} | leaf='{leaf_category or ''}' | store='{store_category or ''}' | sub='{sub_category or ''}'\n"
            )
        logger.debug(
            f"Logged unmapped eBay category '{leaf_category}' for item {item_num}",
            extra={'session_id': current_session_id},
        )
    except Exception as e:
        logger.error(
            f"Failed to log unmapped eBay category '{leaf_category}': {e}",
            extra={'session_id': current_session_id},
        )

def validate_apple_category_specific(store_category, sub_category, leaf_category):
    """
    Specific validation for Apple categories that require both store category and subcategory.
    
    Args:
        store_category (str): Store category (should be "Apple")
        sub_category (str): Store subcategory (e.g., "Laptops", "Desktops")
        leaf_category (str): eBay leaf category (e.g., "Apple Laptops")
    
    Returns:
        dict: Validation results with match status and expected combinations
    """
    # Define the exact Apple category mappings
    apple_category_mappings = {
        'apple laptops': {
            'required_store_category': 'apple',
            'required_subcategories': ['laptops']
        },
        'apple desktops & all-in-ones': {
            'required_store_category': 'apple',
            'required_subcategories': ['computers', 'all-in-ones']
        }
    }
    
    validation_result = {
        'is_apple_category': False,
        'store_to_leaf_match': False,
        'expected_combination': None,
        'missing_requirements': []
    }
    
    if not leaf_category:
        return validation_result
    
    leaf_normalized = leaf_category.lower().strip()
    
    # Check if this is an Apple category
    apple_mapping = None
    for apple_cat, mapping in apple_category_mappings.items():
        if apple_cat == leaf_normalized:
            apple_mapping = mapping
            validation_result['is_apple_category'] = True
            break
    
    if not apple_mapping:
        return validation_result
    
    # For Apple categories, we need specific store category + subcategory combinations
    required_store_cat = apple_mapping['required_store_category']
    required_subcats = apple_mapping['required_subcategories']
    
    validation_result['expected_combination'] = f"Store Category: '{required_store_cat.title()}' + Store Subcategory: one of {[s.title() for s in required_subcats]}"
    
    # Check store category
    store_cat_normalized = store_category.lower().strip() if store_category else ''
    if store_cat_normalized != required_store_cat:
        validation_result['missing_requirements'].append(f"Store category should be '{required_store_cat.title()}', found '{store_category or 'Not found'}'")
    
    # Check subcategory
    subcat_normalized = sub_category.lower().strip() if sub_category else ''
    if subcat_normalized not in required_subcats:
        validation_result['missing_requirements'].append(f"Store subcategory should be one of {[s.title() for s in required_subcats]}, found '{sub_category or 'Not found'}'")
    
    # Match only if both requirements are met
    if store_cat_normalized == required_store_cat and subcat_normalized in required_subcats:
        validation_result['store_to_leaf_match'] = True
    
    logger.debug(f"Apple category validation: leaf='{leaf_category}', store='{store_category}', sub='{sub_category}', match={validation_result['store_to_leaf_match']}", extra={'session_id': current_session_id})
    
    return validation_result

def is_vintage_item(store_category, sub_category, leaf_category):
    """
    Check if an item should be considered vintage and therefore exempt from strict category validation.
    
    Args:
        store_category (str): Store category
        sub_category (str): Store subcategory  
        leaf_category (str): eBay leaf category
        
    Returns:
        bool: True if this should be treated as a vintage item
    """
    if not store_category and not sub_category and not leaf_category:
        return False
    
    # Check for vintage indicators in any category field
    vintage_indicators = ['vintage', 'retro', 'classic', 'antique', 'collectible']
    
    # Check store category
    if store_category:
        store_lower = store_category.lower().strip()
        if any(indicator in store_lower for indicator in vintage_indicators):
            return True
    
    # Check store subcategory
    if sub_category:
        sub_lower = sub_category.lower().strip()
        if any(indicator in sub_lower for indicator in vintage_indicators):
            return True
    
    # Check eBay leaf category
    if leaf_category:
        leaf_lower = leaf_category.lower().strip()
        if any(indicator in leaf_lower for indicator in vintage_indicators):
            return True
    
    return False

def validate_category_consistency(store_category, leaf_category, device_type, category_mapping, sub_category=None):
    """Validate that store category, leaf category, and device type are consistent"""
    validation_results = {
        'store_to_leaf_match': False,
        'device_type_match': False,
        'expected_categories': [],
        'issues': [],
        'is_vintage_exemption': False
    }
    
    logger.debug(f"Validating categories: store='{store_category}', sub='{sub_category}', leaf='{leaf_category}', device='{device_type}'", extra={'session_id': current_session_id})
    
    # NEW: Check for vintage exemption first
    if is_vintage_item(store_category, sub_category, leaf_category):
        validation_results['is_vintage_exemption'] = True
        validation_results['store_to_leaf_match'] = True  # Auto-pass for vintage items
        validation_results['device_type_match'] = True   # Auto-pass device type for vintage
        validation_results['expected_categories'] = [f"Vintage items: '{store_category}' + '{sub_category}' can match any relevant eBay category"]
        logger.debug(f"Vintage exemption applied - auto-passing category validation", extra={'session_id': current_session_id})
        return validation_results
    
    if not store_category or not leaf_category:
        if not store_category:
            validation_results['issues'].append("Store category not found in metadata")
        if not leaf_category:
            validation_results['issues'].append("Leaf category not found")
        return validation_results
    
    # SPECIAL CASE: Store category 'Other' is a master wildcard. Always pass.
    if store_category and store_category.strip().lower() == 'other':
        validation_results['store_to_leaf_match'] = True
        validation_results['device_type_match'] = True
        validation_results['expected_categories'] = ["'Other' is a master wildcard: any eBay category is acceptable"]
        logger.debug("Category validation: 'Other' wildcard unconditional pass", extra={'session_id': current_session_id})
        return validation_results
        return validation_results
         
    # FIRST: Check if this is an Apple category that needs specific validation
    apple_validation = validate_apple_category_specific(store_category, sub_category, leaf_category)
    
    if apple_validation['is_apple_category']:
        logger.debug(f"This is an Apple category, using specific Apple validation", extra={'session_id': current_session_id})
        validation_results['store_to_leaf_match'] = apple_validation['store_to_leaf_match']
        
        if apple_validation['expected_combination']:
            validation_results['expected_categories'] = [apple_validation['expected_combination']]
        
        if apple_validation['missing_requirements']:
            for req in apple_validation['missing_requirements']:
                validation_results['issues'].append(req)
        
        logger.debug(f"Apple validation result: match={validation_results['store_to_leaf_match']}", extra={'session_id': current_session_id})
    else:
        # FALLBACK: Use the existing generic logic for non-Apple categories
        logger.debug(f"Not an Apple category, using generic validation", extra={'session_id': current_session_id})
        
        # ENHANCED: Prioritize subcategory matching for category mapping
        matching_store_key = None
        
        # First priority: Check subcategory against mapping (if it exists)
        if sub_category and category_mapping:
            # Try exact match for subcategory
            if sub_category in category_mapping:
                matching_store_key = sub_category
                logger.debug(f"Found exact subcategory match: '{sub_category}'", extra={'session_id': current_session_id})
            else:
                # Try case-insensitive match for subcategory
                for store_key in category_mapping.keys():
                    if sub_category.lower() == store_key.lower():
                        matching_store_key = store_key
                        logger.debug(f"Found case-insensitive subcategory match: '{sub_category}' -> '{store_key}'", extra={'session_id': current_session_id})
                        break
        
        # Second priority: Check main store category if no subcategory match
        if not matching_store_key and category_mapping:
            if store_category in category_mapping:
                matching_store_key = store_category
                logger.debug(f"Found exact store category match: '{store_category}'", extra={'session_id': current_session_id})
            else:
                # Try case-insensitive match for store category
                for store_key in category_mapping.keys():
                    if store_category.lower() == store_key.lower():
                        matching_store_key = store_key
                        logger.debug(f"Found case-insensitive store category match: '{store_category}' -> '{store_key}'", extra={'session_id': current_session_id})
                        break
        
        # Check if we found a mapping match
        if matching_store_key:
            expected_ebay_categories = category_mapping[matching_store_key]
            validation_results['expected_categories'] = expected_ebay_categories
            
            leaf_normalized = normalize_category_name_for_mapping(leaf_category)
            for expected_cat in expected_ebay_categories:
                expected_normalized = normalize_category_name_for_mapping(expected_cat)
                if leaf_normalized == expected_normalized or leaf_normalized in expected_normalized or expected_normalized in leaf_normalized:
                    validation_results['store_to_leaf_match'] = True
                    logger.debug(f"Category mapping match found: '{matching_store_key}' -> '{expected_cat}' matches '{leaf_category}'", extra={'session_id': current_session_id})
                    break
            
            if not validation_results['store_to_leaf_match']:
                logger.debug(f"No match found in expected categories {expected_ebay_categories} for leaf category '{leaf_category}'", extra={'session_id': current_session_id})
        else:
            validation_results['issues'].append(f"Store category '{store_category}' and subcategory '{sub_category}' not found in mapping")
            logger.debug(f"No mapping found for store category '{store_category}' or subcategory '{sub_category}'", extra={'session_id': current_session_id})
    
    # Check device type consistency with categories (existing logic remains the same)
    if device_type:
        device_lower = device_type.lower()
        leaf_lower = leaf_category.lower()
        
        # Define device type to category mappings
        device_category_mappings = {
            'laptop': ['laptops', 'netbooks', 'apple laptops'],
            'desktop': ['desktops', 'all-in-one', 'apple desktops'],
            'tablet': ['tablets', 'ereaders'],
            'phone': ['cell phones', 'smartphones'],
            'server': ['computer servers', 'servers'],
            'monitor': ['monitors'],
            'switch': ['network switches', 'switches'],
            'router': ['routers', 'networking']
        }
        
        # Check if device type matches leaf category
        if device_lower in device_category_mappings:
            expected_terms = device_category_mappings[device_lower]
            validation_results['device_type_match'] = any(term in leaf_lower for term in expected_terms)
        else:
            # For unknown device types, assume match if not contradictory
            validation_results['device_type_match'] = True
    
    logger.debug(f"Final validation result: {validation_results}", extra={'session_id': current_session_id})
    return validation_results
    
def check_enhanced_category_validation(meta, title, leaf_category, misc_info, misc_issues, issue_strings):
    """Perform enhanced category mapping validation"""
    category_mapping = load_category_mapping()
    
    store_category, sub_category = extract_store_category_info(meta, title)
    device_type = title.get('device_type_key', '').strip()
    
    misc_info.append(f"  - Store Category Validation:")
    misc_info.append(f"    • Store Category: {store_category if store_category else 'Not found'}")
    misc_info.append(f"    • Store Subcategory: {sub_category if sub_category else 'Not found'}")
    misc_info.append(f"    • Leaf Category: {leaf_category if leaf_category else 'Not found'}")
    misc_info.append(f"    • Device Type: {device_type if device_type else 'Not specified'}")
    
    # Always call validate_category_consistency to get validation object
    validation = validate_category_consistency(store_category, leaf_category, device_type, category_mapping or {}, sub_category)
    
    # NEW: Handle vintage exemption
    if validation.get('is_vintage_exemption', False):
        misc_info.append(f"    • Vintage Exemption: APPLIED - Auto-passing category validation for vintage item")
        misc_info.append(f"    • Store to Leaf Match: PASS (Vintage)")
        misc_info.append(f"    • Device Type Match: PASS (Vintage)")
        logger.debug(f"Vintage exemption applied for store='{store_category}', sub='{sub_category}', leaf='{leaf_category}'", extra={'session_id': current_session_id})
        return  # Skip all validation issues for vintage items
    
    if store_category and leaf_category:
        # Gray-area exemption for 2-in-1 and Surface Pro when leaf=laptops, device_type=tablets
        if (
            (leaf_category or '').lower().strip().find('pc laptops & netbooks') != -1 and
            (device_type or '').lower().strip().find('tablets & ebook readers') != -1
        ) or is_gray_area_2in1_case(title, leaf_category, device_type):
            misc_info.append("    • Gray-Area Exemption: APPLIED for 2-in-1/Surface Pro (store vs leaf)")
            misc_info.append("    • Store to Leaf Match: PASS (Gray-Area)")
            misc_info.append("    • Device Type Match: PASS (Gray-Area)")
            return
        # Title-based exemption: base-only/no-screen laptops listed under Laptop Parts in store
        title_text_for_exemption = (title.get('title_title_key', '') or title.get('title_key', '') or title.get('key', '')).lower().strip()
        store_cat_lower = (store_category or '').lower()
        sub_cat_lower = (sub_category or '').lower()
        leaf_lower = (leaf_category or '').lower()
        if (("laptop parts" in store_cat_lower or "laptop parts" in sub_cat_lower) and
            ("laptop" in leaf_lower) and
            ("base only" in title_text_for_exemption or "no screen" in title_text_for_exemption)):
            misc_info.append("    • Title Exemption: APPLIED - Base-only/No-Screen laptop allows Laptops leaf with Laptop Parts store category")
            misc_info.append("    • Store to Leaf Match: PASS (Title Exemption)")
            return
        if validation['expected_categories']:
            misc_info.append(f"    • Expected: {', '.join(validation['expected_categories'])}")
        
        # Check store to leaf category match
        if validation['store_to_leaf_match']:
            misc_info.append(f"    • Store to Leaf Match: PASS")
        else:
            # If the leaf category is not present in mapping anywhere, treat as unmapped: log only, don't create an issue
            if not is_leaf_category_in_mapping(leaf_category, category_mapping or {}):
                log_unmapped_leaf_category(meta, store_category, sub_category, leaf_category)
                misc_info.append("    • Store to Leaf Match: SKIPPED - Unmapped eBay category (logged)")
            else:
                misc_info.append(f"    • Store to Leaf Match: FAIL")
                # Create specific error message for Apple vs generic categories
                apple_validation = validate_apple_category_specific(store_category, sub_category, leaf_category)
                if apple_validation['is_apple_category']:
                    # For Apple categories, show the specific requirements
                    if apple_validation['missing_requirements']:
                        mismatch_msg = f"Apple category mismatch: {'; '.join(apple_validation['missing_requirements'])}"
                    else:
                        mismatch_msg = f"Apple category validation failed for '{leaf_category}'"
                else:
                    # For non-Apple categories, use the generic message
                    if sub_category:
                        mismatch_msg = f"Category mismatch: Store category '{store_category}' with subcategory '{sub_category}' does not match leaf category '{leaf_category}'"
                    else:
                        mismatch_msg = f"Category mismatch: Store category '{store_category}' does not match leaf category '{leaf_category}'"
                    
                    if validation['expected_categories']:
                        mismatch_msg += f". Expected one of: {', '.join(validation['expected_categories'])}"

                    # Add suggested store categories based on mapping
                    suggestions = suggest_store_categories_for_leaf(leaf_category, category_mapping or {})
                    if suggestions:
                        mismatch_msg += f". Suggested store category: {', '.join(suggestions)}"
                
                misc_issues.append((mismatch_msg,))
                issue_strings.append(mismatch_msg)
        
        # Check device type consistency
        if device_type:
            # Apply gray-area exemption for device type vs leaf mismatch as well
            if is_gray_area_2in1_case(title, leaf_category, device_type):
                misc_info.append("    • Device Type Match: PASS (Gray-Area 2-in-1/Surface Pro)")
            elif validation['device_type_match']:
                misc_info.append(f"    • Device Type Match: PASS")
            else:
                misc_info.append(f"    • Device Type Match: FAIL")
                device_mismatch_msg = f"Device type '{device_type}' inconsistent with leaf category '{leaf_category}'"
                misc_issues.append((device_mismatch_msg,))
                issue_strings.append(device_mismatch_msg)
        else:
            misc_info.append(f"    • Device Type Match: SKIPPED - No device type specified")
        
        # Report any additional issues
        for issue in validation.get('issues', []):
            misc_info.append(f"    • Issue: {issue}")
    else:
        misc_info.append(f"    • Validation: SKIPPED - Missing required category information")
        for issue in validation.get('issues', []):
            misc_info.append(f"    • Issue: {issue}")
            
def check_legacy_category_vs_device_type(title, leaf_category, misc_info, misc_issues, issue_strings):
    """Check category vs device type using legacy logic for supported categories"""
    
    # NEW: Check for vintage exemption first
    if leaf_category and is_vintage_item(None, None, leaf_category):
        misc_info.append("  - Category vs Device Type: SKIPPED - Vintage exemption applied")
        logger.debug(f"Legacy category vs device type check skipped for vintage item: '{leaf_category}'", extra={'session_id': current_session_id})
        return
    
    def load_supported_leaf_categories():
        """Load supported leaf categories for device-type check from configs with a sane default."""
        try:
            from pathlib import Path
            import json
            cfg_path = Path(CONFIGS_DIR) / 'supported_leaf_categories.json'
            default_supported = [
                'Computer Servers',
                'PC Laptops & Netbooks',
                'PC Desktops & All-In-Ones',
                'Apple Desktops & All-In-Ones',
                'Apple Laptops',
                'CPUs/Processors'
            ]
            if cfg_path.exists():
                try:
                    with open(cfg_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list) and all(isinstance(x, str) for x in data):
                        return data
                    else:
                        logger.warning(
                            f"supported_leaf_categories.json is not a list of strings; using defaults",
                            extra={'session_id': current_session_id},
                        )
                        return default_supported
                except Exception as e:
                    logger.error(
                        f"Error reading supported_leaf_categories.json: {e}; using defaults",
                        extra={'session_id': current_session_id},
                    )
                    return default_supported
            else:
                return default_supported
        except Exception:
            # Absolute fallback to ensure check is not broken
            return [
                'Computer Servers',
                'PC Laptops & Netbooks',
                'PC Desktops & All-In-Ones',
                'Apple Desktops & All-In-Ones',
                'Apple Laptops',
                'CPUs/Processors'
            ]

    supported_categories = load_supported_leaf_categories()
    
    # Always show supported categories in misc info
    misc_info.append("  - Supported Categories for Device Type Check:")
    for cat in supported_categories:
        misc_info.append(f"    • {cat}")
    
    if leaf_category and 'device_type_key' in title:
        device_type = title['device_type_key'].strip()
        logger.debug(f"Checking category vs device type: leaf_category='{leaf_category}', device_type='{device_type}'", extra={'session_id': current_session_id})
        
        # Determine support based on LEAF category only (restrict check to supported categories)
        leaf_is_supported = any(supported_cat.lower() in leaf_category.lower() for supported_cat in supported_categories)
        device_is_supported = any(supported_cat.lower() in device_type.lower() for supported_cat in supported_categories)

        # Only run this check if the leaf category itself is supported
        if leaf_is_supported:
            leaf_normalized = normalize_category_string(leaf_category)
            device_normalized = normalize_category_string(device_type)

            logger.debug(f"Normalized comparison: leaf_normalized='{leaf_normalized}', device_normalized='{device_normalized}'", extra={'session_id': current_session_id})

            if leaf_normalized != device_normalized:
                # Gray-area exemption for 2-in-1/Surface Pro
                if (
                    (leaf_category or '').lower().find('pc laptops & netbooks') != -1 and
                    (device_type or '').lower().find('tablets & ebook readers') != -1
                ) or is_gray_area_2in1_case(title, leaf_category, device_type):
                    misc_info.append("  - Category vs Device Type: PASS (Gray-Area 2-in-1/Surface Pro)")
                    logger.debug("Gray-area exemption applied in legacy check for 2-in-1/Surface Pro", extra={'session_id': current_session_id})
                else:
                    mismatch_msg = f"Category mismatch: Leaf category '{leaf_category}' not found in device type '{device_type}'"
                    if mismatch_msg not in [m[0] for m in misc_issues]:
                        misc_issues.append((mismatch_msg,))
                        issue_strings.append(mismatch_msg)
                    misc_info.append(f"  - Category vs Device Type: MISMATCH - '{leaf_category}' ≠ '{device_type}'")
                    logger.debug(f"Category mismatch detected: '{leaf_normalized}' != '{device_normalized}'", extra={'session_id': current_session_id})
            else:
                misc_info.append(f"  - Category vs Device Type: MATCH - '{leaf_category}' == '{device_type}'")
                logger.debug(f"Category match found: '{leaf_normalized}' == '{device_normalized}'", extra={'session_id': current_session_id})
        else:
            reason = "leaf category not in supported list"
            # If device type is supported but leaf is not, explicitly note the skip
            if device_is_supported:
                misc_info.append(f"  - Category vs Device Type: SKIPPED - {reason}")
            else:
                misc_info.append(f"  - Category vs Device Type: SKIPPED - neither value in supported list")
            logger.debug(f"Skipping category vs device type check: {reason}; leaf='{leaf_category}', device='{device_type}'", extra={'session_id': current_session_id})
    else:
        if not leaf_category:
            misc_info.append("  - Category vs Device Type: SKIPPED - No leaf category found")
        elif 'device_type_key' not in title:
            misc_info.append("  - Category vs Device Type: SKIPPED - No device type in title")
            
# RAM and Storage validation functions
def check_ram_range_verification(title, listing_data, misc_info, misc_issues):
    """Enhanced RAM Range Verification"""
    if 'ram_range_key' in title or 'ram_size_range_key' in title:
        range_key = 'ram_range_key' if 'ram_range_key' in title else 'ram_size_range_key'
        range_str = title[range_key].lower().strip()
        logger.debug(f"RAM Range Verification starting: range_key='{range_key}', range_str='{range_str}'", extra={'session_id': current_session_id})
        
        table_data = listing_data.get('table_data', [])
        shared_values = listing_data.get('table_shared', {})
        logger.debug(f"Table entries: {len(table_data)}, Shared keys: {list(shared_values.keys())}", extra={'session_id': current_session_id})
        
        range_match = re.match(r'^(\d+)(gb|mb|tb)-(\d+)(gb|mb|tb)$', range_str)
        if not range_match:
            ram_range_issue = f"Invalid RAM size range format: {range_str}"
            misc_issues.append((ram_range_issue,))
            misc_info.append(f"  - RAM Range Verification: FAILED - {ram_range_issue}")
            logger.debug(f"RAM Range Issue: {ram_range_issue}", extra={'session_id': current_session_id})
        else:
            min_size, min_unit, max_size, max_unit = range_match.groups()
            min_size, max_size = int(min_size), int(max_size)
            
            unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
            min_size_gb = min_size * unit_multipliers[min_unit]
            max_size_gb = max_size * unit_multipliers[max_unit]
                        
            entries_checked = 0
            ram_range_issues = []
            
            def extract_ram_size_from_config(config_str):
                """Extract total RAM size from configuration strings like '2x8GB' -> '16GB' or '1x16GB' -> '16GB'"""
                config_str = config_str.strip()
                # Remove parentheses if present
                config_str = config_str.strip('()')
                # Match patterns like "2x8GB" or "1 x 16GB"
                config_match = re.match(r'^(\d+)\s*x\s*(\d+)(gb|mb|tb)$', config_str.lower())
                if config_match:
                    module_count = int(config_match.group(1))
                    module_size = int(config_match.group(2))
                    unit = config_match.group(3)
                    total_size = module_count * module_size
                    return f"{total_size}{unit}"
                return None
            
            # Check shared values first
            ram_keys_to_check = ['table_ram_size_key', 'table_memory_key', 'table_ram_capacity_key', 'table_ram_config_key', 'table_ram_modules_key']
            for ram_key in ram_keys_to_check:
                if ram_key in shared_values and shared_values[ram_key].strip():
                    shared_ram_value = shared_values[ram_key].strip()
                    logger.debug(f"Found shared RAM value: {ram_key}='{shared_ram_value}'", extra={'session_id': current_session_id})
                    
                    # Try to extract RAM size
                    if ram_key in ['table_ram_config_key', 'table_ram_modules_key']:
                        # Extract size from config string like "1x8GB"
                        ram_size = extract_ram_size_from_config(shared_ram_value)
                        if ram_size:
                            shared_ram_size = ram_size.lower()
                            logger.debug(f"Extracted RAM size from config: {shared_ram_size}", extra={'session_id': current_session_id})
                        else:
                            logger.debug(f"Could not extract RAM size from config: {shared_ram_value}", extra={'session_id': current_session_id})
                            continue
                    else:
                        # Direct RAM size
                        shared_ram_size = shared_ram_value.lower().strip()
                    
                    entries_checked += 1
                    ram_match = re.match(r'^(\d+)(gb|mb|tb)$', shared_ram_size)
                    if ram_match:
                        size, unit = ram_match.groups()
                        size = int(size)
                        size_gb = size * unit_multipliers[unit]
                        
                        if not (min_size_gb <= size_gb <= max_size_gb):
                            ram_range_issues.append(f"Shared RAM size out of range: {shared_ram_size} (Range: {range_str})")
                        else:
                            logger.debug(f"Shared RAM size in range: {shared_ram_size}", extra={'session_id': current_session_id})
                    else:
                        ram_range_issues.append(f"Invalid shared RAM size format: {shared_ram_size}")
                    break
            
            for idx, entry in enumerate(table_data, start=1):
                logger.debug(f"Checking table entry {idx}: keys={list(entry.keys())}", extra={'session_id': current_session_id})
                
                ram_size = None
                ram_key_used = None
                
                for ram_key in ram_keys_to_check:
                    if ram_key in entry and entry[ram_key].strip():
                        ram_value = entry[ram_key].strip()
                        
                        # Try to extract RAM size
                        if ram_key in ['table_ram_config_key', 'table_ram_modules_key']:
                            # Extract size from config string like "1x8GB"
                            extracted_size = extract_ram_size_from_config(ram_value)
                            if extracted_size:
                                ram_size = extracted_size.lower()
                                ram_key_used = ram_key
                                logger.debug(f"Extracted RAM size from config: {ram_size}", extra={'session_id': current_session_id})
                                break
                        else:
                            # Direct RAM size
                            ram_size = ram_value.lower().strip()
                            ram_key_used = ram_key
                            break
                
                if ram_size:
                    entries_checked += 1
                    logger.debug(f"Entry {idx} RAM: {ram_key_used}='{ram_size}'", extra={'session_id': current_session_id})
                    
                    ram_match = re.match(r'^(\d+)(gb|mb|tb)$', ram_size)
                    if ram_match:
                        size, unit = ram_match.groups()
                        size = int(size)
                        size_gb = size * unit_multipliers[unit]
                        
                        if not (min_size_gb <= size_gb <= max_size_gb):
                            ram_range_issues.append(f"RAM size out of range in Table Entry {idx}: {ram_size} (Range: {range_str})")
                        else:
                            logger.debug(f"Entry {idx} RAM size in range: {ram_size}", extra={'session_id': current_session_id})
                    else:
                        ram_range_issues.append(f"Invalid RAM size format in Table Entry {idx}: {ram_size}")
                else:
                    logger.debug(f"Entry {idx}: No RAM size keys found", extra={'session_id': current_session_id})
            
            if ram_range_issues:
                for issue in ram_range_issues:
                    misc_issues.append((issue,))
                    logger.debug(f"RAM Range Issue: {issue}", extra={'session_id': current_session_id})
                misc_info.append(f"  - RAM Range Verification: FAILED - {len(ram_range_issues)} issues found")
            elif entries_checked > 0:
                misc_info.append(f"  - RAM Range Verification: PASSED - All {entries_checked} entries within range {range_str}")
                logger.debug(f"RAM Range Verification: All {entries_checked} entries within range {title[range_key]}", extra={'session_id': current_session_id})
            else:
                misc_info.append(f"  - RAM Range Verification: SKIPPED - No RAM entries found to check")
                logger.debug(f"RAM Range Verification: No RAM size keys found in table data", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - RAM Range Verification: SKIPPED - No RAM range specified")

def check_storage_range_verification(title, listing_data, misc_info, misc_issues):
    """Enhanced Storage Range Verification"""
    if 'storage_range_key' in title or 'storage_capacity_range_key' in title:
        range_key = 'storage_range_key' if 'storage_range_key' in title else 'storage_capacity_range_key'
        range_str = title[range_key].lower().strip()
        logger.debug(f"Storage Range Verification starting: range_key='{range_key}', range_str='{range_str}'", extra={'session_id': current_session_id})
        
        table_data = listing_data.get('table_data', [])
        shared_values = listing_data.get('table_shared', {})
        logger.debug(f"Storage verification - Table entries: {len(table_data)}, Shared keys: {list(shared_values.keys())}", extra={'session_id': current_session_id})
        
        range_match = re.match(r'^(\d+)(gb|mb|tb)-(\d+)(gb|mb|tb)$', range_str)
        if not range_match:
            storage_range_issue = f"Invalid storage size range format: {range_str}"
            misc_issues.append((storage_range_issue,))
            misc_info.append(f"  - Storage Range Verification: FAILED - {storage_range_issue}")
            logger.debug(f"Storage Range Issue: {storage_range_issue}", extra={'session_id': current_session_id})
        else:
            min_size, min_unit, max_size, max_unit = range_match.groups()
            min_size, max_size = int(min_size), int(max_size)
            
            unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
            min_size_gb = min_size * unit_multipliers[min_unit]
            max_size_gb = max_size * unit_multipliers[max_unit]
                        
            entries_checked = 0
            storage_range_issues = []
            
            def parse_storage(value):
                match = re.search(r'(\d+\.?\d*)\s*(gb|tb|mb)', value.lower())
                if match:
                    size, unit = match.groups()
                    size = float(size)
                    conversion = {'mb': size / 1000, 'gb': size, 'tb': size * 1000}
                    return conversion.get(unit, 0)
                return None
            
            storage_keys_to_check = [
                'table_storage_capacity_key', 'table_ssd_capacity_key', 'table_hdd_capacity_key', 
                'table_storage_key', 'table_ssd_key', 'table_hdd_key', 'table_hard_drive_key',
                'table_storage_size_key', 'table_drive_capacity_key'
            ]
            for storage_key in storage_keys_to_check:
                if storage_key in shared_values and shared_values[storage_key].strip():
                    entries_checked += 1
                    shared_storage_size = shared_values[storage_key].lower().strip()
                    logger.debug(f"Found shared storage size: {storage_key}='{shared_storage_size}'", extra={'session_id': current_session_id})
                    
                    size_gb = parse_storage(shared_storage_size)
                    if size_gb is not None:
                        if not (min_size_gb <= size_gb <= max_size_gb):
                            storage_range_issues.append(f"Shared storage capacity out of range: {shared_storage_size} (Range: {range_str})")
                        else:
                            logger.debug(f"Shared storage size in range: {shared_storage_size}", extra={'session_id': current_session_id})
                    else:
                        storage_range_issues.append(f"Invalid shared storage capacity format: {shared_storage_size}")
                    break
            
            for idx, entry in enumerate(table_data, start=1):
                logger.debug(f"Checking storage in entry {idx}: keys={list(entry.keys())}", extra={'session_id': current_session_id})
                
                storage_size = None
                storage_key_used = None
                
                for key in entry.keys():
                    if re.match(r'table_storage_capacity\d+_key', key) or key in storage_keys_to_check:
                        if entry[key].strip():
                            storage_size = entry[key].lower().strip()
                            storage_key_used = key
                            break
                
                if storage_size:
                    entries_checked += 1
                    logger.debug(f"Entry {idx} Storage: {storage_key_used}='{storage_size}'", extra={'session_id': current_session_id})
                    
                    size_gb = parse_storage(storage_size)
                    if size_gb is not None:
                        if not (min_size_gb <= size_gb <= max_size_gb):
                            storage_range_issues.append(f"Storage capacity out of range in Table Entry {idx}: {storage_size} (Range: {range_str})")
                        else:
                            logger.debug(f"Entry {idx} storage size in range: {storage_size}", extra={'session_id': current_session_id})
                    else:
                        storage_range_issues.append(f"Invalid storage capacity format in Table Entry {idx}: {storage_size}")
                else:
                    logger.debug(f"Entry {idx}: No storage size keys found", extra={'session_id': current_session_id})
            
            if storage_range_issues:
                for issue in storage_range_issues:
                    misc_issues.append((issue,))
                    logger.debug(f"Storage Range Issue: {issue}", extra={'session_id': current_session_id})
                misc_info.append(f"  - Storage Range Verification: FAILED - {len(storage_range_issues)} issues found")
            elif entries_checked > 0:
                misc_info.append(f"  - Storage Range Verification: PASSED - All {entries_checked} entries within range {range_str}")
                logger.debug(f"Storage Range Verification: All {entries_checked} entries within range {title[range_key]}", extra={'session_id': current_session_id})
            else:
                misc_info.append(f"  - Storage Range Verification: SKIPPED - No storage entries found to check")
                logger.debug(f"Storage Range Verification: No storage size keys found in table data", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - Storage Range Verification: SKIPPED - No storage range specified")

def check_ram_breakdown_verification(title, misc_info, misc_issues):
    """RAM Breakdown Verification"""
    if 'ram_breakdown_key' in title and 'ram_size_key' in title:
        ram_breakdown = title['ram_breakdown_key'].lower()
        ram_size = title['ram_size_key'].lower()
        
        breakdown_parts = ram_breakdown.split('/')
        calculated_total_gb = 0
        breakdown_details = []
        unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
        total_size_match = re.match(r'(\d+)(gb|mb|tb)', ram_size)
        
        if total_size_match:
            total_size = int(total_size_match.group(1))
            total_unit = total_size_match.group(2)
            total_size_gb = total_size * unit_multipliers[total_unit]
            
            for part in breakdown_parts:
                breakdown_match = re.match(r'(\d+)x(\d+)(gb|mb|tb)', part.strip())
                if breakdown_match:
                    num_modules = int(breakdown_match.group(1))
                    module_size = int(breakdown_match.group(2))
                    module_unit = breakdown_match.group(3)
                    module_size_gb = module_size * unit_multipliers[module_unit]
                    calculated_total_gb += num_modules * module_size_gb
                    breakdown_details.append(f"{num_modules} * {module_size}{module_unit} = {num_modules * module_size}{module_unit}")
            
            if breakdown_details and abs(calculated_total_gb - total_size_gb) > 0.001:
                breakdown_calc_str = " + ".join(breakdown_details)
                mismatch_msg = (
                    f"RAM: {ram_breakdown} ({breakdown_calc_str} = {calculated_total_gb}{total_unit}) "
                    f"does not equal total RAM size {ram_size}."
                )
                if mismatch_msg not in [m[0] for m in misc_issues]:
                    misc_issues.append((mismatch_msg,))
                misc_info.append(f"  - RAM Breakdown Verification: FAILED - Calculation mismatch")
                logger.debug(mismatch_msg, extra={'session_id': current_session_id})
            elif breakdown_details:
                breakdown_calc_str = " + ".join(breakdown_details)
                misc_info.append(f"  - RAM Breakdown Verification: PASSED - {breakdown_calc_str} = {calculated_total_gb}{total_unit} matches {ram_size}")
                logger.debug(f"RAM breakdown verified: {ram_breakdown} ({breakdown_calc_str} = {calculated_total_gb}{total_unit}) matches {ram_size}", extra={'session_id': current_session_id})
            else:
                mismatch_msg = "Unable to parse RAM breakdown for verification."
                if mismatch_msg not in [m[0] for m in misc_issues]:
                    misc_issues.append((mismatch_msg,))
                misc_info.append("  - RAM Breakdown Verification: FAILED - Unable to parse breakdown")
                logger.debug(f"Failed to parse RAM breakdown '{ram_breakdown}'", extra={'session_id': current_session_id})
        else:
            mismatch_msg = "Unable to parse total RAM size for verification."
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            misc_info.append("  - RAM Breakdown Verification: FAILED - Unable to parse total RAM size")
            logger.debug(f"Failed to parse total RAM size '{ram_size}'", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - RAM Breakdown Verification: SKIPPED - No RAM breakdown or size specified")

def check_ram_configuration_validation(listing_data, multiple_entries, misc_info, misc_issues):
    """RAM Configuration Validation Check"""
    shared_values = listing_data.get('table_shared', {})
    
    ram_config_key = 'table_ram_config_key'
    ram_modules_key = 'table_ram_modules_key'
    ram_module_size_key = 'table_ram_module_size_key'
    ram_size_key = 'table_ram_size_key'

    ram_config_checks = 0
    ram_config_issues = 0

    # Use ram_config_key first if it contains actual configuration data
    if ram_config_key in shared_values and ram_size_key in shared_values and shared_values[ram_config_key].strip():
        # Check if ram_config_key contains actual configuration (not just a number)
        ram_config = shared_values[ram_config_key]
        if 'x' in ram_config.lower() or 'gb' in ram_config.lower() or 'mb' in ram_config.lower() or 'tb' in ram_config.lower():
            ram_size = shared_values[ram_size_key]
            ram_config_checks += 1
            
            is_valid, message = validate_ram_configuration_helper(ram_config, ram_size)
            if not is_valid:
                ram_config_issues += 1
                entry_label = "Table" if not multiple_entries else "Shared Values"
                mismatch_msg = f"RAM mismatch: {entry_label}: {message.split(':', 1)[1].strip() if ':' in message else message}"
                if mismatch_msg not in [m[0] for m in misc_issues]:
                    misc_issues.append((mismatch_msg,))
                    logger.debug(mismatch_msg, extra={'session_id': current_session_id})
        elif ram_modules_key in shared_values and ram_module_size_key in shared_values:
            # Fallback: construct configuration from modules + module size
            ram_config_checks += 1
            modules_str = shared_values[ram_modules_key]
            module_count = None
            try:
                module_count = int(modules_str)
            except ValueError:
                match = re.search(r'(\d+)\s*x', modules_str)
                if match:
                    module_count = int(match.group(1))
            
            if module_count is not None:
                module_size_str = shared_values[ram_module_size_key]
                module_size_gb = None
                match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|tb|mb)', module_size_str.lower())
                if match:
                    size, unit = match.groups()
                    if unit == 'gb':
                        module_size_gb = float(size)
                    elif unit == 'tb':
                        module_size_gb = float(size) * 1000
                    elif unit == 'mb':
                        module_size_gb = float(size) / 1000
                
                total_size_str = shared_values[ram_size_key]
                total_size_gb = None
                match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|tb|mb)', total_size_str.lower())
                if match:
                    size, unit = match.groups()
                    if unit == 'gb':
                        total_size_gb = float(size)
                    elif unit == 'tb':
                        total_size_gb = float(size) * 1000
                    elif unit == 'mb':
                        total_size_gb = float(size) / 1000
                
                if module_count is not None and module_size_gb is not None and total_size_gb is not None:
                    expected_total = module_count * module_size_gb
                    
                    if abs(expected_total - total_size_gb) > 0.1:
                        ram_config_issues += 1
                        entry_label = "Table" if not multiple_entries else "Shared Values"
                        mismatch_msg = (
                            f"RAM mismatch: {entry_label}: "
                            f"{module_count}x{module_size_str} = {expected_total}GB, "
                            f"but total displayed: {total_size_str}."
                        )
                        if mismatch_msg not in [m[0] for m in misc_issues]:
                            misc_issues.append((mismatch_msg,))
                            logger.debug(mismatch_msg, extra={'session_id': current_session_id})
    elif ram_modules_key in shared_values and ram_module_size_key in shared_values and ram_size_key in shared_values:
        # Use modules + module size when no config key is available
        ram_config_checks += 1
        modules_str = shared_values[ram_modules_key]
        module_count = None
        try:
            module_count = int(modules_str)
        except ValueError:
            match = re.search(r'(\d+)\s*x', modules_str)
            if match:
                module_count = int(match.group(1))
        
        if module_count is not None:
            module_size_str = shared_values[ram_module_size_key]
            module_size_gb = None
            match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|tb|mb)', module_size_str.lower())
            if match:
                size, unit = match.groups()
                if unit == 'gb':
                    module_size_gb = float(size)
                elif unit == 'tb':
                    module_size_gb = float(size) * 1000
                elif unit == 'mb':
                    module_size_gb = float(size) / 1000
            
            total_size_str = shared_values[ram_size_key]
            total_size_gb = None
            match = re.search(r'(\d+(?:\.\d+)?)\s*(gb|tb|mb)', total_size_str.lower())
            if match:
                size, unit = match.groups()
                if unit == 'gb':
                    total_size_gb = float(size)
                elif unit == 'tb':
                    total_size_gb = float(size) * 1000
                elif unit == 'mb':
                    total_size_gb = float(size) / 1000
            
            if module_count is not None and module_size_gb is not None and total_size_gb is not None:
                expected_total = module_count * module_size_gb
                
                if abs(expected_total - total_size_gb) > 0.1:
                    ram_config_issues += 1
                    entry_label = "Table" if not multiple_entries else "Shared Values"
                    mismatch_msg = (
                        f"RAM mismatch: {entry_label}: "
                        f"{module_count}x{module_size_str} = {expected_total}GB, "
                        f"but total displayed: {total_size_str}."
                    )
                    if mismatch_msg not in [m[0] for m in misc_issues]:
                        misc_issues.append((mismatch_msg,))
                        logger.debug(mismatch_msg, extra={'session_id': current_session_id})

    for entry_idx, entry in enumerate(listing_data.get('table_data', []), 1):
        combined_entry = {**shared_values, **entry}
        
        # Use ram_config_key first if it contains actual configuration data
        if ram_config_key in combined_entry and ram_size_key in combined_entry:
            ram_config = combined_entry[ram_config_key]
            if ram_config.strip() and ('x' in ram_config.lower() or 'gb' in ram_config.lower() or 'mb' in ram_config.lower() or 'tb' in ram_config.lower()):
                if any(key in entry for key in [ram_config_key, ram_size_key]):
                    ram_config_checks += 1
                    ram_size = combined_entry[ram_size_key]
                    
                    is_valid, message = validate_ram_configuration_helper(ram_config, ram_size)
                    if not is_valid:
                        ram_config_issues += 1
                        entry_label = f"Entry {entry_idx}" if multiple_entries else "Table"
                        mismatch_msg = f"RAM mismatch: {entry_label}: {message.split(':', 1)[1].strip() if ':' in message else message}"
                        if mismatch_msg not in [m[0] for m in misc_issues]:
                            misc_issues.append((mismatch_msg,))
                            logger.debug(mismatch_msg, extra={'session_id': current_session_id})

    if ram_config_checks > 0:
        if ram_config_issues > 0:
            misc_info.append(f"  - RAM Configuration Validation: FAILED - {ram_config_issues}/{ram_config_checks} configurations invalid")
        else:
            misc_info.append(f"  - RAM Configuration Validation: PASSED - All {ram_config_checks} configurations valid")
    else:
        misc_info.append("  - RAM Configuration Validation: SKIPPED - No RAM configurations found")
        
def check_category_mismatch(title, leaf_category, misc_info, misc_issues):
    # Commented out the entire function body to turn off the category mismatch check
    # if ('type_key' in title and title['type_key'].lower() == 'laptop' and 
    #     leaf_category and leaf_category.lower() != 'electronics' and
    #     leaf_category.lower() not in component_categories and 
    #     leaf_category.lower() not in laptop_leaf_categories):
    #     title_text = listing_data['title'].get('title_title_key', '').lower()
    #     ignore_phrases = ['for laptops', 'docking station', 'monitor']
    #     should_ignore = any(phrase in title_text for phrase in ignore_phrases)
    #     
    #     if not should_ignore:
    #         mismatch_msg = (
    #             f"Title type 'Laptop' does not match category '{leaf_category}' - "
    #             f"expected one of {', '.join(laptop_leaf_categories)}.")
    #         if mismatch_msg not in [m[0] for m in misc_issues]:
    #             misc_issues.append((mismatch_msg,))
    #         misc_info.append(f"  - Category vs Type Check: FAILED - Laptop type doesn't match category '{leaf_category}'")
    #         logger.debug(mismatch_msg, extra={'session_id': current_session_id})
    #     else:
    #         misc_info.append(f"  - Category vs Type Check: SKIPPED - Laptop type ignored due to context phrases")
    # else:
    #     misc_info.append("  - Category vs Type Check: PASSED - Type matches category or not applicable")
    pass  # Function disabled

def check_cpu_suffix_for_laptops(title, leaf_category, misc_info, misc_issues, issue_strings):
    """CPU Suffix Check for Laptops"""
    if (leaf_category and leaf_category.lower() in laptop_leaf_categories and
        'cpu_family_key' in title and 'core i' in title['cpu_family_key'].lower() and
        'cpu_model_key' in title):
        
        has_suffix_key = 'cpu_suffix_key' in title
        
        if not has_suffix_key:
            cpu_model = title.get('cpu_model_key', '').strip()
            mismatch_msg = (
                f"CPU: For laptop category '{leaf_category}', Core processor ({title['cpu_family_key']} {cpu_model}) "
                "has no suffix specified. All mobile Core CPUs must have a suffix."
            )
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            if mismatch_msg not in issue_strings:
                issue_strings.append(mismatch_msg)
            misc_info.append(f"  - CPU Suffix Check (Laptops): FAILED - No suffix for Core processor")
            logger.debug(mismatch_msg, extra={'session_id': current_session_id})
        else:
            misc_info.append(f"  - CPU Suffix Check (Laptops): PASSED - CPU suffix present")
    else:
        misc_info.append("  - CPU Suffix Check (Laptops): SKIPPED - Not applicable or no Core CPU")
        
def check_apple_password(specs, leaf_category, misc_info, misc_issues):
    """Password Check for Apple Categories"""
    if leaf_category and leaf_category.lower() in ("apple laptops", "apple desktops & all-in-ones"):
        password_key = None
        if 'os_password_key' in specs:
            password_key = 'os_password_key'
        elif 'password_key' in specs:
            password_key = 'password_key'
        if password_key is None:
            mismatch_msg = "Missing os_password_key or password_key for category: " + leaf_category.capitalize() + "."
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            misc_info.append(f"  - Apple Password Check: FAILED - Missing password key")
        elif not re.match(r'^12345\b', specs[password_key].strip()):
            mismatch_msg = f"{password_key} does not start with '12345', found: {specs[password_key]}."
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            misc_info.append(f"  - Apple Password Check: FAILED - Incorrect password value")
        else:
            misc_info.append(f"  - Apple Password Check: PASSED - Password starts with '12345'")
    else:
        misc_info.append("  - Apple Password Check: SKIPPED - Not Apple category")
        
def _is_spec_table_format(table_entries):
    """
    Heuristic to detect a spec-table format where each 'Entry' corresponds to
    a single field/value row (e.g., VGA: Yes, HDMI: Yes), not multiple items.
    In this format, the number of entries reflects the number of spec rows,
    not the lot size. We therefore skip lot count comparisons.
    """
    try:
        if not table_entries:
            return False
        total = len(table_entries)
        single_key_rows = 0
        for entry in table_entries:
            if not isinstance(entry, dict):
                continue
            keys = [k for k in entry.keys() if not (isinstance(k, str) and (k.startswith('__') or k.endswith('__')))]
            table_keys = [k for k in keys if isinstance(k, str) and k.startswith('table_')]
            # Treat rows with exactly one table_* key as spec rows
            if len(table_keys) == 1:
                single_key_rows += 1
        # Spec table usually has many rows and mostly single-key entries
        return (total >= 12) and (single_key_rows / max(total, 1) >= 0.8)
    except Exception:
        return False

def check_lot_amount_consistency(title, meta, listing_data, misc_info, misc_issues, issue_strings):
    """Lot Amount Consistency Check"""
    
    # Skip when table appears to be a spec table (per-field rows), not a multi-item lot
    try:
        _table_entries = listing_data.get('table_data', [])
    except Exception:
        _table_entries = []
    if _is_spec_table_format(_table_entries):
        misc_info.append("  - Lot Amount: SKIPPED - Spec table format (one field per entry)")
        logger.debug("Skipping lot amount check due to spec-table format (per-field rows)", extra={'session_id': current_session_id})
        return

    # Skip lot amount check for Server Memory (RAM) as it uses a different table format
    device_type = title.get('device_type_key', '').strip()
    title_device_type = title.get('title_device_type_key', '').strip()
    device_type_lower = device_type.lower()
    title_device_type_lower = title_device_type.lower()
    if device_type == 'Server Memory (RAM)':
        misc_info.append("  - Lot Amount: SKIPPED - Server Memory (RAM) uses different table format")
        logger.debug(f"Skipping lot amount check for Server Memory (RAM) device type", extra={'session_id': current_session_id})
        return
    
    # Skip lot amount check for CPUs/Processors
    if device_type_lower == 'cpus/processors' or title_device_type_lower == 'cpus/processors':
        misc_info.append("  - Lot Amount: SKIPPED - CPUs/Processors do not require lot check")
        logger.debug(f"Skipping lot amount check for CPUs/Processors (device_type or title_device_type)", extra={'session_id': current_session_id})
        return

    # Skip lot amount check for Graphics/Video Cards
    # Memory: At user request, do not enforce lot amount in title for GPUs
    if device_type_lower == 'graphics/video cards' or title_device_type_lower == 'graphics/video cards':
        misc_info.append("  - Lot Amount: SKIPPED - Graphics/Video Cards do not require lot check")
        logger.debug(f"Skipping lot amount check for Graphics/Video Cards (device_type or title_device_type)", extra={'session_id': current_session_id})
        return

    # Skip lot amount check when title indicates Computer Components & Parts
    if title_device_type_lower == 'computer components & parts' or device_type_lower == 'computer components & parts':
        misc_info.append("  - Lot Amount: SKIPPED - Title device type indicates Computer Components & Parts")
        logger.debug(f"Skipping lot amount check for title device type 'Computer Components & Parts'", extra={'session_id': current_session_id})
        return
    
    T = None
    table_shared = listing_data.get('table_shared', {})
    if 'table_entry_count_key' in table_shared:
        entry_count_str = table_shared['table_entry_count_key']
        count_match = re.search(r'Total Entries:\s*(\d+)', entry_count_str)
        if count_match:
            T = int(count_match.group(1))
            logger.debug(f"Using table_entry_count_key for entry count: {T} (extracted from '{entry_count_str}')", 
                         extra={'session_id': current_session_id})
        else:
            try:
                T = int(entry_count_str)
                logger.debug(f"Using table_entry_count_key for entry count: {T} (direct parse)", 
                             extra={'session_id': current_session_id})
            except ValueError:
                logger.debug(f"Could not parse entry count from '{entry_count_str}'", 
                             extra={'session_id': current_session_id})
                T = None
    
    if T is None:
        highest_entry = 0
        for i, entry in enumerate(listing_data.get('table_data', []), 1):
            highest_entry = max(highest_entry, i)
        
        if highest_entry > 0:
            T = highest_entry
            logger.debug(f"Using highest entry number for count: {T}", 
                         extra={'session_id': current_session_id})
        else:
            T = len(listing_data['table_data'])
            logger.debug(f"Using len(table_data) as last resort for entry count: {T}", 
                         extra={'session_id': current_session_id})
    
    L_title_str = title.get('lot_key', '')
    L_meta_str = meta.get('listinginfo_key', '')

    L_title_match = re.search(r'\d+', L_title_str)
    L_title = int(L_title_match.group()) if L_title_match else None

    if L_meta_str.lower() == "single item":
        L_meta = 1
    else:
        # Only consider metadata values that explicitly denote per-lot quantities
        L_meta_match = (re.search(r'(\d+)\s*(?:items?\s*)?per\s*lot', L_meta_str.lower()) or
                        re.search(r'lot\s+of\s+(\d+)', L_meta_str.lower()))
        L_meta = int(L_meta_match.group(1)) if L_meta_match else None

    logger.debug(f"Lot check: T={T}, L_title={L_title}, L_meta={L_meta}", extra={'session_id': current_session_id})

    lot_mismatch_detected = False

    if T >= 2:
        if L_title is not None:
            symbol = "==" if T == L_title else "≠"
            misc_info.append(f"  - Lot Amount: Table Entries ({T}) {symbol} Title ({L_title})")
            if symbol == "≠":
                lot_mismatch_detected = True
        else:
            misc_info.append(f"  - Lot Amount: Table Entries ({T}) ≠ Title (Not specified)")
            lot_mismatch_detected = True
            issue_str = "Lot amount missing in title when there are multiple table entries."
            if issue_str not in issue_strings:
                issue_strings.append(issue_str)
                misc_issues.append((issue_str,))

        if L_title is not None and L_meta not in (None, 1):
            if L_meta is not None:
                symbol = "==" if L_title == L_meta else "≠"
                misc_info.append(f"  - Lot Amount: Title ({L_title}) {symbol} Metadata ({L_meta})")
                if symbol == "≠":
                    lot_mismatch_detected = True
            else:
                misc_info.append(f"  - Lot Amount: Title ({L_title}) ≠ Metadata (Not specified)")
                lot_mismatch_detected = True
                issue_str = "Lot amount missing in metadata when title specifies a lot amount."
                if issue_str not in issue_strings:
                    issue_strings.append(issue_str)
                    misc_issues.append((issue_str,))
        else:
            misc_info.append(f"  - Lot Amount: Metadata ({L_meta}) - comparison skipped (L_title not specified or L_meta = 1)")

        if lot_mismatch_detected:
            lot_issue_str = f"Lot amounts do not match: Table entries ({T}), Title ({L_title if L_title is not None else 'Not specified'}), Metadata ({L_meta if L_meta is not None else 'Not specified'})"
            if lot_issue_str not in issue_strings and "Lot amount missing" not in ''.join(issue_strings):
                issue_strings.append(lot_issue_str)
                misc_issues.append((lot_issue_str,))
    else:
        misc_info.append(f"  - Lot Amount: Table Entries ({T}) - comparison skipped (T < 2)")
        
def check_shipping_policy(meta, misc_info, misc_issues):
    """Shipping Policy Check"""
    shipping_policy = meta.get('listing_shippingpolicy_key', '')
    # Treat 'Unknown' as unspecified; do not generate an issue for unknown
    if isinstance(shipping_policy, str) and shipping_policy.strip().lower() == 'unknown':
        misc_info.append("  - Shipping Policy Check: SKIPPED - Shipping policy is unknown")
        # Archive full context for later analysis
        try:
            item_number = meta.get('itemnumber_key') or meta.get('meta_itemnumber_key') or 'Unknown'
        except Exception:
            item_number = 'Unknown'
        _archive_trouble(item_number, reason='shipping_policy_unknown')
        return
    if shipping_policy:
        if not shipping_policy.startswith('TECHREDO'):
            mismatch_msg = f"Shipping policy does not begin with 'TECHREDO': {shipping_policy}"
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            misc_info.append(f"  - Shipping Policy Check: FAILED - Does not start with 'TECHREDO'")
            logger.debug(mismatch_msg, extra={'session_id': current_session_id})
        else:
            misc_info.append(f"  - Shipping Policy Check: PASSED - Starts with 'TECHREDO'")
    else:
        misc_info.append("  - Shipping Policy Check: SKIPPED - No shipping policy specified")

def check_return_policy(meta, misc_info, misc_issues):
    """Return Policy Check"""
    return_policy = meta.get('listing_returnpolicy_key', '')
    # Ignore unknown return policy values
    if return_policy.strip().lower() == 'unknown':
        misc_info.append("  - Return Policy Check: SKIPPED - Return policy is unknown")
        return
    expected_return_policy = "Returns Accepted,Buyer,30 Days,Money Back#0"
    if return_policy:
        if return_policy != expected_return_policy:
            mismatch_msg = f"Return policy is incorrect. Expected: '{expected_return_policy}', Found: '{return_policy}'"
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
            misc_info.append(f"  - Return Policy Check: FAILED - Incorrect policy")
            logger.debug(mismatch_msg, extra={'session_id': current_session_id})
        else:
            misc_info.append(f"  - Return Policy Check: PASSED - Correct policy")
    else:
        misc_info.append("  - Return Policy Check: SKIPPED - No return policy specified")

def check_phrase_and_spelling(title_text, leaf_category, misc_info, misc_issues):
    """Phrase and Spelling Checks"""
    preferred_spellings = load_preferred_spellings()
    correct_phrases = load_correct_phrases()
    correct_phrases_ci = load_correct_phrases_ci()
    preferred_terms_to_check = get_terms_for_category(preferred_spellings, leaf_category)
    correct_phrases_to_check = get_terms_for_category(correct_phrases, leaf_category)
    correct_phrases_ci_to_check = get_terms_for_category(correct_phrases_ci, leaf_category)
    
    spelling_issues_found = 0
    terms_checked = len(preferred_terms_to_check) + len(correct_phrases_to_check) + len(correct_phrases_ci_to_check)
    
    logger.debug(f"Title text: '{title_text}'", extra={'session_id': current_session_id})
    logger.debug(f"Preferred terms to check: {preferred_terms_to_check}", extra={'session_id': current_session_id})
    logger.debug(f"Correct phrases to check: {correct_phrases_to_check}", extra={'session_id': current_session_id})
    logger.debug(f"Correct phrases CI to check: {correct_phrases_ci_to_check}", extra={'session_id': current_session_id})

    if not preferred_terms_to_check:
        logger.warning("No preferred terms to check; spelling checks skipped", extra={'session_id': current_session_id})
    else:
        for term in preferred_terms_to_check:
            pattern = r'\b' + re.escape(term) + r'\b'
            matches = list(re.finditer(pattern, title_text, re.IGNORECASE))
            if matches:
                for match in matches:
                    found = match.group(0)
                    logger.debug(f"Found potential match for '{term}': '{found}'", extra={'session_id': current_session_id})
                    if not found.isupper() and found != term:
                        issue_text = f"Incorrect capitalization for '{term}': found '{found}'"
                        if issue_text not in [m[0] for m in misc_issues]:
                            misc_issues.append((issue_text,))
                            spelling_issues_found += 1
                            logger.debug(f"Flagged issue: {issue_text}", extra={'session_id': current_session_id})
            else:
                logger.debug(f"No match found for term '{term}'", extra={'session_id': current_session_id})

    for phrase in correct_phrases_to_check:
        if '*' in phrase:
            words = [w.strip() for w in phrase.split('*') if w.strip()]
            in_order_cs, _ = check_words_in_order(title_text, words, case_sensitive=True)
            if not in_order_cs:
                in_order_ci, _ = check_words_in_order(title_text, words, case_sensitive=False)
                if in_order_ci:
                    issue_text = f"Capitalization issue in wildcard phrase (case-sensitive): '{phrase}'"
                    misc_issues.append((issue_text,))
                    spelling_issues_found += 1
                if all(re.search(r'\b' + re.escape(word) + r'\b', title_text) for word in words):
                    issue_text = f"Words of wildcard phrase (case-sensitive) '{phrase}' are not in correct order"
                    misc_issues.append((issue_text,))
                    spelling_issues_found += 1
        else:
            pattern = r'\b' + re.escape(phrase) + r'\b'
            if not re.search(pattern, title_text):
                if re.search(pattern, title_text, re.IGNORECASE):
                    issue_text = f"Capitalization issue for exact phrase (case-sensitive): '{phrase}'"
                    misc_issues.append((issue_text,))
                    spelling_issues_found += 1

    for phrase in correct_phrases_ci_to_check:
        if '*' in phrase:
            words = [w.strip().lower() for w in phrase.split('*') if w.strip()]
            in_order_ci, _ = check_words_in_order(title_text.lower(), words, case_sensitive=False)
            if not in_order_ci:
                if all(re.search(r'\b' + re.escape(word) + r'\b', title_text.lower(), re.IGNORECASE) for word in words):
                    issue_text = f"Words of wildcard phrase (case-insensitive) '{phrase}' are not in correct order"
                    misc_issues.append((issue_text,))
                    spelling_issues_found += 1

    if terms_checked > 0:
        if spelling_issues_found > 0:
            misc_info.append(f"  - Phrase and Spelling Checks: FAILED - {spelling_issues_found} issues found ({terms_checked} terms checked)")
        else:
            misc_info.append(f"  - Phrase and Spelling Checks: PASSED - No issues found ({terms_checked} terms checked)")
    else:
        misc_info.append("  - Phrase and Spelling Checks: SKIPPED - No terms configured for this category")

def check_form_factor_issues(title, specs, table, misc_info, misc_issues):
    """Form Factor Checks"""
    title_text_lower = title.get('title_key', '').lower()
    form_factor_issues = 0
    if 'micro' in title_text_lower or 'mini pc' in title_text_lower:
        form_factor = specs.get('form_factor_key', '').strip().lower()
        if form_factor == 'sff':
            mismatch_msg = "Title contains 'micro' or 'mini pc', specifics form factor is 'SFF'."
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
                form_factor_issues += 1

    brand = None
    for data in [title, specs, table]:
        if 'brand_key' in data:
            brand = data['brand_key'].strip().lower()
            break

    if brand == "dell":
        specs_ff = specs.get('form_factor_key', '').strip().lower()
        table_ff = table.get('form_factor_key', '').strip().lower()
        if specs_ff == "sff" and table_ff != "sff":
            mismatch_msg = "For Dell brand, specifics form factor is SFF, table form factor is not SFF."
            if mismatch_msg not in [m[0] for m in misc_issues]:
                misc_issues.append((mismatch_msg,))
                form_factor_issues += 1

    if form_factor_issues > 0:
        misc_info.append(f"  - Form Factor Checks: FAILED - {form_factor_issues} issues found")
    else:
        misc_info.append("  - Form Factor Checks: PASSED - No form factor issues")

def check_condition_fields(title, specs, table, meta, description, is_power_adapter, misc_info, misc_issues, store_category=None):
    """Condition Checks"""
    conditions = {
        'cosmeticcondition_key': 'Cosmetic',
        'functionalcondition_key': 'Functional',
        'datasanitization_key': 'Data Sanitization'
    }
    condition_values = {
        'cosmeticcondition_key': None,
        'functionalcondition_key': None,
        'datasanitization_key': None
    }

    # Hard drive categories that should skip data sanitization checks
    hard_drive_categories_exempt = [
        'Hard Drives (SAS)',
        'Hard Drives (SATA)',
        'Laptop Hard Drives (SATA)'
    ]

    conditions_found = 0
    for key, label in conditions.items():
        found = False
        for data, prefix in [(title, 'title'), (specs, 'specs'), (table, 'table'), (meta, 'meta'), (description, 'desc')]:
            check_key = f"{prefix}_{key}" if prefix != 'desc' else key
            if prefix == 'desc':
                check_key = key
            if check_key in data:
                condition_values[key] = data[check_key]
                misc_info.append(f"  - {label}: {data[check_key]} (from {prefix})")
                found = True
                conditions_found += 1
                break
        if not found:
            # Skip data sanitization check for specific hard drive categories
            if key == 'datasanitization_key' and store_category and store_category.strip() in hard_drive_categories_exempt:
                misc_info.append(f"  - Data Sanitization: SKIPPED - Exempt category: {store_category}")
                continue
            
            if not (key == 'datasanitization_key' and
                    is_power_adapter and
                    ((condition_values.get('cosmeticcondition_key') or '').strip().startswith("C10") or
                     (condition_values.get('functionalcondition_key') or '').strip().startswith("F10"))):
                issue_text = f" missing {label}."
                if issue_text not in [m[0] for m in misc_issues]:
                    misc_issues.append((issue_text,))

    misc_info.append(f"  - Condition Fields Found: {conditions_found}/3 conditions specified")

def check_for_parts_condition(specs, title, listing_data, sections, misc_info, misc_issues):
    """New Condition Check"""
    condition_check_performed = False
    if specs.get('condition_key', '').lower() == 'for parts or not working':
        condition_check_performed = True
        title_text = title.get('title_title_key', title.get('title_key', ''))
        bad_keywords = ['bad', 'cracked', 'broken', 'damaged', 'defective', 'not working', 'for parts', 'as is', 'junk', 'faulty', 'locked', 'damage']
        logger.debug(f"Title text for condition check: '{title_text}'", extra={'session_id': current_session_id})
        title_indicates_bad = any(re.search(r'\b' + re.escape(keyword) + r'\b', title_text, re.IGNORECASE) for keyword in bad_keywords) or re.search(r'\bpressure spots\b', title_text, re.IGNORECASE)
        logger.debug(f"title_indicates_bad: {title_indicates_bad}, matched keywords: {[keyword for keyword in bad_keywords if re.search(r'\b' + re.escape(keyword) + r'\b', title_text, re.IGNORECASE)]}", extra={'session_id': current_session_id})
        
        seller_notes_indicates_bad = 'untested' in specs.get('seller_notes_key', '').lower()
        logger.debug(f"seller_notes_indicates_bad: {seller_notes_indicates_bad}", extra={'session_id': current_session_id})
        
        table_indicates_bad = False
        if listing_data['table_data']:
            table_indicates_bad = all(
                'not working' in entry.get('table_functionalcondition_key', '').lower() or
                entry.get('table_functionalcondition_key', '').lower().startswith('f1') or
                any(re.search(r'\b' + re.escape(keyword) + r'\b', entry.get('table_notes_key', '').lower(), re.IGNORECASE) for keyword in bad_keywords) or
                any(re.search(r'\b' + re.escape(keyword) + r'\b', entry.get('table_defects_key', '').lower(), re.IGNORECASE) for keyword in bad_keywords) or
                re.search(r'\bpressure spots\b', entry.get('table_notes_key', ''), re.IGNORECASE) or
                re.search(r'\bpressure spots\b', entry.get('table_defects_key', ''), re.IGNORECASE)
                for entry in listing_data['table_data']
            )
        logger.debug(f"table_indicates_bad: {table_indicates_bad}", extra={'session_id': current_session_id})
        
        misc_info.append(f"  - 'For Parts' Condition Validation: PERFORMED - Title bad: {title_indicates_bad}, Seller notes bad: {seller_notes_indicates_bad}, Table bad: {table_indicates_bad}")
        
        power_tested_found = False
        trigger_phrases = ['untested', 'power tested', 'has not been tested', 'not been power tested', 'unable to test', 'damaged']
        trigger_regex = r'\b(' + '|'.join(re.escape(phrase) for phrase in trigger_phrases + bad_keywords) + r')\b'
        logger.debug(f"Trigger regex: {trigger_regex}", extra={'session_id': current_session_id})
        
        sources_checked = ['title', 'seller_notes', 'table_entries', 'description']
        sources_with_triggers = []
        
        if title.get('title_title_key', title.get('title_key', '')):
            title_text = title.get('title_title_key', title.get('title_key', ''))
            if re.search(trigger_regex, title_text, re.IGNORECASE):
                if not (re.search(r'\b(if damaged|if the item is damaged)\b', title_text, re.IGNORECASE) and 'damaged' in title_text.lower()):
                    power_tested_found = True
                    sources_with_triggers.append('title')
        
        if specs.get('seller_notes_key', ''):
            seller_notes_text = specs.get('seller_notes_key', '')
            if re.search(trigger_regex, seller_notes_text, re.IGNORECASE):
                if not (re.search(r'\b(if damaged|if the item is damaged)\b', seller_notes_text, re.IGNORECASE) and 'damaged' in seller_notes_text.lower()):
                    power_tested_found = True
                    sources_with_triggers.append('seller_notes')
        
        if listing_data['table_data']:
            for entry in listing_data['table_data']:
                if entry.get('table_notes_key', '') and re.search(trigger_regex, entry.get('table_notes_key', ''), re.IGNORECASE):
                    if not (re.search(r'\b(if damaged|if the item is damaged)\b', entry.get('table_notes_key', ''), re.IGNORECASE) and 'damaged' in entry.get('table_notes_key', '').lower()):
                        power_tested_found = True
                        sources_with_triggers.append('table_notes')
                        break
                if entry.get('table_defects_key', '') and re.search(trigger_regex, entry.get('table_defects_key', ''), re.IGNORECASE):
                    if not (re.search(r'\b(if damaged|if the item is damaged)\b', entry.get('table_defects_key', ''), re.IGNORECASE) and 'damaged' in entry.get('table_defects_key', '').lower()):
                        power_tested_found = True
                        sources_with_triggers.append('table_defects')
                        break
        
        description_lines = sections.get('DESCRIPTION', [])
        logger.debug(f"Description lines: {description_lines}", extra={'session_id': current_session_id})
        if not description_lines and description.get('description_text_key', ''):
            description_lines = description['description_text_key'].split('\n')
            logger.debug(f"Fallback description lines: {description_lines}", extra={'session_id': current_session_id})
        for line in description_lines:
            if re.search(trigger_regex, line, re.IGNORECASE):
                if not (re.search(r'\b(if damaged|if the item is damaged)\b', line, re.IGNORECASE) and 'damaged' in line.lower()):
                    power_tested_found = True
                    sources_with_triggers.append('description')
                    logger.debug(f"Power tested found in line: '{line}'", extra={'session_id': current_session_id})
                    break
        
        if sources_with_triggers:
            misc_info.append(f"  - Trigger Phrase Detection: FOUND in {', '.join(sources_with_triggers)}")
        else:
            misc_info.append(f"  - Trigger Phrase Detection: NOT FOUND in any sources")
        
        logger.debug(f"power_tested_found: {power_tested_found}", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - 'For Parts' Condition Validation: SKIPPED - Not 'for parts or not working' condition")

def check_untested_items_detection(title, specs, listing_data, sections, description, misc_info):
    """Not Power Tested Functional Condition Check"""
    if (listing_data['title'].get('title_title_key', listing_data['title'].get('title_key', '')) != 'Unknown Title' and 
        listing_data['title'].get('title_model_key', '') != 'Unknown Title'):
        not_power_tested_found = False
        untested_sources = []
        
        if title.get('title_title_key', title.get('title_key', '')):
            title_text = title.get('title_title_key', title.get('title_key', ''))
            if check_for_actual_untested_helper(title_text):
                not_power_tested_found = True
                untested_sources.append('title')
                logger.debug(f"Found untested mention in title: '{title_text}'", extra={'session_id': current_session_id})
        
        if specs.get('seller_notes_key', ''):
            seller_notes_text = specs.get('seller_notes_key', '')
            if check_for_actual_untested_helper(seller_notes_text):
                not_power_tested_found = True
                untested_sources.append('seller_notes')
                logger.debug(f"Found untested mention in seller notes: '{seller_notes_text}'", extra={'session_id': current_session_id})
        
        if listing_data['table_data']:
            for entry in listing_data['table_data']:
                if entry.get('table_notes_key', '') and check_for_actual_untested_helper(entry.get('table_notes_key', '')):
                    not_power_tested_found = True
                    untested_sources.append('table_notes')
                    logger.debug(f"Found untested mention in table notes: '{entry.get('table_notes_key', '')}'", extra={'session_id': current_session_id})
                    break
                if entry.get('table_defects_key', '') and check_for_actual_untested_helper(entry.get('table_defects_key', '')):
                    not_power_tested_found = True
                    untested_sources.append('table_defects')
                    logger.debug(f"Found untested mention in table defects: '{entry.get('table_defects_key', '')}'", extra={'session_id': current_session_id})
                    break
        
        description_lines = sections.get('DESCRIPTION', [])
        if not description_lines and description.get('description_text_key', ''):
            description_lines = description['description_text_key'].split('\n')
        for line in description_lines:
            if check_for_actual_untested_helper(line):
                not_power_tested_found = True
                untested_sources.append('description')
                logger.debug(f"Found untested mention in description: '{line}'", extra={'session_id': current_session_id})
                break

        if untested_sources:
            misc_info.append(f"  - Untested Items Detection: FOUND in {', '.join(untested_sources)}")
        else:
            misc_info.append("  - Untested Items Detection: NOT FOUND - No actual untested items detected")
    else:
        misc_info.append("  - Untested Items Detection: SKIPPED - Unknown title or model")

# Main function
def check_missing_storage_vs_capacity(listing_data, misc_info, misc_issues):
    """Check for contradiction between missing storage components and storage capacity in same entry"""
    table_entries = listing_data.get('table_data', [])
    shared_values = listing_data.get('table_shared', {})
    title_data = listing_data.get('title', {})
    specs_data = listing_data.get('specifics', {})
    
    storage_terms = [
        'no storage', 'storage not included', 'missing storage',
        'no hard drive', 'hard drive not included',
        'no hdd', 'hdd not included',
        'no ssd', 'ssd not included',
        'disk not included', 'drive not included',
        'without storage', 'without hard drive', 'without hdd', 'without ssd',
        'no internal storage', 'no internal drive', 'no disk drive',
        'no nvme', 'nvme not included', 'no nvme drive', 'no internal disk'
    ]
    storage_capacity_base_keys = [
        'storage_capacity', 
        'ssd_capacity', 
        'hdd_capacity',
        'storage',
        'hard_drive',
        'drive_capacity',
        'storage_size'
    ]
    
    contradictions_found = 0
    
    # Function to find all storage-related keys (including numbered variants)
    def find_storage_keys(data_dict, prefix=''):
        storage_keys = []
        for key, value in data_dict.items():
            if value and value.strip():
                # Remove prefix and _key suffix for comparison
                clean_key = key.replace(prefix, '').replace('_key', '')
                # Remove any trailing numbers to get base key
                base_key = re.sub(r'\d+$', '', clean_key)
                
                if base_key in storage_capacity_base_keys:
                    # Skip "no storage" type values
                    if value.strip().lower() not in ['no storage', 'none', 'no', 'n/a', 'not included', 'missing']:
                        storage_keys.append((key, value.strip()))
        return storage_keys
    
    # Function to check if storage is indicated as not included/missing
    def is_storage_not_included(data_dict, prefix=''):
        # Check missing components with context-aware logic
        missing_key = f"{prefix}missing_components_key"
        if missing_key in data_dict:
            missing_components = data_dict[missing_key].lower()
            
            # Simple storage terms for missing components (with context checking)
            simple_storage_terms = ['ssd', 'hdd', 'hard drive', 'storage', 'harddrive', 'drive', 'disk', 'nvme']
            
            for term in simple_storage_terms:
                if term in missing_components:
                    # Check for clarifying context that indicates it's NOT actually missing
                    # Look for phrases that suggest the storage exists but has some other issue
                    context_phrases = [
                        'no os installed on', 'no operating system on', 'no os on',
                        'not tested on', 'untested on', 'needs os on',
                        'requires os on', 'blank', 'formatted', 'wiped',
                        'no software on', 'no programs on'
                    ]
                    
                    # If we find clarifying context, don't treat as missing
                    has_clarifying_context = any(phrase in missing_components for phrase in context_phrases)
                    if not has_clarifying_context:
                        return True
        
        # Check notes with specific missing storage phrases
        notes_key = f"{prefix}notes_key"
        if notes_key in data_dict:
            notes_text = data_dict[notes_key].lower()
            if any(term in notes_text for term in storage_terms):
                return True
        
        # Check storage status
        status_key = f"{prefix}storage_status_key"
        if status_key in data_dict:
            status_value = data_dict[status_key].lower().strip()
            if status_value in ['not included', 'no', 'none', 'n/a', 'missing']:
                return True
        
        return False
    
    # Determine if this is a single item scenario
    is_single_item = len(table_entries) <= 1
    
    # Check shared values first (this covers single-entry tables)
    if is_storage_not_included(shared_values, 'table_'):
        all_storage_keys = []
        
        # Always check table/shared storage keys
        shared_storage_keys = find_storage_keys(shared_values, 'table_')
        if shared_storage_keys:
            all_storage_keys.extend([(f"Table: {key.replace('table_', '').replace('_key', '')}", value) for key, value in shared_storage_keys])
        
        # For single items, also check title and specs
        if is_single_item:
            title_storage_keys = find_storage_keys(title_data, 'title_')
            if title_storage_keys:
                all_storage_keys.extend([(f"Title: {key.replace('title_', '').replace('_key', '')}", value) for key, value in title_storage_keys])
            
            specs_storage_keys = find_storage_keys(specs_data, 'specs_')
            if specs_storage_keys:
                all_storage_keys.extend([(f"Specs: {key.replace('specs_', '').replace('_key', '')}", value) for key, value in specs_storage_keys])
        
        if all_storage_keys:
            contradictions_found += 1
            entry_label = "Table" if len(table_entries) <= 1 else "Shared Values"
            storage_info = [f"{location}: {value}" for location, value in all_storage_keys]
            
            contradiction_msg = (
                f"Storage contradiction in {entry_label}: Storage marked as not included/missing "
                f"but storage capacity is specified ({'; '.join(storage_info)})"
            )
            misc_issues.append((contradiction_msg,))
            logger.debug(contradiction_msg, extra={'session_id': current_session_id})
    
    # Check title storage status against specs/table for single items
    if is_single_item and is_storage_not_included(title_data, 'title_'):
        all_storage_keys = []
        
        # Check specs and table for storage values
        specs_storage_keys = find_storage_keys(specs_data, 'specs_')
        if specs_storage_keys:
            all_storage_keys.extend([(f"Specs: {key.replace('specs_', '').replace('_key', '')}", value) for key, value in specs_storage_keys])
        
        # Check table entries
        for entry in table_entries:
            entry_storage_keys = find_storage_keys(entry, 'table_')
            if entry_storage_keys:
                all_storage_keys.extend([(f"Table: {key.replace('table_', '').replace('_key', '')}", value) for key, value in entry_storage_keys])
        
        # Also check shared table values
        shared_storage_keys = find_storage_keys(shared_values, 'table_')
        if shared_storage_keys:
            all_storage_keys.extend([(f"Table: {key.replace('table_', '').replace('_key', '')}", value) for key, value in shared_storage_keys])
        
        if all_storage_keys:
            contradictions_found += 1
            storage_info = [f"{location}: {value}" for location, value in all_storage_keys]
            
            contradiction_msg = (
                f"Storage contradiction: Title indicates storage not included "
                f"but storage capacity is specified ({'; '.join(storage_info)})"
            )
            misc_issues.append((contradiction_msg,))
            logger.debug(contradiction_msg, extra={'session_id': current_session_id})
    
    # Check individual table entries (for multi-entry tables)
    for idx, entry in enumerate(table_entries, start=1):
        if len(table_entries) <= 1:
            continue  # Skip if single entry (already checked above)
            
        # Combine shared values with entry-specific values
        combined_entry = {**shared_values, **entry}
        
        if is_storage_not_included(combined_entry, 'table_'):
            # For multi-entry tables, only check table data (each entry represents different items)
            entry_storage_keys = find_storage_keys(combined_entry, 'table_')
            
            if entry_storage_keys:
                contradictions_found += 1
                entry_label = f"Entry {idx}"
                storage_info = [f"{key.replace('table_', '').replace('_key', '')}: {value}" for key, value in entry_storage_keys]
                
                contradiction_msg = (
                    f"Storage contradiction in {entry_label}: Storage marked as not included/missing "
                    f"but storage capacity is specified ({'; '.join(storage_info)})"
                )
                misc_issues.append((contradiction_msg,))
                logger.debug(contradiction_msg, extra={'session_id': current_session_id})
    
    if contradictions_found > 0:
        misc_info.append(f"  - Storage Contradiction Check: FAILED - {contradictions_found} contradictions found")
    else:
        misc_info.append(f"  - Storage Contradiction Check: PASSED - No storage contradictions detected")
        
def check_battery_missing_components(listing_data, misc_info, misc_issues):
    """Check for contradiction between missing battery components and battery/capacity fields in same entry"""
    table_entries = listing_data.get('table_data', [])
    shared_values = listing_data.get('table_shared', {})
    
    battery_fields = ['battery', 'battery_capacity', 'battery_health', 'battery_cycle_count']
    contradictions_found = 0
    
    # Helper: detect if content clearly states only the secondary/external battery is present
    def has_secondary_only_context(data_dict, prefix=''):
        for key, value in data_dict.items():
            if not value or not str(value).strip():
                continue
            clean_key = key.replace(prefix, '').replace('_key', '').lower()
            if 'battery' not in clean_key:
                continue
            val = str(value).strip().lower()
            # Strong indicators that only a secondary battery is present
            if re.search(r'\bsecondary\s+only\b', val) or re.search(r'\bonly\s+secondary\b', val) or re.search(r'\bsecondary\b.*\bonly\b', val):
                return True
            # Treat common secondary/external battery terms as non-primary batteries
            if re.search(r'\bslice\s+battery\b', val) or re.search(r'\bultrabay\s+battery\b', val) or re.search(r'\bexternal\s+battery\b', val):
                return True
            # Mixed-lot clarification like "only one has a battery" should not be a contradiction
            # Recognize partial quantities even if the word 'battery' is omitted in the value text
            if re.search(r'\b(only\s+one|only\s+1|1\s+of|one\s+of|\d+\s*/\s*\d+)\b', val):
                return True
        return False

    # Function to find all battery-related keys (including numbered variants)
    def find_battery_keys(data_dict, prefix=''):
        battery_keys = []
        # If this entry/shared block says 'secondary only', do not treat battery fields as contradictions
        if has_secondary_only_context(data_dict, prefix):
            return battery_keys
        for key, value in data_dict.items():
            if value and value.strip():
                # Remove prefix and _key suffix for comparison
                clean_key = key.replace(prefix, '').replace('_key', '')
                # Remove any trailing numbers to get base key
                base_key = re.sub(r'\d+$', '', clean_key)
                
                if base_key in battery_fields:
                    # Skip "no battery" type values and complex descriptions indicating battery is missing/removed
                    value_lower = value.strip().lower()
                    
                    # Check if starts with negative indicators
                    negative_start_indicators = ['no', 'none', 'n/a', 'not included', 'missing', 'removed']
                    starts_with_negative = any(value_lower.startswith(indicator) for indicator in negative_start_indicators)
                    
                    # Check for complex descriptions that indicate internal battery is removed/missing
                    battery_missing_phrases = [
                        'internal battery removed', 'internal battery missing', 
                        'no internal battery', 'internal battery not included'
                    ]
                    has_internal_battery_missing = any(phrase in value_lower for phrase in battery_missing_phrases)
                    
                    # Do not flag contradictions when wording explicitly says only secondary battery is present
                    secondary_only_patterns = [
                        r'\bsecondary\s+only\b', r'\bonly\s+secondary\b', r'\bsecondary\b.*\bonly\b',
                        r'\bslice\s+battery\b', r'\bultrabay\s+battery\b', r'\bexternal\s+battery\b'
                    ]
                    has_secondary_only = any(re.search(pat, value_lower) for pat in secondary_only_patterns)

                    # Only consider it a positive battery value if it doesn't start with negative indicators
                    # AND doesn't indicate internal battery is missing (for mixed battery situations)
                    if not starts_with_negative and not has_internal_battery_missing and not has_secondary_only:
                        battery_keys.append((key, value.strip()))
        return battery_keys
    
    # Function to check if battery is indicated as missing in components
    def is_battery_missing_in_components(data_dict, prefix=''):
        missing_key = f"{prefix}missing_components_key"
        if missing_key in data_dict and data_dict[missing_key]:
            # Normalize the missing components text and remove non-primary battery mentions
            # so that entries like "CMOS battery" or "RTC battery" don't count as the main battery.
            import re
            missing_components_raw = str(data_dict[missing_key]).lower()
            normalized_missing = missing_components_raw
            non_primary_patterns = [
                r"\bcmos\s*battery\b",
                r"\brtc\s*battery\b",
                r"\bcoin[-\s]?cell\s*battery\b",
                r"\bcoin[-\s]?cell\b",
                r"\bcr2032\b",
                r"\bbios\s*battery\b",
                r"\bclock\s*battery\b",
            ]
            for pat in non_primary_patterns:
                normalized_missing = re.sub(pat, ' ', normalized_missing)

            # If, after removing non-primary mentions, no 'battery' remains, it's not a main battery issue
            if 'battery' not in normalized_missing:
                return False

            # If the missing text clearly refers to a secondary/numbered battery (e.g., "battery 2")
            # and the values indicate partial inclusion or secondary-only context, treat as consistent.
            if is_secondary_battery_missing_text(normalized_missing) and (
                has_secondary_only_context(data_dict, prefix) or
                any(
                    is_partial_battery_inclusion_text(val)
                    for key, val in data_dict.items()
                    if key.startswith(prefix) and 'battery' in key.lower()
                )
            ):
                return False
            return True
        return False
    
    # Function to check if title indicates no battery
    def is_battery_not_included_in_title(data_dict, prefix=''):
        battery_terms = [
            'no battery', 'battery not included', 'missing battery',
            'without battery', 'no internal battery', 'battery removed',
            'no primary battery', 'without primary battery', 'no main battery',
            'primary battery missing', 'missing primary battery'
        ]
        
        # Check various title fields
        title_fields = [f"{prefix}title_key", f"{prefix}title_title_key", f"{prefix}text_key"]
        for field in title_fields:
            if field in data_dict and data_dict[field]:
                title_text = data_dict[field].lower()
                if any(term in title_text for term in battery_terms):
                    return True
        return False
    
    # Exemption: detect partial battery inclusion like "1 of 2 included"
    def is_partial_battery_inclusion_text(text):
        import re
        if not text:
            return False
        lowered_text = str(text).lower()
        # Detect quantities like "1 of 2" or "1/2" optionally followed by inclusion wording
        return re.search(r"\b(\d+\s*of\s*\d+|\d+\s*/\s*\d+)\b", lowered_text) is not None
    
    # Detect when the missing component refers to a secondary battery only
    def is_secondary_battery_missing_text(text):
        import re
        if not text:
            return False
        lowered_text = str(text).lower()
        return re.search(r"(\bbattery\s*2\b|\b2nd\s*battery\b|\bsecond\s*battery\b|\bsecondary\s*battery\b)", lowered_text) is not None
    
    # Determine if this is a single item scenario
    is_single_item = len(table_entries) <= 1
    
    # Track whether we've already reported a battery contradiction to avoid duplicate messages
    battery_contradiction_reported = False
    
    # Check shared values first (this covers single-entry tables)
    if is_battery_missing_in_components(shared_values, 'table_'):
        shared_battery_keys = find_battery_keys(shared_values, 'table_')
        
        if shared_battery_keys:
            missing_text = shared_values.get('table_missing_components_key', '')
            has_partial_inclusion = any(is_partial_battery_inclusion_text(value) for _, value in shared_battery_keys)
            # Skip contradiction when partial inclusion is indicated OR the missing component refers to a secondary battery
            if not (has_partial_inclusion or is_secondary_battery_missing_text(missing_text)):
                contradictions_found += 1
                entry_label = "Table" if len(table_entries) <= 1 else "Shared Values"
                battery_info = [f"{key.replace('table_', '').replace('_key', '')}: {value}" for key, value in shared_battery_keys]
                
                contradiction_msg = (
                    f"Battery contradiction in {entry_label}: Battery marked as missing component "
                    f"but battery field(s) have values ({'; '.join(battery_info)})"
                )
                misc_issues.append((contradiction_msg,))
                logger.debug(contradiction_msg, extra={'session_id': current_session_id})
                battery_contradiction_reported = True
    
    # Check title battery status against table for single items (shared values only)
    title_data = listing_data.get('title', {})
    if is_single_item and is_battery_not_included_in_title(title_data, 'title_'):
        all_battery_keys = []
        
        # Check table entries for battery values
        for entry in table_entries:
            entry_battery_keys = find_battery_keys(entry, 'table_')
            if entry_battery_keys:
                all_battery_keys.extend([(f"Table: {key.replace('table_', '').replace('_key', '')}", value) for key, value in entry_battery_keys])
        
        # Also check shared table values
        shared_battery_keys = find_battery_keys(shared_values, 'table_')
        if shared_battery_keys:
            all_battery_keys.extend([(f"Table: {key.replace('table_', '').replace('_key', '')}", value) for key, value in shared_battery_keys])
        
        if all_battery_keys:
            # Exemption: if any table value indicates partial inclusion, don't treat as a title contradiction
            has_partial_inclusion = any(is_partial_battery_inclusion_text(value) for _, value in all_battery_keys)
            if (not has_partial_inclusion) and (not battery_contradiction_reported):
                contradictions_found += 1
                battery_info = [f"{location}: {value}" for location, value in all_battery_keys]
                contradiction_msg = (
                    f"Battery contradiction: Title indicates no battery "
                    f"but battery field(s) have values ({'; '.join(battery_info)})"
                )
                misc_issues.append((contradiction_msg,))
                logger.debug(contradiction_msg, extra={'session_id': current_session_id})
                battery_contradiction_reported = True
    
    # Check individual table entries (for multi-entry tables)
    for idx, entry in enumerate(table_entries, start=1):
        if len(table_entries) <= 1:
            continue  # Skip if single entry (already checked above)
            
        # Combine shared values with entry-specific values
        combined_entry = {**shared_values, **entry}
        
        if is_battery_missing_in_components(combined_entry, 'table_'):
            # For multi-entry tables, only check table data (each entry represents different items)
            entry_battery_keys = find_battery_keys(combined_entry, 'table_')
            
            if entry_battery_keys:
                missing_text = combined_entry.get('table_missing_components_key', '')
                has_partial_inclusion = any(is_partial_battery_inclusion_text(value) for _, value in entry_battery_keys)
                if not (has_partial_inclusion or is_secondary_battery_missing_text(missing_text)):
                    contradictions_found += 1
                    entry_label = f"Entry {idx}"
                    battery_info = [f"{key.replace('table_', '').replace('_key', '')}: {value}" for key, value in entry_battery_keys]
                    
                    contradiction_msg = (
                        f"Battery contradiction in {entry_label}: Battery marked as missing component "
                        f"but battery field(s) have values ({'; '.join(battery_info)})"
                    )
                    misc_issues.append((contradiction_msg,))
                    logger.debug(contradiction_msg, extra={'session_id': current_session_id})
    
    if contradictions_found > 0:
        misc_info.append(f"  - Battery Missing Component Check: FAILED - {contradictions_found} contradictions found")
    else:
        misc_info.append(f"  - Battery Missing Component Check: PASSED - No battery contradictions detected")


def check_cracked_title_condition(title, specs, listing_data, misc_info, misc_issues):
    """Check that items with 'cracked' in title have 'For parts or not working' condition"""
    # Get title from original listing_data to ensure we have the full title
    title_text = listing_data['title'].get('title_title_key', '').lower()
    
    # Check condition in both specs and metadata, similar to other condition checks
    condition = ''
    condition_source = ''
    original_condition = ''
    
    # Check specs first
    if specs.get('condition_key', ''):
        condition = specs.get('condition_key', '').lower().strip()
        original_condition = specs.get('condition_key', '')
        condition_source = 'specs'
    # Check metadata if not found in specs
    elif listing_data.get('metadata', {}).get('meta_listing_condition_key', ''):
        condition = listing_data['metadata'].get('meta_listing_condition_key', '').lower().strip()
        original_condition = listing_data['metadata'].get('meta_listing_condition_key', '')
        condition_source = 'metadata'
    else:
        original_condition = 'Not specified'
    
    logger.debug(f"Checking cracked title condition: title_text='{title_text}', condition='{condition}' (from {condition_source})", extra={'session_id': current_session_id})
    
    if 'cracked' in title_text:
        if condition != 'for parts or not working':
            cracked_issue = f"Title contains 'cracked' but condition is '{original_condition}' instead of 'For parts or not working'"
            misc_issues.append((cracked_issue,))
            misc_info.append(f"  - Cracked Title Condition Check: FAILED - Condition mismatch")
            logger.debug(f"Cracked title condition mismatch: '{condition}' should be 'For parts or not working'", extra={'session_id': current_session_id})
        else:
            misc_info.append(f"  - Cracked Title Condition Check: PASSED - Correct condition for cracked item")
            logger.debug(f"Cracked title condition check passed", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - Cracked Title Condition Check: SKIPPED - No 'cracked' in title")
        logger.debug(f"No 'cracked' found in title: '{title_text}'", extra={'session_id': current_session_id})
     
def check_ebay_vs_store_category_consistency(meta, title, leaf_category, misc_info, misc_issues, issue_strings):
    """
    Direct comparison between eBay leaf category and store category/subcategory.
    This applies to all items regardless of mapping support since both values are human-chosen.
    """
    # Extract store category information
    store_category, sub_category = extract_store_category_info(meta, title)
    
    misc_info.append("  - eBay vs Store Category Check:")
    misc_info.append(f"    • eBay Leaf Category: {leaf_category if leaf_category else 'Not found'}")
    misc_info.append(f"    • Store Category: {store_category if store_category else 'Not found'}")
    misc_info.append(f"    • Store Subcategory: {sub_category if sub_category else 'Not found'}")
    
    # Skip check if we don't have the required information
    if not leaf_category or not store_category:
        missing_items = []
        if not leaf_category:
            missing_items.append("eBay leaf category")
        if not store_category:
            missing_items.append("store category")
        misc_info.append(f"    • Check Result: SKIPPED - Missing {', '.join(missing_items)}")
        return
    
    # NEW: Check for vintage exemption first
    if is_vintage_item(store_category, sub_category, leaf_category):
        misc_info.append(f"    • Check Result: PASS - Vintage exemption applied")
        logger.debug(f"eBay vs Store category check: vintage exemption applied", extra={'session_id': current_session_id})
        return
    
    # Normalize categories for comparison
    ebay_category_lower = leaf_category.lower().strip()
    store_category_lower = store_category.lower().strip()
    sub_category_lower = sub_category.lower().strip() if sub_category else ""
    
    # Load category mapping from file
    category_mapping = load_category_mapping()

    # Gray-area exemption: if this is a 2-in-1/Surface Pro style hybrid with leaf=laptops and device=tablets, skip
    device_type = title.get('device_type_key', '').strip()
    if is_gray_area_2in1_case(title, leaf_category, device_type):
        misc_info.append("    • Check Result: PASS (Gray-Area 2-in-1/Surface Pro)")
        logger.debug("eBay vs Store category consistency: gray-area exemption applied", extra={'session_id': current_session_id})
        return
    # Title-based exemption: base-only/no-screen laptops listed under Laptop Parts in store
    title_text_for_exemption = (
        title.get('title_title_key', '')
        or title.get('title_key', '')
        or title.get('key', '')
        or meta.get('meta_title_key', '')
    ).lower().strip()
    if (("laptop parts" in store_category_lower or "laptop parts" in sub_category_lower) and
        ("laptop" in ebay_category_lower) and
        ("base only" in title_text_for_exemption or "no screen" in title_text_for_exemption)):
        misc_info.append("    • Check Result: PASS - Title Exemption (Base-only/No-Screen laptop)")
        logger.debug("eBay vs Store category check: title-based laptop parts exemption applied", extra={'session_id': current_session_id})
        return

    # SPECIAL CASE: Store category 'Other' is a master wildcard. Always pass.
    if store_category_lower == 'other':
        misc_info.append("    • Check Result: PASS - 'Other' is a master wildcard; no mismatch")
        logger.debug("eBay vs Store category check: 'Other' unconditional pass", extra={'session_id': current_session_id})
        return
    
    # Check for direct matches first
    is_consistent = False
    match_reason = ""
    
    # 1. Direct category name match
    if store_category_lower in ebay_category_lower or ebay_category_lower in store_category_lower:
        is_consistent = True
        match_reason = "Direct store category name match"
    
    # 2. Subcategory match
    elif sub_category_lower and (sub_category_lower in ebay_category_lower or ebay_category_lower in sub_category_lower):
        is_consistent = True
        match_reason = "Store subcategory name match"
    
    # 3. Combined store category + subcategory match
    elif sub_category_lower:
        combined_store = f"{store_category_lower} {sub_category_lower}".strip()
        if combined_store in ebay_category_lower or ebay_category_lower in combined_store:
            is_consistent = True
            match_reason = "Combined store category + subcategory match"
    
    # 4. Check against category mapping file
    if not is_consistent and category_mapping:
        # Check if store category exists in mapping and if eBay category is in its allowed list
        for store_key, allowed_ebay_categories in category_mapping.items():
            if store_category_lower == store_key.lower():
                # Check if eBay category matches any allowed category
                for allowed_category in allowed_ebay_categories:
                    if (ebay_category_lower == allowed_category.lower() or
                        ebay_category_lower in allowed_category.lower() or
                        allowed_category.lower() in ebay_category_lower):
                        is_consistent = True
                        match_reason = f"Category mapping file match (store: {store_key})"
                        break
                if is_consistent:
                    break
        
        # Also check subcategory if no direct store category match
        if not is_consistent and sub_category_lower:
            for store_key, allowed_ebay_categories in category_mapping.items():
                if sub_category_lower == store_key.lower():
                    for allowed_category in allowed_ebay_categories:
                        if (ebay_category_lower == allowed_category.lower() or
                            ebay_category_lower in allowed_category.lower() or
                            allowed_category.lower() in ebay_category_lower):
                            is_consistent = True
                            match_reason = f"Category mapping file match (subcategory: {store_key})"
                            break
                    if is_consistent:
                        break
    
    # 5. Partial word matching for similar categories
    if not is_consistent:
        ebay_words = set(ebay_category_lower.replace('&', ' ').replace('-', ' ').split())
        store_words = set(store_category_lower.replace('&', ' ').replace('-', ' ').split())
        sub_words = set(sub_category_lower.replace('&', ' ').replace('-', ' ').split()) if sub_category_lower else set()
        
        # Remove common words that don't help with categorization
        common_words = {'and', 'the', 'a', 'an', 'of', 'for', 'in', 'on', 'with', 'by'}
        ebay_words -= common_words
        store_words -= common_words
        sub_words -= common_words
        
        # Check for significant word overlap
        store_all_words = store_words | sub_words
        overlap = ebay_words & store_all_words
        
        if overlap and len(overlap) >= 1:  # At least one significant word match
            # Check if it's a meaningful overlap (not just generic words)
            meaningful_words = overlap - {'computers', 'equipment', 'devices', 'parts', 'accessories'}
            if meaningful_words:
                is_consistent = True
                match_reason = f"Significant word overlap: {', '.join(meaningful_words)}"
    
    # Report results
    if is_consistent:
        misc_info.append(f"    • Check Result: PASS - {match_reason}")
        logger.debug(f"eBay vs Store category check passed: {match_reason}", extra={'session_id': current_session_id})
    else:
        # If the leaf category is not present in mapping anywhere, treat as unmapped: log only, don't create an issue
        if not is_leaf_category_in_mapping(leaf_category, category_mapping or {}):
            log_unmapped_leaf_category(meta, store_category, sub_category, leaf_category)
            misc_info.append("    • Check Result: SKIPPED - Unmapped eBay category (logged)")
            logger.debug("eBay vs Store category: unmapped category logged; issue suppressed", extra={'session_id': current_session_id})
        else:
            misc_info.append(f"    • Check Result: FAIL - No logical connection found")
            
            # Create detailed mismatch message
            if sub_category:
                mismatch_msg = f"eBay vs Store category mismatch: eBay category '{leaf_category}' does not logically match store category '{store_category}' with subcategory '{sub_category}'"
            else:
                mismatch_msg = f"eBay vs Store category mismatch: eBay category '{leaf_category}' does not logically match store category '{store_category}'"
            
            misc_issues.append((mismatch_msg,))
            issue_strings.append(mismatch_msg)
            logger.debug(f"eBay vs Store category mismatch detected", extra={'session_id': current_session_id})

def check_category_validation_consolidated(meta, title, leaf_category, misc_info, misc_issues, issue_strings):
    """
    Consolidated category validation that combines enhanced mapping and direct eBay vs Store checks.
    Provides detailed info but avoids redundant error messages.
    """
    # Track issues from both validation methods
    enhanced_issues = []
    direct_issues = []
    enhanced_info = []
    direct_info = []
    
    # Early exit: if store category is 'Other', suppress any consolidated mismatch
    sc_for_guard, ssc_for_guard = extract_store_category_info(meta, title)
    if (sc_for_guard or '').strip().lower() == 'other' or (ssc_for_guard or '').strip().lower() == 'other':
        misc_info.append("  - Category Validation: PASS ('Other' store category is a master wildcard)")
        logger.debug("Consolidated category validation: 'Other' unconditional pass", extra={'session_id': current_session_id})
        return

    # Gray-area early exit: if hybrid 2-in-1 detected, suppress all category mismatch consolidation
    if is_gray_area_2in1_case(title, leaf_category, title.get('device_type_key', '')):
        misc_info.append("  - Category Validation: PASS (Gray-Area 2-in-1/Surface Pro)")
        return
    # Title-based exemption: base-only/no-screen laptops listed under Laptop Parts in store
    tbe_text = (
        title.get('title_title_key', '')
        or title.get('title_key', '')
        or title.get('key', '')
        or meta.get('meta_title_key', '')
    ).lower().strip()
    sc_exempt, ssc_exempt = extract_store_category_info(meta, title)
    sc_lower = (sc_exempt or '').lower()
    ssc_lower = (ssc_exempt or '').lower()
    leaf_lower_for_consolidated = (leaf_category or '').lower()
    if (("laptop parts" in sc_lower or "laptop parts" in ssc_lower) and
        ("laptop" in leaf_lower_for_consolidated) and
        ("base only" in tbe_text or "no screen" in tbe_text)):
        misc_info.append("  - Category Validation: PASS (Title Exemption: Base-only/No-Screen laptop)")
        return

    # Run enhanced validation (captures issues separately)
    temp_misc_issues = []
    temp_issue_strings = []
    temp_misc_info = []
    
    check_enhanced_category_validation(meta, title, leaf_category, temp_misc_info, temp_misc_issues, temp_issue_strings)
    enhanced_issues.extend(temp_issue_strings)
    enhanced_info.extend(temp_misc_info)
    
    # Run direct eBay vs Store validation (captures issues separately)
    temp_misc_issues = []
    temp_issue_strings = []
    temp_misc_info = []
    
    check_ebay_vs_store_category_consistency(meta, title, leaf_category, temp_misc_info, temp_misc_issues, temp_issue_strings)
    direct_issues.extend(temp_issue_strings)
    direct_info.extend(temp_misc_info)
    
    # Add all detailed info to misc_info (this provides the detailed breakdown)
    misc_info.extend(enhanced_info)
    misc_info.extend(direct_info)
    
    # Consolidate issues into a single comprehensive message
    all_issues = enhanced_issues + direct_issues
    if all_issues:
        # Extract store category and subcategory info
        store_category, sub_category = extract_store_category_info(meta, title)
        
        # Final safeguard: suppress consolidated mismatch for gray-area 2-in-1/Surface Pro
        if (
            (leaf_category or '').lower().find('pc laptops & netbooks') != -1 and
            (title.get('device_type_key','') or '').lower().find('tablets & ebook readers') != -1
        ) or is_gray_area_2in1_case(title, leaf_category, title.get('device_type_key','')):
            misc_info.append("  - Category Validation: PASS (Gray-Area 2-in-1/Surface Pro)")
            logger.debug("Consolidated category validation suppressed by gray-area safeguard", extra={'session_id': current_session_id})
            return

        # Create a comprehensive consolidated message
        if sub_category:
            consolidated_message = (
                f"Category mismatch: eBay category '{leaf_category}' does not match "
                f"store category '{store_category}' with subcategory '{sub_category}'"
            )
        else:
            consolidated_message = (
                f"Category mismatch: eBay category '{leaf_category}' does not match "
                f"store category '{store_category}'"
            )
        
        # Add suggested store categories using mapping
        mapping_for_suggestions = load_category_mapping()
        suggestions_for_consolidated = suggest_store_categories_for_leaf(leaf_category, mapping_for_suggestions or {})
        if suggestions_for_consolidated:
            consolidated_message += f". Suggested store category: {', '.join(suggestions_for_consolidated)}"

        # Add specific validation details if available
        apple_validation = validate_apple_category_specific(store_category, sub_category, leaf_category)
        if apple_validation['is_apple_category'] and apple_validation['missing_requirements']:
            consolidated_message += f" - {'; '.join(apple_validation['missing_requirements'])}"
        
        # Add to issues (single consolidated message instead of multiple)
        misc_issues.append((consolidated_message,))
        issue_strings.append(consolidated_message)
        
        logger.debug(f"Consolidated category validation result: {consolidated_message}", extra={'session_id': current_session_id})
    else:
        logger.debug("Category validation passed - no issues found", extra={'session_id': current_session_id})

def check_scheduled_listing_validation(meta, misc_info, misc_issues):
    """Scheduled Listing Date/Time Validation with Manual PDT to EST Conversion"""
    scheduled_date = meta.get('listing_scheduled_date_key', '').strip()
    scheduled_time = meta.get('listing_scheduled_time_key', '').strip()
    scheduled_timezone = meta.get('listing_scheduled_timezone_key', '').strip()
    
    # Skip validation if any value is "unknown" or empty
    if (not scheduled_date or scheduled_date.lower() == 'unknown' or 
        not scheduled_time or scheduled_time.lower() == 'unknown' or
        not scheduled_timezone or scheduled_timezone.lower() == 'unknown'):
        misc_info.append("  - Scheduled Listing Validation: SKIPPED - Date, time, or timezone is unknown")
        return
    
    try:
        from datetime import datetime
        
        # Parse the date (expecting "M/D/YYYY DayName" format)
        date_part = scheduled_date.split()[0] if scheduled_date else ''
        date_parts = date_part.split('/')
        if len(date_parts) != 3:
            misc_issues.append((f"Invalid scheduled date format: {scheduled_date}. Expected M/D/YYYY DayName format.",))
            misc_info.append("  - Scheduled Listing Validation: FAILED - Invalid date format")
            return
        
        month, day, year = map(int, date_parts)
        
        # Parse the time (expecting "HH:MM AM/PM" format)
        time_match = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)', scheduled_time.upper())
        if not time_match:
            misc_issues.append((f"Invalid scheduled time format: {scheduled_time}. Expected HH:MM AM/PM format.",))
            misc_info.append("  - Scheduled Listing Validation: FAILED - Invalid time format")
            return
        
        hour_12, minute, ampm = time_match.groups()
        hour_12 = int(hour_12)
        minute = int(minute)
        
        # Convert to 24-hour format
        if ampm == 'AM':
            if hour_12 == 12:
                hour_24 = 0  # 12:XX AM = 00:XX
            else:
                hour_24 = hour_12  # 1:XX AM - 11:XX AM = 01:XX - 11:XX
        else:  # PM
            if hour_12 == 12:
                hour_24 = 12  # 12:XX PM = 12:XX
            else:
                hour_24 = hour_12 + 12  # 1:XX PM - 11:XX PM = 13:XX - 23:XX
        
        if hour_24 < 0 or hour_24 > 23 or minute < 0 or minute > 59:
            misc_issues.append((f"Invalid scheduled time values: {scheduled_time}.",))
            misc_info.append("  - Scheduled Listing Validation: FAILED - Invalid time values")
            return
        
        # Create datetime object
        source_dt = datetime(year, month, day, hour_24, minute)
        
        # Only support PDT/PST for now since that's what's always used
        if scheduled_timezone.upper() not in ['PDT', 'PST']:
            misc_issues.append((f"Unsupported timezone: {scheduled_timezone}. Only PDT and PST are supported.",))
            misc_info.append("  - Scheduled Listing Validation: FAILED - Unsupported timezone")
            return
        
        # Manual timezone conversion: PDT is UTC-7, EST is UTC-5, so PDT is 3 hours behind EST
        # Convert PDT time to EST by adding 3 hours
        pdt_hour = hour_24
        pdt_minute = minute
        
        est_hour = pdt_hour + 3
        est_minute = pdt_minute
        
        # Handle day rollover if needed
        if est_hour >= 24:
            est_hour -= 24
            # Day would advance, but for our validation purposes we just need the time
        
        # Check day of week (Monday=0, Sunday=6) - use original date for PDT
        day_of_week = source_dt.weekday()
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        validation_issues = []
        
        # Check if it's Friday, Saturday, or Sunday (4, 5, 6)
        if day_of_week >= 4:  # Friday, Saturday, Sunday
            validation_issues.append(f"Scheduled day ({day_names[day_of_week]}) is not allowed. Valid days are Monday-Thursday.")
        
        # Check if EST time is between 5:00 PM (17:00) and 12:00 AM (00:00) Eastern Time
        # Valid hours: 17-23 (5PM-11PM) and 0 (12AM/midnight)
        if not ((est_hour >= 17 and est_hour <= 23) or est_hour == 0):
            # Format the EST time for display
            est_ampm = "AM" if est_hour < 12 else "PM"
            est_display_hour = est_hour if est_hour <= 12 else est_hour - 12
            if est_display_hour == 0:
                est_display_hour = 12
            est_time_str = f"{est_display_hour}:{est_minute:02d} {est_ampm} EST"
            
            validation_issues.append(f"Scheduled time {scheduled_time} is outside allowed hours. Should be 2:00 PM to 9:00 PM PDT (5PM-12AM EST).")
        
        if validation_issues:
            for issue in validation_issues:
                misc_issues.append((issue,))
            misc_info.append(f"  - Scheduled Listing Validation: FAILED - {len(validation_issues)} issue(s) found")
            logger.debug(f"Scheduled listing validation failed: {validation_issues}", extra={'session_id': current_session_id})
        else:
            # Format the EST time for success message
            est_ampm = "AM" if est_hour < 12 else "PM"
            est_display_hour = est_hour if est_hour <= 12 else est_hour - 12
            if est_display_hour == 0:
                est_display_hour = 12
            est_time_str = f"{est_display_hour}:{est_minute:02d} {est_ampm} EST"
            
            misc_info.append(f"  - Scheduled Listing Validation: PASSED - {day_names[day_of_week]} at {scheduled_time} {scheduled_timezone} ({est_time_str}) is valid")
            logger.debug(f"Scheduled listing validation passed: {day_names[day_of_week]} at {scheduled_time} {scheduled_timezone} = {est_time_str}", extra={'session_id': current_session_id})
            
    except ValueError as e:
        misc_issues.append((f"Error parsing scheduled date/time: {scheduled_date} {scheduled_time} {scheduled_timezone} - {str(e)}",))
        misc_info.append("  - Scheduled Listing Validation: FAILED - Parse error")
        logger.debug(f"Error parsing scheduled date/time: {str(e)}", extra={'session_id': current_session_id})
    except Exception as e:
        misc_issues.append((f"Unexpected error validating scheduled listing: {str(e)}",))
        misc_info.append("  - Scheduled Listing Validation: FAILED - Unexpected error")
        logger.error(f"Unexpected error in scheduled listing validation: {str(e)}", extra={'session_id': current_session_id})
        
def check_server_memory_title_calculation(title, misc_info, misc_issues):
    """Server Memory Title Calculation Verification"""
    device_type = title.get('device_type_key', '').strip()
    
    if device_type == 'Server Memory (RAM)':
        # Try multiple possible keys for the title text
        title_text = title.get('key', '') or title.get('title_key', '') or title.get('title_title_key', '')
        logger.debug(f"Checking server memory calculation in title: '{title_text}'", extra={'session_id': current_session_id})
        
        if not title_text:
            misc_info.append(f"  - Server Memory Title Calculation: SKIPPED - No title text found")
            logger.debug(f"No title text found in title data: {list(title.keys())}", extra={'session_id': current_session_id})
            return
        
        # First, let's test if basic regex is working
        simple_test = re.search(r'960GB', title_text)
        logger.debug(f"Simple test for '960GB': {'Found' if simple_test else 'Not found'}", extra={'session_id': current_session_id})
        
        # Test for the parentheses part
        paren_test = re.search(r'\(60 x 16GB\)', title_text)
        logger.debug(f"Parentheses test for '(60 x 16GB)': {'Found' if paren_test else 'Not found'}", extra={'session_id': current_session_id})
        
        # Updated pattern to handle commas in numbers
        simple_pattern = r'([\d,]+)GB \((\d+) x (\d+)GB\)'
        simple_match = re.search(simple_pattern, title_text)
        logger.debug(f"Simple pattern '{simple_pattern}': {'Found' if simple_match else 'Not found'}", extra={'session_id': current_session_id})
        
        if simple_match:
            # Remove commas before converting to int
            total_size = int(simple_match.group(1).replace(',', ''))
            quantity = int(simple_match.group(2))
            module_size = int(simple_match.group(3))
            
            logger.debug(f"Simple pattern matched: {total_size}GB = {quantity} x {module_size}GB", extra={'session_id': current_session_id})
            
            calculated_total = quantity * module_size
            
            if calculated_total != total_size:
                mismatch_msg = (
                    f"Server Memory calculation error: {quantity} x {module_size}GB = "
                    f"{calculated_total}GB, but title shows {total_size}GB"
                )
                misc_issues.append((mismatch_msg,))
                misc_info.append(f"  - Server Memory Title Calculation: FAILED - Math error")
                logger.debug(mismatch_msg, extra={'session_id': current_session_id})
            else:
                misc_info.append(f"  - Server Memory Title Calculation: PASSED - {quantity} x {module_size}GB = {total_size}GB")
                logger.debug(f"Server memory calculation verified: {quantity} x {module_size}GB = {total_size}GB", extra={'session_id': current_session_id})
            return
        
        # If simple pattern didn't work, try more flexible patterns with comma and decimal support
        patterns = [
            r'([\d,]+(?:\.\d+)?)\s*(GB|MB|TB)\s*\(\s*(\d+)\s*x\s*(\d+)\s*(GB|MB|TB)\s*\)',
            r'([\d,]+(?:\.\d+)?)(GB|MB|TB) \((\d+) x (\d+)(GB|MB|TB)\)',
            r'([\d,]+(?:\.\d+)?)(GB|MB|TB)\s*\(\s*(\d+)\s*x\s*(\d+)(GB|MB|TB)\s*\)'
        ]
        
        match_found = False
        for i, pattern in enumerate(patterns):
            logger.debug(f"Trying pattern {i+1}: '{pattern}'", extra={'session_id': current_session_id})
            match = re.search(pattern, title_text, re.IGNORECASE)
            if match:
                logger.debug(f"Pattern {i+1} matched! Groups: {match.groups()}", extra={'session_id': current_session_id})
                match_found = True
                
                # Remove commas before converting; allow decimals
                total_size_raw = match.group(1).replace(',', '')
                total_size_num = float(total_size_raw)
                total_unit = match.group(2).upper()
                quantity = int(match.group(3))
                module_size = int(match.group(4))
                module_unit = match.group(5).upper()
                
                # Convert to common unit (GB) for calculation
                unit_multipliers = {'MB': 0.001, 'GB': 1.0, 'TB': 1000.0}
                
                total_size_gb = total_size_num * unit_multipliers.get(total_unit, 1.0)
                module_size_gb = module_size * unit_multipliers.get(module_unit, 1.0)
                calculated_total_gb = quantity * module_size_gb
                
                # Apply tolerant comparison when title uses TB (decimal or integer): accept GB within [TB*1000, TB*1024]
                passes = False
                if total_unit == 'TB':
                    min_expected = (total_size_gb / 1024.0) * 1000.0  # TB decimal convention
                    max_expected = (total_size_gb)                   # TB binary convention already in GB via *1024 implicit
                    # total_size_gb computed with 1000 multiplier; compute binary upper bound explicitly
                    max_expected = float(total_size_num) * 1024.0
                    # small epsilon
                    passes = (min_expected - 0.5) <= calculated_total_gb <= (max_expected + 0.5)
                else:
                    # For GB/MB totals, require near-equality with small epsilon
                    passes = abs(calculated_total_gb - total_size_gb) <= 0.5

                if not passes:
                    mismatch_msg = (
                        f"Server Memory calculation error: {quantity} x {module_size}{module_unit} = "
                        f"{calculated_total_gb}GB, but title shows {total_size_raw}{total_unit}"
                    )
                    misc_issues.append((mismatch_msg,))
                    misc_info.append(f"  - Server Memory Title Calculation: FAILED - Math error")
                    logger.debug(mismatch_msg, extra={'session_id': current_session_id})
                else:
                    misc_info.append(f"  - Server Memory Title Calculation: PASSED - {quantity} x {module_size}{module_unit} = {total_size_raw}{total_unit}")
                    logger.debug(f"Server memory calculation verified: {quantity} x {module_size}{module_unit} = {total_size_raw}{total_unit}", extra={'session_id': current_session_id})
                break
            else:
                logger.debug(f"Pattern {i+1} did not match", extra={'session_id': current_session_id})
        
        if not match_found:
            misc_info.append(f"  - Server Memory Title Calculation: SKIPPED - No calculation pattern found in title")
            logger.debug(f"No server memory calculation pattern found in title: '{title_text}'", extra={'session_id': current_session_id})
    else:
        misc_info.append("  - Server Memory Title Calculation: SKIPPED - Not Server Memory (RAM) device type")
        
def load_package_validation_rules():
    """Load package validation rules from JSON file."""
    rules_file = "package_validation_rules.json"
    
    try:
        if os.path.exists(rules_file):
            with open(rules_file, 'r', encoding='utf-8') as f:
                rules = json.load(f)
                if logger:
                    logger.debug(f"Loaded package validation rules from {rules_file}", extra={'session_id': current_session_id})
                return rules
        else:
            if logger:
                logger.warning(f"Package validation rules file not found: {rules_file}", extra={'session_id': current_session_id})
            return {"device_types": {}}
    except Exception as e:
        if logger:
            logger.error(f"Error loading package validation rules: {str(e)}", extra={'session_id': current_session_id})
        return {"device_types": {}}

def parse_package_weight(weight_str):
    """Parse package weight string to get numeric value in pounds."""
    if not weight_str:
        return None
    
    # Extract number and unit
    match = re.search(r'(\d+(?:\.\d+)?)\s*(lbs?|pounds?|kg|grams?|g|oz|ounces?)', weight_str.lower())
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2)
    
    # Convert to pounds
    if unit in ['kg']:
        return value * 2.20462
    elif unit in ['g', 'grams', 'gram']:
        return value * 0.00220462
    elif unit in ['oz', 'ounces', 'ounce']:
        return value * 0.0625
    elif unit in ['lbs', 'lb', 'pounds', 'pound']:
        return value
    
    return value  # Assume pounds if unknown unit

def parse_package_dimensions(dimensions_str):
    """Parse package dimensions string to get length, width, height in inches."""
    if not dimensions_str:
        return None, None, None
    
    # Try to extract three numbers (length x width x height)
    # Common formats: "16 x 14 x 4 in", "16x14x4", "16 by 14 by 4 inches"
    pattern = r'(\d+(?:\.\d+)?)\s*[x×by]\s*(\d+(?:\.\d+)?)\s*[x×by]\s*(\d+(?:\.\d+)?)'
    match = re.search(pattern, dimensions_str.lower())
    
    if not match:
        return None, None, None
    
    length = float(match.group(1))
    width = float(match.group(2))
    height = float(match.group(3))
    
    # Check if units are in cm and convert to inches
    if 'cm' in dimensions_str.lower():
        length *= 0.393701
        width *= 0.393701
        height *= 0.393701
    
    return length, width, height

def get_screen_size_range(screen_size_str):
    """Determine which screen size range a screen size falls into."""
    if not screen_size_str:
        return None
    
    # Extract numeric screen size
    match = re.search(r'(\d+(?:\.\d+)?)', screen_size_str)
    if not match:
        return None
    
    size = float(match.group(1))
    
    # Define ranges
    if 7 <= size <= 10:
        return "7-10"
    elif 10 < size <= 12:
        return "10-12"
    elif 12 < size <= 15:
        return "13-15"
    elif 15 < size <= 17:
        return "16-17"
    elif 17 < size <= 20:
        return "18-20"
    
    return None

def get_lot_count(title, meta, listing_data):
    """Extract lot count from various sources."""
    # Check table entry count first
    table_shared = listing_data.get('table_shared', {})
    if 'table_entry_count_key' in table_shared:
        entry_count_str = table_shared['table_entry_count_key']
        count_match = re.search(r'Total Entries:\s*(\d+)', entry_count_str)
        if count_match:
            return int(count_match.group(1))
        try:
            return int(entry_count_str)
        except ValueError:
            pass
    
    # Check actual table entries
    table_entries = len(listing_data.get('table_data', []))
    if table_entries > 1:
        return table_entries
    
    # Check title lot information
    lot_str = title.get('lot_key', '')
    if lot_str:
        lot_match = re.search(r'\d+', lot_str)
        if lot_match:
            return int(lot_match.group())
    
    # Check metadata listing info – only count lots when explicitly stated
    listing_info = meta.get('listinginfo_key', '')
    if listing_info and listing_info.lower() != "single item":
        li_lower = listing_info.lower()
        
        # Explicit "per lot" phrasing – e.g., "5 items per lot" or "5 per lot"
        per_lot_match = re.search(r'(\d+)\s*(?:items?\s*)?per\s*lot', li_lower)
        if per_lot_match:
            return int(per_lot_match.group(1))
        
        # "Lot of X" phrasing – e.g., "Lot of 3 items"
        lot_of_match = re.search(r'lot\s+of\s+(\d+)', li_lower)
        if lot_of_match:
            return int(lot_of_match.group(1))
    
    return 1  # Default to single item

def check_package_validation(title, meta, listing_data, sections, misc_info, misc_issues):
    """
    Validate package weight, dimensions **and price**.
    • Category rules - OR - model–override rules (if text match found in title)
    """

    # ------------------------------------------------------------------ #
    # 1.  Parse package & listing basics
    # ------------------------------------------------------------------ #
    pkg_weight_str   = meta.get('listing_package_weight_key', '')
    pkg_dim_str      = meta.get('listing_package_dimensions_key', '')

    if not pkg_weight_str or pkg_weight_str == 'Unknown':
        misc_issues.append(("Package weight is missing or marked as Unknown",))
    if not pkg_dim_str or pkg_dim_str == 'Unknown':
        misc_issues.append(("Package dimensions are missing or marked as Unknown",))
    if not pkg_weight_str or not pkg_dim_str or \
        pkg_weight_str == 'Unknown' or pkg_dim_str == 'Unknown':
        misc_info.append("  - Package Validation: FAILED - Missing package information")
        return

    leaf_category = None
    for line in sections.get('CATEGORY', []):
        if '[leaf_category_key]' in line:
            parts = line.split(': ', 1)
            if len(parts) == 2:
                leaf_category = parts[1].strip()
            break
    if not leaf_category:
        misc_info.append("  - Package Validation: SKIPPED - No leaf category found")
        return

    lot_count = get_lot_count(title, meta, listing_data)

    # ------------------------------------------------------------------ #
    # 2.  Load rules & detect a model-specific override (e.g. "Dell 3630")
    # ------------------------------------------------------------------ #
    rules          = load_package_validation_rules()
    override_rule  = find_model_override_rule(title, rules)
    device_label   = (override_rule.get('display_name',
                                        override_rule.get('match_text', leaf_category))
                      if override_rule else leaf_category)

    # ------------------------------------------------------------------ #
    # 3.  Weight-UNIT sanity (oz / g) – uses original category name
    # ------------------------------------------------------------------ #
    weight_unit_issues = []
    if pkg_weight_str:
        units_conf = rules.get('weight_units', {})
        allowed    = units_conf.get('allowed_for_device_types', {}).get(leaf_category, [])

        if re.search(r'\b\d+(?:\.\d+)?\s*oz\b', pkg_weight_str.lower()):
            if not any(u in ['oz', 'ounces', 'ounce'] for u in allowed):
                weight_unit_issues.append(
                    f"Package weight in ounces ({pkg_weight_str}) is suspiciously light for "
                    f"{device_label} – should be in pounds")
        elif re.search(r'\b\d+(?:\.\d+)?\s*g\b', pkg_weight_str.lower()):
            if not any(u in ['g', 'grams', 'gram'] for u in allowed):
                weight_unit_issues.append(
                    f"Package weight in grams ({pkg_weight_str}) is suspiciously light for "
                    f"{device_label} – should be in pounds")

    # ------------------------------------------------------------------ #
    # 4.  PRICE VALIDATION  (auction & BIN, incl. model-level overrides)
    # ------------------------------------------------------------------ #
    price_issues = []
    base_price_cfg = rules.get('price_validation', {})
    price_cfg = dict(base_price_cfg)            # shallow copy
    if override_rule and override_rule.get('price_validation'):
        price_cfg.update(override_rule['price_validation'])   # model overrides category/global

    if price_cfg.get('enabled', False):
        price_str    = meta.get('listing_price_key', '')
        listing_type = meta.get('listing_type_key', '').lower()
        if price_str and listing_type:
            price_val = float(re.sub(r'[^\d.]', '', price_str))
            flags     = price_cfg.get('global_flags', {})
            if price_val < flags.get('suspicious_low_price', 0.99):
                price_issues.append(f"Price ${price_val:.2f} is suspiciously low – may be an error")
            elif price_val > flags.get('suspicious_high_price', 10000.00):
                price_issues.append(f"Price ${price_val:.2f} is suspiciously high – may be an error")

            if listing_type == 'auction':
                auc = price_cfg.get('auction_prices', {})
                if not (auc.get('min_starting_price', 0) <= price_val <= auc.get('max_starting_price', 9e9)):
                    price_issues.append(
                        f"Auction start ${price_val:.2f} outside "
                        f"${auc.get('min_starting_price',0):.2f}-${auc.get('max_starting_price',0):.2f} "
                        f"for {device_label}")
            elif listing_type == 'buyitnow':
                bin_cfg  = price_cfg.get('buyitnow_prices', {})
                lot_tbl  = (bin_cfg if 'single' in bin_cfg else
                            bin_cfg.get(device_label) or bin_cfg.get(leaf_category) or {})
                lot_band = ('single' if lot_count == 1 else
                            'small_lot' if lot_count <= 10 else 'large_lot')
                band_cfg = lot_tbl.get(lot_band, {})
                if band_cfg:
                    if not (band_cfg.get('min', 0) <= price_val <= band_cfg.get('max', 9e9)):
                        price_issues.append(
                            f"Buy-It-Now ${price_val:.2f} outside "
                            f"${band_cfg.get('min',0):.2f}-${band_cfg.get('max',0):.2f} "
                            f"({lot_band.replace('_',' ')} – {device_label})")

    # ------------------------------------------------------------------ #
    # 5.  Parse weight & dimensions  +  friendly weight display
    # ------------------------------------------------------------------ #
    weight_lbs            = parse_package_weight(pkg_weight_str)
    length, width, height = parse_package_dimensions(pkg_dim_str)

    # Friendly display string
    weight_disp = pkg_weight_str.strip()
    if weight_lbs is not None:
        if pkg_weight_str and 'oz' in pkg_weight_str.lower():
            weight_disp = pkg_weight_str.strip()
        elif weight_lbs < 1:                           # sub-pound → whole ounces
            weight_disp = f"{round(weight_lbs * 16):.0f} oz"
        else:
            weight_disp = f"{weight_lbs:.1f} lbs"

    if weight_lbs is None and all(v is None for v in (length, width, height)):
        misc_info.append("  - Package Validation: SKIPPED - Could not parse package information")
        return

    # ------------------------------------------------------------------ #
    # 6.  Choose rule-set (category OR model override)
    # ------------------------------------------------------------------ #
    cat_rules  = rules.get('device_types', {}).get(leaf_category) or {}
    applicable = None
    rule_type  = None
    if override_rule:
        applicable = override_rule
        rule_type  = 'model_override'
    else:
        lot_key    = ('single' if lot_count == 1 else
                      'small_lot' if lot_count <= cat_rules.get('small_lot', {}).get('max_lot_size', 10)
                      else 'large_lot')
        applicable = cat_rules.get(lot_key) or cat_rules.get('default')
        rule_type  = lot_key if applicable else None

    if not applicable:
        misc_info.append(f"  - Package Validation: SKIPPED - No applicable rules for {lot_count} {device_label}(s)")
        return
    if not rules.get('validation_settings', {}).get('package_validation_enabled', True):
        misc_info.append("  - Package Validation: SKIPPED - Disabled via validation settings")
        return

    # ------------------------------------------------------------------ #
    # 7.  VALIDATE  (weight → dimensions → typical box) + collect issues
    # ------------------------------------------------------------------ #
    issues = weight_unit_issues + price_issues

    # ---- WEIGHT --------------------------------------------------------
    if weight_lbs is not None and 'weight' in applicable:
        w = applicable['weight']
        w_min = w.get('min', 0)
        w_max = w.get('max', 9e9)
        if not (w_min <= weight_lbs <= w_max):
            issues.append(
                f"Package weight {weight_disp} is outside expected range for {device_label}: "
                f"{w_min}-{w_max} lbs")

    # ---- PER-ITEM WEIGHT for large lots --------------------------------
    if weight_lbs is not None and lot_count > 1 and 'weight_per_item' in applicable:
        per  = applicable['weight_per_item']
        base = applicable.get('base_packaging_weight', 0)
        min_tot = per['min'] * lot_count + base
        max_tot = per['max'] * lot_count + base
        # NEW: helper formatting functions for clearer weight display
        def _format_weight(val):
            if val < 1:
                ounces = val * 16
                return f"{ounces:.1f} oz" if ounces % 1 else f"{int(ounces)} oz"
            return f"{val:.1f} lbs" if val % 1 else f"{int(val)} lbs"
        def _format_weight_range(min_w, max_w):
            return f"{_format_weight(min_w)}-{_format_weight(max_w)}"
        item_range_disp = _format_weight_range(per['min'], per['max'])
        packaging_part = ""
        if base > 0:
            packaging_part = f" + {_format_weight(base)} box"
        paren_disp = f"({item_range_disp}/item{packaging_part})"
        if not (min_tot <= weight_lbs <= max_tot):
            issues.append(
                f"Package weight {weight_disp} is outside expected range for {lot_count} "
                f"{device_label}(s): {min_tot:.1f}-{max_tot:.1f} lbs {paren_disp}")

    # ---- DIMENSIONS ----------------------------------------------------
    if 'dimensions' in applicable:
        dcfg = applicable['dimensions']
        dims_available = all(v is not None for v in (length, width, height))

        if not dims_available:
            if rule_type == 'model_override':
                exp_desc = dcfg.get('description')
                if not exp_desc:
                    if 'exact' in dcfg and isinstance(dcfg['exact'], list) and len(dcfg['exact']) == 3:
                        exp_desc = f"{dcfg['exact'][0]} x {dcfg['exact'][1]} x {dcfg['exact'][2]} in"
                    elif 'max' in dcfg and isinstance(dcfg['max'], list) and len(dcfg['max']) == 3:
                        exp_desc = f"<= {dcfg['max'][0]} x {dcfg['max'][1]} x {dcfg['max'][2]} in"
                    elif 'min' in dcfg and isinstance(dcfg['min'], list) and len(dcfg['min']) == 3:
                        exp_desc = f">= {dcfg['min'][0]} x {dcfg['min'][1]} x {dcfg['min'][2]} in"
                    elif 'exact_options' in dcfg:
                        _opts = [o for o in (dcfg.get('exact_options') or []) if isinstance(o, (list, tuple)) and len(o) == 3]
                        if _opts:
                            exp_desc = ' or '.join(f"{o[0]} x {o[1]} x {o[2]} in" for o in _opts)
                        else:
                            exp_desc = dcfg.get('description') or ""
                issues.append(
                    f"Package dimensions Unknown / not provided but required for {device_label}: {exp_desc}")
        else:
            dims = sorted((length, width, height), reverse=True)

            # Exact match takes precedence
            if 'exact' in dcfg and isinstance(dcfg['exact'], list) and len(dcfg['exact']) == 3:
                if dims != sorted(dcfg['exact'], reverse=True):
                    exp = dcfg['exact']
                    issues.append(
                        f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in do not match expected exact "
                        f"size for {device_label}: {exp[0]} x {exp[1]} x {exp[2]} in")
            # Exact options (only if options are configured)
            elif 'exact_options' in dcfg:
                _opts = [o for o in (dcfg.get('exact_options') or []) if isinstance(o, (list, tuple)) and len(o) == 3]
                if _opts:
                    opts_ok = any(dims == sorted(o, reverse=True) for o in _opts)
                    if not opts_ok:
                        opts = ' or '.join(f"{o[0]} x {o[1]} x {o[2]} in" for o in _opts)
                        issues.append(
                            f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in do not match expected sizes "
                            f"for {device_label}: {opts}")
                # If no options configured, do not enforce
            else:
                # Range/threshold checks
                if 'max' in dcfg and isinstance(dcfg['max'], list) and len(dcfg['max']) == 3:
                    if any(p > m for p, m in zip(dims, sorted(dcfg['max'], reverse=True))):
                        issues.append(
                            f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in exceed maximum allowed "
                            f"for {device_label}: {dcfg['max'][0]} x {dcfg['max'][1]} x {dcfg['max'][2]} in")
                if 'min' in dcfg and isinstance(dcfg['min'], list) and len(dcfg['min']) == 3:
                    if any(p < m for p, m in zip(dims, sorted(dcfg['min'], reverse=True))):
                        issues.append(
                            f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in are below minimum allowed "
                            f"for {device_label}: {dcfg['min'][0]} x {dcfg['min'][1]} x {dcfg['min'][2]} in")
                elif all(k in dcfg for k in ('length', 'width', 'height')):
                    # Range-based validation: each axis may specify min and/or max.
                    axis_configs = [dcfg['length'], dcfg['width'], dcfg['height']]
                    for dim_val, axis_cfg, axis_label in zip(dims, axis_configs, ['L', 'W', 'H']):
                        a_min = axis_cfg.get('min', 0)
                        a_max = axis_cfg.get('max', 9e9)
                        if not (a_min <= dim_val <= a_max):
                            issues.append(
                                f"Package {axis_label}-axis {dim_val:.1f} in outside allowed range {a_min}-{a_max} in "
                                f"for {device_label}")
                            break

    # ---- TYPICAL BOX match (optional) ----------------------------------
    if applicable.get('use_typical_box_sizes') and all(v is not None for v in (length, width, height)):
        boxes = load_typical_box_sizes()
        if boxes and not dimensions_match_typical(length, width, height, boxes)[0]:
            issues.append(
                f"Package dimensions {length:.1f} × {width:.1f} × {height:.1f} in do not match any typical box size")

    # ------------------------------------------------------------------ #
    # 8.  REPORT
    # ------------------------------------------------------------------ #
    if issues:
        for i in issues:
            misc_issues.append((i,))
        misc_info.append(f"  - Package Validation: FAILED – {len(issues)} issue(s) found")
    else:
        misc_info.append("  - Package Validation: PASSED – Package specifications match expectations")

    misc_info.append(f"    • Device: {device_label}, Lot: {lot_count}, Rule: {rule_type}")
    if weight_lbs is not None:
        misc_info.append(f"    • Package: {weight_disp}, {length:.1f} x {width:.1f} x {height:.1f} in")
    else:
        misc_info.append(f"    • Package: {pkg_weight_str}, {length:.1f} x {width:.1f} x {height:.1f} in")
    if price_cfg.get('enabled', False):
        price_show = meta.get('listing_price_key', '')
        typ        = meta.get('listing_type_key', '').title()
        if price_show and typ:
            misc_info.append(f"    • Price: {price_show} ({typ})")
            
def check_misc_issues(listing_data, sections, is_power_adapter, multiple_entries):
    # Keep a copy of the raw title dictionary (with original keys) so that
    # package-validation logic can detect model-specific overrides which rely
    # on keys like "title_text_key" / "title_key".
    raw_title = listing_data['title']
    # Normalised version used by the majority of misc-checks
    title = {k.replace('title_', ''): v for k, v in raw_title.items()}
    specs = {k.replace('specs_', ''): v for k, v in listing_data['specifics'].items()}
    table = {k.replace('table_', ''): v for k, v in listing_data['table_data'][0].items()} if listing_data['table_data'] else {}
    meta = {k.replace('meta_', ''): v for k, v in listing_data['metadata'].items()}
    description = {k.replace('desc_', ''): v for k, v in listing_data['description'].items()}
    misc_info = ["- Misc Comparison"]
    misc_issues = []
    issue_strings = []

    logger.debug(f"Title data keys: {list(listing_data['title'].keys())}", extra={'session_id': current_session_id})

    check_seller_notes_typos(specs, misc_issues)
    
    leaf_category = None
    for line in sections.get('CATEGORY', []):
        if '[leaf_category_key]' in line:
            parts = line.split(': ', 1)
            if len(parts) == 2:
                leaf_category = parts[1].strip()
            break
    logger.debug(f"Extracted leaf_category: '{leaf_category}'", extra={'session_id': current_session_id})

    # Package Validation (use raw_title so model-override rules work correctly)
    #check_package_validation(raw_title, meta, listing_data, sections, misc_info, misc_issues)

    # Consolidated Category Validation
    check_category_validation_consolidated(meta, title, leaf_category, misc_info, misc_issues, issue_strings)

    # Legacy Category vs Device Type Check
    check_legacy_category_vs_device_type(title, leaf_category, misc_info, misc_issues, issue_strings)

    # RAM and Storage Checks
    check_ram_range_verification(title, listing_data, misc_info, misc_issues)
    check_storage_range_verification(title, listing_data, misc_info, misc_issues)
    check_ram_breakdown_verification(title, misc_info, misc_issues)
    check_ram_configuration_validation(listing_data, multiple_entries, misc_info, misc_issues)

    # NEW: Server Memory Title Calculation Check
    check_server_memory_title_calculation(title, misc_info, misc_issues)

    # NEW: Storage Contradiction Check
    check_missing_storage_vs_capacity(listing_data, misc_info, misc_issues)

    # NEW: Battery Missing Components Check
    check_battery_missing_components(listing_data, misc_info, misc_issues)

    # Category and Type Checks
    #check_category_mismatch(title, leaf_category, misc_info, misc_issues)
    check_cpu_suffix_for_laptops(title, leaf_category, misc_info, misc_issues, issue_strings)

    # NEW: Cracked Title Condition Check
    check_cracked_title_condition(title, specs, listing_data, misc_info, misc_issues)

    # Apple and Policy Checks
    check_apple_password(specs, leaf_category, misc_info, misc_issues)
    check_lot_amount_consistency(title, meta, listing_data, misc_info, misc_issues, issue_strings)
    check_shipping_policy(meta, misc_info, misc_issues)
    check_return_policy(meta, misc_info, misc_issues)

    # NEW: Scheduled Listing Validation
    #check_scheduled_listing_validation(meta, misc_info, misc_issues)

    # Title and Form Factor Checks
    title_text = listing_data['title'].get('title_title_key', '')
    check_phrase_and_spelling(title_text, leaf_category, misc_info, misc_issues)
    check_form_factor_issues(title, specs, table, misc_info, misc_issues)

    # Extract store category for condition checks
    store_category, _ = extract_store_category_info(meta, title)

    # Condition Checks
    check_condition_fields(title, specs, table, meta, description, is_power_adapter, misc_info, misc_issues, store_category)
    check_for_parts_condition(specs, title, listing_data, sections, misc_info, misc_issues)
    check_untested_items_detection(title, specs, listing_data, sections, description, misc_info)

    return misc_info, misc_issues, issue_strings
    
def find_non_matched_keys(listing_data, title, specs):
    """Identify keys unique to each section using full table key union (shared + all entries).

    Uses normalized keys for comparison: prefixes like 'title_', 'specs_', 'table_' are removed.
    """
    title_keys = set(title.keys())
    specs_keys = set(specs.keys())

    # Build union of normalized table keys from shared and entry-specific values
    table_union_keys: set[str] = set()
    try:
        shared = listing_data.get('table_shared', {}) or {}
        for k in shared.keys():
            table_union_keys.add(k.replace('table_', ''))
        for entry in listing_data.get('table_data', []) or []:
            for k in entry.keys():
                table_union_keys.add(k.replace('table_', ''))
    except Exception:
        # Fallback to empty if structure is unexpected
        table_union_keys = set()

    unique_to_title = title_keys - (specs_keys | table_union_keys)
    unique_to_specs = specs_keys - (title_keys | table_union_keys)
    unique_to_table = table_union_keys - (title_keys | specs_keys)

    non_matched: list[tuple[str, str, str]] = []

    # Load key mappings
    key_mappings = load_key_mappings().get("mappings", [])

    # Helper function to check if a key is unmatched considering mappings
    def is_unmatched(section: str, key: str, other_sections: list[dict]) -> bool:
        for other_section in other_sections:
            if key in other_section["keys"]:
                return False  # Matched directly
            for mapping in key_mappings:
                if (mapping.get("section1") == section and mapping.get("key1") == key and 
                    mapping.get("section2") == other_section["name"] and mapping.get("key2") in other_section["keys"]) or \
                   (mapping.get("section2") == section and mapping.get("key2") == key and 
                    mapping.get("section1") == other_section["name"] and mapping.get("key1") in other_section["keys"]):
                    return False  # Matched through mapping
        return True

    # Check title keys
    other_sections = [{"name": "specifics", "keys": specs_keys}, {"name": "table", "keys": table_union_keys}]
    for key in unique_to_title:
        if is_unmatched("title", key, other_sections):
            logger.debug(f"Unmatched key in title: {key} = {title.get(key, '')}", extra={'session_id': current_session_id})
            non_matched.append(('title', key, title.get(key, '')))

    # Check specifics keys
    other_sections = [{"name": "title", "keys": title_keys}, {"name": "table", "keys": table_union_keys}]
    for key in unique_to_specs:
        if is_unmatched("specifics", key, other_sections):
            logger.debug(f"Unmatched key in specifics: {key} = {specs.get(key, '')}", extra={'session_id': current_session_id})
            non_matched.append(('specifics', key, specs.get(key, '')))

    # Check table keys
    other_sections = [{"name": "title", "keys": title_keys}, {"name": "specifics", "keys": specs_keys}]
    for key in unique_to_table:
        if is_unmatched("table", key, other_sections):
            # Try to source a representative value from shared values or first entry
            value = ''
            raw_key = 'table_' + key
            if raw_key in (listing_data.get('table_shared') or {}):
                value = (listing_data.get('table_shared') or {}).get(raw_key, '')
            else:
                for entry in (listing_data.get('table_data') or []):
                    if raw_key in entry:
                        value = entry.get(raw_key, '')
                        break
            logger.debug(f"Unmatched key in table: {key} = {value}", extra={'session_id': current_session_id})
            non_matched.append(('table', key, value))

    return non_matched

def group_non_matched_keys(non_matched):
    """Format non-matched keys for display in the 'Non Matched Keys' tab (Section, Key, Value)."""
    grouped_non_matched: list[tuple[str, str, str]] = []
    if non_matched:
        for section, key, value in sorted(non_matched, key=lambda x: (x[0], x[1])):
            grouped_non_matched.append((section.capitalize(), key, str(value)))
    else:
        grouped_non_matched = [("", "    - No non-matched keys", "")] 
    return grouped_non_matched

def consolidate_mismatches(listing_data, title, specs):
    """
    Consolidate mismatches across title, specifics, and table into single-line strings for keys
    where all table entries have the same value.
    
    Args:
        listing_data (dict): Parsed listing data containing table data.
        title (dict): Normalized title data.
        specs (dict): Normalized specifics data.
    
    Returns:
        tuple: (consolidated_issues, consolidated_keys) where consolidated_issues is a list
               of formatted strings and consolidated_keys is a set of keys that were consolidated.
    """
    consolidated_issues = []
    consolidated_keys = set()
    if not listing_data.get('table_data'):
        return consolidated_issues, consolidated_keys

    for key in set(title.keys()) | set(specs.keys()):
        table_key = 'table_' + key
        if table_key not in listing_data['table_data'][0]:
            continue  # Key not in table
        table_values = [entry.get(table_key, 'N/A') for entry in listing_data['table_data']]
        if len(set(table_values)) == 1:  # All table entries have the same value
            table_value = table_values[0]
            values = []
            sources = {}
            if key in title:
                val = title[key]
                values.append(("Title", val))
                sources.setdefault(val, []).append("Title")
            if key in specs:
                val = specs[key]
                values.append(("Specs", val))
                sources.setdefault(val, []).append("Specs")
            if table_value != 'N/A':
                values.append(("Table", table_value))
                sources.setdefault(table_value, []).append("Table")
            if len(values) > 1:  # Need at least two values to compare
                # Check for equivalence across all values
                is_discrepancy = False
                for i in range(len(values)):
                    for j in range(i + 1, len(values)):
                        src1, val1 = values[i]
                        src2, val2 = values[j]
                        if not check_equivalence(key, val1, val2, title=title, specs=specs, table=listing_data['table_data'][0]):
                            is_discrepancy = True
                            break
                    if is_discrepancy:
                        break
                if is_discrepancy:
                    parts = []
                    for val, src_list in sources.items():
                        if val != 'N/A':
                            parts.append(f"'{val}' in {' and '.join(src_list)}")
                    if parts:
                        # Remove '_key' from the display name to avoid "Storage Capacity Key"
                        display_key = key.replace('_key', '').replace('_', ' ').title()
                        issue_str = f"{display_key}: {', '.join(parts)}"
                        consolidated_issues.append(issue_str)
                        consolidated_keys.add(key)
    return consolidated_issues, consolidated_keys

def organize_issues(title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues, title_vs_meta_issues, specs_vs_meta_issues, misc_issues):
    """Organize issues into the issues_content list for the 'Issues' tab."""
    issues_content = []
    if title_vs_specs_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Title vs. Specifics Issues ---", "", "", ""))
        issues_content.extend(title_vs_specs_issues)
    if title_vs_table_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Title vs. Table Issues ---", "", "", ""))
        issues_content.extend(title_vs_table_issues)
    if specs_vs_table_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Specifics vs. Table Issues ---", "", "", ""))
        issues_content.extend(specs_vs_table_issues)
    if title_vs_meta_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Title vs. Metadata Issues ---", "", "", ""))
        issues_content.extend(title_vs_meta_issues)
    if specs_vs_meta_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Specs vs. Metadata Issues ---", "", "", ""))
        issues_content.extend(specs_vs_meta_issues)
    if misc_issues:
        issues_content.append(("", "", "", ""))
        issues_content.append(("    --- Miscellaneous Issues ---", "", "", ""))
        issues_content.extend([(("", issue[0], "", "")) for issue in misc_issues])
    return issues_content

def convert_to_consolidated(issue, num_table_entries):
    """
    Convert a pairwise issue to its consolidated form for single-entry tables.
    
    Args:
        issue (str): The issue string to process.
        num_table_entries (int): The number of table entries.
    
    Returns:
        str: The reformatted issue if applicable, otherwise the original issue.
    """
    if num_table_entries == 1 and "(Entry" in issue:
        match = re.match(r'^(\w+) (.+) in Title, (.+) \(Entry \d+\) in Table Entry$', issue)
        if match:
            key, title_value, table_value = match.groups()
            return f"{key}: '{title_value}' in Title, '{table_value}' in Table"
    return issue

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_copyable_text(summary_issues, misc_issues, metadata, meta, cleaned_sku, num_table_entries, title, specs, table):
    # Create a dictionary to consolidate issues by key
    consolidated = {}
    
    # Define key mappings for normalization
    key_mappings = {
        'processor_speed': 'cpu_speed',
        'clock_speed': 'cpu_speed',
        'processor': 'cpu',
        'memory': 'ram',
        'operating_system': 'os'
    }
    
    # Function to normalize storage units to uppercase
    def normalize_storage_units(value):
        if not value:
            return value
        
        # Replace units with uppercase versions
        pattern = r'(\d+(?:\.\d+)?)\s*(gb|tb|mb|KB)'
        return re.sub(pattern, lambda m: f"{m.group(1)}{m.group(2).upper()}", value, flags=re.IGNORECASE)
    
    # Function to normalize CPU speeds
    def normalize_cpu_speed(value):
        if not isinstance(value, str):
            return value
            
        # Match numeric part followed by GHz or MHz (with or without space)
        pattern = r'(\d+)(?:\.(\d+))?\s*(ghz|mhz)'
        
        def format_speed(match):
            whole = match.group(1)
            decimal = match.group(2) or ""
            unit = match.group(3).upper()
            
            # Ensure consistent decimal places - always show .XX format
            if len(decimal) == 0:
                decimal = "00"
            elif len(decimal) == 1:
                decimal = decimal + "0"
            
            # Format as X.XXGHz without space
            return f"{whole}.{decimal}{unit}"
            
        return re.sub(pattern, format_speed, value, flags=re.IGNORECASE)

    # Helper to deduplicate comma/slash-separated size lists after normalization (e.g., "4GB, 4gb" -> "4GB")
    def dedupe_size_list(value):
        if not value or not isinstance(value, str):
            return value
        unified = value.replace('/', ',')
        parts = [p.strip() for p in unified.split(',') if p.strip()]
        seen = set()
        deduped = []
        for part in parts:
            if part not in seen:
                seen.add(part)
                deduped.append(part)
        return ', '.join(deduped)
    
    # Function to fix CPU family values that are just "i3", "i5", "i7", "i9"
    def fix_cpu_family_value(value):
        if not value:
            return value
        # If the value is exactly "i3", "i5", "i7", or "i9", prepend "Core "
        if re.match(r'^i[3579]$', value.strip()):
            return f"Core {value.strip()}"
        return value
    
    # Function to normalize values for comparison by standardizing separators and splitting into components
    def normalize_for_comparison(value):
        """Normalize a value for comparison by standardizing separators and splitting into components."""
        if not value:
            return set()
        
        # Replace different separators with a standard one
        normalized = value.replace('/', ',')
        
        # Split by comma and clean up each part
        parts = [part.strip().lower() for part in normalized.split(',') if part.strip()]
        
        return set(parts)
    
    # Helper to extract RAM size tokens like '64gb', '1tb' from a string
    def extract_size_tokens(value):
        if not value or not isinstance(value, str):
            return []
        return [f"{m.group(1)}{m.group(2).lower()}" for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(gb|mb|tb)", value, flags=re.IGNORECASE)]
    
    # Function to collect all table values for a given key from listing data
    def collect_all_table_values_for_key(key_name, listing_data):
        """Collect all table values for a specific key across all entries and shared values."""
        all_values = set()
        
        # Normalize the key name to match table keys
        normalized_key = key_name.lower().replace(' ', '_')
        possible_table_keys = [
            f'table_{normalized_key}_key',
            f'{normalized_key}_key',
            normalized_key
        ]
        
        # Check shared values
        shared_values = listing_data.get('table_shared', {})
        for table_key in possible_table_keys:
            if table_key in shared_values and shared_values[table_key]:
                all_values.add(shared_values[table_key].strip())
        
        # Check individual table entries  
        table_entries = listing_data.get('table_data', [])
        for entry in table_entries:
            for table_key in possible_table_keys:
                if table_key in entry and entry[table_key]:
                    all_values.add(entry[table_key].strip())
        
        return sorted(all_values) if all_values else []
    
    # Process the pre-analyzed issue_strings from the main comparison functions
    for issue in summary_issues:
        # Skip any "no issues detected" messages
        if "no issues detected" in issue.lower():
            continue
            
        # Try to extract the key and values
        key_match = re.match(r'^([^:]+):', issue)
        if key_match:
            key = key_match.group(1).strip()
            
            # Initialize entry if this is a new key
            if key not in consolidated:
                consolidated[key] = {
                    'Title': None,
                    'Specs': None,
                    'Table': None,
                    'Table_Entries': {}
                }
            
            # Extract values using multiple patterns
            # Format 1: "Title has 'value'"
            title_has = re.search(r"Title has '([^']+)'", issue)
            specs_has = re.search(r"Specs has '([^']+)'", issue)
            table_has = re.search(r"Table has '([^']+)'", issue)
            
            # Format 2: "'value' in Title"
            title_in = re.search(r"'([^']+)' in Title", issue)
            specs_in = re.search(r"'([^']+)' in Specs", issue)
            table_in = re.search(r"'([^']+)' in Table(?!\s+Entry)", issue)
            
            # Format 3: "Specifics" instead of "Specs"
            specifics_has = re.search(r"Specifics has '([^']+)'", issue)
            specifics_in = re.search(r"'([^']+)' in Specifics", issue)
            
            # Format 4: Check for table entries like "Table Entry X"
            table_entry_match = re.search(r"'([^']+)' in Table Entry (\d+)", issue)
            if not table_entry_match:
                table_entry_match = re.search(r"Table Entry (\d+) has '([^']+)'", issue)
                if table_entry_match:
                    entry_num, value = table_entry_match.groups()
                else:
                    entry_num, value = None, None
            else:
                value, entry_num = table_entry_match.groups()
            
            # Format 5: "value in Specs, value (Table(all entries))"
            table_all_entries = re.search(r"'([^']+)' \(Table\(all entries\)\)", issue)
            if not table_all_entries:
                table_all_entries = re.search(r"([^ ]+) \(Table\(all entries\)\)", issue)
            
            # Store extracted values - Apply CPU family fix if this is a CPU family issue
            if title_has:
                val = title_has.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Title'] = val
            elif title_in:
                val = title_in.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Title'] = val
                
            if specs_has:
                val = specs_has.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Specs'] = val
            elif specs_in:
                val = specs_in.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Specs'] = val
            elif specifics_has:
                val = specifics_has.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Specs'] = val
            elif specifics_in:
                val = specifics_in.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Specs'] = val
                
            if table_has:
                val = table_has.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Table'] = val
            elif table_in:
                val = table_in.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Table'] = val
            elif table_all_entries:
                val = table_all_entries.group(1)
                if key.lower() == "cpu family":
                    val = fix_cpu_family_value(val)
                consolidated[key]['Table'] = val
                
            if entry_num and value:
                if key.lower() == "cpu family":
                    value = fix_cpu_family_value(value)
                consolidated[key]['Table_Entries'][entry_num] = value
        else:
            # Handle special format issues that don't match the key pattern
            if "Missing values" in issue or "Lot amount" in issue:
                if "special_issues" not in consolidated:
                    consolidated["special_issues"] = []
                consolidated["special_issues"].append(issue)
    
    # NEW: Collect table values from source data for keys that need it
    # This is crucial for getting the complete table data when the issue parsing missed values
    if listing and 'table_data' in listing:
        for key in consolidated:
            if key == "special_issues":
                continue
                
            # If we don't have a complete table value, try to collect it from source data
            if not consolidated[key]['Table'] or len(consolidated[key]['Table_Entries']) < num_table_entries:
                collected_table_values = collect_all_table_values_for_key(key, listing)
                if collected_table_values:
                    if len(collected_table_values) == 1:
                        consolidated[key]['Table'] = collected_table_values[0]
                    else:
                        consolidated[key]['Table'] = ', '.join(collected_table_values)
                    logger.debug(f"Collected table values for {key}: {consolidated[key]['Table']}", extra={'session_id': current_session_id})
    
    # Fallback lookup for missing values from source dictionaries
    for key in consolidated:
        if key == "special_issues":
            continue
            
        # Normalize the key for lookup in source dictionaries
        lookup_key = key.lower().replace(' ', '_')
        
        # Try various key formats to find matches in source dictionaries
        possible_keys = [
            lookup_key,
            lookup_key + '_key',
            lookup_key.replace('_', ''),
            lookup_key.replace('motherboard_model', 'motherboard'),
            lookup_key.replace('screen_size', 'display_size'),
            lookup_key.replace('cpu_', 'processor_'),
        ]
        
        # Look up missing values from source dictionaries
        if not consolidated[key]['Title'] and title:
            for possible_key in possible_keys:
                if possible_key in title and title[possible_key]:
                    consolidated[key]['Title'] = title[possible_key]
                    break
                    
        if not consolidated[key]['Specs'] and specs:
            for possible_key in possible_keys:
                if possible_key in specs and specs[possible_key]:
                    consolidated[key]['Specs'] = specs[possible_key]
                    break
                    
        if not consolidated[key]['Table'] and table:
            for possible_key in possible_keys:
                if possible_key in table and table[possible_key]:
                    consolidated[key]['Table'] = table[possible_key]
                    break
    
    # Process misc_issues, extracting only the message part and deduplicating
    misc_issue_texts = []
    sanitization_issues = set()  # Use a set to avoid duplicates
    seen_misc_issues = set()  # Track what we've already added

    for item in misc_issues:
        issue_text = None
        
        if isinstance(item, tuple) and len(item) >= 1:
            # Handle actual tuples
            issue_text = item[0].strip()
        elif isinstance(item, str):
            # Handle strings that might be string representations of tuples
            item_stripped = item.strip()
            
            # Check if this is a string representation of a tuple like "('message',)"
            if item_stripped.startswith("('") and item_stripped.endswith("',)"):
                # Extract the message from the string representation
                # Remove the outer parentheses and quotes
                issue_text = item_stripped[2:-3].strip()  # Remove "(' and ',)"
            elif item_stripped.startswith('("') and item_stripped.endswith('",)'):
                # Handle double quotes version
                issue_text = item_stripped[2:-3].strip()  # Remove '(" and ",)'
            else:
                # Regular string
                issue_text = item_stripped
        else:
            # Fallback for other types
            issue_text = str(item).strip()
        
        # Only process if we have valid text and haven't seen it before
        if issue_text and issue_text not in seen_misc_issues:
            # Handle Data Sanitization specially to avoid duplicates
            if "Data Sanitization" in issue_text or "missing Data" in issue_text:
                sanitization_issues.add(issue_text)
            else:
                misc_issue_texts.append(issue_text)
            seen_misc_issues.add(issue_text)
    
    # Add only one Data Sanitization issue if present
    if sanitization_issues:
        # Prefer "missing Data Sanitization." over other variants
        if "missing Data Sanitization." in sanitization_issues:
            misc_issue_texts.append("missing Data Sanitization.")
        else:
            misc_issue_texts.append(next(iter(sanitization_issues)))
    
    # Generate final consolidated issue strings
    all_issues = []
    
    # Process key-based issues with appropriate normalization
    for key, values in consolidated.items():
        if key == "special_issues":
            all_issues.extend(values)
            continue
            
        # Skip CPU generation issue if CPU model issue exists
        if key == "Cpu Generation" and "Cpu Model" in consolidated:
            logger.debug(f"Skipping CPU generation issue because CPU model issue exists", extra={'session_id': current_session_id})
            continue
            
        # Get the values
        title_val = values.get('Title')
        specs_val = values.get('Specs')
        table_val = values.get('Table')
        
        # Check which type of normalization to apply
        is_storage_or_ram_key = any(term in key.lower() for term in ['ram', 'storage', 'capacity', 'size'])
        is_cpu_speed_key = any(term in key.lower() for term in ['cpu speed', 'processor speed', 'clock speed'])
        
        # Apply appropriate normalization
        if is_storage_or_ram_key:
            if title_val:
                title_val = dedupe_size_list(normalize_storage_units(title_val))
            if specs_val:
                specs_val = dedupe_size_list(normalize_storage_units(specs_val))
            if table_val:
                table_val = dedupe_size_list(normalize_storage_units(table_val))
            # Also normalize and dedupe any table entries
            for entry_num, entry_val in values.get('Table_Entries', {}).items():
                values['Table_Entries'][entry_num] = dedupe_size_list(normalize_storage_units(entry_val))
        elif is_cpu_speed_key:
            if title_val:
                title_val = normalize_cpu_speed(title_val)
            if specs_val:
                specs_val = normalize_cpu_speed(specs_val)
            if table_val:
                table_val = normalize_cpu_speed(table_val)
            # Also normalize any table entries
            for entry_num, entry_val in values.get('Table_Entries', {}).items():
                values['Table_Entries'][entry_num] = normalize_cpu_speed(entry_val)
        
        # Count how many values we have
        existing_values = []
        if title_val:
            existing_values.append(('Title', title_val))
        if specs_val:
            existing_values.append(('Specs', specs_val))
        if table_val:
            existing_values.append(('Table', table_val))
        
        # Handle table entries if no main table value
        if not table_val and values['Table_Entries']:
            if len(values['Table_Entries']) == 1:
                entry_num, entry_val = next(iter(values['Table_Entries'].items()))
                existing_values.append(('Table', entry_val))
                table_val = entry_val  # For consistency
            elif all(v == next(iter(values['Table_Entries'].values())) for v in values['Table_Entries'].values()):
                # All entries have the same value
                entry_val = next(iter(values['Table_Entries'].values()))
                existing_values.append(('Table', f"{entry_val} (all entries)"))
                table_val = entry_val  # For consistency
            else:
                # Multiple entries with different values
                table_parts = []
                for entry_num, entry_val in sorted(values['Table_Entries'].items(), key=lambda x: int(x[0])):
                    table_parts.append(f"Entry {entry_num}: '{entry_val}'")
                table_display = f"[{', '.join(table_parts)}]"
                existing_values.append(('Table', table_display))
        
        if not existing_values:
            continue
        
        # SEPARATE LOGIC FOR 2 VS 3 VALUES
        if len(existing_values) == 1:
            # Single value
            source, val = existing_values[0]
            issue_text = f"{key}: {source} has '{val}'"
            
        elif len(existing_values) == 2:
            # TWO VALUES - Always show both without highlighting
            source1, val1 = existing_values[0]
            source2, val2 = existing_values[1]
            issue_text = f"{key}: {source1} has '{val1}', {source2} has '{val2}'"
            
        elif len(existing_values) == 3:
            # THREE VALUES - Show all with highlighting for the different one
            title_lower = title_val.lower() if title_val else None
            specs_lower = specs_val.lower() if specs_val else None
            table_lower = table_val.lower() if table_val else None
            
            # Check which one is different for highlighting
            if title_lower and specs_lower and table_lower:
                # Normalize for better comparison
                title_parts = normalize_for_comparison(title_val)
                specs_parts = normalize_for_comparison(specs_val)
                table_parts = normalize_for_comparison(table_val)
                
                # INTENT: If Ram Size has only Table differing with exactly two values (e.g., '64GB, 65GB')
                # and there's a separate 'RAM mismatch' message referencing those two sizes (e.g.,
                # '4x16GB = 64GB, but total displayed: 65GB'), suppress the Ram Size mismatch here
                # to avoid duplicate/noisy messaging. Show only the RAM mismatch in that case.
                if 'ram' in key.lower() and 'size' in key.lower():
                    if title_parts == specs_parts:
                        table_tokens = list(dict.fromkeys(extract_size_tokens(table_val)))
                        token_set = set(table_tokens)
                        if len(token_set) == 2:
                            lower_misc = [t.lower() for t in misc_issue_texts]
                            if any(('ram mismatch' in m and all(tok in m for tok in token_set)) for m in lower_misc):
                                # Covered by a more specific RAM mismatch; skip adding this size mismatch
                                continue
                
                # Check which one is different based on content, not just string comparison
                if title_parts != specs_parts and specs_parts == table_parts:
                    # Title is different
                    issue_text = f"{key}: Title has '**{title_val}**', Specs has '{specs_val}', Table has '{table_val}'"
                elif specs_parts != title_parts and title_parts == table_parts:
                    # Specs is different
                    issue_text = f"{key}: Title has '{title_val}', Specs has '**{specs_val}**', Table has '{table_val}'"
                elif table_parts != title_parts and title_parts == specs_parts:
                    # Table is different
                    issue_text = f"{key}: Title has '{title_val}', Specs has '{specs_val}', Table has '**{table_val}**'"
                else:
                    # All different or some other case - check for partial matches
                    title_specs_overlap = bool(title_parts & specs_parts)
                    title_table_overlap = bool(title_parts & table_parts)
                    specs_table_overlap = bool(specs_parts & table_parts)
                    
                    # If title and specs have more in common than either has with table
                    if title_specs_overlap and not (title_table_overlap and specs_table_overlap):
                        issue_text = f"{key}: Title has '{title_val}', Specs has '{specs_val}', Table has '**{table_val}**'"
                    # If title and table have more in common than either has with specs
                    elif title_table_overlap and not (title_specs_overlap and specs_table_overlap):
                        issue_text = f"{key}: Title has '{title_val}', Specs has '**{specs_val}**', Table has '{table_val}'"
                    # If specs and table have more in common than either has with title
                    elif specs_table_overlap and not (title_specs_overlap and title_table_overlap):
                        issue_text = f"{key}: Title has '**{title_val}**', Specs has '{specs_val}', Table has '{table_val}'"
                    else:
                        # All different - no highlighting
                        issue_text = f"{key}: Title has '{title_val}', Specs has '{specs_val}', Table has '{table_val}'"
            else:
                # Fallback
                issue_text = f"{key}: Title has '{title_val}', Specs has '{specs_val}', Table has '{table_val}'"
        
        else:
            # More than 3 values
            parts = [f"{source} has '{val}'" for source, val in existing_values]
            issue_text = f"{key}: {', '.join(parts)}"
        
        all_issues.append(issue_text)
    
    # Add misc issues
    all_issues.extend(misc_issue_texts)
    
    # Remove duplicates while maintaining order
    seen = set()
    unique_issues = []
    for issue in all_issues:
        if issue not in seen:
            seen.add(issue)
            unique_issues.append(issue)

    # Suppress creating an issues message when all three condition fields are missing.
    # Specifically: if the only issues are exactly "missing Cosmetic.", "missing Functional.",
    # and "missing Data Sanitization.", then archive the item context for later analysis
    # and return an empty summary to avoid messaging.
    missing_condition_set = {"missing cosmetic.", "missing functional.", "missing data sanitization."}
    if unique_issues:
        normalized_set = {s.strip().lower() for s in unique_issues}
        if normalized_set == missing_condition_set:
            try:
                item_number_for_archive = (metadata or {}).get('meta_itemnumber_key') or (meta or {}).get('meta_itemnumber_key') or 'Unknown'
                _archive_suppressed_missing_conditions(item_number_for_archive)
                _archive_trouble(item_number_for_archive, reason='missing_condition_triple')
            except Exception as _e:
                try:
                    logger.error(f"Archive failed for suppressed item {item_number_for_archive}: {_e}", extra={'session_id': current_session_id})
                except Exception:
                    pass
            return ""
    
    # Proceed with normal messaging behavior otherwise
    
    # Return empty string if no issues
    if not unique_issues:
        return ""
    
    # Determine listing status (Active or Scheduled)
    listing_status = "Active"  # Default to Active
    
    # Check listing location keys (support new and legacy variants)
    for data_dict in [meta, metadata]:
        if not data_dict:
            continue

        key_variants = [
            'meta_listing_location_key',   # preferred
            'meta_listinglocation_key',    # legacy
            'listing_location_key',        # normalized without meta_
            'listinglocation_key'          # legacy normalized
        ]

        for key_variant in key_variants:
            if key_variant in data_dict:
                value = str(data_dict[key_variant]).strip()
                if value.lower() == "scheduled":
                    listing_status = "Scheduled"
                    logger.debug(
                        f"Determined listing status as Scheduled from {key_variant}: '{value}'",
                        extra={'session_id': current_session_id}
                    )
                    break
        if listing_status == "Scheduled":
            break
    
    # Fallback to original logic if specific key not found
    if listing_status == "Active":
        for data_dict in [meta, metadata]:
            if not data_dict:
                continue
            for key in data_dict:
                key_lower = key.lower()
                if any(term in key_lower for term in ["status", "type", "listing_type"]):
                    value = str(data_dict[key]).lower()
                    if "schedul" in value:
                        listing_status = "Scheduled"
                        logger.debug(f"Determined listing status as Scheduled based on metadata field '{key}' (fallback method)", extra={'session_id': current_session_id})
                        break
            if listing_status == "Scheduled":
                break
    
    # Extract the full title from the title data
    full_title = title.get('title_title_key', '') if title else ''
    if not full_title:
        # Fallback to metadata title if title_title_key is not available
        full_title = metadata.get('meta_title_key', '') if metadata else ''
    
    # Construct the summary text with the new format including full title
    item_number = metadata.get('meta_itemnumber_key', 'Unknown')
    url = f"https://www.ebay.com/lstng?mode=ReviseItem&itemId={item_number}&sr=wn"
    
    lines = []
    if unique_issues:
        # Create header line with just the title (no issues on same line)
        if full_title:
            lines.append(f"⚠ {cleaned_sku} {listing_status}: {full_title}")
        else:
            lines.append(f"⚠ {cleaned_sku} {listing_status}:")
        
        # Add all issues as separate lines with " - " prefix
        for issue in unique_issues:
            lines.append(f" - {issue}")
        
        # Append URL to the last line
        lines[-1] += f" {url}"
    else:
        if full_title:
            lines.append(f"⚠ {cleaned_sku} {listing_status}: {full_title} - No specific issues detected {url}")
        else:
            lines.append(f"⚠ {cleaned_sku} {listing_status}: No specific issues detected {url}")
    
    # Annotate SKU with real name when available for messaging
    if lines and cleaned_sku:
        try:
            annotated = annotate_sku_with_name(cleaned_sku)
            if annotated != cleaned_sku:
                lines[0] = lines[0].replace(cleaned_sku, annotated, 1)
        except Exception:
            pass

    summary_text = "\n".join(lines)
    
    return summary_text

def _archive_suppressed_missing_conditions(item_number: str) -> None:
    """Archive relevant files for items suppressed due to all three missing condition fields.

    Copies related files into archives/suppressed_missing_conditions/<item_number>/ preserving
    the original folder structure for later analysis.
    """
    try:
        if not item_number or item_number == 'Unknown':
            return
        base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        archive_root = base_dir / 'archives' / 'suppressed_missing_conditions' / str(item_number)
        archive_root.mkdir(parents=True, exist_ok=True)

        copied: list[str] = []

        # Helper to copy files that contain the item number, preserving relative structure
        def copy_matches(src_base: Path, rel_root: str):
            if not src_base.exists():
                return
            dest_base = archive_root / rel_root
            for p in src_base.rglob('*'):
                try:
                    if p.is_file() and str(item_number) in p.name:
                        rel = p.relative_to(src_base)
                        dest_path = dest_base / rel
                        dest_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(p, dest_path)
                        # Record as path relative to project root for readability
                        try:
                            copied.append(str((src_base / rel).relative_to(base_dir)))
                        except Exception:
                            copied.append(str(src_base / rel))
                except Exception:
                    # Continue copying other files even if one fails
                    continue

        # Item contents (parsed, description, html, etc.)
        copy_matches(base_dir / 'item_contents', 'item_contents')

        # Logs (processing, pull, compare, etc.) - recurse entire logs tree
        copy_matches(base_dir / 'logs', 'logs')

        # Table data or any other auxiliary folders commonly keyed by item number
        copy_matches(base_dir / 'table_data', 'table_data')
        copy_matches(base_dir / 'description_screenshots', 'description_screenshots')

        # Also attempt to include any html captured by scanners under known folders
        copy_matches(base_dir / 'isolated_zscrape', 'isolated_zscrape')

        # Write a simple manifest for later analysis
        manifest = {
            'item_number': str(item_number),
            'reason': 'suppressed_missing_condition_triple',
            'timestamp': datetime.now().isoformat(),
            'copied_files': copied,
        }
        try:
            with open(archive_root / 'manifest.json', 'w', encoding='utf-8') as mf:
                json.dump(manifest, mf, indent=2)
        except Exception:
            pass

        try:
            logger.info(f"Archived suppressed item {item_number} with {len(copied)} files", extra={'session_id': current_session_id})
        except Exception:
            pass
    except Exception:
        # Silently ignore to avoid impacting the main flow
        pass

def _archive_trouble(item_number: str, reason: str = 'unspecified') -> None:
    """Archive relevant project files for items that require later investigation.

    Writes into a root-level folder 'trouble/<item_number>/' with a manifest and
    copies of logs, item contents, html, and table data when present.
    """
    try:
        base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        trouble_root = base_dir / 'trouble' / str(item_number or 'Unknown')
        trouble_root.mkdir(parents=True, exist_ok=True)

        copied: list[str] = []

        def safe_copy_tree(src_root: Path, rel_name: str):
            if not src_root.exists():
                return
            dest_root = trouble_root / rel_name
            for p in src_root.rglob('*'):
                try:
                    if p.is_file():
                        rel = p.relative_to(src_root)
                        dest_path = dest_root / rel
                        dest_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(p, dest_path)
                        try:
                            copied.append(str((src_root / rel).relative_to(base_dir)))
                        except Exception:
                            copied.append(str(src_root / rel))
                except Exception:
                    continue

        # Common project folders to capture
        safe_copy_tree(base_dir / 'item_contents', 'item_contents')
        safe_copy_tree(base_dir / 'logs', 'logs')
        safe_copy_tree(base_dir / 'table_data', 'table_data')
        safe_copy_tree(base_dir / 'description_screenshots', 'description_screenshots')
        safe_copy_tree(base_dir / 'isolated_zscrape', 'isolated_zscrape')

        manifest = {
            'item_number': str(item_number or 'Unknown'),
            'reason': reason,
            'timestamp': datetime.now().isoformat(),
            'copied_files': copied,
        }
        try:
            with open(trouble_root / 'manifest.json', 'w', encoding='utf-8') as mf:
                json.dump(manifest, mf, indent=2)
        except Exception:
            pass

        try:
            logger.info(f"Trouble archive created for {item_number} ({reason}) with {len(copied)} files", extra={'session_id': current_session_id})
        except Exception:
            pass
    except Exception:
        # Do not interfere with the main flow
        pass

def handle_file_operations(file_path, item_number, result):
    global is_command_line_mode, current_session_id, send_message_flag
    if is_command_line_mode:
        has_handled_file_operations = True
    if file_path and item_number != 'Unknown':
        blacklist = load_blacklist()
        issues = load_items_with_issues()
        has_issues_flag = has_issues(result)
        
        if has_issues_flag:
            summary = result['Issues'][1] if isinstance(result['Issues'], tuple) else ""
            if re.match(r"XX Unknown(?: Active| Scheduled)?:", summary):
                # Create log file in compare_error_logs
                error_log_dir = Path('compare_error_logs')
                error_log_dir.mkdir(exist_ok=True)
                log_file = error_log_dir / f"{item_number}.log"
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write("Can you tell me more about why this was not read or parsed?\n\n")
                    f.write("Title & Metadata:\n")
                    f.write(result['Combined Data'] + "\n\n")
                    f.write("Pull Log:\n")
                    pull_log_content = extract_pull_log_content(item_number)
                    f.write(pull_log_content + "\n\n")
                    f.write("Process Log:\n")
                    process_log_file = Path(PROCESSING_LOGS_DIR) / 'process_logs' / f"process_log_{item_number}.txt"
                    if process_log_file.exists():
                        try:
                            with open(process_log_file, 'r', encoding='utf-8') as pf:
                                f.write(pf.read())
                        except UnicodeDecodeError as e:
                            if SUPPRESS_INVALID_START_BYTE:
                                logger.error(f"Failed to load {process_log_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
                                f.write("Error reading process log: invalid start byte\n")
                            else:
                                raise
                    else:
                        f.write("Process log file not found.\n")
                logger.info(f"Created error log for item {item_number}", extra={'session_id': current_session_id})
            else:
                remove_from_blacklist(item_number, blacklist)
                add_to_issues_list(item_number, issues)
                if summary.strip():
                    # AUTO-GENERATE REPORT whenever we send a message
                    generate_report(file_path, item_number, result)
                    
                    try:
                        logger.info(f"Sending summary for item {item_number} to testmattermostmsg.py", extra={'session_id': current_session_id})
                        mm_script = os.path.join(os.path.dirname(__file__), 'testmattermostmsg.py')
                        subprocess.run([sys.executable, mm_script, summary])
                    except Exception as e:
                        logger.error(f"Failed to invoke testmattermostmsg.py for item {item_number}: {e}", extra={'session_id': current_session_id})
                global sound_played
                if not sound_played:
                    try:
                        winsound.Beep(500, 500)
                        sound_played = True
                        logger.debug("Played sound for issues detected", extra={'session_id': current_session_id})
                    except Exception as e:
                        logger.error(f"Error playing sound: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        else:
            # Always add to blacklist and don't check issues list since we've disabled that functionality
            if True:
                blacklist.add(item_number)
                save_blacklist(blacklist)
                logger.debug(f"Item {item_number} moved to blacklist", extra={'session_id': current_session_id})

def generate_details_log(title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta, non_matched, listing_data=None):
    """Generate a comprehensive details log of all comparisons performed."""
    details_log = []
    
    # Header with summary
    details_log.append("=== DETAILED COMPARISON ANALYSIS ===\n")
    details_log.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    details_log.append(f"Session ID: {current_session_id}\n\n")
    
    # Data summary
    if listing_data:
        title_keys = len(listing_data.get('title', {}))
        specs_keys = len(listing_data.get('specifics', {}))
        table_entries = len(listing_data.get('table_data', []))
        shared_keys = len(listing_data.get('table_shared', {}))
        
        details_log.append("=== DATA SUMMARY ===\n")
        details_log.append(f"Title keys found: {title_keys}\n")
        details_log.append(f"Specifics keys found: {specs_keys}\n")
        details_log.append(f"Table entries: {table_entries}\n")
        details_log.append(f"Shared table keys: {shared_keys}\n\n")
    
    # Helper function to format comparison results
    def format_comparison_section(title, comparison_data, show_matches=True):
        section_log = [f"=== {title.upper()} ===\n"]
        
        if not comparison_data:
            section_log.append("No comparisons performed.\n\n")
            return section_log
        
        matches = 0
        mismatches = 0
        
        for entry in comparison_data:
            if len(entry) >= 3:  # Ensure we have enough elements
                key = entry[0].strip() if entry[0] else "Unknown Key"
                symbol = entry[2] if len(entry) > 2 else "?"
                
                # Skip header/separator entries
                if key.startswith("---") or not key or key.startswith("    -"):
                    continue
                
                is_match = symbol == "=="
                if is_match:
                    matches += 1
                    if show_matches:
                        val1 = entry[1].strip() if len(entry) > 1 and entry[1] else "N/A"
                        val2 = entry[3].strip() if len(entry) > 3 and entry[3] else "N/A"
                        section_log.append(f"✓ MATCH: {key}\n")
                        section_log.append(f"    Source 1: {val1}\n")
                        section_log.append(f"    Source 2: {val2}\n\n")
                else:
                    mismatches += 1
                    val1 = entry[1].strip() if len(entry) > 1 and entry[1] else "N/A"
                    val2 = entry[3].strip() if len(entry) > 3 and entry[3] else "N/A"
                    table_entry = entry[4].strip() if len(entry) > 4 and entry[4] else ""
                    
                    section_log.append(f"✗ MISMATCH: {key}\n")
                    section_log.append(f"    Source 1: {val1}\n")
                    section_log.append(f"    Source 2: {val2}\n")
                    if table_entry:
                        section_log.append(f"    Table Entry: {table_entry}\n")
                    section_log.append("\n")
        
        # Add summary for this section
        total = matches + mismatches
        section_log.insert(1, f"Summary: {matches} matches, {mismatches} mismatches out of {total} comparisons\n\n")
        
        return section_log
    
    # Title vs. Specifics Analysis
    details_log.extend(format_comparison_section("Title vs. Specifics Analysis", title_vs_specs))
    
    # Title vs. Table Analysis
    details_log.extend(format_comparison_section("Title vs. Table Analysis", title_vs_table))
    
    # Specifics vs. Table Analysis
    details_log.extend(format_comparison_section("Specifics vs. Table Analysis", specs_vs_table))
    
    # Title vs. Metadata Analysis
    details_log.extend(format_comparison_section("Title vs. Metadata Analysis", title_vs_meta, show_matches=False))
    
    # Specifics vs. Metadata Analysis
    details_log.extend(format_comparison_section("Specifics vs. Metadata Analysis", specs_vs_meta, show_matches=False))
    
    # Non-Matched Keys Analysis
    details_log.append("=== NON-MATCHED KEYS ANALYSIS ===\n")
    if non_matched:
        by_section = {}
        for section, key, value in non_matched:
            if section not in by_section:
                by_section[section] = []
            by_section[section].append((key, value))
        
        for section, keys in by_section.items():
            details_log.append(f"\n{section.upper()} UNIQUE KEYS:\n")
            for key, value in keys:
                details_log.append(f"  • {key}: {value}\n")
        
        details_log.append(f"\nTotal unique keys: {len(non_matched)}\n\n")
    else:
        details_log.append("No unique keys found - all keys were matched across sections.\n\n")
    
    # Equivalence Rules Applied
    details_log.append("=== EQUIVALENCE RULES CONTEXT ===\n")
    if equivalence_rules:
        details_log.append(f"Total equivalence rules loaded: {len(equivalence_rules)}\n")
        details_log.append("Rules available for keys:\n")
        for key, rules in equivalence_rules.items():
            details_log.append(f"  • {key}: {len(rules)} rule(s)\n")
    else:
        details_log.append("No equivalence rules loaded.\n")
    details_log.append("\n")
    
    # Processing Notes
    details_log.append("=== PROCESSING NOTES ===\n")
    details_log.append("• Keys are normalized by removing prefixes (title_, specs_, table_) and suffixes (_key)\n")
    details_log.append("• Numbered keys (e.g., ram_size1, ram_size2) are consolidated when possible\n")
    details_log.append("• Equivalence rules are applied before declaring mismatches\n")
    details_log.append("• Empty or missing values are handled as 'N/A'\n\n")
    
    return ''.join(details_log)
    
def construct_result(issues_content, copyable_text, misc_issues, title_vs_specs, title_vs_table, specs_vs_table,
                     title_vs_meta, specs_vs_meta, misc_info, grouped_non_matched, combined_data, sections, details_log,
                     consolidated_title_vs_table, consolidated_specs_vs_table, consolidated_title_vs_specs_issues=None):
    """Construct the result dictionary for compare_data."""
    
    # Generate enhanced details log
    enhanced_details_log = generate_details_log(
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta,
        # Convert grouped_non_matched back to the format expected by generate_details_log
        [(row[0], row[1], row[2]) for row in grouped_non_matched if len(row) >= 3 and row[0] and not row[0].startswith('    -')],
        listing  # Pass the global listing data for summary statistics
    )
    
    result = {
        # Changed to an empty list instead of "No issues detected" message
        'Issues': (issues_content if issues_content else [], copyable_text),
        'Misc Issues': misc_issues,
        'Title vs. Specifics': title_vs_specs if title_vs_specs else [("", "    - No matching keys found", "", "")],
        'Title vs. Table Data': title_vs_table if title_vs_table else [("", "    - No matching keys found", "", "")],
        'Specifics vs. Table Data': specs_vs_table if specs_vs_table else [("", "    - No matching keys found", "", "")],
        'Title vs. Metadata': title_vs_meta if title_vs_meta else [("", "    - No matching keys found", "", "")],
        'Specs vs. Metadata': specs_vs_meta if specs_vs_meta else [("", "    - No matching keys found", "", "")],
        'Misc Comparison': "\n".join(misc_info),
        'Non-Matched Keys': grouped_non_matched,
        'Combined Data': combined_data,
        'Specifics': "\n".join(sections['SPECIFICS']),
        'Table Data': "\n".join(sections['TABLE DATA']),
        'Details': enhanced_details_log,  # Use the enhanced details log
        'consolidated_title_vs_table': consolidated_title_vs_table,
        'consolidated_specs_vs_table': consolidated_specs_vs_table,
        'Title vs. Specifics Issues': consolidated_title_vs_specs_issues
    }
    logger.debug(f"Comparison result: {result}", extra={'session_id': current_session_id})
    return result

def display_non_matched_keys(non_matched):
    # Group keys by section
    grouped = defaultdict(list)
    for section, key, value in non_matched:
        grouped[section].append((key, value))
    
    # Build the display output
    output = "Non Matched Keys:\n\n"
    for section in ['title', 'specifics', 'table']:
        if grouped[section]:
            output += f"{section.capitalize()}:\n"
            for key, value in grouped[section]:
                output += f"  - {key}: {value}\n"
            output += "\n"
    
    return output

def normalize_section_data(listing_data):
    """Normalize dictionary keys by removing section-specific prefixes."""
    title = {k.replace('title_', ''): v for k, v in listing_data['title'].items()}
    specs = {k.replace('specs_', ''): v for k, v in listing_data['specifics'].items()}
    table = {k.replace('table_', ''): v for k, v in listing_data['table_data'][0].items()} if listing_data['table_data'] else {}
    meta = {k.replace('meta_', ''): v for k, v in listing_data['metadata'].items()}
    description = {k.replace('desc_', ''): v for k, v in listing_data['description'].items()}
    table_meta = {k.replace('table_', ''): v for k, v in listing_data['table_metadata'].items()}
    return title, specs, table, meta, description, table_meta

def extract_leaf_category(sections):
    """Extract the leaf category from the sections data."""
    leaf_category = None
    for line in sections.get('CATEGORY', []):
        if '[leaf_category_key]' in line:
            parts = line.split(': ', 1)
            if len(parts) == 2:
                leaf_category = parts[1].strip()
            break
    return leaf_category

def log_debugging_info(listing_data, title, specs, table, meta, description, table_meta):
    """Log base keys and table data entries for debugging."""
    logger.debug(f"Table data entries: {[entry.get('table_battery_key', 'Not Set') for entry in listing_data['table_data']]}", extra={'session_id': current_session_id})
    logger.debug(f"Title base keys: {list(title.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"Specs base keys: {list(specs.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"Table base keys: {list(table.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"Meta base keys: {list(meta.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"Description base keys: {list(description.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"Table metadata keys: {list(table_meta.keys())}", extra={'session_id': current_session_id})

def perform_section_comparisons(listing_data, sections, is_power_adapter, multiple_entries, title, specs, table):
    """Perform comparisons between sections using existing comparison functions.

    Args:
        listing_data (dict): Parsed listing data.
        sections (dict): Parsed sections like title, specifics, and table.
        is_power_adapter (bool): Flag for power adapter-specific logic.
        multiple_entries (bool): Indicates if the table has multiple entries.
        title (dict): Normalized title data.
        specs (dict): Normalized specifics data.
        table (dict): Normalized table data.

    Returns:
        tuple: 15 values including issues, comparison results, and consolidated mismatches.
    """
    title_vs_specs_issues = []
    title_vs_table_issues = []
    specs_vs_table_issues = []
    title_vs_meta_issues = []
    specs_vs_meta_issues = []
    title_vs_specs = []
    title_vs_table = []
    specs_vs_table = []
    title_vs_meta = []
    specs_vs_meta = []
    misc_info = ["- Misc Comparison"]
    misc_issues = []
    issue_strings = []

    # Title vs. Specifics
    tvs_result, tvs_issues, tvs_issues_str = compare_title_vs_specifics(listing_data, sections, is_power_adapter)
    title_vs_specs.extend(tvs_result)
    title_vs_specs_issues.extend(tvs_issues)
    issue_strings.extend(tvs_issues_str)

    # Title vs. Table
    tvt_result, tvt_issues, tvt_issues_str, consolidated_title_vs_table = compare_title_vs_table(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    title_vs_table.extend(tvt_result)
    title_vs_table_issues.extend(tvt_issues)
    issue_strings.extend(tvt_issues_str)

    # Specifics vs. Table
    svt_result, svt_issues, svt_issues_str, consolidated_specs_vs_table = compare_specifics_vs_table(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    specs_vs_table.extend(svt_result)
    specs_vs_table_issues.extend(svt_issues)
    issue_strings.extend(svt_issues_str)

    # Title vs. Metadata
    tvm_result, tvm_issues, tvm_issues_str = compare_title_vs_metadata(listing_data, sections, is_power_adapter)
    title_vs_meta.extend(tvm_result)
    title_vs_meta_issues.extend(tvm_issues)
    issue_strings.extend(tvm_issues_str)

    # Specifics vs. Metadata
    svm_result, svm_issues, svm_issues_str = compare_specifics_vs_metadata(listing_data, sections, is_power_adapter)
    specs_vs_meta.extend(svm_result)
    specs_vs_meta_issues.extend(svm_issues)
    issue_strings.extend(svm_issues_str)

    # Miscellaneous Issues
    misc_info_result, misc_issues_result, misc_issues_str = check_misc_issues(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    misc_info.extend(misc_info_result[1:])  # Exclude header
    misc_issues.extend(misc_issues_result)
    issue_strings.extend(misc_issues_str)

    return (
        title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues, title_vs_meta_issues, specs_vs_meta_issues,
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta,
        misc_info, misc_issues, issue_strings,
        consolidated_title_vs_table, consolidated_specs_vs_table
    )

def standardize_key(key, remove_prefix=None):
    """
    Standardizes a key for consistent comparison across data sections.
    
    Args:
        key (str): The original key to standardize.
        remove_prefix (str, optional): A prefix to remove from the key (e.g., 'title_', 'specs_').
    
    Returns:
        str: The standardized key.
    """
    # Strip whitespace and convert to lowercase
    standardized = key.strip().lower()
    
    # Remove specified prefix if provided
    if remove_prefix and standardized.startswith(remove_prefix):
        standardized = standardized[len(remove_prefix):]
    
    # Replace spaces with underscores
    standardized = standardized.replace(' ', '_')
    
    # Optional custom mappings (add more as needed based on data patterns)
    mappings = {
        'memory_ram_key': 'ram_size_key',  # Example mapping
        # Add additional mappings here if logs reveal specific inconsistencies
    }
    
    # Apply mapping if the key exists in the mappings dictionary
    return mappings.get(standardized, standardized)


def initialize_comparison_data(listing_data):
    """
    Initialize data for comparison by determining multiple entries, normalizing sections, and logging.
    """
    # Determine if there are multiple table entries
    multiple_entries = len(listing_data['table_data']) > 1

    # Check for title_model_key with value "Model: Unknown Title" OR title_title_key is "Unknown Title"
    if (listing_data['title'].get('title_model_key', '') == 'Model: Unknown Title' or 
        (listing_data['title'].get('title_title_key', '') == 'Unknown Title' and 
         listing_data['title'].get('title_model_key', '') == 'Title')):
        item_number = listing_data.get('metadata', {}).get('meta_itemnumber_key', 'Unknown')
        error_log_dir = Path('compare_error_logs')
        error_log_dir.mkdir(exist_ok=True)
        log_file = error_log_dir / f"{item_number}.log"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("Error: title has 'Unknown Title' or title_model_key has value 'Model: Unknown Title'\n\n")
            f.write("Title & Metadata:\n")
            # Combine relevant sections for error context
            sections = {
                'TITLE DATA': listing_data.get('title', {}),
                'METADATA': listing_data.get('metadata', {}),
                'CATEGORY': listing_data.get('sections', {}).get('CATEGORY', []),
                'DESCRIPTION': listing_data.get('sections', {}).get('DESCRIPTION', [])
            }
            combined_data = combine_data_sections(sections)
            f.write(combined_data + "\n\n")
            f.write("Pull Log:\n")
            pull_log_content = extract_pull_log_content(item_number)
            f.write(pull_log_content + "\n\n")
            f.write("Process Log:\n")
            process_log_file = Path(PROCESSING_LOGS_DIR) / 'process_logs' / f"process_log_{item_number}.txt"
            if process_log_file.exists():
                try:
                    with open(process_log_file, 'r', encoding='utf-8') as pf:
                        f.write(pf.read())
                except UnicodeDecodeError as e:
                    if SUPPRESS_INVALID_START_BYTE:
                        logger.error(f"Failed to load {process_log_file}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
                        f.write("Error reading process log: invalid start byte\n")
                    else:
                        raise
            else:
                f.write("Process log file not found.\n")
        logger.info(f"Created error log for item {item_number} due to Unknown Title condition", extra={'session_id': current_session_id})

        # Construct a minimal result to skip comparisons - with no issues or summary text
        result = {
            'Issues': ([("", "    - No issues detected", "", "")], ""),  # Empty string for summary text
            'Misc Issues': [],  # Empty list for misc issues
            'Title vs. Specifics': [("", "    - Comparisons skipped", "", "")],
            'Title vs. Table Data': [("", "    - Comparisons skipped", "", "")],
            'Specifics vs. Table Data': [("", "    - Comparisons skipped", "", "")],
            'Title vs. Metadata': [("", "    - Comparisons skipped", "", "")],
            'Specs vs. Metadata': [("", "    - Comparisons skipped", "", "")],
            'Misc Comparison': "- Misc Comparison\n  - No issues detected",
            'Non-Matched Keys': [("", "    - Comparisons skipped", "", "")],
            'Combined Data': combined_data,
            'Specifics': "",
            'Table Data': "",
            'Details': "",
            'consolidated_title_vs_table': [],
            'consolidated_specs_vs_table': [],
            'Title vs. Specifics Issues': [],
            'Duplicate Issues': []
        }
        return multiple_entries, {}, {}, {}, {}, {}, {}, result, True  # Added error_flag=True

    # Normalize section data for consistent comparison
    title, specs, table, meta, description, table_meta = normalize_section_data(listing_data)

    # Log debugging information for analysis
    log_debugging_info(listing_data, title, specs, table, meta, description, table_meta)

    return multiple_entries, title, specs, table, meta, description, table_meta, None, False  # Added error_flag=False
    
# New function for Section Comparisons (originally lines 2607-2630)
def perform_section_comparisons(listing_data, sections, is_power_adapter, multiple_entries, title, specs, table):
    """
    Perform comparisons between sections using imported comparison functions.

    Args:
        listing_data (dict): Parsed listing data.
        sections (dict): Parsed sections like title, specifics, and table.
        is_power_adapter (bool): Flag for power adapter-specific logic.
        multiple_entries (bool): Indicates if the table has multiple entries.
        title (dict): Normalized title data.
        specs (dict): Normalized specifics data.
        table (dict): Normalized table data.

    Returns:
        tuple: 15 values including issues, comparison results, and consolidated mismatches.
    """
    title_vs_specs_issues = []
    title_vs_table_issues = []
    specs_vs_table_issues = []
    title_vs_meta_issues = []
    specs_vs_meta_issues = []
    title_vs_specs = []
    title_vs_table = []
    specs_vs_table = []
    title_vs_meta = []
    specs_vs_meta = []
    misc_info = ["- Misc Comparison"]
    misc_issues = []
    issue_strings = []

    # Title vs. Specifics - using imported function
    tvs_result, tvs_issues, tvs_issues_str = compare_title_vs_specifics(listing_data, sections, is_power_adapter)
    title_vs_specs.extend(tvs_result)
    title_vs_specs_issues.extend(tvs_issues)
    issue_strings.extend(tvs_issues_str)

    # Title vs. Table - using imported function
    tvt_result, tvt_issues, tvt_issues_str, consolidated_title_vs_table = compare_title_vs_table(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    title_vs_table.extend(tvt_result)
    title_vs_table_issues.extend(tvt_issues)
    issue_strings.extend(tvt_issues_str)

    # Specifics vs. Table - using imported function
    svt_result, svt_issues, svt_issues_str, consolidated_specs_vs_table = compare_specifics_vs_table(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    specs_vs_table.extend(svt_result)
    specs_vs_table_issues.extend(svt_issues)
    issue_strings.extend(svt_issues_str)

    # Title vs. Metadata - using imported function
    tvm_result, tvm_issues, tvm_issues_str = compare_title_vs_metadata(listing_data, sections, is_power_adapter)
    title_vs_meta.extend(tvm_result)
    title_vs_meta_issues.extend(tvm_issues)
    issue_strings.extend(tvm_issues_str)

    # Specifics vs. Metadata - using imported function
    svm_result, svm_issues, svm_issues_str = compare_specifics_vs_metadata(listing_data, sections, is_power_adapter)
    specs_vs_meta.extend(svm_result)
    specs_vs_meta_issues.extend(svm_issues)
    issue_strings.extend(svm_issues_str)

    # Miscellaneous Issues - this function stays in main file as it's complex and calls many other functions
    misc_info_result, misc_issues_result, misc_issues_str = check_misc_issues(
        listing_data, sections, is_power_adapter, multiple_entries
    )
    misc_info.extend(misc_info_result[1:])  # Exclude header
    misc_issues.extend(misc_issues_result)
    issue_strings.extend(misc_issues_str)

    return (
        title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues, title_vs_meta_issues, specs_vs_meta_issues,
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta,
        misc_info, misc_issues, issue_strings,
        consolidated_title_vs_table, consolidated_specs_vs_table
    )
    
def consolidate_numbered_keys(issue_strings):
    """
    Consolidate numbered keys (e.g., ram_size1, ram_size2) into single issue entries.
    
    Args:
        issue_strings (list): Current list of issue strings
        
    Returns:
        list: Consolidated issue strings
    """
    # Group issue strings by base key name
    base_key_groups = defaultdict(list)
    result_issues = []
    
    # First pass: identify and group numbered key issues
    for issue in issue_strings:
        # Match patterns like "Ram size1: Title has '74gb', Table has '[8gb, 8gb, 8gb, 8gb]'"
        match = re.match(r'^(\w+\s+\w+?)(\d+):\s+(.+)$', issue)
        if match:
            base_key, number, details = match.groups()
            base_key_groups[base_key.strip()].append((int(number), issue, details))
        else:
            # Not a numbered key issue, keep as is
            result_issues.append(issue)
    
    # Second pass: consolidate each group of numbered keys
    for base_key, issues in base_key_groups.items():
        # Only consolidate if there are multiple entries for this base key
        if len(issues) <= 1:
            for _, issue, _ in issues:
                result_issues.append(issue)
            continue
        
        # Sort by number
        issues.sort(key=lambda x: x[0])
        
        # Extract title and table values
        title_values = []
        table_values = set()
        
        for _, _, details in issues:
            # Extract values from the details portion
            matches = re.match(r"Title has '([^']+)', Table has '([^']+)'", details)
            if matches:
                title_val, table_val = matches.groups()
                title_values.append(title_val)
                if table_val not in table_values:
                    table_values.add(table_val)
        
        # Only consolidate if we successfully extracted values
        if title_values and table_values:
            title_combined = "/".join(title_values)
            table_combined = "/".join(table_values)
            
            # Create consolidated issue string with base key (no number)
            consolidated_issue = f"{base_key}: Title has '{title_combined}', Table has '{table_combined}'"
            result_issues.append(consolidated_issue)
        else:
            # If extraction failed, keep original issues
            for _, issue, _ in issues:
                result_issues.append(issue)
    
    return result_issues

# New function for Consolidation and Filtering (originally lines 2633-2646)
def consolidate_and_filter_issues(listing_data, title, specs, issue_strings):
    """
    Consolidate and filter issues using results from the main comparison functions.
    No longer performs independent comparisons - relies on the three main comparison functions.
    """
    # DISABLED: Process numbered keys since main comparison functions handle this
    # numbered_key_issues = process_numbered_keys(listing_data, title, specs, {}, issue_strings)
    numbered_key_issues = []  # Empty list - main comparison functions handle all cases
    
    # Simply use the issue_strings from the main comparison functions
    pairwise_issues = issue_strings
    
    # Combine the main comparison issues with numbered key issues for the summary
    summary_issues = pairwise_issues + numbered_key_issues
    
    # Return empty set for consolidated_keys since we're no longer doing independent consolidation
    consolidated_keys = set()
    
    logger.debug(f"consolidate_and_filter_issues: {len(pairwise_issues)} main issues + {len(numbered_key_issues)} numbered key issues = {len(summary_issues)} total", 
                extra={'session_id': current_session_id})
    
    return summary_issues, consolidated_keys
    
# New function for Non-Matched Keys and Issues Organization (originally lines 2649-2657)
def organize_non_matched_and_issues(listing_data, title, specs, table, title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues,
                                    title_vs_meta_issues, specs_vs_meta_issues, misc_issues, sections,
                                    title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta):
    """
    Identify non-matched keys, organize issues, and prepare combined data and logs.
    """
    # Identify and group keys not matched across sections
    non_matched = find_non_matched_keys(listing_data, title, specs)
    grouped_non_matched = group_non_matched_keys(non_matched)

    # Organize issues for structured output
    issues_content = organize_issues(
        title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues,
        title_vs_meta_issues, specs_vs_meta_issues, misc_issues
    )

    # Combine data sections for reference
    combined_data = combine_data_sections(sections)

    # Generate detailed log of comparisons
    details_log = generate_details_log(
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta, non_matched
    )

    return issues_content, grouped_non_matched, combined_data, details_log

def combine_data_sections(sections):
    """Combine data sections into a single string for the 'Combined Data' key."""
    return (
        "Title Data\n" + "\n".join(sections.get('TITLE DATA', [])) + "\n\n" +
        "Category\n" + "\n".join(sections.get('CATEGORY', [])) + "\n\n" +
        "Metadata\n" + "\n".join(sections.get('METADATA', [])) + "\n\n" +
        "Description\n" + "\n".join(sections.get('DESCRIPTION', []))
    )

# New function for Metadata and Duplicate Issues (originally lines 2660-2693)
def handle_metadata_and_duplicates(listing_data, meta):
    """
    Extract metadata, collect duplicate issues, and update looked_at_files.
    """
    # Extract metadata and table entry count for summary
    num_table_entries = len(listing_data.get('table_data', []))
    metadata = listing_data.get('metadata', {})
    # Retrieve raw SKU with fallback for key variations
    raw_sku = meta.get('customlabel_key', meta.get('custom_label_key', 'Unknown SKU'))
    cleaned_sku = extract_sku_parts(raw_sku)

    # Duplicate detection disabled: rely on zscrape/external systems.
    # Return no duplicate issues for now.
    try:
        logger.info("Duplicate detection disabled in runit; relying on zscrape", extra={'session_id': current_session_id})
    except Exception:
        pass
    return num_table_entries, metadata, meta, cleaned_sku, []
    
# New function for Summary and Result Construction (originally lines 2696-2718)
def finalize_comparison_result(summary_issues, misc_issues, duplicate_issues, metadata, meta, cleaned_sku, num_table_entries,
                               title, specs, table, issues_content, grouped_non_matched, combined_data, sections,
                               details_log, consolidated_title_vs_table, consolidated_specs_vs_table,
                               title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta, misc_info, file_path=None,
                               consolidated_title_vs_specs_issues=None):
    """
    Generate copyable summary text, construct result dictionary, and handle file operations.
    """
    logger.debug(f"DEBUG: summary_issues before generate_copyable_text: {summary_issues}", extra={'session_id': current_session_id})

    # Generate copyable summary text with full context for equivalence checking
    copyable_text = generate_copyable_text(
        summary_issues, misc_issues + duplicate_issues, metadata, meta, cleaned_sku, num_table_entries, title, specs, table
    )

    # Construct the final result dictionary, including consolidated mismatches and duplicate issues
    result = construct_result(
        issues_content, copyable_text, misc_issues, title_vs_specs, title_vs_table, specs_vs_table,
        title_vs_meta, specs_vs_meta, misc_info, grouped_non_matched, combined_data, sections, details_log,
        consolidated_title_vs_table, consolidated_specs_vs_table, consolidated_title_vs_specs_issues
    )
    result['Duplicate Issues'] = duplicate_issues  # Add for Issues tab sub-section

    # Handle file operations if a file path is provided
    global has_handled_file_operations
    if file_path and not has_handled_file_operations:
        handle_file_operations(file_path, metadata.get('meta_itemnumber_key', 'Unknown'), result)
        has_handled_file_operations = True

    return result
    
def compare_data(listing_data, sections, file_path=None):
    """
    Compare listing data across Title, Specifics, Table, and Metadata, and generate a summary.
    IMPORTANT: This function must ONLY use the passed listing_data parameter, no global variables!
    """
    # Make sure we're working with completely isolated data copies
    isolated_listing_data = {
        'title': dict(listing_data.get('title', {})),
        'specifics': dict(listing_data.get('specifics', {})),
        'table_shared': dict(listing_data.get('table_shared', {})),
        'table_data': [dict(entry) for entry in listing_data.get('table_data', [])],
        'table_metadata': dict(listing_data.get('table_metadata', {})),
        'metadata': dict(listing_data.get('metadata', {})),
        'description': dict(listing_data.get('description', {}))
    }
    
    isolated_sections = {k: list(v) for k, v in sections.items()}
    
    # Define exact match categories and keywords
    exact_match_categories = {''}
    category_keywords = {
        'memory (ram)': ['memory', 'ram'],
        'laptops & netbooks': ['laptop', 'netbook'],
        'desktops & all-in-one pcs': ['desktop', 'all-in-one'],
        # Add more categories as needed
    }
    
    # Initialize comparison data using ONLY isolated data
    item_number = isolated_listing_data.get('metadata', {}).get('meta_itemnumber_key', 'Unknown')
    logger.debug(f"Starting comparison for item {item_number} with isolated data", extra={'session_id': current_session_id})

    multiple_entries, title, specs, table, meta, description, table_meta, result, error_flag = initialize_comparison_data(isolated_listing_data)

    if error_flag:
        logger.debug(f"Aborting comparison for item {item_number} due to title_model_key='Model: Unknown Title'", extra={'session_id': current_session_id})
        return result

    # Determine if the item is a power adapter based on category
    is_power_adapter = False
    for line in isolated_sections.get('CATEGORY', []):
        if 'Power Adapter' in line or 'power adapter' in line:
            is_power_adapter = True
            break

    # Define helper functions for storage check
    def is_storage_not_included(data_dict):
        status = data_dict.get('storage_status', '').lower()
        terms = ['not included', 'no', 'none', 'n/a', 'no storage', 'not applicable']
        return any(term in status for term in terms)

    def has_storage_size(data_dict):
        size_keys = ['storage_capacity', 'ssd_capacity', 'hdd_capacity']
        for key in size_keys:
            if data_dict.get(key, '').strip():
                return True
        return False

    # Execute section comparisons with isolated data
    logger.debug("Performing section comparisons with isolated data", extra={'session_id': current_session_id})
    (
        title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues, title_vs_meta_issues, specs_vs_meta_issues,
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta,
        misc_info, misc_issues, issue_strings,
        consolidated_title_vs_table, consolidated_specs_vs_table
    ) = perform_section_comparisons(
        isolated_listing_data, isolated_sections, is_power_adapter, multiple_entries, title, specs, table
    )

    logger.debug(f"Title vs Specs: {len(title_vs_specs_issues)} issues, Title vs Table: {len(title_vs_table_issues)} issues, Specs vs Table: {len(specs_vs_table_issues)} issues", extra={'session_id': current_session_id})

    # Consolidate issues for title vs table and specifics vs table
    title_vs_table_issues = consolidate_issues(title_vs_table_issues)
    specs_vs_table_issues = consolidate_issues(specs_vs_table_issues)

    # Check for storage mismatch
    storage_issues = []
    if is_storage_not_included(title) and (has_storage_size(specs) or any(has_storage_size(entry) for entry in table)):
        storage_issues.append("Storage Mismatch: Title indicates no storage, but storage size is present in specifics or table")
    if is_storage_not_included(specs) and (has_storage_size(title) or any(has_storage_size(entry) for entry in table)):
        storage_issues.append("Storage Mismatch: Specifics indicate no storage, but storage size is present in title or table")
    misc_issues += storage_issues

    # Lot Detection
    device_type = title.get('device_type_key', '').strip()
    title_device_type = title.get('title_device_type_key', '').strip()
    # Memory: At user request, do not enforce lot amount in title for GPUs
    if (device_type.lower() not in ('cpus/processors', 'graphics/video cards') and
        title_device_type.lower() not in ('computer components & parts', 'cpus/processors', 'graphics/video cards') and
        len(table) > 1 and 'title_title_key' in title):
        title_text = title['title_title_key'].lower()
        if 'lot' not in title_text:
            message = "Lot amount missing in title when there are multiple table entries."
            misc_issues.append(message)

    # Final safeguard: remove lot-missing messages for processors and Computer Components & Parts
    dt_lower = device_type.lower()
    tdt_lower = title_device_type.lower()
    if (dt_lower in ('cpus/processors', 'computer components & parts', 'graphics/video cards') or
        tdt_lower in ('cpus/processors', 'computer components & parts', 'graphics/video cards')):
        lot_missing_msg = "Lot amount missing in title when there are multiple table entries."
        misc_issues = [m for m in misc_issues if lot_missing_msg not in str(m)]
        try:
            issue_strings = [s for s in issue_strings if lot_missing_msg not in str(s)]
        except NameError:
            pass

    # Check for Auction and Buy It Now price conflict
    if 'meta_listingprice_key' in isolated_listing_data['metadata'] and 'meta_meta_listing_buyitnow_price_key_key' in isolated_listing_data['metadata']:
        misc_issues.append("Listing cannot be an Auction and also have a Buy It Now price")

    # Check for Scheduled listing with BuyItNow type conflict
    if (isolated_listing_data['metadata'].get('meta_listing_location_key', '').lower() == 'scheduled' and 
        isolated_listing_data['metadata'].get('meta_listing_type_key', '').lower() == 'buyitnow'):
        misc_issues.append("Scheduled listing cannot have BuyItNow type")

    # Category vs Device Type check - TEMPORARILY DISABLED
    # leaf_category = None
    # for line in isolated_sections.get('CATEGORY', []):
    #     if '[leaf_category_key]' in line:
    #         parts = line.split(': ', 1)
    #         if len(parts) == 2:
    #             leaf_category = parts[1].strip().lower()
    #             break

    # device_type = title.get('device_type_key', '').lower().strip()

    # if leaf_category and device_type:
    #     logger.debug(f"Checking category vs device type: leaf_category='{leaf_category}', device_type='{device_type}'", extra={'session_id': current_session_id})
    #     if leaf_category in exact_match_categories:
    #         if device_type != leaf_category:
    #             misc_issues.append(f"Exact Category Mismatch: Leaf category '{leaf_category}' doesn't match Device Type '{device_type}'")
    #             logger.debug(f"Mismatch detected: Device type '{device_type}' does not exactly match category '{leaf_category}'", extra={'session_id': current_session_id})
    #         else:
    #             logger.debug(f"No mismatch: Device type '{device_type}' exactly matches category '{leaf_category}'", extra={'session_id': current_session_id})
    #     elif leaf_category in category_keywords:
    #         keywords = category_keywords[leaf_category]
    #         if not any(keyword.lower() in device_type for keyword in keywords):
    #             misc_issues.append(f"Category Mismatch: '{leaf_category}' vs Device Type '{device_type}' (no matching keywords)")
    #             logger.debug(f"Mismatch detected: Device type '{device_type}' does not contain required keywords for category '{leaf_category}'", extra={'session_id': current_session_id})
    #         else:
    #             logger.debug(f"No mismatch: Device type '{device_type}' contains required keywords for category '{leaf_category}'", extra={'session_id': current_session_id})
    #     else:
    #         logger.debug(f"No category check defined for '{leaf_category}'", extra={'session_id': current_session_id})

    # Ensure misc_issues is a list of strings before stripping
    misc_issues = [str(issue) for issue in misc_issues]
    # Format misc_issues
    formatted_misc_issues = [issue.strip() for issue in misc_issues]

    # Consolidate and filter issues
    logger.debug("Consolidating and filtering issues", extra={'session_id': current_session_id})
    summary_issues, consolidated_keys = consolidate_and_filter_issues(isolated_listing_data, title, specs, issue_strings)
    logger.debug(f"Consolidated issues: {len(summary_issues)}, Consolidated keys: {consolidated_keys}", extra={'session_id': current_session_id})

    # NEW: Consolidate numbered keys
    logger.debug("Consolidating numbered keys", extra={'session_id': current_session_id})
    summary_issues = consolidate_numbered_keys(summary_issues)
    logger.debug(f"After numbered key consolidation: {len(summary_issues)} summary issues", extra={'session_id': current_session_id})

    # Organize non-matched keys and issues
    logger.debug("Starting non-matched keys detection", extra={'session_id': current_session_id})
    issues_content, grouped_non_matched, combined_data, details_log = organize_non_matched_and_issues(
        isolated_listing_data, title, specs, table, title_vs_specs_issues, title_vs_table_issues, specs_vs_table_issues,
        title_vs_meta_issues, specs_vs_meta_issues, formatted_misc_issues, isolated_sections,
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta
    )
    logger.debug(f"Non-matched keys detection complete. Grouped non-matched: {grouped_non_matched}", extra={'session_id': current_session_id})

    # Handle metadata and duplicates
    logger.debug("Handling metadata and duplicates", extra={'session_id': current_session_id})
    num_table_entries, metadata, meta, cleaned_sku, duplicate_issues = handle_metadata_and_duplicates(isolated_listing_data, meta)
    logger.debug(f"Metadata processed. Table entries: {num_table_entries}, Duplicate issues: {len(duplicate_issues)}", extra={'session_id': current_session_id})

    # Finalize result
    logger.debug("Finalizing comparison result", extra={'session_id': current_session_id})
    result = finalize_comparison_result(
        summary_issues, formatted_misc_issues, duplicate_issues, metadata, meta, cleaned_sku, num_table_entries,
        title, specs, table, issues_content, grouped_non_matched, combined_data, isolated_sections,
        details_log, consolidated_title_vs_table, consolidated_specs_vs_table,
        title_vs_specs, title_vs_table, specs_vs_table, title_vs_meta, specs_vs_meta, misc_info, file_path
    )

    logger.debug(f"Comparison completed for item {item_number}. Total issues: {len(issues_content) if issues_content else 0}, Unmatched keys: {len(grouped_non_matched)}", extra={'session_id': current_session_id})
    return result
    
def consolidate_issues(issues):

    grouped = defaultdict(list)
    non_matching_issues = []

    for issue in issues:
        message = issue[1]
        # Updated regex to match both "Title has 'value', Table has 'value'" and "'value' in Title, 'value' in Table"
        match = re.match(r'([A-Za-z\s]+?)(\d+)?\s*:\s*(?:Title has \'(.*?)\',\s*Table has \'(.*?)\'|\'(.*?)\' in Title,\s*\'(.*?)\' in Table)', message)
        if match:
            base_key = match.group(1).strip()  # e.g., "Ram size"
            # Title and Table values are in different groups depending on format
            if match.group(3) and match.group(4):  # "has" format
                title_val = match.group(3)
                table_val = match.group(4)
            else:  # "in" format
                title_val = match.group(5)
                table_val = match.group(6)
            grouped[base_key].append((title_val, table_val))
        else:
            non_matching_issues.append(issue)

    consolidated = []
    for key, vals in grouped.items():
        if len(vals) > 1 and all(v[1] == vals[0][1] for v in vals):
            title_vals = [v[0] for v in vals]
            table_val = vals[0][1]
            message = f"{key}: Title has {', '.join([f"'{tv}'" for tv in title_vals])}, Table has '{table_val}'"
            consolidated.append(['≠', message, '', '', ''])
        else:
            for v in vals:
                message = f"{key}: Title has '{v[0]}', Table has '{v[1]}'"
                consolidated.append(['≠', message, '', '', ''])

    consolidated.extend(non_matching_issues)
    return consolidated
    
def has_issues(comparisons):
    # If no comparisons data exists, assume no issues
    if not comparisons:
        return False
    
    # Check the 'Issues' section
    issues_content = comparisons.get('Issues', [])
    if isinstance(issues_content, tuple):
        issues_content, copyable_text = issues_content  # Unpack tuple if necessary
    else:
        copyable_text = comparisons.get('Issues', [("", "")])[1] if isinstance(comparisons.get('Issues', []), tuple) else ""
    
    # Check if copyable_text contains any issues
    if copyable_text.strip():
        return True
    # If there is no message to send, do not treat as having issues
    return False
    
    # The logic below is no longer used because we return False when no message is present.
    # Keeping it for reference if behavior needs to be relaxed in the future.

# --- GUI Setup Functions ---
def apply_theme(style):
    """Apply the selected theme to the GUI."""
    global frame_bg, link_fg, text_bg, scrolled_bg, scrolled_fg, summary_bg, summary_fg, error_bg, error_fg
    if theme == 'dark':
        styles = dark_mode_styles
        style_maps = dark_mode_style_maps
        root_bg = "#333333"
        frame_bg = "#333333"
        link_fg = "cyan"
        text_bg = "#555555"
        scrolled_bg = "#444444"
        scrolled_fg = "white"
        summary_bg = "#555555"
        summary_fg = "#00FFFF"
        error_bg = "#555555"
        error_fg = "white"
    elif theme == 'neutral_gray_blue':
        styles = neutral_gray_blue_styles
        style_maps = neutral_gray_blue_style_maps
        root_bg = "#E2E8F0"
        frame_bg = "#A3BFFA"
        link_fg = "#2D3748"
        text_bg = "#A3BFFA"
        scrolled_bg = "#CBD5E0"
        scrolled_fg = "#2D3748"
        summary_bg = "#A3BFFA"
        summary_fg = "#2D3748"
        error_bg = "#A3BFFA"
        error_fg = "#2D3748"
    elif theme == 'dark_neutral_blue':
        styles = dark_neutral_blue_styles
        style_maps = dark_neutral_blue_style_maps
        root_bg = "#1A202C"
        frame_bg = "#2C3E50"
        link_fg = "#E2E8F0"
        text_bg = "#2C3E50"
        scrolled_bg = "#4A5568"
        scrolled_fg = "#E2E8F0"
        summary_bg = "#2C3E50"
        summary_fg = "#F0F8FF"
        error_bg = "#2C3E50"
        error_fg = "#E2E8F0"
    elif theme == 'xp_media_center_black':
        styles = xp_media_center_black_styles
        style_maps = xp_media_center_black_style_maps
        root_bg = "#003366"
        frame_bg = "#003366"
        link_fg = "#E0E0E0"
        text_bg = "#004488"
        scrolled_bg = "#004488"
        scrolled_fg = "#E0E0E0"
        summary_bg = "#004488"
        summary_fg = "#E0E0E0"
        error_bg = "#004488"
        error_fg = "#E0E0E0"
    else:
        styles = light_mode_styles
        style_maps = light_mode_style_maps
        root_bg = "SystemButtonFace"
        frame_bg = "#f0f0f0"
        link_fg = "blue"
        text_bg = "#e0e0e0"
        scrolled_bg = "#ffffff"
        scrolled_fg = "black"
        summary_bg = "#e0e0e0"
        summary_fg = "black"
        error_bg = "#e0e0e0"
        error_fg = "black"
    
    for widget, config in styles.items():
        style.configure(widget, **config)
    for widget, config in style_maps.items():
        style.map(widget, **config)
    root.configure(bg=root_bg)
    
    # Configure links with both foreground and background if they exist
    if 'ebay_link' in globals() and globals()['ebay_link'] is not None:
        globals()['ebay_link'].config(foreground=link_fg, background=frame_bg)
    if 'edit_link' in globals() and globals()['edit_link'] is not None:
        globals()['edit_link'].config(foreground=link_fg, background=frame_bg)

def display_debug_log(widget, item_number):
    widget.config(state='normal')
    widget.delete(1.0, tk.END)
    
    log_file = Path(PROCESSING_LOGS_DIR) / 'parser_debug.log'
    log_content = f"Debug Log Entries for Item {item_number}\n\n"
    
    try:
        if log_file.exists():
            with open(str(log_file), 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
                collecting = False
                section_content = []
                for line in lines:
                    if f"Processing file: item_contents\\{item_number}_description.txt" in line:
                        if collecting:
                            log_content += ''.join(section_content) + "\n---\n"
                        collecting = True
                        section_content = [line]
                    elif collecting and "Processing file: item_contents\\" in line:
                        collecting = False
                        log_content += ''.join(section_content) + "\n---\n"
                        section_content = []
                    elif collecting:
                        section_content.append(line)
                if collecting:
                    log_content += ''.join(section_content)
        else:
            log_content += "No log file found.\n"
    except Exception as e:
        log_content += f"Error reading log file: {str(e)}\n"
        logger.error(f"Error reading log file: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
    
    widget.insert(tk.END, log_content)
    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)

def create_issues_tab(frame):
    logger.debug("Creating Issues tab", extra={'session_id': current_session_id})
    tab_content = {}
    main_frame = ttk.Frame(frame)
    main_frame.pack(expand=True, fill='both', padx=5, pady=5)
    
    sections = [
        ("Title vs. Specifics Issues", "Title", "Specifics"),
        ("Title vs. Table Issues", "Title", "Table"),
        ("Specifics vs. Table Issues", "Specifics", "Table")
    ]
    
    # Create issues sections with smaller treeviews
    for i, (section, source1_name, source2_name) in enumerate(sections):
        section_frame = ttk.LabelFrame(main_frame, text=section, padding=5, style='TFrame')
        section_frame.grid(row=i, column=0, sticky='nsew', padx=5, pady=1)
        
        tree = ttk.Treeview(section_frame, columns=("Key", "Source1", "Symbol", "Source2"), 
                            show="headings", height=8)  # Use default Treeview style
        tree.heading("Key", text="Key", command=lambda c="Key": sort_tree(tree, c))
        tree.heading("Source1", text=source1_name)
        tree.heading("Symbol", text="")
        tree.heading("Source2", text=source2_name)
        tree.column("Key", width=300, anchor='w', stretch=True)
        tree.column("Source1", width=300, anchor='w', stretch=True)
        tree.column("Symbol", width=50, anchor='center', stretch=False)
        tree.column("Source2", width=300, anchor='w', stretch=True)
        
        scrollbar = ttk.Scrollbar(section_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side=tk.LEFT, expand=True, fill='both')
        scrollbar.pack(side=tk.RIGHT, fill='y')
        
        tab_content[section] = {'tree': tree, 'text_frame': ttk.Frame(section_frame)}
        tab_content[section]['text_frame'].pack(fill='x', pady=5)
    
    # Create a bottom frame to hold summary and error sections side by side
    bottom_frame = ttk.Frame(main_frame)
    bottom_frame.grid(row=len(sections), column=0, sticky='nsew', padx=5, pady=5)
    
    # Create summary frame with reduced width
    summary_frame = ttk.LabelFrame(bottom_frame, text="Copyable Summary", padding=5, style='TFrame')
    summary_frame.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)
    
    text_frame = ttk.Frame(summary_frame)
    text_frame.pack(expand=True, fill='both')
    text_widget = tk.Text(text_frame, height=12, width=80, wrap=tk.WORD, cursor="hand2")
    text_widget.pack(side=tk.LEFT, expand=True, fill='both')
    scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=text_widget.yview)
    text_widget.configure(yscrollcommand=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill='y')
    
    tab_content['summary_text'] = text_widget
    
    # Create error frame
    error_frame = ttk.LabelFrame(bottom_frame, text="Detected Errors", padding=5, style='TFrame')
    error_frame.grid(row=0, column=1, sticky='nsew', padx=5, pady=5)
    
    error_text = tk.Text(error_frame, height=12, width=40, wrap=tk.WORD, cursor="hand2")
    error_text.pack(side=tk.LEFT, expand=True, fill='both')
    error_scrollbar = ttk.Scrollbar(error_frame, orient="vertical", command=error_text.yview)
    error_text.configure(yscrollcommand=error_scrollbar.set)
    error_scrollbar.pack(side=tk.RIGHT, fill='y')
    
    tab_content['error_text'] = error_text
    
    # Configure bottom_frame columns
    bottom_frame.grid_columnconfigure(0, weight=2)  # Summary takes more space
    bottom_frame.grid_columnconfigure(1, weight=1)  # Errors take less space
    bottom_frame.grid_rowconfigure(0, weight=1)

    # AI Detected Issues frame (full width, below summary/errors)
    ai_frame = ttk.LabelFrame(main_frame, text="AI Detected Issues", padding=5, style='TFrame')
    ai_frame.grid(row=len(sections)+1, column=0, sticky='nsew', padx=5, pady=5)
    ai_text = scrolledtext.ScrolledText(ai_frame, height=10, wrap=tk.WORD)
    ai_text.pack(expand=True, fill='both')
    tab_content['ai_issues_text'] = ai_text
    
    # Configure grid row weights: less for issues, more for summary/errors
    for i in range(len(sections)):
        main_frame.grid_rowconfigure(i, weight=1)
    main_frame.grid_rowconfigure(len(sections), weight=1)
    
    main_frame.grid_columnconfigure(0, weight=1)
    
    logger.debug("Issues tab creation complete with updated layout", extra={'session_id': current_session_id})
    return tab_content

def extract_errors_for_item(item_number):
    log_files = [
        Path(PROCESSING_LOGS_DIR) / 'pull.txt',
        Path(PROCESSING_LOGS_DIR) / 'compare_log.txt', 
        Path(PROCESSING_LOGS_DIR) / 'parser_debug.txt',
        Path(PROCESSING_LOGS_DIR) / 'parser_debug.log'
    ]
    errors = []
    for log_file in log_files:
        if log_file.exists():
            with open(str(log_file), 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
                for line in lines:
                    if item_number in line and any(keyword in line.lower() for keyword in ['error', 'failed', 'exception']):
                        errors.append(f"[{log_file.name}] {line.strip()}")
    return errors

def open_revise_item():
    global item_number
    if item_number:
        url = f"https://www.ebay.com/lstng?mode=ReviseItem&itemId={item_number}&sr=wn"
        webbrowser.open(url)
    else:
        messagebox.showerror("Error", "No item number available.")

def open_ebay():
    global item_number
    if item_number:
        url = f"https://www.ebay.com/itm/{item_number}"
        webbrowser.open(url)
    else:
        messagebox.showerror("Error", "No item number available.")

def create_tab_content(frame, title):
    logger.debug(f"Creating content for tab: {title}", extra={'session_id': current_session_id})
    try:
        if title == 'Issues':
            return create_issues_tab(frame)
        elif title == '---':
            widgets = create_rule_management_interface(frame)
            return widgets
        elif title in ['Title vs. Specs', 'Title vs. Table', 'Specs vs. Table', 'Non Matched Keys']:
            # For tabs involving table comparisons, add a "Table Entry" column
            if title in ['Title vs. Table', 'Specs vs. Table']:
                widget = ttk.Treeview(frame, columns=("Key", "Source1", "Symbol", "Source2", "Table Entry"), show="headings", height=30)
                widget.heading("Key", text="Key", command=lambda c="Key": sort_tree(widget, c))
                widget.heading("Source1", text="Title" if title == 'Title vs. Table' else "Specifics")
                widget.heading("Symbol", text="")
                widget.heading("Source2", text="Table Value")
                widget.heading("Table Entry", text="Table Entry")
                widget.column("Key", width=200, anchor='w', stretch=True)
                widget.column("Source1", width=300, anchor='w', stretch=True)
                widget.column("Symbol", width=50, anchor='center', stretch=False)
                widget.column("Source2", width=300, anchor='w', stretch=True)
                widget.column("Table Entry", width=100, anchor='w', stretch=True)
            else:
                widget = ttk.Treeview(frame, columns=("Key", "Source1", "Symbol", "Source2"), show="headings", height=30)
                widget.heading("Key", text="Key", command=lambda c="Key": sort_tree(widget, c))
                if title == 'Title vs. Specs':
                    widget.heading("Source1", text="Title")
                    widget.heading("Source2", text="Specs")
                elif title == 'Non Matched Keys':
                    widget.heading("Source1", text="Value")
                    widget.heading("Source2", text="")
                widget.heading("Symbol", text="")
                widget.column("Key", width=300, anchor='w', stretch=True)
                widget.column("Source1", width=300, anchor='w', stretch=True)
                widget.column("Symbol", width=50, anchor='center', stretch=False)
                widget.column("Source2", width=300, anchor='w', stretch=True)
            scrollbar = ttk.Scrollbar(frame, orient="vertical", command=widget.yview)
            widget.configure(yscrollcommand=scrollbar.set)
            widget.pack(side=tk.LEFT, expand=True, fill='both')
            scrollbar.pack(side=tk.RIGHT, fill='y')
            logger.debug(f"Treeview created for {title}", extra={'session_id': current_session_id})
            return widget
        elif title == 'Process Log':
            # Create a frame for filter controls
            control_frame = ttk.Frame(frame)
            control_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
            
            # Add label and dropdown for extractor filter
            ttk.Label(control_frame, text="Filter by extractor:").pack(side=tk.LEFT, padx=(0, 5))
            extractor_var = tk.StringVar(value="All")
            extractor_dropdown = ttk.Combobox(control_frame, textvariable=extractor_var, 
                                              values=get_extractor_types(), 
                                              state="readonly", width=15)
            extractor_dropdown.pack(side=tk.LEFT, padx=(0, 5))
            
            # Create text widget for log content
            text_widget = scrolledtext.ScrolledText(frame, wrap=tk.WORD, width=120, height=38, font=("Arial", 10))
            text_widget.pack(expand=True, fill='both', padx=5, pady=5)
            
            # Bind the dropdown change event to update the log content
            def on_extractor_selected(event):
                global item_number
                if item_number:
                    display_process_log(text_widget, item_number, extractor_var.get())
            
            extractor_dropdown.bind('<<ComboboxSelected>>', on_extractor_selected)
            
            # Return a dictionary with all widgets
            return {
                'text_widget': text_widget,
                'extractor_var': extractor_var,
                'extractor_dropdown': extractor_dropdown
            }
        elif title == 'Supported Categories':
            return create_supported_categories_tab(frame)
        else:
            widget = scrolledtext.ScrolledText(frame, wrap=tk.WORD, width=120, height=40, font=("Arial", 10))
            widget.pack(expand=True, fill='both')
            logger.debug(f"ScrolledText created for {title}", extra={'session_id': current_session_id})
            return widget
    except Exception as e:
        logger.error(f"Error creating tab content for {title}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        raise

def copy_text(entry):
    """Copy selected text from the entry widget to the clipboard."""
    try:
        selected_text = entry.selection_get()
        pyperclip.copy(selected_text)
    except tk.TclError:
        pass  # No selection to copy

def paste_text(entry):
    """Paste text from the clipboard into the entry widget at the cursor position."""
    try:
        clipboard_content = root.clipboard_get()
        entry.insert(tk.INSERT, clipboard_content)
    except tk.TclError:
        pass  # No clipboard content to paste

def select_all(entry):
    """Select all text in the entry widget."""
    entry.select_range(0, tk.END)
    entry.icursor(tk.END)

def show_context_menu(event):
    """Display a context menu with Copy, Paste, and Select All options on right-click."""
    entry = event.widget
    context_menu = tk.Menu(root, tearoff=0)
    context_menu.add_command(label="Copy", command=lambda: copy_text(entry))
    context_menu.add_command(label="Paste", command=lambda: paste_text(entry))
    context_menu.add_command(label="Select All", command=lambda: select_all(entry))
    context_menu.post(event.x_root, event.y_root)

# Function to load ignore lists
def load_ignore_list(file_path):
    """Load the ignore list from a file into a set for efficient lookup."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return set(line.strip().lower() for line in f if line.strip())
        return set()
    except UnicodeDecodeError as e:
        if SUPPRESS_INVALID_START_BYTE:
            logger.error(f"Failed to load {file_path}: invalid start byte - {str(e)}", extra={'session_id': current_session_id})
            return set()
        else:
            raise

# Function to save duplicates to ignore lists
def save_to_ignore_list(file_path, duplicates):
    """Append duplicates to the ignore list file."""
    with open(file_path, 'a', encoding='utf-8') as f:
        for duplicate in duplicates:
            f.write(f"{duplicate}\n")

# Updated function to get and display duplicates content
def get_duplicates_content(duplicates_text):
    """Generate duplicates content, display it, and update ignore lists."""
    # Define file paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ebay_data_dir = os.path.join(script_dir, "eBayListingData")
    state_dir = os.path.join(script_dir, "state")
    os.makedirs(state_dir, exist_ok=True)
    active_file = os.path.join(ebay_data_dir, "duplicate_titles_active.txt")
    scheduled_file = os.path.join(ebay_data_dir, "duplicate_titles_scheduled.txt")
    # Prefer state/ paths; migrate legacy files if present
    ignore_active_file = os.path.join(state_dir, "_ignore_list_active.txt")
    ignore_scheduled_file = os.path.join(state_dir, "_ignore_list_scheduled.txt")
    legacy_ignore_active = os.path.join(script_dir, "_ignore_list_active.txt")
    legacy_ignore_scheduled = os.path.join(script_dir, "_ignore_list_scheduled.txt")
    try:
        if os.path.exists(legacy_ignore_active) and not os.path.exists(ignore_active_file):
            with open(legacy_ignore_active, 'r', encoding='utf-8') as fsrc, open(ignore_active_file, 'w', encoding='utf-8') as fdst:
                fdst.write(fsrc.read())
        if os.path.exists(legacy_ignore_scheduled) and not os.path.exists(ignore_scheduled_file):
            with open(legacy_ignore_scheduled, 'r', encoding='utf-8') as fsrc, open(ignore_scheduled_file, 'w', encoding='utf-8') as fdst:
                fdst.write(fsrc.read())
    except Exception:
        pass

    # Load existing ignore lists
    ignore_active = load_ignore_list(ignore_active_file)
    ignore_scheduled = load_ignore_list(ignore_scheduled_file)

    # Lists to store duplicates to display and later ignore
    active_duplicates = []
    scheduled_duplicates = []

    # Process active duplicates
    if os.path.exists(active_file):
        with open(active_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.lower() not in ignore_active:
                    active_duplicates.append(line)

    # Process scheduled duplicates
    if os.path.exists(scheduled_file):
        with open(scheduled_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.lower() not in ignore_scheduled:
                    scheduled_duplicates.append(line)

    # Build content to display
    content = ""
    if active_duplicates:
        content += "Active Duplicates:\n"
        content += "\n".join(active_duplicates)
        content += "\n\n"
    if scheduled_duplicates:
        content += "Scheduled Duplicates:\n"
        content += "\n".join(scheduled_duplicates)
        content += "\n\n"
    if not content:
        content = "No duplicates detected"

    # Update the duplicates_text widget
    duplicates_text.delete(1.0, tk.END)
    duplicates_text.insert(tk.END, content.strip())
    duplicates_text.config(state='disabled')

    # After displaying, add duplicates to ignore lists
    if active_duplicates:
        save_to_ignore_list(ignore_active_file, active_duplicates)
    if scheduled_duplicates:
        save_to_ignore_list(ignore_scheduled_file, scheduled_duplicates)

    return content

def bind_double_click():
    for tab_title in ['Title vs. Specs', 'Title vs. Table', 'Specs vs. Table']:
        widget = tab_contents[tab_title]
        widget.bind("<Double-1>", toggle_row_state)

def toggle_row_state(event):
    tree = event.widget
    item = tree.identify_row(event.y)
    if not item:
        return
    # Prefer the visible first column (typically 'Key') when using show='headings'
    columns = list(tree["columns"]) if "columns" in tree.keys() else []
    key = None
    if columns:
        # If there is an explicit 'Key' column, use it. Otherwise use first column
        if 'Key' in columns:
            key = tree.set(item, 'Key')
        else:
            key = tree.set(item, columns[0])
    if not key:
        key = tree.item(item, "text")
    if not key:
        return  # Skip if no key (e.g., default message)

    tab_title = notebook.tab(notebook.select(), "text")
    states = load_comparison_states()
    tab_states = states.get(tab_title, {})
    current_state = tab_states.get(key, 'default')
    next_state = {'default': 'blacklist', 'blacklist': 'whitelist', 'whitelist': 'default'}[current_state]
    tab_states[key] = next_state
    states[tab_title] = tab_states
    save_comparison_states(states)
    update_tab(tab_title, comparisons_cache[files[current_file_index]], files[current_file_index], bold_font, normal_font, misc_bold_font, misc_normal_font)

def save_comparison_states(states):
    states_file = "comparison_states.json"
    try:
        with open(states_file, 'w', encoding='utf-8') as f:
            json.dump(states, f, indent=4)  # Write JSON with indentation for readability
        logger.debug(f"Saved comparison states to {states_file}")
    except Exception as e:
        logger.error(f"Error saving comparison states: {str(e)}")

def load_comparison_states():
    states_file = "comparison_states.json"
    if os.path.exists(states_file):
        with open(states_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()  # Remove leading/trailing whitespace
            if content:  # Check if there's any content
                try:
                    data = json.loads(content)
                    # Ensure structure is {tab_title: {key: state}}
                    if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
                        return data
                    # Migrate from legacy structures if needed
                    migrated = {}
                    for k, v in (data.items() if isinstance(data, dict) else []):
                        if isinstance(k, str) and isinstance(v, str):
                            # Attempt to parse tuple-like string keys: "('Tab','Key')"
                            m = re.match(r"^\('(.+?)',\s*'(.+?)'\)$", k)
                            if m:
                                tab, key = m.groups()
                                migrated.setdefault(tab, {})[key] = v
                    return migrated
                except json.JSONDecodeError as e:
                    logger.error(f"Error loading comparison states: {str(e)}")
                    return {}
            else:
                logger.warning("Comparison states file is empty")
                return {}
    return {}

def get_row_state(tab_title, key):
    states = load_comparison_states()
    return states.get(tab_title, {}).get(key, 'default')

def save_matching_keys(matching_keys):
    matching_keys_file = "matching_keys.json"
    with open(matching_keys_file, 'w', encoding='utf-8') as f:
        json.dump(matching_keys, f, indent=4)

def load_matching_keys():
    matching_keys_file = "matching_keys.json"
    if os.path.exists(matching_keys_file):
        with open(matching_keys_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def load_key_mappings():
    mappings_file = "key_mappings.json"
    if os.path.exists(mappings_file):
        with open(mappings_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"mappings": []}

def save_key_mappings(mappings):
    mappings_file = "key_mappings.json"
    with open(mappings_file, 'w', encoding='utf-8') as f:
        json.dump(mappings, f, indent=4)

def _normalize_section_label(label: str) -> str:
    s = (label or '').strip().lower()
    if s.startswith('title'):
        return 'title'
    if s.startswith('specific'):
        return 'specifics'
    if s.startswith('table'):
        return 'table'
    return s

def add_to_matching_keys(tree):
    selected_items = tree.selection()
    if len(selected_items) != 2:
        messagebox.showinfo("Selection Error", "Please select exactly two keys from different sections.")
        return
    
    item1 = selected_items[0]
    item2 = selected_items[1]
    
    # Determine the current tab
    current_tab = notebook.tab(notebook.select(), "text")
    
    if current_tab == "Non Matched Keys":
        # For "Unmatched Keys" tab, section is in the "Section" column
        section1 = _normalize_section_label(tree.set(item1, "Section"))
        section2 = _normalize_section_label(tree.set(item2, "Section"))
        key1 = tree.set(item1, "Key")
        key2 = tree.set(item2, "Key")
    else:
        # Only supported from the 'Non Matched Keys' tab to avoid ambiguity in comparison tabs
        messagebox.showinfo("Selection Error", "Please add mappings from the 'Non Matched Keys' tab.")
        return
    
    if section1 == section2:
        messagebox.showinfo(
            "Selection Error",
            f"Detected same section '{section1}'. Please select one row from Title and one from Table/Specifics in the 'Section' column."
        )
        return
    
    mapping = {
        "section1": section1,
        "key1": key1,
        "section2": section2,
        "key2": key2
    }
    
    key_mappings = load_key_mappings()
    if "mappings" not in key_mappings:
        key_mappings["mappings"] = []
    key_mappings["mappings"].append(mapping)
    save_key_mappings(key_mappings)
    messagebox.showinfo("Success", f"Added mapping: {section1}.{key1} <-> {section2}.{key2}")

# Helper function to check if a key is unmatched
def is_unmatched(section, key, other_sections, key_mappings):
    for other_section in other_sections:
        if key in other_section:
            return False  # Matched directly
        for mapping in key_mappings:
            if (mapping["section1"] == section and mapping["key1"] == key and 
                mapping["section2"] == other_section.name and mapping["key2"] in other_section) or \
               (mapping["section2"] == section and mapping["key2"] == key and 
                mapping["section1"] == other_section.name and mapping["key1"] in other_section):
                return False  # Matched through mapping
    return True

def create_non_matched_keys_tab(frame):
    widget = ttk.Treeview(frame, columns=("Section", "Key", "Value"), show="headings", height=30)
    widget.heading("Section", text="Section")
    widget.heading("Key", text="Key")
    widget.heading("Value", text="Value")
    widget.column("Section", width=100, anchor='w')
    widget.column("Key", width=200, anchor='w')
    widget.column("Value", width=400, anchor='w')
    scrollbar = ttk.Scrollbar(frame, orient="vertical", command=widget.yview)
    widget.configure(yscrollcommand=scrollbar.set)
    widget.pack(side=tk.LEFT, expand=True, fill='both')
    scrollbar.pack(side=tk.RIGHT, fill='y')
    
    # Add "Add to Matching Keys" button
    add_button = ttk.Button(frame, text="Add to Matching Keys", command=lambda: add_to_matching_keys(widget))
    add_button.pack(side=tk.BOTTOM, pady=5)
    
    return widget

def load_supported_leaf_categories_gui():
    try:
        cfg_path = Path(CONFIGS_DIR) / 'supported_leaf_categories.json'
        if cfg_path.exists():
            with open(cfg_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list) and all(isinstance(x, str) for x in data):
                return data
        return [
            'Computer Servers',
            'PC Laptops & Netbooks',
            'PC Desktops & All-In-Ones',
            'Apple Desktops & All-In-Ones',
            'Apple Laptops',
            'CPUs/Processors'
        ]
    except Exception as e:
        logger.error(f"GUI: Failed to load supported categories: {e}", extra={'session_id': current_session_id})
        return []

def save_supported_leaf_categories_gui(categories):
    try:
        cfg_path = Path(CONFIGS_DIR) / 'supported_leaf_categories.json'
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg_path, 'w', encoding='utf-8') as f:
            json.dump(categories, f, indent=2)
        logger.debug("GUI: Saved supported categories", extra={'session_id': current_session_id})
        messagebox.showinfo("Saved", "Supported categories updated.")
    except Exception as e:
        logger.error(f"GUI: Failed to save supported categories: {e}", extra={'session_id': current_session_id})
        messagebox.showerror("Error", f"Failed to save: {e}")

def create_supported_categories_tab(frame):
    container = ttk.Frame(frame)
    container.pack(expand=True, fill='both', padx=8, pady=8)

    header = ttk.Label(container, text="Supported eBay Leaf Categories (used for device-type check)", font=("Arial", 11, "bold"))
    header.pack(anchor='w', pady=(0, 6))

    list_frame = ttk.Frame(container)
    list_frame.pack(side='left', fill='both', expand=True)

    scrollbar = ttk.Scrollbar(list_frame)
    scrollbar.pack(side='right', fill='y')

    listbox = tk.Listbox(list_frame, height=18)
    listbox.pack(side='left', fill='both', expand=True)
    listbox.config(yscrollcommand=scrollbar.set)
    scrollbar.config(command=listbox.yview)

    # Populate
    for cat in load_supported_leaf_categories_gui():
        listbox.insert(tk.END, cat)

    # Controls
    controls = ttk.Frame(container)
    controls.pack(side='left', padx=8, fill='y')

    entry = ttk.Entry(controls, width=32)
    entry.pack(pady=(0, 6))

    def add_category():
        val = entry.get().strip()
        if not val:
            return
        existing = [listbox.get(i) for i in range(listbox.size())]
        if val in existing:
            messagebox.showinfo("Info", "Category already exists.")
            return
        listbox.insert(tk.END, val)
        entry.delete(0, tk.END)

    def remove_selected():
        sel = listbox.curselection()
        for idx in reversed(sel):
            listbox.delete(idx)

    def move(delta):
        sel = listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        new_idx = idx + delta
        if new_idx < 0 or new_idx >= listbox.size():
            return
        val = listbox.get(idx)
        listbox.delete(idx)
        listbox.insert(new_idx, val)
        listbox.selection_set(new_idx)

    def save_categories():
        cats = [listbox.get(i) for i in range(listbox.size())]
        save_supported_leaf_categories_gui(cats)

    ttk.Button(controls, text="Add", command=add_category).pack(fill='x', pady=2)
    ttk.Button(controls, text="Remove", command=remove_selected).pack(fill='x', pady=2)
    ttk.Button(controls, text="Up", command=lambda: move(-1)).pack(fill='x', pady=2)
    ttk.Button(controls, text="Down", command=lambda: move(1)).pack(fill='x', pady=2)
    ttk.Button(controls, text="Save", command=save_categories).pack(fill='x', pady=6)

    help_lbl = ttk.Label(controls, text="Tip: This list controls which leaf categories run the device-type mismatch check.", wraplength=260)
    help_lbl.pack(fill='x', pady=(8,0))

    return {'listbox': listbox, 'entry': entry}

def create_equivalence_tab(frame):
    container = ttk.Frame(frame)
    container.pack(expand=True, fill='both', padx=8, pady=8)

    # Top controls: add rule button
    controls = ttk.Frame(container)
    controls.pack(fill='x', pady=(0, 8))

    add_btn = ttk.Button(controls, text="Add Selected Equivalence")
    add_btn.pack(side='left')

    # Mismatch picker: a Treeview listing all mismatches from current comparisons
    picker_frame = ttk.Frame(container)
    picker_frame.pack(fill='both', expand=True)

    columns = ("Key", "Value A", "Value B", "Source")
    mismatch_tree = ttk.Treeview(picker_frame, columns=columns, show='headings', height=12)
    for col in columns:
        mismatch_tree.heading(col, text=col)
        mismatch_tree.column(col, width=180 if col != 'Source' else 120, anchor='w')
    mismatch_tree.pack(side='left', fill='both', expand=True)
    scroll = ttk.Scrollbar(picker_frame, command=mismatch_tree.yview)
    mismatch_tree.configure(yscrollcommand=scroll.set)
    scroll.pack(side='right', fill='y')

    def populate_mismatches_from_comparisons():
        try:
            mismatch_tree.delete(*mismatch_tree.get_children())
            comp = comparisons_cache.get(files[current_file_index]) or comparisons_cache.get(str(files[current_file_index]))
            if not comp:
                return
            # Collect rows from all three comparison lists
            sources = [
                ('Title vs. Specifics', comp.get('Title vs. Specifics') or []),
                ('Title vs. Table Data', comp.get('Title vs. Table Data') or []),
                ('Specifics vs. Table Data', comp.get('Specifics vs. Table Data') or []),
            ]
            seen = set()
            for source_name, rows in sources:
                for row in rows:
                    if not isinstance(row, (list, tuple)) or len(row) < 4:
                        continue
                    key_disp = str(row[0]).strip()
                    val_a = str(row[1]).strip()
                    val_b = str(row[3]).strip()
                    base_key = key_disp.lower().replace(' ', '_')
                    if not base_key.endswith('_key'):
                        base_key = f"{base_key}_key"
                    # Only include mismatches: look for the mismatch symbol or differing strings
                    is_mismatch = ('≠' in row[2]) or (val_a and val_b and val_a.lower() != val_b.lower())
                    if not is_mismatch:
                        continue
                    sig = (base_key, val_a, val_b, source_name)
                    if sig in seen:
                        continue
                    seen.add(sig)
                    mismatch_tree.insert('', 'end', values=(base_key, val_a, val_b, source_name))
        except Exception as e:
            logger.error(f"Equivalence tab: failed to populate mismatches: {e}", extra={'session_id': current_session_id})

    def add_selected_equivalence():
        sel = mismatch_tree.selection()
        if not sel:
            messagebox.showinfo("Select", "Select a mismatch row to add equivalence.")
            return
        key_name, v1, v2, _src = mismatch_tree.item(sel[0], 'values')
        key_name = (key_name or '').strip()
        v1 = (v1 or '').strip()
        v2 = (v2 or '').strip()
        if not key_name or not v1 or not v2:
            messagebox.showinfo("Invalid", "Row missing values.")
            return
        norm1 = v1.replace("'", "\\'")
        norm2 = v2.replace("'", "\\'")
        rule = (
            "lambda v1, v2, **ctx: v1 is not None and v2 is not None and ("
            f"v1.strip().lower()==v2.strip().lower() or set([v1.strip().lower(), v2.strip().lower()])<=set(['{norm1.lower()}','{norm2.lower()}']))"
        )
        try:
            if key_name not in equivalence_rules:
                equivalence_rules[key_name] = []
            if rule not in equivalence_rules[key_name]:
                equivalence_rules[key_name].append(rule)
                save_equivalence_rules(equivalence_rules)
            messagebox.showinfo("Added", f"Equivalence added for {key_name}: '{v1}' == '{v2}'")
        except Exception as e:
            logger.error(f"Equivalence tab: failed to add selected rule: {e}", extra={'session_id': current_session_id})
            messagebox.showerror("Error", str(e))

    add_btn.config(command=add_selected_equivalence)

    # Existing rules table
    rules_frame = ttk.Frame(container)
    rules_frame.pack(fill='both', expand=True, pady=(8, 0))
    rules_tree = ttk.Treeview(rules_frame, columns=("Rule",), show='headings', height=6)
    rules_tree.heading("Rule", text="Rule")
    rules_tree.pack(side='left', fill='both', expand=True)
    rules_scroll = ttk.Scrollbar(rules_frame, command=rules_tree.yview)
    rules_tree.configure(yscrollcommand=rules_scroll.set)
    rules_scroll.pack(side='right', fill='y')

    def refresh_keys_and_rules():
        try:
            # Load rules for the current selection's key if any; otherwise leave empty
            rules_tree.delete(*rules_tree.get_children())
            sel = mismatch_tree.selection()
            k = ''
            if sel:
                k = mismatch_tree.item(sel[0], 'values')[0]
            for r in equivalence_rules.get(k, []):
                rules_tree.insert('', 'end', values=(r,))
        except Exception as e:
            logger.error(f"Equivalence tab: failed to refresh: {e}", extra={'session_id': current_session_id})

    mismatch_tree.bind('<<TreeviewSelect>>', lambda e: refresh_keys_and_rules())

    # Also refresh equivalence tab when selected from the notebook
    def _refresh_on_select(event=None):
        try:
            # Reuse update_tab path which now handles 'Equivalence'
            selected = notebook.select()
            title = notebook.tab(selected, 'text')
            if title == 'Equivalence':
                # Trigger an explicit update for the equivalence tab
                # Using a minimal comparisons/context since it only needs parsed data
                update_tab('Equivalence', {}, files[current_file_index], bold_font, normal_font, misc_bold_font, misc_normal_font)
        except Exception:
            pass

    notebook.bind('<<NotebookTabChanged>>', _refresh_on_select, add='+')

    def add_equivalence():
        # Compatibility wrapper now uses selected mismatch row
        add_selected_equivalence()

    add_btn = ttk.Button(controls, text="Add Equivalence (Selected)", command=add_selected_equivalence)
    add_btn.pack(side='left')

    # Populate mismatches on init
    populate_mismatches_from_comparisons()

    return {
        'mismatch_tree': mismatch_tree,
        'rules_tree': rules_tree,
    }

def refresh_equivalence_tab(file_path):
    """Populate the Equivalence tab controls from the current item's data."""
    try:
        widgets = tab_contents.get('Equivalence')
        if not isinstance(widgets, dict) or 'key_combo' not in widgets:
            return
        # Reuse the update_tab branch for Equivalence to avoid duplicate logic
        update_tab('Equivalence', {}, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font)
    except Exception as e:
        logger.error(f"Equivalence tab explicit refresh failed: {e}", extra={'session_id': current_session_id})

def setup_gui():
    global root, tabs, right_panel, notebook, show_all_var, show_unseen_issues_var, category_filter_var, file_label, description_label, ebay_link, tab_contents
    logger.debug("Setting up GUI", extra={'session_id': current_session_id})
    try:
        style = ttk.Style()
        style.theme_use('clam')
        apply_theme(style)
        
        # Main frame setup
        main_frame = ttk.Frame(root)
        main_frame.pack(expand=True, fill='both', padx=5, pady=5)

        # Center panel
        center_panel = ttk.Frame(main_frame)
        center_panel.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)

        # Right panel with increased width (20% larger: from 200 to 240 pixels)
        right_panel = ttk.Frame(main_frame, width=440)
        right_panel.grid(row=0, column=1, sticky='nsew', padx=5)

        # Configure main_frame grid to respect the right panel's width
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)  # Center panel takes extra space
        main_frame.grid_columnconfigure(1, weight=0, minsize=440)  # Right panel fixed at 240 pixels

        # Notebook setup
        notebook = ttk.Notebook(center_panel)
        notebook.pack(expand=True, fill='both', padx=5, pady=5)
        
        tab_titles = [
            'Issues', 'Title vs. Specs', 'Title vs. Table',
            'Specs vs. Table', 'Misc', 'Non Matched Keys', 'Equivalence',
            '---', 'Title & Metadata', 'Specifics', 'Table', 'Rules', 'Supported Categories',
            'Pull Log', 'Compare Log', 'Process Log', 'Details'
        ]
        
        tab_contents = {}
        
        for title in tab_titles:
            logger.debug(f"Creating tab: {title}", extra={'session_id': current_session_id})
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=title)
            tabs[title] = frame
            if title == 'Issues':
                tab_contents[title] = create_issues_tab(frame)
            elif title == 'Non Matched Keys':
                tab_contents[title] = create_non_matched_keys_tab(frame)
            elif title == 'Equivalence':
                tab_contents[title] = create_equivalence_tab(frame)
            elif title in ['Title vs. Specs', 'Title vs. Table', 'Specs vs. Table']:
                tab_contents[title] = create_tab_content(frame, title)
            elif title == 'Supported Categories':
                tab_contents[title] = create_supported_categories_tab(frame)
            else:
                tab_contents[title] = None
        
        # **Misc Issues frame**
        misc_frame = ttk.LabelFrame(right_panel, text="Misc Issues", padding=10, style='TFrame')
        misc_text = scrolledtext.ScrolledText(misc_frame, wrap=tk.WORD, width=40, height=5, font=("Arial", 8))
        misc_text.pack(expand=True, fill='both')
        tab_contents['Issues']['misc_issues'] = misc_text  # Update reference to new widget

        # **Duplicates frame**
        duplicates_frame = ttk.LabelFrame(right_panel, text="Duplicates", padding=10, style='TFrame')
        duplicates_text = scrolledtext.ScrolledText(duplicates_frame, wrap=tk.WORD, width=40, height=5, font=("Arial", 8))
        duplicates_text.pack(expand=True, fill='both')
        duplicates_content = get_duplicates_content(duplicates_text)  # Call top-level function
        duplicates_text.insert(tk.END, duplicates_content)
        duplicates_text.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
        duplicates_text.bind("<Button-1>", lambda e, tw=duplicates_text: copy_to_clipboard(tw))

        # **Control frame**
        control_frame = ttk.Frame(right_panel)
        
        # **Configure right_panel grid**
        right_panel.grid_rowconfigure(0, weight=6)  # Misc Issues gets 60% of expandable space
        right_panel.grid_rowconfigure(1, weight=4)  # Duplicates gets 40% of expandable space
        right_panel.grid_rowconfigure(2, weight=0)  # Control frame takes natural size
        right_panel.grid_columnconfigure(0, weight=1)  # Allow horizontal expansion

        # **Place frames in grid**
        misc_frame.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)
        duplicates_frame.grid(row=1, column=0, sticky='nsew', padx=5, pady=5)
        control_frame.grid(row=2, column=0, sticky='ew', padx=5, pady=5)

        # Control frame widgets
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(side='top', padx=2, pady=2, fill='x')
        
        ttk.Button(button_frame, text="Previous", command=load_previous, width=10).pack(side='left', padx=2, pady=2)
        next_button = ttk.Button(button_frame, text="Next", command=load_next, width=10)
        next_button.pack(side='left', padx=2, pady=2)
        next_button.bind('<Return>', lambda event: load_next())
        ttk.Button(button_frame, text="Report", command=generate_report, width=10).pack(side='left', padx=2, pady=2)
        ttk.Button(button_frame, text="Special", command=save_special_description, width=10).pack(side='left', padx=2, pady=2)
        root.bind('<Control-r>', lambda event: generate_report())
        
        ttk.Button(control_frame, text="Cycle Theme", command=cycle_theme, width=20).pack(side='top', padx=2, pady=2)

        show_all_var = tk.BooleanVar(value=show_all)
        ttk.Checkbutton(control_frame, text="Show All Files", variable=show_all_var, 
                        command=toggle_show_all).pack(side='top', padx=2, pady=2)

        show_unseen_issues_var = tk.BooleanVar(value=show_unseen_issues)
        ttk.Checkbutton(control_frame, text="Show Unseen Issues Only", variable=show_unseen_issues_var, 
                        command=toggle_show_unseen_issues).pack(side='top', padx=2, pady=2)
        
        category_frame = ttk.LabelFrame(control_frame, text="Category Filter", padding=5)
        category_frame.pack(side='top', padx=2, pady=5, fill='x')
        
        category_filter_var = tk.IntVar(value=category_filter)
        ttk.Radiobutton(category_frame, text="All Categories", variable=category_filter_var, 
                        value=0, command=lambda: set_category_filter(0)).pack(anchor='w')
        ttk.Radiobutton(category_frame, text="Laptops/PC Only", variable=category_filter_var, 
                        value=1, command=lambda: set_category_filter(1)).pack(anchor='w')
        ttk.Radiobutton(category_frame, text="Non-Laptops/PC Only", variable=category_filter_var, 
                        value=2, command=lambda: set_category_filter(2)).pack(anchor='w')

        file_label = ttk.Label(control_frame, text="", cursor="hand2", font=("Arial", 10))
        file_label.pack(side='top', padx=2, pady=2, anchor='w')
        file_label.bind("<Button-1>", lambda e: open_file("parsed"))
        globals()['file_label'] = file_label

        search_frame = ttk.LabelFrame(control_frame, text="Quick Search", padding=5)
        search_frame.pack(side='top', padx=2, pady=5, fill='x')

        ttk.Label(search_frame, text="Search (SKU, Item #, Category, Title):").pack(anchor='w')
        search_entry = ttk.Entry(search_frame, textvariable=search_var, width=20)
        search_entry.pack(side='top', padx=2, pady=2, fill='x')
        search_entry.bind("<KeyRelease>", schedule_search)
        search_entry.bind("<Button-3>", show_context_menu)

        buttons_frame = ttk.Frame(search_frame)
        buttons_frame.pack(side='top', fill='x', pady=5)

        ttk.Button(buttons_frame, text="Clear Search", command=lambda: [clear_search(), search_entry.focus_set()]).pack(side='left', expand=True, fill='x', padx=1)

        description_label = ttk.Label(control_frame, text="Open Description", cursor="hand2", font=("Arial", 10))
        description_label.pack(side='top', padx=2, pady=2, anchor='w')
        description_label.bind("<Button-1>", open_description_file)
        globals()['description_label'] = description_label
        
        # Links frame
        links_frame = ttk.Frame(control_frame)
        links_frame.pack(side='top', padx=2, pady=2, anchor='w')

        ebay_link = tk.Label(links_frame, text="View on eBay", 
                             foreground=link_fg, 
                             background=frame_bg, 
                             cursor="hand2", font=("Arial", 10, "underline"))
        ebay_link.pack(side='left', padx=2)
        ebay_link.bind("<Button-1>", lambda e: open_ebay())
        globals()['ebay_link'] = ebay_link

        edit_link = tk.Label(links_frame, text="Edit", 
                             foreground=link_fg, 
                             background=frame_bg, 
                             cursor="hand2", font=("Arial", 10, "underline"))
        edit_link.pack(side='left', padx=2)
        edit_link.bind("<Button-1>", lambda e: open_revise_item())
        globals()['edit_link'] = ebay_link
        
        logger.debug("GUI setup complete with lazy loading tabs", extra={'session_id': current_session_id})
        
        # Bind double-click event for comparison tabs after GUI setup
        bind_double_click()
    except Exception as e:
        logger.error(f"Error in setup_gui: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        raise

def update_summary_text():
    """Update the styling of the Copyable Summary text widget based on the current theme."""
    if 'Issues' not in tab_contents or tab_contents['Issues'] is None:
        return
    
    text_widget = tab_contents['Issues']['summary_text']
    current_content = text_widget.get("1.0", tk.END).strip()
    
    # Use global theme variables for consistent styling
    text_widget.config(bg=summary_bg, fg=summary_fg)
    
    # Ensure the widget remains in the correct state
    text_widget.config(state='normal', relief='flat', borderwidth=1)
    text_widget.delete(1.0, tk.END)
    text_widget.insert(tk.END, current_content)
    text_widget.config(state='normal')  # Keep it clickable for copying
    text_widget.bind("<Button-1>", lambda e, tw=text_widget: copy_to_clipboard(tw))
    logger.debug(f"Updated Copyable Summary styling for theme: {theme}", extra={'session_id': current_session_id})

def update_tab(tab_title, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font):
    """Update the content of a specific tab."""
    if tab_title == 'Issues':
        # Update the main components of the 'Issues' tab
        text_widget = tab_contents[tab_title]['summary_text']
        content, copyable_text_with_url = comparisons.get(tab_title) if isinstance(comparisons.get(tab_title), tuple) else (comparisons.get(tab_title), "")
        text_widget.delete(1.0, tk.END)
        text_widget.insert(tk.END, copyable_text_with_url)
        # Use global theme variables for summary text
        text_widget.config(bg=summary_bg, fg=summary_fg)
        text_widget.config(state='normal', relief='flat', borderwidth=1)
        text_widget.bind("<Button-1>", lambda e, tw=text_widget: copy_to_clipboard(tw))
        
        # Update error text widget
        error_widget = tab_contents[tab_title]['error_text']
        error_widget.config(state='normal')
        error_widget.delete(1.0, tk.END)
        errors = extract_errors_for_item(item_number)
        if errors:
            error_widget.insert(tk.END, "\n".join(errors))
        else:
            error_widget.insert(tk.END, "No errors detected")
        error_widget.config(state='disabled', bg=error_bg, fg=error_fg)
        error_widget.bind("<Button-1>", lambda e, tw=error_widget: copy_to_clipboard(tw))
        
        # Updated to handle ScrolledText instead of Treeview
        misc_widget = tab_contents[tab_title]['misc_issues']
        misc_widget.config(state='normal')
        misc_widget.delete(1.0, tk.END)
        misc_content = comparisons.get('Misc Issues', [])
        if misc_content:
            for issue in misc_content:
                cleaned_issue = issue[0].strip() if isinstance(issue, tuple) else issue.strip()
                misc_widget.insert(tk.END, f"{cleaned_issue}\n")
        else:
            misc_widget.insert(tk.END, "No misc issues detected\n")
        misc_widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)

        # Update AI Detected Issues from live file if present
        try:
            ai_widget = tab_contents[tab_title].get('ai_issues_text')
            if ai_widget is not None:
                ai_widget.config(state='normal')
                ai_widget.delete(1.0, tk.END)
                live_dir = Path('training') / 'live_issues'
                live_file = live_dir / f"{item_number}.txt"
                if live_file.exists():
                    ai_text = live_file.read_text(encoding='utf-8', errors='replace')
                    ai_widget.insert(tk.END, ai_text)
                else:
                    ai_widget.insert(tk.END, "No AI issues found for this item.")
                ai_widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
        except Exception as e:
            logger.error(f"Failed to load AI issues for item {item_number}: {e}", extra={'session_id': current_session_id})

        # Update the sub-sections within the 'Issues' tab
        sub_sections = [
            ("Title vs. Specifics Issues", 'consolidated_title_vs_specs_issues'),
            ("Title vs. Table Issues", 'consolidated_title_vs_table'),
            ("Specifics vs. Table Issues", 'consolidated_specs_vs_table')
        ]
        for section, comparison_key in sub_sections:
            widget = tab_contents['Issues'][section]['tree']
            for item in widget.get_children():
                widget.delete(item)
            content = comparisons.get(comparison_key, [])
            if content:
                for row in content:
                    cleaned_row = [val.strip() if isinstance(val, str) else val for val in row]
                    widget.insert("", tk.END, values=cleaned_row, tags=("bold",))
            else:
                # Determine the appropriate message based on the section
                if comparison_key == 'consolidated_title_vs_specs_issues':
                    message = "No title vs. specifics issues detected"
                elif comparison_key == 'consolidated_title_vs_table':
                    message = "No title vs. table issues detected"
                elif comparison_key == 'consolidated_specs_vs_table':
                    message = "No specifics vs. table issues detected"
                widget.insert("", tk.END, values=("", message, "", ""), tags=("normal",))
            widget.tag_configure("bold", font=bold_font)
            widget.tag_configure("normal", font=normal_font)
    elif tab_title == '---':
        if tab_contents[tab_title] is not None:
            widgets = tab_contents[tab_title]
            rule_listbox = widgets['rule_listbox']
            tree = widgets['tree']

            # Refresh the listbox with rule types
            rule_listbox.delete(0, tk.END)
            for rt in rule_types:
                rule_listbox.insert(tk.END, rt.name)

            # Refresh the treeview with comparison types and rules
            tree.delete(*tree.get_children())
            comparison_types = ['title', 'specifics', 'metadata', 'description']
            keys = set()
            for file_path in all_files:
                if file_path in parsed_data:
                    listing_data, _ = parsed_data[file_path]
                    for section in comparison_types:
                        for key in listing_data.get(section, {}):
                            if '_' in key:
                                keys.add((section, '_'.join(key.split('_')[1:])))
            
            for ctype in comparison_types:
                parent = tree.insert("", "end", text=ctype.capitalize(), open=True)
                for section, key in keys:
                    if section == ctype:
                        key_node = tree.insert(parent, "end", text=key)
                        if key in equivalence_rules:
                            for rule_str in equivalence_rules[key]:
                                rule_id = str(uuid.uuid4())[:8]
                                tree.insert(key_node, "end", values=(rule_str, rule_id))
            logger.debug(f"Updated '---' tab: listbox with {rule_listbox.size()} rule types, treeview with {len(equivalence_rules)} keys and {sum(len(r) for r in equivalence_rules.values())} rules", extra={'session_id': current_session_id})
        else:
            logger.warning("Widgets for '---' tab are not initialized", extra={'session_id': current_session_id})
    elif tab_title == 'Rules':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_equivalence_rules(widget)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Pull Log':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_file_log(widget, file_path)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Compare Log':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_compare_log(widget, item_number)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Equivalence':
        # Populate mismatches from current comparisons or fall back to cache
        widgets = tab_contents[tab_title]
        try:
            if not isinstance(widgets, dict) or 'mismatch_tree' not in widgets:
                return
            tree = widgets['mismatch_tree']
            tree.delete(*tree.get_children())

            def populate_from(comp_dict):
                def extract_rows(name):
                    return comp_dict.get(name) or []
                sources_local = [
                    ('Title vs. Specifics', extract_rows('Title vs. Specifics')),
                    ('Title vs. Table Data', extract_rows('Title vs. Table Data')),
                    ('Specifics vs. Table Data', extract_rows('Specifics vs. Table Data')),
                ]
                seen_local = set()
                count_local = 0
                for source_name, rows in sources_local:
                    for row in rows:
                        if not isinstance(row, (list, tuple)) or len(row) < 4:
                            continue
                        key_disp = str(row[0]).strip()
                        val_a = str(row[1]).splitlines()[0].strip()
                        symbol = str(row[2]).strip() if len(row) > 2 else ''
                        val_b = str(row[3]).splitlines()[0].strip() if len(row) > 3 else ''
                        base_key = key_disp.lower().replace(' ', '_')
                        if not base_key.endswith('_key'):
                            base_key = f"{base_key}_key"
                        is_mismatch = (symbol == '≠') or (val_a and val_b and val_a.lower() != val_b.lower())
                        if not is_mismatch:
                            continue
                        sig = (base_key, val_a, val_b, source_name)
                        if sig in seen_local:
                            continue
                        seen_local.add(sig)
                        tree.insert('', 'end', values=(base_key, val_a, val_b, source_name))
                        count_local += 1
                return count_local

            # Try current comparisons first
            total = populate_from(comparisons if isinstance(comparisons, dict) else {})
            if total == 0:
                # Fallback: use cached comparisons for current file path
                comp_alt = comparisons_cache.get(str(file_path)) or comparisons_cache.get(file_path)
                if isinstance(comp_alt, dict):
                    total = populate_from(comp_alt)
            logger.debug(f"Equivalence tab: populated {total} mismatches", extra={'session_id': current_session_id})
        except Exception as e:
            logger.error(f"Equivalence tab: mismatch population failed: {e}", extra={'session_id': current_session_id})
    elif tab_title == 'Process Log':
        # Handle new dictionary structure for the Process Log tab
        if isinstance(tab_contents[tab_title], dict):
            widget = tab_contents[tab_title]['text_widget']
            extractor_filter = tab_contents[tab_title]['extractor_var'].get()
            widget.config(state='normal')
            widget.delete(1.0, tk.END)
            display_process_log(widget, item_number, extractor_filter)
            widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
        else:
            # Backward compatibility for old widget structure
            widget = tab_contents[tab_title]
            widget.config(state='normal')
            widget.delete(1.0, tk.END)
            display_process_log(widget, item_number)
            widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Debug':
        widget = tab_contents[tab_title]
        display_debug_log(widget, item_number)
    elif isinstance(tab_contents[tab_title], scrolledtext.ScrolledText):
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        # Map new tab titles to original comparison keys for ScrolledText tabs
        key_map = {
            'Misc': 'Misc Comparison',
            'Title & Metadata': 'Combined Data',
            'Specifics': 'Specifics',
            'Table': 'Table Data'
        }
        comparison_key = key_map.get(tab_title, tab_title)
        content = comparisons.get(comparison_key, "")
        widget.insert(tk.END, content if content else "    - No data available")
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    else:  # Treeview tabs like 'Title vs. Specs', etc.
        widget = tab_contents[tab_title]
        # If this tab is not a Treeview (e.g., 'Equivalence' returns a dict of widgets), skip
        if not isinstance(widget, ttk.Treeview):
            return
        for item in widget.get_children():
            widget.delete(item)
        # Map new tab titles to original comparison keys for Treeview tabs
        treeview_key_map = {
            'Title vs. Specs': 'Title vs. Specifics',
            'Title vs. Table': 'Title vs. Table Data',
            'Specs vs. Table': 'Specifics vs. Table Data',
            'Non Matched Keys': 'Non-Matched Keys'
        }
        comparison_key = treeview_key_map.get(tab_title, tab_title)
        content = comparisons.get(comparison_key, [])
        if isinstance(content, list) and content:
            for row in content:
                cleaned_row = [val.strip() if isinstance(val, str) else val for val in row]
                # For Non Matched Keys, content rows are (Section, Key, Value)
                # For comparison tabs, rows follow (Key, Source1, Symbol, Source2[, Table Entry])
                widget.insert("", tk.END, values=cleaned_row)
                # Apply background color based on state for comparison tabs
                if tab_title in ['Title vs. Specs', 'Title vs. Table', 'Specs vs. Table']:
                    key_value = cleaned_row[0] if cleaned_row else ''
                    state = get_row_state(tab_title, key_value)
                    if state == 'blacklist':
                        widget.item(widget.get_children()[-1], tags=('blacklist',))
                    elif state == 'whitelist':
                        widget.item(widget.get_children()[-1], tags=('whitelist',))
        else:
            # Default message adjusted for number of columns
            if tab_title == 'Title vs. Specs':
                widget.insert("", tk.END, values=("    - No matching keys found", "", ""), tags=("normal",))
            elif tab_title in ['Title vs. Table', 'Specs vs. Table']:
                widget.insert("", tk.END, values=("    - No matching keys found", "", "", ""), tags=("normal",))
            else:
                widget.insert("", tk.END, values=("    - No matching keys found",) * len(widget["columns"]), tags=("normal",))
        widget.tag_configure("bold", font=bold_font)
        widget.tag_configure("normal", font=normal_font)
        widget.tag_configure('blacklist', background='dark gray')
        widget.tag_configure('whitelist', background='light green')
        
def update_widget_colors(widget):
    """Update the colors of a widget based on its type and the current theme."""
    if isinstance(widget, (tk.Text, scrolledtext.ScrolledText)):
        widget.config(background=scrolled_bg, foreground=scrolled_fg)
    elif isinstance(widget, tk.Label):
        widget.config(background=frame_bg, foreground=link_fg if 'link' in widget.winfo_name() else 'black')

def update_all_widgets():
    """Update the colors of all widgets that have direct color settings."""
    for tab_title, content in tab_contents.items():
        if content is None:
            continue
        if isinstance(content, dict):  # For 'Issues' tab
            for sub_widget in content.values():
                if isinstance(sub_widget, (tk.Text, scrolledtext.ScrolledText)):
                    update_widget_colors(sub_widget)
        elif isinstance(content, (tk.Text, scrolledtext.ScrolledText)):
            update_widget_colors(content)

def cycle_theme():
    global theme
    themes = ['light', 'dark', 'neutral_gray_blue', 'dark_neutral_blue', 'xp_media_center_black']
    current_index = themes.index(theme)
    next_index = (current_index + 1) % len(themes)
    theme = themes[next_index]
    save_settings()
    apply_theme(ttk.Style())
    update_all_widgets()  # Add this line to update all widgets after theme change
    # Refresh the "Copyable Summary" immediately if the Issues tab exists
    if 'Issues' in tab_contents and tab_contents['Issues'] is not None and files:
        update_summary_text()
    logger.debug(f"Cycled theme to {theme}", extra={'session_id': current_session_id})

def toggle_show_all():
    global show_all, show_unseen_issues
    show_all = show_all_var.get()
    show_unseen_issues = False
    show_unseen_issues_var.set(False)
    save_settings()
    update_file_list_and_load()
    logger.debug(f"Toggled show_all to {show_all}", extra={'session_id': current_session_id})

def toggle_show_unseen_issues():
    global show_unseen_issues, show_all
    show_unseen_issues = show_unseen_issues_var.get()
    show_all = False
    show_all_var.set(False)
    files_with_issues = filter_files_with_issues()
    save_settings()
    update_file_list_and_load()
    logger.debug(f"Toggled show_unseen_issues to {show_unseen_issues}", extra={'session_id': current_session_id})

def set_category_filter(value):
    global category_filter
    if category_filter != value:
        category_filter = value
        save_settings()
        update_file_list_and_load()
        logger.debug(f"Set category filter to {value}", extra={'session_id': current_session_id})

def schedule_search(event):
    global search_timer
    if search_timer:
        root.after_cancel(search_timer)
    search_timer = root.after(300, update_file_list_and_load)

def update_file_list_and_load():
    update_file_list()
    global current_file_index
    current_file_index = 0
    if files:
        load_file(files[0])
    else:
        file_label.config(text="No search results found")

def clear_search():
    search_var.set("")
    update_file_list_and_load()

def update_file_list():
    global current_session_id, files
    current_session_id = str(uuid.uuid4())[:8]
    logger.debug("Updating file list", extra={'session_id': current_session_id})
    
    # Start with all files and apply existing filters
    filtered_files = all_files[:]
    logger.debug(f"Initial files: {len(filtered_files)}", extra={'session_id': current_session_id})
    
    # Filter out files without issues if 'show_all' is False
    if not show_all:
        filtered_files_new = []
        for f in filtered_files:
            if f not in comparisons_cache:
                logger.debug(f"Excluding {f.name}: Not in comparisons_cache", extra={'session_id': current_session_id})
                continue
            if not has_issues(comparisons_cache[f]):
                logger.debug(f"Excluding {f.name}: No issues detected", extra={'session_id': current_session_id})
                continue
            filtered_files_new.append(f)
        filtered_files = filtered_files_new
        logger.debug(f"After show_all filter: {len(filtered_files)} files", extra={'session_id': current_session_id})
    
    # Show only files with unseen issues if enabled
    if show_unseen_issues:
        filtered_files = [f for f in filtered_files if str(f) not in looked_at_files]
        logger.debug(f"After show_unseen_issues filter: {len(filtered_files)} files", extra={'session_id': current_session_id})
    
    # Apply category filter (1 for laptops, 2 for non-laptops)
    if category_filter == 1:
        filtered_files = [f for f in filtered_files if f in parsed_data and 'laptop' in ' '.join(parsed_data[f][1].get('CATEGORY', [])).lower()]
        logger.debug(f"After laptop category filter: {len(filtered_files)} files", extra={'session_id': current_session_id})
    elif category_filter == 2:
        filtered_files = [f for f in filtered_files if f in parsed_data and 'laptop' not in ' '.join(parsed_data[f][1].get('CATEGORY', [])).lower()]
        logger.debug(f"After non-laptop category filter: {len(filtered_files)} files", extra={'session_id': current_session_id})
    
    # Get and process the search term
    search_term = search_var.get().strip().lower()
    if search_term:
        # Split search term into individual words
        search_words = search_term.split()
        search_results = []
        
        # Check each file against the search criteria
        for file_path in filtered_files:
            if file_path not in parsed_data:
                logger.warning(f"Missing preloaded data for {file_path}", extra={'session_id': current_session_id})
                continue
            
            # Extract data from parsed_data
            listing_data, sections = parsed_data[file_path]
            
            # Get item number from filename
            item_num = file_path.name.replace('python_parsed_', '').replace('.txt', '').lower()
            
            # Get SKU from metadata
            metadata = {k.replace('meta_', ''): v for k, v in listing_data['metadata'].items()}
            sku = metadata.get('customlabel_key', '').lower()
            
            # Get category text
            category_section = sections.get('CATEGORY', [])
            category_text = ' '.join(category_section).lower()
            
            # Get Full Title from title_title_key
            full_title = listing_data['title'].get('title_title_key', '').lower()
            
            # Include file if all search words are in any one of the fields
            if (all(word in item_num for word in search_words) or
                all(word in sku for word in search_words) or
                all(word in category_text for word in search_words) or
                all(word in full_title for word in search_words)):
                search_results.append(file_path)
            
            logger.debug(f"Search for '{search_term}' on {file_path.name}: {'Included' if file_path in search_results else 'Excluded'}", extra={'session_id': current_session_id})
        
        # Update files list with search results
        files = search_results
        logger.debug(f"After search filter: {len(files)} files", extra={'session_id': current_session_id})
    else:
        # If no search term, show all filtered files
        files = filtered_files
    
    # Log the result
    logger.debug(f"File list updated. Total files: {len(files)}", extra={'session_id': current_session_id})

def sort_tree(tree, column):
    try:
        items = [(tree.set(item, column), item) for item in tree.get_children('')]
        reverse_sort = not any(tree.set(item, column) == tree.set(items[0][1], column) for item in tree.get_children(''))
        
        if column == "Symbol":
            def symbol_sort_key(item_tuple):
                symbol = item_tuple[0]
                if symbol == "==":
                    return 0
                elif symbol == "∅":
                    return 1
                elif symbol == "≢":
                    return 2
                return 3
            items.sort(key=symbol_sort_key, reverse=reverse_sort)
        else:
            def general_sort_key(item_tuple):
                value = item_tuple[0]
                if "[Missing" in value:
                    return "ZZZ" + value
                return value
            items.sort(key=general_sort_key, reverse=reverse_sort)
        
        for index, (val, item) in enumerate(items):
            tree.move(item, '', index)
        
        logger.debug(f"Sorted tree by column {column}", extra={'session_id': current_session_id})
    except Exception as e:
        logger.error(f"Error sorting tree: {str(e)}", exc_info=True, extra={'session_id': current_session_id})

def display_equivalence_rules(widget):
    widget.config(state='normal')
    widget.delete(1.0, tk.END)
    
    rules_text = "Equivalence Rules\n\n"
    equivalence_rules = {
        'storage_key': ["Matches if both are 'no storage', 'none', 'no', 'n/a', or 'no (m.2)', or 'nvme' wildcard with size like '256gb nvme'"],
        'ram_size_key': ["Matches if values are equal after removing spaces"],
        'operating_system_key': ["Matches if both are 'no os' or 'not included'"],
        'type_key': ["Matches if one value is a substring or abbreviation (e.g., 'SFF' = 'Small Form Factor (SFF)')"],
        'model_key': ["Matches if all present parts are consistent with no contradictions"],
        'series_key': ["Matches if all present parts are consistent with no contradictions"],
        'mpn_key': ["Matches if all present parts are consistent with no contradictions"],
        'screen_size_key': ["Matches if numerical sizes are equal, ignoring extra details"],
        'gpu_description_key': ["Matches if both indicate integrated or dedicated GPUs"],
        'condition_key': ["Matches if 'bad' aligns with 'for parts or not working'"],
        'storage_description_key': ["Matches if no storage indicated or 'nvme' wildcard with size"],
        'battery_key': ["Matches if Title 'no battery', 'no', or 'n/a' with Table 'no battery', 'no', 'n/a', or 'not included', else strict match"],
        'connectivity_key': ["Matches if any connectivity option in one value is present in the other (e.g., '5G' matches '4G, 5G, Wi-Fi')"]
    }
    
    for key, descriptions in equivalence_rules.items():
        rules_text += f"{key.replace('_key', '').replace('_', ' ').title()}:\n"
        for desc in descriptions:
            rules_text += f"  - {desc}\n"
        rules_text += "\n"
    
    widget.insert(tk.END, rules_text)
    widget.config(state='disabled', 
                  background="#ffffff" if theme == 'light' else "#444444" if theme == 'dark' else "#CBD5E0" if theme == 'neutral_gray_blue' else "#4A5568",
                  foreground="black" if theme == 'light' else "white" if theme == 'dark' else "#2D3748" if theme == 'neutral_gray_blue' else "#E2E8F0")

def display_file_log(widget, file_path):
    widget.config(state='normal')
    widget.delete(1.0, tk.END)
    
    # Use the global item_number set by load_file
    log_file = Path(PROCESSING_LOGS_DIR) / 'pull_logs' / f"{item_number}_pull_log.txt"
    log_content = f"Pull Log Entries for Item {item_number}\n\n"
    
    try:
        # Verify directory exists
        if not log_file.parent.exists():
            log_content += f"Pull logs directory not found: {log_file.parent}\n"
            logger.error(f"Pull logs directory not found: {log_file.parent}", extra={'session_id': current_session_id})
        else:
            # Check if file exists
            if log_file.exists():
                # Verify file is accessible
                try:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                    if lines:
                        # Process lines to remove one empty newline per non-empty line
                        processed_lines = []
                        i = 0
                        while i < len(lines):
                            line = lines[i]
                            if line.strip():  # Non-empty line
                                processed_lines.append(line.rstrip('\n') + '\n')
                                # Skip the next empty line if it exists
                                if i + 1 < len(lines) and not lines[i + 1].strip():
                                    i += 2  # Skip the empty line
                                else:
                                    i += 1
                            else:
                                # Preserve empty lines (e.g., section breaks)
                                processed_lines.append(line)
                                i += 1
                        log_content += ''.join(processed_lines)
                    else:
                        log_content += "No log entries found in file.\n"
                except PermissionError as e:
                    log_content += f"Permission denied accessing {log_file}: {str(e)}\n"
                    logger.error(f"Permission denied accessing {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
                except Exception as e:
                    log_content += f"Unexpected error reading {log_file}: {str(e)}\n"
                    logger.error(f"Unexpected error reading {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
            else:
                log_content += f"No pull log file found for this item: {log_file}\n"
                logger.debug(f"No pull log file found: {log_file}", extra={'session_id': current_session_id})
    except Exception as e:
        log_content += f"Error accessing pull log directory or file: {str(e)}\n"
        logger.error(f"Error accessing pull log directory or file for item {item_number}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
    
    widget.insert(tk.END, log_content)
    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)

def display_compare_log(widget, item_number):
    widget.config(state='normal')
    widget.delete(1.0, tk.END)
    
    log_file = Path(PROCESSING_LOGS_DIR) / 'compare_logs' / f"{item_number}.log"
    log_content = f"Debug Log for Item {item_number}\n\n"
    
    logger.debug(f"Displaying compare log for item {item_number}: {log_file}", extra={'session_id': current_session_id})
    
    try:
        # Verify directory exists
        if not log_file.parent.exists():
            log_content += f"Compare logs directory not found: {log_file.parent}\n"
            logger.error(f"Compare logs directory not found: {log_file.parent}", extra={'session_id': current_session_id})
        else:
            if not log_file.exists():
                logger.debug(f"Compare log file does not exist: {log_file}. Generating comparison.", extra={'session_id': current_session_id})
                # Generate comparison log if file is missing
                file_path = Path('item_contents') / f"python_parsed_{item_number}.txt"
                if file_path.exists():
                    session_id = str(uuid.uuid4())[:8]
                    set_item_log_file(item_number, session_id)
                    listing_data, sections = parse_file(file_path)
                    parsed_data[file_path] = (listing_data, sections)
                    comparisons_cache[file_path] = compare_data(listing_data, sections, file_path)
                    logger.debug(f"Generated comparison log for {item_number}", extra={'session_id': session_id})
                else:
                    log_content += f"No source file found for item {item_number}.\n"
                    widget.insert(tk.END, log_content)
                    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
                    logger.debug(f"No source file for {item_number}", extra={'session_id': current_session_id})
                    return
            
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                
                logger.debug(f"Read {len(lines)} lines from {log_file}", extra={'session_id': current_session_id})
                
                if not lines:
                    log_content += "No log entries found in file.\n"
                    widget.insert(tk.END, log_content)
                    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
                    return
                
                # Show all debug entries except the verbose comparison result dumps
                debug_entries = []
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Extract just the message part
                    match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - \[([^\]]+)\] - (.+)', line)
                    if match:
                        timestamp, session_id, message = match.groups()
                        
                        # Skip the verbose comparison result dumps
                        if "Comparison result:" in message:
                            continue
                        
                        # Use just time portion of timestamp
                        time_only = timestamp.split(' ')[1].split(',')[0]
                        debug_entries.append(f"[{time_only}] {message}")
                
                if debug_entries:
                    log_content += '\n'.join(debug_entries)
                    log_content += f"\n\n--- {len(debug_entries)} debug entries logged ---"
                else:
                    log_content += f"No debug entries found for item {item_number}.\n"
                    
            except PermissionError as e:
                log_content += f"Permission denied accessing {log_file}: {str(e)}\n"
                logger.error(f"Permission denied accessing {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
            except Exception as e:
                log_content += f"Unexpected error reading {log_file}: {str(e)}\n"
                logger.error(f"Unexpected error reading {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
                
    except Exception as e:
        log_content += f"Error accessing compare log directory or file: {str(e)}\n"
        logger.error(f"Error accessing compare log directory or file for item {item_number}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
    
    widget.insert(tk.END, log_content)
    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    logger.debug(f"Displayed compare log for item {item_number} with content length: {len(log_content)}", extra={'session_id': current_session_id})
    
def get_extractor_types():
    """Returns a list of extractor types from the configs directory."""
    extractor_types = ["All"]
    try:
        for filename in os.listdir(CONFIGS_DIR):
            if filename.startswith("extractor_") and filename.endswith(".py"):
                extractor_type = filename.replace("extractor_", "").replace(".py", "")
                extractor_types.append(extractor_type)
        return sorted(extractor_types)
    except Exception as e:
        logger.error(f"Error loading extractor types: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        return ["All"]

def display_process_log(widget, item_number, extractor_filter=None):
    widget.config(state='normal')
    widget.delete(1.0, tk.END)
    
    log_file = Path(PROCESSING_LOGS_DIR) / 'process_logs' / f"process_log_{item_number}.txt"
    log_content = f"Process Log Entries for Item {item_number}"
    if extractor_filter and extractor_filter != "All":
        log_content += f" (Filtered by: {extractor_filter})"
    log_content += "\n\n"
    
    try:
        # Verify directory exists
        if not log_file.parent.exists():
            log_content += f"Process logs directory not found: {log_file.parent}\n"
            logger.error(f"Process logs directory not found: {log_file.parent}", extra={'session_id': current_session_id})
        else:
            # Check if file exists
            if log_file.exists():
                # Verify file is accessible
                try:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.readlines()
                    if content:
                        if extractor_filter and extractor_filter != "All":
                            # More comprehensive filtering patterns to catch all extractor-related log entries
                            extractor_name = extractor_filter.lower()
                            filtered_content = []
                            
                            for line in content:
                                line_lower = line.lower()
                                if (f"extractor_{extractor_name}" in line_lower or 
                                    f"{extractor_name}_extractor" in line_lower or
                                    f"{extractor_name}: " in line or
                                    f"{extractor_name}_" in line_lower or
                                    f"applying extractor: {extractor_name}" in line_lower or
                                    f"applying {extractor_name}" in line_lower or
                                    f"{extractor_name} extractor" in line_lower or
                                    f"{extractor_name} extract" in line_lower):
                                    filtered_content.append(line)
                                # Special handling for debug messages from the extractors
                                elif "DEBUG" in line:
                                    if (f"Storage:" in line and extractor_name == "storage" or
                                        f"CPU:" in line and extractor_name == "cpu" or
                                        f"RAM:" in line and extractor_name == "ram" or
                                        f"GPU:" in line and extractor_name == "gpu" or
                                        f"OS:" in line and extractor_name == "os" or
                                        f"Screen:" in line and extractor_name == "screen" or
                                        f"Battery:" in line and extractor_name == "battery" or
                                        f"Adapter:" in line and extractor_name == "adapter" or
                                        f"Switch:" in line and extractor_name == "switch"):
                                        filtered_content.append(line)
                            if filtered_content:
                                log_content += "".join(filtered_content)
                            else:
                                log_content += f"No log entries found for extractor type: {extractor_filter}\n"
                        else:
                            log_content += "".join(content)
                    else:
                        log_content += "No log entries found in file.\n"
                except PermissionError as e:
                    log_content += f"Permission denied accessing {log_file}: {str(e)}\n"
                    logger.error(f"Permission denied accessing {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
                except Exception as e:
                    log_content += f"Unexpected error reading {log_file}: {str(e)}\n"
                    logger.error(f"Unexpected error reading {log_file}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
            else:
                log_content += f"No process log file found for this item: {log_file}\n"
                logger.debug(f"No process log file found: {log_file}", extra={'session_id': current_session_id})
    except Exception as e:
        log_content += f"Error accessing process log directory or file: {str(e)}\n"
        logger.error(f"Error accessing process log directory or file for item {item_number}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
    
    widget.insert(tk.END, log_content)
    widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)

def extract_sku_parts_orig(sku):
    """Original implementation: Extract SKU parts from a string"""
    parts = re.split(r'[\s-]+', sku.strip())
    initials = "XX"
    wid = None
    
    # Find initials
    if parts and re.match(r'^[A-Za-z]{2}$', parts[0]):
        initials = parts[0].upper()
        
        # Look for 3-6 digit number right after initials
        for i in range(1, len(parts)):
            if re.match(r'^\d{3,6}$', parts[i]):
                wid = parts[i]
                break
    
    # Fallback: look for any 3-6 digit number anywhere in the string
    if not wid:
        for part in parts:
            if re.match(r'^\d{3,6}$', part):
                wid = part
                break
    
    # Final fallback: any digit sequence (for backwards compatibility)
    if not wid:
        for part in reversed(parts):
            if re.match(r'^\d+$', part):
                wid = part
                break
    
    wid = wid if wid else "Unknown"
    return f"{initials} {wid}"

def extract_sku_parts(sku):
    """Extract SKU parts from a string. Returns formatted SKU as 'XX ####'"""
    if USE_STANDARDIZED_SKU_HANDLING:
        try:
            # Use the standardized implementation from sku_utils.py
            return std_format_sku(sku)
        except Exception as e:
            # Log error and fall back to original implementation
            logger.error(f"Error using standardized SKU extraction: {e}", extra={'session_id': current_session_id})
            return extract_sku_parts_orig(sku)
    else:
        # Use the original implementation
        return extract_sku_parts_orig(sku)
    
# --- Navigation and Display Functions ---
def load_file(file_path):
    global current_session_id, item_number, has_handled_file_operations
    item_number = file_path.name.replace('python_parsed_', '').replace('.txt', '')
    session_id = str(uuid.uuid4())[:8]
    set_item_log_file(item_number, session_id)
    logger.debug(f"Loading file: {file_path}", extra={'session_id': current_session_id})

    file_name = file_path.name
    item_number = file_name.replace('python_parsed_', '').replace('.txt', '')

    try:
        # ENHANCED: Use database-first loading
        listing_data, sections = enhanced_parse_file(file_path)
        
        if listing_data is None:
            logger.error(f"❌ Failed to load data for {file_path}", extra={'session_id': current_session_id})
            return
            
        parsed_data[file_path] = (listing_data, sections)
        
        # DON'T SET GLOBAL LISTING VARIABLE - keep data isolated
        comparisons = compare_data(listing_data, sections, file_path)
        
        # Get the current item's data for window title (but don't store globally)
        current_listing_data, current_sections = parsed_data[file_path]
        root.title(current_listing_data['title'].get('title_title_key', 'No Title Available'))
        
        # Override item_number with meta_itemnumber_key if available
        metadata = current_listing_data.get('metadata', {})
        meta_itemnumber = metadata.get('meta_itemnumber_key', None)
        item_number = meta_itemnumber if meta_itemnumber else file_name.replace('python_parsed_', '').replace('.txt', '')
        if item_number == '388177670416':
            logger.debug(f"Item number set to meta_itemnumber_key: {item_number}", extra={'session_id': current_session_id})
        
        looked_at_files.add(str(file_path))
        save_looked_at_files()
        
        bold_font = font.Font(family="Arial", size=12, weight="bold")
        normal_font = font.Font(family="Arial", size=12)
        misc_bold_font = font.Font(family="Arial", size=8, weight="bold")
        misc_normal_font = font.Font(family="Arial", size=8)
        
        # Check if comparisons indicate title_model_key error
        issues_content = comparisons.get('Issues', [])
        if isinstance(issues_content, tuple):
            issues_content, _ = issues_content
        if issues_content and any("title_model_key has value 'Model: Unknown Title'" in item[1] for item in issues_content):
            logger.debug(f"Skipping full GUI update for {item_number} due to title_model_key error", extra={'session_id': current_session_id})
            # Update only the Issues tab
            update_tab('Issues', comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font)
            file_label.config(text=f"File: {file_path.name} ({current_file_index + 1}/{len(files)})")
            ebay_link.config(text="View on eBay", 
                             foreground="blue" if theme == 'light' else "cyan" if theme == 'dark' else "#2D3748" if theme == 'neutral_gray_blue' else "#E2E8F0")
            description_label.config(text="Open Description")
            return

        # Get the currently selected tab
        selected_tab = notebook.select()
        selected_tab_title = notebook.tab(selected_tab, "text")
        
        # Lazy load and update the selected tab
        if tab_contents.get(selected_tab_title) is None and selected_tab_title != 'Issues':
            tab_contents[selected_tab_title] = create_tab_content(tabs[selected_tab_title], selected_tab_title)
        update_tab(selected_tab_title, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font)
        
        # Bind tab change event for other tabs
        for tab_title in tabs:
            if tab_title == '---' or tab_title is None or tab_title == selected_tab_title:
                continue
            # Ensure tabs[tab_title] is a widget (frame), not content
            tab_frame = tabs[tab_title]
            if not isinstance(tab_frame, ttk.Frame):  # Check if it's a valid frame
                continue
            if tab_contents.get(tab_title) is None:
                notebook.tab(tab_frame, state='normal')  # Enable the tab
            notebook.unbind("<<NotebookTabChanged>>")
            notebook.bind("<<NotebookTabChanged>>", 
                          lambda event: update_tab_on_select(event, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font))
        
        file_label.config(text=f"File: {file_path.name} ({current_file_index + 1}/{len(files)})")
        ebay_link.config(text="View on eBay", 
                         foreground="blue" if theme == 'light' else "cyan" if theme == 'dark' else "#2D3748" if theme == 'neutral_gray_blue' else "#E2E8F0")
        description_label.config(text="Open Description")
        logger.debug("File load complete", extra={'session_id': current_session_id})

    except Exception as e:
        logger.error(f"Error loading file {file_path}: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        messagebox.showerror("Error", f"Failed to load file {file_path.name}: {str(e)}")
        raise
        
def update_tab(tab_title, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font):
    """Update the content of a specific tab."""
    if tab_title == 'Issues':
        # Update the main components of the 'Issues' tab
        text_widget = tab_contents[tab_title]['summary_text']
        content, copyable_text_with_url = comparisons.get(tab_title) if isinstance(comparisons.get(tab_title), tuple) else (comparisons.get(tab_title), "")
        text_widget.delete(1.0, tk.END)
        text_widget.insert(tk.END, copyable_text_with_url)
        # Use global theme variables for summary text
        text_widget.config(bg=summary_bg, fg=summary_fg)
        text_widget.config(state='normal', relief='flat', borderwidth=1)
        text_widget.bind("<Button-1>", lambda e, tw=text_widget: copy_to_clipboard(tw))
        
        # Update error text widget
        error_widget = tab_contents[tab_title]['error_text']
        error_widget.config(state='normal')
        error_widget.delete(1.0, tk.END)
        errors = extract_errors_for_item(item_number)
        if errors:
            error_widget.insert(tk.END, "\n".join(errors))
        else:
            error_widget.insert(tk.END, "No errors detected")
        error_widget.config(state='disabled', bg=error_bg, fg=error_fg)
        error_widget.bind("<Button-1>", lambda e, tw=error_widget: copy_to_clipboard(tw))
        
        # Updated to handle ScrolledText instead of Treeview
        misc_widget = tab_contents[tab_title]['misc_issues']
        misc_widget.config(state='normal')
        misc_widget.delete(1.0, tk.END)
        misc_content = comparisons.get('Misc Issues', [])
        if misc_content:
            for issue in misc_content:
                cleaned_issue = issue[0].strip() if isinstance(issue, tuple) else issue.strip()
                misc_widget.insert(tk.END, f"{cleaned_issue}\n")
        else:
            misc_widget.insert(tk.END, "No misc issues detected\n")
        misc_widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)

        # Update the sub-sections within the 'Issues' tab
        sub_sections = [
            ("Title vs. Specifics Issues", 'Title vs. Specifics Issues'),
            ("Title vs. Table Issues", 'consolidated_title_vs_table'),
            ("Specifics vs. Table Issues", 'consolidated_specs_vs_table')
        ]
        for section, comparison_key in sub_sections:
            widget = tab_contents['Issues'][section]['tree']
            for item in widget.get_children():
                widget.delete(item)
            content = comparisons.get(comparison_key, [])
            if content:
                for row in content:
                    cleaned_row = [val.strip() if isinstance(val, str) else val for val in row]
                    widget.insert("", tk.END, values=cleaned_row, tags=("bold",))
            else:
                # Determine the appropriate message based on the section
                if comparison_key == 'Title vs. Specifics Issues':
                    message = "No title vs. specifics issues detected"
                elif comparison_key == 'consolidated_title_vs_table':
                    message = "No title vs. table issues detected"
                elif comparison_key == 'consolidated_specs_vs_table':
                    message = "No specifics vs. table issues detected"
                widget.insert("", tk.END, values=("", message, "", ""), tags=("normal",))
            widget.tag_configure("bold", font=bold_font)
            widget.tag_configure("normal", font=normal_font)
    elif tab_title == '---':
        if tab_contents[tab_title] is not None:
            widgets = tab_contents[tab_title]
            rule_listbox = widgets['rule_listbox']
            tree = widgets['tree']

            # Refresh the listbox with rule types
            rule_listbox.delete(0, tk.END)
            for rt in rule_types:
                rule_listbox.insert(tk.END, rt.name)

            # Refresh the treeview with comparison types and rules
            tree.delete(*tree.get_children())
            comparison_types = ['title', 'specifics', 'metadata', 'description']
            keys = set()
            for file_path in all_files:
                if file_path in parsed_data:
                    listing_data, _ = parsed_data[file_path]
                    for section in comparison_types:
                        for key in listing_data.get(section, {}):
                            if '_' in key:
                                keys.add((section, '_'.join(key.split('_')[1:])))
            
            for ctype in comparison_types:
                parent = tree.insert("", "end", text=ctype.capitalize(), open=True)
                for section, key in keys:
                    if section == ctype:
                        key_node = tree.insert(parent, "end", text=key)
                        if key in equivalence_rules:
                            for rule_str in equivalence_rules[key]:
                                rule_id = str(uuid.uuid4())[:8]
                                tree.insert(key_node, "end", values=(rule_str, rule_id))
            logger.debug(f"Updated '---' tab: listbox with {rule_listbox.size()} rule types, treeview with {len(equivalence_rules)} keys and {sum(len(r) for r in equivalence_rules.values())} rules", extra={'session_id': current_session_id})
        else:
            logger.warning("Widgets for '---' tab are not initialized", extra={'session_id': current_session_id})
    elif tab_title == 'Rules':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_equivalence_rules(widget)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Pull Log':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_file_log(widget, file_path)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Compare Log':
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        display_compare_log(widget, item_number)
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Process Log':
        # Handle new dictionary structure for the Process Log tab
        if isinstance(tab_contents[tab_title], dict):
            widget = tab_contents[tab_title]['text_widget']
            extractor_filter = tab_contents[tab_title]['extractor_var'].get()
            widget.config(state='normal')
            widget.delete(1.0, tk.END)
            display_process_log(widget, item_number, extractor_filter)
            widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
        else:
            # Backward compatibility for old widget structure
            widget = tab_contents[tab_title]
            widget.config(state='normal')
            widget.delete(1.0, tk.END)
            display_process_log(widget, item_number)
            widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    elif tab_title == 'Debug':
        widget = tab_contents[tab_title]
        display_debug_log(widget, item_number)
    elif isinstance(tab_contents[tab_title], scrolledtext.ScrolledText):
        widget = tab_contents[tab_title]
        widget.config(state='normal')
        widget.delete(1.0, tk.END)
        # Map new tab titles to original comparison keys for ScrolledText tabs
        key_map = {
            'Misc': 'Misc Comparison',
            'Title & Metadata': 'Combined Data',
            'Specifics': 'Specifics',
            'Table': 'Table Data'
        }
        comparison_key = key_map.get(tab_title, tab_title)
        content = comparisons.get(comparison_key, "")
        widget.insert(tk.END, content if content else "    - No data available")
        widget.config(state='disabled', background=scrolled_bg, foreground=scrolled_fg)
    else:  # Treeview tabs like 'Title vs. Specs', etc.
        widget = tab_contents[tab_title]
        # If this tab content isn't a Treeview, do nothing
        if not isinstance(widget, ttk.Treeview):
            return
        for item in widget.get_children():
            widget.delete(item)
        # Map new tab titles to original comparison keys for Treeview tabs
        treeview_key_map = {
            'Title vs. Specs': 'Title vs. Specifics',
            'Title vs. Table': 'Title vs. Table Data',
            'Specs vs. Table': 'Specifics vs. Table Data',
            'Non Matched Keys': 'Non-Matched Keys'
        }
        comparison_key = treeview_key_map.get(tab_title, tab_title)
        content = comparisons.get(comparison_key, [])
        if isinstance(content, list) and content:
            num_columns = len(widget["columns"])
            for row in content:
                cleaned_row = [val.strip() if isinstance(val, str) else val for val in row]
                # Ensure the row has enough values for the number of columns
                display_vals = cleaned_row[:num_columns]
                widget.insert("", tk.END, values=display_vals)
                # Apply background color based on state for comparison tabs
                if tab_title in ['Title vs. Specs', 'Title vs. Table', 'Specs vs. Table']:
                    key_value = display_vals[0] if display_vals else ''
                    state = get_row_state(tab_title, key_value)
                    if state == 'blacklist':
                        widget.item(widget.get_children()[-1], tags=('blacklist',))
                    elif state == 'whitelist':
                        widget.item(widget.get_children()[-1], tags=('whitelist',))
        else:
            # Adjust the default message based on the number of columns
            if tab_title in ['Title vs. Table', 'Specs vs. Table']:
                widget.insert("", tk.END, values=("", "    - No matching keys found", "", "", ""), tags=("normal",))
            else:
                widget.insert("", tk.END, values=("", "    - No matching keys found", "", ""), tags=("normal",))
        widget.tag_configure("bold", font=bold_font)
        widget.tag_configure("normal", font=normal_font)
        widget.tag_configure('blacklist', background='dark gray')
        widget.tag_configure('whitelist', background='light green')

def update_tab_on_select(event, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font):
    """Update a tab when it is selected."""
    selected_tab = event.widget.select()
    selected_tab_title = event.widget.tab(selected_tab, "text")
    if tab_contents[selected_tab_title] is None and selected_tab_title != 'Issues':
        tab_contents[selected_tab_title] = create_tab_content(tabs[selected_tab_title], selected_tab_title)
    update_tab(selected_tab_title, comparisons, file_path, bold_font, normal_font, misc_bold_font, misc_normal_font)

def load_previous():
    global current_file_index, has_handled_file_operations
    has_handled_file_operations = False  # Reset flag for new file
    if current_file_index > 0:
        current_file_index -= 1
        load_file(files[current_file_index])
        logger.debug("Loaded previous file", extra={'session_id': current_session_id})

def load_next():
    global current_file_index, has_handled_file_operations
    has_handled_file_operations = False  # Reset flag for new file
    if current_file_index < len(files) - 1:
        current_file_index += 1
        load_file(files[current_file_index])
        for widget in right_panel.winfo_children():
            if isinstance(widget, ttk.Frame):
                for child in widget.winfo_children():
                    if isinstance(child, ttk.Button) and child['text'] == "Next":
                        child.focus_set()
                        break
        logger.debug("Loaded next file", extra={'session_id': current_session_id})
    else:
        logger.debug("No more files to process, closing application", extra={'session_id': current_session_id})
        root.quit()



def copy_to_clipboard(text_widget):
    text = text_widget.get("1.0", tk.END).strip()
    pyperclip.copy(text)
    logger.debug(f"Copied to clipboard: {text}", extra={'session_id': current_session_id})
    
def open_file(file_type):
    try:
        if file_type == "parsed":
            file_path = str(files[current_file_index])
            if os.path.exists(file_path):
                if platform.system() == "Windows":
                    os.startfile(file_path)
                elif platform.system() == "Darwin":
                    subprocess.call(["open", file_path])
                else:
                    subprocess.call(["xdg-open", file_path])
                file_label.config(text=f"File: {files[current_file_index].name} ({current_file_index + 1}/{len(files)})")
            else:
                messagebox.showerror("File Error", f"The parsed file could not be found: {file_path}")
        else:
            messagebox.showerror("Invalid Type", f"File type '{file_type}' not supported.")
    except Exception as e:
        logger.error(f"Error opening {file_type} file: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        messagebox.showerror("Error", f"Failed to open the {file_type} file: {str(e)}")

ITEM_CONTENTS_DIR = "item_contents"
# Load data from item_contents (simplified simulation)
def load_data():
    global all_files, parsed_data
    all_files = [os.path.join(ITEM_CONTENTS_DIR, f) for f in os.listdir(ITEM_CONTENTS_DIR) if f.endswith('.json')]
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                parsed_data[file_path] = (data, None)  # Simulated listing_data, _
            logger.debug(f"Loaded data from {file_path}")
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")

def open_description_file(event=None):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        description_file = f"{item_number}_description.txt"
        file_path = os.path.join(script_dir, "item_contents", description_file)
        if os.path.exists(file_path):
            os.startfile(file_path)
        else:
            messagebox.showerror("File Error", f"Description file not found: {description_file}")
    except AttributeError:
        messagebox.showerror("No Data", "No item number available. Load a file first.")
    except Exception as e:
        logger.error(f"Error opening description file: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        messagebox.showerror("Error", f"Failed to open description file: {str(e)}")

def save_special_description():
    """Copy the current item's description file into item_contents/special/.

    This helps collect unusual description formats for targeted testing later.
    """
    try:
        global item_number
        if not item_number:
            messagebox.showerror("No Data", "No item number available. Load a file first.")
            return

        script_dir = os.path.dirname(os.path.abspath(__file__))
        source_file_name = f"{item_number}_description.txt"
        src_path = os.path.join(script_dir, "item_contents", source_file_name)

        if not os.path.exists(src_path):
            messagebox.showerror("File Error", f"Description file not found: {source_file_name}")
            return

        dest_dir = os.path.join(script_dir, "item_contents", "special")
        os.makedirs(dest_dir, exist_ok=True)

        # Avoid overwriting: if exists, append an incrementing suffix
        dest_path = os.path.join(dest_dir, source_file_name)
        if os.path.exists(dest_path):
            for i in range(1, 1000):
                candidate = os.path.join(dest_dir, f"{item_number}_description_{i}.txt")
                if not os.path.exists(candidate):
                    dest_path = candidate
                    break

        shutil.copy2(src_path, dest_path)
        logger.info(f"Copied description to special: {dest_path}", extra={'session_id': current_session_id})
        messagebox.showinfo("Saved", f"Copied to special: {os.path.basename(dest_path)}")
    except Exception as e:
        logger.error(f"Error saving special description: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        messagebox.showerror("Error", f"Failed to save special description: {str(e)}")
		
def generate_report(file_path=None, item_number=None, result=None):
    try:
        # Use provided parameters or fall back to current loaded file
        if file_path is None:
            if not files or current_file_index < 0:
                return False
            file_path = files[current_file_index]
        
        if item_number is None:
            item_number = file_path.name.replace('python_parsed_', '').replace('.txt', '') if hasattr(file_path, 'name') else str(file_path).split('_')[-1].replace('.txt', '')
        
        if result is None:
            if file_path in comparisons_cache:
                result = comparisons_cache[file_path]
            else:
                return False

        current_date = datetime.now()
        monday = current_date - timedelta(days=current_date.weekday())
        sunday = monday + timedelta(days=6)
        
        week_start_num = monday.strftime("%Y%m%d")
        week_end_num = sunday.strftime("%Y%m%d")
        
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        start_month = month_names[monday.month]
        end_month = month_names[sunday.month]
        start_day = str(monday.day).zfill(2)
        end_day = str(sunday.day).zfill(2)
        year = monday.year
        
        readable_date = f"{start_month}{start_day}-{end_month}{end_day}_{year}"
        
        # Improved SKU extraction logic
        sku = 'UNKNOWN-SKU'
        metadata = {}
        
        # First, try to get metadata from parsed_data
        if file_path in parsed_data:
            listing_data, _ = parsed_data[file_path]
            raw_metadata = listing_data.get('metadata', {})
            
            # Look for SKU in both prefixed and non-prefixed keys
            sku_keys = ['meta_customlabel_key', 'customlabel_key', 'meta_custom_label_key', 'custom_label_key']
            for sku_key in sku_keys:
                if sku_key in raw_metadata and raw_metadata[sku_key]:
                    sku = raw_metadata[sku_key]
                    logger.debug(f"Found SKU '{sku}' using key '{sku_key}'", extra={'session_id': current_session_id})
                    break
            
            # Normalize metadata for other uses
            metadata = {k.replace('meta_', ''): v for k, v in raw_metadata.items()}
        
        # If we still don't have a valid SKU, try extracting from the summary text
        if sku == 'UNKNOWN-SKU' and result:
            if isinstance(result.get('Issues'), tuple):
                _, summary_text = result.get('Issues')
                # Try to extract SKU from summary text like "⚠ HN 814 Active:"
                summary_match = re.match(r'⚠\s*([A-Z]{2}\s+\d+)', summary_text)
                if summary_match:
                    sku = summary_match.group(1)
                    logger.debug(f"Extracted SKU '{sku}' from summary text", extra={'session_id': current_session_id})

        if isinstance(result.get('Issues'), tuple):
            _, summary_text = result.get('Issues')
        else:
            summary_text = "No issues detected"

        cleaned_sku = extract_sku_parts(sku)
        if summary_text.startswith(cleaned_sku + " "):
            summary_text = summary_text[len(cleaned_sku)+1:]
        elif summary_text == cleaned_sku:
            summary_text = "No issues detected"

        category_leaf = "Unknown Category"
        if file_path in parsed_data:
            sections = parsed_data[file_path][1]
            for line in sections.get('CATEGORY', []):
                if '[leaf_category_key]' in line:
                    parts = line.split(': ', 1)
                    if len(parts) == 2:
                        category_leaf = parts[1].strip()
                    break

        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)

        report_filename = f"report_{week_start_num}_{week_end_num}_{readable_date}.txt"
        report_path = reports_dir / report_filename

        with open(report_path, "a", encoding="utf-8") as f:
            formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f'DATE={formatted_date}\tITEM={item_number}\tSKU={sku}\tCATEGORY={category_leaf}\tSUMMARY={summary_text}\n')

        # Trivially discoverable: record the last scanned item for external consumers
        try:
            last_scanned_path = Path(PROCESSING_LOGS_DIR) / 'last_scanned.txt'
            with open(last_scanned_path, 'w', encoding='utf-8') as lf:
                lf.write(
                    f'DATE={formatted_date}\tITEM={item_number}\tSKU={sku}\tCLEANED_SKU={cleaned_sku}\tCATEGORY={category_leaf}\n'
                )
        except Exception as e:
            logger.warning(f"Unable to write last_scanned.txt: {e}", extra={'session_id': current_session_id})

        logger.debug(f"Auto-reported item {item_number} to {report_path} with SKU '{sku}', category '{category_leaf}' and summary '{summary_text}'", extra={'session_id': current_session_id})
        return True

    except Exception as e:
        logger.error(f"Error auto-reporting item: {str(e)}", exc_info=True, extra={'session_id': current_session_id})
        return False
        
def filter_files_with_issues():
    """
    Find files with issues by checking each file directly.
    Note: items_with_issues.txt tracking has been disabled - this function now relies solely on direct analysis.
    """
    files_with_issues_list = []
    logger.debug(f"Filtering files from {len(all_files)} total files", extra={'session_id': current_session_id})
    for file_path in all_files:
        session_id = str(uuid.uuid4())[:8]
        item_num = file_path.name.replace('python_parsed_', '').replace('.txt', '')
        logger.debug(f"Checking file for issues: {file_path} (Item: {item_num})", extra={'session_id': session_id})

        # Ensure item-specific logger is set
        set_item_log_file(item_num, session_id)

        try:
            # Use string representation of path for consistent dictionary keys
            if file_path not in parsed_data:
                # ENHANCED: Use database-first loading
                listing_data, sections = enhanced_parse_file(file_path)
                
                if listing_data is None:
                    logger.warning(f"⚠️ Could not load data for {file_path}", extra={'session_id': session_id})
                    continue
                
                # Store with string key for consistency
                parsed_data[str(file_path)] = (listing_data, sections)
                comparisons_cache[str(file_path)] = compare_data(listing_data, sections, file_path)
            
            # Look up with string key for consistency
            comparisons = comparisons_cache.get(str(file_path))
            if not comparisons:
                # Fallback for backward compatibility
                comparisons = comparisons_cache.get(file_path)
            
            if comparisons and has_issues(comparisons):
                files_with_issues_list.append(file_path)
                logger.debug(f"File {file_path.name} has issues", extra={'session_id': session_id})
            else:
                logger.debug(f"File {file_path.name} has no issues", extra={'session_id': session_id})
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}", exc_info=True, extra={'session_id': session_id})

    logger.debug(f"Files with issues: {len(files_with_issues_list)}", extra={'session_id': current_session_id})
    return files_with_issues_list
    
def get_blacklist_file_path():
    """Return the path to the processed items blacklist.

    Prefer state/ to align with other runtime state, with legacy root fallback for reads.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    state_dir = os.path.join(script_dir, 'state')
    try:
        os.makedirs(state_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(state_dir, 'processed_items_blacklist.txt')

def load_blacklist():
    """Load the processed items blacklist (state preferred, root fallback)."""
    state_file = get_blacklist_file_path()
    legacy_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed_items_blacklist.txt')
    blacklist = set()
    source_used = None
    try:
        if os.path.exists(state_file):
            with open(state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    v = line.strip()
                    if v:
                        blacklist.add(v)
            source_used = 'state'
        elif os.path.exists(legacy_file):
            with open(legacy_file, 'r', encoding='utf-8') as f:
                for line in f:
                    v = line.strip()
                    if v:
                        blacklist.add(v)
            source_used = 'legacy'
    except Exception:
        pass
    logger.debug(f"Loaded blacklist with {len(blacklist)} items (source={source_used or 'none'})", extra={'session_id': current_session_id})
    return blacklist

def save_blacklist(blacklist):
    """Save the processed items blacklist."""
    blacklist_file = get_blacklist_file_path()
    try:
        # Ensure parent directory exists
        parent_dir = os.path.dirname(blacklist_file)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        # If the in-memory set is empty, do not truncate the on-disk file
        if not blacklist:
            logger.debug("Skipping save_blacklist: in-memory set is empty; leaving existing file unchanged", extra={'session_id': current_session_id})
            return

        tmp_file = blacklist_file + '.tmp'
        with open(tmp_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted(blacklist)) + '\n')

        # Atomic replace to avoid partial writes
        os.replace(tmp_file, blacklist_file)
        logger.debug(f"Saved blacklist with {len(blacklist)} items to {blacklist_file}", extra={'session_id': current_session_id})
    except Exception as e:
        logger.error(f"Error saving blacklist: {str(e)}", exc_info=True, extra={'session_id': current_session_id})

def load_items_with_issues():
    """Previously loaded items with issues - now disabled and returns empty dict."""
    # Functionality disabled as requested
    logger.debug("Issues tracking disabled - returning empty issues dictionary", extra={'session_id': current_session_id})
    return {}

def save_items_with_issues(issues):
    """Previously saved items with issues - now disabled."""
    # Functionality disabled as requested
    logger.debug("Issues tracking disabled - not saving issues", extra={'session_id': current_session_id})
    pass

def remove_from_blacklist(item_num, blacklist):
    """Remove an item from the blacklist if it exists."""
    if item_num in blacklist:
        blacklist.remove(item_num)
        logger.debug(f"Removed {item_num} from blacklist", extra={'session_id': current_session_id})
        save_blacklist(blacklist)

def add_to_issues_list(item_num, issues):
    """Previously added items to the issues list - now disabled."""
    # Functionality disabled as requested
    logger.debug(f"Issues tracking disabled - not adding {item_num} to issues list", extra={'session_id': current_session_id})
    pass

def should_rescan(item_num, issues):
    """Check if an item should be rescanned - disabled and always returns False."""
    return False

def ensure_files_exist():
    logger.debug("Entering ensure_files_exist()", extra={'session_id': current_session_id})
    files_to_create = {
        'preferred_spellings.txt': "color colour",
        'correct_phrases.txt': "high quality low cost",
        'correct_phrases_case_insensitive.txt': "high quality low cost"  # New file for case-insensitive phrases
    }
    for file_name, content in files_to_create.items():
        logger.debug(f"Checking if {file_name} exists", extra={'session_id': current_session_id})
        if os.path.exists(file_name):
            logger.debug(f"{file_name} already exists", extra={'session_id': current_session_id})
        else:
            logger.debug(f"Creating {file_name}", extra={'session_id': current_session_id})
            try:
                with open(file_name, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Successfully created {file_name}", extra={'session_id': current_session_id})
            except Exception as e:
                logger.error(f"Failed to create {file_name}: {str(e)}", extra={'session_id': current_session_id})

# --- Main Initialization ---
def initialize():
    global root, all_files, files, files_with_issues, parsed_data, comparisons_cache, looked_at_files, search_var, current_file_index, current_session_id, is_command_line_mode, send_message_flag, has_handled_file_operations
    current_session_id = str(uuid.uuid4())[:8]  # Set session_id early
    
    # Delete all comparison log files at startup
    log_dir = Path(PROCESSING_LOGS_DIR) / 'compare_logs'
    log_dir.mkdir(exist_ok=True)
    for log_file in log_dir.glob('*.log'):
        try:
            log_file.unlink()
        except Exception as e:
            logger.error(f"Failed to delete log file {log_file}: {str(e)}", extra={'session_id': current_session_id})
    
    setup_logging()
    # handle_duplicates()  # DISABLED: Duplicates now handled immediately by zscrape script for reduced latency
    looked_at_files = load_looked_at_files()
    is_command_line_mode = False  # Initialize the flag
    send_message_flag = False  # Initialize the new flag
    has_handled_file_operations = False  # Reset flag at start

    # Load equivalence rules from JSON file
    load_equivalence_rules()
    logger.debug("Equivalence rules loaded", extra={'session_id': current_session_id})

    # ENHANCED: Initialize database connection
    database_initialized = initialize_database()
    
    if database_initialized:
        logger.info("🚀 ENHANCED MODE: Database + File hybrid system active", extra={'session_id': current_session_id})
    else:
        logger.info("📁 STANDARD MODE: File-only system active", extra={'session_id': current_session_id})

    root = tk.Tk()
    root.title("Listing Data Analyzer - " + ("DATABASE MODE" if database_initialized else "FILE MODE"))
    root.geometry("1440x800")
    root.state('zoomed')  # Maximize the window

    # Check for command-line argument first
    use_specific_item = False
    specific_item_number = None

    if len(sys.argv) > 1 and sys.argv[1].strip():
        specific_item_number = sys.argv[1].strip()
        use_specific_item = True
        logger.info(f"Item number provided via command line: {specific_item_number}", extra={'session_id': current_session_id})

        # If Caps Lock is ON, open the GUI with the specific item preloaded instead of CLI-only mode
        caps_on = False
        try:
            caps_on = is_caps_lock_on()
        except Exception:
            caps_on = False

        if caps_on:
            logger.info("Caps Lock ON with CLI arg: launching GUI instead of CLI mode", extra={'session_id': current_session_id})
            is_command_line_mode = False
            send_message_flag = True
            # Do not process/exit here; continue to GUI initialization. The later block
            # (use_specific_item & specific_item_number) will filter the loaded files to this item.
        else:
            # CLI mode (legacy behavior): process and exit
            is_command_line_mode = True
            send_message_flag = True

            # ENHANCED: Process the specific item using database-first approach
            target_file = Path('item_contents') / f"python_parsed_{specific_item_number}.txt"

            try:
                # Try enhanced parsing (database first, then file fallback)
                listing_data, sections = enhanced_parse_file(specific_item_number)

                if listing_data is not None:
                    comparisons = compare_data(listing_data, sections)
                    handle_file_operations(target_file, specific_item_number, comparisons)
                    logger.info(f"✅ Processed item {specific_item_number} in command-line mode", extra={'session_id': current_session_id})
                else:
                    logger.error(f"❌ Could not load item {specific_item_number} from database or file", extra={'session_id': current_session_id})
                    print(f"Error: Could not load item {specific_item_number} from database or file")

            except Exception as e:
                logger.error(f"❌ Error processing item {specific_item_number}: {str(e)}", extra={'session_id': current_session_id})

            # Exit after processing
            root.destroy()
            sys.exit(0)
    else:
        # Fallback to Caps Lock check if no command-line argument
        try:
            if is_caps_lock_on():
                use_specific_item = True
                send_message_flag = True  # Set to True when Caps Lock is on to ensure message is sent
                logger.info("Caps Lock is ON", extra={'session_id': current_session_id})
                print("Caps Lock detected!")
            else:
                logger.info("Caps Lock is OFF", extra={'session_id': current_session_id})
                print("Caps Lock not detected, proceeding with normal processing")
        except ModuleNotFoundError:
            logger.warning("Tkinter not available, proceeding with normal processing", extra={'session_id': current_session_id})
            print("Tkinter not available, proceeding with normal processing")

        # If Caps Lock is on and no CLI argument, prompt for item number
        if use_specific_item:
            try:
                specific_item_number = simpledialog.askstring(
                    "Item Number",
                    "Caps Lock detected. Enter the item number to process:",
                    parent=root
                )
                if specific_item_number:
                    logger.info(f"Item number provided: {specific_item_number}", extra={'session_id': current_session_id})
                    print(f"Processing only item number: {specific_item_number}")
                else:
                    logger.info("No item number provided, exiting.", extra={'session_id': current_session_id})
                    print("No item number provided, exiting.")
                    root.destroy()
                    sys.exit(0)
            except Exception as e:
                logger.error(f"Error showing input dialog: {str(e)}", extra={'session_id': current_session_id})
                print(f"Error showing input dialog: {str(e)}, proceeding normally")
                use_specific_item = False  # Fallback to normal processing

    # ENHANCED: Get all available items from database + files
    all_files = []
    database_items = []
    file_items = []
    
    # Get items from database (if available)
    if database_initialized:
        try:
            database_items = get_all_item_numbers_from_database()
            logger.info(f"💾 Database: Found {len(database_items)} items", extra={'session_id': current_session_id})
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve database items: {e}", extra={'session_id': current_session_id})
    
    # Get files from directory (only if fallback enabled)
    if FALLBACK_TO_FILES:
        file_paths = list(Path('item_contents').glob('python_parsed_*.txt'))
        file_items = [f.name.replace('python_parsed_', '').replace('.txt', '') for f in file_paths]
        logger.info(f"📁 Files: Found {len(file_items)} items", extra={'session_id': current_session_id})
        
        # Combine items from both sources (database takes priority)
        all_item_numbers = set(database_items + file_items)
    else:
        # Database-only mode - only use database items
        file_items = []
        all_item_numbers = set(database_items)
        logger.info(f"💾 Database-only mode: Using {len(database_items)} database items", extra={'session_id': current_session_id})
    
    # Convert back to file paths for compatibility with existing code
    all_files = [Path(f'item_contents/python_parsed_{item}.txt') for item in sorted(all_item_numbers)]
    
    logger.info(f"🔍 TOTAL: {len(all_files)} unique items available (DB: {len(database_items)}, Files: {len(file_items)})", 
                extra={'session_id': current_session_id})

    if use_specific_item and specific_item_number:
        target_file = Path('item_contents') / f"python_parsed_{specific_item_number}.txt"
        if target_file in all_files:
            all_files = [target_file]
            logger.info(f"Processing single file: {target_file}", extra={'session_id': current_session_id})
        else:
            logger.error(f"File for item number {specific_item_number} not found.", extra={'session_id': current_session_id})
            print(f"Error: File for item number {specific_item_number} not found.")
            root.destroy()
            sys.exit(0)

    parsed_data = {}
    comparisons_cache = {}
    blacklist = load_blacklist()
    issues = load_items_with_issues()

    # Preload data for all files, ensuring comparison logs are generated
    for file_path in all_files:
        session_id = str(uuid.uuid4())[:8]
        item_num = file_path.name.replace('python_parsed_', '').replace('.txt', '')
        logger.debug(f"Preloading file: {file_path} (Item: {item_num})", extra={'session_id': session_id})

        # Set item-specific logger
        set_item_log_file(item_num, session_id)

        try:
            listing_data, sections = parse_file(file_path)
            parsed_data[file_path] = (listing_data, sections)
            comparisons_cache[file_path] = compare_data(listing_data, sections)
            logger.debug(f"Successfully preloaded {file_path.name}", extra={'session_id': session_id})
        except Exception as e:
            logger.error(f"Failed to preload {file_path}: {str(e)}", exc_info=True, extra={'session_id': session_id})

    # After preloading, reset to main handler
    setup_logging()

    # Sort files by SKU (highest number first)
    def get_sku_number(file_path):
        if file_path in parsed_data:
            metadata = {k.replace('meta_', ''): v for k, v in parsed_data[file_path][0]['metadata'].items()}
            sku = metadata.get('customlabel_key', 'XX-00000')
            match = re.search(r'\d+$', sku)
            if match:
                return int(match.group())
        return -1

    all_files = sorted(all_files, key=get_sku_number, reverse=True)
    logger.debug(f"Sorted files by SKU (newest first): {all_files}", extra={'session_id': current_session_id})

    load_settings()
    search_var = tk.StringVar(value="")

    setup_gui()

    files_with_issues = filter_files_with_issues()
    files = files_with_issues if not show_all else all_files
    logger.debug(f"Files to process: {files}", extra={'session_id': current_session_id})

    update_file_list()

    if files:
        current_file_index = 0
        load_file(files[0])
    else:
        logger.warning("No files found to process", extra={'session_id': current_session_id})
        show_no_files_message()

    # Bind keyboard shortcuts for navigation
    root.bind('<Return>', lambda event: load_next())
    root.bind('<Tab>', lambda event: load_next())
    root.bind('<Shift_L>', lambda event: load_previous())
    root.bind('<Shift_R>', lambda event: load_previous())

    # Enter the main event loop to keep GUI open
    root.mainloop()
    
# Ensure setup_logging() uses LOG_FILE consistently
def setup_logging():
    global logger, current_session_id
    main_log_file = Path(PROCESSING_LOGS_DIR) / 'main_log.txt'
    logger = logging.getLogger('listing_analyzer')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Prevent logs from going to parent loggers
    
    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    
    # Set up main handler
    main_handler = logging.FileHandler(str(main_log_file), mode='w', encoding='utf-8')
    main_handler.setFormatter(logging.Formatter('%(asctime)s - [%(session_id)s] - %(message)s'))
    main_handler.is_main = True  # Custom attribute to identify it
    logger.addHandler(main_handler)
    
    current_session_id = "initial_session"
    logger.debug("Initializing application", extra={'session_id': current_session_id})

def show_no_files_message():
    top = tk.Toplevel(root)
    top.title("No Files")
    width, height = 300, 150
    root.update_idletasks()
    parent_x, parent_y = root.winfo_x(), root.winfo_y()
    parent_width, parent_height = root.winfo_width(), root.winfo_height()
    msg_x = parent_x + parent_width // 2 - width // 2
    msg_y = parent_y + parent_height // 2 - height // 2
    top.geometry(f"{width}x{height}+{int(msg_x)}+{int(msg_y)}")
    top.lift()
    top.focus_set()
    label = tk.Label(top, text="No files with issues found. Enable 'Show All' to view all files.")
    label.pack(padx=20, pady=20)
    button = tk.Button(top, text="OK", command=top.destroy)
    button.pack(pady=10)
    top.grab_set()
    timer_id = root.after(300000, lambda: top.winfo_exists() and (top.destroy(), root.destroy()))
    top.protocol("WM_DELETE_WINDOW", lambda: (root.after_cancel(timer_id), top.destroy()))
    top.wait_window()

# --- Run the Application ---
if __name__ == "__main__":
    initialize()