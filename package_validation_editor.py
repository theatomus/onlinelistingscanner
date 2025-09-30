#!/usr/bin/env python3
"""
Package Validation Rules Editor
A GUI tool for managing package validation rules used in the listing comparison system.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import json
import os
import copy
from typing import Dict, Any, Optional

def pounds_to_display(pounds_value):
    """Convert pounds to display format (oz for < 1 lb, lbs for >= 1 lb)."""
    if pounds_value is None or pounds_value == "":
        return ""
    
    try:
        pounds = float(pounds_value)
        if pounds < 1:
            ounces = pounds * 16
            return f"{ounces:g} oz"
        else:
            return f"{pounds:g} lbs"
    except (ValueError, TypeError):
        return str(pounds_value)

def display_to_pounds(display_value):
    """Convert display format back to pounds."""
    if not display_value or display_value == "":
        return None
    
    display_str = str(display_value).strip().lower()
    
    try:
        if 'oz' in display_str:
            # Extract number from oz string
            ounces = float(display_str.replace('oz', '').strip())
            return ounces / 16
        elif 'lbs' in display_str or 'lb' in display_str:
            # Extract number from lbs string
            pounds = float(display_str.replace('lbs', '').replace('lb', '').strip())
            return pounds
        else:
            # Assume it's a raw number (treat as pounds)
            return float(display_str)
    except (ValueError, TypeError):
        return None

class PackageValidationEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("Package Validation Rules Editor")
        self.root.geometry("1200x800")
        
        # Current rules data
        self.rules_file = "package_validation_rules.json"
        self.rules_data = self.load_rules()
        self.modified = False
        
        # Setup GUI
        self.setup_gui()
        self.populate_tree()
        
        # Bind window close event
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def setup_gui(self):
        """Setup the main GUI layout."""
        # Create main paned window
        self.main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left panel - Tree view
        self.setup_tree_panel()
        
        # Right panel - Edit form
        self.setup_edit_panel()
        
        # Bottom panel - Buttons
        self.setup_button_panel()
    
    def setup_tree_panel(self):
        """Setup the left panel with tree view."""
        tree_frame = ttk.Frame(self.main_paned)
        self.main_paned.add(tree_frame, weight=1)
        
        # Tree view
        tree_scroll = ttk.Scrollbar(tree_frame)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.tree = ttk.Treeview(tree_frame, yscrollcommand=tree_scroll.set)
        self.tree.pack(fill=tk.BOTH, expand=True)
        tree_scroll.config(command=self.tree.yview)
        
        # Configure tree columns
        self.tree['columns'] = ('type', 'description')
        self.tree.column('#0', width=200, minwidth=150)
        self.tree.column('type', width=100, minwidth=80)
        self.tree.column('description', width=200, minwidth=150)
        
        self.tree.heading('#0', text='Device / Rule', anchor=tk.W)
        self.tree.heading('type', text='Type', anchor=tk.W)
        self.tree.heading('description', text='Description', anchor=tk.W)
        
        # Bind tree selection
        self.tree.bind('<<TreeviewSelect>>', self.on_tree_select)
        
        # Context menu for tree
        self.setup_tree_context_menu()
    
    def setup_tree_context_menu(self):
        """Setup context menu for tree view."""
        self.tree_menu = tk.Menu(self.root, tearoff=0)
        self.tree_menu.add_command(label="Add Device Type", command=self.add_device_type)
        self.tree_menu.add_command(label="Add Rule", command=self.add_rule)
        self.tree_menu.add_separator()
        self.tree_menu.add_command(label="Edit", command=self.edit_selected)
        self.tree_menu.add_command(label="Delete", command=self.delete_selected)
        
        self.tree.bind("<Button-3>", self.show_tree_context_menu)  # Right-click
    
    def setup_edit_panel(self):
        """Setup the right panel with edit form."""
        edit_frame = ttk.Frame(self.main_paned)
        self.main_paned.add(edit_frame, weight=2)
        
        # Title
        self.edit_title = ttk.Label(edit_frame, text="Select an item to edit", font=('Arial', 12, 'bold'))
        self.edit_title.pack(pady=10)
        
        # Create notebook for different edit types
        self.edit_notebook = ttk.Notebook(edit_frame)
        self.edit_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Setup different edit tabs
        self.setup_device_edit_tab()
        self.setup_rule_edit_tab()
        self.setup_weight_unit_tab()
        self.setup_price_validation_tab()
        self.setup_model_check_tab()
        self.setup_validation_settings_tab()
        
        # Initially hide notebook
        self.edit_notebook.pack_forget()
    
    def setup_device_edit_tab(self):
        """Setup tab for editing device type properties."""
        self.device_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.device_tab, text="Device Type")
        
        # Device name
        ttk.Label(self.device_tab, text="Device Type Name:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.device_name_var = tk.StringVar()
        self.device_name_entry = ttk.Entry(self.device_tab, textvariable=self.device_name_var, width=40)
        self.device_name_entry.grid(row=0, column=1, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=5)
        
        # Has screen size ranges checkbox
        self.has_screen_ranges_var = tk.BooleanVar()
        self.screen_ranges_check = ttk.Checkbutton(
            self.device_tab, 
            text="Uses screen size ranges (like tablets)", 
            variable=self.has_screen_ranges_var
        )
        self.screen_ranges_check.grid(row=1, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Configure grid weights
        self.device_tab.columnconfigure(1, weight=1)
    
    def setup_rule_edit_tab(self):
        """Setup tab for editing individual rules."""
        self.rule_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.rule_tab, text="Rule Details")
        
        # Rule name
        ttk.Label(self.rule_tab, text="Rule Name:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.rule_name_var = tk.StringVar()
        self.rule_name_entry = ttk.Entry(self.rule_tab, textvariable=self.rule_name_var, width=30)
        self.rule_name_entry.grid(row=0, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        
        # Description
        ttk.Label(self.rule_tab, text="Description:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.rule_desc_var = tk.StringVar()
        self.rule_desc_entry = ttk.Entry(self.rule_tab, textvariable=self.rule_desc_var, width=30)
        self.rule_desc_entry.grid(row=1, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        
        # Max lot size
        ttk.Label(self.rule_tab, text="Max Lot Size:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.max_lot_var = tk.StringVar()
        self.max_lot_entry = ttk.Entry(self.rule_tab, textvariable=self.max_lot_var, width=10)
        self.max_lot_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Weight section
        weight_frame = ttk.LabelFrame(self.rule_tab, text="Weight Settings")
        weight_frame.grid(row=3, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=10)
        
        # Weight min/max
        ttk.Label(weight_frame, text="Weight Min (oz/lbs):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.weight_min_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.weight_min_var, width=10).grid(row=0, column=1, padx=5, pady=2)
        
        ttk.Label(weight_frame, text="Weight Max (oz/lbs):").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.weight_max_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.weight_max_var, width=10).grid(row=0, column=3, padx=5, pady=2)
        
        # Weight per item (for lots)
        ttk.Label(weight_frame, text="Weight per Item Min (oz/lbs):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.weight_item_min_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.weight_item_min_var, width=10).grid(row=1, column=1, padx=5, pady=2)
        
        ttk.Label(weight_frame, text="Weight per Item Max (oz/lbs):").grid(row=1, column=2, sticky=tk.W, padx=5, pady=2)
        self.weight_item_max_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.weight_item_max_var, width=10).grid(row=1, column=3, padx=5, pady=2)
        
        # Base packaging weight
        ttk.Label(weight_frame, text="Base Packaging Weight (oz/lbs):").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        self.base_packaging_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.base_packaging_var, width=10).grid(row=2, column=1, padx=5, pady=2)
        
        # Dimensions section
        dim_frame = ttk.LabelFrame(self.rule_tab, text="Dimension Settings")
        dim_frame.grid(row=4, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=10)
        
        # Dimension type selection
        ttk.Label(dim_frame, text="Dimension Type:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.dim_type_var = tk.StringVar(value="exact")
        dim_type_combo = ttk.Combobox(dim_frame, textvariable=self.dim_type_var, 
                                     values=["exact", "exact_options", "min", "max", "range"], 
                                     width=15, state="readonly")
        dim_type_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        dim_type_combo.bind('<<ComboboxSelected>>', self.on_dimension_type_change)
        
        # Dimension input frame (content changes based on type)
        self.dim_input_frame = ttk.Frame(dim_frame)
        self.dim_input_frame.grid(row=1, column=0, columnspan=4, sticky=tk.W+tk.E, padx=5, pady=5)
        
        # Initialize dimension inputs
        self.setup_dimension_inputs()
        
        # Configure grid weights
        self.rule_tab.columnconfigure(1, weight=1)
        weight_frame.columnconfigure(4, weight=1)
        dim_frame.columnconfigure(4, weight=1)
        
        # Additional options
        options_frame = ttk.LabelFrame(self.rule_tab, text="Additional Options")
        options_frame.grid(row=6, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=10)

        self.use_typical_box_var = tk.BooleanVar()
        ttk.Checkbutton(options_frame, text="Use Typical Box Sizes Database", variable=self.use_typical_box_var).grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        
        # Configure grid weights for new frames
        options_frame.columnconfigure(0, weight=1)
    
    def setup_dimension_inputs(self):
        """Setup dimension input fields based on current type."""
        # Clear existing widgets
        for widget in self.dim_input_frame.winfo_children():
            widget.destroy()
        
        dim_type = self.dim_type_var.get()
        
        if dim_type == "exact":
            # Single exact dimension (L x W x H)
            ttk.Label(self.dim_input_frame, text="Exact Dimensions (L x W x H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.exact_l_var = tk.StringVar()
            self.exact_w_var = tk.StringVar()
            self.exact_h_var = tk.StringVar()
            
            ttk.Entry(self.dim_input_frame, textvariable=self.exact_l_var, width=8).grid(row=0, column=1, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.exact_w_var, width=8).grid(row=0, column=3, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=4, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.exact_h_var, width=8).grid(row=0, column=5, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="inches").grid(row=0, column=6, padx=2)
            
        elif dim_type == "exact_options":
            # Multiple exact options
            ttk.Label(self.dim_input_frame, text="Exact Options (one per line, format: L,W,H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.exact_options_text = tk.Text(self.dim_input_frame, width=40, height=4)
            self.exact_options_text.grid(row=1, column=0, columnspan=4, sticky=tk.W+tk.E, pady=2)
            
        elif dim_type == "min":
            # Minimum dimensions
            ttk.Label(self.dim_input_frame, text="Minimum Dimensions (L x W x H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.min_l_var = tk.StringVar()
            self.min_w_var = tk.StringVar()
            self.min_h_var = tk.StringVar()
            ttk.Entry(self.dim_input_frame, textvariable=self.min_l_var, width=8).grid(row=0, column=1, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.min_w_var, width=8).grid(row=0, column=3, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=4, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.min_h_var, width=8).grid(row=0, column=5, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="inches").grid(row=0, column=6, padx=2)
        elif dim_type == "max":
            # Maximum dimensions
            ttk.Label(self.dim_input_frame, text="Maximum Dimensions (L x W x H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.max_l_var = tk.StringVar()
            self.max_w_var = tk.StringVar()
            self.max_h_var = tk.StringVar()
            
            ttk.Entry(self.dim_input_frame, textvariable=self.max_l_var, width=8).grid(row=0, column=1, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.max_w_var, width=8).grid(row=0, column=3, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="x").grid(row=0, column=4, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.max_h_var, width=8).grid(row=0, column=5, padx=2, pady=2)
            ttk.Label(self.dim_input_frame, text="inches").grid(row=0, column=6, padx=2)
            
        elif dim_type == "range":
            # Range dimensions (legacy format)
            ttk.Label(self.dim_input_frame, text="Length Range:").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.range_l_min_var = tk.StringVar()
            self.range_l_max_var = tk.StringVar()
            ttk.Entry(self.dim_input_frame, textvariable=self.range_l_min_var, width=8).grid(row=0, column=1, padx=2)
            ttk.Label(self.dim_input_frame, text="to").grid(row=0, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.range_l_max_var, width=8).grid(row=0, column=3, padx=2)
            
            ttk.Label(self.dim_input_frame, text="Width Range:").grid(row=1, column=0, sticky=tk.W, pady=2)
            self.range_w_min_var = tk.StringVar()
            self.range_w_max_var = tk.StringVar()
            ttk.Entry(self.dim_input_frame, textvariable=self.range_w_min_var, width=8).grid(row=1, column=1, padx=2)
            ttk.Label(self.dim_input_frame, text="to").grid(row=1, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.range_w_max_var, width=8).grid(row=1, column=3, padx=2)
            
            ttk.Label(self.dim_input_frame, text="Height Range:").grid(row=2, column=0, sticky=tk.W, pady=2)
            self.range_h_min_var = tk.StringVar()
            self.range_h_max_var = tk.StringVar()
            ttk.Entry(self.dim_input_frame, textvariable=self.range_h_min_var, width=8).grid(row=2, column=1, padx=2)
            ttk.Label(self.dim_input_frame, text="to").grid(row=2, column=2, padx=2)
            ttk.Entry(self.dim_input_frame, textvariable=self.range_h_max_var, width=8).grid(row=2, column=3, padx=2)
    
    def setup_weight_unit_tab(self):
        """Setup tab for editing weight unit validation."""
        self.weight_unit_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.weight_unit_tab, text="Weight Units")
        
        ttk.Label(self.weight_unit_tab, text="Allowed Units (comma-separated):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.allowed_units_var = tk.StringVar()
        ttk.Entry(self.weight_unit_tab, textvariable=self.allowed_units_var, width=40).grid(row=0, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        
        ttk.Label(self.weight_unit_tab, text="Flagged Units (comma-separated):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.flagged_units_var = tk.StringVar()
        ttk.Entry(self.weight_unit_tab, textvariable=self.flagged_units_var, width=40).grid(row=1, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        
        # Configure grid weights
        self.weight_unit_tab.columnconfigure(1, weight=1)
    
    def setup_price_validation_tab(self):
        """Setup tab for editing price validation settings."""
        self.price_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.price_tab, text="Price Settings")
        
        # Price validation enabled checkbox
        self.price_enabled_var = tk.BooleanVar()
        ttk.Checkbutton(self.price_tab, text="Enable Price Validation", 
                       variable=self.price_enabled_var).grid(row=0, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)
        
        # Auction prices section
        auction_frame = ttk.LabelFrame(self.price_tab, text="Auction Price Settings")
        auction_frame.grid(row=1, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=10)
        
        ttk.Label(auction_frame, text="Min Starting Price ($):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.auction_min_var = tk.StringVar()
        ttk.Entry(auction_frame, textvariable=self.auction_min_var, width=10).grid(row=0, column=1, padx=5, pady=2)
        
        ttk.Label(auction_frame, text="Max Starting Price ($):").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.auction_max_var = tk.StringVar()
        ttk.Entry(auction_frame, textvariable=self.auction_max_var, width=10).grid(row=0, column=3, padx=5, pady=2)
        
        # Global flags section
        global_frame = ttk.LabelFrame(self.price_tab, text="Global Price Flags")
        global_frame.grid(row=2, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=10)
        
        ttk.Label(global_frame, text="Suspicious Low Price ($):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.global_low_var = tk.StringVar()
        ttk.Entry(global_frame, textvariable=self.global_low_var, width=10).grid(row=0, column=1, padx=5, pady=2)
        
        ttk.Label(global_frame, text="Suspicious High Price ($):").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.global_high_var = tk.StringVar()
        ttk.Entry(global_frame, textvariable=self.global_high_var, width=10).grid(row=0, column=3, padx=5, pady=2)
        
        # Buy-It-Now device-specific prices (read-only display)
        buyitnow_frame = ttk.LabelFrame(self.price_tab, text="Buy-It-Now Price Ranges")
        buyitnow_frame.grid(row=3, column=0, columnspan=2, sticky=tk.W+tk.E+tk.N+tk.S, padx=5, pady=10)
        
        # Create editable text widget with scrollbar for buy-it-now price ranges (JSON).
        # Users can paste or type a valid JSON object that maps device types → lot bands.
        text_frame = ttk.Frame(buyitnow_frame)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.buyitnow_text = tk.Text(text_frame, height=10, width=60)
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.buyitnow_text.yview)
        self.buyitnow_text.configure(yscrollcommand=scrollbar.set)
        
        self.buyitnow_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Edit the JSON below to change Buy-It-Now price ranges, then click Apply.
        ttk.Label(buyitnow_frame, text="Edit the JSON below to change Buy-It-Now price ranges, then click Apply.",
                 font=('Arial', 8), foreground='gray').pack(pady=(5, 0))
        
        # Configure grid weights
        self.price_tab.columnconfigure(1, weight=1)
        self.price_tab.rowconfigure(3, weight=1)
    
    def setup_model_check_tab(self):
        """Create the Model-Check editor tab with dynamic dimension controls."""
        self.model_check_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.model_check_tab, text="Model Check")

        # Enable / disable checkbox
        initial_model_enabled = self.rules_data.get("model_check", {}).get("enabled", False)
        self.model_check_enabled_var = tk.BooleanVar(value=initial_model_enabled)
        ttk.Checkbutton(self.model_check_tab, text="Enable Model Check",
                       variable=self.model_check_enabled_var
                       ).grid(row=0, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)

        # Rules listbox
        self.model_rules = []
        self.model_rules_listbox = tk.Listbox(self.model_check_tab, selectmode=tk.SINGLE,
                                              width=40, height=10)
        # Place the listbox a bit lower now that the action buttons are at the top.
        self.model_rules_listbox.grid(row=2, column=0, columnspan=2,
                                      sticky=tk.W + tk.E, padx=5, pady=5)
        self.model_rules_listbox.bind("<<ListboxSelect>>", self.on_model_rule_select)

        # ---------------- Rule-detail frame ----------------
        self.model_rule_frame = ttk.LabelFrame(self.model_check_tab, text="Model Rule Details")
        self.model_rule_frame.grid(row=3, column=0, columnspan=2, sticky=tk.W + tk.E,
                                   padx=5, pady=5)

        # Model name
        ttk.Label(self.model_rule_frame, text="Model Name:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_name_var = tk.StringVar()
        ttk.Entry(self.model_rule_frame, textvariable=self.model_name_var,
                  width=30).grid(row=0, column=1, sticky=tk.W + tk.E, padx=5, pady=2)

        # --------------- Weight settings -------------------
        weight_frame = ttk.LabelFrame(self.model_rule_frame, text="Weight Settings")
        weight_frame.grid(row=1, column=0, columnspan=2, sticky=tk.W + tk.E, padx=5, pady=5)

        ttk.Label(weight_frame, text="Weight Min (oz/lbs):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_weight_min_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.model_weight_min_var, width=10).grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(weight_frame, text="Weight Max (oz/lbs):").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_weight_max_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.model_weight_max_var, width=10).grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(weight_frame, text="Weight per Item Min (oz/lbs):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_weight_item_min_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.model_weight_item_min_var, width=10).grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(weight_frame, text="Weight per Item Max (oz/lbs):").grid(row=1, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_weight_item_max_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.model_weight_item_max_var, width=10).grid(row=1, column=3, padx=5, pady=2)

        ttk.Label(weight_frame, text="Base Packaging Weight (oz/lbs):").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_base_packaging_var = tk.StringVar()
        ttk.Entry(weight_frame, textvariable=self.model_base_packaging_var, width=10).grid(row=2, column=1, padx=5, pady=2)

        # --------------- Dimension settings -----------------
        dim_frame = ttk.LabelFrame(self.model_rule_frame, text="Dimension Settings")
        dim_frame.grid(row=2, column=0, columnspan=2, sticky=tk.W + tk.E, padx=5, pady=5)

        ttk.Label(dim_frame, text="Dimension Type:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.model_dim_type_var = tk.StringVar(value="max")
        dim_type_combo = ttk.Combobox(dim_frame, textvariable=self.model_dim_type_var,
                                      values=["exact", "exact_options", "min", "max", "range"],
                                      width=15, state="readonly")
        dim_type_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        dim_type_combo.bind("<<ComboboxSelected>>", self.on_model_dimension_type_change)

        self.model_dim_input_frame = ttk.Frame(dim_frame)
        self.model_dim_input_frame.grid(row=1, column=0, columnspan=7, sticky=tk.W + tk.E, padx=5, pady=5)

        self.setup_model_dimension_inputs()

        # Use typical box sizes
        self.use_typical_box_var = tk.BooleanVar()
        ttk.Checkbutton(self.model_rule_frame, text="Use Typical Box Sizes Database",
                        variable=self.use_typical_box_var
                        ).grid(row=3, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)

        # --------------------------- Price Settings ---------------------------
        price_frame = ttk.LabelFrame(self.model_rule_frame, text="Price Settings")
        price_frame.grid(row=4, column=0, columnspan=2, sticky=tk.W+tk.E, padx=5, pady=5)

        # Auction prices
        ttk.Label(price_frame, text="Auction Min ($)").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_auc_min_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_auc_min_var, width=10).grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(price_frame, text="Auction Max ($)").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_auc_max_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_auc_max_var, width=10).grid(row=0, column=3, padx=5, pady=2)

        # BIN Single
        ttk.Label(price_frame, text="BIN Single Min ($)").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_bin_single_min_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_single_min_var, width=10).grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Single Max ($)").grid(row=1, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_bin_single_max_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_single_max_var, width=10).grid(row=1, column=3, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Single Typical").grid(row=1, column=4, sticky=tk.W, padx=5, pady=2)
        self.model_bin_single_typ_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_single_typ_var, width=10).grid(row=1, column=5, padx=5, pady=2)

        # BIN Small Lot
        ttk.Label(price_frame, text="BIN Small Min ($)").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_bin_small_min_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_small_min_var, width=10).grid(row=2, column=1, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Small Max ($)").grid(row=2, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_bin_small_max_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_small_max_var, width=10).grid(row=2, column=3, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Small Typical").grid(row=2, column=4, sticky=tk.W, padx=5, pady=2)
        self.model_bin_small_typ_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_small_typ_var, width=10).grid(row=2, column=5, padx=5, pady=2)

        # BIN Large Lot
        ttk.Label(price_frame, text="BIN Large Min ($)").grid(row=3, column=0, sticky=tk.W, padx=5, pady=2)
        self.model_bin_large_min_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_large_min_var, width=10).grid(row=3, column=1, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Large Max ($)").grid(row=3, column=2, sticky=tk.W, padx=5, pady=2)
        self.model_bin_large_max_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_large_max_var, width=10).grid(row=3, column=3, padx=5, pady=2)

        ttk.Label(price_frame, text="BIN Large Typical").grid(row=3, column=4, sticky=tk.W, padx=5, pady=2)
        self.model_bin_large_typ_var = tk.StringVar()
        ttk.Entry(price_frame, textvariable=self.model_bin_large_typ_var, width=10).grid(row=3, column=5, padx=5, pady=2)

        price_frame.columnconfigure(5, weight=1)

        # ------------------- Action Buttons -------------------
        button_frame = ttk.Frame(self.model_check_tab)
        # Move the button frame just below the enable-checkbox so it's always visible.
        button_frame.grid(row=1, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)

        ttk.Button(button_frame, text="Add Model", command=self.add_model_rule).grid(row=0, column=0, padx=2)
        ttk.Button(button_frame, text="Edit Model", command=self.edit_model_rule).grid(row=0, column=1, padx=2)
        ttk.Button(button_frame, text="Delete Model", command=self.delete_model_rule).grid(row=0, column=2, padx=2)

        # Grid weight
        self.model_check_tab.columnconfigure(1, weight=1)
    
    def setup_validation_settings_tab(self):
        """Setup tab for editing validation settings."""
        self.validation_settings_tab = ttk.Frame(self.edit_notebook)
        self.edit_notebook.add(self.validation_settings_tab, text="Validation Settings")
        
        # Package validation enabled
        initial_validation_enabled = self.rules_data.get("validation_settings", {}).get("package_validation_enabled", True)
        self.package_validation_enabled_var = tk.BooleanVar(value=initial_validation_enabled)
        ttk.Checkbutton(self.validation_settings_tab, text="Enable Package Validation", variable=self.package_validation_enabled_var).grid(row=0, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)
    
    def setup_button_panel(self):
        """Setup button panel at the top of the window (was bottom)."""
        button_frame = ttk.Frame(self.root)
        # Place the toolbar at the very top so it's always visible regardless of
        # window size.
        button_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5, before=self.main_paned)
        
        # Left side buttons
        left_frame = ttk.Frame(button_frame)
        left_frame.pack(side=tk.LEFT)
        
        ttk.Button(left_frame, text="Add Device Type", command=self.add_device_type).pack(side=tk.LEFT, padx=2)
        ttk.Button(left_frame, text="Add Rule", command=self.add_rule).pack(side=tk.LEFT, padx=2)
        ttk.Button(left_frame, text="Delete Selected", command=self.delete_selected).pack(side=tk.LEFT, padx=2)
        
        # Right side buttons
        right_frame = ttk.Frame(button_frame)
        right_frame.pack(side=tk.RIGHT)
        ttk.Button(right_frame, text="Save As...", command=self.save_as_rules).pack(side=tk.RIGHT, padx=2)
        ttk.Button(right_frame, text="Load...", command=self.load_rules_file).pack(side=tk.RIGHT, padx=2)
        ttk.Button(right_frame, text="Apply Changes", command=self.apply_changes).pack(side=tk.RIGHT, padx=2)
    
    def load_rules(self) -> Dict[str, Any]:
        """Load rules from JSON file."""
        if os.path.exists(self.rules_file):
            try:
                with open(self.rules_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load rules file: {str(e)}")
        
        # Return default structure if file doesn't exist
        return {
            "device_types": {},
            "weight_unit_validation": {
                "allowed_units": ["lbs", "pounds"],
                "flag_units": ["oz", "ounces", "g", "grams"],
                "description": "Units that should be flagged as suspicious for device types"
            },
            "version": "1.0",
            "last_updated": "2024-12-21"
        }
    
    def save_rules(self):
        """Save rules to the current file."""
        try:
            self.apply_model_check_changes()
            self.apply_validation_settings_changes()
            with open(self.rules_file, 'w', encoding='utf-8') as f:
                json.dump(self.rules_data, f, indent=4)
            self.modified = False
            messagebox.showinfo("Success", f"Rules saved to {self.rules_file}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save rules: {str(e)}")
    
    def save_as_rules(self):
        """Save rules to a new file."""
        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            try:
                self.apply_model_check_changes()
                self.apply_validation_settings_changes()
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.rules_data, f, indent=4)
                self.rules_file = filename
                self.modified = False
                messagebox.showinfo("Success", f"Rules saved to {filename}")
                self.root.title(f"Package Validation Rules Editor - {os.path.basename(filename)}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save rules: {str(e)}")
    
    def load_rules_file(self):
        """Load rules from a file."""
        filename = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    self.rules_data = json.load(f)
                self.rules_file = filename
                self.modified = False
                self.populate_tree()
                messagebox.showinfo("Success", f"Rules loaded from {filename}")
                self.root.title(f"Package Validation Rules Editor - {os.path.basename(filename)}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load rules: {str(e)}")
    
    def populate_tree(self):
        """Populate the tree view with current rules."""
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Add device types
        device_types = self.rules_data.get("device_types", {})
        for device_name, device_rules in device_types.items():
            device_id = self.tree.insert("", "end", text=device_name, values=("device", ""), open=True)
            
            # Add rules for this device
            for rule_name, rule_data in device_rules.items():
                if rule_name == "screen_size_ranges":
                    # Handle screen size ranges
                    screen_id = self.tree.insert(device_id, "end", text="Screen Size Ranges", values=("screen_ranges", ""), open=True)
                    for size_range, size_rules in rule_data.items():
                        size_id = self.tree.insert(screen_id, "end", text=f"{size_range} inch", values=("size_range", size_range), open=True)
                        for sub_rule_name, sub_rule_data in size_rules.items():
                            description = sub_rule_data.get("description", "")
                            self.tree.insert(size_id, "end", text=sub_rule_name, values=("rule", description))
                else:
                    # Regular rule
                    description = rule_data.get("description", "")
                    self.tree.insert(device_id, "end", text=rule_name, values=("rule", description))
        
        # Add weight unit validation
        if "weight_unit_validation" in self.rules_data:
            weight_id = self.tree.insert("", "end", text="Weight Unit Validation", values=("weight_units", ""), open=True)
        
        # Add price validation
        if "price_validation" in self.rules_data:
            price_id = self.tree.insert("", "end", text="Price Validation", values=("price_validation", ""), open=True)
        
        # Add model check
        if "model_check" in self.rules_data:
            model_id = self.tree.insert("", "end", text="Model Check", values=("model_check", ""), open=True)
    
    def on_tree_select(self, event):
        """Handle tree selection change."""
        selection = self.tree.selection()
        if not selection:
            self.edit_notebook.pack_forget()
            self.edit_title.config(text="Select an item to edit")
            return
        
        item = selection[0]
        item_text = self.tree.item(item, "text")
        item_type = self.tree.item(item, "values")[0] if self.tree.item(item, "values") else ""
        
        self.edit_title.config(text=f"Editing: {item_text}")
        self.edit_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Load appropriate data based on item type
        if item_type == "device":
            self.edit_notebook.select(self.device_tab)
            self.load_device_data(item_text)
        elif item_type == "rule":
            self.edit_notebook.select(self.rule_tab)
            self.load_rule_data(item)
        elif item_type == "weight_units":
            self.edit_notebook.select(self.weight_unit_tab)
            self.load_weight_unit_data()
        elif item_type == "price_validation":
            self.edit_notebook.select(self.price_tab)
            self.load_price_validation_data()
        elif item_type == "model_check":
            self.edit_notebook.select(self.model_check_tab)
            self.load_model_check_data()
    
    def load_device_data(self, device_name):
        """Load device type data into the edit form."""
        self.device_name_var.set(device_name)
        device_data = self.rules_data.get("device_types", {}).get(device_name, {})
        self.has_screen_ranges_var.set("screen_size_ranges" in device_data)
    
    def load_rule_data(self, item):
        """Load rule data into the edit form."""
        # Navigate up the tree to find the full path
        path = []
        current_item = item
        while current_item:
            path.insert(0, self.tree.item(current_item, "text"))
            current_item = self.tree.parent(current_item)
        
        # Get the rule data
        rule_data = self.get_rule_data_by_path(path)
        if not rule_data:
            return
        
        # Load basic rule info
        self.rule_name_var.set(path[-1])
        self.rule_desc_var.set(rule_data.get("description", ""))
        self.max_lot_var.set(str(rule_data.get("max_lot_size", "")))
        
        # Load weight data - convert to display format (oz/lbs)
        if "weight" in rule_data:
            weight = rule_data["weight"]
            self.weight_min_var.set(pounds_to_display(weight.get("min", "")))
            self.weight_max_var.set(pounds_to_display(weight.get("max", "")))
        else:
            self.weight_min_var.set("")
            self.weight_max_var.set("")
        
        if "weight_per_item" in rule_data:
            weight_item = rule_data["weight_per_item"]
            self.weight_item_min_var.set(pounds_to_display(weight_item.get("min", "")))
            self.weight_item_max_var.set(pounds_to_display(weight_item.get("max", "")))
        else:
            self.weight_item_min_var.set("")
            self.weight_item_max_var.set("")
        
        self.base_packaging_var.set(pounds_to_display(rule_data.get("base_packaging_weight", "")))
        
        # Load dimension data
        self.load_dimension_data(rule_data.get("dimensions", {}))
        
        # Model check is managed globally; no per-rule model data to load
    
    def load_dimension_data(self, dim_data):
        """Load dimension data into the form."""
        if "exact" in dim_data:
            self.dim_type_var.set("exact")
            exact = dim_data["exact"]
            if len(exact) >= 3:
                self.setup_dimension_inputs()
                self.exact_l_var.set(str(exact[0]))
                self.exact_w_var.set(str(exact[1]))
                self.exact_h_var.set(str(exact[2]))
        elif "exact_options" in dim_data:
            self.dim_type_var.set("exact_options")
            self.setup_dimension_inputs()
            # Load existing exact options into the text widget
            options = dim_data["exact_options"]
            options_text = "\n".join([f"{opt[0]}, {opt[1]}, {opt[2]}" for opt in options])
            self.exact_options_text.delete(1.0, tk.END)
            self.exact_options_text.insert(1.0, options_text)
        elif "min" in dim_data:
            self.dim_type_var.set("min")
            min_dims = dim_data["min"]
            if len(min_dims) >= 3:
                self.setup_dimension_inputs()
                self.min_l_var.set(str(min_dims[0]))
                self.min_w_var.set(str(min_dims[1]))
                self.min_h_var.set(str(min_dims[2]))
        elif "max" in dim_data:
            self.dim_type_var.set("max")
            max_dims = dim_data["max"]
            if len(max_dims) >= 3:
                self.setup_dimension_inputs()
                self.max_l_var.set(str(max_dims[0]))
                self.max_w_var.set(str(max_dims[1]))
                self.max_h_var.set(str(max_dims[2]))
        elif "length" in dim_data and "width" in dim_data and "height" in dim_data:
            self.dim_type_var.set("range")
            self.setup_dimension_inputs()
            length = dim_data["length"]
            width = dim_data["width"]
            height = dim_data["height"]
            self.range_l_min_var.set(str(length.get("min", "")))
            self.range_l_max_var.set(str(length.get("max", "")))
            self.range_w_min_var.set(str(width.get("min", "")))
            self.range_w_max_var.set(str(width.get("max", "")))
            self.range_h_min_var.set(str(height.get("min", "")))
            self.range_h_max_var.set(str(height.get("max", "")))
    
    def load_weight_unit_data(self):
        """Load weight unit validation data."""
        weight_unit_data = self.rules_data.get("weight_unit_validation", {})
        allowed = weight_unit_data.get("allowed_units", [])
        flagged = weight_unit_data.get("flag_units", [])
        
        self.allowed_units_var.set(", ".join(allowed))
        self.flagged_units_var.set(", ".join(flagged))
    
    def load_price_validation_data(self):
        """Load price validation data."""
        price_data = self.rules_data.get("price_validation", {})
        
        # Load enabled status
        self.price_enabled_var.set(price_data.get("enabled", False))
        
        # Load auction prices
        auction_data = price_data.get("auction_prices", {})
        self.auction_min_var.set(str(auction_data.get("min_starting_price", "")))
        self.auction_max_var.set(str(auction_data.get("max_starting_price", "")))
        
        # Load global flags
        global_data = price_data.get("global_flags", {})
        self.global_low_var.set(str(global_data.get("suspicious_low_price", "")))
        self.global_high_var.set(str(global_data.get("suspicious_high_price", "")))
        
        # Load buy-it-now prices (read-only display)
        buyitnow_data = price_data.get("buyitnow_prices", {})
        self.buyitnow_text.delete(1.0, tk.END)
        
        if buyitnow_data:
            self.buyitnow_text.insert(tk.END, "Buy-It-Now Price Ranges by Device Type:\n\n")
            for device_type, device_prices in buyitnow_data.items():
                self.buyitnow_text.insert(tk.END, f"{device_type}:\n")
                for lot_type, price_rules in device_prices.items():
                    min_price = price_rules.get("min", "N/A")
                    max_price = price_rules.get("max", "N/A")
                    typical = price_rules.get("typical_range", "N/A")
                    self.buyitnow_text.insert(tk.END, f"  {lot_type}: ${min_price}-${max_price} (typical: ${typical})\n")
            self.buyitnow_text.insert(tk.END, "\n")
        else:
            self.buyitnow_text.insert(tk.END, "No Buy-It-Now price data configured.")
    
    def get_rule_data_by_path(self, path):
        """Get rule data by tree path."""
        if len(path) < 2:
            return None
        
        device_name = path[0]
        device_data = self.rules_data.get("device_types", {}).get(device_name, {})
        
        if len(path) == 2:
            return device_data.get(path[1], {})
        elif len(path) == 3 and path[1] == "Screen Size Ranges":
            return device_data.get("screen_size_ranges", {}).get(path[2], {})
        elif len(path) == 4 and path[1] == "Screen Size Ranges":
            return device_data.get("screen_size_ranges", {}).get(path[2], {}).get(path[3], {})
        
        return None
    
    def on_dimension_type_change(self, event):
        """Handle dimension type combo change."""
        self.setup_dimension_inputs()
    
    def show_tree_context_menu(self, event):
        """Show context menu for tree."""
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            self.tree_menu.post(event.x_root, event.y_root)
    
    def add_device_type(self):
        """Add a new device type."""
        name = simpledialog.askstring("Add Device Type", "Enter device type name:")
        if name and name not in self.rules_data.get("device_types", {}):
            if "device_types" not in self.rules_data:
                self.rules_data["device_types"] = {}
            self.rules_data["device_types"][name] = {}
            self.populate_tree()
            self.mark_modified()
    
    def add_rule(self):
        """Add a new rule to selected device type."""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a device type first")
            return
        
        item = selection[0]
        item_type = self.tree.item(item, "values")[0] if self.tree.item(item, "values") else ""
        
        if item_type != "device":
            messagebox.showwarning("Warning", "Please select a device type")
            return
        
        device_name = self.tree.item(item, "text")
        rule_name = simpledialog.askstring("Add Rule", "Enter rule name:")
        
        if rule_name:
            device_data = self.rules_data["device_types"][device_name]
            device_data[rule_name] = {
                "description": "",
                "weight": {"min": 0, "max": 10},
                "dimensions": {"exact": [10, 10, 10]},
                "max_lot_size": 1
            }
            self.populate_tree()
            self.mark_modified()
    
    def delete_selected(self):
        """Delete selected item."""
        selection = self.tree.selection()
        if not selection:
            return
        
        if messagebox.askyesno("Confirm Delete", "Are you sure you want to delete the selected item?"):
            # Implementation for deleting items would go here
            # This is a simplified version
            messagebox.showinfo("Info", "Delete functionality would be implemented here")
            self.mark_modified()
    
    def edit_selected(self):
        """Edit selected item (same as double-click)."""
        selection = self.tree.selection()
        if selection:
            self.on_tree_select(None)
    
    def apply_changes(self):
        """Apply changes from the edit form."""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = selection[0]
        item_type = self.tree.item(item, "values")[0] if self.tree.item(item, "values") else ""
        
        if item_type == "weight_units":
            self.apply_weight_unit_changes()
        elif item_type == "price_validation":
            self.apply_price_validation_changes()
        elif item_type == "rule":
            self.apply_rule_changes(item)
        elif item_type == "device":
            self.apply_device_changes(item)
        
            self.populate_tree()
        messagebox.showinfo("Success", "Changes applied")
        self.mark_modified()
    
    def apply_weight_unit_changes(self):
        """Apply weight unit validation changes."""
        allowed = [unit.strip() for unit in self.allowed_units_var.get().split(",") if unit.strip()]
        flagged = [unit.strip() for unit in self.flagged_units_var.get().split(",") if unit.strip()]
        
        self.rules_data["weight_unit_validation"] = {
            "allowed_units": allowed,
            "flag_units": flagged,
            "description": "Units that should be flagged as suspicious for device types"
        }
    
    def apply_price_validation_changes(self):
        """Apply price validation changes."""
        if "price_validation" not in self.rules_data:
            self.rules_data["price_validation"] = {}
        
        price_data = self.rules_data["price_validation"]
        
        # Update enabled status
        price_data["enabled"] = self.price_enabled_var.get()
        
        # Update auction prices
        if "auction_prices" not in price_data:
            price_data["auction_prices"] = {}
        
        try:
            if self.auction_min_var.get():
                price_data["auction_prices"]["min_starting_price"] = float(self.auction_min_var.get())
            if self.auction_max_var.get():
                price_data["auction_prices"]["max_starting_price"] = float(self.auction_max_var.get())
        except ValueError:
            pass
        
        # Update global flags
        if "global_flags" not in price_data:
            price_data["global_flags"] = {}
        
        try:
            if self.global_low_var.get():
                price_data["global_flags"]["suspicious_low_price"] = float(self.global_low_var.get())
            if self.global_high_var.get():
                price_data["global_flags"]["suspicious_high_price"] = float(self.global_high_var.get())
        except ValueError:
            pass
        
        # Update Buy-It-Now ranges (expects valid JSON)
        try:
            bin_text = self.buyitnow_text.get("1.0", tk.END).strip()
            if bin_text:
                price_data["buyitnow_prices"] = json.loads(bin_text)
        except json.JSONDecodeError:
            messagebox.showwarning("Invalid JSON", "Buy-It-Now price ranges must be valid JSON. Changes ignored.")
        except Exception:
            pass
    
    def apply_rule_changes(self, item):
        """Apply rule changes."""
        # Get the path to the rule
        path = []
        current_item = item
        while current_item:
            path.insert(0, self.tree.item(current_item, "text"))
            current_item = self.tree.parent(current_item)
        
        # Get rule data reference
        rule_data = self.get_rule_data_by_path(path)
        if not rule_data:
            return
        
        # Update rule data
        rule_data["description"] = self.rule_desc_var.get()
        
        if self.max_lot_var.get():
            try:
                rule_data["max_lot_size"] = int(self.max_lot_var.get())
            except ValueError:
                pass
        
        # Update weight data - convert from display format back to pounds
        if self.weight_min_var.get() or self.weight_max_var.get():
            rule_data["weight"] = {}
        if self.weight_min_var.get():
                min_pounds = display_to_pounds(self.weight_min_var.get())
                if min_pounds is not None:
                    rule_data["weight"]["min"] = min_pounds
        if self.weight_max_var.get():
                max_pounds = display_to_pounds(self.weight_max_var.get())
                if max_pounds is not None:
                    rule_data["weight"]["max"] = max_pounds
        
        if self.weight_item_min_var.get() or self.weight_item_max_var.get():
            rule_data["weight_per_item"] = {}
        if self.weight_item_min_var.get():
                min_pounds = display_to_pounds(self.weight_item_min_var.get())
                if min_pounds is not None:
                    rule_data["weight_per_item"]["min"] = min_pounds
        if self.weight_item_max_var.get():
                max_pounds = display_to_pounds(self.weight_item_max_var.get())
                if max_pounds is not None:
                    rule_data["weight_per_item"]["max"] = max_pounds
        
        if self.base_packaging_var.get():
            packaging_pounds = display_to_pounds(self.base_packaging_var.get())
            if packaging_pounds is not None:
                rule_data["base_packaging_weight"] = packaging_pounds
        
        # Model check settings are managed in the dedicated Model Check tab; no per-rule fields here
        
        # Update dimension data
        self.apply_dimension_changes(rule_data)
    
    def apply_dimension_changes(self, rule_data):
        """Apply dimension changes to rule data."""
        dim_type = self.dim_type_var.get()
        
        if dim_type == "exact":
            try:
                l = float(self.exact_l_var.get())
                w = float(self.exact_w_var.get())
                h = float(self.exact_h_var.get())
                rule_data["dimensions"] = {
                    "exact": [l, w, h],
                    "description": f"{l} x {w} x {h} inches (exact)"
                }
            except ValueError:
                pass
        elif dim_type == "exact_options":
            try:
                options_text = self.exact_options_text.get(1.0, tk.END).strip()
                options = []
                for line in options_text.split('\n'):
                    if line.strip():
                        parts = [float(x.strip()) for x in line.split(',')]
                        if len(parts) == 3:
                            options.append(parts)
                rule_data["dimensions"] = {
                    "exact_options": options,
                    "description": " or ".join([f"{opt[0]} x {opt[1]} x {opt[2]} inches" for opt in options])
                }
            except ValueError:
                pass
        elif dim_type == "min":
            try:
                l = float(self.min_l_var.get())
                w = float(self.min_w_var.get())
                h = float(self.min_h_var.get())
                rule_data["dimensions"] = {
                    "min": [l, w, h],
                    "description": f"At least {l} x {w} x {h} inches"
                }
            except ValueError:
                pass
        elif dim_type == "max":
            try:
                l = float(self.max_l_var.get())
                w = float(self.max_w_var.get())
                h = float(self.max_h_var.get())
                rule_data["dimensions"] = {
                    "max": [l, w, h],
                    "description": f"Up to {l} x {w} x {h} inches (range)"
                }
            except ValueError:
                pass
        elif dim_type == "range":
            try:
                rule_data["dimensions"] = {
                    "length": {
                        "min": float(self.range_l_min_var.get()),
                        "max": float(self.range_l_max_var.get())
                    },
                    "width": {
                        "min": float(self.range_w_min_var.get()),
                        "max": float(self.range_w_max_var.get())
                    },
                    "height": {
                        "min": float(self.range_h_min_var.get()),
                        "max": float(self.range_h_max_var.get())
                    }
                }
            except ValueError:
                pass
    
    def apply_device_changes(self, item):
        """Apply device type changes."""
        old_name = self.tree.item(item, "text")
        new_name = self.device_name_var.get()
        
        if old_name != new_name and new_name:
            # Rename device type
            device_data = self.rules_data["device_types"].pop(old_name)
            self.rules_data["device_types"][new_name] = device_data
    
    def load_model_check_data(self):
        """Load model check data into listbox and variables."""
        self.model_rules = self.rules_data.get("model_check", {}).get("rules", []) if self.rules_data.get("model_check") else []
        # Sync enable checkbox with stored value
        self.model_check_enabled_var.set(self.rules_data.get("model_check", {}).get("enabled", False))
        # Populate listbox
        self.model_rules_listbox.delete(0, tk.END)
        for rule in self.model_rules:
            self.model_rules_listbox.insert(tk.END, rule.get("match_text", "(Unnamed)"))
        # Select first item by default
        if self.model_rules:
            self.model_rules_listbox.selection_set(0)
            self.on_model_rule_select(None)

    def on_model_rule_select(self, event):
        """Handle selection in the model rules listbox."""
        selection = self.model_rules_listbox.curselection()
        if not selection:
            return

        index = selection[0]
        rule = self.model_rules[index]

        # ---------------- Basic fields ----------------
        self.model_name_var.set(rule.get("match_text", ""))
        weight = rule.get("weight", {})
        self.model_weight_min_var.set(pounds_to_display(weight.get("min", "")))
        self.model_weight_max_var.set(pounds_to_display(weight.get("max", "")))
        wpi = rule.get("weight_per_item", {})
        self.model_weight_item_min_var.set(pounds_to_display(wpi.get("min", "")))
        self.model_weight_item_max_var.set(pounds_to_display(wpi.get("max", "")))
        self.model_base_packaging_var.set(pounds_to_display(rule.get("base_packaging_weight", "")))

        # ---------------- Dimension fields ----------------
        dim_data = rule.get("dimensions", {})
        if "exact" in dim_data:
            dim_type = "exact"
        elif "exact_options" in dim_data:
            dim_type = "exact_options"
        elif "min" in dim_data:
            dim_type = "min"
        elif {"length", "width", "height"}.issubset(dim_data.keys()):
            dim_type = "range"
        else:
            dim_type = "max"

        # Update combo box and refresh inputs
        self.model_dim_type_var.set(dim_type)
        self.setup_model_dimension_inputs()

        if dim_type == "exact":
            dims = dim_data.get("exact", [])
            if len(dims) == 3:
                self.model_exact_l_var.set(str(dims[0]))
                self.model_exact_w_var.set(str(dims[1]))
                self.model_exact_h_var.set(str(dims[2]))
        elif dim_type == "exact_options":
            self.model_exact_options_text.delete(1.0, tk.END)
            for opt in dim_data.get("exact_options", []):
                if len(opt) == 3:
                    self.model_exact_options_text.insert(tk.END, f"{opt[0]}, {opt[1]}, {opt[2]}\n")
        elif dim_type == "min":
            dims = dim_data.get("min", [])
            if len(dims) == 3:
                self.model_min_l_var.set(str(dims[0]))
                self.model_min_w_var.set(str(dims[1]))
                self.model_min_h_var.set(str(dims[2]))
        elif dim_type == "range":
            length = dim_data.get("length", {})
            width = dim_data.get("width", {})
            height = dim_data.get("height", {})
            self.model_range_l_min_var.set(str(length.get("min", "")))
            self.model_range_l_max_var.set(str(length.get("max", "")))
            self.model_range_w_min_var.set(str(width.get("min", "")))
            self.model_range_w_max_var.set(str(width.get("max", "")))
            self.model_range_h_min_var.set(str(height.get("min", "")))
            self.model_range_h_max_var.set(str(height.get("max", "")))
        else:  # max
            dims = dim_data.get("max", [])
            if len(dims) == 3:
                self.model_max_l_var.set(str(dims[0]))
                self.model_max_w_var.set(str(dims[1]))
                self.model_max_h_var.set(str(dims[2]))
            else:
                self.model_max_l_var.set("")
                self.model_max_w_var.set("")
                self.model_max_h_var.set("")

        # ---------------- Price & misc fields ----------------
        self.use_typical_box_var.set(rule.get("use_typical_box_sizes", False))

        pv = rule.get("price_validation", {})
        auc = pv.get("auction_prices", {})
        self.model_auc_min_var.set(str(auc.get("min_starting_price", "")))
        self.model_auc_max_var.set(str(auc.get("max_starting_price", "")))

        bin_prices = pv.get("buyitnow_prices", {})
        single = bin_prices.get("single", {})
        small = bin_prices.get("small_lot", {})
        large = bin_prices.get("large_lot", {})
        self.model_bin_single_min_var.set(str(single.get("min", "")))
        self.model_bin_single_max_var.set(str(single.get("max", "")))
        self.model_bin_single_typ_var.set(single.get("typical_range", ""))
        self.model_bin_small_min_var.set(str(small.get("min", "")))
        self.model_bin_small_max_var.set(str(small.get("max", "")))
        self.model_bin_small_typ_var.set(small.get("typical_range", ""))
        self.model_bin_large_min_var.set(str(large.get("min", "")))
        self.model_bin_large_max_var.set(str(large.get("max", "")))
        self.model_bin_large_typ_var.set(large.get("typical_range", ""))

    def setup_model_dimension_inputs(self):
        """Create / refresh the dimension-input widgets for the Model Check tab
        based on the currently selected dimension type."""
        # Clear any existing widgets first
        for w in self.model_dim_input_frame.winfo_children():
            w.destroy()

        dim_type = self.model_dim_type_var.get()

        # ------------------------ EXACT ------------------------
        if dim_type == "exact":
            ttk.Label(self.model_dim_input_frame,
                      text="Exact Dimensions (L × W × H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.model_exact_l_var = tk.StringVar()
            self.model_exact_w_var = tk.StringVar()
            self.model_exact_h_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_exact_l_var, width=8).grid(row=0, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_exact_w_var, width=8).grid(row=0, column=3, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=4)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_exact_h_var, width=8).grid(row=0, column=5, padx=2)
            ttk.Label(self.model_dim_input_frame, text="inches").grid(row=0, column=6, padx=2)

        # -------------------- EXACT OPTIONS --------------------
        elif dim_type == "exact_options":
            ttk.Label(self.model_dim_input_frame,
                      text="Exact Options (one per line, L,W,H):").grid(row=0, column=0, sticky=tk.W)
            self.model_exact_options_text = tk.Text(self.model_dim_input_frame, width=40, height=4)
            self.model_exact_options_text.grid(row=1, column=0, columnspan=4, sticky=tk.W + tk.E)

        # ------------------------- RANGE -----------------------
        elif dim_type == "range":
            # Length
            ttk.Label(self.model_dim_input_frame, text="Length Range:").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.model_range_l_min_var = tk.StringVar()
            self.model_range_l_max_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_l_min_var, width=8).grid(row=0, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="to").grid(row=0, column=2, padx=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_l_max_var, width=8).grid(row=0, column=3, padx=2)
            # Width
            ttk.Label(self.model_dim_input_frame, text="Width Range:").grid(row=1, column=0, sticky=tk.W, pady=2)
            self.model_range_w_min_var = tk.StringVar()
            self.model_range_w_max_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_w_min_var, width=8).grid(row=1, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="to").grid(row=1, column=2, padx=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_w_max_var, width=8).grid(row=1, column=3, padx=2)
            # Height
            ttk.Label(self.model_dim_input_frame, text="Height Range:").grid(row=2, column=0, sticky=tk.W, pady=2)
            self.model_range_h_min_var = tk.StringVar()
            self.model_range_h_max_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_h_min_var, width=8).grid(row=2, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="to").grid(row=2, column=2, padx=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_range_h_max_var, width=8).grid(row=2, column=3, padx=2)

        # -------------------------- MIN ------------------------
        elif dim_type == "min":
            ttk.Label(self.model_dim_input_frame, text="Min Dimensions (L × W × H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.model_min_l_var = tk.StringVar()
            self.model_min_w_var = tk.StringVar()
            self.model_min_h_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_min_l_var, width=8).grid(row=0, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_min_w_var, width=8).grid(row=0, column=3, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=4)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_min_h_var, width=8).grid(row=0, column=5, padx=2)
            ttk.Label(self.model_dim_input_frame, text="inches").grid(row=0, column=6, padx=2)

        # ------------------------- MAX -------------------------
        else:  # "max" (default)
            ttk.Label(self.model_dim_input_frame,
                      text="Max Dimensions (L × W × H):").grid(row=0, column=0, sticky=tk.W, pady=2)
            self.model_max_l_var = tk.StringVar()
            self.model_max_w_var = tk.StringVar()
            self.model_max_h_var = tk.StringVar()
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_max_l_var, width=8).grid(row=0, column=1, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=2)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_max_w_var, width=8).grid(row=0, column=3, padx=2)
            ttk.Label(self.model_dim_input_frame, text="×").grid(row=0, column=4)
            ttk.Entry(self.model_dim_input_frame, textvariable=self.model_max_h_var, width=8).grid(row=0, column=5, padx=2)
            ttk.Label(self.model_dim_input_frame, text="inches").grid(row=0, column=6, padx=2)

    def _collect_current_model_rule(self):
        """Collect data from UI into a model rule dict."""
        rule = {
            "match_text": self.model_name_var.get().strip(),
            "use_typical_box_sizes": self.use_typical_box_var.get()
        }
        # Weight
        if self.model_weight_min_var.get() or self.model_weight_max_var.get():
            rule["weight"] = {}
            if self.model_weight_min_var.get():
                w_min = display_to_pounds(self.model_weight_min_var.get())
                if w_min is not None:
                    rule["weight"]["min"] = w_min
            if self.model_weight_max_var.get():
                w_max = display_to_pounds(self.model_weight_max_var.get())
                if w_max is not None:
                    rule["weight"]["max"] = w_max
        if self.model_weight_item_min_var.get() or self.model_weight_item_max_var.get():
            rule["weight_per_item"] = {}
            if self.model_weight_item_min_var.get():
                wpi_min = display_to_pounds(self.model_weight_item_min_var.get())
                if wpi_min is not None:
                    rule["weight_per_item"]["min"] = wpi_min
            if self.model_weight_item_max_var.get():
                wpi_max = display_to_pounds(self.model_weight_item_max_var.get())
                if wpi_max is not None:
                    rule["weight_per_item"]["max"] = wpi_max
        # Base packaging
        if self.model_base_packaging_var.get():
            bp = display_to_pounds(self.model_base_packaging_var.get())
            if bp is not None:
                rule["base_packaging_weight"] = bp
        # ---------------- Dimensions ----------------
        dim_type = self.model_dim_type_var.get()
        try:
            if dim_type == "exact":
                if self.model_exact_l_var.get() and self.model_exact_w_var.get() and self.model_exact_h_var.get():
                    l = float(self.model_exact_l_var.get())
                    w = float(self.model_exact_w_var.get())
                    h = float(self.model_exact_h_var.get())
                    rule["dimensions"] = {"exact": [l, w, h]}
            elif dim_type == "exact_options":
                opts_raw = self.model_exact_options_text.get(1.0, tk.END).strip()
                opts = []
                for line in opts_raw.split("\n"):
                    if line.strip():
                        parts = [float(x.strip()) for x in line.split(',') if x.strip()]
                        if len(parts) == 3:
                            opts.append(parts)
                if opts:
                    rule["dimensions"] = {"exact_options": opts}
            elif dim_type == "min":
                if self.model_min_l_var.get() and self.model_min_w_var.get() and self.model_min_h_var.get():
                    l = float(self.model_min_l_var.get())
                    w = float(self.model_min_w_var.get())
                    h = float(self.model_min_h_var.get())
                    rule["dimensions"] = {"min": [l, w, h]}
            elif dim_type == "range":
                lmin = float(self.model_range_l_min_var.get()) if self.model_range_l_min_var.get() else None
                lmax = float(self.model_range_l_max_var.get()) if self.model_range_l_max_var.get() else None
                wmin = float(self.model_range_w_min_var.get()) if self.model_range_w_min_var.get() else None
                wmax = float(self.model_range_w_max_var.get()) if self.model_range_w_max_var.get() else None
                hmin = float(self.model_range_h_min_var.get()) if self.model_range_h_min_var.get() else None
                hmax = float(self.model_range_h_max_var.get()) if self.model_range_h_max_var.get() else None
                rule["dimensions"] = {
                    "length": {"min": lmin, "max": lmax},
                    "width":  {"min": wmin, "max": wmax},
                    "height": {"min": hmin, "max": hmax}
                }
            else:  # max
                if self.model_max_l_var.get() and self.model_max_w_var.get() and self.model_max_h_var.get():
                    l = float(self.model_max_l_var.get())
                    w = float(self.model_max_w_var.get())
                    h = float(self.model_max_h_var.get())
                    rule["dimensions"] = {"max": [l, w, h]}
        except ValueError:
            pass  # Ignore bad numbers; user will see on save
        # Price validation
        pv = {
            "enabled": True,
            "auction_prices": {},
            "buyitnow_prices": {}
        }
        try:
            if self.model_auc_min_var.get():
                pv["auction_prices"]["min_starting_price"] = float(self.model_auc_min_var.get())
            if self.model_auc_max_var.get():
                pv["auction_prices"]["max_starting_price"] = float(self.model_auc_max_var.get())
        except ValueError:
            pass
        def _bin(bmin,bmax,btyp):
            band = {}
            try:
                if bmin: band["min"] = float(bmin)
                if bmax: band["max"] = float(bmax)
            except ValueError:
                pass
            if btyp:
                band["typical_range"] = btyp
            return band
        single = _bin(self.model_bin_single_min_var.get(), self.model_bin_single_max_var.get(), self.model_bin_single_typ_var.get())
        small  = _bin(self.model_bin_small_min_var.get(),  self.model_bin_small_max_var.get(),  self.model_bin_small_typ_var.get())
        large  = _bin(self.model_bin_large_min_var.get(),  self.model_bin_large_max_var.get(),  self.model_bin_large_typ_var.get())
        if single: pv["buyitnow_prices"]["single"] = single
        if small:  pv["buyitnow_prices"]["small_lot"] = small
        if large:  pv["buyitnow_prices"]["large_lot"] = large
        if pv["auction_prices"] or pv["buyitnow_prices"]:
            rule["price_validation"] = pv
        return rule

    def add_model_rule(self):
        """Add a new model rule."""
        new_rule = self._collect_current_model_rule()
        if not new_rule.get("match_text"):
            messagebox.showwarning("Validation", "Model Name cannot be empty")
            return
        self.model_rules.append(new_rule)
        self.model_rules_listbox.insert(tk.END, new_rule["match_text"])
        self.model_rules_listbox.selection_clear(0, tk.END)
        self.model_rules_listbox.selection_set(tk.END)
        self.on_model_rule_select(None)
        self.mark_modified()

    def edit_model_rule(self):
        """Edit the selected model rule with current form values."""
        selection = self.model_rules_listbox.curselection()
        if not selection:
            messagebox.showwarning("Selection", "Please select a model rule to edit")
            return
        index = selection[0]
        updated_rule = self._collect_current_model_rule()
        if not updated_rule.get("match_text"):
            messagebox.showwarning("Validation", "Model Name cannot be empty")
            return
        self.model_rules[index] = updated_rule
        self.model_rules_listbox.delete(index)
        self.model_rules_listbox.insert(index, updated_rule["match_text"])
        self.model_rules_listbox.selection_set(index)
        self.mark_modified()

    def delete_model_rule(self):
        """Delete the selected model rule."""
        selection = self.model_rules_listbox.curselection()
        if not selection:
            return
        index = selection[0]
        self.model_rules.pop(index)
        self.model_rules_listbox.delete(index)
        self.mark_modified()

    def apply_model_check_changes(self):
        """Persist model check settings to rules_data."""
        # First, make sure the form values for the currently selected model rule
        # are copied back into self.model_rules – this guarantees that toggling
        # the "Use Typical Box Sizes" checkbox (or price fields, etc.) is not
        # lost when the user clicks the top-level Save without pressing the
        # "Edit Model" button.
        sel = self.model_rules_listbox.curselection()
        if sel:
            idx = sel[0]
            if 0 <= idx < len(self.model_rules):
                self.model_rules[idx] = self._collect_current_model_rule()
                self.modified = True
 
        self.rules_data["model_check"] = {
            "enabled": self.model_check_enabled_var.get(),
            "rules": self.model_rules
        }

    def apply_validation_settings_changes(self):
        """Persist validation settings (e.g., global toggles) to rules_data."""
        if "validation_settings" not in self.rules_data:
            self.rules_data["validation_settings"] = {}
        self.rules_data["validation_settings"]["package_validation_enabled"] = self.package_validation_enabled_var.get()
    
    def on_closing(self):
        """Handle window closing."""
        if self.modified:
            result = messagebox.askyesnocancel("Unsaved Changes", 
                                             "You have unsaved changes. Do you want to save before closing?")
            if result is True:
                self.save_rules()
                self.root.destroy()
            elif result is False:
                self.root.destroy()
            # If None (Cancel), do nothing
        else:
            self.root.destroy()

    def on_model_dimension_type_change(self, event):
        """Rebuild dimension-input widgets when the type changes."""
        self.setup_model_dimension_inputs()

    # ----------------------- Auto-save support -----------------------
    def auto_save(self):
        """Silently save the current rules to disk. This is invoked automatically
        whenever the data is marked as modified so the user doesn't need to
        press a dedicated Save button."""
        try:
            # Ensure in-memory data reflects the latest UI state before saving.
            self.apply_model_check_changes()
            self.apply_validation_settings_changes()
            with open(self.rules_file, "w", encoding="utf-8") as f:
                json.dump(self.rules_data, f, indent=4)
            self.modified = False
        except Exception as exc:
            # Do not interrupt the user with dialogs for background saves.
            print(f"[Auto-save] Failed to save rules: {exc}")

    def mark_modified(self):
        """Mark the data as modified and trigger an auto-save."""
        self.modified = True
        # Defer the save slightly so that rapid consecutive edits don't thrash
        # the disk – this debounces multiple calls within ~200 ms.
        if hasattr(self, "_autosave_after_id") and self._autosave_after_id:
            self.root.after_cancel(self._autosave_after_id)
        self._autosave_after_id = self.root.after(200, self.auto_save)

def main():
    root = tk.Tk()
    app = PackageValidationEditor(root)
    root.mainloop()

if __name__ == "__main__":
    main()