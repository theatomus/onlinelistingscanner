import re
import logging
from collections import Counter, defaultdict
from .base import get_globals_from_main, log_debug, is_range_format, check_range_compatibility

def compare_title_vs_table(listing_data, sections, is_power_adapter, multiple_entries):
    """Compare title data against table data."""
    # Make absolutely sure we're only using passed parameters with isolated copies
    title = dict(listing_data.get('title', {}))
    shared_values = dict(listing_data.get('table_shared', {}))
    table_entries = [dict(entry) for entry in listing_data.get('table_data', [])]
    specs = {k.replace('specs_', '').replace('_key', ''): v for k, v in listing_data.get('specifics', {}).items()}
    
    title_vs_table = []
    title_vs_table_issues = []
    issue_strings = []
    consolidated_title_vs_table = []

    # Get globals from main module
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    current_session_id = globals_dict['current_session_id']
    format_comparison = globals_dict['format_comparison']
    check_equivalence = globals_dict['check_equivalence']
    load_key_mappings = globals_dict['load_key_mappings']

    # Log to make sure we're not getting contaminated data
    logger.debug(f"[Comparison] TITLE-TABLE START - Title keys: {list(title.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"[Comparison] TITLE-TABLE START - Shared keys: {list(shared_values.keys())}", extra={'session_id': current_session_id})
    for idx, entry in enumerate(table_entries, 1):
        logger.debug(f"[Comparison] TITLE-TABLE START - Entry {idx}: {list(entry.keys())}", extra={'session_id': current_session_id})

    # Helper function to check if a value is in range format (e.g., "32GB-256GB" or "4GB-16GB")
    def is_range_format(value):
        if not value:
            return False
        return bool(re.match(r'^(\d+(?:\.\d+)?)(gb|mb|tb)-(\d+(?:\.\d+)?)(gb|mb|tb)$', value.lower().strip()))

    # Helper function to check if all values fall within a range
    def check_range_compatibility(range_value, individual_values, value_type="storage"):
        """
        Check if all individual values fall within the specified range.
        
        Args:
            range_value (str): Range like "32GB-256GB" or "4GB-16GB"
            individual_values (list): List of individual values like ["128GB", "256GB", "32GB"]
            value_type (str): "storage" or "ram" for logging purposes
            
        Returns:
            bool: True if all individual values fall within the range
        """
        if not range_value or not individual_values:
            return False
            
        # Parse the range
        range_match = re.match(r'^(\d+(?:\.\d+)?)(gb|mb|tb)-(\d+(?:\.\d+)?)(gb|mb|tb)$', range_value.lower().strip())
        if not range_match:
            return False
            
        min_size, min_unit, max_size, max_unit = range_match.groups()
        min_size, max_size = float(min_size), float(max_size)
        
        # Convert to consistent units (GB)
        unit_multipliers = {'mb': 0.001, 'gb': 1, 'tb': 1000}
        min_size_gb = min_size * unit_multipliers.get(min_unit, 1)
        max_size_gb = max_size * unit_multipliers.get(max_unit, 1)
        
        # Check each individual value
        for value in individual_values:
            if not value or not value.strip():
                continue
                
            value_match = re.match(r'^(\d+(?:\.\d+)?)(gb|mb|tb)$', value.lower().strip())
            if not value_match:
                logger.debug(f"Invalid {value_type} format: '{value}'", extra={'session_id': current_session_id})
                return False
                
            size, unit = value_match.groups()
            size = float(size)
            size_gb = size * unit_multipliers.get(unit, 1)
            
            # Check if this value falls within the range
            if not (min_size_gb <= size_gb <= max_size_gb):
                logger.debug(f"{value_type.title()} value '{value}' ({size_gb}GB) is outside range '{range_value}' ({min_size_gb}GB-{max_size_gb}GB)", 
                           extra={'session_id': current_session_id})
                return False
                
        logger.debug(f"All {value_type} values {individual_values} fall within range '{range_value}'", 
                   extra={'session_id': current_session_id})
        return True

    if not table_entries and shared_values:
        table_entries = [{}]
        
    if not table_entries and not shared_values:
        return title_vs_table, title_vs_table_issues, issue_strings, consolidated_title_vs_table

    # Normalize title keys by removing prefixes and suffixes
    normalized_title = {}
    for k, v in title.items():
        clean_key = k
        # Remove title_ prefix
        if clean_key.startswith('title_'):
            clean_key = clean_key[6:]  # Remove 'title_'
        # Remove _key suffix
        if clean_key.endswith('_key'):
            clean_key = clean_key[:-4]  # Remove '_key'
        normalized_title[clean_key] = v
    
    # Get ALL table keys from THIS ITEM ONLY (both shared and entry-specific)
    all_table_keys = set(shared_values.keys())
    for entry in table_entries:
        all_table_keys.update(entry.keys())
    
    # Normalize table keys the same way
    normalized_table_keys = {}
    for k in all_table_keys:
        clean_key = k
        # Remove table_ prefix
        if clean_key.startswith('table_'):
            clean_key = clean_key[6:]  # Remove 'table_'
        # Remove _key suffix
        if clean_key.endswith('_key'):
            clean_key = clean_key[:-4]  # Remove '_key'
        normalized_table_keys[clean_key] = k  # Map clean key to original key
    
    logger.debug(f"[Comparison] TITLE-TABLE - Normalized title keys: {list(normalized_title.keys())}", extra={'session_id': current_session_id})
    logger.debug(f"[Comparison] TITLE-TABLE - Normalized table keys: {list(normalized_table_keys.keys())}", extra={'session_id': current_session_id})
    
    # Find base key matches (removing trailing digits)
    def get_base_key(key):
        return re.sub(r'\d+$', '', key)
    
    # Group title keys by base
    title_base_groups = {}
    for key in normalized_title.keys():
        base = get_base_key(key)
        if base not in title_base_groups:
            title_base_groups[base] = []
        title_base_groups[base].append(key)
    
    # Group table keys by base
    table_base_groups = {}
    for key in normalized_table_keys.keys():
        base = get_base_key(key)
        if base not in table_base_groups:
            table_base_groups[base] = []
        table_base_groups[base].append(key)
    
    # Find common base keys
    common_base_keys = set(title_base_groups.keys()) & set(table_base_groups.keys())

    # --- User-defined key mappings (Title <-> Table) ---
    # Build maps of normalized base keys according to user mappings from key_mappings.json
    user_title_to_table = {}
    user_table_to_title = {}
    try:
        user_mappings = load_key_mappings().get("mappings", [])
    except Exception:
        user_mappings = []
    for m in user_mappings:
        s1 = (m.get("section1") or "").strip().lower()
        s2 = (m.get("section2") or "").strip().lower()
        k1 = (m.get("key1") or "").strip()
        k2 = (m.get("key2") or "").strip()
        # Normalize to base keys (remove _key suffix if present)
        b1 = re.sub(r'_key$', '', k1)
        b2 = re.sub(r'_key$', '', k2)
        if s1 == 'title' and s2 == 'table':
            user_title_to_table[b1] = b2
        elif s1 == 'table' and s2 == 'title':
            user_table_to_title[b1] = b2

    # Fold user-mapped pairs into common_base_keys when both sides exist in their respective domains
    for t_base, tbl_base in user_title_to_table.items():
        if t_base in title_base_groups and (tbl_base in table_base_groups or tbl_base in normalized_table_keys):
            common_base_keys.add(t_base)
    for tbl_base, t_base in user_table_to_title.items():
        if t_base in title_base_groups and (tbl_base in table_base_groups or tbl_base in normalized_table_keys):
            common_base_keys.add(t_base)
    
    # NEW: Extract leaf category for RAM-specific mappings
    leaf_category = None
    for line in sections.get('CATEGORY', []):
        if '[leaf_category_key]' in line:
            parts = line.split(': ', 1)
            if len(parts) == 2:
                leaf_category = parts[1].strip()
            break
    
    # NEW: Special handling for RAM category key mappings
    ram_key_mappings = {}
    if leaf_category and ('memory' in leaf_category.lower() or 'ram' in leaf_category.lower()):
        logger.debug(f"[Comparison] TITLE-TABLE - Applying RAM category mappings for: {leaf_category}", extra={'session_id': current_session_id})
        
        # Define RAM-specific key mappings (title_key -> table_key)
        ram_key_mappings = {
            'ram_application': 'application',
            'ram_capacity': 'capacity', 
            'ram_modules': 'modules',
            'ram_config': 'config',
            'ram_error_correction': 'error_correction',
            'ram_brand': 'manufacturer',
            'ram_speed_grade': 'speed_grade_range',
            'ram_total': 'total_capacity',
        }
        
        # Check for these specific mappings and add to common keys
        for title_key, table_key in ram_key_mappings.items():
            if title_key in normalized_title and table_key in normalized_table_keys:
                common_base_keys.add(title_key)  # Use title key as the base
                logger.debug(f"[Comparison] TITLE-TABLE - RAM mapping: {title_key} -> {table_key}", extra={'session_id': current_session_id})
    
    # Determine which keys to actually compare
    keys_to_compare = set()
    keys_to_skip = set()  # Individual numbered keys that will be handled by base key aggregation
    
    for base_key in common_base_keys:
        title_variants = title_base_groups.get(base_key, [base_key] if base_key in normalized_title else [])
        table_variants = table_base_groups.get(base_key, [base_key] if base_key in normalized_table_keys else [])
        
        # Check if we have numbered variants (more than just the base key, or keys ending with digits)
        title_has_numbered = any(key != base_key and re.search(r'\d+$', key) for key in title_variants)
        table_has_numbered = any(key != base_key and re.search(r'\d+$', key) for key in table_variants)
        
        if title_has_numbered or table_has_numbered:
            # Use aggregated base key comparison
            keys_to_compare.add(base_key)
            # Skip individual numbered variants
            for key in title_variants:
                if key != base_key and re.search(r'\d+$', key):
                    keys_to_skip.add(key)
            for key in table_variants:
                if key != base_key and re.search(r'\d+$', key):
                    keys_to_skip.add(key)
            logger.debug(f"[Comparison] TITLE-TABLE - Will aggregate {base_key}, skipping: {keys_to_skip}", extra={'session_id': current_session_id})
        else:
            # Use exact key matching and include user-defined mappings in either direction
            mapped_tbl = user_title_to_table.get(base_key)
            # Any table bases that map to this title base via reverse mapping
            reverse_tbl_bases = [tbl for tbl, t in user_table_to_title.items() if t == base_key]
            table_has_base_or_mapped = (
                base_key in normalized_table_keys or
                (mapped_tbl is not None and (mapped_tbl in normalized_table_keys or mapped_tbl in table_base_groups)) or
                any((tbl in normalized_table_keys or tbl in table_base_groups) for tbl in reverse_tbl_bases) or
                base_key in ram_key_mappings
            )
            if base_key in normalized_title and table_has_base_or_mapped:
                keys_to_compare.add(base_key)
    
    # Also add exact matches that aren't part of numbered groups
    exact_matches = set(normalized_title.keys()) & set(normalized_table_keys.keys())
    for key in exact_matches:
        if key not in keys_to_skip:
            keys_to_compare.add(key)
    
    # Exclude cpu_suffix from comparisons
    if 'cpu_suffix' in keys_to_compare:
        keys_to_compare.remove('cpu_suffix')
        logger.debug("[Comparison] TITLE-TABLE - Excluded cpu_suffix from comparison", extra={'session_id': current_session_id})
    
    logger.debug(f"[Comparison] TITLE-TABLE - Keys to compare: {keys_to_compare}", extra={'session_id': current_session_id})
    logger.debug(f"[Comparison] TITLE-TABLE - Keys to skip: {keys_to_skip}", extra={'session_id': current_session_id})

    # Function to collect all values for a given base key
    def collect_title_values(base_key):
        """Collect all title values for a base key (including numbered variants)."""
        values = set()
        
        # Check for exact match
        if base_key in normalized_title:
            val = normalized_title[base_key]
            if val and val.strip():
                for item in re.split(r'[\/,]', val):
                    if item.strip():
                        values.add(item.strip())
        
        # Check for numbered variants (e.g., cpu_family1, cpu_family2)
        for key in normalized_title:
            if key.startswith(base_key) and key != base_key:
                suffix = key[len(base_key):]
                if suffix.isdigit():  # This is a numbered variant
                    val = normalized_title[key]
                    if val and val.strip():
                        for item in re.split(r'[\/,]', val):
                            if item.strip():
                                values.add(item.strip())
        
        return sorted(list(values))
    
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

    def collect_table_values(base_key):
        """Collect all table values for a base key (from shared values and entries)."""
        values = set()
        
        # NEW: Check for RAM key mappings
        search_keys = [base_key]
        if base_key in ram_key_mappings:
            search_keys.append(ram_key_mappings[base_key])
            logger.debug(f"[Comparison] TITLE-TABLE - Looking for table key '{ram_key_mappings[base_key]}' to match title key '{base_key}'", extra={'session_id': current_session_id})
        
        # Special handling for RAM size - if we're looking for ram_size and it's not found, check ram_config
        if base_key == 'ram_size':
            ram_config_found = False
            # Check shared values for RAM config
            for shared_key, shared_val in shared_values.items():
                clean_key = shared_key
                if clean_key.startswith('table_'):
                    clean_key = clean_key[6:]
                if clean_key.endswith('_key'):
                    clean_key = clean_key[:-4]
                
                if clean_key in ['ram_config', 'ram_modules'] and shared_val and shared_val.strip():
                    extracted_size = extract_ram_size_from_config(shared_val)
                    if extracted_size:
                        values.add(extracted_size)
                        ram_config_found = True
                        logger.debug(f"[Comparison] TITLE-TABLE - Extracted RAM size from shared config: {extracted_size}", extra={'session_id': current_session_id})
                        break
            
            # Check entries for RAM config if not found in shared
            if not ram_config_found:
                for idx, entry in enumerate(table_entries, 1):
                    for entry_key, entry_val in entry.items():
                        clean_key = entry_key
                        if clean_key.startswith('table_'):
                            clean_key = clean_key[6:]
                        if clean_key.endswith('_key'):
                            clean_key = clean_key[:-4]
                        
                        if clean_key in ['ram_config', 'ram_modules'] and entry_val and entry_val.strip():
                            extracted_size = extract_ram_size_from_config(entry_val)
                            if extracted_size:
                                values.add(extracted_size)
                                logger.debug(f"[Comparison] TITLE-TABLE - Extracted RAM size from Entry {idx} config: {extracted_size}", extra={'session_id': current_session_id})
        
        # Incorporate user-defined mapping for this base key (title -> table) and reverse (table -> title)
        mapped_table_base = user_title_to_table.get(base_key)
        reverse_tbl_bases = [tbl for tbl, t in user_table_to_title.items() if t == base_key]
        search_keys = [base_key]
        if mapped_table_base:
            search_keys.append(mapped_table_base)
            logger.debug(f"[Comparison] TITLE-TABLE - Using user mapping for '{base_key}' -> table '{mapped_table_base}'", extra={'session_id': current_session_id})
        for tbl_base in reverse_tbl_bases:
            if tbl_base not in search_keys:
                search_keys.append(tbl_base)
                logger.debug(f"[Comparison] TITLE-TABLE - Using reverse user mapping for title '{base_key}' <- table '{tbl_base}'", extra={'session_id': current_session_id})

        # Check shared values
        for shared_key, shared_val in shared_values.items():
            clean_key = shared_key
            if clean_key.startswith('table_'):
                clean_key = clean_key[6:]
            if clean_key.endswith('_key'):
                clean_key = clean_key[:-4]
            
            # Check exact match or base key match or RAM mapping match
            if clean_key in search_keys or get_base_key(clean_key) in search_keys:
                if shared_val and shared_val.strip():
                    for item in re.split(r'[\/,]', shared_val):
                        if item.strip():
                            values.add(item.strip())
                    logger.debug(f"[Comparison] TITLE-TABLE - Found {base_key} in shared: {shared_key}='{shared_val}'", extra={'session_id': current_session_id})
        
        # Check entry-specific values
        for idx, entry in enumerate(table_entries, 1):
            for entry_key, entry_val in entry.items():
                clean_key = entry_key
                if clean_key.startswith('table_'):
                    clean_key = clean_key[6:]
                if clean_key.endswith('_key'):
                    clean_key = clean_key[:-4]
                
                # Check exact match or base key match or RAM mapping match
                if clean_key in search_keys or get_base_key(clean_key) in search_keys:
                    if entry_val and entry_val.strip():
                        for item in re.split(r'[\/,]', entry_val):
                            if item.strip():
                                values.add(item.strip())
                        logger.debug(f"[Comparison] TITLE-TABLE - Found {base_key} in Entry {idx}: {entry_key}='{entry_val}'", extra={'session_id': current_session_id})
        
        return sorted(list(values))

    # Process each key to compare
    for base_key in keys_to_compare:
        if not base_key or base_key == '':
            continue
            
        # Collect all values for this base key
        title_values = collect_title_values(base_key)
        table_values = collect_table_values(base_key)
        
        if not title_values or not table_values:
            logger.debug(f"[Comparison] TITLE-TABLE - Skipping {base_key}: title_values={title_values}, table_values={table_values}", extra={'session_id': current_session_id})
            continue
            
        logger.debug(f"[Comparison] TITLE-TABLE - Comparing {base_key}: Title={title_values} vs Table={table_values}", extra={'session_id': current_session_id})
        
        # ENHANCED RANGE CHECKING FOR STORAGE_CAPACITY AND RAM_CAPACITY
        if base_key in ['storage_capacity', 'ram_capacity', 'ram_size']:
            # Check if title has a range and table has individual values
            if len(title_values) == 1 and is_range_format(title_values[0]):
                range_value = title_values[0]
                value_type = "RAM" if base_key.startswith('ram') else "Storage"
                
                if check_range_compatibility(range_value, table_values, value_type.lower()):
                    logger.debug(f"[Comparison] TITLE-TABLE {base_key}: RANGE MATCH - All table values {table_values} fall within title range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    display_key = base_key.replace('_', ' ').title()
                    title_display = '/'.join(title_values)
                    table_display = '/'.join(table_values)
                    
                    entry, issue_str = format_comparison(
                        display_key,
                        f"title_{base_key}_key",
                        title_display,
                        "table_entries",
                        table_display,
                        True,  # This is a match
                        multiple_entries=len(table_entries) > 1
                    )
                    title_vs_table.append(entry)
                    continue
                else:
                    logger.debug(f"[Comparison] TITLE-TABLE {base_key}: RANGE MISMATCH - Some table values {table_values} fall outside title range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    display_key = base_key.replace('_', ' ').title()
                    title_display = '/'.join(title_values)
                    table_display = '/'.join(table_values)
                    
                    entry, issue_str = format_comparison(
                        display_key,
                        f"title_{base_key}_key",
                        title_display,
                        "table_entries",
                        table_display,
                        False,  # This is a mismatch
                        multiple_entries=len(table_entries) > 1
                    )
                    title_vs_table.append(entry)
                    title_vs_table_issues.append(entry)
                    consolidated_title_vs_table.append((display_key, title_display, table_display, "All Entries"))
                    
                    consolidated_issue = f"{display_key}: Title has range '{title_display}', but table values '{table_display}' include values outside this range"
                    issue_strings.append(consolidated_issue)
                    logger.debug(f"[Comparison] TITLE-TABLE RANGE MISMATCH: {consolidated_issue}", extra={'session_id': current_session_id})
                    continue
                    
            # Check if table has a range and title has individual values
            elif len(table_values) == 1 and is_range_format(table_values[0]):
                range_value = table_values[0]
                value_type = "RAM" if base_key.startswith('ram') else "Storage"
                
                if check_range_compatibility(range_value, title_values, value_type.lower()):
                    logger.debug(f"[Comparison] TITLE-TABLE {base_key}: RANGE MATCH - All title values {title_values} fall within table range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    display_key = base_key.replace('_', ' ').title()
                    title_display = '/'.join(title_values)
                    table_display = '/'.join(table_values)
                    
                    entry, issue_str = format_comparison(
                        display_key,
                        f"title_{base_key}_key",
                        title_display,
                        "table_entries",
                        table_display,
                        True,  # This is a match
                        multiple_entries=len(table_entries) > 1
                    )
                    title_vs_table.append(entry)
                    continue
                else:
                    logger.debug(f"[Comparison] TITLE-TABLE {base_key}: RANGE MISMATCH - Some title values {title_values} fall outside table range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    display_key = base_key.replace('_', ' ').title()
                    title_display = '/'.join(title_values)
                    table_display = '/'.join(table_values)
                    
                    entry, issue_str = format_comparison(
                        display_key,
                        f"title_{base_key}_key",
                        title_display,
                        "table_entries",
                        table_display,
                        False,  # This is a mismatch
                        multiple_entries=len(table_entries) > 1
                    )
                    title_vs_table.append(entry)
                    title_vs_table_issues.append(entry)
                    consolidated_title_vs_table.append((display_key, title_display, table_display, "All Entries"))
                    
                    consolidated_issue = f"{display_key}: Table has range '{table_display}', but title values '{title_display}' include values outside this range"
                    issue_strings.append(consolidated_issue)
                    logger.debug(f"[Comparison] TITLE-TABLE RANGE MISMATCH: {consolidated_issue}", extra={'session_id': current_session_id})
                    continue
        
        # Check for equivalence using sets to ignore order, but with proper equivalence checking (existing logic)
        title_set = set(title_values)
        table_set = set(table_values)
        
        # Create combined table data for check_equivalence to access videocard_key and other table fields
        combined_table_data = dict(shared_values)  # Start with shared values
        for entry in table_entries:
            combined_table_data.update(entry)  # Add entry-specific values
        
        # Check if all title values have equivalent table values
        is_mismatch = False
        unmatched_title = []
        unmatched_table = []
        
        for t_val in title_set:
            found_match = False
            for tab_val in table_set:
                if check_equivalence(f"{base_key}_key", t_val, tab_val, title={}, specs=specs, table=combined_table_data):
                    found_match = True
                    break
            if not found_match:
                unmatched_title.append(t_val)
                is_mismatch = True
                logger.debug(f"[Comparison] TITLE-TABLE - Title value '{t_val}' has no equivalent in table for {base_key}", extra={'session_id': current_session_id})
        
        # Check if all table values have equivalent title values
        for tab_val in table_set:
            found_match = False
            for t_val in title_set:
                if check_equivalence(f"{base_key}_key", t_val, tab_val, title={}, specs=specs, table=combined_table_data):
                    found_match = True
                    break
            if not found_match:
                unmatched_table.append(tab_val)
                is_mismatch = True
                logger.debug(f"[Comparison] TITLE-TABLE - Table value '{tab_val}' has no equivalent in title for {base_key}", extra={'session_id': current_session_id})
        
        # Create comparison entry
        display_key = base_key.replace('_', ' ').title()
        title_display = '/'.join(title_values)
        table_display = '/'.join(table_values)
        
        entry, issue_str = format_comparison(
            display_key,
            f"title_{base_key}_key",
            title_display,
            "table_entries",
            table_display,
            not is_mismatch,
            multiple_entries=len(table_entries) > 1
        )
        title_vs_table.append(entry)
        
        if is_mismatch:
            title_vs_table_issues.append(entry)
            consolidated_title_vs_table.append((display_key, title_display, table_display, "All Entries"))
            
            # Enhanced issue string with mismatch details
            issue_parts = []
            if unmatched_title:
                issue_parts.append(f"Title extra: {'/'.join(unmatched_title)}")
            if unmatched_table:
                issue_parts.append(f"Table extra: {'/'.join(unmatched_table)}")
            
            if issue_parts:
                consolidated_issue = f"{display_key}: Title has '{title_display}', Table has '{table_display}' ({'; '.join(issue_parts)})"
            else:
                consolidated_issue = f"{display_key}: Title has '{title_display}', Table has '{table_display}'"
                
            issue_strings.append(consolidated_issue)
            logger.debug(f"[Comparison] TITLE-TABLE MISMATCH: {consolidated_issue}", extra={'session_id': current_session_id})
        else:
            logger.debug(f"[Comparison] TITLE-TABLE MATCH: {display_key}", extra={'session_id': current_session_id})

    logger.debug(f"[Comparison] TITLE-TABLE COMPLETE - Found {len(keys_to_compare)} keys to compare, {len(issue_strings)} issues", extra={'session_id': current_session_id})
    return title_vs_table, title_vs_table_issues, issue_strings, consolidated_title_vs_table
    