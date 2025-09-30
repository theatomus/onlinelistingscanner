import re
import logging
from collections import Counter, defaultdict
from .base import get_globals_from_main, log_debug, is_range_format, check_range_compatibility

def compare_specifics_vs_table(listing_data, sections, is_power_adapter, multiple_entries):
    """Compare specifics data against table data."""
    # Extract and clean specifics and title data
    specs = {k.replace('specs_', '').replace('_key', ''): v for k, v in listing_data['specifics'].items()}
    title = {k.replace('title_', '').replace('_key', ''): v for k, v in listing_data['title'].items()}
    
    # Initialize result lists
    specs_vs_table = []
    specs_vs_table_issues = []
    issue_strings = []
    consolidated_specs_vs_table = []

    # Get globals from main module
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    current_session_id = globals_dict['current_session_id']
    format_comparison = globals_dict['format_comparison']
    check_equivalence = globals_dict['check_equivalence']
    load_key_mappings = globals_dict['load_key_mappings']

    # Check logger level
    if logger.getEffectiveLevel() > 20:  # DEBUG level
        logger.warning("[Comparison] Logger level is above DEBUG; comparison logs may not be recorded", extra={'session_id': current_session_id})

    # Extract table data safely
    shared_values = listing_data.get('table_shared', {})
    table_entries = listing_data.get('table_data', [])

    # If no table entries but we have shared values, create a synthetic entry
    if not table_entries and shared_values:
        logger.debug("[Comparison] No table entries found, using shared values as single entry", extra={'session_id': current_session_id})
        table_entries = [{}]

    if not table_entries and not shared_values:
        logger.debug("[Comparison] No table entries or shared values found for Specifics vs Table comparison", extra={'session_id': current_session_id})
        return specs_vs_table, specs_vs_table_issues, issue_strings, consolidated_specs_vs_table

    # Collect all table keys (shared + entry-specific)
    table_keys = set(shared_values.keys())
    for entry in table_entries:
        table_keys.update(entry.keys())

    # Define key normalization function
    def normalize_key(key):
        prefixes = ['title_', 'specs_', 'table_']
        for prefix in prefixes:
            if key.startswith(prefix):
                key = key[len(prefix):]
        if key.endswith('_key'):
            key = key[:-4]
        return key.lower()

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

    # Enhanced value collection for base keys with numbered variants
    def collect_all_specs_values_for_base_key(base_key):
        """Collect all specs values for a base key, including numbered variants."""
        all_values = set()
        
        # Check for exact match
        if base_key in specs and specs[base_key] and specs[base_key].strip():
            spec_val = specs[base_key]
            # Split by comma or slash and add individual items
            items = re.split(r'[\/,]', spec_val)
            for item in items:
                cleaned_item = item.strip()
                if cleaned_item:
                    all_values.add(cleaned_item)
            logger.debug(f"[Comparison] Found {base_key} in specs (exact): '{spec_val}'", extra={'session_id': current_session_id})
        
        # Check for numbered variants (e.g., cpu_model1, cpu_model2)
        for spec_key, spec_value in specs.items():
            if spec_key.startswith(base_key) and spec_key != base_key:
                suffix = spec_key[len(base_key):]
                if suffix.isdigit():  # This is a numbered variant
                    if spec_value and spec_value.strip():
                        # Split by comma or slash and add individual items
                        items = re.split(r'[\/,]', spec_value)
                        for item in items:
                            cleaned_item = item.strip()
                            if cleaned_item:
                                all_values.add(cleaned_item)
                        logger.debug(f"[Comparison] Found {base_key} in specs (numbered): '{spec_key}' = '{spec_value}'", extra={'session_id': current_session_id})

        return sorted(all_values) if all_values else []

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

    def collect_all_table_values_for_base_key(base_key):
        """Collect all table values for a base key, including numbered variants."""
        all_values = set()
        
        # Special handling for RAM size - if we're looking for ram_size and it's not found, check ram_config
        if base_key == 'ram_size':
            ram_config_found = False
            # Check shared values for RAM config
            for shared_key, shared_value in shared_values.items():
                clean_key = shared_key.replace('table_', '').replace('_key', '')
                if clean_key in ['ram_config', 'ram_modules'] and shared_value and shared_value.strip():
                    extracted_size = extract_ram_size_from_config(shared_value)
                    if extracted_size:
                        all_values.add(extracted_size)
                        ram_config_found = True
                        logger.debug(f"[Comparison] Extracted RAM size from shared config: {extracted_size}", extra={'session_id': current_session_id})
                        break
            
            # Check entries for RAM config if not found in shared
            if not ram_config_found:
                for idx, table_entry in enumerate(table_entries, start=1):
                    for entry_key, entry_value in table_entry.items():
                        clean_key = entry_key.replace('table_', '').replace('_key', '')
                        if clean_key in ['ram_config', 'ram_modules'] and entry_value and entry_value.strip():
                            extracted_size = extract_ram_size_from_config(entry_value)
                            if extracted_size:
                                all_values.add(extracted_size)
                                logger.debug(f"[Comparison] Extracted RAM size from Entry {idx} config: {extracted_size}", extra={'session_id': current_session_id})
        
        # Check shared values
        for shared_key, shared_value in shared_values.items():
            clean_key = shared_key.replace('table_', '').replace('_key', '')
            # Remove numbered suffix to get base key
            clean_base = re.sub(r'\d+$', '', clean_key)
            if clean_base == base_key and shared_value and shared_value.strip():
                # Split by comma or slash and add individual items
                items = re.split(r'[\/,]', shared_value)
                for item in items:
                    cleaned_item = item.strip()
                    if cleaned_item:
                        all_values.add(cleaned_item)
                logger.debug(f"[Comparison] Found {base_key} in shared values: '{shared_value}'", extra={'session_id': current_session_id})
        
        # Check entry-specific values
        for idx, table_entry in enumerate(table_entries, start=1):
            for entry_key, entry_value in table_entry.items():
                clean_key = entry_key.replace('table_', '').replace('_key', '')
                # Remove numbered suffix to get base key
                clean_base = re.sub(r'\d+$', '', clean_key)
                if clean_base == base_key and entry_value and entry_value.strip():
                    # Split by comma or slash and add individual items
                    items = re.split(r'[\/,]', entry_value)
                    for item in items:
                        cleaned_item = item.strip()
                        if cleaned_item:
                            all_values.add(cleaned_item)
                    logger.debug(f"[Comparison] Found {base_key} in Entry {idx}: '{entry_value}' (key: {entry_key})", extra={'session_id': current_session_id})

        return sorted(all_values) if all_values else []

    # Find potential base keys from specs and table
    potential_specs_keys = set()
    potential_table_keys = set()
    
    # Extract base keys from specs
    for spec_key in specs.keys():
        base_key = re.sub(r'\d+$', '', spec_key)  # Remove trailing numbers
        if base_key:
            potential_specs_keys.add(base_key)
    
    # Extract base keys from table
    for table_key in table_keys:
        clean_key = table_key.replace('table_', '').replace('_key', '')
        base_key = re.sub(r'\d+$', '', clean_key)
        if base_key:
            potential_table_keys.add(base_key)
    
    # Find common base keys
    common_base_keys = potential_specs_keys & potential_table_keys
    
    logger.debug(f"[Comparison] Potential specs keys: {potential_specs_keys}", extra={'session_id': current_session_id})
    logger.debug(f"[Comparison] Potential table keys: {potential_table_keys}", extra={'session_id': current_session_id})
    logger.debug(f"[Comparison] Common base keys: {common_base_keys}", extra={'session_id': current_session_id})

    mismatch_groups = defaultdict(list)
    user_key_mappings = load_key_mappings().get("mappings", [])

    # Process each common base key
    for base_key in common_base_keys:
        # Collect ALL specs values for this base key (including numbered variants)
        all_specs_values = collect_all_specs_values_for_base_key(base_key)
        
        if not all_specs_values:
            logger.debug(f"[Comparison] Skipping {base_key}: No specs values found", extra={'session_id': current_session_id})
            continue

        full_key1 = f"specs_{base_key}_key"
        display_key = base_key.replace('_', ' ').title()

        # Collect ALL table values for this base key
        all_table_values = collect_all_table_values_for_base_key(base_key)
        
        if not all_table_values:
            logger.debug(f"[Comparison] No table values found for {base_key}", extra={'session_id': current_session_id})
            continue

        # Log what we collected
        logger.debug(f"[Comparison] Collected specs values for {base_key}: {all_specs_values}", extra={'session_id': current_session_id})
        logger.debug(f"[Comparison] Collected table values for {base_key}: {all_table_values}", extra={'session_id': current_session_id})

        # Create combined specs value string
        specs_display = ', '.join(sorted(all_specs_values))
        
        # Handle 'see notes' wildcard
        if any(val.lower().strip() == 'see notes' for val in all_specs_values):
            logger.debug(f"[Comparison] Equivalence for {base_key}: 'see notes' wildcard", extra={'session_id': current_session_id})
            table_display = ', '.join(sorted(all_table_values))
            entry, _ = format_comparison(
                display_key,
                full_key1,
                specs_display,
                "table_values",
                table_display,
                True,
                multiple_entries=len(table_entries) > 1
            )
            specs_vs_table.append(entry)
            continue

        # ENHANCED RANGE CHECKING FOR STORAGE_CAPACITY AND RAM_CAPACITY
        if base_key in ['storage_capacity', 'ram_capacity', 'ram_size']:
            # Check if specs has a range and table has individual values
            if len(all_specs_values) == 1 and is_range_format(all_specs_values[0]):
                range_value = all_specs_values[0]
                value_type = "RAM" if base_key.startswith('ram') else "Storage"
                
                if check_range_compatibility(range_value, all_table_values, value_type.lower()):
                    logger.debug(f"[Comparison] {base_key}: RANGE MATCH - All table values {all_table_values} fall within specs range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    table_display = ', '.join(sorted(all_table_values))
                    entry, _ = format_comparison(
                        display_key,
                        full_key1,
                        specs_display,
                        "table_entries",
                        table_display,
                        True,  # This is a match
                        multiple_entries=len(table_entries) > 1
                    )
                    specs_vs_table.append(entry)
                    continue
                else:
                    logger.debug(f"[Comparison] {base_key}: RANGE MISMATCH - Some table values {all_table_values} fall outside specs range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    table_display = ', '.join(sorted(all_table_values))
                    entry, issue_str = format_comparison(
                        display_key,
                        full_key1,
                        specs_display,
                        "table_entries",
                        table_display,
                        False,  # This is a mismatch
                        multiple_entries=len(table_entries) > 1
                    )
                    specs_vs_table.append(entry)
                    specs_vs_table_issues.append(entry)
                    issue_str = f"{display_key}: Specs has range '{specs_display}', but table values '{table_display}' include values outside this range"
                    issue_strings.append(issue_str)
                    continue
                    
            # Check if table has a range and specs has individual values
            elif len(all_table_values) == 1 and is_range_format(all_table_values[0]):
                range_value = all_table_values[0]
                value_type = "RAM" if base_key.startswith('ram') else "Storage"
                
                if check_range_compatibility(range_value, all_specs_values, value_type.lower()):
                    logger.debug(f"[Comparison] {base_key}: RANGE MATCH - All specs values {all_specs_values} fall within table range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    table_display = ', '.join(sorted(all_table_values))
                    entry, _ = format_comparison(
                        display_key,
                        full_key1,
                        specs_display,
                        "table_entries",
                        table_display,
                        True,  # This is a match
                        multiple_entries=len(table_entries) > 1
                    )
                    specs_vs_table.append(entry)
                    continue
                else:
                    logger.debug(f"[Comparison] {base_key}: RANGE MISMATCH - Some specs values {all_specs_values} fall outside table range '{range_value}'", 
                               extra={'session_id': current_session_id})
                    table_display = ', '.join(sorted(all_table_values))
                    entry, issue_str = format_comparison(
                        display_key,
                        full_key1,
                        specs_display,
                        "table_entries",
                        table_display,
                        False,  # This is a mismatch
                        multiple_entries=len(table_entries) > 1
                    )
                    specs_vs_table.append(entry)
                    specs_vs_table_issues.append(entry)
                    issue_str = f"{display_key}: Table has range '{table_display}', but specs values '{specs_display}' include values outside this range"
                    issue_strings.append(issue_str)
                    continue

        # Convert to sets for comparison (existing logic for non-range cases)
        specs_values_set = set(val.strip() for val in all_specs_values if val.strip())
        table_values_set = set(val.strip() for val in all_table_values if val.strip())

        # Determine matching strategy based on value counts (existing logic)
        specs_count = len(specs_values_set)
        table_count = len(table_values_set)
        
        logger.debug(f"[Comparison] {base_key}: specs_count={specs_count}, table_count={table_count}", extra={'session_id': current_session_id})

        # Create combined table data for check_equivalence to access videocard_key and other table fields
        combined_table_data = dict(shared_values)  # Start with shared values
        for entry in table_entries:
            combined_table_data.update(entry)  # Add entry-specific values

        # Check equivalence using different strategies
        if specs_count <= table_count:
            # SUBSET MATCHING: Specs has same or fewer values than table
            # Each specs value must have a match in table, but table can have extras
            specs_matched = set()
            
            for spec_val in specs_values_set:
                for table_val in table_values_set:
                    if check_equivalence(base_key + '_key', spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                        specs_matched.add(spec_val)
                        break
            
            # Match if ALL specs values are matched (table can have extras)
            is_match = len(specs_matched) == len(specs_values_set)
            
            if is_match:
                logger.debug(f"[Comparison] {base_key}: SUBSET MATCH - All {specs_count} specs values matched in {table_count} table values", extra={'session_id': current_session_id})
            else:
                unmatched_specs = specs_values_set - specs_matched
                logger.debug(f"[Comparison] {base_key}: SUBSET MISMATCH - Unmatched specs: {unmatched_specs}", extra={'session_id': current_session_id})
                
        else:
            # FULL MATCHING: Specs has more values than table
            # Do bidirectional matching as before
            specs_matched = set()
            table_matched = set()
            
            # For each specs value, find a matching table value
            for spec_val in specs_values_set:
                for table_val in table_values_set:
                    if check_equivalence(base_key + '_key', spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                        specs_matched.add(spec_val)
                        table_matched.add(table_val)
                        break

            # For each table value, find a matching specs value
            for table_val in table_values_set:
                for spec_val in specs_values_set:
                    if check_equivalence(base_key + '_key', spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                        specs_matched.add(spec_val)
                        table_matched.add(table_val)
                        break

            # Match if all values from both sides are matched
            is_match = len(specs_matched) == len(specs_values_set) and len(table_matched) == len(table_values_set)
            
            if is_match:
                logger.debug(f"[Comparison] {base_key}: FULL MATCH - All values matched", extra={'session_id': current_session_id})
            else:
                logger.debug(f"[Comparison] {base_key}: FULL MISMATCH - specs_matched: {specs_matched}, table_matched: {table_matched}", extra={'session_id': current_session_id})
        
        # Format display values
        table_display = ', '.join(sorted(table_values_set))
        
        logger.debug(f"[Comparison] {base_key}: Specs={specs_values_set} vs Table={table_values_set} -> {'Match' if is_match else 'Mismatch'}", extra={'session_id': current_session_id})

        # Create comparison entry
        entry, issue_str = format_comparison(
            display_key,
            full_key1,
            specs_display,
            "table_entries",
            table_display,
            is_match,
            multiple_entries=False
        )
        specs_vs_table.append(entry)
        
        if not is_match:
            if specs_count <= table_count:
                # For subset matching, only show unmatched specs values
                unmatched_specs = specs_values_set - specs_matched if 'specs_matched' in locals() else specs_values_set
                mismatch_details = []
                if unmatched_specs:
                    mismatch_details.append(f"specs unmatched: {', '.join(unmatched_specs)}")
            else:
                # For full matching, show both unmatched specs and table
                unmatched_specs = specs_values_set - specs_matched if 'specs_matched' in locals() else set()
                unmatched_table = table_values_set - table_matched if 'table_matched' in locals() else set()
                mismatch_details = []
                if unmatched_specs:
                    mismatch_details.append(f"specs unmatched: {', '.join(unmatched_specs)}")
                if unmatched_table:
                    mismatch_details.append(f"table unmatched: {', '.join(unmatched_table)}")
            
            mismatch_key = (display_key, specs_display, table_display)
            mismatch_groups[mismatch_key].append("collective_mismatch")
            specs_vs_table_issues.append(entry)
            issue_str = f"{display_key}: Specs has '{specs_display}', Table has '{table_display}'"
            if mismatch_details:
                issue_str += f" ({'; '.join(mismatch_details)})"
            issue_strings.append(issue_str)

    # Handle user-defined key mappings with normalization and same logic
    def _normalize_specs_map_key(k: str) -> str:
        k2 = k
        if k2.startswith('specs_'):
            k2 = k2[len('specs_'):]
        if k2.endswith('_key'):
            k2 = k2[:-4]
        return k2

    def _candidate_table_keys(k: str):
        # Generate possible raw table keys to check in shared_values/entries
        keys = set()
        raw = k
        # Add original as-is
        keys.add(raw)
        # Add with table_ prefix if missing
        if not raw.startswith('table_'):
            keys.add('table_' + raw)
        # Add _key suffix variants
        if not raw.endswith('_key'):
            keys.add(raw + '_key')
            if not raw.startswith('table_'):
                keys.add('table_' + raw + '_key')
        else:
            base = raw[:-4]
            keys.add(base)
            if not base.startswith('table_'):
                keys.add('table_' + base)
        return list(keys)

    for mapping in user_key_mappings:
        if mapping.get("section1") == "specifics" and mapping.get("section2") == "table":
            specs_key_raw = mapping.get("key1", "")
            table_key_raw = mapping.get("key2", "")
            specs_key = _normalize_specs_map_key(specs_key_raw)
            table_key_candidates = _candidate_table_keys(table_key_raw)

            if specs_key in specs:
                s_val = specs[specs_key]
                
                # Collect all table values for this mapped key
                all_table_values = set()
                
                # Check shared values
                for tk in table_key_candidates:
                    if tk in shared_values and shared_values[tk]:
                        all_table_values.add(shared_values[tk].strip())
                
                # Check entry-specific values
                for idx, table_entry in enumerate(table_entries, start=1):
                    for tk in table_key_candidates:
                        if tk in table_entry and table_entry[tk]:
                            all_table_values.add(table_entry[tk].strip())

                if not all_table_values:
                    continue

                # ENHANCED RANGE CHECKING FOR MAPPED KEYS
                if specs_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key']:
                    # Check if specs has a range and table has individual values
                    if is_range_format(s_val):
                        value_type = "RAM" if 'ram' in specs_key else "Storage"
                        
                        if check_range_compatibility(s_val, list(all_table_values), value_type.lower()):
                            logger.debug(f"[Comparison] Mapped {specs_key}: RANGE MATCH - All table values fall within specs range", extra={'session_id': current_session_id})
                            table_display = ', '.join(sorted(all_table_values))
                            entry, _ = format_comparison(specs_key.replace('_', ' ').title(), f"specs_{specs_key}_key (mapped)", s_val, f"table_{table_key}_key (mapped)", table_display, True, multiple_entries=len(table_entries) > 1)
                            specs_vs_table.append(entry)
                            continue
                        else:
                            logger.debug(f"[Comparison] Mapped {specs_key}: RANGE MISMATCH - Some table values fall outside specs range", extra={'session_id': current_session_id})
                            table_display = ', '.join(sorted(all_table_values))
                            entry, issue_str = format_comparison(specs_key.replace('_', ' ').title(), f"specs_{specs_key}_key (mapped)", s_val, f"table_{table_key}_key (mapped)", table_display, False, multiple_entries=len(table_entries) > 1)
                            specs_vs_table.append(entry)
                            specs_vs_table_issues.append(entry)
                            issue_str = f"{specs_key.replace('_', ' ').title()}: Specs has range '{s_val}', but table values include values outside this range"
                            issue_strings.append(issue_str)
                            continue

                # Apply same subset/full matching logic for mapped keys (existing logic)
                if isinstance(s_val, str) and s_val.lower().strip() == 'see notes':
                    table_display = ', '.join(sorted(all_table_values))
                    entry, _ = format_comparison(specs_key.replace('_', ' ').title(), f"specs_{specs_key}_key (mapped)", s_val, f"{table_key_raw} (mapped)", table_display, True, multiple_entries=len(table_entries) > 1)
                    specs_vs_table.append(entry)
                    continue

                # Parse specs values
                if '/' in s_val or ',' in s_val:
                    specs_parts = []
                    for part in re.split(r'[\/,]', s_val):
                        if part.strip():
                            specs_parts.append(part.strip())
                    specs_values = set(specs_parts)
                else:
                    specs_values = {s_val.strip()}

                table_values_set = set(val.strip() for val in all_table_values if val.strip())

                # Apply subset/full matching logic
                specs_count = len(specs_values)
                table_count = len(table_values_set)
                
                # Ensure we choose the right comparator by passing the mapping's specs key with '_key'
                eq_key = specs_key_raw if specs_key_raw.endswith('_key') else (specs_key + '_key')

                if specs_count <= table_count:
                    # Subset matching
                    specs_matched = set()
                    for spec_val in specs_values:
                        for table_val in table_values_set:
                            if check_equivalence(eq_key, spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                                specs_matched.add(spec_val)
                                break
                    is_match = len(specs_matched) == len(specs_values)
                else:
                    # Full matching
                    specs_matched = set()
                    table_matched = set()
                    
                    for spec_val in specs_values:
                        for table_val in table_values_set:
                            if check_equivalence(eq_key, spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                                specs_matched.add(spec_val)
                                table_matched.add(table_val)
                                break

                    for table_val in table_values_set:
                        for spec_val in specs_values:
                            if check_equivalence(eq_key, spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                                specs_matched.add(spec_val)
                                table_matched.add(table_val)
                                break

                    is_match = len(specs_matched) == len(specs_values) and len(table_matched) == len(table_values_set)

                specs_display = ', '.join(sorted(specs_values))
                table_display = ', '.join(sorted(table_values_set))

                entry, issue_str = format_comparison(specs_key.replace('_', ' ').title(), f"specs_{specs_key}_key (mapped)", specs_display, f"{table_key_raw} (mapped)", table_display, is_match, multiple_entries=len(table_entries) > 1)
                specs_vs_table.append(entry)
                
                if not is_match:
                    mismatch_key = (specs_key.replace('_', ' ').title(), specs_display, table_display)
                    mismatch_groups[mismatch_key].append("collective_mismatch")
                    specs_vs_table_issues.append(entry)
                    issue_strings.append(issue_str)

    # Keep existing storage key comparison logic unchanged
    storage_keys = {
        'specs': ['storage', 'storage_type', 'storage_capacity'],
        'table': ['ssd', 'hdd', 'hard_drive', 'storage_type', 'storage_capacity']
    }
    if any(key in specs for key in storage_keys['specs']):
        if 'storage' in specs:
            s_val = specs['storage']
            
            # Collect all storage values from table
            all_storage_values = set()
            for idx, table_entry in enumerate(table_entries, start=1):
                combined_table = {**shared_values, **table_entry}
                for storage_key in storage_keys['table']:
                    if storage_key in combined_table and combined_table[storage_key]:
                        all_storage_values.add(combined_table[storage_key].strip())

            if all_storage_values:
                # Apply subset matching for storage
                if '/' in s_val or ',' in s_val:
                    specs_storage_values = set(item.strip() for item in re.split(r'[\/,]', s_val) if item.strip())
                else:
                    specs_storage_values = {s_val.strip()}

                matched_specs_storage = set()
                for spec_val in specs_storage_values:
                    for table_val in all_storage_values:
                        if check_equivalence('storage', spec_val, table_val, title=title, specs=specs, table=combined_table_data):
                            matched_specs_storage.add(spec_val)
                            break

                is_match = len(matched_specs_storage) == len(specs_storage_values)
                
                if not is_match:
                    specs_display = ', '.join(sorted(specs_storage_values))
                    table_display = ', '.join(sorted(all_storage_values))
                    entry, issue_str = format_comparison("Storage", "specs_storage_key", specs_display, "table_storage_keys", table_display, False, multiple_entries=len(table_entries) > 1)
                    specs_vs_table.append(entry)
                    specs_vs_table_issues.append(entry)
                    issue_strings.append(issue_str)

    # FIXED: Coverage check logic with case-insensitive comparison
    coverage_keys = ['cpu_model', 'cpu_family', 'cpu_suffix', 'ram_size', 'screen_size', 'cpu_speed', 'cpu_generation']
    for key in coverage_keys:
        if key in specs and len(table_entries) > 1:
            specs_val = specs[key]
            if '/' in specs_val or ',' in specs_val:
                # Convert specs options to lowercase for case-insensitive comparison
                options = set(opt.strip().lower() for opt in re.split(r'[\/,]', specs_val))
                table_vals = set()
                
                # Special handling for RAM size - use existing config extraction logic
                if key == 'ram_size':
                    # Check shared values for RAM config
                    for shared_key, shared_value in shared_values.items():
                        clean_key = shared_key.replace('table_', '').replace('_key', '')
                        if clean_key in ['ram_config', 'ram_modules'] and shared_value and shared_value.strip():
                            extracted_size = extract_ram_size_from_config(shared_value)
                            if extracted_size:
                                table_vals.add(extracted_size.strip().lower())
                                logger.debug(f"[Coverage] Extracted RAM size from shared config: {extracted_size}", extra={'session_id': current_session_id})
                                break
                    
                    # Check entries for RAM config if not found in shared
                    if not table_vals:
                        for idx, table_entry in enumerate(table_entries, start=1):
                            for entry_key, entry_value in table_entry.items():
                                clean_key = entry_key.replace('table_', '').replace('_key', '')
                                if clean_key in ['ram_config', 'ram_modules'] and entry_value and entry_value.strip():
                                    extracted_size = extract_ram_size_from_config(entry_value)
                                    if extracted_size:
                                        table_vals.add(extracted_size.strip().lower())
                                        logger.debug(f"[Coverage] Extracted RAM size from Entry {idx} config: {extracted_size}", extra={'session_id': current_session_id})
                
                # Standard logic for all other keys
                for entry in table_entries:
                    combined_table = {**shared_values, **entry}
                    # Check for base key and numbered variants
                    for table_key, table_val in combined_table.items():
                        clean_key = table_key.replace('table_', '').replace('_key', '')
                        clean_base = re.sub(r'\d+$', '', clean_key)
                        if clean_base == key and table_val:
                            # Convert table values to lowercase for case-insensitive comparison
                            table_vals.add(table_val.strip().lower())
                
                # FIXED: Normalize capacity tokens so '8 GB' == '8gb' (and similar) before comparison
                def _normalize_capacity_token(value: str) -> str:
                    if not isinstance(value, str):
                        return str(value)
                    m = re.match(r'^\s*(\d+(?:\.\d+)?)\s*(gb|mb|tb)\s*$', value, re.IGNORECASE)
                    if m:
                        num, unit = m.group(1), m.group(2).lower()
                        return f"{num}{unit}"
                    return value.strip().lower()

                normalized_options = {_normalize_capacity_token(v) for v in options}
                normalized_table_vals = {_normalize_capacity_token(v) for v in table_vals}

                missing = normalized_options - normalized_table_vals
                if missing:
                    # Convert back to original case for display purposes
                    original_missing = []
                    # Map normalized missing tokens back to the original specs strings for display
                    spec_tokens = [opt.strip() for opt in re.split(r'[\/,]', specs_val) if opt.strip()]
                    for missing_val in missing:
                        for opt in spec_tokens:
                            if _normalize_capacity_token(opt) == missing_val:
                                original_missing.append(opt)
                                break
                    
                    if original_missing:  # Only report if we have actual missing values
                        issue_str = f"Missing values in table for {key}: {', '.join(original_missing)}"
                        entry = (key.replace('_', ' ').title(), specs_val, "≠", f"Table missing: {', '.join(original_missing)}")
                        specs_vs_table_issues.append(entry)
                        issue_strings.append(issue_str)

    # Consolidate mismatches
    for (display_key, val1, val2), entries in mismatch_groups.items():
        if "collective_mismatch" in entries:
            issue_str = f"{display_key}: Specs has '{val1}', Table has '{val2}'"
            entry_display = "Collective Mismatch"
        else:
            if len(entries) == len(table_entries):
                issue_str = f"{display_key}: '{val1}' in Specs, '{val2}' in Table"
                entry_display = "All Entries"
            else:
                if len(entries) == 1:
                    entry_str = f"Table Entry {entries[0]}"
                    entry_display = f"Entry {entries[0]}"
                else:
                    entry_str = 'Table Entries ' + ', '.join(str(e) for e in entries)
                    entry_display = f"Entries {', '.join(str(e) for e in entries)}"
                issue_str = f"{display_key}: '{val1}' in Specs, '{val2}' in {entry_str}"
            
        consolidated_specs_vs_table.append((display_key, val1, val2, entry_display))
        issue_strings.append(issue_str)

    return specs_vs_table, specs_vs_table_issues, issue_strings, consolidated_specs_vs_table