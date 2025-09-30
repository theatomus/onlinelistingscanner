import re
import logging
from collections import Counter, defaultdict
from .base import get_globals_from_main, log_debug, is_range_format, check_range_compatibility

def compare_title_vs_specifics(listing_data, sections, is_power_adapter):
    """Compare title data against specifics data."""
    globals_dict = get_globals_from_main()
    
    # Helper function to extract RAM size from config strings
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
    
    # Get required functions and data from main module
    format_comparison = globals_dict['format_comparison']
    check_equivalence = globals_dict['check_equivalence']
    load_key_mappings = globals_dict['load_key_mappings']
    
    # Extract and normalize data
    title = {k.replace('title_', ''): v for k, v in listing_data['title'].items()}
    specs = {k.replace('specs_', ''): v for k, v in listing_data['specifics'].items()}
    table = {k.replace('table_', ''): v for k, v in listing_data['table_data'][0].items()} if listing_data['table_data'] else {}
    title_vs_specs_issues = []
    title_vs_specs = []
    issue_strings = []

    # Check if title_model_key is 'Model: Unknown Title' or 'Unknown Title'
    if title.get('model_key', '') in ['Model: Unknown Title', 'Unknown Title']:
        log_debug("Skipping Title vs Specifics comparison due to title_model_key='Model: Unknown Title' or 'Unknown Title'")
        return title_vs_specs, title_vs_specs_issues, issue_strings

    # Check logger level
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    if logger.getEffectiveLevel() > logging.DEBUG:
        logger.warning("[Comparison] Logger level is above DEBUG; comparison logs may not be recorded", extra={'session_id': globals_dict['current_session_id']})

    # Helper function to check if a value is in range format (e.g., "32GB-256GB" or "4GB-16GB")
    def is_range_format_local(value):
        if not value:
            return False
        return bool(re.match(r'^(\d+(?:\.\d+)?)(gb|mb|tb)-(\d+(?:\.\d+)?)(gb|mb|tb)$', value.lower().strip()))

    # Helper function to check if all values fall within a range
    def check_range_compatibility_local(range_value, individual_values, value_type="storage"):
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
                log_debug(f"Invalid {value_type} format: '{value}'")
                return False
                
            size, unit = value_match.groups()
            size = float(size)
            size_gb = size * unit_multipliers.get(unit, 1)
            
            # Check if this value falls within the range
            if not (min_size_gb <= size_gb <= max_size_gb):
                log_debug(f"{value_type.title()} value '{value}' ({size_gb}GB) is outside range '{range_value}' ({min_size_gb}GB-{max_size_gb}GB)")
                return False
                
        log_debug(f"All {value_type} values {individual_values} fall within range '{range_value}'")
        return True

    # Define CPU and RAM keys to ensure they are compared
    cpu_keys = ['cpu_model_key', 'cpu_generation_key', 'cpu_family_key', 'cpu_speed_key']
    ram_keys = ['ram_size_key', 'ram_type_key', 'ram_features_key', 'ram_capacity_key', 'ram_modules_key']
    relevant_keys = cpu_keys + ram_keys

    # Add mapping for cpu_speed_key to clock_speed_key
    key_mappings = {
        'cpu_speed_key': 'clock_speed_key'
    }

    # Load user-defined key mappings
    user_key_mappings = load_key_mappings().get("mappings", [])

    # Get common keys and ensure CPU/RAM keys are included if present in both sections
    title_specs_common = set(title.keys()) & set(specs.keys())
    title_specs_common.update(key for key in relevant_keys if key in title and key in specs)
    log_debug(f"[Comparison] Title vs Specs common keys: {title_specs_common}")

    # Helper to collect base and numbered title values for a given specs key
    def get_title_values_for_spec_key(section_title: dict, spec_key_name: str):
        if not spec_key_name.endswith('_key'):
            return []
        base_name = spec_key_name[:-4]  # drop '_key'
        pattern = re.compile(rf'^{re.escape(base_name)}(\d+)?_key$')
        values = [section_title[k] for k in section_title if pattern.match(k) and section_title.get(k)]
        return values

    # Special handling for RAM size - if specs has ram_size but title has ram_config, extract from config
    if 'ram_size' in specs and 'ram_size' not in title:
        # Check if title has ram_config that we can extract from
        for title_key, title_value in title.items():
            if 'ram_config' in title_key and title_value and title_value.strip():
                extracted_size = extract_ram_size_from_config(title_value)
                if extracted_size:
                    # Add extracted size to title for comparison
                    title['ram_size'] = extracted_size
                    title_specs_common.add('ram_size')
                    log_debug(f"[Comparison] Extracted RAM size from title config: {extracted_size}")
                    break
    
    # Compare common keys, aggregating numbered title variants when present
    for key in title_specs_common:
        if key in {'storage_capacity_key', 'storage_capacity2_key'}:
            continue
        t_val = title.get(key)
        s_val = specs.get(key)
        if t_val is None or s_val is None:
            log_debug(f"[Comparison] Skipping comparison for {key}: Title value='{t_val}', Specs value='{s_val}' (one or both missing)")
            continue
        if s_val.lower().strip() == 'see notes':
            log_debug(f"[Comparison] Equivalence found for {key}: Specs value '{s_val}' is 'see notes', acting as wildcard")
            full_key1 = f"title_{key}_key"
            full_key2 = f"specs_{key}_key"
            entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, True)
            title_vs_specs.append(entry)
            continue

        # If title contains numbered variants for this key (e.g., cpu_generation2_key), aggregate them
        title_variants = get_title_values_for_spec_key(title, key)
        has_numbered_variant = False
        if key.endswith('_key'):
            base_name = key[:-4]
            numbered_pattern = re.compile(rf'^{re.escape(base_name)}\d+_key$')
            has_numbered_variant = any(numbered_pattern.match(k) for k in title.keys())

        if has_numbered_variant and title_variants:
            # Compare any of the title variants against the single specs value
            is_match = any(
                check_equivalence(key, t_variant, s_val, title=title, specs=specs, table=table)
                for t_variant in title_variants
            )
            title_display = '/'.join(title_variants)
            display_key = key.replace('_key', '')
            full_key1 = f"title_{display_key}_keys"
            full_key2 = f"specs_{key}"
            log_debug(f"[Comparison] Comparing {key} with numbered keys: Title='{title_display}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
            entry, issue_str = format_comparison(display_key, full_key1, title_display, full_key2, s_val, is_match)
            title_vs_specs.append(entry)
            if not is_match:
                log_debug(f"[Comparison] Adding issue: {key} mismatch with numbered keys")
                title_vs_specs_issues.append(entry)
                issue_strings.append(issue_str)
            continue

        # ENHANCED RANGE CHECKING FOR STORAGE_CAPACITY AND RAM_CAPACITY
        if key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key']:
            # Check if title has a range and specs has individual values
            if is_range_format_local(t_val):
                # Parse specs value into individual values
                if '/' in s_val or ',' in s_val:
                    individual_values = [item.strip() for item in re.split(r'[\/,]', s_val) if item.strip()]
                else:
                    individual_values = [s_val.strip()] if s_val.strip() else []
                
                value_type = "RAM" if key.startswith('ram') else "Storage"
                
                if check_range_compatibility_local(t_val, individual_values, value_type.lower()):
                    log_debug(f"[Comparison] {key}: RANGE MATCH - All specs values {individual_values} fall within title range '{t_val}'")
                    full_key1 = f"title_{key}_key"
                    full_key2 = f"specs_{key}_key"
                    entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, True)
                    title_vs_specs.append(entry)
                    continue
                else:
                    log_debug(f"[Comparison] {key}: RANGE MISMATCH - Some specs values {individual_values} fall outside title range '{t_val}'")
                    full_key1 = f"title_{key}_key"
                    full_key2 = f"specs_{key}_key"
                    entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, False)
                    title_vs_specs.append(entry)
                    title_vs_specs_issues.append(entry)
                    issue_str = f"{key.replace('_key', '').replace('_', ' ').title()}: Title has range '{t_val}', but specs values '{s_val}' include values outside this range"
                    issue_strings.append(issue_str)
                    continue
                    
            # Check if specs has a range and title has individual values
            elif is_range_format_local(s_val):
                # Parse title value into individual values
                if '/' in t_val or ',' in t_val:
                    individual_values = [item.strip() for item in re.split(r'[\/,]', t_val) if item.strip()]
                else:
                    individual_values = [t_val.strip()] if t_val.strip() else []
                
                value_type = "RAM" if key.startswith('ram') else "Storage"
                
                if check_range_compatibility_local(s_val, individual_values, value_type.lower()):
                    log_debug(f"[Comparison] {key}: RANGE MATCH - All title values {individual_values} fall within specs range '{s_val}'")
                    full_key1 = f"title_{key}_key"
                    full_key2 = f"specs_{key}_key"
                    entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, True)
                    title_vs_specs.append(entry)
                    continue
                else:
                    log_debug(f"[Comparison] {key}: RANGE MISMATCH - Some title values {individual_values} fall outside specs range '{s_val}'")
                    full_key1 = f"title_{key}_key"
                    full_key2 = f"specs_{key}_key"
                    entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, False)
                    title_vs_specs.append(entry)
                    title_vs_specs_issues.append(entry)
                    issue_str = f"{key.replace('_key', '').replace('_', ' ').title()}: Specs has range '{s_val}', but title values '{t_val}' include values outside this range"
                    issue_strings.append(issue_str)
                    continue

        # Continue with existing equivalence checking for non-range cases
        full_key1 = f"title_{key}_key"
        full_key2 = f"specs_{key}_key"
        is_match = check_equivalence(key, t_val, s_val, title=title, specs=specs, table=table)
        log_debug(f"[Comparison] Comparing {key}: Title='{t_val}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
        if key in ram_keys:
            log_debug(f"[Comparison] RAM comparison for {key}: {t_val} vs {s_val} -> {'match' if is_match else 'mismatch'}")
        elif key in cpu_keys:
            log_debug(f"[Comparison] CPU comparison for {key}: {t_val} vs {s_val} -> {'match' if is_match else 'mismatch'}")
        entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, s_val, is_match)
        title_vs_specs.append(entry)
        if not is_match:
            log_debug(f"[Comparison] Adding issue: {key} mismatch")
            title_vs_specs_issues.append(entry)
            issue_strings.append(issue_str)

    # Compare specifics keys with numbered title keys (for spec keys missing base in title)

    for spec_key in specs:
        if spec_key in title_specs_common:  # Skip if already compared
            continue
        t_vals = get_title_values_for_spec_key(title, spec_key)
        if t_vals:
            s_val = specs[spec_key]
            # Normalize specs value to use slash separation like title_display
            s_val_normalized = s_val.replace(', ', '/')
            is_match = any(check_equivalence(spec_key, t_val, s_val, title=title, specs=specs, table=table) for t_val in t_vals)
            title_display = '/'.join(t_vals)
            display_key = spec_key.replace('_key', '')
            full_key1 = f"title_{spec_key.replace('_key', '')}_keys"
            full_key2 = f"specs_{spec_key}"
            # Use normalized value for comparison logic
            if not is_match and title_display == s_val_normalized:
                is_match = True
            log_debug(f"[Comparison] Comparing {spec_key} with numbered keys: Title='{title_display}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
            entry, issue_str = format_comparison(display_key, full_key1, title_display, full_key2, s_val, is_match)
            title_vs_specs.append(entry)
            if not is_match:
                log_debug(f"[Comparison] Adding issue: {spec_key} mismatch with numbered keys")
                title_vs_specs_issues.append(entry)
                issue_strings.append(issue_str)

    # Handle mapped key comparisons (e.g., cpu_speed_key to clock_speed_key)
    for title_key, specs_key in key_mappings.items():
        if title_key in title and specs_key in specs:
            t_val = title[title_key]
            s_val = specs[specs_key]
            if s_val.lower().strip() == 'see notes':
                log_debug(f"[Comparison] Equivalence found for {title_key} vs {specs_key}: Specs value '{s_val}' is 'see notes', acting as wildcard")
                full_key1 = f"title_{title_key}_key"
                full_key2 = f"specs_{specs_key}_key"
                entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                title_vs_specs.append(entry)
                continue

            # ENHANCED RANGE CHECKING FOR MAPPED KEYS
            if title_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key'] or specs_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key']:
                # Check if title has a range and specs has individual values
                if is_range_format_local(t_val):
                    if '/' in s_val or ',' in s_val:
                        individual_values = [item.strip() for item in re.split(r'[\/,]', s_val) if item.strip()]
                    else:
                        individual_values = [s_val.strip()] if s_val.strip() else []
                    
                    value_type = "RAM" if 'ram' in title_key else "Storage"
                    
                    if check_range_compatibility_local(t_val, individual_values, value_type.lower()):
                        log_debug(f"[Comparison] Mapped {title_key}: RANGE MATCH - All specs values fall within title range")
                        full_key1 = f"title_{title_key}_key"
                        full_key2 = f"specs_{specs_key}_key"
                        entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                        title_vs_specs.append(entry)
                        continue
                    else:
                        log_debug(f"[Comparison] Mapped {title_key}: RANGE MISMATCH - Some specs values fall outside title range")
                        full_key1 = f"title_{title_key}_key"
                        full_key2 = f"specs_{specs_key}_key"
                        entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, False)
                        title_vs_specs.append(entry)
                        title_vs_specs_issues.append(entry)
                        issue_str = f"{title_key.replace('_key', '').replace('_', ' ').title()}: Title has range '{t_val}', but specs values include values outside this range"
                        issue_strings.append(issue_str)
                        continue
                        
                # Check if specs has a range and title has individual values
                elif is_range_format_local(s_val):
                    if '/' in t_val or ',' in t_val:
                        individual_values = [item.strip() for item in re.split(r'[\/,]', t_val) if item.strip()]
                    else:
                        individual_values = [t_val.strip()] if t_val.strip() else []
                    
                    value_type = "RAM" if 'ram' in specs_key else "Storage"
                    
                    if check_range_compatibility_local(s_val, individual_values, value_type.lower()):
                        log_debug(f"[Comparison] Mapped {specs_key}: RANGE MATCH - All title values fall within specs range")
                        full_key1 = f"title_{title_key}_key"
                        full_key2 = f"specs_{specs_key}_key"
                        entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                        title_vs_specs.append(entry)
                        continue
                    else:
                        log_debug(f"[Comparison] Mapped {specs_key}: RANGE MISMATCH - Some title values fall outside specs range")
                        full_key1 = f"title_{title_key}_key"
                        full_key2 = f"specs_{specs_key}_key"
                        entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, False)
                        title_vs_specs.append(entry)
                        title_vs_specs_issues.append(entry)
                        issue_str = f"{title_key.replace('_key', '').replace('_', ' ').title()}: Specs has range '{s_val}', but title values include values outside this range"
                        issue_strings.append(issue_str)
                        continue

            # Continue with existing equivalence checking for mapped keys
            full_key1 = f"title_{title_key}_key"
            full_key2 = f"specs_{specs_key}_key"
            is_match = check_equivalence(title_key, t_val, s_val, title=title, specs=specs, table=table)
            log_debug(f"[Comparison] Comparing mapped keys {title_key} vs {specs_key}: Title='{t_val}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
            entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, is_match)
            title_vs_specs.append(entry)
            if not is_match:
                log_debug(f"[Comparison] Adding issue: {title_key} vs {specs_key} mismatch")
                title_vs_specs_issues.append(entry)
                issue_strings.append(issue_str)

    # Handle user-defined key mappings
    for mapping in user_key_mappings:
        if mapping["section1"] == "title" and mapping["section2"] == "specifics":
            title_key = mapping["key1"]
            specs_key = mapping["key2"]
            if title_key in title and specs_key in specs:
                t_val = title[title_key]
                s_val = specs[specs_key]
                if s_val.lower().strip() == 'see notes':
                    log_debug(f"[Comparison] Equivalence found for mapped {title_key} vs {specs_key}: Specs value '{s_val}' is 'see notes', acting as wildcard")
                    full_key1 = f"title_{title_key}_key (mapped)"
                    full_key2 = f"specs_{specs_key}_key (mapped)"
                    entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                    title_vs_specs.append(entry)
                    continue

                # ENHANCED RANGE CHECKING FOR USER-DEFINED MAPPINGS
                if title_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key'] or specs_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key']:
                    # Check both directions for range compatibility
                    if is_range_format_local(t_val):
                        if '/' in s_val or ',' in s_val:
                            individual_values = [item.strip() for item in re.split(r'[\/,]', s_val) if item.strip()]
                        else:
                            individual_values = [s_val.strip()] if s_val.strip() else []
                        
                        value_type = "RAM" if 'ram' in title_key else "Storage"
                        
                        if check_range_compatibility_local(t_val, individual_values, value_type.lower()):
                            full_key1 = f"title_{title_key}_key (mapped)"
                            full_key2 = f"specs_{specs_key}_key (mapped)"
                            entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                            title_vs_specs.append(entry)
                            continue
                    elif is_range_format_local(s_val):
                        if '/' in t_val or ',' in t_val:
                            individual_values = [item.strip() for item in re.split(r'[\/,]', t_val) if item.strip()]
                        else:
                            individual_values = [t_val.strip()] if t_val.strip() else []
                        
                        value_type = "RAM" if 'ram' in specs_key else "Storage"
                        
                        if check_range_compatibility_local(s_val, individual_values, value_type.lower()):
                            full_key1 = f"title_{title_key}_key (mapped)"
                            full_key2 = f"specs_{specs_key}_key (mapped)"
                            entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                            title_vs_specs.append(entry)
                            continue

                # Continue with existing user-defined mapping logic
                full_key1 = f"title_{title_key}_key (mapped)"
                full_key2 = f"specs_{specs_key}_key (mapped)"
                is_match = check_equivalence(title_key, t_val, s_val, title=title, specs=specs, table=table)
                log_debug(f"[Comparison] Comparing user-mapped keys {title_key} vs {specs_key}: Title='{t_val}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
                entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, is_match)
                title_vs_specs.append(entry)
                if not is_match:
                    log_debug(f"[Comparison] Adding issue: user-mapped {title_key} vs {specs_key} mismatch")
                    title_vs_specs_issues.append(entry)
                    issue_strings.append(issue_str)
        elif mapping["section1"] == "specifics" and mapping["section2"] == "title":
            title_key = mapping["key2"]
            specs_key = mapping["key1"]
            if title_key in title and specs_key in specs:
                t_val = title[title_key]
                s_val = specs[specs_key]
                if s_val.lower().strip() == 'see notes':
                    log_debug(f"[Comparison] Equivalence found for mapped {title_key} vs {specs_key}: Specs value '{s_val}' is 'see notes', acting as wildcard")
                    full_key1 = f"title_{title_key}_key (mapped)"
                    full_key2 = f"specs_{specs_key}_key (mapped)"
                    entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                    title_vs_specs.append(entry)
                    continue

                # ENHANCED RANGE CHECKING FOR REVERSE USER-DEFINED MAPPINGS
                if title_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key'] or specs_key in ['storage_capacity_key', 'ram_capacity_key', 'ram_size_key']:
                    # Check both directions for range compatibility
                    if is_range_format_local(t_val):
                        if '/' in s_val or ',' in s_val:
                            individual_values = [item.strip() for item in re.split(r'[\/,]', s_val) if item.strip()]
                        else:
                            individual_values = [s_val.strip()] if s_val.strip() else []
                        
                        value_type = "RAM" if 'ram' in title_key else "Storage"
                        
                        if check_range_compatibility_local(t_val, individual_values, value_type.lower()):
                            full_key1 = f"title_{title_key}_key (mapped)"
                            full_key2 = f"specs_{specs_key}_key (mapped)"
                            entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                            title_vs_specs.append(entry)
                            continue
                    elif is_range_format_local(s_val):
                        if '/' in t_val or ',' in t_val:
                            individual_values = [item.strip() for item in re.split(r'[\/,]', t_val) if item.strip()]
                        else:
                            individual_values = [t_val.strip()] if t_val.strip() else []
                        
                        value_type = "RAM" if 'ram' in specs_key else "Storage"
                        
                        if check_range_compatibility_local(s_val, individual_values, value_type.lower()):
                            full_key1 = f"title_{title_key}_key (mapped)"
                            full_key2 = f"specs_{specs_key}_key (mapped)"
                            entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                            title_vs_specs.append(entry)
                            continue

                # Continue with existing reverse user-defined mapping logic
                full_key1 = f"title_{title_key}_key (mapped)"
                full_key2 = f"specs_{specs_key}_key (mapped)"
                is_match = check_equivalence(title_key, t_val, s_val, title=title, specs=specs, table=table)
                log_debug(f"[Comparison] Comparing user-mapped keys {title_key} vs {specs_key}: Title='{t_val}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
                entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, is_match)
                title_vs_specs.append(entry)
                if not is_match:
                    log_debug(f"[Comparison] Adding issue: user-mapped {title_key} vs {specs_key} mismatch")
                    title_vs_specs_issues.append(entry)
                    issue_strings.append(issue_str)

    # Continue with existing RAM category mappings logic...
    ram_category_mappings = {
        'ram_size_key': 'total_capacity_key',
        'ram_total': 'total_capacity_key',
        'ram_type_key': 'type_key',
        'ram_modules_key': 'number_of_modules_key'
    }
    is_ram_category = False
    category_section = sections.get('CATEGORY', [])
    for line in category_section:
        if '[leaf_category_key]' in line and 'Memory (RAM)' in line:
            is_ram_category = True
            log_debug(f"[Comparison] Detected RAM category via leaf_category_key: Memory (RAM)")
            break
    
    if is_ram_category:
        log_debug(f"[Comparison] Applying RAM category-specific comparisons")
        for title_key, specs_key in ram_category_mappings.items():
            if title_key in title and specs_key in specs:
                t_val = title[title_key]
                s_val = specs[specs_key]
                if s_val.lower().strip() == 'see notes':
                    log_debug(f"[Comparison] Equivalence found for {title_key} vs {specs_key}: Specs value '{s_val}' is 'see notes', acting as wildcard")
                    full_key1 = f"title_{title_key}_key"
                    full_key2 = f"specs_{specs_key}_key"
                    entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, True)
                    title_vs_specs.append(entry)
                    continue
                full_key1 = f"title_{title_key}_key"
                full_key2 = f"specs_{specs_key}_key"
                is_match = check_equivalence(title_key, t_val, s_val, title=title, specs=specs, table=table)
                log_debug(f"[Comparison] RAM category comparison {title_key} vs {specs_key}: Title='{t_val}' vs Specs='{s_val}' -> {'Match' if is_match else 'Mismatch'}")
                entry, issue_str = format_comparison(title_key, full_key1, t_val, full_key2, s_val, is_match)
                title_vs_specs.append(entry)
                if not is_match:
                    log_debug(f"[Comparison] RAM category issue: {title_key}")
                    title_vs_specs_issues.append(entry)
                    issue_strings.append(issue_str)

    # Continue with existing storage key comparison logic...
    storage_related_keys = {
        'title': ['title_storage_key', 'title_storage_type_key', 'title_storage_capacity_key'],
        'specs': ['specs_storage_key', 'specs_storage_type_key', 'specs_storage_capacity_key']
    }

    if any(key in title for key in storage_related_keys['title']):
        if 'title_storage_key' in title and 'specs_storage_key' in specs:
            title_storage_value = title['title_storage_key']
            specs_storage_value = specs['specs_storage_key']
            if specs_storage_value.lower().strip() == 'see notes':
                log_debug(f"[Comparison] Equivalence found for storage_key: Specs value '{specs_storage_value}' is 'see notes', acting as wildcard")
                entry, issue_str = format_comparison(
                    "Storage",
                    "title_storage_key",
                    title_storage_value,
                    "specs_storage_key",
                    specs_storage_value,
                    True
                )
                title_vs_specs.append(entry)
            else:
                is_match = check_equivalence('storage_key', title_storage_value, specs_storage_value, title=title, specs=specs, table=table)
                log_debug(f"[Comparison] Comparing storage_key: Title='{title_storage_value}' vs Specs='{specs_storage_value}' -> {'Match' if is_match else 'Mismatch'}")
                entry, issue_str = format_comparison(
                    "Storage",
                    "title_storage_key",
                    title_storage_value,
                    "specs_storage_key",
                    specs_storage_value,
                    is_match
                )
                title_vs_specs.append(entry)
                if not is_match:
                    title_vs_specs_issues.append(entry)
                    issue_strings.append(issue_str)
        elif 'title_storage_key' in title and title['title_storage_key'].lower() not in ["no storage", "none", "no", "n/a", "no (m.2)"]:
            title_storage_value = title['title_storage_key']
            log_debug(f"[Comparison] Comparing storage_key: Title='{title_storage_value}' vs Specs='Missing' -> Mismatch")
            entry, issue_str = format_comparison(
                "Storage",
                "title_storage_key",
                title_storage_value,
                "specs_storage_key",
                "Missing",
                False
            )
            title_vs_specs.append(entry)
            title_vs_specs_issues.append(entry)
            issue_strings.append(issue_str)

        if 'title_storage_type_key' in title and 'specs_storage_type_key' in specs:
            title_type_value = title['title_storage_type_key']
            specs_type_value = specs['specs_storage_type_key']
            if specs_type_value.lower().strip() == 'see notes':
                log_debug(f"[Comparison] Equivalence found for storage_type_key: Specs value '{specs_type_value}' is 'see notes', acting as wildcard")
                entry, issue_str = format_comparison(
                    "Storage Type",
                    "title_storage_type_key",
                    title_type_value,
                    "specs_storage_type_key",
                    specs_type_value,
                    True
                )
                title_vs_specs.append(entry)
            else:
                is_match = check_equivalence('storage_type_key', title_type_value, specs_type_value, title=title, specs=specs, table=table)
                log_debug(f"[Comparison] Comparing storage_type_key: Title='{title_type_value}' vs Specs='{specs_type_value}' -> {'Match' if is_match else 'Mismatch'}")
                entry, issue_str = format_comparison(
                    "Storage Type",
                    "title_storage_type_key",
                    title_type_value,
                    "specs_storage_type_key",
                    specs_type_value,
                    is_match
                )
                title_vs_specs.append(entry)
                if not is_match:
                    title_vs_specs_issues.append(entry)
                    issue_strings.append(issue_str)

        # Use normalized keys (no title_/specs_ prefixes) collected above
        if 'storage_capacity_key' in title and 'storage_capacity_key' in specs:
            def normalize_capacity_value(val: str) -> str:
                if not val:
                    return ''
                v = val.strip().lower()
                # Remove common formatting like asterisks and spaces
                v = re.sub(r'[^a-z0-9./-]', '', v)
                # Normalize spaces around '/'
                v = v.replace(' ', '')
                return v

            def collect_capacity_values(section_dict: dict) -> list:
                # Gather base and numbered variants into a list, normalized, from normalized dicts
                values = []
                base_key = 'storage_capacity_key'
                if base_key in section_dict and section_dict[base_key]:
                    values.append(normalize_capacity_value(section_dict[base_key]))
                # Numbered variants
                for k, v in section_dict.items():
                    if re.match(r'^storage_capacity\d+_key$', k) and v:
                        values.append(normalize_capacity_value(v))
                return values

            title_values = collect_capacity_values(title)
            specs_values = collect_capacity_values(specs)

            # If both have multiple capacities, compare as unordered multisets
            if title_values and specs_values and (len(title_values) > 1 or len(specs_values) > 1):
                if sorted(title_values) == sorted(specs_values):
                    title_display = '/'.join([title['storage_capacity_key']] + [v for k, v in sorted(((k, v) for k, v in title.items() if re.match(r'^storage_capacity\d+_key$', k)), key=lambda kv: int(re.search(r'(\d+)', kv[0]).group(1)))])
                    specs_display = '/'.join([specs['storage_capacity_key']] + [v for k, v in sorted(((k, v) for k, v in specs.items() if re.match(r'^storage_capacity\d+_key$', k)), key=lambda kv: int(re.search(r'(\d+)', kv[0]).group(1)))])
                    entry, issue_str = format_comparison(
                        "Storage Capacity",
                        "storage_capacity_key (+ numbered)",
                        title_display,
                        "storage_capacity_key (+ numbered)",
                        specs_display,
                        True
                    )
                    title_vs_specs.append(entry)
                else:
                    # Fall back to original single-value comparison and record issue
                    title_capacity_value = title['storage_capacity_key']
                    specs_capacity_value = specs['storage_capacity_key']
                    is_match = check_equivalence('storage_capacity_key', title_capacity_value, specs_capacity_value, title=title, specs=specs, table=table)
                    log_debug(f"[Comparison] Comparing storage_capacity_key (multi): Title='{title_capacity_value}' vs Specs='{specs_capacity_value}' -> {'Match' if is_match else 'Mismatch'}")
                    entry, issue_str = format_comparison(
                        "Storage Capacity",
                        "storage_capacity_key",
                        title_capacity_value,
                        "storage_capacity_key",
                        specs_capacity_value,
                        is_match
                    )
                    title_vs_specs.append(entry)
                    if not is_match:
                        title_vs_specs_issues.append(entry)
                        issue_strings.append(issue_str)
            else:
                # Original single-value comparison path
                title_capacity_value = title['storage_capacity_key']
                specs_capacity_value = specs['storage_capacity_key']
                if specs_capacity_value.lower().strip() == 'see notes':
                    log_debug(f"[Comparison] Equivalence found for storage_capacity_key: Specs value '{specs_capacity_value}' is 'see notes', acting as wildcard")
                    entry, issue_str = format_comparison(
                        "Storage Capacity",
                        "storage_capacity_key",
                        title_capacity_value,
                        "storage_capacity_key",
                        specs_capacity_value,
                        True
                    )
                    title_vs_specs.append(entry)
                else:
                    is_match = check_equivalence('storage_capacity_key', title_capacity_value, specs_capacity_value, title=title, specs=specs, table=table)
                    log_debug(f"[Comparison] Comparing storage_capacity_key: Title='{title_capacity_value}' vs Specs='{specs_capacity_value}' -> {'Match' if is_match else 'Mismatch'}")
                    entry, issue_str = format_comparison(
                        "Storage Capacity",
                        "storage_capacity_key",
                        title_capacity_value,
                        "storage_capacity_key",
                        specs_capacity_value,
                        is_match
                    )
                    title_vs_specs.append(entry)
                    if not is_match:
                        title_vs_specs_issues.append(entry)
                        issue_strings.append(issue_str)

    return title_vs_specs, title_vs_specs_issues, issue_strings