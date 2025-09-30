import re
import logging
from collections import Counter, defaultdict
from .base import get_globals_from_main, log_debug

def compare_multi_item_lists(key, source_val, table_entries, title=None, specs=None, table=None, is_mobile_device=False):
    """
    Compare a source value (possibly containing multiple items separated by comma or slash)
    with values from all table entries for the given key.
    
    Args:
        key (str): The key to compare (e.g., 'model_key').
        source_val (str): The value from the source (e.g., title or specs, like "Item1/Item2,Item3").
        table_entries (list): List of table entry dictionaries.
        title, specs, table, is_mobile_device: Contextual parameters for equivalence checking.
    
    Returns:
        tuple: (is_match, source_list, table_list)
            - is_match (bool): True if the lists match (same set of values).
            - source_list (list): List of values extracted from source_val.
            - table_list (list): List of values extracted from table entries.
    """
    # Get globals from main module
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    current_session_id = globals_dict['current_session_id']
    check_equivalence = globals_dict['check_equivalence']
    enhance_cpu_model_comparison = globals_dict['enhance_cpu_model_comparison']

    # Extract values from source_val (e.g., "Item1/Item2,Item3" -> ["Item1", "Item2", "Item3"])
    if not source_val:
        source_list = []
    else:
        # Split by either comma or slash, then clean up
        source_items = re.split(r'[\/,]', source_val)
        source_list = [item.strip() for item in source_items if item.strip()]

    # Extract values from table entries for the given key
    table_key = f"table_{key}_key"
    table_list = []
    
    # Check for shared values in listing_data's table_shared
    shared_values = {}
    if isinstance(title, dict) and 'table_shared' in title:
        shared_values = title.get('table_shared', {})
    
    # Add shared value if present
    if table_key in shared_values and shared_values[table_key]:
        table_list.append(shared_values[table_key].strip())
    
    # Then check entry-specific values
    for entry in table_entries:
        if table_key in entry:
            val = entry[table_key]
            if val:
                table_list.append(val.strip())

    # Enhanced model matching for special cases
    if key == 'model':
        # Log what we're processing for debugging
        logger.debug(f"Model comparison: source='{source_val}', source_list={source_list}, table_list={table_list}", 
                    extra={'session_id': current_session_id})
        
        # Special handling for network equipment, server models, etc.
        if (any("catalyst" in item.lower() for item in source_list + table_list) or
            any(re.search(r'[A-Z]{2}-C\d+', item, re.IGNORECASE) for item in source_list + table_list)):
            
            # Extract product identifiers from source list (patterns like WS-CXXXX-XX)
            source_identifiers = []
            for item in source_list:
                # Look for specific patterns like WS-C3750G-24PS-S
                matches = re.findall(r'([A-Z]{2}-C\d+[A-Za-z]*-\d+[A-Za-z]*-[A-Za-z])', item, re.IGNORECASE)
                if matches:
                    source_identifiers.extend(matches)
                else:
                    # For items without clear identifiers, extract model numbers
                    matches = re.findall(r'(\d{4}[A-Za-z]*)', item)
                    if matches:
                        source_identifiers.extend(matches)
                    else:
                        # If no specific pattern, just add the whole item
                        source_identifiers.append(item)
            
            # Extract product identifiers from table list
            table_identifiers = []
            for item in table_list:
                matches = re.findall(r'([A-Z]{2}-C\d+[A-Za-z]*-\d+[A-Za-z]*-[A-Za-z])', item, re.IGNORECASE)
                if matches:
                    table_identifiers.extend(matches)
                else:
                    matches = re.findall(r'(\d{4}[A-Za-z]*)', item)
                    if matches:
                        table_identifiers.extend(matches)
                    else:
                        table_identifiers.append(item)
            
            logger.debug(f"Model identifiers: source={source_identifiers}, table={table_identifiers}", 
                       extra={'session_id': current_session_id})
            
            # Check for identifier matches
            if source_identifiers and table_identifiers:
                identifier_match = False
                # Check if any source identifier is contained within any table identifier or vice versa
                for s_id in source_identifiers:
                    for t_id in table_identifiers:
                        s_lower = s_id.lower()
                        t_lower = t_id.lower()
                        if s_lower in t_lower or t_lower in s_lower:
                            identifier_match = True
                            logger.debug(f"Found match between '{s_id}' and '{t_id}'", 
                                       extra={'session_id': current_session_id})
                            break
                    if identifier_match:
                        break
                
                if identifier_match:
                    source_display = ', '.join(source_list) if source_list else 'N/A'
                    table_display = ', '.join(table_list) if table_list else 'N/A'
                    return True, source_display, table_display

        # Remove "Latitude" prefix from all items for comparison (keep existing logic)
        source_list_normalized = []
        for item in source_list:
            normalized = item.lower().replace('latitude', '').strip()
            source_list_normalized.append(normalized)
        
        table_list_normalized = []
        for item in table_list:
            normalized = item.lower().replace('latitude', '').strip()
            table_list_normalized.append(normalized)
            
        # Check if all normalized items match
        source_set = set(source_list_normalized)
        table_set = set(table_list_normalized)
        
        # Additional check for partial matching of model numbers
        is_match = source_set == table_set
        if not is_match and source_set and table_set:
            # Try to find matching patterns across normalized values
            for s_val in source_set:
                for t_val in table_set:
                    # Check if either value contains the other
                    if s_val in t_val or t_val in s_val:
                        is_match = True
                        logger.debug(f"Found partial model match: '{s_val}' and '{t_val}'", 
                                   extra={'session_id': current_session_id})
                        break
                if is_match:
                    break
    elif key == 'cpu_model':
        # NEW: Enhanced CPU model comparison with format normalization
        logger.debug(f"CPU model comparison: source='{source_val}', source_list={source_list}, table_list={table_list}", 
                    extra={'session_id': current_session_id})
        
        # Normalize source and table lists by converting different separators to consistent format
        def normalize_cpu_list(cpu_list):
            """Normalize a list of CPU values by splitting on separators and cleaning"""
            normalized = []
            for item in cpu_list:
                # Split by both comma and slash, then clean
                sub_items = re.split(r'[\/,]', item)
                for sub_item in sub_items:
                    cleaned = sub_item.strip()
                    if cleaned:
                        normalized.append(cleaned)
            return normalized
        
        # Normalize both lists to handle mixed separators
        normalized_source = normalize_cpu_list(source_list)
        normalized_table = normalize_cpu_list(table_list)
        
        logger.debug(f"Normalized CPU values: source={normalized_source}, table={normalized_table}", 
                    extra={'session_id': current_session_id})
        
        # Compare using sets to ignore order and duplicates
        source_set = set(item.lower().strip() for item in normalized_source)
        table_set = set(item.lower().strip() for item in normalized_table)
        
        # Check for equivalence using the existing CPU model comparison logic
        is_match = True
        unmatched_source = []
        unmatched_table = []
        
        # For each value in source_set, find a match in table_set using CPU model equivalence
        for s_val in source_set:
            matched = False
            for t_val in table_set:
                if enhance_cpu_model_comparison(s_val, t_val, title=title, specs=specs, table=table):
                    matched = True
                    break
            if not matched:
                unmatched_source.append(s_val)
                is_match = False
        
        # For each value in table_set, find a match in source_set using CPU model equivalence
        if is_match:  # Only check if we haven't already found a mismatch
            for t_val in table_set:
                matched = False
                for s_val in source_set:
                    if enhance_cpu_model_comparison(s_val, t_val, title=title, specs=specs, table=table):
                        matched = True
                        break
                if not matched:
                    unmatched_table.append(t_val)
                    is_match = False
        
        logger.debug(f"CPU model equivalence result: is_match={is_match}, unmatched_source={unmatched_source}, unmatched_table={unmatched_table}", 
                    extra={'session_id': current_session_id})
    else:
        # Remove duplicates by converting to sets
        source_set = set(source_list)
        table_set = set(table_list)

        # Compare using equivalence rules for each pair of values
        is_match = True
        unmatched_source = []
        unmatched_table = []

        if source_set and table_set:
            # For each value in source_set, find a match in table_set
            for s_val in source_set:
                matched = False
                for t_val in table_set:
                    if check_equivalence(key, s_val, t_val, title=title, specs=specs, table=table, is_mobile_device=is_mobile_device):
                        matched = True
                        break
                if not matched:
                    unmatched_source.append(s_val)
                    is_match = False

            # For each value in table_set, find a match in source_set
            for t_val in table_set:
                matched = False
                for s_val in source_set:
                    if check_equivalence(key, s_val, t_val, title=title, specs=specs, table=table, is_mobile_device=is_mobile_device):
                        matched = True
                        break
                if not matched:
                    unmatched_table.append(t_val)
                    is_match = False
        else:
            # If either list is empty, they match only if both are empty
            is_match = len(source_set) == len(table_set) == 0

    # Format the lists for display, including unmatched items
    if key == 'model':
        source_display = ', '.join(source_list) if source_list else 'N/A'
        table_display = ', '.join(table_list) if table_list else 'N/A'
        if not is_match and source_list and table_list:  # Ensure both lists have values before showing missing
            missing_in_table = set(source_list_normalized) - set(table_list_normalized)
            missing_in_source = set(table_list_normalized) - set(source_list_normalized)
            if missing_in_table:
                # Use safer approach with list comprehension to find the original values
                missing_values = []
                for m in missing_in_table:
                    for i, s in enumerate(source_list_normalized):
                        if s == m and i < len(source_list):
                            missing_values.append(source_list[i])
                if missing_values:
                    table_display += f" (Missing: {', '.join(missing_values)})"
            if missing_in_source:
                # Same safer approach for source missing values
                missing_values = []
                for m in missing_in_source:
                    for i, t in enumerate(table_list_normalized):
                        if t == m and i < len(table_list):
                            missing_values.append(table_list[i])
                if missing_values:
                    source_display += f" (Missing: {', '.join(missing_values)})"
    else:
        source_display = ', '.join(source_list) if source_list else 'N/A'
        table_display = ', '.join(table_list) if table_list else 'N/A'
        if 'unmatched_source' in locals() and unmatched_source:
            source_display += f" (Unmatched: {', '.join(unmatched_source)})"
        if 'unmatched_table' in locals() and unmatched_table:
            table_display += f" (Unmatched: {', '.join(unmatched_table)})"

    logger.debug(f"Multi-item list comparison for {key}: source={source_list}, table={table_list}, is_match={is_match}, source_display='{source_display}', table_display='{table_display}'", extra={'session_id': current_session_id})
    return is_match, source_display, table_display