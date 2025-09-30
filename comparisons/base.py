# Shared utilities and helper functions used by comparison modules
import re
import logging
from collections import Counter, defaultdict
from pathlib import Path

def get_globals_from_main():
    """Get global variables from the main module"""
    import sys
    main_module = sys.modules['__main__']
    return {
        'logger': getattr(main_module, 'logger', logging.getLogger()),
        'current_session_id': getattr(main_module, 'current_session_id', 'unknown'),
        'equivalence_rules': getattr(main_module, 'equivalence_rules', {}),
        'condition_mappings': getattr(main_module, 'condition_mappings', {}),
        'laptop_pc_leaf_categories': getattr(main_module, 'laptop_pc_leaf_categories', set()),
        'format_comparison': getattr(main_module, 'format_comparison'),
        'check_equivalence': getattr(main_module, 'check_equivalence'),
        'load_key_mappings': getattr(main_module, 'load_key_mappings'),
        'enhance_cpu_model_comparison': getattr(main_module, 'enhance_cpu_model_comparison'),
        'compare_ram_modules': getattr(main_module, 'compare_ram_modules'),
        'multi_value_partial_match': getattr(main_module, 'multi_value_partial_match'),
        'compare_cpu_generation': getattr(main_module, 'compare_cpu_generation'),
    }

def log_debug(message, session_id=None):
    """Helper function for debug logging"""
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    if not session_id:
        session_id = globals_dict['current_session_id']
    logger.debug(message, extra={'session_id': session_id})

def is_range_format(value):
    """Check if a value is in range format (e.g., '32GB-256GB' or '4GB-16GB')"""
    if not value:
        return False
    return bool(re.match(r'^(\d+(?:\.\d+)?)(gb|mb|tb)-(\d+(?:\.\d+)?)(gb|mb|tb)$', value.lower().strip()))

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