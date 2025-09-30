"""
SKU Utilities - Shared module for consistent SKU handling across all scripts
"""
import re
from typing import Tuple, Optional, Dict

# Toggle to enable/disable the new standardized SKU handling
ENABLE_STANDARDIZED_SKU_HANDLING = True  # Set to False to use original implementations

def extract_sku_parts(sku: str) -> Tuple[str, Optional[str]]:
    """
    Extract the components of a SKU string.
    
    Args:
        sku: The SKU string to parse (e.g. "XX - 1234 - Location")
        
    Returns:
        Tuple of (initials, numeric_id)
    """
    if not sku or not isinstance(sku, str):
        return "XX", None
        
    parts = re.split(r'[\s-]+', sku.strip())
    initials = "XX"
    numeric_id = None
    
    # Find initials (usually 2-3 letter code at beginning)
    if parts and re.match(r'^[A-Za-z]{2,3}$', parts[0]):
        initials = parts[0].upper()
        
        # Look for 3-6 digit number right after initials
        for i in range(1, len(parts)):
            if re.match(r'^\d{3,6}$', parts[i]):
                numeric_id = parts[i]
                break
    
    # Fallback: look for any 3-6 digit number anywhere in the string
    if not numeric_id:
        for part in parts:
            if re.match(r'^\d{3,6}$', part):
                numeric_id = part
                break
    
    # Final fallback: any digit sequence (for backwards compatibility)
    if not numeric_id:
        for part in reversed(parts):
            if re.match(r'^\d+$', part):
                numeric_id = part
                break
    
    return initials, numeric_id

def is_valid_sku(sku: str) -> bool:
    """
    Check if a SKU is valid according to standardized rules.
    
    A valid SKU must have:
    - 2-3 letter prefix
    - Followed by some separator (space, dash)
    - Containing a numeric sequence (typically 3-6 digits)
    
    Args:
        sku: The SKU string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    initials, numeric_id = extract_sku_parts(sku)
    return numeric_id is not None

def is_empty_sku(sku: str) -> bool:
    """
    Check if a SKU should be considered "empty" (missing required components).
    
    Args:
        sku: The SKU string to check
        
    Returns:
        bool: True if empty, False otherwise
    """
    # Check if it matches patterns like "SF--M9" (no numeric sequence)
    if re.match(r'^[A-Z]{2,3}\s*-+\s*[A-Z0-9]+$', sku) and not re.search(r'\d{3,6}', sku):
        return True
    return False

def format_sku(sku: str) -> str:
    """
    Format a SKU into a standardized format: "XX ####"
    
    Args:
        sku: The SKU string to format
        
    Returns:
        str: Formatted SKU
    """
    initials, numeric_id = extract_sku_parts(sku)
    if numeric_id:
        return f"{initials} {numeric_id}"
    return sku  # Return original if we can't parse it

def extract_sku_number(sku: str) -> Optional[int]:
    """
    Extract just the numeric part of the SKU as an integer.
    
    Args:
        sku: The SKU string
        
    Returns:
        Optional[int]: The numeric part as integer, or None if not found
    """
    _, numeric_id = extract_sku_parts(sku)
    if numeric_id and numeric_id.isdigit():
        return int(numeric_id)
    return None

def extract_sku_prefix(sku: str) -> str:
    """
    Extract just the prefix part of the SKU (letters at beginning).
    
    Args:
        sku: The SKU string
        
    Returns:
        str: The prefix part, or "UNKNOWN" if not found
    """
    initials, _ = extract_sku_parts(sku)
    return initials if initials != "XX" else "UNKNOWN"

# Testing function
def test_sku_utils():
    """Test the SKU utilities with various formats"""
    test_cases = [
        "SF - 12345 - M9",
        "JW - M9 Shelf C 3809",
        "SF--M9",
        "DD - 4145 - G4 (lot of 2)",
        "MC - 2923 - C4",
        "ABC-123-Location",
        "ABC123",
        "XX - Location",
    ]
    
    results = {}
    for case in test_cases:
        initials, numeric_id = extract_sku_parts(case)
        is_valid = is_valid_sku(case)
        is_empty = is_empty_sku(case)
        formatted = format_sku(case)
        numeric = extract_sku_number(case)
        
        results[case] = {
            "initials": initials,
            "numeric_id": numeric_id,
            "is_valid": is_valid,
            "is_empty": is_empty,
            "formatted": formatted,
            "numeric": numeric
        }
    
    return results

if __name__ == "__main__":
    # When run directly, show test results
    print("Running SKU utilities test...\n")
    results = test_sku_utils()
    for sku, data in results.items():
        print(f"SKU: '{sku}'")
        print(f"  Initials: {data['initials']}")
        print(f"  Numeric ID: {data['numeric_id']}")
        print(f"  Is Valid: {data['is_valid']}")
        print(f"  Is Empty: {data['is_empty']}")
        print(f"  Formatted: '{data['formatted']}'")
        print(f"  Numeric: {data['numeric']}")
        print()