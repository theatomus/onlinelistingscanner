import json
import os
import re
import logging
from pathlib import Path

# Assume logger is configured in the main script
logger = logging.getLogger(__name__)
current_session_id = None

BASE_DIR = os.path.dirname(__file__)
PACKAGE_DIR = os.path.join(BASE_DIR, 'packages')
CONFIG_DIR = os.path.join(BASE_DIR, 'configs')

def set_validation_logger_session(session_id):
    """Sets the session ID for logging."""
    global current_session_id
    current_session_id = session_id

# This function is already in package_validation_helpers.py, but included here for completeness
# if you want a single file. If package_validation_helpers.py is used, this can be removed
# and imported instead.
def find_model_override_rule(title, model_rules):
    """Find the first model rule that matches the title."""
    if not title or not model_rules:
        return None
    
    title_lower = title.lower()
    
    for rule in model_rules:
        match_text = rule.get('match_text', '').lower()
        if match_text and match_text in title_lower:
            return rule
            
    return None

# This function is already in package_validation_helpers.py, but included here for completeness.
def load_typical_box_sizes():
    """Load typical box sizes from JSON file."""
    sizes_file = os.path.join(CONFIG_DIR, "typical_box_sizes.json")
    if os.path.exists(sizes_file):
        try:
            with open(sizes_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading typical box sizes: {e}", extra={'session_id': current_session_id})
    return []

# This function is already in package_validation_helpers.py, but included here for completeness.
def dimensions_match_typical(pkg_dims_sorted, typical_box_sizes):
    """Check if package dimensions match one of the typical box sizes."""
    if not typical_box_sizes:
        return False
        
    for typical_box in typical_box_sizes:
        typical_dims_sorted = sorted([
            typical_box.get('length', 0),
            typical_box.get('width', 0),
            typical_box.get('height', 0)
        ])
        
        # Check if package dimensions are reasonably close to a typical size
        if all(abs(pkg_d - typ_d) < 0.5 for pkg_d, typ_d in zip(pkg_dims_sorted, typical_dims_sorted)):
            return True
            
    return False

def load_package_validation_rules():
    """Load package validation rules from JSON file."""
    rules_file = os.path.join(CONFIG_DIR, "package_validation_rules.json")
    
    try:
        if os.path.exists(rules_file):
            with open(rules_file, 'r', encoding='utf-8') as f:
                rules = json.load(f)
                logger.debug(f"Loaded package validation rules from {rules_file}", extra={'session_id': current_session_id})
                return rules
        else:
            logger.warning(f"Package validation rules file not found: {rules_file}", extra={'session_id': current_session_id})
            return {"device_types": {}}
    except Exception as e:
        logger.error(f"Error loading package validation rules: {str(e)}", extra={'session_id': current_session_id})
        return {"device_types": {}}

def parse_package_weight(weight_str):
    """Parse package weight string to get numeric value in pounds."""
    if not weight_str:
        return None
    
    match = re.search(r'(\d+(?:\.\d+)?)\s*(lbs?|pounds?|kg|grams?|g|oz|ounces?)', weight_str.lower())
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2)
    
    if unit in ['kg']:
        return value * 2.20462
    elif unit in ['g', 'grams', 'gram']:
        return value * 0.00220462
    elif unit in ['oz', 'ounces', 'ounce']:
        return value * 0.0625
    
    return value

def parse_package_dimensions(dimensions_str):
    """Parse package dimensions string to get length, width, height in inches."""
    if not dimensions_str:
        return None, None, None
    
    pattern = r'(\d+(?:\.\d+)?)\s*[x×by]\s*(\d+(?:\.\d+)?)\s*[x×by]\s*(\d+(?:\.\d+)?)'
    match = re.search(pattern, dimensions_str.lower())
    
    if not match:
        return None, None, None
    
    length = float(match.group(1))
    width = float(match.group(2))
    height = float(match.group(3))
    
    if 'cm' in dimensions_str.lower():
        length *= 0.393701
        width *= 0.393701
        height *= 0.393701
    
    return length, width, height

def get_screen_size_range(screen_size_str):
    """Determine which screen size range a screen size falls into."""
    if not screen_size_str:
        return None
    
    match = re.search(r'(\d+(?:\.\d+)?)', screen_size_str)
    if not match:
        return None
    
    size = float(match.group(1))
    
    if 7 <= size <= 10: return "7-10"
    if 10 < size <= 12: return "10-12"
    if 12 < size <= 15: return "13-15"
    if 15 < size <= 17: return "16-17"
    if 17 < size <= 20: return "18-20"
    
    return None

def get_lot_count(title, meta, listing_data):
    """Extract lot count from various sources."""
    table_shared = listing_data.get('table_shared', {})
    if 'table_entry_count_key' in table_shared:
        entry_count_str = table_shared['table_entry_count_key']
        count_match = re.search(r'Total Entries:\s*(\d+)', entry_count_str)
        if count_match:
            return int(count_match.group(1))
        try:
            return int(entry_count_str)
        except (ValueError, TypeError):
            pass
    
    if len(listing_data.get('table_data', [])) > 1:
        return len(listing_data.get('table_data', []))
    
    lot_str = title.get('lot_key', '')
    if lot_str:
        lot_match = re.search(r'\d+', lot_str)
        if lot_match:
            return int(lot_match.group())
    
    listing_info = meta.get('listinginfo_key', '')
    if listing_info and isinstance(listing_info, str) and "single item" not in listing_info.lower():
        info_match = re.search(r'(\d+)\s*items?', listing_info.lower())
        if info_match:
            return int(info_match.group(1))
            
    return 1

def check_package_validation(title, meta, listing_data, sections, misc_info, misc_issues):
    """
    Validate package weight, dimensions **and price**.
    • Category rules - OR - model–override rules (if text match found in title)
    • Handles   – single, small-lot, large-lot
               – auction vs buy-it-now price bands
    """

    # ------------------------------------------------------------------ #
    # 1.  Parse package & listing basics
    # ------------------------------------------------------------------ #
    pkg_weight_str   = meta.get('listing_package_weight_key', '')
    pkg_dim_str      = meta.get('listing_package_dimensions_key', '')
    if not pkg_weight_str and not pkg_dim_str:
        misc_info.append("  - Package Validation: SKIPPED - No package information found")
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
    # 2.  Load rules & detect a model-specific override (e.g. “Dell 3630”)
    # ------------------------------------------------------------------ #
    rules          = load_package_validation_rules()
    override_rule  = find_model_override_rule(title, rules)
    device_label   = (override_rule.get('display_name',
                                        override_rule.get('match_text', leaf_category))
                      if override_rule else leaf_category)

    # ------------------------------------------------------------------ #
    # 3.  Weight-UNIT sanity (oz / g) - uses original category name
    # ------------------------------------------------------------------ #
    weight_unit_issues = []
    if pkg_weight_str:
        units_conf = rules.get('weight_units', {})
        allowed    = units_conf.get('allowed_for_device_types', {}).get(leaf_category, [])

        if re.search(r'\b\d+(?:\.\d+)?\s*oz\b', pkg_weight_str.lower()):
            if not any(u in ['oz', 'ounces', 'ounce'] for u in allowed):
                weight_unit_issues.append(
                    f"Package weight in ounces ({pkg_weight_str}) is suspiciously light for "
                    f"{device_label} - should be in pounds")
        elif re.search(r'\b\d+(?:\.\d+)?\s*g\b', pkg_weight_str.lower()):
            if not any(u in ['g', 'grams', 'gram'] for u in allowed):
                weight_unit_issues.append(
                    f"Package weight in grams ({pkg_weight_str}) is suspiciously light for "
                    f"{device_label} - should be in pounds")

    # ------------------------------------------------------------------ #
    # 4.  PRICE VALIDATION  (auction & BIN, incl. model-level overrides)
    # ------------------------------------------------------------------ #
    price_issues = []
    base_price_cfg = rules.get('price_validation', {})
    price_cfg = dict(base_price_cfg)  # shallow copy
    if override_rule and override_rule.get('price_validation'):
        # model settings shadow category/global
        price_cfg.update(override_rule['price_validation'])

    if price_cfg.get('enabled', False):
        price_str    = meta.get('listing_price_key', '')
        listing_type = meta.get('listing_type_key', '').lower()
        if price_str and listing_type:
            price_val = float(re.sub(r'[^\d.]', '', price_str))
            flags     = price_cfg.get('global_flags', {})
            if price_val < flags.get('suspicious_low_price', 0.99):
                price_issues.append(f"Price ${price_val:.2f} is suspiciously low - may be an error")
            elif price_val > flags.get('suspicious_high_price', 10000.00):
                price_issues.append(f"Price ${price_val:.2f} is suspiciously high - may be an error")

            if listing_type == 'auction':
                auc = price_cfg.get('auction_prices', {})
                if not (auc.get('min_starting_price', 0) <= price_val <= auc.get('max_starting_price', 9e9)):
                    price_issues.append(
                        f"Auction start ${price_val:.2f} outside ${auc.get('min_starting_price',0):.2f}-"
                        f"${auc.get('max_starting_price',0):.2f} for {device_label}")
            elif listing_type == 'buyitnow':
                bin_cfg = price_cfg.get('buyitnow_prices', {})
                # Model-override may supply the lot bands directly; otherwise look up category.
                lot_table = (bin_cfg if 'single' in bin_cfg else
                             bin_cfg.get(device_label) or bin_cfg.get(leaf_category) or {})
                lot_band  = ('single' if lot_count == 1 else
                              'small_lot' if lot_count <= 10 else
                              'large_lot')
                band_cfg  = lot_table.get(lot_band, {})
                if band_cfg:
                    if not (band_cfg.get('min', 0) <= price_val <= band_cfg.get('max', 9e9)):
                        price_issues.append(
                            f"Buy-It-Now ${price_val:.2f} outside "
                            f"${band_cfg.get('min',0):.2f}-${band_cfg.get('max',0):.2f} "
                            f"({lot_band.replace('_',' ')} – {device_label})")

    # ------------------------------------------------------------------ #
    # 5.  Parse weight & dimensions
    # ------------------------------------------------------------------ #
    weight_lbs             = parse_package_weight(pkg_weight_str)
    length, width, height  = parse_package_dimensions(pkg_dim_str)
    if weight_lbs is None and all(v is None for v in (length, width, height)):
        misc_info.append("  - Package Validation: SKIPPED - Could not parse package information")
        return

    # ------------------------------------------------------------------ #
    # 6.  Choose rule-set (category OR model override)
    # ------------------------------------------------------------------ #
    cat_rules     = rules.get('device_types', {}).get(leaf_category) or {}
    applicable    = None
    rule_type     = None
    if override_rule:
        applicable = override_rule
        rule_type  = 'model_override'
    else:
        # --- lot / single selection (legacy + new structure) -------------
        lot_key = ('single' if lot_count == 1 else
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
        if not (w['min'] <= weight_lbs <= w['max']):
            issues.append(
                f"Package weight {weight_lbs:.1f} lbs is outside expected range for {device_label}: "
                f"{w['min']}-{w['max']} lbs")

    # ---- PER-ITEM WEIGHT for large lots --------------------------------
    if weight_lbs is not None and lot_count > 1 and 'weight_per_item' in applicable:
        per = applicable['weight_per_item']
        base = applicable.get('base_packaging_weight', 0)
        min_tot = per['min'] * lot_count + base
        max_tot = per['max'] * lot_count + base
        if not (min_tot <= weight_lbs <= max_tot):
            issues.append(
                f"Package weight {weight_lbs:.1f} lbs is outside expected range for {lot_count} "
                f"{device_label}(s): {min_tot:.1f}-{max_tot:.1f} lbs "
                f"({per['min']}-{per['max']} lbs/item + {base} lbs pkg)")

    # ---- DIMENSIONS ----------------------------------------------------
    if 'dimensions' in applicable:
        dcfg = applicable['dimensions']
        dims_available = all(v is not None for v in (length, width, height))

        # When a model-override rule specifies box dimensions we
        # require the listing to include a usable dimensions value.
        if not dims_available:
            if rule_type == 'model_override':
                # Build a human-readable expectation string for the error.
                exp_desc = dcfg.get('description')
                if not exp_desc:
                    if 'exact' in dcfg and len(dcfg['exact']) == 3:
                        exp_desc = f"{dcfg['exact'][0]} x {dcfg['exact'][1]} x {dcfg['exact'][2]} in"
                    elif 'max' in dcfg and len(dcfg['max']) == 3:
                        exp_desc = f"<= {dcfg['max'][0]} x {dcfg['max'][1]} x {dcfg['max'][2]} in"
                    elif 'exact_options' in dcfg:
                        exp_desc = " or ".join(f"{o[0]} x {o[1]} x {o[2]} in" for o in dcfg['exact_options'])
                issues.append(
                    f"Package dimensions Unknown / not provided but required for {device_label}: {exp_desc}")
        else:
            dims = sorted((length, width, height), reverse=True)
            if 'max' in dcfg:
                if any(p > m for p, m in zip(dims, sorted(dcfg['max'], reverse=True))):
                    issues.append(
                        f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in exceed maximum allowed "
                        f"for {device_label}: {dcfg['max'][0]} x {dcfg['max'][1]} x {dcfg['max'][2]} in")
            elif 'exact' in dcfg:
                if dims != sorted(dcfg['exact'], reverse=True):
                    exp = dcfg['exact']
                    issues.append(
                        f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in do not match expected exact "
                        f"size for {device_label}: {exp[0]} x {exp[1]} x {exp[2]} in")
            elif 'exact_options' in dcfg:
                _opts = [o for o in (dcfg.get('exact_options') or []) if isinstance(o, (list, tuple)) and len(o) == 3]
                if _opts:
                    opts_ok = any(dims == sorted(o, reverse=True) for o in _opts)
                    if not opts_ok:
                        opts = " or ".join(f"{o[0]} x {o[1]} x {o[2]} in" for o in _opts)
                        issues.append(
                            f"Package dimensions {length:.1f} x {width:.1f} x {height:.1f} in do not match expected sizes "
                            f"for {device_label}: {opts}")
                # If no options configured, do not enforce
            # New: handle min/max arrays alongside legacy range objects
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
            elif all(k in dcfg for k in ('length','width','height')):
                rngs = [dcfg['length'], dcfg['width'], dcfg['height']]
                for val, axis_rng, axis_name in zip(dims, rngs, ['L','W','H']):
                    axis_min = axis_rng.get('min', 0)
                    axis_max = axis_rng.get('max', 9e9)
                    if not (axis_min <= val <= axis_max):
                        issues.append(
                            f"Package {axis_name}-axis {val:.1f} in outside allowed range {axis_min}-{axis_max} in "
                            f"for {device_label}")
                        break

    # ---- TYPICAL BOX match (optional) ----------------------------------
    if applicable.get('use_typical_box_sizes') and all(v is not None for v in (length, width, height)):
        boxes = load_typical_box_sizes()
        if boxes and not dimensions_match_typical(length, width, height, boxes)[0]:
            issues.append(f"Package dimensions {length:.1f} × {width:.1f} × {height:.1f} in do not match any typical box size")

    # ------------------------------------------------------------------ #
    # 8.  REPORT
    # ------------------------------------------------------------------ #
    if issues:
        for i in issues:
            misc_issues.append((i,))
        misc_info.append(f"  - Package Validation: FAILED - {len(issues)} issue(s) found")
    else:
        misc_info.append("  - Package Validation: PASSED - Package specifications match expectations")

    misc_info.append(f"    • Device: {device_label}, Lot: {lot_count}, Rule: {rule_type}")
    if weight_lbs is not None:
        misc_info.append(f"    • Package: {weight_lbs:.1f} lbs, {length:.1f} x {width:.1f} x {height:.1f} in")
    else:
        misc_info.append(f"    • Package: {pkg_weight_str}, {length:.1f} x {width:.1f} x {height:.1f} in")
    if price_cfg.get('enabled', False):
        price_show = meta.get('listing_price_key', '')
        typ        = meta.get('listing_type_key', '').title()
        if price_show and typ:
            misc_info.append(f"    • Price: {price_show} ({typ})") 