import re
import logging
from collections import Counter, defaultdict
from .base import get_globals_from_main, log_debug

def compare_title_vs_metadata(listing_data, sections, is_power_adapter):
    """Compare title data against metadata."""
    title = {k.replace('title_', ''): v for k, v in listing_data['title'].items()}
    meta = {k.replace('meta_', ''): v for k, v in listing_data['metadata'].items()}
    specs = {k.replace('specs_', ''): v for k, v in listing_data['specifics'].items()}
    table = {k.replace('table_', ''): v for k, v in listing_data['table_data'][0].items()} if listing_data['table_data'] else {}
    title_vs_meta_issues = []
    title_vs_meta = []
    issue_strings = []

    # Get globals from main module
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    current_session_id = globals_dict['current_session_id']
    format_comparison = globals_dict['format_comparison']
    check_equivalence = globals_dict['check_equivalence']

    title_meta_common = set(title.keys()) & set(meta.keys())
    logger.debug(f"Title vs Meta common keys: {title_meta_common}", extra={'session_id': current_session_id})
    for key in title_meta_common:
        t_val, m_val = title[key], meta[key]
        full_key1 = f"title_{key}_key"
        full_key2 = f"meta_{key}_key"
        is_match = check_equivalence(key, t_val, m_val, title=title, specs=specs, table=table)
        entry, issue_str = format_comparison(key, full_key1, t_val, full_key2, m_val, is_match)
        title_vs_meta.append(entry)
        if not is_match:
            logger.debug(f"Adding issue: {issue_str}", extra={'session_id': current_session_id})
            title_vs_meta_issues.append(entry)
            if key != 'cpu_suffix_key':
                if issue_str not in issue_strings:  # Prevent duplication
                    issue_strings.append(issue_str)

    return title_vs_meta, title_vs_meta_issues, issue_strings

def compare_specifics_vs_metadata(listing_data, sections, is_power_adapter):
    """Compare specifics data against metadata."""
    specs = {k.replace('specs_', ''): v for k, v in listing_data['specifics'].items()}
    meta = {k.replace('meta_', ''): v for k, v in listing_data['metadata'].items()}
    title = {k.replace('title_', ''): v for k, v in listing_data['title'].items()}
    table = {k.replace('table_', ''): v for k, v in listing_data['table_data'][0].items()} if listing_data['table_data'] else {}
    specs_vs_meta_issues = []
    specs_vs_meta = []
    issue_strings = []

    # Get globals from main module
    globals_dict = get_globals_from_main()
    logger = globals_dict['logger']
    current_session_id = globals_dict['current_session_id']
    format_comparison = globals_dict['format_comparison']
    check_equivalence = globals_dict['check_equivalence']

    specs_meta_common = set(specs.keys()) & set(meta.keys())
    logger.debug(f"Specs vs Meta common keys: {specs_meta_common}", extra={'session_id': current_session_id})
    for key in specs_meta_common:
        s_val, m_val = specs[key], meta[key]
        full_key1 = f"specs_{key}_key"
        full_key2 = f"meta_{key}_key"
        is_match = check_equivalence(key, s_val, m_val, title=title, specs=specs, table=table)
        entry, issue_str = format_comparison(key, full_key1, s_val, full_key2, m_val, is_match)
        specs_vs_meta.append(entry)
        if not is_match:
            logger.debug(f"Adding issue: {issue_str}", extra={'session_id': current_session_id})
            specs_vs_meta_issues.append(entry)
            if key != 'cpu_suffix_key':
                if issue_str not in issue_strings:  # Prevent duplication
                    issue_strings.append(issue_str)

    return specs_vs_meta, specs_vs_meta_issues, issue_strings