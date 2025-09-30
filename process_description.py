import os
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
import re
import logging
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Set, Any, Optional
import sys
import importlib.util
import queue
import argparse
from collections import Counter
from collections import defaultdict
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
from pathlib import Path

# Database imports for SQLite integration
try:
    from listing_database import get_database, insert_listing
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("Warning: Database module not available, running in file-only mode")

BASE_DIR     = os.path.dirname(__file__)
OUTPUT_DIR   = os.path.join(BASE_DIR, 'output')      # or 'outputs' if that's your folder
# Reduce risk of logging raising exceptions on broken streams/handles
logging.raiseExceptions = False


# Database configuration flags
ENABLE_DATABASE_STORAGE = False and DATABASE_AVAILABLE  # Enable database storage
KEEP_FILE_OUTPUT = True  # Re-enabled - Dual mode (database + files) for safety

# USER DIRECTIVE: Disable toner cartridge classification to avoid false positives for PCs/laptops
ENABLE_TONER_DETECTION = False
# USER DIRECTIVE: Keep 2-in-1 convertible override active (convert laptops to tablets when detected)
ENABLE_2IN1_TO_TABLET_OVERRIDE = True


def force_extract_cpu_speeds(title: str, data: Dict) -> Dict:
    """Force extract CPU speeds from title regardless of other extractors."""
    print(f"DEBUG: force_extract_cpu_speeds called with title: {title}")
    
    # Context-sensitive CPU speed detection. Only accept if speed occurs after CPU indicators
    # and not in RAM context (e.g., ignore 'DDR4 2400MHz').
    speed_token_regexes = [
        r'@[0-9]+\.[0-9]+[gG][hH][zZ]',
        r'[0-9]+\.[0-9]+[gG][hH][zZ]',
        r'[0-9]+[gG][hH][zZ]',
        r'\b\d{2,4}[mM][hH][zZ]\b',
    ]

    cpu_indicators = [
        r'\b(intel|amd|apple)\b', r'\b(cpu|processor)\b', r'\bcore\b', r'\bultra\b',
        r'\bi[3579]\b', r'\bm[357]\b', r'\bryzen\b', r'\bxeon\b', r'\bpentium\b', r'\bceleron\b', r'\bathlon\b',
        r'\bi[3579]-[0-9]{3,5}[A-Za-z0-9]*\b', r'\b[0-9]{1,2}[A-Za-z][0-9]{2,3}\b'
    ]
    ram_indicators = [
        r'\bddr\d?\b', r'\blpddr\d\b', r'\bram\b', r'\bmemory\b', r'\bdimm\b', r'\bsodimm\b', r'\bso-dimm\b',
        r'\budimm\b', r'\brdimm\b', r'\blrdimm\b', r'\becc\b', r'\bpc[34]-?\d{3,4}\b'
    ]

    tokens = re.findall(r"[A-Za-z0-9@\.\-]+", title)
    tokens_lower = [t.lower() for t in tokens]

    def is_ghz_token(tok: str) -> bool:
        return any(re.fullmatch(rx, tok) for rx in [
            r'@[0-9]+\.[0-9]+ghz', r'[0-9]+\.[0-9]+ghz', r'[0-9]+ghz'
        ])

    def is_mhz_token(tok: str) -> bool:
        return bool(re.fullmatch(r'\d{2,4}mhz', tok))

    def has_cpu_context_before(idx: int) -> bool:
        start = max(0, idx - 8)
        window = " ".join(tokens_lower[start:idx])
        return any(re.search(rx, window) for rx in cpu_indicators)

    def has_ram_context_near(idx: int) -> bool:
        start = max(0, idx - 3)
        end = min(len(tokens_lower), idx + 1)
        window = " ".join(tokens_lower[start:end])
        return any(re.search(rx, window) for rx in ram_indicators)

    speeds_found: List[str] = []
    for i, tok in enumerate(tokens_lower):
        if is_ghz_token(tok):
            # Accept standalone GHz regardless of CPU context; just avoid obvious RAM window
            if not has_ram_context_near(i):
                num = float(re.match(r'@?([0-9]+(?:\.[0-9]+)?)ghz', tok).group(1))
                speeds_found.append(f"{num:.2f}GHz")
                print(f"DEBUG: force_extract_cpu_speeds accepted GHz: {speeds_found[-1]}")
            else:
                print(f"DEBUG: force_extract_cpu_speeds rejected GHz due to RAM context: {tok}")
        elif is_mhz_token(tok):
            # MHz must have CPU context and must not follow RAM markers
            if has_cpu_context_before(i) and not has_ram_context_near(i):
                num = re.match(r'(\d{2,4})mhz', tok).group(1)
                speeds_found.append(f"{num}MHz")
                print(f"DEBUG: force_extract_cpu_speeds accepted MHz: {speeds_found[-1]}")
            else:
                print(f"DEBUG: force_extract_cpu_speeds rejected MHz due to context: {tok}")
    
    # Remove duplicates while preserving order
    unique_speeds = []
    for speed in speeds_found:
        if speed not in unique_speeds:
            unique_speeds.append(speed)
    
    # Add CPU speeds to data with proper formatting
    if unique_speeds:
        if len(unique_speeds) == 1:
            data['title_cpu_speed_key'] = unique_speeds[0]
            print(f"DEBUG: Added title_cpu_speed_key = {unique_speeds[0]}")
        else:
            for i, speed in enumerate(unique_speeds, 1):
                key = f'title_cpu_speed{i}_key'
                data[key] = speed
                print(f"DEBUG: Added {key} = {speed}")
    
    return data
CONFIG_DIR   = os.path.join(BASE_DIR, 'configs')
sys.dont_write_bytecode = True
try:
    from configs.known_brands import known_brands
except ImportError:
    known_brands = ["Dell", "HP", "Lenovo", "Apple", "Acer", "Asus", "Toshiba", "Samsung", "Microsoft", "Sony", "IBM", "Gateway", "Compaq", "Fujitsu", "Panasonic", "LG", "MSI", "Razer", "Alienware"]

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip().lstrip('\ufeff')
    
    # Remove the problematic "Â" character that often appears before trademark symbols
    text = text.replace('Â', '')
    
    # Replace trademark, copyright, and registered symbols with spaces to preserve token boundaries
    # This includes various Unicode representations of these symbols
    text = re.sub(r'[™©®\u00ae\u00a9\u2122\u00a0\u2120\u2117]', ' ', text)
    
    # Replace other common encoding artifacts and special characters
    text = text.replace('@', ' ')
    text = text.replace('°', ' ')  # degree symbol often appears in specs
    
    # Replace pipe characters with spaces to treat them as separators
    text = text.replace('|', ' ')
    
    # Replace multiple whitespace with a single space
    text = re.sub(r'\s+', ' ', text)
    return text
    
def normalize_units(text):
    """Convert formats like '8/16GB' to '8GB/16GB' but avoid longer sequences"""
    
    # Only apply to patterns that are:
    # 1. At word boundaries 
    # 2. Not preceded by "number/"
    # 3. Not followed by "/number"
    pattern = r'(?<![/\d])\b(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)([A-Z][A-Za-z]*)\b(?!/\d)'
    
    def replace_func(match):
        first_num = match.group(1)
        second_num = match.group(2)
        unit = match.group(3)
        return f"{first_num}{unit}/{second_num}{unit}"
    
    return re.sub(pattern, replace_func, text)
    
def tokenize_with_slash_splitting(text: str, logger: logging.Logger) -> List[str]:
    logger.debug(f"Tokenizing text: '{text}'")
    text = clean_text(text)
    if not text:
        return []
    
    # Convert & signs to / for consistent parsing
    text = text.replace('&', '/')

    # Normalize unit slashes like 8/16GB -> 8GB/16GB (also safe for GHz)
    try:
        text = normalize_units(text)
    except Exception:
        pass
    
    # Normalize CPU "and" patterns like "i5-7300U and 5300U" to "i5-7300U/5300U"
    import re
    cpu_and_pattern = r'\b(i[357]-\d{4}[A-Z]*)\s+and\s+(\d{4}[A-Z]*)\b'
    text = re.sub(cpu_and_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize CPU speed patterns like "2.60GHz/ 2.30GHz" to "2.60GHz/2.30GHz"
    cpu_speed_pattern = r'\b(\d+\.?\d*GHz)\s*/\s*(\d+\.?\d*GHz)\b'
    text = re.sub(cpu_speed_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # NEW: Fill in abbreviated CPU speed pairs like "2.80/70GHz" -> "2.80GHz/2.70GHz"
    def _expand_abbrev_speed(match):
        int_part = match.group(1)            # "2"
        first_dec = match.group(2)          # "80"
        second_digits = match.group(3)      # "70"
        # Pad/truncate second decimal part to match length of first
        second_dec = second_digits.zfill(len(first_dec))[:len(first_dec)]
        first_val = f"{int_part}.{first_dec}GHz"
        second_val = f"{int_part}.{second_dec}GHz"
        return f"{first_val}/{second_val}"
    cpu_speed_abbrev_pattern = r'\b(\d+)\.(\d+)\s*/\s*(\d{1,3})(?:[gG][hH][zZ])\b'
    text = re.sub(cpu_speed_abbrev_pattern, _expand_abbrev_speed, text)

    # NEW: Fill missing GHz on the second value: "2.00GHz/ 2.90" -> "2.00GHz/2.90GHz"
    cpu_speed_missing_second_unit = r'\b(\d+(?:\.\d+)?)\s*[Gg][Hh][Zz]\s*/\s*(\d+(?:\.\d+)?)(?=\b|\s*/)'
    text = re.sub(cpu_speed_missing_second_unit, r'\1GHz/\2GHz', text)

    # NEW: Fill missing GHz on the first value: "2.00/2.90GHz" -> "2.00GHz/2.90GHz"
    cpu_speed_missing_first_unit = r'\b(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s*[Gg][Hh][Zz]\b'
    text = re.sub(cpu_speed_missing_first_unit, r'\1GHz/\2GHz', text)

    # NEW: Split compounded lot quantity like "Lot(3)" -> "Lot (3)"
    text = re.sub(r'\b([Ll]ot)\s*\((\d+)\)', r'Lot (\2)', text)
    
    # FIRST: Normalize common compound words
    # Split "intelcore" into "Intel Core" (preserving case pattern)
    def replace_intelcore(match):
        original = match.group(0)
        if original.isupper():
            return 'INTEL CORE'
        elif original.islower():
            return 'intel core'
        else:
            return 'Intel Core'
    text = re.sub(r'\bintelcore\b', replace_intelcore, text, flags=re.IGNORECASE)
    
    # Split "Xthgen" patterns into "Xth Gen" (e.g., "8thgen" -> "8th gen", preserving case)
    def replace_thgen(match):
        number = match.group(1)
        original_suffix = match.group(0)[len(number):]  # Get "thgen" part
        if original_suffix.isupper():
            return f'{number}TH GEN'
        elif original_suffix.islower():
            return f'{number}th gen'
        else:
            return f'{number}th Gen'
    text = re.sub(r'\b(\d+)thgen\b', replace_thgen, text, flags=re.IGNORECASE)
    
    # Split RTX/GTX followed by numbers (e.g., "RTX4000" -> "RTX 4000", preserving case)
    def replace_rtx_gtx(match):
        prefix = match.group(1)  # RTX or GTX
        number = match.group(2)  # The number part
        return f'{prefix} {number}'
    text = re.sub(r'\b(RTX|GTX)(\d+)\b', replace_rtx_gtx, text, flags=re.IGNORECASE)
    logger.debug(f"After compound word normalization: '{text}'")
    
    # SECOND: Normalize units in slash patterns like "8/16GB" to "8GB/16GB"
    text = normalize_units(text)
    logger.debug(f"After unit normalization: '{text}'")
    
    initial_tokens = text.split()
    tokens = []
    i = 0
    while i < len(initial_tokens):
        # ENHANCED: Combine numbers with storage/memory units (GB, TB, MB) or frequency units (GHz, MHz, etc.)
        # This handles patterns like "32 GB", "16 TB", "2.4 GHz"
        if (i + 1 < len(initial_tokens) and 
            re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
            re.match(r"^(GB|TB|MB|GHz|MHz|KHz|THz)$", initial_tokens[i+1], re.IGNORECASE)):
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            logger.debug(f"Combined storage/frequency unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}'")
            i += 2
        # ENHANCED: Also handle memory type combinations like "32 GB RAM", "16 GB DDR4"
        elif (i + 2 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(GB|TB|MB)$", initial_tokens[i+1], re.IGNORECASE) and
              re.match(r"^(RAM|MEMORY|DDR|DDR2|DDR3|DDR4|DDR5)$", initial_tokens[i+2], re.IGNORECASE)):
            # Combine the number and unit, but keep the type separate
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            tokens.append(initial_tokens[i+2])
            logger.debug(f"Combined memory unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}', kept '{initial_tokens[i+2]}' separate")
            i += 3
        # ENHANCED: Handle storage type combinations like "32 GB SSD", "1 TB HDD"
        elif (i + 2 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(GB|TB|MB)$", initial_tokens[i+1], re.IGNORECASE) and
              re.match(r"^(SSD|HDD|NVME|M\.2|EMMC|STORAGE)$", initial_tokens[i+2], re.IGNORECASE)):
            # Combine the number and unit, but keep the type separate
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            tokens.append(initial_tokens[i+2])
            logger.debug(f"Combined storage unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}', kept '{initial_tokens[i+2]}' separate")
            i += 3
        # ENHANCED: Handle screen size combinations like "10.2 in", "15.6 inch" - FIXED with word boundaries
        elif (i + 1 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(IN|INCH|INCHES)$", initial_tokens[i+1], re.IGNORECASE)):
            combined = initial_tokens[i] + "in"  # Normalize to "in"
            tokens.append(combined)
            logger.debug(f"Combined screen size: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}'")
            i += 2
        # NEW: Handle speed/rate patterns like "6Gb/s", "6GB/s" - DON'T split these
        elif re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", initial_tokens[i], re.IGNORECASE):
            tokens.append(initial_tokens[i])
            logger.debug(f"Preserved speed/rate pattern: '{initial_tokens[i]}'")
            i += 1
        else:
            tokens.append(initial_tokens[i])
            i += 1
    
    result = []
    i = 0
    
    while i < len(tokens):
        token = tokens[i]
        
        # Handle pattern like "(1x)Intel", "(2x)AMD", "(x2)Intel", or "(x3)AMD" - split into separate tokens
        cpu_brand_pattern = re.match(r'^\((\d+x|x\d+)\)([\w-]+)$', token, re.IGNORECASE)
        if cpu_brand_pattern:
            quantity_part = f"({cpu_brand_pattern.group(1)})"
            brand_part = cpu_brand_pattern.group(2)
            result.append(quantity_part)
            result.append(brand_part)
            i += 1
            continue
        
        # Handle combined RAM patterns like "8GBRAM", "16GBRAM"
        ram_pattern = re.match(r'^(\d+)(GB|TB|MB)(RAM|MEMORY)$', token, re.IGNORECASE)
        if ram_pattern:
            size_part = ram_pattern.group(1) + ram_pattern.group(2)
            type_part = ram_pattern.group(3)
            result.append(size_part)
            result.append(type_part)
            i += 1
            continue
        
        # Handle combined storage patterns like "256GBSSD", "1TBHDD"
        storage_pattern = re.match(r'^(\d+)(GB|TB|MB)(SSD|HDD|NVME|EMMC)$', token, re.IGNORECASE)
        if storage_pattern:
            size_part = storage_pattern.group(1) + storage_pattern.group(2)
            type_part = storage_pattern.group(3)
            result.append(size_part)
            result.append(type_part)
            i += 1
            continue
        
        if token == "N/A":  # Preserve "N/A" as a single token
            result.append(token)
            i += 1
        elif token.lower() in ["no", "without"] and i + 1 < len(tokens):
            next_token = tokens[i + 1]
            if '/' in next_token and 'M.2/BATTERY' not in next_token and not re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", next_token, re.IGNORECASE):
                parts = next_token.split('/')
                for part in parts:
                    if part:
                        result.append(token)
                        result.append(part)
                i += 2
            else:
                result.append(token + " " + next_token)
                i += 2
        elif '/' in token and 'M.2/BATTERY' not in token and not re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", token, re.IGNORECASE):
            parts = []
            current = ""
            for char in token:
                if char == '/':
                    if current:
                        parts.append(current)
                        current = ""
                    parts.append('/')
                else:
                    current += char
            if current:
                parts.append(current)
            result.extend(parts)
            i += 1
        else:
            # Remove any punctuation that should be separate
            clean_token = re.sub(r'([@®©™])', r' \1 ', token)
            if clean_token != token and ' ' in clean_token:
                # If we added spaces, split it up
                sub_tokens = clean_token.split()
                result.extend([t for t in sub_tokens if t])
            else:
                result.append(token)
            i += 1
    
    # NEW: Merge CPU family token with adjacent model number (e.g., "i7-" + "3770" -> "i7-3770").
    merged_tokens = []
    i = 0
    while i < len(result):
        if (
            i + 1 < len(result)
            and re.match(r'^i[3579]-$', result[i], re.IGNORECASE)
            and re.match(r'^\d', result[i + 1])
        ):
            merged = result[i] + result[i + 1]
            merged_tokens.append(merged)
            logger.debug(
                f"Merged CPU family and model tokens: '{result[i]}' + '{result[i + 1]}' = '{merged}'"
            )
            i += 2
        else:
            merged_tokens.append(result[i])
            i += 1

    logger.debug(f"Tokenized into {len(merged_tokens)} tokens: {merged_tokens}")
    return merged_tokens
    
def tokenize(text: str, logger: logging.Logger) -> List[str]:
    logger.debug(f"Tokenizing text: '{text}'")
    text = clean_text(text)
    if not text:
        logger.debug("Empty text provided for tokenization")
        return []
    
    # Normalize CPU "and" patterns like "i5-7300U and 5300U" to "i5-7300U/5300U"
    import re
    cpu_and_pattern = r'\b(i[357]-\d{4}[A-Z]*)\s+and\s+(\d{4}[A-Z]*)\b'
    text = re.sub(cpu_and_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize CPU speed patterns like "2.60GHz/ 2.30GHz" to "2.60GHz/2.30GHz"
    cpu_speed_pattern = r'\b(\d+\.?\d*GHz)\s*/\s*(\d+\.?\d*GHz)\b'
    text = re.sub(cpu_speed_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize common compound words
    # Split "intelcore" into "Intel Core" (preserving case pattern)
    def replace_intelcore(match):
        original = match.group(0)
        if original.isupper():
            return 'INTEL CORE'
        elif original.islower():
            return 'intel core'
        else:
            return 'Intel Core'
    text = re.sub(r'\bintelcore\b', replace_intelcore, text, flags=re.IGNORECASE)
    
    # Split "Xthgen" patterns into "Xth Gen" (e.g., "8thgen" -> "8th gen", preserving case)
    def replace_thgen(match):
        number = match.group(1)
        original_suffix = match.group(0)[len(number):]  # Get "thgen" part
        if original_suffix.isupper():
            return f'{number}TH GEN'
        elif original_suffix.islower():
            return f'{number}th gen'
        else:
            return f'{number}th Gen'
    text = re.sub(r'\b(\d+)thgen\b', replace_thgen, text, flags=re.IGNORECASE)
    
    # Split RTX/GTX followed by numbers (e.g., "RTX4000" -> "RTX 4000", preserving case)
    def replace_rtx_gtx(match):
        prefix = match.group(1)  # RTX or GTX
        number = match.group(2)  # The number part
        return f'{prefix} {number}'
    text = re.sub(r'\b(RTX|GTX)(\d+)\b', replace_rtx_gtx, text, flags=re.IGNORECASE)
    logger.debug(f"After compound word normalization: '{text}'")
    
    tokens = text.split()
    result = []
    i = 0
    while i < len(tokens):
        current_token = tokens[i]
        if current_token.lower() in ["no", "with", "includes"] and i + 1 < len(tokens):
            result.append(current_token + " " + tokens[i + 1])
            logger.debug(f"Combined special token: '{result[-1]}'")
            i += 2
        else:
            result.append(current_token)
            i += 1
    logger.debug(f"Tokenized into {len(result)} tokens: {result}")
    return result

def load_extractors(logger=None) -> List[Any]:
    extractors = []
    extractor_priorities = {
        "lot": 0,
        "cpu": 1,
        "ram": 2,
        "storage": 3,
        "screen": 4,
        "gpu": 5,
        "os": 6,
        "device_type": 7,
        "battery": 8,
        "switch": 9,
        "adapter": 10
    }
    all_extractors = []
    for filename in os.listdir(CONFIG_DIR):
        if filename.startswith("extractor_") and filename.endswith(".py"):
            module_name = f"configs.{filename[:-3]}"
            spec = importlib.util.spec_from_file_location(module_name, os.path.join(CONFIG_DIR, filename))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            configs = getattr(module, "extractor_config", [])
            for config in configs:
                extractor_class = config["class"]
                # Pass logger to the extractor constructor
                if "logger" in extractor_class.__init__.__code__.co_varnames:
                    extractor_instance = extractor_class(config, logger)
                else:
                    # For older extractor classes that don't accept logger
                    extractor_instance = extractor_class(config)
                    
                # Set logger attribute directly if not handled by constructor
                if not hasattr(extractor_instance, 'logger'):
                    setattr(extractor_instance, 'logger', logger)
                
                # Apply consume_on_match setting from config to extractor instance
                if "consume_on_match" in config:
                    extractor_instance.consume_on_match = config["consume_on_match"]
                
                all_extractors.append((extractor_instance, config["name"]))
    
    def get_priority(extractor_tuple):
        extractor_name = extractor_tuple[1]
        for prefix, priority in extractor_priorities.items():
            if extractor_name.startswith(prefix):
                return priority
        return 999
    
    all_extractors.sort(key=get_priority)
    return [ext for ext, _ in all_extractors]
    
def process_multi_instance_data(data: Dict, base_name: str, matches: List[List[int]], extractor: Any, tokens: List[str], consumed: Set[int] = None) -> Dict:
    try:
        # Create an empty set if consumed is None
        if consumed is None:
            consumed = set()
            
        if len(matches) > 1:
            for i, match_indices in enumerate(matches, 1):
                flat_indices = []
                for idx in match_indices:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        if consumed is not None:
                            consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        for key, value in extracted.items():
                            if key == f"{base_name}_size" or key == f"{base_name}_capacity":
                                data[f"{key}{i}"] = value
                                if i == 1:
                                    data[key] = value
                            elif key.startswith(f"{base_name}_"):
                                data[key] = value
                                data[f"{key}{i}"] = value
                            else:
                                data[key] = value
        elif matches:
            if matches[0]:
                flat_indices = []
                for idx in matches[0]:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        data.update(extracted)
    except Exception as e:
        import traceback
        print(f"Error in process_multi_instance_data: {e}")
        print(traceback.format_exc())
    
    return data

try:
    import win32api
    import pyperclip
except ImportError as e:
    print(f"Required module missing: {e}. Install with 'pip install pywin32 pyperclip'")
    sys.exit(1)

# Optional import for known phone carriers (used to enrich title parsing for carrier strings like 'Verizon')
try:
    from configs.extractor_phone import network_carriers as PHONE_NETWORK_CARRIERS
except Exception:
    PHONE_NETWORK_CARRIERS = [
        "Verizon", "AT&T", "T-Mobile", "US Cellular", "Cricket", "Metro",
        "Boost Mobile", "Mint Mobile", "Google Fi", "Xfinity Mobile", "Spectrum Mobile",
        "Straight Talk", "Total by Verizon", "Simple Mobile"
    ]

PHONE_CARRIER_SYNONYMS = {
    "vzw": "Verizon",
    "att": "AT&T",
    "tmobile": "T-Mobile",
}

def enrich_network_carriers_from_title_tokens(tokens: List[str], data: Dict, logger: logging.Logger) -> None:
    """Detect known carriers in title tokens and add numbered network_carrier keys.

    Non-destructive: does not overwrite existing network_carrier keys.
    """
    try:
        title_text_lower = " ".join(tokens).lower()
        found: List[str] = []
        # Direct names
        for carrier in PHONE_NETWORK_CARRIERS:
            if carrier and carrier.strip():
                if carrier.lower() in title_text_lower:
                    found.append(carrier)
        # Synonyms
        for syn, canonical in PHONE_CARRIER_SYNONYMS.items():
            if syn in title_text_lower and canonical not in found:
                found.append(canonical)

        # Deduplicate while preserving order
        seen = set()
        carriers_unique = [c for c in found if not (c in seen or seen.add(c))]

        if not carriers_unique:
            return

        # Determine next index
        existing_indices = []
        for key in data.keys():
            m = re.match(r"network_carrier(\d+)$", key)
            if m:
                existing_indices.append(int(m.group(1)))
        next_idx = max(existing_indices, default=0) + 1

        for carrier in carriers_unique:
            key = f"network_carrier{next_idx}"
            if key not in data:
                data[key] = carrier
                if logger:
                    logger.debug(f"Title enrichment added {key}: {carrier}")
                next_idx += 1
    except Exception as e:
        if logger:
            logger.debug(f"Carrier enrichment skipped due to error: {e}")

LOGGING_ENABLED = True
PROCESSING_TIMEOUT = 300  # 5 minutes timeout per file


try:
    from configs.known_brands import known_brands
except ImportError:
    known_brands = ["Dell", "HP", "Lenovo", "Apple", "Acer", "Asus", "Toshiba", "Samsung", "Microsoft", "Sony", "IBM", "Gateway", "Compaq", "Fujitsu", "Panasonic", "LG", "MSI", "Razer", "Alienware"]
            
def find_token_indices(tokens, match):
    """Helper function to find token indices for regex matches"""
    start, end = match.span()
    char_pos = 0
    indices = []
    for i, token in enumerate(tokens):
        token_start = char_pos
        token_end = char_pos + len(token)
        if token_start < end and token_end > start:
            indices.append(i)
        char_pos += len(token) + 1  # +1 for space
    return indices

    extractors = []
    extractor_priorities = {
        "lot": 0,
        "cpu": 1,
        "ram": 2,
        "storage": 3,
        "screen": 4,
        "gpu": 5,
        "os": 6,
        "device_type": 7,
        "battery": 8,
        "switch": 9,
        "adapter": 10
    }
    all_extractors = []
    for filename in os.listdir(CONFIG_DIR):
        if filename.startswith("extractor_") and filename.endswith(".py"):
            module_name = f"configs.{filename[:-3]}"
            spec = importlib.util.spec_from_file_location(module_name, os.path.join(CONFIG_DIR, filename))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            configs = getattr(module, "extractor_config", [])
            for config in configs:
                extractor_class = config["class"]
                extractor_instance = extractor_class(config)
                
                # Apply consume_on_match setting from config to extractor instance
                if "consume_on_match" in config:
                    extractor_instance.consume_on_match = config["consume_on_match"]
                
                all_extractors.append((extractor_instance, config["name"]))
    
    def get_priority(extractor_tuple):
        extractor_name = extractor_tuple[1]
        for prefix, priority in extractor_priorities.items():
            if extractor_name.startswith(prefix):
                return priority
        return 999
    
    all_extractors.sort(key=get_priority)
    return [ext for ext, _ in all_extractors]
    
    try:
        if len(matches) > 1:
            for i, match_indices in enumerate(matches, 1):
                flat_indices = []
                for idx in match_indices:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        for key, value in extracted.items():
                            if key == f"{base_name}_size" or key == f"{base_name}_capacity":
                                data[f"{key}{i}"] = value
                                if i == 1:
                                    data[key] = value
                            elif key.startswith(f"{base_name}_"):
                                data[key] = value
                                data[f"{key}{i}"] = value
                            else:
                                data[key] = value
        elif matches:
            if matches[0]:
                flat_indices = []
                for idx in matches[0]:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        data.update(extracted)
    except Exception as e:
        import traceback
        print(f"Error in process_multi_instance_data: {e}")
        print(traceback.format_exc())
    
    return data

    if not text:
        return ""
    text = text.strip().lstrip('\ufeff')
    
    # Remove the problematic "Â" character that often appears before trademark symbols
    text = text.replace('Â', '')
    
    # Replace trademark, copyright, and registered symbols with spaces to preserve token boundaries
    # This includes various Unicode representations of these symbols
    text = re.sub(r'[™©®\u00ae\u00a9\u2122\u00a0\u2120\u2117]', ' ', text)
    
    # Replace other common encoding artifacts and special characters
    text = text.replace('@', ' ')
    text = text.replace('°', ' ')  # degree symbol often appears in specs
    
    # Replace pipe characters with spaces to treat them as separators
    text = text.replace('|', ' ')
    
    # Replace multiple whitespace with a single space
    text = re.sub(r'\s+', ' ', text)
    return text
    
    """Convert formats like '8/16GB' to '8GB/16GB' but avoid longer sequences"""
    
    # Only apply to patterns that are:
    # 1. At word boundaries 
    # 2. Not preceded by "number/"
    # 3. Not followed by "/number"
    pattern = r'(?<![/\d])\b(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)([A-Z][A-Za-z]*)\b(?!/\d)'
    
    def replace_func(match):
        first_num = match.group(1)
        second_num = match.group(2)
        unit = match.group(3)
        return f"{first_num}{unit}/{second_num}{unit}"
    
    return re.sub(pattern, replace_func, text)
    
    logger.debug(f"Tokenizing text: '{text}'")
    text = clean_text(text)
    if not text:
        return []
    
    # Convert & signs to / for consistent parsing
    text = text.replace('&', '/')
    
    # Normalize CPU "and" patterns like "i5-7300U and 5300U" to "i5-7300U/5300U"
    import re
    cpu_and_pattern = r'\b(i[357]-\d{4}[A-Z]*)\s+and\s+(\d{4}[A-Z]*)\b'
    text = re.sub(cpu_and_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize CPU speed patterns like "2.60GHz/ 2.30GHz" to "2.60GHz/2.30GHz"
    cpu_speed_pattern = r'\b(\d+\.?\d*GHz)\s*/\s*(\d+\.?\d*GHz)\b'
    text = re.sub(cpu_speed_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # NEW: Fill in abbreviated CPU speed pairs like "2.80/70GHz" -> "2.80GHz/2.70GHz"
    def _expand_abbrev_speed(match):
        int_part = match.group(1)            # "2"
        first_dec = match.group(2)          # "80"
        second_digits = match.group(3)      # "70"
        # Pad/truncate second decimal part to match length of first
        second_dec = second_digits.zfill(len(first_dec))[:len(first_dec)]
        first_val = f"{int_part}.{first_dec}GHz"
        second_val = f"{int_part}.{second_dec}GHz"
        return f"{first_val}/{second_val}"
    cpu_speed_abbrev_pattern = r'\b(\d+)\.(\d+)\s*/\s*(\d{1,3})(?:[gG][hH][zZ])\b'
    text = re.sub(cpu_speed_abbrev_pattern, _expand_abbrev_speed, text)
    
    # FIRST: Normalize common compound words
    # Split "intelcore" into "Intel Core" (preserving case pattern)
    def replace_intelcore(match):
        original = match.group(0)
        if original.isupper():
            return 'INTEL CORE'
        elif original.islower():
            return 'intel core'
        else:
            return 'Intel Core'
    text = re.sub(r'\bintelcore\b', replace_intelcore, text, flags=re.IGNORECASE)
    
    # Split "Xthgen" patterns into "Xth Gen" (e.g., "8thgen" -> "8th gen", preserving case)
    def replace_thgen(match):
        number = match.group(1)
        original_suffix = match.group(0)[len(number):]  # Get "thgen" part
        if original_suffix.isupper():
            return f'{number}TH GEN'
        elif original_suffix.islower():
            return f'{number}th gen'
        else:
            return f'{number}th Gen'
    text = re.sub(r'\b(\d+)thgen\b', replace_thgen, text, flags=re.IGNORECASE)
    
    # Split RTX/GTX followed by numbers (e.g., "RTX4000" -> "RTX 4000", preserving case)
    def replace_rtx_gtx(match):
        prefix = match.group(1)  # RTX or GTX
        number = match.group(2)  # The number part
        return f'{prefix} {number}'
    text = re.sub(r'\b(RTX|GTX)(\d+)\b', replace_rtx_gtx, text, flags=re.IGNORECASE)
    logger.debug(f"After compound word normalization: '{text}'")
    
    # SECOND: Normalize units in slash patterns like "8/16GB" to "8GB/16GB"
    text = normalize_units(text)
    logger.debug(f"After unit normalization: '{text}'")
    
    initial_tokens = text.split()
    tokens = []
    i = 0
    while i < len(initial_tokens):
        # ENHANCED: Combine numbers with storage/memory units (GB, TB, MB) or frequency units (GHz, MHz, etc.)
        # This handles patterns like "32 GB", "16 TB", "2.4 GHz"
        if (i + 1 < len(initial_tokens) and 
            re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
            re.match(r"^(GB|TB|MB|GHz|MHz|KHz|THz)$", initial_tokens[i+1], re.IGNORECASE)):
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            logger.debug(f"Combined storage/frequency unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}'")
            i += 2
        # ENHANCED: Also handle memory type combinations like "32 GB RAM", "16 GB DDR4"
        elif (i + 2 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(GB|TB|MB)$", initial_tokens[i+1], re.IGNORECASE) and
              re.match(r"^(RAM|MEMORY|DDR|DDR2|DDR3|DDR4|DDR5)$", initial_tokens[i+2], re.IGNORECASE)):
            # Combine the number and unit, but keep the type separate
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            tokens.append(initial_tokens[i+2])
            logger.debug(f"Combined memory unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}', kept '{initial_tokens[i+2]}' separate")
            i += 3
        # ENHANCED: Handle storage type combinations like "32 GB SSD", "1 TB HDD"
        elif (i + 2 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(GB|TB|MB)$", initial_tokens[i+1], re.IGNORECASE) and
              re.match(r"^(SSD|HDD|NVME|M\.2|EMMC|STORAGE)$", initial_tokens[i+2], re.IGNORECASE)):
            # Combine the number and unit, but keep the type separate
            combined = initial_tokens[i] + initial_tokens[i+1]
            tokens.append(combined)
            tokens.append(initial_tokens[i+2])
            logger.debug(f"Combined storage unit: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}', kept '{initial_tokens[i+2]}' separate")
            i += 3
        # ENHANCED: Handle screen size combinations like "10.2 in", "15.6 inch" - FIXED with word boundaries
        elif (i + 1 < len(initial_tokens) and 
              re.match(r"\d+(?:\.\d+)?", initial_tokens[i]) and 
              re.match(r"^(IN|INCH|INCHES)$", initial_tokens[i+1], re.IGNORECASE)):
            combined = initial_tokens[i] + "in"  # Normalize to "in"
            tokens.append(combined)
            logger.debug(f"Combined screen size: '{initial_tokens[i]}' + '{initial_tokens[i+1]}' = '{combined}'")
            i += 2
        # NEW: Handle speed/rate patterns like "6Gb/s", "6GB/s" - DON'T split these
        elif re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", initial_tokens[i], re.IGNORECASE):
            tokens.append(initial_tokens[i])
            logger.debug(f"Preserved speed/rate pattern: '{initial_tokens[i]}'")
            i += 1
        else:
            tokens.append(initial_tokens[i])
            i += 1
    
    result = []
    i = 0
    
    while i < len(tokens):
        token = tokens[i]
        
        # Handle pattern like "(1x)Intel", "(2x)AMD", "(x2)Intel", or "(x3)AMD" - split into separate tokens
        cpu_brand_pattern = re.match(r'^\((\d+x|x\d+)\)([\w-]+)$', token, re.IGNORECASE)
        if cpu_brand_pattern:
            quantity_part = f"({cpu_brand_pattern.group(1)})"
            brand_part = cpu_brand_pattern.group(2)
            result.append(quantity_part)
            result.append(brand_part)
            i += 1
            continue
        
        # Handle combined RAM patterns like "8GBRAM", "16GBRAM"
        ram_pattern = re.match(r'^(\d+)(GB|TB|MB)(RAM|MEMORY)$', token, re.IGNORECASE)
        if ram_pattern:
            size_part = ram_pattern.group(1) + ram_pattern.group(2)
            type_part = ram_pattern.group(3)
            result.append(size_part)
            result.append(type_part)
            i += 1
            continue
        
        # Handle combined storage patterns like "256GBSSD", "1TBHDD"
        storage_pattern = re.match(r'^(\d+)(GB|TB|MB)(SSD|HDD|NVME|EMMC)$', token, re.IGNORECASE)
        if storage_pattern:
            size_part = storage_pattern.group(1) + storage_pattern.group(2)
            type_part = storage_pattern.group(3)
            result.append(size_part)
            result.append(type_part)
            i += 1
            continue
        
        if token == "N/A":  # Preserve "N/A" as a single token
            result.append(token)
            i += 1
        elif token.lower() in ["no", "without"] and i + 1 < len(tokens):
            next_token = tokens[i + 1]
            if '/' in next_token and 'M.2/BATTERY' not in next_token and not re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", next_token, re.IGNORECASE):
                parts = next_token.split('/')
                for part in parts:
                    if part:
                        result.append(token)
                        result.append(part)
                i += 2
            else:
                result.append(token + " " + next_token)
                i += 2
        elif '/' in token and 'M.2/BATTERY' not in token and not re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", token, re.IGNORECASE):
            parts = []
            current = ""
            for char in token:
                if char == '/':
                    if current:
                        parts.append(current)
                        current = ""
                    parts.append('/')
                else:
                    current += char
            if current:
                parts.append(current)
            result.extend(parts)
            i += 1
        else:
            # Remove any punctuation that should be separate
            clean_token = re.sub(r'([@®©™])', r' \1 ', token)
            if clean_token != token and ' ' in clean_token:
                # If we added spaces, split it up
                sub_tokens = clean_token.split()
                result.extend([t for t in sub_tokens if t])
            else:
                result.append(token)
            i += 1
    
    # NEW: Merge CPU family token with adjacent model number (e.g., "i7-" + "3770" -> "i7-3770").
    merged_tokens = []
    i = 0
    while i < len(result):
        if (
            i + 1 < len(result)
            and re.match(r'^i[3579]-$', result[i], re.IGNORECASE)
            and re.match(r'^\d', result[i + 1])
        ):
            merged = result[i] + result[i + 1]
            merged_tokens.append(merged)
            logger.debug(
                f"Merged CPU family and model tokens: '{result[i]}' + '{result[i + 1]}' = '{merged}'"
            )
            i += 2
        else:
            merged_tokens.append(result[i])
            i += 1

    logger.debug(f"Tokenized into {len(merged_tokens)} tokens: {merged_tokens}")
    return merged_tokens
    
    logger.debug(f"Tokenizing text: '{text}'")
    text = clean_text(text)
    if not text:
        logger.debug("Empty text provided for tokenization")
        return []
    
    # Convert & signs to / for consistent parsing
    text = text.replace('&', '/')
    
    # Normalize CPU "and" patterns like "i5-7300U and 5300U" to "i5-7300U/5300U"
    import re
    cpu_and_pattern = r'\b(i[357]-\d{4}[A-Z]*)\s+and\s+(\d{4}[A-Z]*)\b'
    text = re.sub(cpu_and_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize CPU speed patterns like "2.60GHz/ 2.30GHz" to "2.60GHz/2.30GHz"
    cpu_speed_pattern = r'\b(\d+\.?\d*GHz)\s*/\s*(\d+\.?\d*GHz)\b'
    text = re.sub(cpu_speed_pattern, r'\1/\2', text, flags=re.IGNORECASE)
    
    # Normalize common compound words
    # Split "intelcore" into "Intel Core" (preserving case pattern)
    def replace_intelcore(match):
        original = match.group(0)
        if original.isupper():
            return 'INTEL CORE'
        elif original.islower():
            return 'intel core'
        else:
            return 'Intel Core'
    text = re.sub(r'\bintelcore\b', replace_intelcore, text, flags=re.IGNORECASE)
    
    # Split "Xthgen" patterns into "Xth Gen" (e.g., "8thgen" -> "8th gen", preserving case)
    def replace_thgen(match):
        number = match.group(1)
        original_suffix = match.group(0)[len(number):]  # Get "thgen" part
        if original_suffix.isupper():
            return f'{number}TH GEN'
        elif original_suffix.islower():
            return f'{number}th gen'
        else:
            return f'{number}th Gen'
    text = re.sub(r'\b(\d+)thgen\b', replace_thgen, text, flags=re.IGNORECASE)
    
    # Split RTX/GTX followed by numbers (e.g., "RTX4000" -> "RTX 4000", preserving case)
    def replace_rtx_gtx(match):
        prefix = match.group(1)  # RTX or GTX
        number = match.group(2)  # The number part
        return f'{prefix} {number}'
    text = re.sub(r'\b(RTX|GTX)(\d+)\b', replace_rtx_gtx, text, flags=re.IGNORECASE)
    logger.debug(f"After compound word normalization: '{text}'")
    
    tokens = text.split()
    result = []
    i = 0
    while i < len(tokens):
        current_token = tokens[i]
        if current_token.lower() in ["no", "with", "includes"] and i + 1 < len(tokens):
            result.append(current_token + " " + tokens[i + 1])
            logger.debug(f"Combined special token: '{result[-1]}'")
            i += 2
        else:
            result.append(current_token)
            i += 1
    logger.debug(f"Tokenized into {len(result)} tokens: {result}")
    return result

    extractors = []
    extractor_priorities = {
        "lot": 0,
        "cpu": 1,
        "ram": 2,
        "storage": 3,
        "screen": 4,
        "gpu": 5,
        "os": 6,
        "device_type": 7,
        "battery": 8,
        "switch": 9,
        "adapter": 10
    }
    all_extractors = []
    for filename in os.listdir(CONFIG_DIR):
        if filename.startswith("extractor_") and filename.endswith(".py"):
            module_name = f"configs.{filename[:-3]}"
            spec = importlib.util.spec_from_file_location(module_name, os.path.join(CONFIG_DIR, filename))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            configs = getattr(module, "extractor_config", [])
            for config in configs:
                extractor_class = config["class"]
                extractor_instance = extractor_class(config)
                
                # Apply consume_on_match setting from config to extractor instance
                if "consume_on_match" in config:
                    extractor_instance.consume_on_match = config["consume_on_match"]
                
                all_extractors.append((extractor_instance, config["name"]))
    
    def get_priority(extractor_tuple):
        extractor_name = extractor_tuple[1]
        for prefix, priority in extractor_priorities.items():
            if extractor_name.startswith(prefix):
                return priority
        return 999
    
    all_extractors.sort(key=get_priority)
    return [ext for ext, _ in all_extractors]
    
    try:
        if len(matches) > 1:
            for i, match_indices in enumerate(matches, 1):
                flat_indices = []
                for idx in match_indices:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        for key, value in extracted.items():
                            if key == f"{base_name}_size" or key == f"{base_name}_capacity":
                                data[f"{key}{i}"] = value
                                if i == 1:
                                    data[key] = value
                            elif key.startswith(f"{base_name}_"):
                                data[key] = value
                                data[f"{key}{i}"] = value
                            else:
                                data[key] = value
        elif matches:
            if matches[0]:
                flat_indices = []
                for idx in matches[0]:
                    if isinstance(idx, int):
                        flat_indices.append(idx)
                        consumed.add(idx)
                
                if flat_indices:
                    extracted = extractor.process_match(tokens, flat_indices)
                    has_numbered_keys = any(re.match(rf'{base_name}_[a-z_]+\d+', key) for key in extracted)
                    if has_numbered_keys:
                        data.update(extracted)
                    else:
                        data.update(extracted)
    except Exception as e:
        import traceback
        print(f"Error in process_multi_instance_data: {e}")
        print(traceback.format_exc())
    
    return data

def add_device_type(data: Dict, logger: logging.Logger) -> Dict:
    """Add device type information as a post-processing step after parsing."""
    # Only perform this if device_type is not already set
    if "device_type" not in data:
        try:
            # Import the brand model types data
            try:
                from configs.brand_model_types import brand_model_types
                print(f"DEBUG: Loaded brand_model_types successfully. HP entry: {brand_model_types.get('HP', 'NOT FOUND')}")
                logger.debug("Loaded brand model types for device type detection")
            except ImportError:
                logger.warning("Could not load brand_model_types.py")
                brand_model_types = {}
                
            # Import Dell model sets for laptop and desktop detection
            try:
                from configs.dell_models import dell_laptop_models, dell_desktop_models
                logger.debug("Loaded Dell laptop and desktop model sets")
            except ImportError:
                logger.warning("Could not load dell_models.py")
                dell_laptop_models = set()
                dell_desktop_models = set()
                
            # Look through the full title for brand-model combinations
            full_title = data.get("Full Title", "").lower()
            
            # Check for audio equipment patterns FIRST (HIGHEST PRIORITY)
            audio_indicators = [
                "amplifier", "amp", "power amplifier", "audio amplifier", 
                "stereo amplifier", "integrated amplifier", "preamp", "preamplifier",
                "receiver", "audio receiver", "stereo receiver", "av receiver",
                "mixer", "audio mixer", "mixing console", "soundboard",
                "equalizer", "eq", "crossover", "compressor", "limiter"
            ]
            
            if any(indicator in full_title for indicator in audio_indicators):
                data["device_type"] = "Amplifiers"
                logger.debug("Set device_type to 'Amplifiers' from audio equipment pattern")
                return data
            
            # Check for toner cartridges and imaging units (HIGH PRIORITY)
            toner_cartridge_indicators = [
                "toner", "cartridge", "toner cartridge", "imaging unit", "imaging drum",
                "ink cartridge", "ink", "print cartridge", "printer cartridge",
                "drum unit", "drum cartridge", "developer unit", "fuser unit",
                "maintenance kit", "transfer unit", "waste toner", "photoconductor",
                "return program", "yield", "black cartridge", "color cartridge",
                "cyan cartridge", "magenta cartridge", "yellow cartridge"
            ]
            
            # Toner cartridge model patterns (specific patterns for toner/ink cartridge part numbers)
            toner_model_patterns = [
                r'\b[0-9]{2,3}[a-z][0-9][a-z][0-9]{2,3}\b',  # Pattern like 50F0Z00, 78C0K10
                r'\b[a-z]{1,2}[0-9]{3,4}[a-z]?\b',           # Pattern like CF410A, TN450
                r'\bhp\s*[0-9]{2,3}[a-z]?\b',                # HP specific like HP 85A
                r'\bce[0-9]{3}[a-z]?\b',                     # HP CE patterns
                r'\bcf[0-9]{3}[a-z]?\b',                     # HP CF patterns
                r'\btn[0-9]{3,4}\b',                         # Brother TN patterns
                r'\blc[0-9]{3,4}\b',                         # Brother LC patterns
                r'\bpgi[0-9]{3,4}\b',                        # Canon PGI patterns
                r'\bcli[0-9]{3,4}\b'                        # Canon CLI patterns
            ]
            
            # Printer/toner brands
            printer_brands = [
                "hp", "canon", "brother", "lexmark", "epson", "xerox", "dell", 
                "samsung", "kyocera", "ricoh", "sharp", "konica", "minolta",
                "oki", "okidata", "panasonic", "toshiba"
            ]
            
            # Check for toner cartridge keywords
            has_toner_keywords = any(re.search(r'\b' + re.escape(indicator) + r'\b', full_title) for indicator in toner_cartridge_indicators)
            
            # Check for toner cartridge model patterns
            has_toner_model_pattern = any(re.search(pattern, full_title) for pattern in toner_model_patterns)
            
            # Check for printer brand context
            has_printer_brand = any(re.search(r'\b' + re.escape(brand) + r'\b', full_title) for brand in printer_brands)
            
            # Exclude non-cartridge items
            exclusions = ["printer", "scanner", "copier", "fax", "multifunction", "all-in-one", "laptop", "desktop", "server"]
            
            # Add server-specific exclusions for Dell and other server models
            server_exclusions = ["poweredge", "proliant", "primergy", "system x", "thinkserver", "r420", "r620", "r720", "r820", "t420", "t620", "t720", "oemr", "xeon", "rack", "1u", "2u", "3u", "4u"]
            
            has_exclusions = any(exclusion in full_title for exclusion in exclusions) or any(exclusion in full_title.lower() for exclusion in server_exclusions)
            
            if ENABLE_TONER_DETECTION and (has_toner_keywords or has_toner_model_pattern) and has_printer_brand and not has_exclusions:
                data["device_type"] = "Toner Cartridges"
                logger.debug("Set device_type to 'Toner Cartridges' from toner cartridge pattern")
                return data
            
            # Check for power adapter patterns (HIGH PRIORITY)
            power_adapter_indicators = [
                "adapter", "adapters", "charger", "chargers", "power adapter", "power adapters", 
                "ac adapter", "ac adapters", "power supply"
            ]
            
            # Wattage patterns that indicate power adapters
            wattage_pattern = re.search(r'\b(\d+)w\b', full_title)
            
            # Check for power adapter keywords
            has_power_adapter_keywords = any(re.search(r'\b' + re.escape(indicator) + r'\b', full_title) for indicator in power_adapter_indicators)
            
            # Special case: exclude "network adapter", audio equipment, and similar terms
            networking_exclusions = ["network adapter", "network adapters", "wireless adapter", "wifi adapter", "ethernet adapter"]
            has_networking_exclusions = any(exclusion in full_title for exclusion in networking_exclusions)
            
            # Audio equipment exclusions
            audio_exclusions = ["power amplifier", "amplifier", "amp", "audio", "stereo", "speaker", "receiver"]
            has_audio_exclusions = any(exclusion in full_title for exclusion in audio_exclusions)
            
            if has_power_adapter_keywords and not has_networking_exclusions and not has_audio_exclusions:
                # Additional context checks to confirm it's a laptop power adapter
                laptop_power_context = [
                    wattage_pattern is not None,  # Has wattage like "90W"
                    any(word in full_title for word in ["laptop", "notebook", "dell", "hp", "lenovo", "apple", "asus", "acer"]),  # Known laptop brands
                    "ac" in full_title,  # AC adapter
                    "power" in full_title,  # Power-related
                    "no power cord" in full_title,  # Common description for adapter lots
                    "cord" in full_title,  # Power cord mentioned
                ]
                
                # If we have strong power adapter indicators and supporting context
                if sum(laptop_power_context) >= 2:
                    data["device_type"] = "Laptop Power Adapters/Chargers"
                    logger.debug("Set device_type to 'Laptop Power Adapters/Chargers' from power adapter pattern")
                    return data
            
            # Check for optical drive patterns FIRST (HIGH PRIORITY)
            optical_drive_patterns = [
                r'\b(dvd|cd|blu-ray|blu ray|bluray)\s+(drive|player|writer|burner|reader|rom)\b',
                r'\b(optical|disc)\s+(drive|player|writer|burner|reader)\b',
                r'\bdvd\+?/?-?rw?\b',
                r'\bcd-?rw?\b',
                r'\bbd-?r(e|om)?\b',
                r'\bslim\s+(dvd|cd|optical)\b',
                r'\busb\s+(dvd|cd|optical)\s+(drive|player)\b',
                r'\bexternal\s+(dvd|cd|optical)\s+(drive|player)\b',
                r'\binternal\s+(dvd|cd|optical)\s+(drive|player)\b'
            ]
            
            for pattern in optical_drive_patterns:
                if re.search(pattern, full_title):
                    data["device_type"] = "CD, DVD & Blu-ray Drives"
                    logger.debug("Set device_type to 'CD, DVD & Blu-ray Drives' from optical drive pattern")
                    return data
            
            # Check for processor patterns with specific form factors
            processor_pattern = re.search(r'(?:desktop|laptop|server|mobile)\s+(processor|processors|cpu|cpus)\b', full_title, re.IGNORECASE)
            if processor_pattern:
                data["device_type"] = "CPUs/Processors"
                logger.debug("Set device_type to 'CPUs/Processors' from specific processor pattern")
                return data
                
            # More general processor detection
            if ("cpu_model" in data or "cpu_family" in data) and any(term in full_title.lower() for term in ["processor", "processors", "cpu", "cpus"]):
                system_words = ["laptop", "notebook", "desktop", "server", "tower"]
                if not any(re.search(r'\b{}\s+(?!processor|processors|cpu|cpus)'.format(word), full_title, re.IGNORECASE) 
                          for word in system_words):
                    data["device_type"] = "CPUs/Processors"
                    logger.debug("Set device_type to 'CPUs/Processors' based on CPU attributes and context")
                    return data
            
            # Check for monitor keywords - FIXED: Use word boundaries instead of substring matching
            if any(re.search(r'\b' + re.escape(indicator) + r'\b', full_title) for indicator in ["monitor", "widescreen"]) and not any(term in full_title.lower() for term in ["laptop", "desktop", "server", "processor", "cpu"]):
                # BUSINESS RULE: 'Monitors' device type is deprecated; map to 'Computer Servers'
                data["device_type"] = "Computer Servers"
                logger.debug("Mapped deprecated 'Monitors' classification to 'Computer Servers' based on monitor keywords")
                return data
            
            # FIXED: More specific display detection that excludes "DisplayPort"
            # Only match "display" when it's clearly referring to a monitor/screen, not a port/connector
            display_monitor_patterns = [
                r'\b\d+(?:\.\d+)?\s*(?:inch|"|′)\s+display\b',  # "24 inch display"
                r'\bdisplay\s+(?:monitor|screen)\b',            # "display monitor"
                r'\b(?:led|lcd|oled)\s+display\b',              # "LED display"
                r'\bexternal\s+display\b',                      # "external display"
                r'\bdisplay\s+panel\b'                          # "display panel"
            ]
            
            if any(re.search(pattern, full_title, re.IGNORECASE) for pattern in display_monitor_patterns) and not any(term in full_title.lower() for term in ["laptop", "desktop", "server", "processor", "cpu"]):
                # BUSINESS RULE: 'Monitors' device type is deprecated; map to 'Computer Servers'
                data["device_type"] = "Computer Servers"
                logger.debug("Mapped deprecated 'Monitors' classification to 'Computer Servers' based on display patterns")
                return data
            
            # Inspiron Desktop override: if title clearly indicates Dell Inspiron desktop, force desktops
            try:
                tl = full_title.lower()
                if ("dell" in tl and "inspiron" in tl and (
                    "desktop" in tl or "tower" in tl or "all-in-one" in tl or "aio" in tl
                )):
                    data["device_type"] = "PC Desktops & All-In-Ones"
                    logger.debug("Set device_type to 'PC Desktops & All-In-Ones' from Dell Inspiron desktop override (title)")
                    return data
            except Exception:
                pass

            # ENHANCED: Check for Dell laptops and desktops using model numbers
            if "brand" in data and "model" in data and data["model"]:
                brand = data["brand"].lower()
                model_full = data["model"].lower()
                if brand == "dell":
                    # Extract just the numeric model number from the full model string
                    # For example, "Inspiron 3650 Desktop" -> "3650"
                    model_numbers = re.findall(r'\b(T?\d{4})\b', model_full, re.IGNORECASE)
                    
                    for model_num in model_numbers:
                        if model_num in dell_laptop_models:
                            data["device_type"] = "PC Laptops & Netbooks"
                            logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on Dell laptop model: {model_num}")
                            return data
                        elif model_num in dell_desktop_models:
                            data["device_type"] = "PC Desktops & All-In-Ones"
                            logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell desktop model: {model_num}")
                            return data
            
            # Thin client detection handled earlier in determine_device_type – no action needed here
            
            # If no specific device type is found, return the original data
            return data
        except Exception as e:
            logger.error(f"Error in add_device_type: {e}")
            return data
            
def find_token_indices(tokens, match):
    """Helper function to find token indices for regex matches"""
    start, end = match.span()
    char_pos = 0
    indices = []
    for i, token in enumerate(tokens):
        token_start = char_pos
        token_end = char_pos + len(token)
        if token_start < end and token_end > start:
            indices.append(i)
        char_pos += len(token) + 1  # +1 for space
    return indices

def detect_listing_context(sanitized_title: str, logger: logging.Logger) -> Dict[str, bool]:
    """Detect various contexts like GPU context, phone context, storage array context, etc."""
    context = {}
    
    # Simplified - only keep the contexts we actually need
    context['is_processor_listing'] = False  # We'll handle this in device type detection
    
    # PRIORITY 1: Check for thin client and server contexts first (HIGHEST PRIORITY)
    thin_client_indicators = [
        "thin client", "thin-client", "terminal client", "zero client", 
        # HP thin client models - be more specific to avoid GPU false positives
        "hp t430", "hp t530", "hp t630", "hp t640", "hp t730", "hp t740",
        "hp mt21", "hp mt22", "hp mt440", "hp mt645",
        # Also allow standalone model patterns but with more context
        r"\s+t430\s", r"\s+t530\s", r"\s+t630\s", r"\s+t640\s", r"\s+t730\s", r"\s+t740\s",
        r"\s+mt21\s", r"\s+mt22\s", r"\s+mt440\s", r"\s+mt645\s"
    ]
    
    server_indicators = [
        "blade", "rack", "poweredge", "proliant", "supermicro",
        "compute node", "blade server", "rack server", "tower server"
    ]
    
    title_lower = sanitized_title.lower()
    
    # Check for thin client context
    has_thin_client_context = False
    for indicator in thin_client_indicators:
        if indicator.startswith(r'\b'):  # Regex pattern
            if re.search(indicator, title_lower):
                has_thin_client_context = True
                break
        else:  # Simple string matching
            if indicator in title_lower:
                has_thin_client_context = True
                break
    
    # Check for server context  
    has_server_context = any(indicator in title_lower for indicator in server_indicators)
    
    # Check for accessory context (NEW)
    accessory_context_keywords = [
        "attachable keyboard", "keyboard for", "keyboard case", "keyboard cover", 
        "keyboard folio", "case for", "cover for", "folio for", "skin for",
        "protector for", "stand for", "dock for", "charger for", "adapter for"
    ]
    
    has_accessory_context = any(re.search(r'\b' + re.escape(keyword) + r'\b', title_lower) 
                               for keyword in accessory_context_keywords)
    context['has_accessory_context'] = has_accessory_context
    
    # FIXED: More specific laptop/desktop context detection
    laptop_indicators = ["laptop", "notebook", "latitude", "inspiron", "thinkpad", "toughpad", "macbook", "elitebook", "probook", "chromebook", "zbook", "mobile workstation"]
    desktop_indicators = ["desktop", "optiplex", "vostro", "workstation", "all-in-one", "aio"]
    
    # NEW: Check for laptop/desktop accessories first to exclude them
    # Broadened patterns: allow arbitrary words (e.g. "USB-C") between the laptop/notebook keyword and the
    # accessory keyword, and vice-versa.  This ensures we detect titles like:
    #   "65W Laptop USB-C Chargers"  or  "Charger 65 W for Dell Laptop".
    laptop_accessory_patterns = [
        # laptop/notebook followed anywhere later by an accessory keyword
        r'\blaptop\b.*\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|case|bag|stand|dock|mount|video\s+card|graphics\s+card|gpu)\b',
        r'\bnotebook\b.*\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|case|bag|stand|dock|mount|video\s+card|graphics\s+card|gpu)\b',
        # accessory keyword followed anywhere later by laptop/notebook
        r'\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|case|bag|stand|dock|mount|video\s+card|graphics\s+card|gpu)\b.*\b(?:laptop|notebook)\b',
        # existing simple patterns for backward compatibility
        r'(?:charger|adapter|power|ac|cord|cable|supply|case|bag|stand|dock|mount)\s+(?:for\s+)?(?:laptop|notebook)',
        r'\blaptop\s+(?:charger|adapter|power|ac|cord|cable|supply|case|bag|stand|dock|mount|video\s+card|graphics\s+card|gpu)',
        r'\bnotebook\s+(?:charger|adapter|power|ac|cord|cable|supply|case|bag|stand|dock|mount|video\s+card|graphics\s+card|gpu)',
        r'(?:charger|adapter|power|ac|cord|cable|supply)\s+adapters?'
    ]
    
    desktop_accessory_patterns = [
        r'\bdesktop\s+(?:case|stand|mount|bracket)',
        r'(?:case|stand|mount|bracket)\s+(?:for\s+)?desktop'
    ]
    
    # Check if this is a laptop/desktop accessory
    is_laptop_accessory = any(re.search(pattern, title_lower) for pattern in laptop_accessory_patterns)
    is_desktop_accessory = any(re.search(pattern, title_lower) for pattern in desktop_accessory_patterns)
    
    # ENHANCED: Handle Dell Precision specifically
    dell_precision_desktop_indicators = [
        "precision tower", "precision workstation", "precision desktop",
        "precision t3", "precision t5", "precision t7",  # T-series are always desktops
        "precision 38", "precision 58", "precision 78", "precision 79",  # 4-digit desktop models
        "precision 5810", "precision 5820", "precision 7810", "precision 7820", "precision 7910", "precision 7920"
    ]
    
    dell_precision_laptop_indicators = [
        "precision mobile", "precision laptop", "precision notebook",
        "precision 35", "precision 55", "precision 75",  # Mobile precision series
        "precision m"  # M-series mobile workstations
    ]
    
    # Check for laptop/desktop context but exclude accessories and CPU component patterns
    has_laptop_context = False
    has_desktop_context = False
    
    if not is_laptop_accessory:
        has_laptop_context = any(indicator in title_lower for indicator in laptop_indicators)
    
    if not is_desktop_accessory:
        has_desktop_context = any(indicator in title_lower for indicator in desktop_indicators)
    
    # ENHANCED: Dell Precision specific handling
    if "dell" in title_lower and "precision" in title_lower and not is_laptop_accessory and not is_desktop_accessory:
        if any(indicator in title_lower for indicator in dell_precision_desktop_indicators):
            has_desktop_context = True
            has_laptop_context = False
            logger.debug("Dell Precision detected as desktop from specific indicators")
        elif any(indicator in title_lower for indicator in dell_precision_laptop_indicators):
            has_laptop_context = True
            has_desktop_context = False
            logger.debug("Dell Precision detected as laptop from specific indicators")
        elif "tower" in title_lower or "sff" in title_lower or "desktop" in title_lower:
            has_desktop_context = True
            has_laptop_context = False
            logger.debug("Dell Precision detected as desktop from form factor indicators")
        else:
            # Check for specific Dell Precision desktop model numbers
            dell_precision_desktop_models = [
                "3420", "3430", "3431", "3440", "3460", "3630", "3631", "3640", "3650", "3660", "3680", 
                "5810", "5820", "5860", "7820", "7875", "7920", "7960", "3450"
            ]
            
            # Extract model numbers from title to check against desktop models
            precision_model_match = re.search(r'precision\s+(\d{4})', title_lower)
            if precision_model_match:
                model_number = precision_model_match.group(1)
                if model_number in dell_precision_desktop_models:
                    has_desktop_context = True
                    has_laptop_context = False
                    logger.debug(f"Dell Precision detected as desktop from specific model number: {model_number}")
                else:
                    # Default Precision models without explicit desktop indicators should be laptops
                    has_laptop_context = True
                    logger.debug("Dell Precision detected as laptop (default for mobile workstations)")
            else:
                # Default Precision models without explicit desktop indicators should be laptops
                has_laptop_context = True
                logger.debug("Dell Precision detected as laptop (default for mobile workstations)")
        # If no specific indicators, fall back to model number checking later
    
    # ENHANCED: Dell XPS specific handling  
    if "dell" in title_lower and "xps" in title_lower and not is_laptop_accessory and not is_desktop_accessory:
        # XPS models with screen sizes (13, 15, 17) are laptops
        xps_laptop_patterns = [r'xps\s+1[357]', r'xps\s+1[357]\s+\d{4}']
        if any(re.search(pattern, title_lower) for pattern in xps_laptop_patterns):
            has_laptop_context = True
            has_desktop_context = False
            logger.debug("Dell XPS detected as laptop from screen size indicators")
    
    # OVERRIDE: Enhanced CPU component pattern detection
    cpu_component_patterns = [
        r'\bdesktop\s+cpu\b',                    # "Desktop CPU"
        r'\blaptop\s+cpu\b',                     # "Laptop CPU" 
        r'\bmobile\s+cpu\b',                     # "Mobile CPU"
        r'\bserver\s+cpu\b',                     # "Server CPU"
        r'\bdesktop\s+processor\b',              # "Desktop Processor"
        r'\bdesktop\s+processors\b',             # "Desktop Processors" (plural)
        r'\blaptop\s+processor\b',               # "Laptop Processor"
        r'\blaptop\s+processors\b',              # "Laptop Processors" (plural)
        r'\bmobile\s+processor\b',               # "Mobile Processor"
        r'\bmobile\s+processors\b',              # "Mobile Processors" (plural)
        r'\bserver\s+processor\b',               # "Server Processor"
        r'\bserver\s+processors\b',              # "Server Processors" (plural)
        r'\bdesktop\s+lga\d+\s+cpu\b',          # "Desktop LGA1155 CPU"
        r'\bdesktop\s+lga\d+\s+processor\b',    # "Desktop LGA1155 Processor"
        r'\blga\s*\d+\s+.*?\bdesktop\s+processor\b', # "LGA1151 ... Desktop Processor"
        r'\blga\s*\d+\s+.*?\bdesktop\s+cpu\b',       # "LGA1151 ... Desktop CPU"
        r'\bdesktop\s+.*?\bprocessor\s+cpu\b',       # "Desktop ... Processor CPU"
        r'\bprocessor\s+cpu\b',                      # "Processor CPU" (generic CPU component)
        r'\bcpu\s+processor\b',                      # "CPU Processor" (generic CPU component)
        r'\b\d+\s*-?\s*core\s+desktop\s+processors?\b', # "4-Core Desktop Processors"
        r'\bdesktop\s+processors?\s+lga\d+\b',       # "Desktop Processors LGA1155"
    ]
    
    if any(re.search(pattern, title_lower) for pattern in cpu_component_patterns):
        has_laptop_context = False
        has_desktop_context = False
        logger.debug("Override: Found CPU component pattern - disabling laptop/desktop context")
    
    # Enhanced graphics card/GPU context detection with CPU context awareness
    gpu_context_keywords = [
        "graphics", "video", "gpu", "nvidia", "amd", "geforce", "radeon", "quadro", 
        "tesla", "arc", "iris", "gtx", "rtx", "rx", "hd graphics", "uhd graphics", 
        "iris xe", "video card", "graphics card", "gpu card", "vga", "display adapter",
        "cuda", "purevideo", "directx", "opengl", "gddr", "vram", "ddr3", "ddr5", "ddr6",
        # Graphics card series and models
        "geforce 8400", "geforce 8800", "geforce 9", "geforce gt", "geforce gts",
        "radeon hd", "radeon r5", "radeon r7", "radeon r9", "radeon rx",
        # Graphics card manufacturers
        "evga", "msi", "asus", "gigabyte", "zotac", "sapphire", "xfx", "powercolor",
        "pny", "asrock", "visiontek", "gainward", "palit", "inno3d", "colorful", 
        "kfa2", "galax", "yeston", "biostar",
        # Graphics card model series
        "strix", "gaming", "windforce", "nitro", "pulse", "red devil", "amp",
        "ftw", "classified", "kingpin", "aorus", "eagle", "vision", "phantom gaming",
        # ADDED: K-series mobile Quadro models
        "k1100m", "k2100m", "k3100m", "k4100m", "k5100m", "k610m", "k510m",
        "quadro k1100m", "quadro k2100m", "quadro k3100m", "quadro k4100m", "quadro k5100m"
    ]
    
    # CPU context indicators that override GPU context for Intel/AMD
    cpu_context_keywords = [
        "processor", "processors", "cpu", "cpus", "socket", "lga", "am4", "tr4", "pga", 
        "l3 cache", "l2 cache", "core i3", "core i5", "core i7", "core i9", 
        "ryzen 3", "ryzen 5", "ryzen 7", "ryzen 9", "xeon", "threadripper", 
        "fx-", "athlon", "pentium", "celeron", "epyc", "sr32w", "sr", "stepping"
    ]
    
    negative_gpu_patterns = [r'\bno\s+gpu\b', r'\bwithout\s+gpu\b', r'\bmissing\s+gpu\b', r'\bno\s+graphics\b']
    has_negative_gpu = any(re.search(pattern, title_lower) for pattern in negative_gpu_patterns)
    
    # Check for CPU context first
    has_cpu_context = any(keyword in title_lower for keyword in cpu_context_keywords)
    
    # Check for GPU context
    has_gpu_keywords = any(keyword in title_lower for keyword in gpu_context_keywords)
    
    # PRIORITY OVERRIDE: Thin client or server context overrides GPU context
    if has_thin_client_context or has_server_context:
        has_gpu_context = False
        logger.debug(f"Thin client or server context detected - overriding GPU context detection")
    # PRIORITY OVERRIDE: If this is clearly a laptop/desktop system, still detect GPU but mark as system context
    elif has_laptop_context or has_desktop_context:
        # For laptops/desktops, we want to detect GPU but prioritize system context
        specific_gpu_models = ["k1100m", "k2100m", "k3100m", "k4100m", "k5100m", "k610m", "k510m", "gtx", "rtx", "quadro"]
        has_specific_gpu = any(model in title_lower for model in specific_gpu_models)
        
        if has_specific_gpu or has_gpu_keywords:
            has_gpu_context = True
            context['is_system_with_gpu'] = True  # Mark as system with GPU, not standalone GPU
            logger.debug(f"Laptop/Desktop with GPU detected - enabling GPU extraction but maintaining system context")
        else:
            has_gpu_context = has_gpu_keywords and not has_negative_gpu
            context['is_system_with_gpu'] = False
    # For Intel and AMD, CPU context overrides GPU context UNLESS we have specific GPU models
    elif (("intel" in title_lower or "amd" in title_lower) and has_cpu_context):
        # Check for specific GPU model indicators that should override CPU context
        specific_gpu_models = ["k1100m", "k2100m", "k3100m", "k4100m", "k5100m", "k610m", "k510m", "gtx", "rtx", "quadro"]
        has_specific_gpu = any(model in title_lower for model in specific_gpu_models)
        
        if has_specific_gpu:
            has_gpu_context = True
            context['is_system_with_gpu'] = True  # Mark as system with GPU
            logger.debug(f"Specific GPU model detected - overriding CPU context detection")
        else:
            has_gpu_context = False
            context['is_system_with_gpu'] = False
            logger.debug(f"Intel/AMD CPU context detected - overriding GPU context detection")
    else:
        # Normal GPU context detection
        has_gpu_context = has_gpu_keywords and not has_negative_gpu
        context['is_system_with_gpu'] = False
    
    context['has_gpu_context'] = has_gpu_context
    context['has_laptop_context'] = has_laptop_context
    context['has_desktop_context'] = has_desktop_context
    context['has_thin_client_context'] = has_thin_client_context
    context['has_server_context'] = has_server_context

    # Check for phone context
    phone_context_keywords = ["iphone", "ipad", "galaxy", "pixel", "oneplus", "nord"]
    has_phone_context = any(keyword in title_lower for keyword in phone_context_keywords)
    context['has_phone_context'] = has_phone_context

    # ENHANCED: Check for parts context with smarter missing/no/without detection
    parts_context_keywords = ["housing", "charger", "cable", "case", "cover", "lens", "camera", "speaker", "microphone", "button", "flex", "ribbon", "digitizer", "lcd", "oled", "motherboard", "logic board", "charging port", "headphone jack", "sim tray", "back glass", "front glass", "assembly", "replacement", "repair", "aftermarket", "parts", "part", "component", "components"]
    
    # ENHANCED: Check for missing/no/without descriptors that indicate the item is NOT a part
    missing_descriptors = [
        r'\bmissing\s+\w+\s+cover\b',      # "missing ram cover", "missing battery cover"
        r'\bno\s+\w+\s+cover\b',           # "no ram cover", "no battery cover" 
        r'\bwithout\s+\w+\s+cover\b',      # "without ram cover", "without battery cover"
        r'\bmissing\s+cover\b',            # "missing cover"
        r'\bno\s+cover\b',                 # "no cover"
        r'\bwithout\s+cover\b',            # "without cover"
        r'\bmissing\s+\w+\s+cable\b',      # "missing power cable"
        r'\bno\s+\w+\s+cable\b',           # "no power cable"
        r'\bwithout\s+\w+\s+cable\b',      # "without power cable"
        r'\bmissing\s+\w+\s+adapter\b',    # "missing power adapter"
        r'\bno\s+\w+\s+adapter\b',         # "no power adapter"
        r'\bwithout\s+\w+\s+adapter\b',    # "without power adapter"
        r'\bno\s+chargers?\b',             # "no charger" or "no chargers"
        r'\bmissing\s+chargers?\b',        # "missing charger" or "missing chargers"
        r'\bmissing\s+adapters?\b',        # "missing adapter" or "missing adapters" 
        r'\bmissing\s+cables?\b',          # "missing cable" or "missing cables"
        r'\bmissing\s+cords?\b',           # "missing cord" or "missing cords"
        r'\bmissing\s+power\s+cords?\b',   # "missing power cord" or "missing power cords"
        r'\bmissing\s+ac\s+adapters?\b',   # "missing ac adapter" or "missing ac adapters"
        r'\bmissing\s+power\s+adapters?\b',# "missing power adapter" or "missing power adapters"
        r'\bno\s+hdds?\b',                            # "no hdd" or "no hdds"
        r'\bno\s+ssds?\b',                            # "no ssd" or "no ssds" 
        r'\bmissing\s+parts\b',                       # "missing parts"
        r'\bmissing\s+components?\b',                 # "missing component" or "missing components"        
        
        # ADD THESE LINES FOR DAMAGED/BROKEN COMPONENTS:
        r'\bdamaged\s+case\b',             # "damaged case"
        r'\bdamaged\s+\w+\s+case\b',       # "damaged ram case", etc.
        r'\bbroken\s+case\b',              # "broken case"
        r'\bcracked\s+case\b',             # "cracked case"
        r'\bdefective\s+case\b',           # "defective case"
        r'\bbent\s+case\b',                # "bent case"
        r'\bdented\s+case\b',    
        
        # SPECIFIC PATTERNS for common missing component formats
        r'\bno\s+battery/charger/hard\s*drive\b',     # "no battery/charger/hard drive" or "no battery/charger/harddrive"
        r'\bno\s+battery/charger/hdd\b',              # "no battery/charger/hdd"
        r'\bno\s+battery/charger/ssd\b',              # "no battery/charger/ssd"
        r'\bno\s+charger/battery/hard\s*drive\b',     # "no charger/battery/hard drive"
        r'\bno\s+charger/battery/hdd\b',              # "no charger/battery/hdd"
        r'\bno\s+battery/charger\b',                  # "no battery/charger"
        r'\bno\s+charger/battery\b',                  # "no charger/battery"
        
        # NEW: Add patterns for the specific case we're seeing
        r'\bno\s+battery\s*/\s*charger\s*/\s*ssd\b',  # "no battery / charger/ ssd"
        r'\bno\s+battery\s*/\s*charger\s*/\s*hdd\b',  # "no battery / charger/ hdd"
        r'\bno\s+battery\s*/\s*charger\s*/\s*hard\s*drive\b', # "no battery / charger/ hard drive"
        
        r'\bmissing\s+battery/charger/hard\s*drive\b', # "missing battery/charger/hard drive"
        r'\bmissing\s+battery/charger/hdd\b',          # "missing battery/charger/hdd"
        r'\bmissing\s+battery/charger/ssd\b',          # "missing battery/charger/ssd"
        r'\bmissing\s+charger/battery/hard\s*drive\b', # "missing charger/battery/hard drive"
        r'\bmissing\s+charger/battery/hdd\b',          # "missing charger/battery/hdd"
        r'\bmissing\s+battery/charger\b',              # "missing battery/charger"
        r'\bmissing\s+charger/battery\b',              # "missing charger/battery"
        
        r'\bwithout\s+battery/charger/hard\s*drive\b', # "without battery/charger/hard drive"
        r'\bwithout\s+battery/charger/hdd\b',          # "without battery/charger/hdd"
        r'\bwithout\s+battery/charger/ssd\b',          # "without battery/charger/ssd"
        r'\bwithout\s+charger/battery/hard\s*drive\b', # "without charger/battery/hard drive"
        r'\bwithout\s+charger/battery/hdd\b',          # "without charger/battery/hdd"
        r'\bwithout\s+battery/charger\b',              # "without battery/charger"
        r'\bwithout\s+charger/battery\b',              # "without charger/battery"
        
        # ADDITIONAL PATTERNS for more variations
        r'\bno\s+os/hdd\b',                           # "no os/hdd"
        r'\bno\s+hdd/os\b',                           # "no hdd/os"
        r'\bno\s+os/hard\s*drive\b',                  # "no os/hard drive"
        r'\bno\s+hard\s*drive/os\b',                  # "no hard drive/os"
        r'\bno\s+power\s+cord\b',                     # "no power cord"
        r'\bno\s+ac\s+adapter\b',                     # "no ac adapter"
        r'\bno\s+power\s+adapter\b',                  # "no power adapter"
        
        # GENERIC PATTERNS for slash-separated lists with more flexible spacing
        r'\bno\s+(?:[a-z]+\s*/\s*){1,5}[a-z]*(?:drive|battery|charger|adapter|cable|cord|hdd|ssd|os)s?\b',
        r'\bmissing\s+(?:[a-z]+\s*/\s*){1,5}[a-z]*(?:drive|battery|charger|adapter|cable|cord|hdd|ssd|os)s?\b',
        r'\bwithout\s+(?:[a-z]+\s*/\s*){1,5}[a-z]*(?:drive|battery|charger|adapter|cable|cord|hdd|ssd|os)s?\b',
    ]
    
    # Check if any missing descriptors are present
    has_missing_descriptors = any(re.search(pattern, title_lower, re.IGNORECASE) for pattern in missing_descriptors)
    
    # Only consider it parts context if we have parts keywords AND no missing descriptors
    initial_parts_context = any(keyword in title_lower for keyword in parts_context_keywords)
    has_parts_context = initial_parts_context and not has_missing_descriptors
    
    # Log the decision for debugging
    if initial_parts_context and has_missing_descriptors:
        logger.debug(f"Parts keywords found but overridden by missing descriptors - not setting parts context")
        logger.debug(f"Title: '{sanitized_title}'")
        # Find which pattern matched
        for pattern in missing_descriptors:
            if re.search(pattern, title_lower, re.IGNORECASE):
                logger.debug(f"Matched missing descriptor pattern: {pattern}")
                break
    elif initial_parts_context:
        logger.debug(f"Parts context detected without missing descriptors override")
    
    context['has_parts_context'] = has_parts_context

    # Enhanced storage array context detection
    storage_array_keywords = [
        "storage array", "disk array", "san", "nas", "disk shelf", "storage system", 
        "storage appliance", "array controller", "disk enclosure", "storage enclosure",
        "fc array", "iscsi array", "sas array", "fibre channel array", "network storage",
        "storage chassis", "drive enclosure", "jbod", "raid array", "storage unit"
    ]
    storage_array_brands = [
        "netapp", "emc", "equallogic", "3par", "compellent", "dell compellent", 
        "pure storage", "hitachi", "hds", "quantum", "synology", "qnap", "drobo",
        "promise", "infortrend", "nexsan", "overland", "tandberg", "xyratex"
    ]
    storage_array_models = [
        "fas", "aff", "naj", "ds4", "ds2", "e-series", "ef", "vnx", "vmax", "unity",
        "powervault", "msa", "eva", "lefthand", "nimble", "primera", "flasharray",
        "flashblade", "vsp", "hus", "ams", "diskstation", "rackstation", "vtrak"
    ]
    
    # Use word-boundary matching for keywords to avoid false positives like 'san' in 'samsung'
    has_storage_array_keyword = any(
        re.search(r'\b' + re.escape(keyword) + r'\b', title_lower)
        for keyword in storage_array_keywords
    )
    has_storage_array_brand = any(
        re.search(r'\b' + re.escape(brand) + r'\b', title_lower)
        for brand in storage_array_brands
    )
    has_storage_array_model = any(
        re.search(r'\b' + re.escape(model) + r'\b', title_lower)
        for model in storage_array_models
    )

    has_storage_array_context = (
        has_storage_array_keyword or
        has_storage_array_brand or
        has_storage_array_model or
        # Check for specific NetApp model patterns
        re.search(r'\bds\d{4}\b', title_lower) or  # DS2246, DS4246, etc.
        re.search(r'\bnaj-\d+\b', title_lower) or  # NAJ-1501, etc.
        re.search(r'\bfas\d+\b', title_lower) or   # FAS2750, etc.
        re.search(r'\baff\d+\b', title_lower)      # AFF220, etc.
    )
    context['has_storage_array_context'] = has_storage_array_context
    
    logger.debug(f"Laptop context detected: {has_laptop_context}")
    logger.debug(f"Desktop context detected: {has_desktop_context}")
    logger.debug(f"Thin client context detected: {has_thin_client_context}")
    logger.debug(f"Server context detected: {has_server_context}")
    logger.debug(f"Graphics card context detected: {has_gpu_context}")
    logger.debug(f"CPU context detected: {has_cpu_context}")
    logger.debug(f"Parts context detected: {has_parts_context}")
    logger.debug(f"Storage array context detected: {has_storage_array_context}")
    logger.debug(f"Accessory context detected: {has_accessory_context}")
    logger.debug(f"System with GPU: {context.get('is_system_with_gpu', False)}")
    
    return context
    
def has_multiple_components(title: str) -> bool:
    """Check if the title mentions both CPU and RAM/memory."""
    title_lower = title.lower()
    has_cpu = bool(re.search(r'\b(cpu|processor)\b', title_lower))
    has_ram = bool(re.search(r'\b(ram|memory)\b', title_lower))
    return has_cpu and has_ram

def infer_device_type_from_title(title: str) -> str:
    """Infer device type based on keywords in the title."""
    title_lower = title.lower()
    if any(keyword in title_lower for keyword in ["system x", "poweredge", "proliant", "rack"]):
        return "Computer Servers"
    elif any(keyword in title_lower for keyword in ["laptop", "notebook", "ultrabook"]):
        return "PC Laptops & Netbooks"
    elif any(keyword in title_lower for keyword in ["desktop", "workstation", "all-in-one", "aio", "optiplex", "elitedesk", "prodesk"]):
        return "PC Desktops & All-In-Ones"
    return "Unknown"

def _detect_toner_cartridges(title_lower: str, brand: str, logger: logging.Logger) -> str:
    """Detect toner cartridges with highest priority."""
    toner_cartridge_indicators = [
        "toner", "cartridge", "toner cartridge", "imaging unit", "imaging drum",
        "ink cartridge", "ink", "print cartridge", "printer cartridge",
        "drum unit", "drum cartridge", "developer unit", "fuser unit",
        "maintenance kit", "transfer unit", "waste toner", "photoconductor",
        "return program", "yield", "black cartridge", "color cartridge",
        "cyan cartridge", "magenta cartridge", "yellow cartridge"
    ]
    
    # Toner cartridge model patterns
    toner_model_patterns = [
        r'\b[0-9]{2,3}[a-z][0-9][a-z][0-9]{2,3}\b',  # Pattern like 50F0Z00, 78C0K10
        r'\bcf[0-9]{3}[a-z]?\b',                     # HP CF patterns like CF410A
        r'\bce[0-9]{3}[a-z]?\b',                     # HP CE patterns
        r'\btn[0-9]{3,4}\b',                         # Brother TN patterns
        r'\blc[0-9]{3,4}\b',                         # Brother LC patterns
        r'\bpgi[0-9]{3,4}\b',                        # Canon PGI patterns
        r'\bcli[0-9]{3,4}\b',                        # Canon CLI patterns
        r'\bhp\s*[0-9]{2,3}[a-z]?\b',                # HP specific like HP 85A
        r'\b[0-9]{3,4}[a-z]{1,2}(?![0-9])\b'         # General pattern like 410A, 78CK
    ]
    
    # CPU exclusion patterns
    cpu_exclusion_patterns = [
        r'\be[3579]-\d+\b',      # Intel Xeon E3/E5/E7/E9 patterns
        r'\bi[3579]-\d+\b',      # Intel Core i3/i5/i7/i9 patterns  
        r'\bxeon\b', r'\bcore\b', r'\bryzen\b', r'\bathlon\b', r'\bfx-\d+\b'
    ]
    
    # System exclusions
    system_exclusions = [
        "workstation", "desktop", "laptop", "notebook", "server", "computer", 
        "tower", "mini pc", "all-in-one", "motherboard", "processor", "cpu",
        "ram", "memory", "hdd", "ssd", "graphics", "gpu", "psu", "power supply"
    ]
    
    # Printer brands
    printer_brands = [
        "hp", "canon", "brother", "lexmark", "epson", "xerox", "dell", 
        "samsung", "kyocera", "ricoh", "sharp", "konica", "minolta",
        "oki", "okidata", "panasonic", "toshiba"
    ]
    
    # Check conditions
    has_toner_keywords = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in toner_cartridge_indicators)
    has_toner_model_pattern = any(re.search(pattern, title_lower) for pattern in toner_model_patterns)
    has_cpu_exclusions = any(re.search(pattern, title_lower) for pattern in cpu_exclusion_patterns)
    has_printer_brand = any(re.search(r'\b' + re.escape(brand_name) + r'\b', title_lower) for brand_name in printer_brands)
    has_system_exclusions = any(exclusion in title_lower for exclusion in system_exclusions)
    
    if (has_toner_keywords and 
        has_toner_model_pattern and
        brand.lower() == "lexmark" and 
        has_printer_brand and 
        not has_cpu_exclusions and 
        not has_system_exclusions):
        logger.debug("Set device_type to 'Toner Cartridges' from toner cartridge pattern")
        return "Toner Cartridges"
    
    return None

def override_category_for_device_type(category_info: Dict, device_type: str, brand: str, logger: logging.Logger) -> Dict:
    """Override category based on device type detection, especially for brand-specific requirements."""
    if not category_info or not device_type:
        return category_info
    
    # Apple phone parts override removed - they will now stay in their natural category
    
    return category_info
    
def _detect_parts_context(context: Dict, title_lower: str, brand_data: Dict, logger: logging.Logger) -> str:
    """Detect parts context with high priority."""
    if not context.get('has_parts_context', False):
        return None
        
    logger.debug("Parts context detected - checking for specific part types")

    # SAFETY GUARDS to avoid misclassifying whole systems as components
    # If there is clear laptop/desktop context, do not treat as components
    if context.get('has_laptop_context', False) or context.get('has_desktop_context', False):
        logger.debug("Skipping component classification due to laptop/desktop context")
        return None
    # If both CPU and RAM/memory appear, strongly indicates a complete system
    if re.search(r'\b(cpu|processor)\b', title_lower) and re.search(r'\b(ram|memory)\b', title_lower):
        logger.debug("Skipping component classification due to CPU+RAM indicators")
        return None
    
    # Cell phone parts
    if context.get('has_phone_context', False) or any(phone_term in title_lower for phone_term in ["iphone", "galaxy", "pixel", "android"]):
        logger.debug("Set device_type to 'Cell Phone & Smartphone Parts' based on phone parts context")
        return "Cell Phone & Smartphone Parts"

    # Laptop screen / LCD panel parts (specialized detection before generic computer parts)
    laptop_screen_keywords = [
        "screen", "lcd", "display", "panel", "digitizer", "lcd panel", "screen assembly", "display assembly"
    ]
    laptop_identifiers = [
        "laptop", "notebook", "macbook", "chromebook", "thinkpad", "latitude", "inspiron", "elitebook", "probook", "zenbook", "vivobook", "surface book", "surface laptop"
    ]

    has_screen_indicator = any(keyword in title_lower for keyword in laptop_screen_keywords)
    has_laptop_identifier = (
        any(identifier in title_lower for identifier in laptop_identifiers)
        # Additional check: Apple brand + MacBook context (covers cases like "For MacBook Pro A2141")
        or (
            brand_data.get("brand")
            and brand_data["brand"].lower() == "apple"
            and "macbook" in title_lower
        )
    )

    if has_screen_indicator and has_laptop_identifier:
        logger.debug("Set device_type to 'Laptop Screens & LCD Panels' based on laptop screen replacement context")
        return "Laptop Screens & LCD Panels"

    # Miscellaneous laptop replacement parts (keys, feet, hinges, etc.)
    laptop_misc_parts_keywords = [
        "key", "keys", "keycap", "keycaps", "foot", "feet", "hinge", "hinges", "palmrest", "palm rest", "bottom case", "touchpad", "trackpad button"
    ]
    if (
        (context.get('has_laptop_context', False) or (brand_data.get("brand") and brand_data["brand"].lower() == "apple" and "macbook" in title_lower))
        and any(keyword in title_lower for keyword in laptop_misc_parts_keywords)
    ):
        logger.debug("Set device_type to 'Other Laptop Replacement Parts' based on miscellaneous laptop component indicators")
        return "Other Laptop Replacement Parts"

    # Computer parts
    computer_part_indicators = [
        "motherboard", "logic board", "cpu", "processor", "ram", "memory", "hard drive", 
        "ssd", "graphics card", "gpu", "power supply", "fan", "heatsink", "keyboard", 
        "trackpad", "touchpad", "webcam", "wifi card", "bluetooth", "battery"
    ]
    if any(indicator in title_lower for indicator in computer_part_indicators):
        logger.debug("Set device_type to 'Computer Components & Parts' based on computer parts context")
        return "Computer Components & Parts"
    
    return None
    
def _detect_audio_equipment(title_lower: str, logger: logging.Logger) -> str:
    """Detect audio equipment with highest priority."""
    audio_indicators = [
        "amplifier", "amp", "power amplifier", "audio amplifier", 
        "stereo amplifier", "integrated amplifier", "preamp", "preamplifier",
        "receiver", "audio receiver", "stereo receiver", "av receiver",
        "mixer", "audio mixer", "mixing console", "soundboard",
        "equalizer", "eq", "crossover", "compressor", "limiter"
    ]
    
    if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in audio_indicators):
        logger.debug("Set device_type to 'Amplifiers' from audio equipment detection")
        return "Amplifiers"
    
    return None

def _detect_dell_device_type(brand_data: Dict, logger: logging.Logger) -> str:
    """Detect Dell device type using model numbers with keyword fallback."""
    brand = brand_data.get("brand")
    if not (brand and brand.lower() == "dell" and "model" in brand_data and brand_data["model"]):
        return None
        
    logger.debug("Checking Dell model numbers for device type detection")
    try:
        from configs.dell_models import dell_laptop_models, dell_desktop_models, dell_2in1_models
        
        model_full = brand_data["model"].lower()
        
        # PRIORITY 1: Check for explicit Dell Precision Tower models (HIGHEST PRIORITY)
        precision_tower_patterns = [
            r'\bprecision\s+tower\b',
            r'\btower\s+\d{4}\b',  # Tower 3430, Tower 7820, etc.
        ]
        
        if any(re.search(pattern, model_full) for pattern in precision_tower_patterns):
            logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell Precision Tower pattern")
            return "PC Desktops & All-In-Ones"
        
        # PRIORITY 2: Check for explicit desktop indicators in model
        explicit_desktop_indicators = [
            r'\bsff\s+desktop\b',      # "SFF Desktop"
            r'\bmini\s+tower\b',       # "Mini Tower"
            r'\bmicro\s+tower\b',      # "Micro Tower"
            r'\bmt\b',                 # "MT" (Mini Tower)
            r'\bsff\b',                # "SFF" (Small Form Factor)
            r'\bdesktop\b',            # Explicit "Desktop"
        ]
        
        if any(re.search(pattern, model_full) for pattern in explicit_desktop_indicators):
            logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on explicit desktop indicators")
            return "PC Desktops & All-In-Ones"
        
        # PRIORITY 3: Check for Dell Precision workstation models (specific model numbers that are desktops)
        precision_desktop_models = [
            r'\bprecision\s+38\d{2}\b',    # Precision 3820, 3840, etc.
            r'\bprecision\s+58\d{2}\b',    # Precision 5810, 5820, etc.
            r'\bprecision\s+78\d{2}\b',    # Precision 7810, 7820, etc.
            r'\bprecision\s+79\d{2}\b',    # Precision 7910, 7920, etc.
            r'\bprecision\s+\d{4}\b',      # General 4-digit Precision models (most are desktops)
        ]
        
        # Check if it's a Precision model but NOT a mobile one
        is_precision = "precision" in model_full
        is_precision_desktop = any(re.search(pattern, model_full) for pattern in precision_desktop_models)
        is_mobile_precision = any(term in model_full for term in ["mobile", "laptop", "notebook"])
        
        if is_precision and is_precision_desktop and not is_mobile_precision:
            logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell Precision desktop model pattern")
            return "PC Desktops & All-In-Ones"
        
        # Extract all 4-digit model numbers from the full model string
        # REMOVED: import re  (this was causing the scoping issue)
        model_numbers = re.findall(r'\b\d{4}\b', model_full)
        
        # Define desktop and laptop keywords for fallback detection
        desktop_keywords = [
            "desktop", "sff", "tower", "workstation", "mini tower", "micro tower", 
            "all-in-one", "aio", "small form factor", "mini pc", "micro pc",
            "rack", "rackmount", "1u", "2u", "3u", "4u"
        ]
        laptop_keywords = [
            "laptop", "notebook", "mobile", "portable", "ultrabook", "2-in-1", 
            "convertible", "detachable"
        ]
        
        # Check for explicit keywords first for strong indicators
        has_desktop_keywords = any(keyword in model_full for keyword in desktop_keywords)
        has_laptop_keywords = any(keyword in model_full for keyword in laptop_keywords)

        # Immediate override: known Dell 2-in-1 models should be tablets
        if any(m in dell_2in1_models for m in model_numbers):
            logger.debug("Known Dell 2-in-1 model detected; setting device_type to 'Tablets & eBook Readers'")
            return "Tablets & eBook Readers"
        
        # If we have clear keyword indicators, use them to resolve conflicts
        if has_desktop_keywords and not has_laptop_keywords:
            logger.debug(f"Strong desktop keywords found in model: {model_full}")
            # Still check model numbers but prefer desktop interpretation
            for model_num in model_numbers:
                t_prefixed = f"T{model_num}"
                if model_num in dell_desktop_models or t_prefixed in dell_desktop_models:
                    logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell desktop model: {model_num} with desktop keywords")
                    return "PC Desktops & All-In-Ones"
            
            # Even if not in desktop models, strong keywords override
            if model_numbers:  # Only if we have model numbers that look like Dell models
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on strong desktop keywords override")
                return "PC Desktops & All-In-Ones"
                
        elif has_laptop_keywords and not has_desktop_keywords:
            logger.debug(f"Strong laptop keywords found in model: {model_full}")
            # Still check model numbers but prefer laptop interpretation
            for model_num in model_numbers:
                if model_num in dell_laptop_models:
                    logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on Dell laptop model: {model_num} with laptop keywords")
                    return "PC Laptops & Netbooks"
            
            # Even if not in laptop models, strong keywords override
            if model_numbers:  # Only if we have model numbers that look like Dell models
                logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on strong laptop keywords override")
                return "PC Laptops & Netbooks"
        
        # Original model number checking (for cases without keyword conflicts)
        for model_num in model_numbers:
            # For each 4-digit number, check BOTH the bare number AND the T-prefixed version
            t_prefixed = f"T{model_num}"
            
            # Check laptops first (only if no desktop keywords present)
            if not has_desktop_keywords and (model_num in dell_laptop_models or t_prefixed in dell_laptop_models):
                logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on Dell laptop model: {model_num}")
                return "PC Laptops & Netbooks"
            
            # Check desktops 
            if model_num in dell_desktop_models or t_prefixed in dell_desktop_models:
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell desktop model: {model_num} (T-optional)")
                return "PC Desktops & All-In-Ones"
        
        # Additional context-based detection for Precision models when no direct match
        if "precision" in model_full:
            # Look for desktop indicators
            desktop_indicators = ["pc", "tower", "desktop", "workstation", "mini tower", "micro tower", "all-in-one", "aio", "small form factor", "mini pc", "micro pc", "rack", "rackmount", "1u", "2u", "3u", "4u"] + desktop_keywords
            laptop_indicators = ["mobile", "laptop", "notebook", "battery"] + laptop_keywords
            
            has_desktop_indicators = any(indicator in model_full for indicator in desktop_indicators)
            has_laptop_indicators = any(indicator in model_full for indicator in laptop_indicators)
            
            if has_desktop_indicators and not has_laptop_indicators:
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Precision desktop context indicators")
                return "PC Desktops & All-In-Ones"
            elif has_laptop_indicators and not has_desktop_indicators:
                logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on Precision mobile context indicators")
                return "PC Laptops & Netbooks"
            elif not has_laptop_indicators:
                # Default Precision models to desktop if no laptop indicators
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Precision default (no laptop indicators)")
                return "PC Desktops & All-In-Ones"
        
        # Final fallback: if we have model numbers but no matches, use keywords
        if model_numbers:
            if has_desktop_keywords:
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on keyword fallback")
                return "PC Desktops & All-In-Ones"
            elif has_laptop_keywords:
                logger.debug(f"Set device_type to 'PC Laptops & Netbooks' based on keyword fallback")
                return "PC Laptops & Netbooks"
        
        # Check for explicit T-prefix models in case regex missed them
        t_model_match = re.search(r'\bt(\d{4})\b', model_full)
        if t_model_match:
            t_model = f"T{t_model_match.group(1)}"
            if t_model in dell_desktop_models:
                logger.debug(f"Set device_type to 'PC Desktops & All-In-Ones' based on Dell Precision desktop model with T-prefix: {t_model}")
                return "PC Desktops & All-In-Ones"
                
    except ImportError:
        logger.warning("Could not load dell_models.py for Dell model checking")
    
    return None
    
def _detect_standalone_processor(brand: str, title_lower: str, logger: logging.Logger) -> str:
    """Detect standalone processors for Intel/AMD."""
    if not (brand and brand.lower() in ["intel", "amd"]):
        return None
        
    logger.debug(f"Checking Intel/AMD processor detection for brand: {brand}")
    
    processor_indicators = ["processor", "processors", "cpu", "cpus"]
    cpu_model_patterns = [
        r'core\s+i[3579]',  # Intel Core i3/i5/i7/i9
        r'ryzen\s+[3579]',  # AMD Ryzen 3/5/7/9
        r'xeon\s+\w+',     # Intel Xeon
        r'threadripper',    # AMD Threadripper
        r'fx-\d+',          # AMD FX series
        r'a[4-8]-\d+',      # AMD A-series
        r'athlon\s+\w+',    # AMD Athlon
        r'pentium\s+\w+',   # Intel Pentium
        r'celeron\s+\w+',   # Intel Celeron
        r'epyc\s+\w+',      # AMD EPYC
    ]
    
    cpu_specific_indicators = [
        r'lga\d+', r'am[45]', r'tr4', r'strx4',  # Socket types
        r'sr[0-9a-z]{3,4}',  # Intel stepping codes
        r'\d+mb',   # Cache specifications
        r'\d+\.\d+ghz',     # CPU speeds
        r'\d+\s+core'            # Core count
    ]
    
    has_processor_indicators = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in processor_indicators)
    has_cpu_model = any(re.search(pattern, title_lower) for pattern in cpu_model_patterns)
    has_cpu_specific = any(re.search(pattern, title_lower) for pattern in cpu_specific_indicators)
    
    # System indicators that suggest complete system
    system_indicators = [
        r'\d+gb\s+ram', r'\d+gb\s+memory', r'\d+tb\s+hdd', r'\d+gb\s+ssd',
        r'no\s+hdd', r'no\s+ssd', r'laptop\s+computer', r'notebook\s+computer', 
        r'desktop\s+computer', r'workstation\s+computer', r'all-in-one'
    ]
    has_system_indicators = any(re.search(pattern, title_lower) for pattern in system_indicators)
    
    if (has_processor_indicators or has_cpu_specific) and has_cpu_model and not has_system_indicators:
        logger.debug("Set device_type to 'CPUs/Processors' for Intel/AMD standalone processor")
        return "CPUs/Processors"
    
    return None

def _detect_custom_pc_components(brand: str, title_lower: str, logger: logging.Logger) -> str:
    """Detect custom PC builds, components, and cases.

    This logic now also covers listings that explicitly mention
    a "custom pc" (e.g. "Custom PC w/ Fractal Case …") or those that
    advertise a computer **case** bundled **with** core system
    components (commonly written as "case w/").  These represent
    whole desktop systems (often missing drives / GPU) rather than
    individual parts, so we classify them as
    "PC Desktops & All-In-Ones".
    """
    if not brand:
        return None

    # 1) Explicit "Custom PC" phrasing – applies regardless of brand
    if "custom pc" in title_lower or "custom build" in title_lower:
        logger.debug("Set device_type to 'PC Desktops & All-In-Ones' for explicit 'custom pc' phrase")
        return "PC Desktops & All-In-Ones"

    # 2) Listings that contain "case w/" and also mention CPU/RAM, indicating a complete system
    if re.search(r"\bcase\s+w[\/ ]", title_lower):
        core_system_terms = [
            "cpu", "intel", "amd", "core", "ryzen", "fx", "i3", "i5", "i7", "i9", "ram", "gb ram"
        ]
        if any(term in title_lower for term in core_system_terms):
            logger.debug("Set device_type to 'PC Desktops & All-In-Ones' for 'case w/' system listing")
            return "PC Desktops & All-In-Ones"

    # 3) Original behaviour – brand literally set to "custom"
    if brand.lower() == "custom":
        component_indicators = [
            "case", "chassis", "tower", "build", "pc", "computer", "thermaltake", "corsair", "nzxt", "fractal"
        ]
        if any(indicator in title_lower for indicator in component_indicators):
            logger.debug("Set device_type to 'Computer Components & Parts' for custom PC build/case (brand='Custom')")
            return "Computer Components & Parts"

    return None
    
def _detect_brand_series(brand: str, title_lower: str, logger: logging.Logger) -> str:
    """Detect device type based on brand and series from brand_model_types."""
    if not brand:
        return None
        
    try:
        from configs.brand_model_types import brand_model_types
        logger.debug("Loaded brand model types for device type detection")
    except ImportError:
        logger.warning("Could not load brand_model_types.py")
        return None
    
    if brand not in brand_model_types:
        return None
        
    logger.debug(f"Found brand '{brand}' in brand_model_types")
    
    # SPECIAL CASE: Check for KVM dongle cable adapters for Cisco before generic UCS lookup
    if brand.lower() == "cisco" and any(kvm_term in title_lower for kvm_term in ["kvm", "dongle", "cable adapter"]):
        logger.debug("Set device_type to 'Network Cables' for Cisco KVM dongle cable adapter")
        return "Network Cables"
    
    # SPECIAL CASE: Raritan CIM/KVM dongle modules should be treated as KVM Cables
    if brand.lower() == "raritan":
        raritan_kvm_terms = ["kvm", "cim", "kx", "cim module", "dongle", "virtual media"]
        if any(term in title_lower for term in raritan_kvm_terms):
            logger.debug("Set device_type to 'KVM Cables' for Raritan CIM/KVM module")
            return "KVM Cables"

    for model_key, type_info in brand_model_types[brand].items():
        # More flexible matching - check for the model key in the title with flexible hyphen/space handling
        flexible_pattern = re.escape(model_key.lower()).replace(r'\-', r'[\-\s]?').replace(r'\ ', r'[\-\s]?')
        if re.search(r'\b' + flexible_pattern + r'\b', title_lower):
            logger.debug(f"Found model '{model_key}' in title")
            
            # SPECIAL CASE: Inspiron desktop override for Dell
            if brand.lower() == "dell" and model_key.lower() == "inspiron":
                # If title clearly indicates a desktop context, force desktops
                desktop_indicators = ["desktop", "tower", "all-in-one", "aio"]
                if any(re.search(r'\b' + re.escape(ind) + r'\b', title_lower) for ind in desktop_indicators):
                    logger.debug("Inspiron + desktop indicators detected; setting device_type to 'PC Desktops & All-In-Ones' via brand-series")
                    return "PC Desktops & All-In-Ones"
                logger.debug("Skipping Inspiron brand model lookup - Dell model number checking should handle this")
                continue
            
            if isinstance(type_info, str):
                # Guard: Do not map to Toner Cartridges when toner detection is disabled
                if not ENABLE_TONER_DETECTION and type_info == "Toner Cartridges":
                    logger.debug("Toner detection disabled; skipping brand-model mapping to 'Toner Cartridges'")
                    continue
                # GUARD: Prevent false positives for Intel Xeon listings that are part of complete server systems
                if brand.lower() == "intel" and type_info == "Server CPUs/Processors":
                    # Indicators that this listing is a complete server rather than loose CPUs
                    system_context_patterns = [
                        r"\b\d+\s*gb\s+ram\b",          # e.g. "128GB RAM"
                        r"\b\d+\s*gb\s+memory\b",       # e.g. "64GB Memory"
                        r"\bno\s+hdd\b", r"\bno\s+ssd\b", r"\bno\s+hard\s*drive\b",
                        r"\bdell\b", r"\bhp\b", r"\blenovo\b",  # common server manufacturers appearing alongside Intel
                        r"\bpoweredge\b", r"\boemr\b", r"\bproliant\b",  # server product lines
                        r"\br\d{3,4}\b",                   # Dell R-series model numbers (e.g. R420, R730)
                    ]
                    has_system_context = any(re.search(pat, title_lower) for pat in system_context_patterns)

                    if has_system_context:
                        logger.debug("Intel brand mapping to 'Server CPUs/Processors' suppressed due to system context; returning 'Computer Servers' instead to avoid false positive")
                        return "Computer Servers"

                logger.debug(f"Set device_type to '{type_info}' from brand model lookup")
                return type_info
            elif isinstance(type_info, dict):
                logger.debug(f"Model has multiple device types: {type_info}")
                for dev_type, indicators in type_info.items():
                    if not ENABLE_TONER_DETECTION and dev_type == "Toner Cartridges":
                        continue
                    if any(re.search(r'\b' + re.escape(indicator.lower()) + r'\b', title_lower) for indicator in indicators):
                        logger.debug(f"Set device_type to '{dev_type}' from specific indicators")
                        return dev_type
                # Default to first type
                device_type = next((device_type_key for device_type_key in type_info.keys() if ENABLE_TONER_DETECTION or device_type_key != "Toner Cartridges"), None)
                if device_type is None:
                    continue
                logger.debug(f"Set device_type to default '{device_type}' from brand model types")
                return device_type
    
    return None
    
def _detect_brand_fallbacks(brand: str, title_lower: str, context: Dict[str, bool], logger: logging.Logger) -> str:
    """Detect device type using brand-specific fallback logic."""
    if not brand:
        return None
        
    brand_lower = brand.lower()
    
    if brand_lower == "apple":
        # Check for parts context first for Apple
        if context.get('has_parts_context', False):
            if context.get('has_phone_context', False) or any(phone_term in title_lower for phone_term in ["iphone", "ipad"]):
                return "Cell Phone & Smartphone Parts"
            else:
                return "Computer Components & Parts"
        elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["laptop", "macbook", "notebook"]):
            return "Apple Laptops"
        elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["desktop", "imac", "mac mini", "mac pro", "mac studio"]):
            return "Apple Desktops & All-In-Ones"
        elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["ipad", "tablet"]):
            return "Tablets & eBook Readers"
        elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["iphone"]):
            return "Cell Phones & Smartphones"
    
    elif brand_lower == "supermicro":
        return "Computer Servers"
        
    elif brand_lower in ["netapp", "emc", "pure storage", "hitachi"]:
        return "SAN Disk Arrays"
        
    elif brand_lower in ["synology", "qnap", "drobo"]:
        return "NAS Disk Arrays"
        
    elif brand_lower in ["dell", "hp", "lenovo"]:
        # Expanded patterns to detect laptop/notebook power adapters even when extra words (e.g. "USB-C") exist
        # between the laptop keyword and the adapter keyword.  These more flexible patterns use ".*" to allow
        # intervening descriptors so titles like "Laptop USB-C Chargers" or "Charger 65W for Dell Laptop" match.
        laptop_accessory_patterns = [
            # laptop followed (anywhere later) by a charger/adapter/graphics term
            r'\blaptop\b.*\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|video\s+card|graphics\s+card|gpu)\b',
            # notebook followed by a charger/adapter/graphics term
            r'\bnotebook\b.*\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|video\s+card|graphics\s+card|gpu)\b',
            # charger/adapter/graphics term followed (anywhere later) by laptop
            r'\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|video\s+card|graphics\s+card|gpu)\b.*\blaptop\b',
            # charger/adapter/graphics term followed by notebook
            r'\b(?:charger|adapter|adapters|chargers|power\s+adapter|power\s+supply|ac\s+adapter|cord|cable|supply|video\s+card|graphics\s+card|gpu)\b.*\bnotebook\b'
        ]

        # If this is clearly a laptop/notebook accessory, immediately classify as such to avoid
        # accidental fallback to "PC Laptops & Netbooks" later in this function.
        if any(re.search(pattern, title_lower) for pattern in laptop_accessory_patterns):
            # Check if it's a graphics card component
            if any(graphics_term in title_lower for graphics_term in ["video card", "graphics card", "gpu"]):
                logger.debug("Detected laptop graphics card in brand fallback logic – setting device_type to 'Graphics/Video Cards'.")
                return "Graphics/Video Cards"
            else:
                logger.debug("Detected laptop power accessory in brand fallback logic – setting device_type to 'Laptop Power Adapters/Chargers'.")
                return "Laptop Power Adapters/Chargers"

        # Previous simple patterns (kept for backward-compatibility to flag accessory context and bypass laptop fallback)
        simple_accessory_patterns = [
            r'\blaptop\s+(?:charger|adapter|power|ac|cord|cable|supply)',
            r'\bnotebook\s+(?:charger|adapter|power|ac|cord|cable|supply)',
            r'(?:charger|adapter|power|ac|cord|cable|supply)\s+(?:for\s+)?laptop',
            r'(?:charger|adapter|power|ac|cord|cable|supply)\s+(?:for\s+)?notebook'
        ]
        is_laptop_accessory = any(re.search(pattern, title_lower) for pattern in simple_accessory_patterns)
        
        if not is_laptop_accessory:
            # Dell Inspiron desktop preference when both laptop and desktop words occur
            if brand_lower == "dell" and "inspiron" in title_lower and any(ind in title_lower for ind in ["desktop", "tower", "all-in-one", "aio"]):
                return "PC Desktops & All-In-Ones"
            if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["desktop", "workstation", "precision", "optiplex", "elitedesk", "prodesk", "dm"]):
                return "PC Desktops & All-In-Ones"
            elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["laptop", "notebook", "portable"]):
                return "PC Laptops & Netbooks"
            elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["system x", "poweredge", "proliant", "rack"]):
                return "Computer Servers"
            elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["monitor", "widescreen", "display"]):
                # BUSINESS RULE: Monitors device type deprecated; map to Computer Servers
                return "Computer Servers"
            
    elif brand_lower in ["evga", "msi", "asus", "gigabyte", "zotac", "sapphire", "xfx", "powercolor", "pny", "asrock", "visiontek", "nvidia", "amd"] and context.get('has_gpu_context', False):
        return "Graphics/Video Cards"
    
    return None
    
def _detect_storage_arrays(context: Dict, title_lower: str, logger: logging.Logger) -> str:
    """Detect storage arrays with high priority."""
    if not context.get('has_storage_array_context', False):
        return None
        
    logger.debug("Storage array context detected - checking for SAN/NAS indicators")
    
    san_indicators = ["san", "storage area network", "fibre channel", "fc", "iscsi", "sas array", "block storage"]
    nas_indicators = ["nas", "network attached storage", "nfs", "cifs", "smb", "file server", "file storage"]
    
    if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in san_indicators):
        logger.debug("Set device_type to 'SAN Disk Arrays' based on SAN indicators")
        return "SAN Disk Arrays"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in nas_indicators):
        logger.debug("Set device_type to 'NAS Disk Arrays' based on NAS indicators")
        return "NAS Disk Arrays"
    else:
        # Default storage arrays to SAN
        logger.debug("Set device_type to 'SAN Disk Arrays' as default for storage arrays")
        return "SAN Disk Arrays"

def _detect_power_adapters(title_lower: str, logger: logging.Logger) -> str:
    """Detect power adapters/chargers."""
    power_adapter_indicators = [
        "adapter", "adapters", "charger", "chargers", "power adapter", "power adapters", 
        "ac adapter", "ac adapters", "power supply", "power cord", "power cable"
    ]
    
    wattage_pattern = re.search(r'\b(\d+)w\b', title_lower)
    has_power_adapter_keywords = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in power_adapter_indicators)
    
    # Exclusions
    networking_exclusions = ["network adapter", "network adapters", "wireless adapter", "wifi adapter", "ethernet adapter"]
    audio_exclusions = ["power amplifier", "amplifier", "amp", "audio", "stereo", "speaker", "receiver"]
    musical_exclusions = [
        "keyboard", "midi", "piano", "organ", "synthesizer", "electronic keyboard", 
        "digital piano", "yamaha", "casio", "roland", "korg", "kurzweil", "nord", "moog", 
        "alesis", "novation", "psr", "dgx", "clavinova", "workstation", "arranger"
    ]
    
    has_networking_exclusions = any(exclusion in title_lower for exclusion in networking_exclusions)
    has_audio_exclusions = any(exclusion in title_lower for exclusion in audio_exclusions)
    has_musical_exclusions = any(exclusion in title_lower for exclusion in musical_exclusions)
    
    # NEW: Check for "no power supply" pattern which indicates missing power supply, not a power supply listing
    no_power_patterns = [
        r'\bno\s+power\s+supply\b',
        r'\bwithout\s+power\s+supply\b', 
        r'\bmissing\s+power\s+supply\b',
        r'\bno\s+ac\s+adapter\b',
        r'\bwithout\s+ac\s+adapter\b',
        r'\bmissing\s+ac\s+adapter\b'
    ]
    has_no_power_patterns = any(re.search(pattern, title_lower) for pattern in no_power_patterns)
    
    if (has_power_adapter_keywords and 
        not has_networking_exclusions and 
        not has_audio_exclusions and 
        not has_musical_exclusions and
        not has_no_power_patterns):
        
        laptop_power_context = [
            wattage_pattern is not None,
            any(word in title_lower for word in ["laptop", "notebook", "dell", "hp", "lenovo", "apple", "asus", "acer"]),
            "ac" in title_lower,
            "power" in title_lower,
            "no power cord" in title_lower,
            "cord" in title_lower,
        ]
        
        if sum(laptop_power_context) >= 2:
            logger.debug("Set device_type to 'Laptop Power Adapters/Chargers' based on power adapter indicators")
            return "Laptop Power Adapters/Chargers"
    
    return None
    
def _detect_gpu_context(context: Dict, logger: logging.Logger) -> str:
    """Detect graphics cards based on GPU context."""
    if not context.get('has_gpu_context', False):
        return None
        
    logger.debug("Graphics card context detected - setting device type")
    return "Graphics/Video Cards"

def _detect_general_processors(title_lower: str, brand: str, context: Dict, logger: logging.Logger) -> str:
    """Detect standalone processors (general fallback for non-Intel/AMD)."""
    processor_indicators = ["processor", "processors", "cpu", "cpus"]
    cpu_model_patterns = [
        r'core\s+i[3579]',  # Intel Core i3/i5/i7/i9
        r'ryzen\s+[3579]',  # AMD Ryzen 3/5/7/9
        r'xeon\s+\w+',     # Intel Xeon
        r'threadripper',    # AMD Threadripper
        r'fx-\d+',          # AMD FX series
        r'a[4-8]-\d+',      # AMD A-series
        r'athlon\s+\w+',    # AMD Athlon
        r'pentium\s+\w+',   # Intel Pentium
        r'celeron\s+\w+',   # Intel Celeron
        r'epyc\s+\w+',      # AMD EPYC
    ]
    
    has_processor_indicators = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in processor_indicators)
    has_cpu_model = any(re.search(pattern, title_lower) for pattern in cpu_model_patterns)
    
    system_indicators = [r'\d+gb\s+ram', r'\d+gb\s+memory', r'\d+tb\s+hdd', r'\d+gb\s+ssd', r'no\s+hdd', r'no\s+ssd']
    has_system_indicators = any(re.search(pattern, title_lower) for pattern in system_indicators)
    
    if has_processor_indicators and has_cpu_model and not has_system_indicators and not brand:
        logger.debug("Set device_type to 'CPUs/Processors' based on standalone processor indicators")
        return "CPUs/Processors"
    
    return None

def _detect_fallback_generic(title_lower: str, logger: logging.Logger) -> str:
    """Final fallback detection for unrecognized items."""
    # NEW: Check for storage drive patterns first
    if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["hdd", "ssd", "nvme", "hard drive", "solid state"]):
        if any(re.search(r'\b' + re.escape(size) + r'\b', title_lower) for size in ["tb", "gb", "terabyte", "gigabyte"]):
            logger.debug("Set device_type to 'Internal Hard Disk Drives' based on storage drive indicators")
            return "Internal Hard Disk Drives"
    
    # Server memory detection moved to higher priority - removed from here
    
    if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["laptop", "notebook", "portable"]):
        return "PC Laptops & Netbooks"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["desktop", "workstation", "all-in-one", "aio", "optiplex", "elitedesk", "prodesk"]):
        return "PC Desktops & All-In-Ones"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["system x", "poweredge", "proliant"]) and not any(re.search(r'\b' + re.escape(storage_word) + r'\b', title_lower) for storage_word in ["storage", "array", "disk"]):
        return "Computer Servers"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["switch", "switches"]):
        return "Network Switches"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["router", "routers"]):
        return "Enterprise Routers"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["monitor", "widescreen", "display"]):
        logger.debug("Mapped deprecated 'Monitors' classification to 'Computer Servers' based on generic indicators")
        return "Computer Servers"
    elif any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in ["case", "chassis", "build"]):
        logger.debug("Set device_type to 'Computer Components & Parts' based on case indicators")
        return "Computer Components & Parts"
    
    return None
    
def _detect_screen_protectors(title_lower: str, logger: logging.Logger) -> str:
    """Detect screen protectors with high priority."""
    screen_protector_indicators = [
        "screen protector", "screen protectors", "tempered glass screen protector",
        "glass screen protector", "privacy screen", "anti-glare screen",
        "screen guard", "screen film", "display protector"
    ]
    
    # Check for explicit screen protector mentions
    if any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) for indicator in screen_protector_indicators):
        # Additional context check - look for "for" indicating it's an accessory
        for_pattern_indicators = [
            r'\bfor\s+\d+["\′]?\s*macbook\b',     # "for 16" MacBook"
            r'\bfor\s+macbook\b',                  # "for MacBook"
            r'\bfor\s+iphone\b',                   # "for iPhone"
            r'\bfor\s+ipad\b',                     # "for iPad"
            r'\bfor\s+samsung\b',                  # "for Samsung"
            r'\bfor\s+pixel\b'                     # "for Pixel"
        ]
        
        has_for_context = any(re.search(pattern, title_lower) for pattern in for_pattern_indicators)
        
        if has_for_context or any(indicator in title_lower for indicator in screen_protector_indicators):
            logger.debug("Set device_type to 'Screen Protectors' from screen protector detection")
            return "Screen Protectors"
    
    return None

def _detect_storage_drives(title_lower: str, logger: logging.Logger) -> str:
    """Detect standalone storage drives with very strict criteria."""
    
    # Must have explicit storage drive indicators  
    explicit_drive_indicators = [
        "hdd", "ssd", "nvme", "hard drive", "solid state drive", "hard disk drive",
        "internal drive", "desktop drive", "enterprise drive", "sata drive"
    ]
    
    # Must have capacity indicators
    capacity_patterns = [
        r'\b\d+(?:\.\d+)?tb\b',  # TB capacities
        r'\b[5-9]\d{2,}gb\b',    # Large GB capacities (500GB+)
        r'\b[1-9]\d{3,}gb\b'     # Very large GB capacities (1000GB+)
    ]
    
    # Must have technical interface or form factor indicators
    technical_indicators = [
        r'\bsata\b', r'\bide\b', r'\bscsi\b', r'\bsas\b',
        r'\b[23]\.5["\′]?\b',    # Form factors
        r'\b[23]\.5\s*inch\b',
        r'\brpm\b', r'\b\d{4,5}rpm\b',  # RPM indicators
        r'\b\d+gb/s\b', r'\b\d+mb/s\b'  # Transfer rates
    ]
    
    # STRICT exclusions - any of these disqualify
    strict_exclusions = [
        # Computer systems
        r'\blaptop\b', r'\bnotebook\b', r'\bdesktop computer\b', r'\btower\b', 
        r'\bworkstation\b', r'\bserver\b', r'\ball-in-one\b', r'\bpc\b',
        
        # Computer brands/models that indicate complete systems
        r'\blatitude\b', r'\binspiron\b', r'\boptiplex\b', r'\bthinkpad\b',
        r'\bmacbook\b', r'\bsurface\b', r'\bpavilion\b', r'\benvy\b',
        r'\bprecision\b', r'\belitebook\b', r'\bprobook\b',
        
        # Lot indicators
        r'\blot of\b', r'\blots of\b', r'\bqty\b', r'\bquantity\b',
        
        # Missing/no storage indicators
        r'\bno ssd\b', r'\bno hdd\b', r'\bno hard drive\b', 
        r'\bwithout ssd\b', r'\bwithout hdd\b', r'\bno storage\b'
    ]
    
    # Check conditions
    has_explicit_drives = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) 
                             for indicator in explicit_drive_indicators)
    
    has_capacity = any(re.search(pattern, title_lower) for pattern in capacity_patterns)
    
    has_technical = any(re.search(pattern, title_lower) for pattern in technical_indicators)
    
    has_exclusions = any(re.search(pattern, title_lower) for pattern in strict_exclusions)
    
    # VERY STRICT: Must have ALL three positive indicators AND no exclusions
    if has_explicit_drives and has_capacity and has_technical and not has_exclusions:
        logger.debug("Set device_type to 'Internal Hard Disk Drives' from strict storage drive detection")
        return "Internal Hard Disk Drives"
    else:
        if has_exclusions:
            logger.debug(f"Storage drive detection excluded due to system context in: {title_lower}")
        else:
            logger.debug(f"Insufficient storage drive indicators: drives={has_explicit_drives}, capacity={has_capacity}, technical={has_technical}")
    
    return None
    
def _detect_switch_power_supplies(title_lower: str, logger: logging.Logger) -> str:
    """Detect switch power supplies with high priority."""
    power_supply_indicators = [
        "power supply", "power supplies", "psu", "psus", "power module", "power modules"
    ]
    
    # Network equipment power supply context
    network_power_context = [
        "cisco", "juniper", "aruba", "hp", "dell", "brocade", "fortinet", "ubiquiti",
        "switch", "switches", "router", "routers", "nexus", "catalyst", "procurve"
    ]
    
    # Wattage indicators
    wattage_pattern = re.search(r'\b(\d+)w\b', title_lower)
    
    has_power_supply_keywords = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) 
                                   for indicator in power_supply_indicators)
    
    has_network_context = any(re.search(r'\b' + re.escape(context) + r'\b', title_lower) 
                             for context in network_power_context)
    
    # Check for "for [network device]" pattern
    for_pattern = re.search(r'\bfor\s+(?:cisco|juniper|hp|dell|aruba|brocade)\s+(?:nexus|catalyst|procurve|switch)', title_lower)
    
    if has_power_supply_keywords and (has_network_context or for_pattern or wattage_pattern):
        logger.debug("Set device_type to 'Switch Power Supplies' from power supply detection")
        return "Switch Power Supplies"
    
    return None

def _detect_keyboard_accessories(title_lower: str, logger: logging.Logger) -> str:
    """Detect keyboard accessories with high priority."""
    keyboard_accessory_patterns = [
        r'\battachable\s+keyboard\b',
        r'\bkeyboard\s+for\b', 
        r'\bkeyboard\s+case\b',
        r'\bkeyboard\s+cover\b',
        r'\bkeyboard\s+folio\b',
        r'\bwireless\s+keyboard\b',
        r'\bbluetooth\s+keyboard\b',
        r'\btablet\s+keyboard\b'
    ]
    
    if any(re.search(pattern, title_lower) for pattern in keyboard_accessory_patterns):
        logger.debug("Set device_type to 'Cases, Covers, Keyboard Folios' from keyboard accessory detection")
        return "Cases, Covers, Keyboard Folios"
    
    return None

def _detect_electronic_keyboards(title_lower: str, logger: logging.Logger) -> str:
    """Detect electronic keyboards and musical instruments."""
    keyboard_indicators = [
        "midi keyboard", "electronic keyboard", "digital piano", 
        "synthesizer", "synth", "organ", "workstation", "arranger",
        "psr", "pss", "ypt", "dgx", "cvp", "clavinova",  # Yamaha model prefixes
        "casiotone", "ctx", "lk", "sa",  # Casio model prefixes
        "rd", "fp", "go", "fantom", "juno", "jupiter",  # Roland model prefixes
        "krome", "kross", "kronos", "pa", "micro"  # Korg model prefixes
    ]
    
    musical_brands = ["yamaha", "casio", "roland", "korg", "kurzweil", "nord", "moog", "alesis", "novation"]
    
    # Computer accessory exclusions
    computer_accessory_indicators = [
        "attachable", "for laptop", "for notebook", "for tablet", "for dell", "for hp", 
        "for lenovo", "for apple", "for surface", "latitude", "inspiron", "thinkpad",
        "rugged tablet", "keyboard case", "keyboard cover", "keyboard folio",
        "ikey", "logitech", "microsoft keyboard", "apple keyboard"
    ]
    
    # ENHANCED: Add computer brand and model exclusions
    computer_brands_and_models = [
        "dell optiplex", "dell latitude", "dell inspiron", "dell precision", "dell xps",
        "hp elitebook", "hp probook", "hp pavilion", "hp envy", "hp omen",
        "lenovo thinkpad", "lenovo ideapad", "lenovo yoga", "lenovo legion",
        "apple macbook", "apple imac", "apple mac mini", "apple mac pro",
        "microsoft surface", "asus zenbook", "asus vivobook", "asus rog",
        "acer aspire", "acer predator", "acer nitro", "acer swift"
    ]
    
    # ENHANCED: Add exclusions for memory/server components
    memory_server_exclusions = [
        "server", "ram", "memory", "ecc", "registered", "pc3", "pc4", "ddr3", "ddr4", "ddr5",
        "hynix", "micron", "samsung", "crucial", "corsair", "kingston"
    ]
    
    has_keyboard_indicators = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) 
                                 for indicator in keyboard_indicators)
    has_musical_brand = any(re.search(r'\b' + re.escape(brand) + r'\b', title_lower) 
                           for brand in musical_brands)
    has_computer_accessory = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) 
                                for indicator in computer_accessory_indicators)
    
    # NEW: Check for computer brand + model combinations
    has_computer_brand_model = any(brand_model in title_lower for brand_model in computer_brands_and_models)
    
    # NEW: Check for memory/server component context
    has_memory_server_context = any(re.search(r'\b' + re.escape(indicator) + r'\b', title_lower) 
                                   for indicator in memory_server_exclusions)
    
    # NEW: Specific pattern for Casio MT keyboards (requires "casio" + "mt" + digits)
    casio_mt_pattern = r'\bcasio\s+mt\d+\b'
    has_casio_mt = bool(re.search(casio_mt_pattern, title_lower))
    
    # Check for key count indicators (common in electronic keyboards) but exclude if computer accessory context
    key_count_pattern = re.search(r'\b(\d+)\s*-?\s*key\b', title_lower)
    
    # Only detect as electronic keyboard if:
    # 1. Has musical keyboard indicators OR specific Casio MT pattern OR (musical brand AND key count)
    # 2. AND does NOT have computer accessory indicators
    # 3. AND does NOT have computer brand+model combinations  
    # 4. AND does NOT have memory/server component context
    if ((has_keyboard_indicators or has_casio_mt or (has_musical_brand and key_count_pattern)) and 
        not has_computer_accessory and not has_computer_brand_model and not has_memory_server_context):
        logger.debug("Set device_type to 'Electronic Keyboards' from keyboard detection")
        return "Electronic Keyboards"
    
    return None
    
def _detect_server_memory(title_lower: str, logger: logging.Logger) -> str:
    """Detect server memory/RAM with high priority."""
    server_memory_indicators = [
        r'\bserver\s+ram\b',
        r'\bserver\s+memory\b', 
        r'\bram\s+server\b',
        r'\bmemory\s+server\b',
        r'\becc\s+server\b',
        r'\bserver\s+ecc\b',
        r'\breg\s+ecc\s+server\b',
        r'\bregistered\s+ecc\s+server\b',
        r'\bddr[0-9]?\s+.*server\s+ram\b',
        r'\bddr[0-9]?\s+.*server\s+memory\b',
        r'\bpc3.*server\s+ram\b',
        r'\bpc4.*server\s+ram\b',
        r'\bserver.*ddr[0-9]?\s+.*\b(ram|memory)\b',
        r'\b(ram|memory).*server.*ddr[0-9]?\b'
    ]
    
    if any(re.search(pattern, title_lower) for pattern in server_memory_indicators):
        logger.debug("Set device_type to 'Server Memory (RAM)' based on server memory indicators")
        return "Server Memory (RAM)"
    
    return None

def _detect_memory_modules(title_lower: str, logger: logging.Logger) -> str:
    """Detect standalone memory modules/sticks - very conservative to avoid false positives on complete systems."""
    
    # EXPLICIT module/stick language - these are strong indicators of standalone memory
    explicit_module_patterns = [
        r'\bcamm\s+module\b',           # CAMM Module (very specific)
        r'\bsodimm\s+module\b',         # SODIMM Module
        r'\bdimm\s+module\b',           # DIMM Module  
        r'\bmemory\s+module\b',         # Memory Module
        r'\bram\s+module\b',            # RAM Module
        r'\bmemory\s+stick\b',          # Memory Stick
        r'\bram\s+stick\b',             # RAM Stick
        r'\bmemory\s+kit\b',            # Memory Kit
        r'\bram\s+kit\b',               # RAM Kit
    ]
    
    # Check for explicit module language first
    if any(re.search(pattern, title_lower) for pattern in explicit_module_patterns):
        # SAFETY CHECK: Exclude if it mentions complete system components
        system_component_exclusions = [
            "processor", "cpu", "intel core", "amd ryzen", "core i3", "core i5", "core i7", "core i9",
            "hdd", "ssd", "tb", "terabyte", "hard drive", "solid state",
            "imac", "macbook", "laptop", "desktop", "computer", "pc", "system", "tower", "workstation",
            "all-in-one", "optiplex", "precision workstation", "elitedesk", "prodesk",
            "poweredge", "proliant", "server"
        ]
        
        if not any(exclusion in title_lower for exclusion in system_component_exclusions):
            logger.debug("Set device_type to 'Memory (RAM)' based on explicit memory module patterns")
            return "Memory (RAM)"
    
    return None

def _detect_rack_monitor(title_lower: str, logger: logging.Logger) -> str:
    """Detect rack-mounted monitors that should be classified as Monitors, not Servers."""
    rack_monitor_patterns = [
        r'\brack\s+monitor\b',           # Rack Monitor
        r'\brack\s+mounted\s+monitor\b', # Rack Mounted Monitor
        r'\brack\s+mount\s+monitor\b',   # Rack Mount Monitor
        r'\b\d+u\s+monitor\b',           # 1U Monitor, 2U Monitor, etc.
        r'\brack\s+lcd\s+monitor\b',     # Rack LCD Monitor
        r'\bkvm\s+monitor\b',            # KVM Monitor (often rack-mounted)
        r'\bkvm\s+console\s+monitor\b',  # KVM Console Monitor
    ]
    
    if any(re.search(pattern, title_lower) for pattern in rack_monitor_patterns):
        logger.debug("Mapped deprecated 'Monitors' classification to 'Computer Servers' based on rack monitor patterns")
        return "Computer Servers"
    
    return None

def determine_device_type(brand_data: Dict, context: Dict, title: str, sanitized_title: str, logger: logging.Logger) -> str:
    """Determine the device type based on title content and brand/model data."""
    brand = brand_data.get("brand")
    title_lower = title.lower()
    
    logger.debug(f"determine_device_type called with brand='{brand}'")
    logger.debug(f"Full title: '{title}'")
    logger.debug(f"Graphics card context: {context.get('has_gpu_context', False)}")
    logger.debug(f"Is system with GPU: {context.get('is_system_with_gpu', False)}")
    logger.debug(f"Laptop context: {context.get('has_laptop_context', False)}")
    logger.debug(f"Desktop context: {context.get('has_desktop_context', False)}")

    # ABSOLUTE OVERRIDE: Dell Inspiron with desktop indicators must be a desktop
    try:
        if brand and str(brand).lower() == "dell":
            if "inspiron" in title_lower and any(x in title_lower for x in ("desktop", "tower", "all-in-one", "aio")):
                logger.debug("Dell Inspiron + desktop indicators detected; setting device_type to 'PC Desktops & All-In-Ones'")
                return "PC Desktops & All-In-Ones"
    except Exception:
        pass
    
    # PRIORITY 0.0: Keyboard accessory detection (HIGHEST PRIORITY - before electronic keyboards)
    device_type = _detect_keyboard_accessories(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.01: Electronic keyboard detection (HIGH PRIORITY - after keyboard accessories)
    device_type = _detect_electronic_keyboards(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.02: Toner cartridge detection (HIGHEST PRIORITY)
    if ENABLE_TONER_DETECTION:
        device_type = _detect_toner_cartridges(title_lower, brand, logger)
        if device_type:
            return device_type
        
    # PRIORITY 0.05: Screen protector detection (HIGHEST PRIORITY)
    device_type = _detect_screen_protectors(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.1: Parts context detection (HIGHEST PRIORITY)
    device_type = _detect_parts_context(context, title_lower, brand_data, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.12: Server memory detection (HIGH PRIORITY - before storage arrays)
    device_type = _detect_server_memory(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.13: Memory module detection (HIGH PRIORITY - very specific patterns only)
    device_type = _detect_memory_modules(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.14: Rack monitor detection (HIGH PRIORITY - before server detection)
    device_type = _detect_rack_monitor(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.15: Storage drive detection (HIGH PRIORITY) - NEW
    device_type = _detect_storage_drives(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.2: Audio equipment detection (HIGHEST PRIORITY)
    device_type = _detect_audio_equipment(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.25: Switch power supply detection (HIGH PRIORITY)
    device_type = _detect_switch_power_supplies(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.29: Thin client detection (HIGH PRIORITY - before laptop/desktop detection)
    if context.get('has_thin_client_context', False):
        logger.debug("Thin client context detected – setting device_type to 'Servers, Clients & Terminals'")
        return "Servers, Clients & Terminals"
    
    # PRIORITY 0.295: Explicit server product lines (e.g., Dell PowerEdge) before laptop/desktop detection
    # Ensure any Dell PowerEdge listing is classified as a server regardless of 'Desktop' wording in title
    try:
        if re.search(r"\bpoweredge\b", title_lower):
            logger.debug("Detected 'PowerEdge' in title – setting device_type to 'Computer Servers'")
            return "Computer Servers"
        model_val_for_server = (brand_data.get("model") or "")
        if brand and brand.lower() == "dell" and re.search(r"\bpoweredge\b", model_val_for_server.lower()):
            logger.debug("Detected 'PowerEdge' in model – setting device_type to 'Computer Servers'")
            return "Computer Servers"
    except Exception:
        # Non-fatal: continue with normal flow if any issue arises
        pass

    # PRIORITY 0.3: Laptop/Desktop detection (OVERRIDE GPU context for device type)
    if context.get('has_laptop_context', False):
        if brand and brand.lower() == "apple":
            return "Apple Laptops"
        else:
            # SPECIAL CASE: Dell Inspiron with desktop indicators → prefer desktops even if laptop context exists
            try:
                if brand and brand.lower() == "dell" and "inspiron" in title_lower and any(x in title_lower for x in ("desktop", "tower", "all-in-one", "aio")):
                    logger.debug("Laptop+Desktop context with Dell Inspiron; preferring 'PC Desktops & All-In-Ones'")
                    return "PC Desktops & All-In-Ones"
            except Exception:
                pass
            # Gray-area override: if this is a known Dell 2-in-1 model, treat as tablet
            try:
                from configs.dell_models import dell_2in1_models
            except Exception:
                dell_2in1_models = set()
            model_val = brand_data.get("model", "") or ""
            model_nums = re.findall(r"\b\d{4}\b", model_val.lower())
            if brand and brand.lower() == "dell" and any(m in dell_2in1_models for m in model_nums):
                return "Tablets & eBook Readers"
            return "PC Laptops & Netbooks"
    
    if context.get('has_desktop_context', False):
        if brand and brand.lower() == "apple":
            return "Apple Desktops & All-In-Ones"
        else:
            return "PC Desktops & All-In-Ones"
    
    # PRIORITY 0.4: Dell model number checking (BEFORE brand model lookup)
    device_type = _detect_dell_device_type(brand_data, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.5: Custom PC components detection
    device_type = _detect_custom_pc_components(brand, title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 0.6: Standalone processor detection for Intel/AMD (BEFORE brand model lookup)
    device_type = _detect_standalone_processor(brand, title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 1: Brand-based series detection
    device_type = _detect_brand_series(brand, title_lower, logger)
    if device_type:
        return device_type
    
    # Brand fallbacks
    device_type = _detect_brand_fallbacks(brand, title_lower, context, logger)
    if device_type:
        return device_type
    
    # PRIORITY 2: Check for storage array context (HIGH PRIORITY)
    device_type = _detect_storage_arrays(context, title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 3: Check for power adapter/charger indicators
    device_type = _detect_power_adapters(title_lower, logger)
    if device_type:
        return device_type
    
    # PRIORITY 4: Check for graphics card context (ONLY if not a system with GPU)
    if context.get('has_gpu_context', False) and not context.get('is_system_with_gpu', False):
        device_type = _detect_gpu_context(context, logger)
        if device_type:
            return device_type
    
    # PRIORITY 5: Check for standalone processor indicators (GENERAL FALLBACK)
    device_type = _detect_general_processors(title_lower, brand, context, logger)
    if device_type:
        return device_type
    
    # PRIORITY 6: Final fallback for unrecognized items
    device_type = _detect_fallback_generic(title_lower, logger)
    if device_type:
        logger.debug(f"Set device_type to '{device_type}' from generic indicators")
        return device_type
    
    logger.debug(f"Final device_type determined: None")
    return None
    
def apply_lot_extractors(extractors: List, tokens: List[str], consumed: Set[int], logger: logging.Logger) -> Dict:
    """Apply lot extractors and return extracted data."""
    data = {}
    
    # FIRST: Apply CPU speed extraction before anything else can consume the GHz tokens
    cpu_speed_extractors = [ext for ext in extractors if ext.name == "cpu_speed"]
    for extractor in cpu_speed_extractors:
        logger.debug(f"EARLY CPU speed extraction: {extractor.name}")
        try:
            matches = extractor.extract(tokens, consumed)
            logger.debug(f"EARLY CPU speed extractor returned matches: {matches}")
            if matches:
                for i, match in enumerate(matches, start=1):
                    if isinstance(match, tuple) and len(match) == 2:
                        match_indices, consume_indices = match
                    else:
                        match_indices = match
                        consume_indices = match_indices
                    logger.debug(f"Processing EARLY CPU speed tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                    flat_indices = []
                    for idx in match_indices:
                        if isinstance(idx, int):
                            flat_indices.append(idx)
                            if getattr(extractor, 'consume_on_match', True):
                                consumed.add(idx)
                    if flat_indices:
                        extracted = extractor.process_match(tokens, flat_indices)
                        for key, value in extracted.items():
                            if len(matches) > 1:
                                # Use title_cpu_speed_key format for title data
                                numbered_key = f"title_{key}{i}_key"
                                data[numbered_key] = value
                                logger.debug(f"Added EARLY numbered CPU speed field: {numbered_key} = {value}")
                            else:
                                # Use title_cpu_speed_key format for title data
                                title_key = f"title_{key}_key"
                                data[title_key] = value
                                logger.debug(f"Added EARLY CPU speed field: {title_key} = {value}")
        except Exception as e:
            logger.error(f"Error in EARLY CPU speed extractor {extractor.name}: {str(e)}")
            continue
    
    lot_extractors = [ext for ext in extractors if ext.name == "lot"]
    
    # Define port/connector terms that should exclude lot detection
    port_connector_terms = [
        # Display/Video ports
        "displayport", "mini displayport", "dp", "hdmi", "dvi", "dvi-d", "dvi-i", 
        "vga", "thunderbolt", "usb-c", "usb", "usb-a", "usb-b", "type-c",
        
        # Audio/Network ports
        "audio", "mic", "microphone", "headphone", "speaker", "ethernet", "rj45",
        "fiber", "optical", "coax", "bnc",
        
        # Power/Data connectors
        "sata", "pcie", "pci-e", "molex", "power", "ac", "dc", "barrel",
        
        # Memory/Expansion slots
        "dimm", "sodimm", "simm", "slot", "bay", "m.2", "nvme",
        
        # Other technical connectors
        "gpio", "uart", "i2c", "spi", "jtag", "antenna", "rf"
    ]
    
    for extractor in lot_extractors:
        logger.debug(f"Applying extractor: {extractor.name}")
        try:
            matches = extractor.extract(tokens, consumed)
            logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
            if matches:
                # Check each potential lot match for port/connector context
                valid_matches = []
                
                for match in matches:
                    if isinstance(match, tuple) and len(match) == 2:
                        match_indices, consume_indices = match
                    else:
                        match_indices = match
                        consume_indices = match_indices
                    
                    # Check if this is actually a port/connector count
                    is_port_context = False
                    
                    # Look at tokens around the match for port/connector terms
                    for match_idx in match_indices:
                        # Check tokens after the match (within 3 positions)
                        for check_idx in range(match_idx + 1, min(len(tokens), match_idx + 4)):
                            if check_idx < len(tokens):
                                check_token = tokens[check_idx].lower()
                                # Remove common punctuation for comparison
                                check_token = re.sub(r'[^\w\s-]', '', check_token)
                                
                                if check_token in port_connector_terms:
                                    is_port_context = True
                                    logger.debug(f"Excluding lot match at index {match_idx} due to port/connector context: '{tokens[match_idx]}' followed by '{tokens[check_idx]}'")
                                    break
                        
                        if is_port_context:
                            break
                    
                    # Only process if it's not a port/connector context
                    if not is_port_context:
                        valid_matches.append((match_indices, consume_indices))
                
                # Process valid matches
                for match_indices, consume_indices in valid_matches:
                    logger.debug(f"Processing valid lot tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted {extractor.name}: {extracted}")
                    
        except ValueError as e:
            logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
            continue
        except Exception as e:
            logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
            continue
    return data
    
def apply_phone_and_status_extractors(extractors: List, tokens: List[str], consumed: Set[int], context: Dict, device_type: str, logger: logging.Logger) -> Dict:
    """Apply phone and status extractors."""
    data = {}
    
    # Detect if this is a phone device (exclude computer tablets like Surface)
    is_computer_tablet = device_type == "Tablets & eBook Readers" and any(brand.lower() in ["microsoft", "lenovo", "hp", "dell", "asus", "acer", "samsung"] for brand in tokens)
    is_phone_device = (device_type in ["Cell Phones & Smartphones"] or (device_type == "Tablets & eBook Readers" and not is_computer_tablet)) and context['has_phone_context']
    
    # ENHANCED: Also apply to Apple tablets
    is_apple_tablet = device_type == "Tablets & eBook Readers" and any(token.lower() == "apple" for token in tokens)
    
    # Apply phone extractor if phone context is detected OR if it's an Apple tablet
    if is_phone_device or context['has_phone_context'] or is_apple_tablet:
        phone_extractors = [ext for ext in extractors if ext.name == "phone"]
        for extractor in phone_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    for match in matches:
                        if isinstance(match, tuple) and len(match) == 2:
                            match_indices, consume_indices = match
                        else:
                            match_indices = match
                            consume_indices = match_indices
                        logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                        extracted = extractor.process_match(tokens, match_indices)
                        data.update(extracted)
                        if getattr(extractor, 'consume_on_match', True):
                            for idx in consume_indices:
                                if isinstance(idx, int):
                                    consumed.add(idx)
                        logger.debug(f"Extracted {extractor.name}: {extracted}")
                        
                        # Override device type if phone extractor sets it
                        if "device_type" in extracted:
                            device_type = extracted["device_type"]
                            is_phone_device = True
                            
                # ENHANCED: Also try extracting from additional_info if present
                # This is for cases where network status, model numbers are in additional_info
                try:
                    # Use the additional_info specific extractor
                    additional_matches = extractor.extract_from_additional_info(tokens, consumed)
                    logger.debug(f"Extractor {extractor.name} additional_info returned matches: {additional_matches}")
                    if additional_matches:
                        for match in additional_matches:
                            if isinstance(match, tuple) and len(match) == 2:
                                match_indices, consume_indices = match
                            else:
                                match_indices = match
                                consume_indices = match_indices
                            logger.debug(f"Processing additional_info tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                            extracted = extractor.process_additional_info_match(tokens, match_indices)
                            data.update(extracted)
                            if getattr(extractor, 'consume_on_match', True):
                                for idx in consume_indices:
                                    if isinstance(idx, int):
                                        consumed.add(idx)
                            logger.debug(f"Extracted from additional_info {extractor.name}: {extracted}")
                except AttributeError:
                    # Extractor doesn't have additional_info methods, skip
                    pass
                            
            except Exception as e:
                logger.error(f"Error in {extractor.name}: {str(e)}")

    # Apply status extractors for ALL device types (not just phones)
    # Collect all matched indices from all status extractors before consuming
    status_matched_indices = set()
    status_extractors = [ext for ext in extractors if ext.name in ["storage_status", "battery_status", "os_status", "bios_status"]]
    
    for extractor in status_extractors:
        logger.debug(f"Applying status extractor: {extractor.name}")
        try:
            matches = extractor.extract(tokens, consumed)
            logger.debug(f"Status extractor {extractor.name} returned matches: {matches}")
            if matches:
                for match in matches:
                    if isinstance(match, tuple) and len(match) == 2:
                        match_indices, consume_indices = match
                    else:
                        match_indices = match
                        consume_indices = match_indices
                    logger.debug(f"Processing status tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    data.update(extracted)
                    
                    # Collect indices to consume later (don't consume yet)
                    for idx in match_indices:
                        if isinstance(idx, int):
                            status_matched_indices.add(idx)
                    
                    logger.debug(f"Extracted status {extractor.name}: {extracted}")
        except Exception as e:
            logger.error(f"Error in status extractor {extractor.name}: {str(e)}")
    
    # Now consume all the status-matched indices at once
    if status_matched_indices:
        logger.debug(f"Consuming status extractor matched indices: {sorted(status_matched_indices)}")
        consumed.update(status_matched_indices)
    
    data['is_phone_device'] = is_phone_device or is_apple_tablet
    return data
    
def apply_switch_adapter_extractors(extractors: List, tokens: List[str], consumed: Set[int], is_phone_device: bool, logger: logging.Logger) -> Dict:
    """Apply switch and adapter extractors for non-phone devices."""
    data = {}
    
    # Skip switch/adapter extractors if it's a phone device
    if not is_phone_device:
        switch_extractors = [ext for ext in extractors if ext.name.startswith("switch_")]
        for extractor in switch_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                        match_indices, consume_indices = matches[0]
                    else:
                        match_indices = matches[0]
                        consume_indices = match_indices
                    logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted {extractor.name}: {extracted}")
            except Exception as e:
                logger.error(f"Error in {extractor.name}: {str(e)}")

        adapter_extractors = [ext for ext in extractors if ext.name.startswith("adapter_")]
        for extractor in adapter_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                        match_indices, consume_indices = matches[0]
                    else:
                        match_indices = matches[0]
                        consume_indices = match_indices
                    logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted {extractor.name}: {extracted}")
            except Exception as e:
                logger.error(f"Error in {extractor.name}: {str(e)}")
    
    return data

def apply_cpu_ram_storage_extractors(extractors: List, tokens: List[str], consumed: Set[int], is_network_device: bool, is_phone_device: bool, context: Dict, logger: logging.Logger) -> Dict:
    """Apply CPU, RAM, and storage extractors for appropriate device types."""
    data = {}
    cpu_extracted = False
    
    if not is_network_device and not is_phone_device:
        # Handle "Dual Core" and "Quad Core" patterns before regular extraction
        for i in range(len(tokens) - 1):
            if i < len(tokens) - 1 and tokens[i].lower() in ["dual", "quad"] and tokens[i+1].lower() == "core":
                logger.debug(f"Found '{tokens[i]} {tokens[i+1]}' pattern - will be treated as CPU attribute, not quantity")

        cpu_model_extractors = [ext for ext in extractors if ext.name == "cpu_model"]
        cpu_speed_extractors = [ext for ext in extractors if ext.name == "cpu_speed"]
        
        # Apply CPU speed extractor first
        for extractor in cpu_speed_extractors:
            logger.debug(f"Applying CPU speed extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"CPU speed extractor {extractor.name} returned matches: {matches}")
                if matches:
                    for i, match in enumerate(matches, start=1):
                        if isinstance(match, tuple) and len(match) == 2:
                            match_indices, consume_indices = match
                        else:
                            match_indices = match
                            consume_indices = match_indices
                        logger.debug(f"Processing CPU speed tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                        flat_indices = []
                        for idx in match_indices:
                            if isinstance(idx, int):
                                flat_indices.append(idx)
                                if getattr(extractor, 'consume_on_match', True):
                                    consumed.add(idx)
                        if flat_indices:
                            extracted = extractor.process_match(tokens, flat_indices)
                            for key, value in extracted.items():
                                if len(matches) > 1:
                                    numbered_key = f"{key}{i}"
                                    data[numbered_key] = value
                                    logger.debug(f"Added numbered CPU speed field: {numbered_key} = {value}")
                                else:
                                    data[key] = value
                                    logger.debug(f"Added CPU speed field: {key} = {value}")
            except Exception as e:
                logger.error(f"Error in CPU speed extractor {extractor.name}: {str(e)}")
                continue
        
        # Apply CPU model extractors
        for extractor in cpu_model_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    cpu_extracted = True
                    
                    if len(matches) > 1:
                        # Handle multiple separate CPU matches
                        for i, match in enumerate(matches, start=1):
                            if isinstance(match, tuple) and len(match) == 2:
                                match_indices, consume_indices = match
                            else:
                                match_indices = match
                                consume_indices = match_indices
                            logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                                    if getattr(extractor, 'consume_on_match', True):
                                        consumed.add(idx)
                            if flat_indices:
                                extracted = extractor.process_match(tokens, flat_indices)
                                
                                # Add numbered fields for each match
                                for key, value in extracted.items():
                                    numbered_key = f"{key}{i}"
                                    data[numbered_key] = value
                                    logger.debug(f"Added numbered CPU field: {numbered_key} = {value}")
                                
                        # Set shared keys only if all CPUs have the same values
                        # Check for shared cpu_brand
                        cpu_brands = [data.get(f"cpu_brand{i}") for i in range(1, len(matches) + 1)]
                        cpu_brands = [b for b in cpu_brands if b]
                        if cpu_brands and len(set(cpu_brands)) == 1:
                            data["cpu_brand"] = cpu_brands[0]
                        
                        # Check for shared cpu_family
                        cpu_families = [data.get(f"cpu_family{i}") for i in range(1, len(matches) + 1)]
                        cpu_families = [f for f in cpu_families if f]
                        if cpu_families and len(set(cpu_families)) == 1:
                            data["cpu_family"] = cpu_families[0]
                        
                        # Check for shared cpu_suffix
                        cpu_suffixes = [data.get(f"cpu_suffix{i}") for i in range(1, len(matches) + 1)]
                        cpu_suffixes = [s for s in cpu_suffixes if s]
                        if cpu_suffixes and len(set(cpu_suffixes)) == 1:
                            data["cpu_suffix"] = cpu_suffixes[0]
                            logger.debug(f"Added shared cpu_suffix: {cpu_suffixes[0]}")
                    else:
                        # Single match - use base keys
                        if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                            match_indices, consume_indices = matches[0]
                        else:
                            match_indices = matches[0]
                            consume_indices = match_indices
                        flat_indices = []
                        for idx in match_indices:
                            if isinstance(idx, int):
                                flat_indices.append(idx)
                                if getattr(extractor, 'consume_on_match', True):
                                    consumed.add(idx)
                        if flat_indices:
                            extracted = extractor.process_match(tokens, flat_indices)
                            data.update(extracted)
                            logger.debug(f"Added single CPU fields: {extracted}")
                            
            except ValueError as e:
                logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue

        # RAM extractors - FIXED: Use startswith to get all RAM extractors
        ram_extractors = [ext for ext in extractors if ext.name.startswith("ram_")]
        for extractor in ram_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if len(matches) > 1:
                        for i, match in enumerate(matches, start=1):
                            if isinstance(match, tuple) and len(match) == 2:
                                match_indices, consume_indices = match
                            else:
                                match_indices = match
                                consume_indices = match_indices
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                                    if getattr(extractor, 'consume_on_match', True):
                                        consumed.add(idx)
                            if flat_indices:
                                extracted = extractor.process_match(tokens, flat_indices)
                                for key, value in extracted.items():
                                    if isinstance(value, str):
                                        parts = value.split()
                                        value = " ".join(dict.fromkeys(parts))
                                    if key == "ram_size":
                                        data[f"ram_size{i}"] = value
                                    else:
                                        data[f"{key}{i}"] = value
                        if len(matches) > 0 and matches[0]:
                            if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                                match_indices, consume_indices = matches[0]
                            else:
                                match_indices = matches[0]
                                consume_indices = match_indices
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                            if flat_indices:
                                first_match = extractor.process_match(tokens, flat_indices)
                                for key, value in first_match.items():
                                    if key in ["ram_type", "ram_brand"]:
                                        parts = value.split()
                                        value = " ".join(dict.fromkeys(parts))
                                        data[key] = value
                    else:
                        if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                            match_indices, consume_indices = matches[0]
                        else:
                            match_indices = matches[0]
                            consume_indices = match_indices
                        flat_indices = []
                        for idx in match_indices:
                            if isinstance(idx, int):
                                flat_indices.append(idx)
                                if getattr(extractor, 'consume_on_match', True):
                                    consumed.add(idx)
                        if flat_indices:
                            extracted = extractor.process_match(tokens, flat_indices)
                            for key, value in extracted.items():
                                if isinstance(value, str):
                                    parts = value.split()
                                    extracted[key] = " ".join(dict.fromkeys(parts))
                            data.update(extracted)
            except ValueError as e:
                logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue

    # ENHANCED: Apply storage extractors for all device types except network devices with context awareness
    if not is_network_device:
        # Dynamic detection of any storage-related keys already extracted
        storage_already_extracted = any(
            key.startswith('storage_') or 
            re.match(r'.*storage.*\d*$', key) or
            key in ['storage_size', 'storage_capacity', 'storage_type', 'storage_status'] or
            re.match(r'storage_capacity\d+$', key)
            for key in data.keys()
        )
        
        if not storage_already_extracted:
            # Storage extractors with device context
            storage_extractors = [ext for ext in extractors if ext.name.startswith("storage_")]
            for extractor in storage_extractors:
                # FIXED: Pass device context with determined device_type from data
                # Get device_type from current data if available, otherwise from context
                current_device_type = data.get('device_type') or context.get('device_type')
                
                extractor.device_context = {
                    'is_phone_device': is_phone_device,
                    'device_type': current_device_type,  # Use the determined device type
                    'has_phone_context': context.get('has_phone_context', False),
                    'has_gpu_context': context.get('has_gpu_context', False)
                }
                
                logger.debug(f"Applying extractor: {extractor.name} with device context: {extractor.device_context}")
                try:
                    matches = extractor.extract(tokens, consumed)
                    logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                    if matches:
                        if len(matches) > 1:
                            # Multiple separate storage matches
                            for i, match in enumerate(matches, start=1):
                                if isinstance(match, tuple) and len(match) == 2:
                                    match_indices, consume_indices = match
                                else:
                                    match_indices = match
                                    consume_indices = match_indices
                                flat_indices = []
                                for idx in match_indices:
                                    if isinstance(idx, int):
                                        flat_indices.append(idx)
                                        if getattr(extractor, 'consume_on_match', True):
                                            consumed.add(idx)
                                if flat_indices:
                                    extracted = extractor.process_match(tokens, flat_indices)
                                    
                                    # ENHANCED: Check if the extractor returned multiple numbered storage capacities
                                    has_numbered_capacities = any(re.match(r'storage_capacity\d+$', key) for key in extracted.keys())
                                    
                                    if has_numbered_capacities:
                                        # The extractor already created numbered fields, use them directly
                                        data.update(extracted)
                                        logger.debug(f"Added numbered storage fields from single match: {extracted}")
                                    else:
                                        # Traditional numbered field creation
                                        for key, value in extracted.items():
                                            if isinstance(value, str):
                                                parts = value.split()
                                                value = " ".join(dict.fromkeys(parts))
                                            if key == "storage_capacity":
                                                data[f"storage_capacity{i}"] = value
                                            elif key == "storage_type":
                                                data[f"storage_type{i}"] = value
                                            else:
                                                data[f"{key}{i}"] = value
                                        logger.debug(f"Added traditional numbered storage fields: {extracted}")
                            
                            # Set shared fields for first match if no numbered capacities were found
                            if len(matches) > 0 and matches[0] and not any(re.match(r'storage_capacity\d+$', key) for key in data.keys()):
                                if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                                    match_indices, consume_indices = matches[0]
                                else:
                                    match_indices = matches[0]
                                    consume_indices = match_indices
                                flat_indices = []
                                for idx in match_indices:
                                    if isinstance(idx, int):
                                        flat_indices.append(idx)
                                if flat_indices:
                                    first_match = extractor.process_match(tokens, flat_indices)
                                    for key, value in first_match.items():
                                        if key in ["storage_type", "storage_drive_count"]:
                                            parts = value.split()
                                            value = " ".join(dict.fromkeys(parts))
                                            data[key] = value
                        else:
                            # Single storage match
                            if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                                match_indices, consume_indices = matches[0]
                            else:
                                match_indices = matches[0]
                                consume_indices = match_indices
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                                    if getattr(extractor, 'consume_on_match', True):
                                        consumed.add(idx)
                            if flat_indices:
                                extracted = extractor.process_match(tokens, flat_indices)
                                
                                # ENHANCED: Check if the extractor returned multiple numbered storage capacities
                                has_numbered_capacities = any(re.match(r'storage_capacity\d+$', key) for key in extracted.keys())
                                
                                if has_numbered_capacities:
                                    # The extractor already created numbered fields, use them directly
                                    data.update(extracted)
                                    logger.debug(f"Added numbered storage fields from single match: {extracted}")
                                else:
                                    # Traditional single field processing
                                    for key, value in extracted.items():
                                        if isinstance(value, str):
                                            parts = value.split()
                                            extracted[key] = " ".join(dict.fromkeys(parts))
                                    data.update(extracted)
                                    logger.debug(f"Added single storage fields: {extracted}")
                except ValueError as e:
                    logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                    continue
        else:
            logger.debug("Skipping storage extractors as storage data already extracted")

        # PHONE FALLBACK: If no storage capacity extracted, capture simple NUMBER + GB (or NUMBERGB)
        try:
            storage_keys_present = any(k.startswith('storage_capacity') or k in {'storage_capacity', 'storage_size', 'storage_type'} for k in data.keys())
            phone_like = is_phone_device or context.get('device_type') in ['Cell Phones & Smartphones', 'Tablets & eBook Readers']
            if phone_like and not storage_keys_present:
                for i in range(len(tokens)):
                    # Combined form like "64GB"
                    tok = tokens[i]
                    m = re.match(r'^(\d+)(gb)$', tok, re.IGNORECASE)
                    if m:
                        # Avoid if part of a slash-separated or percent sequence
                        if (i > 0 and tokens[i-1] == '/') or (i + 1 < len(tokens) and tokens[i+1] == '/'):
                            continue
                        if (i + 1 < len(tokens) and '%' in tokens[i+1]) or ('%' in tok):
                            continue
                        data['storage_capacity1'] = f"{m.group(1)}GB"
                        logger.debug(f"Phone fallback set storage_capacity1 from combined token: {data['storage_capacity1']}")
                        break
                    # Separated form like "64 GB"
                    if i + 1 < len(tokens) and re.match(r'^\d+$', tokens[i]) and re.match(r'^(gb)$', tokens[i+1], re.IGNORECASE):
                        # Avoid if part of a slash-separated or percent sequence
                        if (i > 0 and tokens[i-1] == '/') or (i + 2 < len(tokens) and tokens[i+2] == '/'):
                            continue
                        if (i + 2 < len(tokens) and '%' in tokens[i+2]):
                            continue
                        data['storage_capacity1'] = f"{tokens[i]}GB"
                        logger.debug(f"Phone fallback set storage_capacity1 from separated tokens: {data['storage_capacity1']}")
                        break
        except Exception as e:
            logger.debug(f"Phone fallback storage detection error: {e}")
    
    data['cpu_extracted'] = cpu_extracted
    return data
    
def apply_network_device_extractors(tokens: List[str], consumed: Set[int], is_network_device: bool, sanitized_title: str, logger: logging.Logger) -> Dict:
    """Apply network device specific extractors."""
    data = {}
    
    if is_network_device:
        logger.debug("Processing as network device - skipping CPU/RAM/storage extractors")
        port_match = re.search(r'(\d+)\s*(?:x\s*)?(?:Port|Ports)', sanitized_title, re.IGNORECASE)
        if port_match:
            data["switch_ports"] = port_match.group(1)
            port_indices = find_token_indices(tokens, port_match)
            for idx in port_indices:
                consumed.add(idx)
            logger.debug(f"Extracted switch_ports: {data['switch_ports']} and consumed tokens: {port_indices}")
        
        speed_match = re.search(r'(\d+(?:\.\d+)?)\s*(Gb|Gbit|Gbps|Gig|MB|Mbit|Mbps)', sanitized_title, re.IGNORECASE)
        if speed_match:
            speed = speed_match.group(1)
            unit = speed_match.group(2).lower()
            if unit.startswith('g'):
                data["switch_speed"] = f"{speed}Gbps"
            elif unit.startswith('m'):
                data["switch_speed"] = f"{speed}Mbps"
            speed_indices = find_token_indices(tokens, speed_match)
            for idx in speed_indices:
                consumed.add(idx)
            logger.debug(f"Extracted switch_speed: {data['switch_speed']} and consumed tokens: {speed_indices}")
        
        model_match = re.search(r'(WS-C\d+\w+-\d+\w+-\w+)', sanitized_title, re.IGNORECASE)
        if model_match:
            data["switch_model"] = model_match.group(1)
            model_indices = find_token_indices(tokens, model_match)
            for idx in model_indices:
                consumed.add(idx)
            logger.debug(f"Extracted switch_model: {data['switch_model']} and consumed tokens: {model_indices}")
    
    return data

def apply_priority_extractors(extractors: List, tokens: List[str], consumed: Set[int], is_network_device: bool, is_phone_device: bool, context: Dict, cpu_extracted: bool, logger: logging.Logger) -> Dict:
    """Apply priority extractors based on device type."""
    data = {}
    
    non_lot_extractors = [ext for ext in extractors if not ext.multiple and 
                          ext.name != "lot" and 
                          ext.name not in ["storage_status", "battery_status", "os_status", "cpu_model", "ram_size", "storage_capacity", "phone"]]
    
    if is_network_device:
        priority_extractors = [ext for ext in non_lot_extractors if ext.name.startswith(("switch_", "adapter_"))]
    elif is_phone_device:
        priority_extractors = []  # Phone extractor already ran
    else:
        priority_extractors = [ext for ext in non_lot_extractors if ext.name.startswith(("cpu_", "ram_", "storage_"))]
    
    for extractor in priority_extractors:
        logger.debug(f"Applying extractor: {extractor.name}")
        try:
            matches = extractor.extract(tokens, consumed)
            logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
            if matches:
                if extractor.name == "cpu_quantity" and is_network_device:
                    logger.debug(f"Skipping cpu_quantity for network device")
                    continue
                if extractor.name == "cpu_quantity" and context['is_processor_listing']:
                    skip_quantity = False
                    for idx in matches[0]:
                        i = idx
                        if i < len(tokens) and tokens[i].lower() in ["dual", "quad"]:
                            if i+1 < len(tokens) and tokens[i+1].lower() == "core":
                                skip_quantity = True
                                logger.debug(f"Skipping cpu_quantity extraction for '{tokens[i]} {tokens[i+1]}' in processor listing")
                                break
                    if skip_quantity:
                        continue
                
                if extractor.name == "cpu_quantity":
                    cpu_found = any(key.startswith("cpu_") for key in data.keys() if key != "Full Title")
                    if not cpu_found:
                        logger.debug(f"Skipping cpu_quantity as no other CPU attributes found")
                        continue
                if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                    match_indices, consume_indices = matches[0]
                else:
                    match_indices = matches[0]
                    consume_indices = match_indices
                logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                extracted = extractor.process_match(tokens, match_indices)
                if cpu_extracted and extractor.name == "cpu_family" and "cpu_family" in data:
                    logger.debug(f"Skipping cpu_family update as cpu_model already set: {data['cpu_family']}")
                    continue
                for key, value in extracted.items():
                    if isinstance(value, str):
                        parts = value.split()
                        extracted[key] = " ".join(dict.fromkeys(parts))
                data.update(extracted)
                if getattr(extractor, 'consume_on_match', True):
                    for idx in consume_indices:
                        if isinstance(idx, int):
                            consumed.add(idx)
                logger.debug(f"Extracted {extractor.name}: {extracted}")
        except ValueError as e:
            logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
            continue
        except Exception as e:
            logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
            continue
    
    return data

def filter_extractors_by_device_type(extractors: List, device_type: str) -> List:
    """Filter extractors based on device type."""
    if not device_type:
        return extractors
    
    filtered = []
    for extractor in extractors:
        # Check if extractor has device_types restriction
        if hasattr(extractor, 'device_types') and extractor.device_types:
            if device_type in extractor.device_types:
                filtered.append(extractor)
        else:
            # No restriction, include all extractors that aren't HDD-specific
            if not extractor.name.startswith('hdd_'):
                filtered.append(extractor)
    
    return filtered

def apply_remaining_extractors(extractors: List, tokens: List[str], consumed: Set[int], is_phone_device: bool, context: Dict, logger: logging.Logger) -> Dict:
    """Apply remaining non-GPU and GPU extractors."""
    data = {}
    
    # Filter extractors based on device type
    device_type = context.get('device_type', '')
    logger.debug(f"Device type for extractor filtering: {device_type}")
    
    if device_type == "Internal Hard Disk Drives":
        # For HDDs, run HDD-specific extractors first
        hdd_extractors = [ext for ext in extractors if ext.name.startswith('hdd_')]
        logger.debug(f"Found HDD extractors: {[e.name for e in hdd_extractors]}")
        
        for extractor in hdd_extractors:
            logger.debug(f"Applying HDD extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"HDD extractor {extractor.name} returned matches: {matches}")
                if matches:
                    for match in matches:
                        if isinstance(match, tuple) and len(match) == 2:
                            match_indices, consume_indices = match
                        else:
                            match_indices = match
                            consume_indices = match_indices
                        logger.debug(f"Processing HDD tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                        extracted = extractor.process_match(tokens, match_indices)
                        data.update(extracted)
                        if getattr(extractor, 'consume_on_match', True):
                            for idx in consume_indices:
                                if isinstance(idx, int):
                                    consumed.add(idx)
                        logger.debug(f"Extracted HDD {extractor.name}: {extracted}")
            except Exception as e:
                logger.error(f"Error in HDD extractor {extractor.name}: {str(e)}")
        
        # For HDDs, only allow basic extractors (no screen, cpu, ram, etc.)
        allowed_extractors = [ext for ext in extractors if 
                             ext.name in ['brand', 'model', 'device_type'] or
                             ext.name.startswith('hdd_')]
        extractors = allowed_extractors
        logger.debug(f"Filtered to HDD-compatible extractors: {[e.name for e in extractors]}")
    
    non_lot_extractors = [ext for ext in extractors if not ext.multiple and 
                          ext.name != "lot" and 
                          ext.name not in ["storage_status", "battery_status", "os_status", "cpu_model", "ram_size", "storage_capacity", "phone"]]
    
    remaining_extractors = [ext for ext in non_lot_extractors if not ext.name.startswith(("cpu_", "ram_", "storage_", "switch_", "adapter_")) and not ext.multiple]
    
    # Exclude screen extractors for HDDs to prevent form factor being detected as screen size
    if device_type == "Internal Hard Disk Drives":
        remaining_extractors = [ext for ext in remaining_extractors if not ext.name.startswith("screen_")]
        logger.debug("Excluded screen extractors for HDD device type")
    
    # Apply GPU extractors only if GPU context is detected (and not phone device)
    gpu_extractors = [ext for ext in remaining_extractors if ext.name.startswith("gpu")]
    non_gpu_extractors = [ext for ext in remaining_extractors if not ext.name.startswith("gpu")]
    
    # Apply non-GPU extractors first (skip if phone device)
    if not is_phone_device:
        for extractor in non_gpu_extractors:
            # Skip HDD extractors here since we already ran them above
            if device_type == "Internal Hard Disk Drives" and extractor.name.startswith('hdd_'):
                continue
                
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                        match_indices, consume_indices = matches[0]
                    else:
                        match_indices = matches[0]
                        consume_indices = match_indices
                    logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    for key, value in extracted.items():
                        if isinstance(value, str):
                            parts = value.split()
                            extracted[key] = " ".join(dict.fromkeys(parts))
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted {extractor.name}: {extracted}")
            except ValueError as e:
                logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
    
    # Apply GPU extractors only if GPU context is detected (and not phone device)
    if context['has_gpu_context'] and not is_phone_device:
        logger.debug("Applying GPU extractors (GPU context detected)")
        for extractor in gpu_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                        match_indices, consume_indices = matches[0]
                    else:
                        match_indices = matches[0]
                        consume_indices = match_indices
                    logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    for key, value in extracted.items():
                        if isinstance(value, str):
                            parts = value.split()
                            extracted[key] = " ".join(dict.fromkeys(parts))
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted {extractor.name}: {extracted}")
            except ValueError as e:
                logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
    else:
        logger.debug("Skipping GPU extractors (no GPU context detected or phone device)")
    
    return data
    
def apply_multiple_extractors(extractors: List, tokens: List[str], consumed: Set[int], is_network_device: bool, is_phone_device: bool, accumulated_data: Dict, logger: logging.Logger) -> Dict:
    """Apply multiple extractors for complex components."""
    data = {}
    
    logger.debug(f"apply_multiple_extractors: is_network_device={is_network_device}, is_phone_device={is_phone_device}")
    
    if not is_network_device and not is_phone_device:
        multiple_extractors = [ext for ext in extractors if ext.multiple and 
                              ext.name not in ["storage_status", "battery_status", "os_status", "cpu_model", 
                                               "ram_size", "storage_capacity"]]
        
        logger.debug(f"Multiple extractors found: {[ext.name for ext in multiple_extractors]}")
        
        for extractor in multiple_extractors:
            logger.debug(f"Applying extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} (multiple) returned matches: {matches}")
                if matches:
                    # FIXED: Special handling for CPU generation when there are multiple CPUs
                    if extractor.name == "cpu_generation":
                        # Check if we have multiple CPUs by looking for numbered CPU fields in accumulated_data
                        has_multiple_cpus = any(re.match(r'cpu_\w+\d+$', key) for key in accumulated_data.keys())
                        
                        logger.debug(f"CPU generation extractor: has_multiple_cpus = {has_multiple_cpus}, matches = {len(matches)}")
                        
                        # If we have multiple CPUs, always use numbered keys for generations
                        if has_multiple_cpus or len(matches) > 1:
                            for i, match in enumerate(matches, start=1):
                                if isinstance(match, tuple) and len(match) == 2:
                                    match_indices, consume_indices = match
                                else:
                                    match_indices = match
                                    consume_indices = match_indices
                                logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                                flat_indices = []
                                for idx in match_indices:
                                    if isinstance(idx, int):
                                        flat_indices.append(idx)
                                        if getattr(extractor, 'consume_on_match', True):
                                            consumed.add(idx)
                                if flat_indices:
                                    extracted = extractor.process_match(tokens, flat_indices)
                                    for key, value in extracted.items():
                                        if isinstance(value, str):
                                            parts = value.split()
                                            extracted[key] = " ".join(dict.fromkeys(parts))
                                        # Always use numbered keys for CPU generations when multiple CPUs exist
                                        data[f"{key}{i}"] = value
                                    logger.debug(f"Extracted {extractor.name} match {i}: {extracted}")
                        else:
                            # Single CPU case - use base keys
                            match = matches[0]
                            if isinstance(match, tuple) and len(match) == 2:
                                match_indices, consume_indices = match
                            else:
                                match_indices = match
                                consume_indices = match_indices
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                                    if getattr(extractor, 'consume_on_match', True):
                                        consumed.add(idx)
                            if flat_indices:
                                extracted = extractor.process_match(tokens, flat_indices)
                                for key, value in extracted.items():
                                    if isinstance(value, str):
                                        parts = value.split()
                                        extracted[key] = " ".join(dict.fromkeys(parts))
                                data.update(extracted)
                                logger.debug(f"Extracted single {extractor.name}: {extracted}")
                    else:
                        # Original logic for other extractors
                        for i, match in enumerate(matches, start=1):
                            if isinstance(match, tuple) and len(match) == 2:
                                match_indices, consume_indices = match
                            else:
                                match_indices = match
                                consume_indices = match_indices
                            logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                            flat_indices = []
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    flat_indices.append(idx)
                                    if getattr(extractor, 'consume_on_match', True):
                                        consumed.add(idx)
                            if flat_indices:
                                extracted = extractor.process_match(tokens, flat_indices)
                                has_numbered_keys = any(re.match(rf'{extractor.name}\d+', key) for key in extracted)
                                for key, value in extracted.items():
                                    if isinstance(value, str):
                                        parts = value.split()
                                        extracted[key] = " ".join(dict.fromkeys(parts))
                                if has_numbered_keys:
                                    data.update(extracted)
                                else:
                                    if i == 1:
                                        data.update(extracted)
                                    else:
                                        for key, value in extracted.items():
                                            data[f"{key}{i}"] = value
                                logger.debug(f"Extracted {extractor.name} match {i}: {extracted}")
            except ValueError as e:
                logger.warning(f"Unpacking error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in {extractor.name}: {str(e)}. Skipping extractor.")
                continue
    
    return data
    
def _is_cpu_quantity_pattern(token: str) -> bool:
    """Check if a token matches a CPU quantity pattern that should be left for CPU extractor."""
    quantity_patterns = [
        r'^\d+x$',           # 2x, 4x
        r'^x\d+$',           # x2, x4
        r'^\(\d+x\)$',       # (2x)
        r'^\(x\d+\)$',       # (x2)
        r'^\(\d+\)$',        # (2)
    ]
    return any(re.match(pattern, token, re.IGNORECASE) for pattern in quantity_patterns)

def _is_component_token(token: str) -> bool:
    """Check if a token should be left for component extractors."""
    token_lower = token.lower()
    
    # CPU speed patterns
    if re.match(r'@?\d+\.?\d*[gm]hz$', token_lower):
        return True
    
    # Memory size patterns (including ranges like 16GB-32GB)
    if re.match(r'\d+[gt]b$', token_lower) or re.match(r'\d+[gt]b-\d+[gt]b$', token_lower):
        return True
    
    # Storage size patterns (including ranges like 500GB-1TB)
    if re.match(r'\d+[gt]b-\d+[gt]b$', token_lower):
        return True
    
    # Component keywords - UPDATED to include more CPU types
    component_keywords = ['intel', 'amd', 'apple', 'core', 'cpu', 'processor', 'xeon', 'ram', 'memory', 'ssd', 'hdd', 'nvme', 'emmc', 'pentium', 'celeron', 'atom', 'athlon', 'ryzen']
    if token_lower in component_keywords:
        return True
    
    # CPU model patterns - UPDATED to include more patterns
    if re.match(r'i[3579]-\d{3,4}[a-z]*$', token_lower):  # Intel Core i3/i5/i7/i9
        return True
    
    # CPU family patterns (standalone i3, i5, i7, i9) - NEW
    if re.match(r'i[3579]$', token_lower):
        return True
    
    # Intel m-series patterns (like m3-8100Y)
    if re.match(r'm[3579]-\d{4}[a-z]*$', token_lower):
        return True
    
    # Apple M-series patterns
    if re.match(r'm[123](?:\s+(?:pro|max|ultra))?$', token_lower):
        return True
    
    # Intel Pentium/Celeron patterns (like 4425Y) - FIXED: More restrictive pattern
    if re.match(r'^[3-6]\d{3}[a-z]$', token_lower):  # 4-digit number starting with 3-6, single letter suffix
        return True
    
    # Memory configurations like 8GBRAM, 4GBRAM
    if re.match(r'\d+gb\w*$', token_lower):
        return True
    
    # Storage configurations like 256GBSSD, 64GBSSD  
    if re.match(r'\d+gb\w+$', token_lower):
        return True
    
    # Generation patterns
    if re.match(r'\d+(?:st|nd|rd|th)$', token_lower):
        return True
    
    return False
    
def _is_storage_or_memory_range(token: str) -> bool:
    """Check if token is a storage or memory range that should be left for extractors."""
    token_lower = token.lower()
    # Patterns like 500GB-1TB, 16GB-32GB, etc.
    return bool(re.match(r'\d+[gmtk]b-\d+[gmtk]b$', token_lower))

def _get_brand_lists():
    """Get all brand lists and hierarchies."""
    try:
        from configs.brand_model_types import brand_model_types
        all_known_brands = list(brand_model_types.keys())
    except ImportError:
        all_known_brands = []
    
    # Add known_brands as fallback - UPDATED to include printer brands
    additional_brands = ["Dell", "HP", "Lenovo", "Apple", "Acer", "Asus", "Toshiba", "Samsung", "Microsoft", "Sony", "IBM", "Gateway", "Compaq", "Fujitsu", "Panasonic", "LG", "MSI", "Razer", "Alienware", "Lexmark", "Canon", "Brother", "Epson", "Xerox", "Kyocera", "Ricoh", "Sharp", "Konica", "Minolta", "OKI", "OKIData", "SK hynix", "hynix",]
    accessory_brands = ["iKey", "Logitech", "Microsoft", "Apple", "Zagg", "Belkin", "Targus", "Case-Mate"]
    all_known_brands.extend([b for b in additional_brands if b not in all_known_brands])
    
    # Define brand hierarchy for prioritization
    parent_brands = ["Lenovo", "Dell", "HP", "Apple", "Microsoft", "Asus", "Acer", "Samsung", "LG", "MSI", "Razer", "Supermicro", "Cisco", "Juniper", "HPE", "IBM", "Lexmark", "Canon", "Brother", "Epson", "Xerox"]
    sub_brands = ["ThinkPad", "Alienware", "OptiPlex", "Latitude", "XPS", "Inspiron", "EliteBook", "ProBook", "MacBook", "Surface"]
    
    # Graphics card brands (HIGH PRIORITY for GPU context)
    gpu_brands = ["EVGA", "GIGABYTE", "MSI", "ASUS", "ZOTAC", "Sapphire", "XFX", "PowerColor", "PNY", "ASRock", "VisionTek", "Gainward", "Palit", "Inno3D", "Colorful", "KFA2", "Galax", "Yeston", "Biostar", "NVIDIA", "AMD", "Intel"]
    
    # Common prefixes to ignore when detecting brand
    ignore_prefixes = ["NEW", "USED", "REFURBISHED", "OPEN", "BOX", "SEALED", "GENUINE", "ORIGINAL", "OEM", "RETAIL", "BULK", "LOT", "QTY", "QUANTITY", "#", "WORKING", "TESTED", "UNTESTED", "AND", "THE", "FOR", "WITH", "FROM"]
    
    return {
        'all_known_brands': all_known_brands,
        'parent_brands': parent_brands,
        'sub_brands': sub_brands,
        'gpu_brands': gpu_brands,
        'ignore_prefixes': ignore_prefixes
    }

def _detect_gpu_brand(tokens: List[str], consumed: Set[int], context: Dict[str, bool], brand_lists: Dict, logger: logging.Logger) -> Tuple[str, int]:
    """Detect GPU brand if GPU context is present."""
    if not context.get('has_gpu_context', False):
        return None, -1
    
    logger.debug("GPU context detected - prioritizing graphics card brand detection")
    for i, token in enumerate(tokens):
        if token.upper() not in brand_lists['ignore_prefixes']:
            # Check GPU brands first - look in all tokens, even consumed ones
            for brand in brand_lists['gpu_brands']:
                if token.lower() == brand.lower():
                    # Only consume if not already consumed
                    if i not in consumed:
                        consumed.add(i)
                    logger.debug(f"Detected GPU brand: {brand}")
                    return brand, i
    return None, -1

def _detect_parent_brands(tokens: List[str], consumed: Set[int], context: Dict[str, bool], brand_lists: Dict, logger: logging.Logger) -> Tuple[str, int]:
    """Detect parent brands."""
    for i, token in enumerate(tokens):
        if i not in consumed and token.upper() not in brand_lists['ignore_prefixes']:
            # Skip if this appears to be a model name being used in compatibility context
            if i > 0 and tokens[i-1].lower() in ["for", "compatible", "fits"]:
                continue
                
            # Check parent brands first
            for brand in brand_lists['parent_brands']:
                if token.lower() == brand.lower():
                    # Skip CPU brands for system detection unless it's clearly a processor listing
                    if not context.get('is_processor_listing', False) or token.lower() not in ["intel", "amd"]:
                        consumed.add(i)
                        logger.debug(f"Detected parent brand: {brand}")
                        return brand, i
            
            # Check all known brands but skip model names in compatibility context
            for brand in brand_lists['all_known_brands']:
                if token.lower() == brand.lower():
                    # Additional check for model names that shouldn't be brands
                    if token.lower() in ["latitude", "inspiron", "optiplex", "thinkpad"] and i > 0:
                        if tokens[i-1].lower() in ["for", "compatible", "fits"]:
                            continue
                    
                    if not context.get('is_processor_listing', False) or token.lower() not in ["intel", "amd"]:
                        consumed.add(i)
                        logger.debug(f"Detected brand from known brands: {brand}")
                        return brand, i
    return None, -1
    
def _detect_sub_brands(tokens: List[str], consumed: Set[int], context: Dict[str, bool], brand_lists: Dict, logger: logging.Logger) -> Tuple[str, int, int]:
    """Detect sub-brands and return brand, brand_index, macbook_index."""
    for i, token in enumerate(tokens):
        if i not in consumed and token.upper() not in brand_lists['ignore_prefixes']:
            for sub_brand in brand_lists['sub_brands']:
                if token.lower() == sub_brand.lower():
                    # For sub-brands, check if the parent brand exists nearby
                    if token.lower() == "thinkpad":
                        # Look for "Lenovo" in nearby tokens
                        for j in range(max(0, i-3), min(len(tokens), i+3)):
                            if j < len(tokens) and tokens[j].lower() == "lenovo":
                                consumed.add(j)
                                logger.debug(f"Detected Lenovo brand from ThinkPad context")
                                return "Lenovo", j, -1
                    elif token.lower() == "macbook":
                        # Map MacBook to Apple brand but don't consume MacBook yet
                        # Don't consume the MacBook token - we need it for the model
                        logger.debug(f"Detected Apple brand from MacBook context at index {i}")
                        return "Apple", i, i  # brand_index and macbook_index are the same
                    
                    # If no parent found nearby, use the sub-brand as fallback
                    if not context.get('is_processor_listing', False) or token.lower() not in ["intel", "amd"]:
                        consumed.add(i)
                        logger.debug(f"Detected sub-brand: {sub_brand}")
                        return sub_brand, i, -1
    return None, -1, -1

def _detect_phone_brands(tokens: List[str], consumed: Set[int], context: Dict[str, bool], logger: logging.Logger) -> Tuple[str, int]:
    """Detect brands from phone context."""
    if not context.get('has_phone_context', False):
        return None, -1
    
    for i, token in enumerate(tokens):
        if i not in consumed and token.lower() in ['iphone', 'ipad']:
            consumed.add(i)
            logger.debug(f"Set brand to Apple based on {token} in phone context")
            return "Apple", i
    return None, -1

def _detect_fallback_brand(tokens: List[str], consumed: Set[int], context: Dict[str, bool], brand_lists: Dict, logger: logging.Logger) -> Tuple[str, int]:
    """Detect brand using fallback logic."""
    # Check if this might be a lot first
    has_lot_indicators = any(token.lower() in ['lot', 'lots', 'qty', 'quantity', 'x', 'units'] for token in tokens)
    if has_lot_indicators:
        return None, -1
    
    unconsumed = [i for i in range(len(tokens)) if i not in consumed and tokens[i].upper() not in brand_lists['ignore_prefixes']]
    
    # For GPU context, exclude GPU-related tokens from fallback brand selection
    if context.get('has_gpu_context', False):
        gpu_related_tokens = ['pcie', 'gddr5', 'gddr6', 'ddr3', 'ddr4', 'graphics', 'card', 'video', 'display', 'displayport', 'hdmi', 'dvi', 'vga', 'profile', 'high', 'low', 'mini', 'port', 'ports']
        unconsumed = [i for i in unconsumed if tokens[i].lower() not in gpu_related_tokens]
        logger.debug(f"Filtered out GPU-related tokens for brand fallback in GPU context")
    
    # Additional filtering for common non-brand words
    common_words = ['and', 'or', 'but', 'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'black', 'white', 'red', 'blue', 'green', 'yellow', 'program', 'unit', 'return', 'genuine']
    unconsumed = [i for i in unconsumed if tokens[i].lower() not in common_words]
    
    if unconsumed:
        system_brand_index = min(unconsumed)
        system_brand = tokens[system_brand_index]
        consumed.add(system_brand_index)
        logger.debug(f"Used fallback brand: {system_brand}")
        return system_brand, system_brand_index
    
    return None, -1

def _extract_gpu_model(tokens: List[str], consumed: Set[int], system_brand: str, system_brand_index: int, context: Dict[str, bool], logger: logging.Logger) -> str:
    """Extract GPU model for GPU context."""
    if not context.get('has_gpu_context', False) or system_brand.lower() not in ['nvidia', 'amd', 'intel']:
        return None
    
    logger.debug("GPU context detected - looking for GPU model patterns")
    model_tokens = []
    
    # Look for GPU series/model patterns after the brand
    current_index = system_brand_index + 1
    gpu_model_patterns = [
        r'(quadro|geforce|radeon|arc|iris|uhd)',  # GPU series
        r'(gtx|rtx|rx|r9|r7|r5)',                # GPU families
        r'([a-z]?\d{3,4}[a-z]*)',               # Model numbers like P620, GTX1080, RX580
    ]
    
    # Try to capture GPU series and model
    while current_index < len(tokens):
        token = tokens[current_index]
        token_lower = token.lower()
        
        # Stop at obvious non-GPU tokens
        if token_lower in ['pcie', 'gddr5', 'gddr6', '2gb', '4gb', '8gb', '16gb', 'graphics', 'card', 'video']:
            break
        
        # Check if this looks like a GPU model component
        is_gpu_component = any(re.match(pattern, token_lower) for pattern in gpu_model_patterns)
        
        if is_gpu_component or current_index == system_brand_index + 1:  # Always take the first token after brand
            model_tokens.append(token)
            consumed.add(current_index)
            logger.debug(f"Added GPU model token: {token}")
        else:
            break
        
        current_index += 1
    
    if model_tokens:
        model = " ".join(model_tokens)
        logger.debug(f"Built GPU model: {model}")
        return model
    
    return None

def has_apple_model_ahead(tokens: List[str], start_index: int) -> int:
    """Scan ahead to find Apple model number (A####). Returns index if found, -1 if not."""
    for i in range(start_index, min(len(tokens), start_index + 8)):  # Look ahead max 8 tokens
        if re.match(r'^A\d{4}$', tokens[i]):
            return i
    return -1

def is_storage_pattern(tokens: List[str], index: int, logger) -> bool:
    """Check if current position starts a storage pattern using storage extractor logic."""
    if index >= len(tokens):
        return False
    
    # Pattern 1: Single token with slash like "64/256GB"
    if re.search(r'^\d+/\d+(gb|tb)$', tokens[index].lower(), re.IGNORECASE):
        logger.debug(f"Found single-token storage pattern at {index}: {tokens[index]}")
        return True
    
    # Pattern 2: "64 / 256GB" - number slash number+unit
    if (index + 2 < len(tokens) and 
        tokens[index].isdigit() and 
        tokens[index + 1] == '/' and 
        re.search(r'^\d+(gb|tb)$', tokens[index + 2].lower(), re.IGNORECASE)):
        logger.debug(f"Found 3-token storage pattern at {index}: {tokens[index]} {tokens[index+1]} {tokens[index+2]}")
        return True
    
    # Pattern 3: "64 / 256 GB" - number slash number unit
    if (index + 3 < len(tokens) and 
        tokens[index].isdigit() and 
        tokens[index + 1] == '/' and 
        tokens[index + 2].isdigit() and
        tokens[index + 3].lower() in ['gb', 'tb']):
        logger.debug(f"Found 4-token storage pattern at {index}: {tokens[index]} {tokens[index+1]} {tokens[index+2]} {tokens[index+3]}")
        return True
    
    # Pattern 4: Check if we're in the middle of a long slash-separated sequence
    # Look backwards to see if we're part of a number/slash pattern
    if tokens[index].isdigit() and index > 0:
        # Check if we're in a sequence like: number / number / number / ...
        sequence_start = index
        
        # Walk backwards to find the start of the sequence
        while (sequence_start > 1 and 
               tokens[sequence_start - 1] == '/' and 
               tokens[sequence_start - 2].isdigit()):
            sequence_start -= 2
        
        # If we found a sequence start different from current position, check if it's a storage pattern
        if sequence_start < index:
            # Count numbers in the sequence from start
            numbers_found = 0
            scan_index = sequence_start
            
            while (scan_index < len(tokens) and 
                   tokens[scan_index].isdigit()):
                numbers_found += 1
                scan_index += 1
                # Skip slash if present
                if scan_index < len(tokens) and tokens[scan_index] == '/':
                    scan_index += 1
            
            # Check if followed by storage unit or if any number has unit attached
            has_storage_unit = (scan_index < len(tokens) and 
                               tokens[scan_index].lower() in ['gb', 'tb', 'mb'])
            
            has_attached_unit = any(re.search(r'^\d+(gb|tb|mb)$', tokens[i].lower(), re.IGNORECASE) 
                                   for i in range(sequence_start, min(len(tokens), scan_index + 1)))
            
            if numbers_found >= 2 and (has_storage_unit or has_attached_unit):
                logger.debug(f"Found we're in middle of long storage sequence starting at {sequence_start}, current position {index}: {numbers_found} numbers")
                return True
    
    # Pattern 5: IMPROVED Long slash-separated sequence detection
    if tokens[index].isdigit() and index + 1 < len(tokens) and tokens[index + 1] == '/':
        # Look ahead to count numbers in the sequence
        numbers_found = 0
        scan_index = index
        has_attached_unit = False
        
        # Count all numbers in the slash-separated sequence
        while scan_index < len(tokens):
            current_token = tokens[scan_index]
            
            # Check if it's a number
            if current_token.isdigit():
                numbers_found += 1
                scan_index += 1
                
                # Check if next token is a slash (more numbers follow)
                if scan_index < len(tokens) and tokens[scan_index] == '/':
                    scan_index += 1  # Skip the slash
                    continue
                else:
                    # No more slashes, check if followed by storage unit
                    if (scan_index < len(tokens) and 
                        tokens[scan_index].lower() in ['gb', 'tb', 'mb']):
                        # Found unit after last number
                        break
                    else:
                        # No unit after last number, sequence ends here
                        break
            
            # Check if it's a number with unit attached (like "256GB")
            elif re.search(r'^\d+(gb|tb|mb)$', current_token.lower(), re.IGNORECASE):
                numbers_found += 1
                has_attached_unit = True
                scan_index += 1
                break  # Unit attached means end of sequence
            
            else:
                # Not a number or number+unit, stop scanning
                break
        
        # Check if we found a valid storage sequence
        has_storage_unit = (scan_index < len(tokens) and 
                           tokens[scan_index].lower() in ['gb', 'tb', 'mb'])
        
        # Also check for attached units in the scanned range
        if not has_attached_unit:
            has_attached_unit = any(re.search(r'^\d+(gb|tb|mb)$', tokens[i].lower(), re.IGNORECASE) 
                                   for i in range(index, scan_index))
        
        # If we have multiple numbers and a storage unit, it's a storage pattern
        if numbers_found >= 2 and (has_storage_unit or has_attached_unit):
            logger.debug(f"Found long slash-separated storage pattern starting at {index}: {numbers_found} numbers, has_unit={has_storage_unit or has_attached_unit}")
            return True
    
    # Pattern 6: Standalone storage sizes (like "256GB") - use size-based heuristics
    if re.search(r'^\d+(gb|tb)$', tokens[index].lower(), re.IGNORECASE):
        size_match = re.search(r'^(\d+)(gb|tb)$', tokens[index].lower(), re.IGNORECASE)
        if size_match:
            size_value = int(size_match.group(1))
            size_unit = size_match.group(2).lower()
            # Use storage extractor logic: TB values or large GB values are likely storage
            if size_unit == 'tb' or (size_unit == 'gb' and size_value >= 128):
                logger.debug(f"Found standalone storage size at {index}: {tokens[index]}")
                return True
            # For smaller GB values, check context
            elif size_unit == 'gb' and size_value >= 8:  # LOWERED from 32 to 8 for tablets/phones
                # Check for RAM context nearby
                has_ram_context = False
                for j in range(max(0, index-2), min(len(tokens), index+3)):
                    if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                        has_ram_context = True
                        break
                if not has_ram_context:
                    logger.debug(f"Found likely storage size at {index}: {tokens[index]} (no RAM context)")
                    return True
    
    # Pattern 7: Number that might start a storage pattern
    if (tokens[index].isdigit() and 
        index + 1 < len(tokens) and 
        tokens[index + 1] == '/'):
        # Use storage extractor logic: check if the number is a reasonable storage size
        num_value = int(tokens[index])
        # LOWERED threshold from 32 to 8 for tablets/phones, and removed power-of-2 requirement
        if num_value >= 8:  # Accept any storage size 8GB and up
            logger.debug(f"Found potential storage number at {index}: {tokens[index]} (followed by slash)")
            return True
    
    return False
    
def _extract_accessory_model(tokens: List[str], consumed: Set[int], system_brand_index: int, logger: logging.Logger) -> str:
    """Extract accessory model, stopping at compatibility indicators."""
    model_tokens = []
    current_index = system_brand_index + 1
    
    while current_index < len(tokens) and current_index not in consumed:
        token = tokens[current_index]
        token_lower = token.lower()
        
        # Stop at compatibility indicators
        if token_lower in ["for", "compatible", "fits", "attachable", "keyboard", "case", "cover"]:
            break
            
        # Stop at brand names that indicate compatibility (Dell, HP, etc.)
        compatibility_brands = ["dell", "hp", "lenovo", "apple", "asus", "acer", "samsung", "microsoft"]
        if token_lower in compatibility_brands:
            break
            
        # Include model numbers and descriptive terms
        model_tokens.append(token)
        consumed.add(current_index)
        current_index += 1
    
    if model_tokens:
        model = " ".join(model_tokens)
        logger.debug(f"Built accessory model: {model}")
        return model
    
    return None
    
def _extract_apple_model(tokens: List[str], consumed: Set[int], system_brand_index: int, macbook_index: int, logger: logging.Logger) -> str:
    """Extract Apple model."""
    logger.debug(f"Processing Apple model detection from index {system_brand_index}")
    model_tokens = []
    
    # If we detected Apple from MacBook, start with MacBook
    if macbook_index != -1:
        logger.debug(f"Starting Apple model with MacBook at index {macbook_index}")
        model_tokens.append(tokens[macbook_index])  # Add "MacBook"
        consumed.add(macbook_index)
        current_index = macbook_index + 1
    else:
        # Check if there's a MacBook token before the Apple brand detection
        macbook_search_index = -1
        for search_i in range(system_brand_index):
            if search_i not in consumed and tokens[search_i].lower() == "macbook":
                macbook_search_index = search_i
                break
        
        if macbook_search_index != -1:
            # Found MacBook before Apple token, start from MacBook
            logger.debug(f"Found MacBook at index {macbook_search_index} before Apple brand")
            model_tokens.append(tokens[macbook_search_index])  # Add "MacBook"
            consumed.add(macbook_search_index)
            current_index = macbook_search_index + 1
        else:
            # Regular Apple detection, start after the Apple token
            current_index = system_brand_index + 1
    
    def detect_storage_sequence_ahead(start_idx):
        """Look ahead to detect if this is the start of a storage sequence like 16/32/64/128/256"""
        if start_idx >= len(tokens):
            return False
        
        # Check if current token is a number
        if not tokens[start_idx].isdigit():
            return False
        
        # ENHANCED: Check for storage capacity sizes that indicate this is storage, not model
        current_num = int(tokens[start_idx])
        # Common storage sizes that shouldn't be in Apple model names
        storage_size_indicators = [16, 32, 64, 128, 256, 512, 1024, 2048]
        
        if current_num in storage_size_indicators:
            # Look ahead to see if this is part of a storage sequence
            # Check for pattern: number / number / number (at least 2 slashes)
            slash_count = 0
            scan_idx = start_idx + 1
            numbers_in_sequence = [current_num]
            
            while scan_idx < len(tokens) and slash_count < 6:  # Check up to 6 slashes for long sequences
                if scan_idx < len(tokens) and tokens[scan_idx] == '/':
                    slash_count += 1
                    scan_idx += 1
                    # Next should be a number or number+unit
                    if scan_idx < len(tokens):
                        next_token = tokens[scan_idx]
                        if next_token.isdigit():
                            numbers_in_sequence.append(int(next_token))
                            scan_idx += 1
                            continue
                        elif re.match(r'^\d+(gb|tb|mb)$', next_token, re.IGNORECASE):
                            # Number with unit attached
                            num_match = re.match(r'^(\d+)', next_token, re.IGNORECASE)
                            if num_match:
                                numbers_in_sequence.append(int(num_match.group(1)))
                            scan_idx += 1
                            break  # Unit attached means end of sequence
                        else:
                            break
                else:
                    # Check if next token is a storage unit
                    if (scan_idx < len(tokens) and 
                        tokens[scan_idx].lower() in ['gb', 'tb', 'mb']):
                        break  # Found unit, end of sequence
                    
            # Enhanced detection criteria:
            # 1. At least 2 numbers in sequence, OR
            # 2. Single number followed by storage unit, OR  
            # 3. Multiple storage-sized numbers in sequence
            has_storage_unit = (scan_idx < len(tokens) and 
                               tokens[scan_idx].lower() in ['gb', 'tb', 'mb'])
            
            multiple_storage_numbers = len([n for n in numbers_in_sequence if n in storage_size_indicators]) >= 2
            
            if (slash_count >= 1 and has_storage_unit) or multiple_storage_numbers:
                logger.debug(f"Detected storage sequence starting at {start_idx}: numbers={numbers_in_sequence}, slashes={slash_count}, has_unit={has_storage_unit}")
                return True
        
        # Original logic as fallback
        slash_count = 0
        scan_idx = start_idx + 1
        
        while scan_idx < len(tokens) and slash_count < 5:  # Check up to 5 slashes
            if scan_idx < len(tokens) and tokens[scan_idx] == '/':
                slash_count += 1
                scan_idx += 1
                # Next should be a number or number+unit
                if scan_idx < len(tokens):
                    next_token = tokens[scan_idx]
                    if next_token.isdigit() or re.match(r'^\d+(gb|tb|mb)$', next_token, re.IGNORECASE):
                        scan_idx += 1
                        continue
                    else:
                        break
            else:
                break
        
        # If we found at least 2 slashes, it's likely a storage sequence
        if slash_count >= 2:
            logger.debug(f"Detected storage sequence starting at {start_idx} with {slash_count} slashes")
            return True
        
        # Also check for single number followed by GB/TB unit nearby
        if (start_idx + 1 < len(tokens) and 
            re.match(r'^(gb|tb|mb)$', tokens[start_idx + 1], re.IGNORECASE)):
            logger.debug(f"Detected storage capacity at {start_idx}: {tokens[start_idx]}{tokens[start_idx + 1]}")
            return True
        
        return False
    
    # Continue building the model
    while current_index < len(tokens):
        token = tokens[current_index]
        token_lower = token.lower()
        
        # ENHANCED PRIORITY CHECK: Look ahead for storage sequences before processing any token
        if detect_storage_sequence_ahead(current_index):
            logger.debug(f"Stopping Apple model extraction - detected storage sequence starting at {current_index}: {token}")
            break
        
        # PRIORITY CHECK: Stop at storage patterns - CHECK THIS FIRST
        if is_storage_pattern(tokens, current_index, logger):
            logger.debug(f"Stopping Apple model extraction at storage pattern starting at {current_index}: {token}")
            break
        
        # Check if this is an Apple model number - if so, add it and continue (don't stop)
        if re.match(r'^A\d{4}$', token):
            model_tokens.append(token)
            consumed.add(current_index)
            logger.debug(f"Added Apple model number: {token}")
            current_index += 1
            continue
        
        # Check if there's an Apple model number ahead before stopping
        apple_model_index = has_apple_model_ahead(tokens, current_index)
        
        # Stop at component tokens that should be left for extractors
        if _is_component_token(token):
            # For Apple devices, if there's an Apple model number ahead, skip over CPU indicators
            if apple_model_index != -1:
                logger.debug(f"Found Apple model number ahead at index {apple_model_index}, continuing past component token: {token}")
                current_index += 1
                continue
            else:
                logger.debug(f"Stopping Apple model extraction at component token: {token}")
                break
        
        # Stop at CPU quantity patterns - leave these for CPU extractor
        if _is_cpu_quantity_pattern(token):
            if apple_model_index != -1:
                logger.debug(f"Found Apple model number ahead at index {apple_model_index}, continuing past CPU quantity pattern: {token}")
                current_index += 1
                continue
            else:
                logger.debug(f"Stopping Apple model extraction at CPU quantity pattern: {token}")
                break
        
        # Stop at forward slash that might be storage separator
        if token == "/":
            # ENHANCED: Check if this slash is part of a storage pattern
            if current_index > 0 and tokens[current_index - 1].isdigit():
                # Previous token was a number, this slash might be part of storage pattern
                prev_num = int(tokens[current_index - 1])
                storage_indicators = [16, 32, 64, 128, 256, 512, 1024, 2048]
                
                if prev_num in storage_indicators:
                    # Look ahead to see if next token is also a storage-sized number
                    if (current_index + 1 < len(tokens) and 
                        tokens[current_index + 1].isdigit() and
                        int(tokens[current_index + 1]) in storage_indicators):
                        logger.debug(f"Stopping Apple model extraction at storage slash separator: {token} (prev: {prev_num}, next: {tokens[current_index + 1]})")
                        break
                    # Also check for GB/TB unit after slash
                    elif (current_index + 1 < len(tokens) and 
                          tokens[current_index + 1].lower() in ['gb', 'tb', 'mb']):
                        logger.debug(f"Stopping Apple model extraction at storage slash with unit: {token}")
                        break
            
            # For Apple model slashes (like iPad mini/Air/Pro), continue
            logger.debug(f"Including slash in Apple model: {token}")
            model_tokens.append(token)
            consumed.add(current_index)
            current_index += 1
            continue
        
        # Stop at storage/memory ranges that should be extracted separately
        if _is_storage_or_memory_range(token):
            logger.debug(f"Stopping Apple model extraction at storage/memory range: {token}")
            break
        
        # Include Apple model components with tighter patterns
        is_valid_year = re.match(r'^\d{4}$', token) and 1990 <= int(token) <= 2030
        is_screen_size = re.match(r'^\d+(?:\.\d+)?(?:["\′]|in)$', token)  # Must end with quote mark or "in"
        is_apple_variant = token_lower in ['early', 'mid', 'late', 'pro', 'air', 'mini', 'max', 'ultra', 'studio', 'mac', 'macbook', 'ipad']
        
        # ENHANCED: Don't include standalone numbers that look like storage capacities
        is_storage_sized_number = (token.isdigit() and 
                                  int(token) in [16, 32, 64, 128, 256, 512, 1024, 2048])
        
        if (is_valid_year or is_screen_size or is_apple_variant) and not is_storage_sized_number:
            model_tokens.append(token)
            consumed.add(current_index)
            logger.debug(f"Added Apple model token: {token}")
            current_index += 1
        else:
            # Stop at anything that doesn't look like part of Apple model
            logger.debug(f"Stopping Apple model extraction at non-Apple token: {token}")
            break
    
    # After the main loop, scan ahead for any remaining Apple model numbers (A####)
    remaining_apple_models = []
    for i in range(current_index, min(len(tokens), current_index + 8)):
        if i not in consumed and re.match(r'^A\d{4}$', tokens[i]):
            remaining_apple_models.append(tokens[i])
            consumed.add(i)
            logger.debug(f"Found additional Apple model number: {tokens[i]}")
    
    if remaining_apple_models:
        model_tokens.extend(remaining_apple_models)
    
    if model_tokens:
        model = " ".join(model_tokens)
        logger.debug(f"Built Apple model: {model}")
        return model
    
    return None
    
def _extract_general_model(tokens: List[str], consumed: Set[int], system_brand_index: int, context: Dict[str, bool], logger: logging.Logger) -> str:
    """Extract general model for non-Apple, non-GPU brands."""
    model_tokens = []
    current_index = system_brand_index + 1
    
    while current_index < len(tokens) and current_index not in consumed:
        token = tokens[current_index]
        token_lower = token.lower()
        
        # Stop at slash-separated capacity patterns (common in phones) - single token like "64/128GB"
        if re.match(r'\d+/\d+\s*(gb|tb|mb)$', token_lower):
            logger.debug(f"Stopping model extraction at slash-separated storage pattern: {token}")
            break
        
        # Stop at number followed by slash (potential storage pattern) - multi-token like "64 / 128GB"
        if (tokens[current_index].isdigit() and 
            current_index + 1 < len(tokens) and 
            tokens[current_index + 1] == '/' and
            current_index + 2 < len(tokens) and
            re.match(r'\d+(gb|tb|mb)$', tokens[current_index + 2], re.IGNORECASE)):
            logger.debug(f"Stopping model extraction at multi-token storage pattern: {token} / {tokens[current_index + 2]}")
            break
        
        # Stop at clear component indicators (not model numbers)
        if token_lower in ['intel', 'amd', 'core', 'cpu', 'processor', 'xeon', 'ram', 'memory', 'ddr', 'ddr2', 'ddr3', 'ddr4', 'ddr5', 'ssd', 'hdd', 'nvme', 'emmc']:
            break
        
        # Stop at obvious CPU model patterns (with dashes)
        if re.match(r'i[3579]-\d{3,4}[a-z]*$', token_lower):
            break
        
        # Stop at CPU family patterns like "i5-", "i7-", etc. (even without model number)
        if re.match(r'i[3579]-?$', token_lower):
            break
        
        # Stop at CPU speeds
        if re.match(r'\d+\.\d+ghz$', token_lower):
            break
        
        # Stop at memory/storage sizes (but be more specific)
        if re.match(r'\d+gb$', token_lower) and int(re.match(r'(\d+)', token_lower).group(1)) >= 4:
            # Only stop if it's a reasonable RAM/storage size (4GB or larger)
            break
            
        # Stop at storage/memory ranges
        if re.match(r'\d+gb-\d+gb$', token_lower):
            break
        
        # Stop at obvious storage sizes
        if re.match(r'\d+(tb|gb)$', token_lower):
            size_match = re.match(r'(\d+)(tb|gb)$', token_lower)
            if size_match:
                size_val = int(size_match.group(1))
                unit = size_match.group(2)
                # Stop if it's clearly storage (TB or large GB)
                if unit == 'tb' or (unit == 'gb' and size_val >= 32):
                    break
        
        # Stop at CPU quantity patterns
        if _is_cpu_quantity_pattern(token):
            break
        
        # For GPU context, stop at GPU-specific tokens
        if context.get('has_gpu_context', False):
            if token_lower in ['pcie', 'gddr5', 'gddr6', 'graphics', 'card', 'video', 'displayport', 'hdmi']:
                break
        
        # Include everything else in the model (including alphanumeric model numbers)
        model_tokens.append(token)
        consumed.add(current_index)
        current_index += 1
    
    if model_tokens:
        model = " ".join(model_tokens)
        return model
    
    return None
    
def _detect_apple_brand_priority(tokens: List[str], consumed: Set[int], logger: logging.Logger) -> Tuple[str, int]:
    """Detect Apple brand with high priority for MacBook products."""
    for i, token in enumerate(tokens):
        if i not in consumed:
            # Direct Apple brand detection
            if token.lower() == "apple":
                consumed.add(i)
                logger.debug(f"Detected Apple brand directly at index {i}")
                return "Apple", i
            # MacBook implies Apple brand
            elif token.lower() == "macbook":
                logger.debug(f"Detected Apple brand from MacBook at index {i}")
                return "Apple", i  # Don't consume MacBook - need it for model
    return None, -1

def detect_brand_and_model_comprehensive(tokens: List[str], consumed: Set[int], context: Dict[str, bool], sanitized_title: str, logger: logging.Logger) -> Dict[str, Any]:
    """Comprehensive brand and model detection from tokens."""
    brand_data = {}
    
    # Check for form factor in the tokens
    form_factor_tokens = {"sff": "Small Form Factor (SFF)", "usff": "Ultra Small Form Factor (USFF)"}
    for i, token in enumerate(tokens):
        if token.lower() in form_factor_tokens:
            brand_data["form_factor"] = form_factor_tokens[token.lower()]
            break
    
    # Get brand lists and hierarchies
    brand_lists = _get_brand_lists()
    
    # Initialize variables
    system_brand = None
    system_brand_index = -1
    macbook_index = -1
    
    # PRIORITY 0: Apple brand detection (HIGHEST PRIORITY for MacBook devices)
    if "macbook" in sanitized_title.lower() or "apple" in sanitized_title.lower():
        logger.debug("Prioritizing Apple brand detection for MacBook/Apple device")
        system_brand, system_brand_index = _detect_apple_brand_priority(tokens, consumed, logger)
        if system_brand:
            brand_data["brand"] = system_brand
            # Find MacBook index for model extraction
            for i, token in enumerate(tokens):
                if token.lower() == "macbook":
                    macbook_index = i
                    break
    
    # PRIORITY 1: System brands (Dell, HP, etc.) - Only if no Apple brand found
    if not system_brand and (context.get('is_system_with_gpu', False) or context.get('has_laptop_context', False) or context.get('has_desktop_context', False)):
        logger.debug("Prioritizing system brand detection due to laptop/desktop/system with GPU context")
        
        # Look for system brands first
        system_brands = ["Dell", "HP", "Lenovo", "Apple", "Microsoft", "Asus", "Acer", "Samsung", "LG", "MSI"]
        for i, token in enumerate(tokens):
            if i not in consumed and token.upper() not in brand_lists['ignore_prefixes']:
                for brand in system_brands:
                    if token.lower() == brand.lower():
                        consumed.add(i)
                        system_brand = brand
                        system_brand_index = i
                        logger.debug(f"Detected system brand: {brand}")
                        break
                if system_brand:
                    break
        
        if system_brand:
            brand_data["brand"] = system_brand
    
    # PRIORITY 2: Graphics card brands (if not system context or no system brand found)
    if not system_brand and context.get('has_gpu_context', False):
        system_brand, system_brand_index = _detect_gpu_brand(tokens, consumed, context, brand_lists, logger)
        if system_brand:
            brand_data["brand"] = system_brand
    
    # PRIORITY 3: Parent brands (if no GPU or system brand found)
    if not system_brand:
        system_brand, system_brand_index = _detect_parent_brands(tokens, consumed, context, brand_lists, logger)
        if system_brand:
            brand_data["brand"] = system_brand
    
    # PRIORITY 4: Sub-brands (if no parent brand found)
    if not system_brand:
        system_brand, system_brand_index, macbook_index = _detect_sub_brands(tokens, consumed, context, brand_lists, logger)
        if system_brand:
            brand_data["brand"] = system_brand
    
    # Special handling for phone lots
    if not system_brand:
        system_brand, system_brand_index = _detect_phone_brands(tokens, consumed, context, logger)
        if system_brand:
            brand_data["brand"] = system_brand
    
    # Final fallback
    if not system_brand:
        system_brand, system_brand_index = _detect_fallback_brand(tokens, consumed, context, brand_lists, logger)
        if system_brand:
            brand_data["brand"] = system_brand

    brand_data['system_brand_index'] = system_brand_index
    
    # Extract model if brand was found
    if system_brand and system_brand_index != -1 and "model" not in brand_data:
        model = None
        
        # PRIORITY 1: Accessory model extraction (NEW)
        if context.get('has_accessory_context', False):
            logger.debug("Prioritizing accessory model extraction due to accessory context")
            model = _extract_accessory_model(tokens, consumed, system_brand_index, logger)
        
        # PRIORITY 2: For systems with GPU context, prioritize system model over GPU model
        if not model and (context.get('is_system_with_gpu', False) or context.get('has_laptop_context', False) or context.get('has_desktop_context', False)):
            logger.debug("Prioritizing system model extraction due to system context")
            
            # Try Apple model extraction
            if system_brand.lower() == "apple":
                model = _extract_apple_model(tokens, consumed, system_brand_index, macbook_index, logger)
            else:
                # Try general system model extraction (avoiding GPU tokens)
                model = _extract_system_model(tokens, consumed, system_brand_index, context, logger)
        
        # PRIORITY 3: Try GPU model extraction only if no system model and GPU context
        if not model and context.get('has_gpu_context', False) and not context.get('is_system_with_gpu', False):
            model = _extract_gpu_model(tokens, consumed, system_brand, system_brand_index, context, logger)
        
        # PRIORITY 4: Try general model extraction (if not Apple or if previous extractions failed)
        if not model and not context.get('has_gpu_context', False):
            model = _extract_general_model(tokens, consumed, system_brand_index, context, logger)
        
        if model:
            brand_data["model"] = model
    
    logger.debug(f"Comprehensive brand detection result: {brand_data}")
    return brand_data
    
def _extract_system_model(tokens: List[str], consumed: Set[int], system_brand_index: int, context: Dict[str, bool], logger: logging.Logger) -> str:
    """Extract system model for laptops/desktops, avoiding GPU tokens."""
    model_tokens = []
    current_index = system_brand_index + 1
    form_factor = None
    
    # Define GPU-related tokens to avoid
    gpu_tokens = {"k1100m", "k2100m", "k3100m", "k4100m", "k5100m", "gtx", "rtx", "quadro", "geforce", "radeon"}
    
    # Define common form factor tokens
    form_factor_tokens = {"sff", "usff", "atx", "matx", "itx", "tower", "slim", "desktop", "blade", "1u", "2u", "3u", "4u"}
    
    while current_index < len(tokens) and current_index not in consumed:
        token = tokens[current_index]
        token_lower = token.lower()
        
        # Check for form factor
        if token_lower in form_factor_tokens:
            form_factor = token
            # Include form factor in model but also extract it separately
            model_tokens.append(token)
            consumed.add(current_index)
            current_index += 1
            continue
        
        # Stop at GPU tokens
        if token_lower in gpu_tokens:
            logger.debug(f"Stopping system model extraction at GPU token: {token}")
            break
        
        # Stop at slash-separated capacity patterns (common in phones) - single token like "64/128GB"
        if re.match(r'\d+/\d+\s*(gb|tb|mb)$', token_lower):
            logger.debug(f"Stopping model extraction at slash-separated storage pattern: {token}")
            break
        
        # Stop at number followed by slash (potential storage pattern) - multi-token like "64 / 128GB"
        if (tokens[current_index].isdigit() and 
            current_index + 1 < len(tokens) and 
            tokens[current_index + 1] == '/' and
            current_index + 2 < len(tokens) and
            re.match(r'\d+(gb|tb|mb)$', tokens[current_index + 2], re.IGNORECASE)):
            logger.debug(f"Stopping model extraction at multi-token storage pattern: {token} / {tokens[current_index + 2]}")
            break
        
        # Stop at clear component indicators (not model numbers)
        if token_lower in ['intel', 'amd', 'core', 'cpu', 'processor', 'xeon', 'ram', 'memory', 'ddr', 'ddr2', 'ddr3', 'ddr4', 'ddr5', 'ssd', 'hdd', 'nvme', 'emmc']:
            break
        
        # Stop at obvious CPU model patterns (with dashes)
        if re.match(r'i[3579]-\d{3,4}[a-z]*$', token_lower):
            break
        
        # Stop at CPU family patterns like "i5-", "i7-", etc. (even without model number)
        if re.match(r'i[3579]-?$', token_lower):
            break
        
        # Stop at CPU speeds
        if re.match(r'\d+\.\d+ghz$', token_lower):
            break
        
        # Stop at memory/storage sizes (but be more specific)
        if re.match(r'\d+gb$', token_lower) and int(re.match(r'(\d+)', token_lower).group(1)) >= 4:
            # Only stop if it's a reasonable RAM/storage size (4GB or larger)
            break
            
        # Stop at storage/memory ranges
        if re.match(r'\d+gb-\d+gb$', token_lower):
            break
        
        # Stop at obvious storage sizes
        if re.match(r'\d+(tb|gb)$', token_lower):
            size_match = re.match(r'(\d+)(tb|gb)$', token_lower)
            if size_match:
                size_val = int(size_match.group(1))
                unit = size_match.group(2)
                # Stop if it's clearly storage (TB or large GB)
                if unit == 'tb' or (unit == 'gb' and size_val >= 32):
                    break
        
        # Stop at CPU quantity patterns
        if _is_cpu_quantity_pattern(token):
            break
        
        # Include everything else in the model (including alphanumeric model numbers)
        model_tokens.append(token)
        consumed.add(current_index)
        current_index += 1
    
    # Special handling for form factors
    result = {}
    if form_factor:
        if form_factor.upper() == "SFF":
            result["form_factor"] = "Small Form Factor (SFF)"
        elif form_factor.upper() == "USFF":
            result["form_factor"] = "Ultra Small Form Factor (USFF)"
        # Add more mappings as needed
    
    if model_tokens:
        model = " ".join(model_tokens)
        logger.debug(f"Built system model: {model}")
        return model
    
    return None
    
def check_and_reassign_ambiguous_storage_to_ram(data: Dict, specifics: Dict, table_data: List[Dict], logger: logging.Logger) -> Dict:
    """Check if storage is not included anywhere and reassign ambiguous storage capacities to RAM - ONLY as last resort."""
    
    # EARLY EXIT: If we already have both RAM and storage clearly identified, don't do anything
    has_ram = any(key.startswith('ram_') for key in data.keys())
    has_storage = any(key.startswith('storage_') for key in data.keys())
    
    if has_ram and has_storage:
        logger.debug("Both RAM and storage already identified - no reassignment needed")
        return data
    
    # EARLY EXIT: If we have clear storage indicators in title, don't reassign
    full_title = data.get("Full Title", "").lower()
    clear_storage_indicators = ["ssd", "hdd", "nvme", "storage", "hard drive", "harddrive"]
    if any(indicator in full_title for indicator in clear_storage_indicators):
        logger.debug("Clear storage indicators found in title - no reassignment needed")
        return data
    
    # Step 1: Determine if storage is explicitly not included anywhere in the data
    storage_not_included = False

    # 1a) Respect storage_status extracted from title/status extractors
    try:
        st = str(data.get("storage_status") or "").strip().lower()
        if st and ("not included" in st or "no" == st or "without" == st):
            storage_not_included = True
            logger.debug("Storage not included inferred from storage_status field in title data")
    except Exception:
        pass

    # 1b) Detect compact/spaced grouped negations like "No PowerCord/HardDrive/SSD(a)" in title/additional_info
    try:
        import re as _re
        _text = (str(data.get("Full Title") or "") + " " + str(data.get("additional_info") or "")).lower()
        pat = _re.compile(r"\bno(?:\s*power\s*(?:cord|adapter)|power\s*cord|power\s*adapter|powercord|poweradapter)\b[^,;:\n]{0,160}(?:/|\|)\s*(?:hard\s*drive|hdd|ssd)\b", _re.IGNORECASE)
        if not storage_not_included and pat.search(_text):
            storage_not_included = True
            logger.debug("Storage not included inferred from compact/spaced 'No PowerCord/HardDrive/SSD' pattern in title")
    except Exception:
        pass
    
    # Check specs for storage not included (more specific checking)
    for key, value in specifics.items():
        # Only check actual storage-related fields
        if (key.startswith('specs_') and 
            ('storage' in key.lower() or 'ssd' in key.lower() or 'hdd' in key.lower() or 'hard_drive' in key.lower()) and
            'optical' not in key.lower()):  # Exclude optical drives
            if value.lower() in ['no', 'none', 'n/a', 'not included']:
                storage_not_included = True
                logger.debug(f"Storage not included found in specs: {key} = {value}")
                break
    
    # Check table data for storage not included - MUCH more specific checking
    if not storage_not_included and table_data:
        for entry in table_data:
            for key, value in entry.items():
                # VERY SPECIFIC: Only check actual storage fields, not optical drives or other drives
                actual_storage_fields = [
                    'table_storage', 'table_ssd', 'table_hdd', 'table_hard_drive',
                    'table_harddrive', 'table_nvme', 'table_emmc', 'table_local_storage'
                ]
                
                if key in actual_storage_fields and value.lower() in ['no', 'none', 'n/a', 'not included']:
                    storage_not_included = True
                    logger.debug(f"Storage not included found in table: {key} = {value}")
                    break
                elif key == 'table_missing_components' and value:
                    # Check if missing components include storage-related items (but be specific)
                    missing_lower = value.lower()
                    storage_terms = ['ssd', 'hdd', 'hard drive', 'harddrive', 'nvme', 'storage drive', 'internal storage']
                    if any(term in missing_lower for term in storage_terms):
                        storage_not_included = True
                        logger.debug(f"Storage missing found in missing components: {value}")
                        break
            if storage_not_included:
                break
    
    # Step 2: ONLY if storage is explicitly not included AND we have ambiguous capacities
    if storage_not_included:
        logger.debug("Storage is explicitly not included - checking for ambiguous capacities to reassign to RAM")
        
        # Find storage capacity fields that could be ambiguous
        storage_keys_to_check = []
        for key in data.keys():
            if key.startswith('storage_capacity') and key in data:
                storage_keys_to_check.append(key)
        
        # ONLY reassign if we don't have RAM already AND the capacities are truly ambiguous
        if storage_keys_to_check and not has_ram:
            logger.debug(f"Found ambiguous storage capacity keys to potentially reassign: {storage_keys_to_check}")
            
            # Reassign the first storage capacity as RAM
            primary_storage_key = 'storage_capacity1' if 'storage_capacity1' in data else storage_keys_to_check[0]
            if primary_storage_key in data:
                ram_value = data[primary_storage_key]
                data['ram_size'] = ram_value
                logger.debug(f"Reassigned {primary_storage_key} ({ram_value}) to ram_size")
                
                # Remove the storage keys since storage is not included
                for key in storage_keys_to_check:
                    if key in data:
                        removed_value = data.pop(key)
                        logger.debug(f"Removed storage key {key} (was {removed_value})")
                
                # Also remove storage_type if present since storage is not included
                if 'storage_type' in data:
                    removed_type = data.pop('storage_type')
                    logger.debug(f"Removed storage_type (was {removed_type})")
        else:
            logger.debug("Either no ambiguous storage capacities found or RAM already exists - no reassignment needed")
    else:
        logger.debug("Storage is included or status unclear - no reassignment needed")
    
    return data
    
def post_process_data(data: Dict, is_network_device: bool, context: Dict, logger: logging.Logger, specifics: Dict = None, table_data: List[Dict] = None) -> Dict:
    """Perform post-processing on extracted data."""
    # Handle multiple CPU families from slash-separated patterns like "Core i5/i7"
    if not is_network_device and "cpu_family" in data:
        cpu_family = data["cpu_family"]
        # Check if the CPU family contains slash-separated patterns
        if "/" in cpu_family:
            # Parse patterns like "Core i5/i7"
            import re
            match = re.match(r"Core (i[3579])/i([3579])", cpu_family)
            if match:
                first_family = match.group(1)
                second_family = match.group(2)
                # Set individual CPU families
                data["cpu_family1"] = f"Core {first_family}"
                data["cpu_family2"] = f"Core i{second_family}"
                # Keep the first family as the primary
                data["cpu_family"] = data["cpu_family1"]
                # Set CPU brand if not already set
                if "cpu_brand" not in data:
                    data["cpu_brand"] = "Intel"
                logger.debug(f"Parsed multiple CPU families: {data['cpu_family1']} and {data['cpu_family2']}")
    
    # Fixed logic for setting shared CPU family - only if all numbered families are the same
    if not is_network_device and "cpu_family" not in data:
        family_keys = [k for k in data.keys() if k.startswith("cpu_family") and k != "cpu_family"]
        if family_keys:
            # Get all the family values
            family_values = [data[k] for k in family_keys]
            # Only set shared cpu_family if all values are the same
            if len(set(family_values)) == 1:
                data["cpu_family"] = family_values[0]
                logger.debug(f"Set shared cpu_family from numbered families: {data['cpu_family']}")
            else:
                logger.debug(f"Multiple different CPU families found ({family_values}), not setting shared cpu_family")
    
    # Handle multiple CPU families by ensuring we have both cpu_family1 and cpu_family2 when appropriate
    if not is_network_device:
        # Check if we have cpu_family1 and cpu_family2 from slash-separated patterns
        if "cpu_family1" in data and "cpu_family2" in data:
            # Ensure both are properly set and different
            if data["cpu_family1"] != data["cpu_family2"]:
                logger.debug(f"Multiple CPU families detected: {data['cpu_family1']} and {data['cpu_family2']}")
                # Set the primary cpu_family to the first one if not already set
                if "cpu_family" not in data:
                    data["cpu_family"] = data["cpu_family1"]
                    logger.debug(f"Set primary cpu_family to first family: {data['cpu_family']}")
        
        # Handle CPU brand and generation for multiple CPUs
        brand_keys = [k for k in data.keys() if k.startswith("cpu_brand") and k != "cpu_brand"]
        if brand_keys and "cpu_brand" not in data:
            # Get all the brand values
            brand_values = [data[k] for k in brand_keys]
            # Only set shared cpu_brand if all values are the same
            if len(set(brand_values)) == 1:
                data["cpu_brand"] = brand_values[0]
                logger.debug(f"Set shared cpu_brand from numbered brands: {data['cpu_brand']}")
        
        generation_keys = [k for k in data.keys() if k.startswith("cpu_generation") and k != "cpu_generation"]
        if generation_keys and "cpu_generation" not in data:
            # Get all the generation values
            generation_values = [data[k] for k in generation_keys]
            # Only set shared cpu_generation if all values are the same
            if len(set(generation_values)) == 1:
                data["cpu_generation"] = generation_values[0]
                logger.debug(f"Set shared cpu_generation from numbered generations: {data['cpu_generation']}")

    if context['is_processor_listing'] and "ram_size" in data:
        ram_size = data["ram_size"].lower()
        if ram_size.endswith("mb") and int(re.sub(r'[^\d]', '', ram_size)) <= 32:
            logger.debug(f"Removing small memory size {ram_size} in processor listing as it's likely CPU cache")
            data.pop("ram_size")

    # Normalize single-CPU numbered fields: promote cpu_*1 -> cpu_* when there is no cpu_*2
    try:
        if not is_network_device:
            # Detect if any cpu_*2 exists (indicates multi-CPU context)
            has_second_cpu = any(re.match(r"^cpu_\w+2$", k) for k in data.keys())
            if not has_second_cpu:
                cpu_fields_to_promote = [
                    "cpu_brand",
                    "cpu_family",
                    "cpu_generation",
                    "cpu_model",
                    "cpu_speed",
                    "cpu_suffix",
                ]
                for base_key in cpu_fields_to_promote:
                    numbered_key = f"{base_key}1"
                    if base_key not in data and numbered_key in data:
                        data[base_key] = data[numbered_key]
                        logger.debug(f"Promoted {numbered_key} -> {base_key} (single-CPU normalization)")
    except Exception:
        # Non-fatal; continue without promotion if anything unexpected occurs
        pass

    # NEW: Check and reassign ambiguous storage to RAM if storage is not included
    if specifics is not None and table_data is not None:
        data = check_and_reassign_ambiguous_storage_to_ram(data, specifics, table_data, logger)

    if "device_type" not in data:
        data = add_device_type(data, logger)
    # Normalization: map deprecated Monitors to Computer Servers
    try:
        if data.get("device_type") == "Monitors":
            logger.debug("Normalizing deprecated device_type 'Monitors' to 'Computer Servers'")
            data["device_type"] = "Computer Servers"
            context["device_type"] = "Computer Servers"
    except Exception:
        pass
    
    # NEW: Fallback unfolding for slash-separated storage capacities in title data
    try:
        if 'storage_capacity' in data and isinstance(data['storage_capacity'], str):
            cap_text = data['storage_capacity']
            if re.search(r'\d+\s*/\s*\d+', cap_text, re.IGNORECASE):
                # Extract numbers and unit (GB/TB/MB) appearing anywhere in the text
                nums = re.findall(r'(\d+(?:\.\d+)?)', cap_text)
                unit_match = re.search(r'(gb|tb|mb)', cap_text, re.IGNORECASE)
                unit = unit_match.group(1).upper() if unit_match else ''
                if nums and unit:
                    for i, n in enumerate(nums, 1):
                        data[f'storage_capacity{i}'] = f"{n}{unit}"
                    logger.debug(f"Unfolded title storage_capacity into numbered fields: {[data.get(f'storage_capacity{i}') for i in range(1, len(nums)+1)]}")
    except Exception:
        pass

    # NEW: Fallback unfolding for slash-separated storage capacities in specifics
    try:
        if specifics is not None and 'specs_storage_capacity' in specifics and isinstance(specifics['specs_storage_capacity'], str):
            cap_text = specifics['specs_storage_capacity']
            if re.search(r'\d+\s*/\s*\d+', cap_text, re.IGNORECASE):
                nums = re.findall(r'(\d+(?:\.\d+)?)', cap_text)
                unit_match = re.search(r'(gb|tb|mb)', cap_text, re.IGNORECASE)
                unit = unit_match.group(1).upper() if unit_match else ''
                if nums and unit:
                    for i, n in enumerate(nums, 1):
                        specifics[f'specs_storage_capacity{i}'] = f"{n}{unit}"
                    logger.debug(f"Unfolded specs_storage_capacity into numbered fields: {[specifics.get(f'specs_storage_capacity{i}') for i in range(1, len(nums)+1)]}")
    except Exception:
        pass

    # NOTE: Storage capacity fallback extraction moved to post-processing script
    # See fix_missing_storage_keys.py which runs automatically after file generation

    # NEW: Robustly bind slash-separated CPU speeds to numbered CPUs from the title text
    try:
        has_multi_cpu = (
            ('cpu_model1' in data and 'cpu_model2' in data) or
            ('cpu_family1' in data and 'cpu_family2' in data)
        )
        full_title_raw = data.get('Full Title', '')
        if has_multi_cpu and isinstance(full_title_raw, str) and full_title_raw:
            # Find all GHz values in the title in order of appearance
            speed_matches = list(re.finditer(r'(\d+(?:\.\d+)?)\s*[Gg][Hh][Zz]', full_title_raw))
            if len(speed_matches) >= 2:
                first_span_end = speed_matches[0].end()
                second_span_start = speed_matches[1].start()
                slice_between = full_title_raw[first_span_end:second_span_start]
                # Only enforce mapping when the two speeds are presented as a pair (slash-separated)
                if '/' in slice_between:
                    s1 = f"{speed_matches[0].group(1)}GHz"
                    s2 = f"{speed_matches[1].group(1)}GHz"
                    # Prefer decimal-precision values if present in the title
                    # This avoids integer-GHz fallback like "2GHz" overriding "2.50GHz".
                    def prefer_decimal(s: str) -> str:
                        m = re.match(r'^(\d+)(?:\.(\d+))?GHz$', s, re.IGNORECASE)
                        if not m:
                            return s
                        return f"{m.group(1)}.{m.group(2)}GHz" if m.group(2) else f"{m.group(1)}.00GHz"
                    s1 = prefer_decimal(s1)
                    s2 = prefer_decimal(s2)
                    # Assign without clobbering if already correct; otherwise correct them
                    if data.get('cpu_speed1') != s1:
                        data['cpu_speed1'] = s1
                        logger.debug(f"Bound cpu_speed1 from title pair to: {s1}")
                    if data.get('cpu_speed2') != s2:
                        data['cpu_speed2'] = s2
                        logger.debug(f"Bound cpu_speed2 from title pair to: {s2}")
    except Exception as e:
        try:
            logger.debug(f"CPU speed binding from title failed: {e}")
        except Exception:
            pass

    # Minimal post-only refinement: choose brand/model segment closest to CPU tokens in mixed-brand titles
    try:
        full_title_raw = data.get('Full Title', '') or ''
        if isinstance(full_title_raw, str) and '/' in full_title_raw and data.get('brand'):
            # Find first CPU token position in the raw title
            cpu_match = re.search(r'(i[3579]-\d{3,5}[A-Za-z]*|\d+(?:st|nd|rd|th)\s*Gen|\d+\.\d+\s*GHz)', full_title_raw, flags=re.IGNORECASE)
            if cpu_match:
                cpu_pos = cpu_match.start()
                # Collect brand occurrences with positions
                brands = [
                    'Dell','HP','Lenovo','Apple','Acer','Asus','Toshiba','Samsung','Microsoft','Sony','IBM','Gateway','Compaq','Fujitsu','Panasonic','LG','MSI','Razer','Alienware'
                ]
                brand_positions = []
                for b in brands:
                    for m in re.finditer(rf'\b{re.escape(b)}\b', full_title_raw, flags=re.IGNORECASE):
                        brand_positions.append((m.start(), b))
                if brand_positions:
                    # Choose the last brand occurrence at or before the first CPU token
                    brand_positions.sort(key=lambda x: x[0])
                    chosen = None
                    for pos, b in brand_positions:
                        if pos <= cpu_pos:
                            chosen = (pos, b)
                        else:
                            break
                    if chosen is None:
                        chosen = brand_positions[-1]
                    _, chosen_brand = chosen
                    if chosen_brand:
                        data['brand'] = chosen_brand
                        # Try to refine model from the chosen brand segment
                        # Take text after the chosen brand up to next '/' or CPU token
                        after = full_title_raw.split(chosen_brand, 1)[1]
                        stop_match = re.search(r'(\/|i[3579]-\d{3,5}[A-Za-z]*|\d+(?:st|nd|rd|th)\s*Gen|\d+\.\d+\s*GHz)', after, flags=re.IGNORECASE)
                        segment = after[:stop_match.start()] if stop_match else after
                        # Clean model segment
                        segment = segment.strip(' -|,;')
                        # Keep only first 3 words to avoid trailing descriptors
                        model_guess = ' '.join([w for w in segment.split()[:3]])
                        if model_guess:
                            # If model already exists and contains '/', prefer the part that contains this guess
                            existing_model = data.get('model', '') or ''
                            if '/' in existing_model:
                                parts = [p.strip() for p in existing_model.split('/')]
                                preferred = None
                                for p in parts:
                                    if model_guess.split()[0].lower() in p.lower():
                                        preferred = p
                                        break
                                data['model'] = preferred or model_guess
                            else:
                                data['model'] = model_guess
    except Exception as e:
        try:
            logger.debug(f"Brand/model proximity refinement skipped: {e}")
        except Exception:
            pass

    return data
    
def parse_title(title: str, logger: logging.Logger) -> Dict:
    """Main parse_title function that orchestrates the parsing process."""
    sanitized_title = re.sub(r'[\*]', '', title)
    tokens = tokenize_with_slash_splitting(sanitized_title, logger)
    consumed = set()
    data = {"Full Title": title}
    extractors = load_extractors(logger)
    logger.debug(f"Tokens for title '{sanitized_title}': {tokens}")

    # Step 1: Detect various contexts
    context = detect_listing_context(sanitized_title, logger)
    
    # Step 1.5: Apply GPU extractors FIRST (before brand detection consumes tokens)
    if context['has_gpu_context']:
        logger.debug("Applying GPU extractors early (before brand detection)")
        gpu_extractors = [ext for ext in extractors if ext.name.startswith("gpu")]
        for extractor in gpu_extractors:
            logger.debug(f"Applying early GPU extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Early GPU extractor {extractor.name} returned matches: {matches}")
                if matches:
                    if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                        match_indices, consume_indices = matches[0]
                    else:
                        match_indices = matches[0]
                        consume_indices = match_indices
                    logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[i] for i in match_indices]}")
                    extracted = extractor.process_match(tokens, match_indices)
                    for key, value in extracted.items():
                        if isinstance(value, str):
                            parts = value.split()
                            extracted[key] = " ".join(dict.fromkeys(parts))
                    data.update(extracted)
                    if getattr(extractor, 'consume_on_match', True):
                        for idx in consume_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                    logger.debug(f"Extracted early GPU {extractor.name}: {extracted}")
            except ValueError as e:
                logger.warning(f"Unpacking error in early GPU extractor {extractor.name}: {str(e)}. Skipping extractor.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error in early GPU extractor {extractor.name}: {str(e)}. Skipping extractor.")
                continue
    
    # Step 2: Comprehensive brand and model detection (now runs after GPU extraction)
    brand_data = detect_brand_and_model_comprehensive(tokens, consumed, context, sanitized_title, logger)
    
    # FIXED: If GPU brand was extracted, use it as the main brand ONLY for standalone graphics cards
    if 'gpu_brand' in data and context['has_gpu_context'] and not context.get('is_system_with_gpu', False):
        brand_data['brand'] = data['gpu_brand']
        logger.debug(f"Using GPU brand as main brand: {data['gpu_brand']} (standalone graphics card)")
    # ADDITION: If no GPU brand but we have GPU spec, try to extract brand from it (only for standalone cards)
    elif 'gpu_spec' in data and context['has_gpu_context'] and not context.get('is_system_with_gpu', False) and not brand_data.get('brand'):
        gpu_spec = data['gpu_spec'].lower()
        gpu_brands = ["nvidia", "amd", "intel"]
        for brand in gpu_brands:
            if brand in gpu_spec:
                brand_data['brand'] = brand.upper()
                logger.debug(f"Extracted brand '{brand.upper()}' from GPU spec: {data['gpu_spec']}")
                break
    # For systems with GPUs, keep the original system brand
    elif 'gpu_brand' in data and context.get('is_system_with_gpu', False):
        logger.debug(f"Keeping system brand '{brand_data.get('brand')}' for system with GPU (GPU brand: {data['gpu_brand']})")
    
    data.update(brand_data)
    
    # Step 3: Determine device type EARLY so it's available for storage extraction
    device_type = determine_device_type(brand_data, context, title, sanitized_title, logger)

    # Override rule: If it's a 2-in-1 convertible and would normally be a PC laptop,
    # classify as a tablet category to avoid false positives.
    # Patterns covered: "2 in 1", "2-in-1", "2in1" (case-insensitive)
    if ENABLE_2IN1_TO_TABLET_OVERRIDE and device_type == "PC Laptops & Netbooks":
        tl = sanitized_title.lower()
        if (
            "2in1" in tl
            or re.search(r"\b2\s*in\s*1\b", tl)
            or re.search(r"\b2\s*-\s*in\s*-\s*1\b", tl)
        ):
            logger.debug("2-in-1 pattern detected; overriding device_type from 'PC Laptops & Netbooks' to 'Tablets & eBook Readers'")
            device_type = "Tablets & eBook Readers"

    if device_type:
        data["device_type"] = device_type
        # CRITICAL: Update context with determined device type for storage extraction
        context["device_type"] = device_type
        logger.debug(f"Updated context with device_type: {device_type}")
    
    # ENHANCED: For GPU context, override device type to Graphics/Video Cards ONLY for standalone graphics cards
    if context['has_gpu_context'] and not context.get('is_system_with_gpu', False) and (not data.get("device_type") or data.get("device_type") == "Monitors"):
        data["device_type"] = "Graphics/Video Cards"
        context["device_type"] = "Graphics/Video Cards"
        logger.debug("Override device_type to 'Graphics/Video Cards' for standalone GPU context")

    # Detect if this is a phone device (exclude computer tablets like Surface)
    is_computer_tablet = device_type == "Tablets & eBook Readers" and any(brand.lower() in ["microsoft", "lenovo", "hp", "dell", "asus", "acer", "samsung"] for brand in tokens)
    is_phone_device = (device_type in ["Cell Phones & Smartphones"] or (device_type == "Tablets & eBook Readers" and not is_computer_tablet)) and context['has_phone_context']
    is_network_device = device_type in ["Enterprise Networking, Servers"] and not is_phone_device
    is_server_device = device_type in ["Computer Servers"]
    
    # Step 4: Apply lot extractors
    lot_data = apply_lot_extractors(extractors, tokens, consumed, logger)
    data.update(lot_data)

    # Fallback lot detection: infer lot from model-style tokens joined by '+'
    # Example: "ThinkPad T460+T570" -> lot = 2
    # Intention: Treat '+' between two model-like tokens (both containing at least one digit)
    # as a mixed lot indicator. This avoids false positives like 'A+ Grade' or 'Wi-Fi + BT'.
    try:
        if 'lot' not in data:
            plus_matches = list(re.finditer(r"(?<!\w)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\s*\+\s*([A-Za-z0-9-]*\d[A-Za-z0-9-]*)(?!\w)", sanitized_title))
            if plus_matches:
                # Count distinct sides with digits across matches to handle rare multi-plus cases
                count = 0
                seen_fragments = set()
                for m in plus_matches:
                    lhs = m.group(1)
                    rhs = m.group(2)
                    # Normalize fragments
                    for frag in (lhs, rhs):
                        f = frag.strip().upper()
                        if f and any(ch.isdigit() for ch in f):
                            seen_fragments.add(f)
                count = len(seen_fragments) if len(seen_fragments) >= 2 else 2
                data['lot'] = str(count)
                logger.debug(f"Inferred lot from '+' model pattern: {data['lot']} (fragments={sorted(seen_fragments)})")
    except Exception:
        pass
    
    # Step 5: Apply phone and status extractors
    phone_status_data = apply_phone_and_status_extractors(extractors, tokens, consumed, context, device_type, logger)
    is_phone_device = phone_status_data.pop('is_phone_device', is_phone_device)
    data.update(phone_status_data)
    
    # Step 6: Apply switch/adapter extractors
    switch_adapter_data = apply_switch_adapter_extractors(extractors, tokens, consumed, is_phone_device, logger)
    data.update(switch_adapter_data)
    
    # Step 7: Apply CPU/RAM/Storage extractors (now with correct device context)
    cpu_ram_storage_data = apply_cpu_ram_storage_extractors(extractors, tokens, consumed, is_network_device, is_phone_device, context, logger)
    cpu_extracted = cpu_ram_storage_data.pop('cpu_extracted', False)
    data.update(cpu_ram_storage_data)
    
    # Step 8: Apply network device extractors
    network_data = apply_network_device_extractors(tokens, consumed, is_network_device, sanitized_title, logger)
    data.update(network_data)
    
    # Step 9: Apply priority extractors
    priority_data = apply_priority_extractors(extractors, tokens, consumed, is_network_device, is_phone_device, context, cpu_extracted, logger)
    data.update(priority_data)
    
    # Step 10: Apply remaining extractors (skip GPU since already applied)
    remaining_data = apply_remaining_extractors(extractors, tokens, consumed, is_phone_device, context, logger)
    data.update(remaining_data)
    
    # Step 11: Apply multiple extractors
    multiple_data = apply_multiple_extractors(extractors, tokens, consumed, is_network_device, is_phone_device, data, logger)
    data.update(multiple_data)
    
    # Step 12: Handle remaining tokens
    remaining_tokens = [tokens[i] for i in range(len(tokens)) if i not in consumed]
    if remaining_tokens:
        data["additional_info"] = " ".join(remaining_tokens)
    
    # Step 13: Enrich with carrier strings from leftover/additional tokens
    try:
        enrich_network_carriers_from_title_tokens(tokens, data, logger)
    except Exception as e:
        if logger:
            logger.debug(f"Carrier enrichment error: {e}")

    # Step 14: Post-processing
    data = post_process_data(data, is_network_device, context, logger, {}, [])
    
    logger.debug(f"Parsed title data: {data}")
    return data
    
def configure_root_logger():
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        try:
            console_handler.stream = open(sys.stdout.fileno(), mode='w', encoding='utf-8', errors='replace')
        except Exception:
            # Fallback to default stdout without replacing the stream if unavailable
            pass
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)


class NonBlockingFileHandler(logging.FileHandler):
    """File handler that disables itself on I/O errors instead of propagating.

    This prevents the pipeline from stalling if the log file becomes unavailable
    (e.g., removable drive ejected) while processing.
    """
    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except Exception:
            # Best-effort cleanup and detach this handler from the logger
            try:
                self.close()
            except Exception:
                pass
            try:
                logging.getLogger(record.name).removeHandler(self)
            except Exception:
                pass

def setup_logging(item_number=None):
    if item_number:
        logger = logging.getLogger(f"item_{item_number}")
    else:
        logger = logging.getLogger("generic")
    logger.setLevel(logging.DEBUG)
    if LOGGING_ENABLED and item_number:
        from pathlib import Path
        # Create centralized logs directory structure
        base_logs_dir = Path("logs")
        log_dir = base_logs_dir / "processing" / "process_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"process_log_{item_number}.txt"
        log_file_path = log_dir / filename
        try:
            file_handler = NonBlockingFileHandler(log_file_path, mode='w', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"Error setting up file handler for {item_number}: {e}")
    return logger

def standardize_key(key: str) -> str:
    key = re.sub(r'[^a-z0-9]', '_', key.lower()).strip('_')
    return key

@dataclass
class ListingData:
    title: Dict = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)
    category: str = ""
    specifics: Dict = field(default_factory=dict)
    table_data: List = field(default_factory=list)
    description: Dict = field(default_factory=lambda: {'description_text': ''})

def parse_metadata(text: str, logger: logging.Logger) -> Dict:
    logger.debug("Starting metadata parsing")
    metadata = {}
    for line in text.split('\n'):
        line = line.strip().lstrip('\ufeff')
        if not line:
            continue
        if "===CATEGORY PATH===" in line:
            logger.debug("Encountered category path delimiter; ending metadata parsing")
            break
        if line and not line.startswith("==="):
            tokens = tokenize(line, logger)
            colon_index = -1
            key_tokens = []
            for i, token in enumerate(tokens):
                if ':' in token:
                    colon_index = i
                    if token != ':':
                        key_part = token[:token.index(':')]
                        if key_part:
                            key_tokens.append(key_part)
                    break
                key_tokens.append(token)
            if colon_index != -1:
                key = ' '.join(key_tokens).strip()
                value = ' '.join(tokens[colon_index+1:]).strip() if colon_index + 1 < len(tokens) else ''
                if key:
                    metadata[key] = clean_text(value)
                    logger.debug(f"Extracted metadata key-value pair: '{key}' = '{value}'")
    logger.debug(f"Completed metadata parsing with {len(metadata)} entries")
    return metadata

def parse_category(text: str, logger: logging.Logger) -> Dict:
    logger.debug("Starting category parsing")
    category_info = {}
    category_lines = []
    in_category = False
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        if "===CATEGORY PATH===" in line:
            in_category = True
            logger.debug("Entered category path section")
            continue
        elif "===" in line:
            in_category = False
            logger.debug("Exited category path section")
            continue
        if in_category:
            tokens = tokenize(line, logger)
            category_line = ' '.join(tokens).strip()
            category_lines.append(category_line)
            logger.debug(f"Collected category path component: '{category_line}'")
    if category_lines:
        category_info["category_path"] = " > ".join(category_lines)
        category_info["leaf_category"] = category_lines[-1].strip()
        logger.debug(f"Set category path: '{category_info['category_path']}' and leaf category: '{category_info['leaf_category']}'")
    else:
        logger.debug("No category path components found")
    logger.debug("Completed category parsing")
    return category_info

def enhance_server_memory_title_extraction(data: Dict, logger: logging.Logger) -> Dict:
    """Enhance title data for server memory - ONLY extract what's explicitly in the title."""
    
    # Only apply to server memory devices
    if data.get('device_type') != 'Server Memory (RAM)':
        return data
    
    full_title = data.get('Full Title', '').lower()
    
    # Extract total capacity and configuration (explicitly in title)
    # Support GB/TB totals, including decimals like '1.5TB', and remove commas
    total_config_pattern = re.search(r'([\d,]+(?:\.\d+)?)(gb|tb)\s*\((\d+)\s*x\s*(\d+)(gb|tb)\)', full_title)
    if total_config_pattern:
        total_num = total_config_pattern.group(1).replace(',', '')
        total_unit = total_config_pattern.group(2).upper()
        total_capacity = f"{total_num}{total_unit}"
        module_count = total_config_pattern.group(3)
        module_size_num = total_config_pattern.group(4)
        module_size_unit = total_config_pattern.group(5).upper()
        module_size = f"{module_size_num}{module_size_unit}"
        
        data['ram_total'] = total_capacity
        data['ram_capacity'] = module_size  # Unified with table_capacity
        data['ram_modules'] = module_count
        data['ram_config'] = f"{module_count}x{module_size}"
        logger.debug(f"Extracted server RAM config from title: {total_capacity} ({module_count}x{module_size})")
    
    # Extract DDR type (explicitly in title)
    ddr_match = re.search(r'(ddr\d+)', full_title)
    if ddr_match:
        data['ram_type'] = ddr_match.group(1).upper()  # Just DDR3, not SDRAM
        logger.debug(f"Extracted RAM type from title: {data['ram_type']}")
    
    # Extract speed grade from PC3/PC3L pattern (explicitly in title)
    speed_grades = []
    
    # Check for combined pattern like "PC3/PC3L-8500R" first
    combined_pattern = re.search(r'pc3/pc3l-(\d+)r?', full_title)
    if combined_pattern:
        speed_number = combined_pattern.group(1)
        speed_grades = [f"PC3-{speed_number}R", f"PC3L-{speed_number}R"]
    else:
        # Look for individual patterns
        pc3_match = re.search(r'pc3-(\d+)r?', full_title)
        if pc3_match:
            speed_number = pc3_match.group(1)
            speed_grades.append(f"PC3-{speed_number}R")
        
        pc3l_match = re.search(r'pc3l-(\d+)r?', full_title)
        if pc3l_match:
            speed_number = pc3l_match.group(1)
            speed_grades.append(f"PC3L-{speed_number}R")
    
    if speed_grades:
        data['ram_speed_grade'] = ' / '.join(speed_grades)
        logger.debug(f"Extracted RAM speed grade from title: {data['ram_speed_grade']}")
    
    # Extract ECC/Registered info (explicitly in title)
    if 'reg ecc' in full_title or 'registered ecc' in full_title:
        data['ram_error_correction'] = 'Registered ECC'
        data['ram_registered'] = 'Yes'
        logger.debug("Extracted ECC type from title: Registered ECC")
    elif 'ecc' in full_title:
        data['ram_error_correction'] = 'ECC'
        logger.debug("Extracted ECC type from title: ECC")
    
    # Extract mixed lot info (explicitly in title)
    if 'mixed lot' in full_title:
        data['ram_details'] = 'Mixed Lot'
        logger.debug("Detected mixed lot from title")
    
    # Extract server designation (explicitly in title)
    if 'server' in full_title:
        data['ram_application'] = 'Server'
        logger.debug("Extracted RAM application from title: Server")
    
    return data
    
def parse_title_components(title_text: str, logger: logging.Logger) -> Dict:
    data = {}
    if not title_text:
        return data
    
    # Parse initial title data
    extracted_data = parse_title(title_text, logger)
    data.update(extracted_data)
    
    # Add device type as a separate post-processing step (completely title-based)
    device_type_result = add_device_type(data, logger)
    if device_type_result is not None:
        data = device_type_result
    
    # Only attempt server memory enhancement if device_type exists
    if data and data.get('device_type'):
        server_memory_result = enhance_server_memory_title_extraction(data, logger)
        if server_memory_result is not None:
            data = server_memory_result
    
    return data
    
def parse_item_specifics(text: str, category_info: Dict, logger: logging.Logger, listing: 'ListingData' = None) -> Dict:
   logger.debug("Starting item specifics parsing")
   specifics = {}
   key_order = []
   in_specifics = False
   current_key = None
   accumulated_value = []
   key_mapping = {
       'processor': 'specs_cpu',
       'cpu': 'specs_cpu',
       'processor model': 'specs_processor_model',  # Added mapping for processor model
       'processor speed': 'specs_cpu_speed',  # Added mapping for processor speed
       'memory': 'specs_ram',
       'ram': 'specs_ram',
       'storage': 'specs_storage',
       'ssd': 'specs_storage',
       'hard drive capacity': 'specs_storage_capacity',
       'screen size': 'specs_screen_size',
       'screensize': 'specs_screen_size',
       'display': 'specs_screen_size',
       'resolution': 'specs_screen_resolution',
       'video card': 'specs_videocard',
       'graphics card': 'specs_videocard',
       'gpu': 'specs_videocard',
       'battery': 'specs_battery',
       'webcam': 'specs_webcam',
       'ethernet': 'specs_ethernet',
       'wifi': 'specs_wifi',
       'operating system': 'specs_os',
       'os': 'specs_os',
       'defects': 'specs_defects',
       'missing components': 'specs_missing_components',
       'notes': 'specs_notes',
       'test result': 'specs_test_result',
       'network / carrier': 'specs_network_carrier',
       'battery health': 'specs_battery_health',
       'battery health as is': 'specs_battery_health',
       'version': 'specs_version',
       'ios version': 'specs_version',
       'missing': 'specs_missing',
       'serial number / imei': 'specs_serial_number_imei',
       'ports': 'specs_ports',
       'poe specs': 'specs_poe_specs',
       'rack size': 'specs_rack_size',
       'power supply': 'specs_power_supply',
       'seller notes': 'specs_seller_notes',
       'form factor': 'specs_form_factor',
       'device type': 'specs_device_type',
       'touch screen': 'specs_screen_touch',
       'panel type': 'specs_screen_panel_type',
       'switch': 'specs_switch',
       'network switch': 'specs_switch',
       'adapter': 'specs_adapter',
       'network adapter': 'specs_adapter',
       'nic': 'specs_adapter',
       'network interface': 'specs_adapter',
   }

   for line in text.split('\n'):
       line = line.strip().lstrip('\ufeff')
       if not line:
           continue
       if "===ITEM SPECIFICS===" in line:
           in_specifics = True
           logger.debug("Entered item specifics section")
           continue
       elif "===" in line:
           if current_key and accumulated_value:
               value = clean_text(' '.join(accumulated_value))
               mapped_key = key_mapping.get(current_key.lower(), f"specs_{standardize_key(current_key)}")
               if mapped_key == 'specs_notes':
                   value = ', '.join([v.strip() for v in accumulated_value if v.strip()])
               # Normalize storage capacity format (remove spaces from TB/GB)
               if mapped_key == 'specs_storage_capacity':
                   value = re.sub(r'(\d+)\s+(TB|GB|tb|gb)', r'\1\2', value).lower()
               specifics[mapped_key] = value
               if mapped_key not in [k for k, _ in key_order]:
                   key_order.append((mapped_key, current_key))
                   logger.debug(f"Stored specifics key-value pair: '{mapped_key}' = '{value}'")
           in_specifics = False
           logger.debug("Exited item specifics section")
           continue
       if in_specifics:
           tokens = tokenize(line, logger)
           colon_index = -1
           key_tokens = []
           for i, token in enumerate(tokens):
               if ':' in token:
                   colon_index = i
                   if token != ':':
                       key_part = token[:token.index(':')]
                       if key_part:
                           key_tokens.append(key_part)
                   break
               key_tokens.append(token)
           if colon_index != -1:
               if current_key and accumulated_value:
                   value = clean_text(' '.join(accumulated_value))
                   mapped_key = key_mapping.get(current_key.lower(), f"specs_{standardize_key(current_key)}")
                   if mapped_key == 'specs_notes':
                       value = ', '.join([v.strip() for v in accumulated_value if v.strip()])
                   # Normalize storage capacity format (remove spaces from TB/GB)
                   if mapped_key == 'specs_storage_capacity':
                       value = re.sub(r'(\d+)\s+(TB|GB|tb|gb)', r'\1\2', value).lower()
                   specifics[mapped_key] = value
                   if mapped_key not in [k for k, _ in key_order]:
                       key_order.append((mapped_key, current_key))
                       logger.debug(f"Stored specifics key-value pair: '{mapped_key}' = '{value}'")
               current_key = ' '.join(key_tokens).strip()
               accumulated_value = tokens[colon_index+1:] if colon_index + 1 < len(tokens) else []
               logger.debug(f"Identified specifics key: '{current_key}'")
           elif current_key:
               accumulated_value.append(line)
               logger.debug(f"Accumulating value for specifics key '{current_key}': '{line}'")

   if current_key and accumulated_value:
       value = clean_text(' '.join(accumulated_value))
       mapped_key = key_mapping.get(current_key.lower(), f"specs_{standardize_key(current_key)}")
       if mapped_key == 'specs_notes':
           value = ', '.join([v.strip() for v in accumulated_value if v.strip()])
       # Normalize storage capacity format (remove spaces from TB/GB)
       if mapped_key == 'specs_storage_capacity':
           value = re.sub(r'(\d+)\s+(TB|GB|tb|gb)', r'\1\2', value).lower()
       specifics[mapped_key] = value
       if mapped_key not in [k for k, _ in key_order]:
           key_order.append((mapped_key, current_key))
           logger.debug(f"Stored final specifics key-value pair: '{mapped_key}' = '{value}'")

   extractors = load_extractors(logger)
   cpu_extractors = [ext for ext in extractors if ext.name.startswith("cpu_")]
   ram_extractors = [ext for ext in extractors if ext.name.startswith("ram_")]
   storage_extractors = [ext for ext in extractors if ext.name.startswith("storage_")]
   screen_extractors = [ext for ext in extractors if ext.name.startswith("screen_")]
   gpu_extractors = [ext for ext in extractors if ext.name == "gpu" or ext.name.startswith("gpu_")]
   os_extractors = [ext for ext in extractors if ext.name.startswith("os_")]
   device_extractors = [ext for ext in extractors if ext.name.startswith("device_") or ext.name.startswith("form_factor")]
   battery_extractors = [ext for ext in extractors if ext.name.startswith("battery_")]
   switch_extractors = [ext for ext in extractors if ext.name.startswith("switch_")]
   adapter_extractors = [ext for ext in extractors if ext.name.startswith("adapter_")]

   # Process screen size specific using screen extractors if it exists
   if "specs_screen_size" in specifics:
       logger.debug(f"Normalizing screen size from specifics: '{specifics['specs_screen_size']}'")
       tokens = tokenize_with_slash_splitting(specifics['specs_screen_size'], logger)
       consumed = set()
       size_normalized = False
       
       # First try with the dedicated screen_size extractor
       for extractor in [ext for ext in screen_extractors if ext.name == "screen_size"]:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Screen size extractor returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   if "screen_size" in extracted and extracted["screen_size"]:
                       specifics["specs_screen_size"] = extracted["screen_size"]
                       size_normalized = True
                       logger.debug(f"Normalized screen size to: {specifics['specs_screen_size']}")
                       break
           except Exception as e:
               logger.error(f"Error in screen extractor: {str(e)}")
       
       # If no direct match, try to normalize manually
       if not size_normalized:
           # Look for a number followed by optional space and "in" or "inch"
           match = re.search(r'(\d+\.?\d*)\s*(in|inch|")', specifics['specs_screen_size'], re.IGNORECASE)
           if match:
               number = match.group(1)
               specifics["specs_screen_size"] = f"{number}in"
               logger.debug(f"Manually normalized screen size to: {specifics['specs_screen_size']}")

   # Check for and process CPU information from different possible fields - UPDATED WITH RESTRICTIONS
   # Only apply CPU extractors to fields that are clearly about CPUs, not compatibility descriptions
   cpu_specific_fields = ["specs_cpu", "specs_processor_model", "specs_processor"]
   
   # Don't apply CPU extractors to seller notes, descriptions, or other non-CPU fields
   excluded_fields = [
       "specs_seller_notes", "specs_notes", "specs_description", 
       "specs_compatibility", "specs_supports", "specs_designed_for",
       "specs_model", "specs_brand"  # Don't extract CPU info from model/brand fields
   ]
   
   cpu_key = next((k for k in specifics if k in cpu_specific_fields and k not in excluded_fields), None)
   if cpu_key:
       logger.debug(f"Enhancing CPU data from '{specifics[cpu_key]}'")
       clean_cpu_text = clean_text(specifics[cpu_key])
       tokens = tokenize_with_slash_splitting(clean_cpu_text, logger)
       consumed = set()
       
       # Apply CPU model extraction with special handling for multiple matches like in title parsing
       cpu_model_extractors = [ext for ext in cpu_extractors if ext.name == "cpu_model"]
       for extractor in cpu_model_extractors:
           logger.debug(f"Applying CPU model extractor: {extractor.name}")
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
               if matches:
                   # Check if we have multiple separate CPU matches (like [[1], [4]]) and treat as multi-CPU
                   if len(matches) > 1:
                       # Multiple separate CPU models found - handle like title parsing does
                       for i, match in enumerate(matches, start=1):
                           if isinstance(match, tuple) and len(match) == 2:
                               match_indices, consume_indices = match
                           else:
                               match_indices = match
                               consume_indices = match_indices
                           flat_indices = []
                           for idx in match_indices:
                               if isinstance(idx, int):
                                   flat_indices.append(idx)
                                   if getattr(extractor, 'consume_on_match', True):
                                       consumed.add(idx)
                           if flat_indices:
                               extracted = extractor.process_match(tokens, flat_indices)
                               for key, value in extracted.items():
                                   if isinstance(value, str):
                                       parts = value.split()
                                       extracted[key] = " ".join(dict.fromkeys(parts))
                               if i == 1:
                                   # For first match, set both numbered and base keys for shared fields
                                   for key, value in extracted.items():
                                       if key in ["cpu_family", "cpu_brand"]:
                                           specifics[f"specs_{key}"] = value
                                       specifics[f"specs_{key}{i}"] = value
                               else:
                                   # For subsequent matches, only set numbered keys
                                   for key, value in extracted.items():
                                       specifics[f"specs_{key}{i}"] = value
                   else:
                       # Single match or single match with multiple indices - handle normally
                       all_extracted = {}
                       for i, match in enumerate(matches, start=1):
                           if isinstance(match, tuple) and len(match) == 2:
                               match_indices, consume_indices = match
                           else:
                               match_indices = match
                               consume_indices = match_indices
                           logger.debug(f"Processing tokens at indices {match_indices}: {[tokens[j] for j in match_indices]}")
                           flat_indices = []
                           for idx in match_indices:
                               if isinstance(idx, int):
                                   flat_indices.append(idx)
                                   if getattr(extractor, 'consume_on_match', True):
                                       consumed.add(idx)
                           if flat_indices:
                               extracted = extractor.process_match(tokens, flat_indices)
                               all_extracted.update(extracted)
                               logger.debug(f"Extracted {extractor.name} match {i}: {extracted}")
                       
                       # Check if we have numbered keys from the extractor (cpu_model_1, cpu_family_1, etc.)
                       has_marked_keys = any(key for key in all_extracted if re.match(r'cpu_\w+_\d+$', key))
                       if has_marked_keys:
                           # Handle numbered keys - convert cpu_model_1 to specs_cpu_model1
                           for key, value in all_extracted.items():
                               if not re.match(r'cpu_\w+_\d+$', key):
                                   specifics[f"specs_{key}"] = value
                           for key, value in all_extracted.items():
                               match = re.match(r'(cpu_\w+)_(\d+)$', key)
                               if match:
                                   base_field, num = match.groups()
                                   specifics[f"specs_{base_field}{num}"] = value
                       else:
                           # Single match - use base keys
                           for key, value in all_extracted.items():
                               if isinstance(value, str):
                                   parts = value.split()
                                   all_extracted[key] = " ".join(dict.fromkeys(parts))
                               specifics[f"specs_{key}"] = value
           except Exception as e:
               logger.error(f"Error in CPU model extractor {extractor.name}: {str(e)}")
       
       # Apply other CPU extractors normally (but skip cpu_model since we handled it above)
       other_cpu_extractors = [ext for ext in cpu_extractors if ext.name != "cpu_model"]
       for extractor in other_cpu_extractors:
           logger.debug(f"Applying CPU extractor: {extractor.name}")
           try:
               # For CPU generation extractor, add additional context check
               if extractor.name == "cpu_generation":
                   # Check if this looks like a compatibility description
                   compatibility_phrases = [
                       'supports', 'support', 'compatible', 'compatibility', 'designed for',
                       'optimized for', 'works with', 'fits', 'socket', 'chipset', 'platform',
                       'family processors', 'processor family', 'motherboard', 'server'
                   ]
                   
                   full_text = clean_cpu_text.lower()
                   has_compatibility_context = any(phrase in full_text for phrase in compatibility_phrases)
                   
                   # Skip generation extraction for compatibility descriptions
                   if has_compatibility_context:
                       logger.debug(f"Skipping CPU generation extraction due to compatibility context")
                       continue
               
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced CPU field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in CPU extractor {extractor.name}: {str(e)}")

   ram_key = next((k for k in specifics if k in ["specs_ram", "specs_ram_size"]), None)
   if ram_key:
       logger.debug(f"Enhancing RAM data from '{specifics[ram_key]}'")
       
       # PREPROCESSING: Convert "&" pattern to "/" pattern for RAM fields
       ram_value = specifics[ram_key]
       # Convert patterns like "8 & 16GB" to "8GB/16GB"
       ampersand_pattern = re.search(r'(\d+)\s*&\s*(\d+)(gb|tb|mb)', ram_value, re.IGNORECASE)
       if ampersand_pattern:
           first_num = ampersand_pattern.group(1)
           second_num = ampersand_pattern.group(2)
           unit = ampersand_pattern.group(3)
           ram_value = f"{first_num}{unit}/{second_num}{unit}"
           logger.debug(f"Converted RAM '&' pattern to '/': {specifics[ram_key]} -> {ram_value}")
           # Update the original specifics value too
           specifics[ram_key] = ram_value
       
       tokens = tokenize_with_slash_splitting(ram_value, logger)
       consumed = set()
       for extractor in ram_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced RAM field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in RAM extractor {extractor.name}: {str(e)}")

   storage_key = next((k for k in specifics if k in ["specs_storage", "specs_ssd", "specs_storage_capacity"]), None)
   if storage_key:
       logger.debug(f"Enhancing storage data from '{specifics[storage_key]}'")
       
       # PREPROCESSING: Convert "&" pattern to "/" pattern for storage fields
       storage_value = specifics[storage_key]
       # Convert patterns like "256 & 512GB" to "256GB/512GB"
       ampersand_pattern = re.search(r'(\d+)\s*&\s*(\d+)(gb|tb|mb)', storage_value, re.IGNORECASE)
       if ampersand_pattern:
           first_num = ampersand_pattern.group(1)
           second_num = ampersand_pattern.group(2)
           unit = ampersand_pattern.group(3)
           storage_value = f"{first_num}{unit}/{second_num}{unit}"
           logger.debug(f"Converted storage '&' pattern to '/': {specifics[storage_key]} -> {storage_value}")
           # Update the original specifics value too
           specifics[storage_key] = storage_value
       
       tokens = tokenize_with_slash_splitting(storage_value, logger)
       consumed = set()
       for extractor in storage_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       # Properly map extracted storage keys to specs namespace
                       if key.startswith("storage_"):
                           specs_key = f"specs_{key}"
                       else:
                           specs_key = f"specs_storage_{key}"
                       
                       specifics[specs_key] = value
                       logger.debug(f"Added enhanced storage field: {specs_key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in storage extractor {extractor.name}: {str(e)}")

   screen_key = next((k for k in specifics if k in ["specs_screen_size", "specs_display"]), None)
   if screen_key:
       logger.debug(f"Enhancing screen data from '{specifics[screen_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[screen_key], logger)
       consumed = set()
       for extractor in screen_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced screen field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in screen extractor {extractor.name}: {str(e)}")

   gpu_key = next((k for k in specifics if k in ["specs_videocard", "specs_gpu"]), None)
   if gpu_key:
       logger.debug(f"Enhancing GPU data from '{specifics[gpu_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[gpu_key], logger)
       consumed = set()
       for extractor in gpu_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced GPU field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in GPU extractor {extractor.name}: {str(e)}")

   os_key = next((k for k in specifics if k in ["specs_os", "specs_operating_system"]), None)
   if os_key:
       logger.debug(f"Enhancing OS data from '{specifics[os_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[os_key], logger)
       consumed = set()
       for extractor in os_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced OS field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in OS extractor {extractor.name}: {str(e)}")

   device_key = next((k for k in specifics if k in ["specs_device_type", "specs_form_factor"]), None)
   if device_key:
       logger.debug(f"Enhancing device type data from '{specifics[device_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[device_key], logger)
       consumed = set()
       for extractor in device_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced device field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in device extractor {extractor.name}: {str(e)}")

   battery_key = next((k for k in specifics if k in ["specs_battery", "specs_battery_health"]), None)
   if battery_key:
       logger.debug(f"Enhancing battery data from '{specifics[battery_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[battery_key], logger)
       consumed = set()
       for extractor in battery_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced battery field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in battery extractor {extractor.name}: {str(e)}")

   switch_key = next((k for k in specifics if k in ["specs_switch", "specs_network_switch"]), None)
   if switch_key:
       logger.debug(f"Enhancing switch data from '{specifics[switch_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[switch_key], logger)
       consumed = set()
       for extractor in switch_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced switch field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in switch extractor {extractor.name}: {str(e)}")

   adapter_key = next((k for k in specifics if k in ["specs_adapter", "specs_network_adapter", "specs_nic"]), None)
   if adapter_key:
       logger.debug(f"Enhancing adapter data from '{specifics[adapter_key]}'")
       tokens = tokenize_with_slash_splitting(specifics[adapter_key], logger)
       consumed = set()
       for extractor in adapter_extractors:
           try:
               matches = extractor.extract(tokens, consumed)
               logger.debug(f"Extractor {extractor.name} returned: {matches}")
               for match_indices in matches:
                   extracted = extractor.process_match(tokens, match_indices)
                   for key, value in extracted.items():
                       specifics[f"specs_{key}"] = value
                       logger.debug(f"Added enhanced adapter field: specs_{key} = {value}")
                   for idx in match_indices:
                       if isinstance(idx, int):
                           consumed.add(idx)
           except Exception as e:
               logger.error(f"Error in adapter extractor {extractor.name}: {str(e)}")

   # POST-PROCESSING: Normalize GPU models that are just numbers
   if 'specs_gpu_model' in specifics and 'specs_gpu_series' in specifics:
       if specifics['specs_gpu_series'].upper() == 'GEFORCE':
           model = specifics['specs_gpu_model'].strip()
           # Check if model is just a number (like "1070", "2080", etc.)
           if re.match(r'^\d{3,4}$', model):
               model_num = int(model)
               # Determine series based on model number
               if model_num >= 2000:  # RTX series (2000, 3000, 4000+)
                   prefix = "RTX"
               elif model_num >= 600:  # GTX series (600, 700, 900, 1000)
                   prefix = "GTX"
               else:  # GT series for lower numbers
                   prefix = "GT"
               
               normalized_model = f"{prefix} {model}"
               specifics['specs_gpu_model'] = normalized_model
               logger.debug(f"Normalized GPU model from '{model}' to '{normalized_model}'")

   specifics['__key_order__'] = key_order
   logger.debug(f"Completed item specifics parsing with {len(specifics)} entries")
   return specifics
   
def process_table_ram_data(entry: Dict, ram_extractors: List, logger: logging.Logger) -> None:
    """Process RAM data for a table entry and extract size, config, speed, and type."""
    if "table_ram" not in entry:
        return
        
    logger.debug(f"Enhancing RAM data for table entry: '{entry['table_ram']}'")
    
    ram_text = entry['table_ram']

    # FAST PATH: Handle simple size-only values like "8GB", "16 GB", or unit variants
    try:
        simple_size_match = re.match(r'^\s*(\d+(?:\.\d+)?)\s*(GB|TB|MB)\s*$', ram_text, re.IGNORECASE)
        if simple_size_match:
            size_val = float(simple_size_match.group(1))
            unit = simple_size_match.group(2).upper()
            # Normalize to GB
            if unit == 'TB':
                size_val *= 1024.0
            elif unit == 'MB':
                size_val /= 1024.0
            normalized = f"{int(size_val) if abs(size_val - int(size_val)) < 1e-6 else size_val}GB"
            entry["table_ram_size"] = normalized
            # Keep description aligned to size when no other info is present
            entry.setdefault("table_ram_description", normalized)
            logger.debug(f"RAM: Detected simple size-only value -> table_ram_size = {normalized}")
            return
    except Exception:
        # Non-fatal; continue with comprehensive parsing below
        pass
    
    # NEW: Handle patterns like "16GB (2x8GB) + 32GB (2x16GB Integrated)" where multiple
    # parenthetical RAM configurations are joined by a "+" sign. This will create a
    # combined total size and a unified configuration list.
    multi_plus_pattern = re.search(r'(\d+(?:gb|tb|mb))\s*\(\s*(\d+\s*x\s*\d+(?:gb|tb|mb))[^)]*\)\s*\+\s*(\d+(?:gb|tb|mb))\s*\(\s*(\d+\s*x\s*\d+(?:gb|tb|mb))[^)]*\)', ram_text, re.IGNORECASE)
    if multi_plus_pattern:
        size1_str = multi_plus_pattern.group(1).lower()
        config1_raw = multi_plus_pattern.group(2)
        size2_str = multi_plus_pattern.group(3).lower()
        config2_raw = multi_plus_pattern.group(4)

        def _normalise_config(cfg: str) -> str:
            """Remove whitespace and uppercase units, e.g. '2 x 8gb' -> '2x8GB'"""
            return cfg.replace(' ', '').upper()

        config1 = _normalise_config(config1_raw)
        config2 = _normalise_config(config2_raw)

        def _to_gb(size_str: str) -> float:
            m = re.match(r'(\d+(?:\.\d+)?)(gb|tb|mb)', size_str, re.IGNORECASE)
            if not m:
                return 0.0
            val = float(m.group(1))
            unit = m.group(2).lower()
            if unit == 'tb':
                return val * 1024
            if unit == 'mb':
                return val / 1024
            return val  # already GB

        total_gb = _to_gb(size1_str) + _to_gb(size2_str)
        total_size_str = f"{int(total_gb) if total_gb.is_integer() else total_gb}GB"

        entry["table_ram_size"] = total_size_str
        entry["table_ram_config"] = f"{config1}, {config2}"
        entry["table_ram_description"] = entry["table_ram_config"]
        logger.debug(f"Extracted multi-part RAM config: total={total_size_str}, config={entry['table_ram_config']}")
        return

    # NEW: Handle single-parentheses plus-separated configs like "12GB (1x4GB + 1x8GB)"
    # or cases where only the parenthetical exists like "(1x4GB + 1x8GB) DDR4"
    plus_inside_paren_match = re.search(r'\(([^)]+)\)', ram_text, re.IGNORECASE)
    if plus_inside_paren_match and '+' in plus_inside_paren_match.group(1):
        inner_text = plus_inside_paren_match.group(1)
        cfg_matches = re.findall(r'(\d+)\s*[xX]\s*(\d+)\s*(GB|TB|MB)', inner_text, re.IGNORECASE)
        if cfg_matches:
            formatted_cfgs = [f"{count}x{size}{unit.upper()}" for count, size, unit in cfg_matches]
            entry["table_ram_config"] = ', '.join(formatted_cfgs)

            # Prefer computing total from the configs inside parentheses
            total_gb = 0.0
            for count, size, unit in cfg_matches:
                c = int(count)
                s = int(size)
                u = unit.lower()
                mult = 1.0
                if u == 'tb':
                    mult = 1024.0
                elif u == 'mb':
                    mult = 1.0 / 1024.0
                total_gb += c * s * mult
            if total_gb > 0:
                entry["table_ram_size"] = f"{int(total_gb) if abs(total_gb - int(total_gb)) < 1e-6 else total_gb}GB"
            else:
                # If a total size is present anywhere in the text, capture it
                total_size_match = re.search(r'(\d+(?:gb|tb|mb))', ram_text, re.IGNORECASE)
                if total_size_match:
                    entry["table_ram_size"] = total_size_match.group(1).upper()

            # Keep description aligned with config for readability
            entry["table_ram_description"] = entry["table_ram_config"]
            logger.debug(f"Extracted plus-separated RAM configs inside parentheses -> config: {entry['table_ram_config']}")
            return
    
    # NEW: Handle parentheses with config + built-in/onboard/soldered/integrated mention
    # Example: "16GB (1x8GB, 8GB built-in)" -> 2x8GB
    paren_with_built_in = re.search(r'\(([^)]{0,300})\)', ram_text, re.IGNORECASE)
    if paren_with_built_in:
        inner_text = paren_with_built_in.group(1)
        cfg_matches = re.findall(r'(\d+)\s*[xX]\s*(\d+)\s*(GB|TB|MB)', inner_text, re.IGNORECASE)
        # Capture: "8GB built-in", "8 GB onboard", "8GB on-board", "8GB on board",
        #          "8GB soldered", "8GB soldered RAM", "8GB soldered memory",
        #          "8GB integrated", "8GB fixed"
        built_in_sizes = [int(x) for x in re.findall(
            r'(\d+)\s*GB(?:\s*(?:RAM|MEMORY))?\s*(?:built[\-\s]?in|on[\-\s]?board|on\s*board|soldered(?:\s*(?:ram|memory))?|integrated|fixed)',
            inner_text,
            flags=re.IGNORECASE,
        )]

        if cfg_matches and built_in_sizes:
            # Prefer the first explicit config inside parentheses
            count_str, size_str, unit = cfg_matches[0]
            current_count = int(count_str)
            per_stick_gb = int(size_str)
            additional_matches = sum(1 for s in built_in_sizes if s == per_stick_gb)

            if additional_matches > 0:
                total_count = current_count + additional_matches
                entry["table_ram_config"] = f"{total_count}x{per_stick_gb}{unit.upper()}"
                # Keep description aligned
                entry["table_ram_description"] = entry["table_ram_config"]
                logger.debug(
                    f"Adjusted RAM config using built-in mention(s): {entry['table_ram_config']} from inner '{inner_text}'"
                )
                return
            else:
                # Sizes differ: append built-in as 1x entries
                formatted_cfgs = [f"{int(count_str)}x{int(size_str)}{unit.upper()}"]
                formatted_cfgs += [f"1x{size}GB" for size in built_in_sizes]
                entry["table_ram_config"] = ', '.join(formatted_cfgs)
                entry["table_ram_description"] = entry["table_ram_config"]
                logger.debug(
                    f"Combined RAM config with built-in sizes (mismatched): {entry['table_ram_config']} from inner '{inner_text}'"
                )
                return
        elif not cfg_matches and built_in_sizes:
            # Only built-in sizes noted; try to derive config from total size if divisible
            total_match_anywhere = re.search(r'(\d+)(GB|TB|MB)', ram_text, re.IGNORECASE)
            if total_match_anywhere:
                total_val = int(total_match_anywhere.group(1))
                total_unit = total_match_anywhere.group(2).upper()
                # Prefer larger built-in size
                for s in sorted(set(built_in_sizes), reverse=True):
                    if total_unit == 'GB' and s > 0 and total_val % s == 0:
                        entry["table_ram_config"] = f"{total_val // s}x{s}GB"
                        entry["table_ram_description"] = entry["table_ram_config"]
                        logger.debug(
                            f"Derived RAM config from built-in mention only: {entry['table_ram_config']} from inner '{inner_text}'"
                        )
                        return

    # NEW: Check for format like "24GB RAM (1x16GB)( 8GB soldered)" first
    complex_soldered_pattern = re.search(r'(\d+(?:gb|tb|mb))\s+ram\s+\(([^)]+)\)\s*\(\s*(\d+(?:gb|tb|mb))\s+soldered\s*\)', ram_text, re.IGNORECASE)
    if complex_soldered_pattern:
        total_size = complex_soldered_pattern.group(1).upper()
        removable_config = complex_soldered_pattern.group(2).strip()
        soldered_size = complex_soldered_pattern.group(3).lower()
        
        entry["table_ram_size"] = total_size
        
        # Convert soldered memory to 1x format as requested
        soldered_config = f"1x{soldered_size}"
        
        # Combine configs: removable + soldered
        entry["table_ram_config"] = f"{removable_config}, {soldered_config}"
        entry["table_ram_description"] = f"{removable_config} + {soldered_size} soldered"
        
        logger.debug(f"Extracted complex soldered RAM: total={total_size}, config={entry['table_ram_config']}")
        return
    
    # Check for format like "20GB (4GB Soldered + 16GB DIMM)" first
    total_with_description_pattern = re.search(r'(\d+(?:gb|tb|mb))\s*\(\s*([^)]*(?:\+|soldered|dimm)[^)]*)\s*\)', ram_text, re.IGNORECASE)
    if total_with_description_pattern:
        total_size = total_with_description_pattern.group(1).upper()
        description = total_with_description_pattern.group(2)
        
        entry["table_ram_size"] = total_size
        entry["table_ram_description"] = description
        logger.debug(f"Extracted RAM total: {total_size} with description: {description}")
        
        # Also check for speed and type in the description
        speed_match = re.search(r'(\d+)\s*mhz', description, re.IGNORECASE)
        if speed_match:
            entry["table_ram_speed_grade"] = f"{speed_match.group(1)}MHz"
            logger.debug(f"Extracted RAM speed from description: {entry['table_ram_speed_grade']}")
        
        type_match = re.search(r'(ddr[0-9]?)', description, re.IGNORECASE)
        if type_match:
            entry["table_ram_type"] = type_match.group(1).upper()
            logger.debug(f"Extracted RAM type from description: {entry['table_ram_type']}")
        
        return
    
    # NEW: Capture a total size that appears BEFORE any parentheses even if text is in between,
    # e.g., "8GB DDR4-2400 SDRAM (2x4GB)" -> table_ram_size = 8gb
    if 'table_ram_size' not in entry:
        size_before_paren_match = re.search(r'(\d+(?:gb|tb|mb))(?=[^()]*\()', ram_text, re.IGNORECASE)
        if size_before_paren_match:
            entry["table_ram_size"] = size_before_paren_match.group(1).upper()
            logger.debug(f"Captured RAM size before parentheses: {entry['table_ram_size']}")

    # Check for complex node format 
    if re.search(r'\d+gb.*?mhz.*?\(.*?x.*?\).*?node', ram_text.lower()):
        logger.debug("Detected complex RAM node format")
        from configs.extractor_ram import parse_complex_ram_format
        complex_result = parse_complex_ram_format(ram_text, logger)
        for key, value in complex_result.items():
            entry[f"table_{key}"] = value
            logger.debug(f"Added complex RAM field: table_{key} = {value}")
        return
    
    # New: handle multiple parentheses config groups like "(2x2GB) (2x8GB)"
    paren_configs = re.findall(r'\(\s*(\d+\s*x\s*\d+(?:gb|tb|mb))\s*\)', ram_text, re.IGNORECASE)
    if len(paren_configs) > 1:
        total_match = re.search(r'(\d+(?:gb|tb|mb))', ram_text, re.IGNORECASE)
        if total_match:
            entry["table_ram_size"] = total_match.group(1).upper()
        
        formatted = [cfg.replace(' ', '').upper() for cfg in paren_configs]

        # Sort by module size (e.g., 2x8GB before 2x2GB)
        def module_size(cfg_str):
            size_match = re.search(r'x(\d+)(?:GB|TB|MB)', cfg_str, re.IGNORECASE)
            return int(size_match.group(1)) if size_match else 0

        formatted_sorted = sorted(formatted, key=module_size, reverse=True)
        entry["table_ram_config"] = ', '.join(formatted_sorted)

        # Keep a unified description identical to config for readability
        entry["table_ram_description"] = entry["table_ram_config"]
        logger.debug(f"Extracted multiple RAM parentheses configs -> config: {entry['table_ram_config']}")
        return
    
    # Check for "XGB(YxZGB)" format like "8GB(1x8GB)"
    elif re.search(r'(\d+(?:gb|tb|mb))\s*\(\s*(\d+\s*x\s*\d+(?:gb|tb|mb))\s*\)', ram_text.lower()):
        size_config_match = re.search(r'(\d+(?:gb|tb|mb))\s*\(\s*(\d+\s*x\s*\d+(?:gb|tb|mb))\s*\)', ram_text.lower())
        if size_config_match:
            ram_size = size_config_match.group(1).upper()
            ram_config = size_config_match.group(2).replace(' ', '').upper()
            
            entry["table_ram_size"] = ram_size
            entry["table_ram_config"] = ram_config
            logger.debug(f"Extracted RAM size: {ram_size} and config: {ram_config}")
            
            # Also check for speed and type in the same text
            speed_match = re.search(r'(\d+)\s*mhz', ram_text.lower())
            if speed_match:
                entry["table_ram_speed_grade"] = f"{speed_match.group(1)}MHz"
                logger.debug(f"Extracted RAM speed: {entry['table_ram_speed_grade']}")
            
            type_match = re.search(r'(ddr[0-9]?)', ram_text.lower())
            if type_match:
                entry["table_ram_type"] = type_match.group(1).upper()
                logger.debug(f"Extracted RAM type: {entry['table_ram_type']}")
        return
    
    else:
        # Check for simple "Total:" pattern first
        total_match = re.search(r'total:\s*(\d+(?:gb|tb|mb))', ram_text, re.IGNORECASE)
        if total_match:
            entry["table_ram_size"] = total_match.group(1).upper()
            logger.debug(f"Found RAM Total size: {entry['table_ram_size']}")
        
        # Enhanced parsing for multiple configs with speed/type separation
        # Split by common delimiters that separate different RAM configs
        configs = re.split(r'(?:\s*,\s*|\s*;\s*|\s+and\s+)', ram_text)
        
        ram_configs = []
        ram_speeds = []
        ram_types = []
        
        for config in configs:
            config = config.strip()
            if not config:
                continue
            
            logger.debug(f"Processing RAM config: '{config}'")
            
            # Extract configuration (XxYGB or X x YGB)
            config_match = re.search(r'(\d+)\s*x\s*(\d+(?:gb|tb|mb))', config.lower())
            if config_match:
                ram_configs.append(f"{config_match.group(1)}x{config_match.group(2).upper()}")
            
            # Extract speed (1600MHz, 2667MHz, etc.)
            speed_match = re.search(r'(\d+)\s*mhz', config.lower())
            if speed_match:
                ram_speeds.append(f"{speed_match.group(1)}MHz")
            
            # Extract DDR type
            type_match = re.search(r'(ddr[0-9]?)', config.lower())
            if type_match:
                ram_types.append(type_match.group(1).upper())
        
        # Set the extracted values
        if ram_configs:
            entry["table_ram_config"] = ', '.join(ram_configs)
            logger.debug(f"Set table_ram_config: {entry['table_ram_config']}")

            # NEW: Adjust RAM configuration when specs are provided per node
            # Example format handled:
            #   "RAM Total: 512GB (128GB per Node) RAM Config: 4 x 32GB (per Node)"
            # If total divides exactly by the per-node size, we can compute total modules.
            try:
                if ('table_ram_size' in entry and
                    re.search(r'per\s*node', ram_text, re.IGNORECASE)):

                    total_match = re.match(r'^(\d+)(gb|tb|mb)', entry['table_ram_size'], re.IGNORECASE)
                    config_match = re.match(r'^(\d+)x(\d+)(gb|tb|mb)', entry['table_ram_config'].replace(' ', '').lower())
                    per_node_match = re.search(r'\(\s*(\d+)(gb|tb|mb)\s*per\s*node\s*\)', ram_text, re.IGNORECASE)

                    if total_match and config_match:
                        total_value = int(total_match.group(1))
                        total_unit = total_match.group(2).lower()

                        modules_per_node = int(config_match.group(1))
                        module_size_value = int(config_match.group(2))
                        module_unit = config_match.group(3).lower()

                        # Determine number of nodes using explicit per-node size if provided
                        nodes = None
                        if per_node_match:
                            per_node_value = int(per_node_match.group(1))
                            per_node_unit = per_node_match.group(2).lower()
                            if per_node_unit == total_unit and per_node_value > 0:
                                nodes = total_value // per_node_value
                                # Validate exact division
                                if total_value % per_node_value != 0:
                                    nodes = None
                        else:
                            # Fallback: infer per-node size from modules per node and module size
                            if module_unit == total_unit:
                                per_node_value = modules_per_node * module_size_value
                                if per_node_value > 0 and total_value % per_node_value == 0:
                                    nodes = total_value // per_node_value

                        # If a valid node count is determined, update total module count
                        if nodes and nodes > 1:
                            total_modules = nodes * modules_per_node
                            entry['table_ram_config'] = f"{total_modules}x{module_size_value}{module_unit.upper()}"
                            logger.debug(
                                f"Adjusted RAM config using per-node data: {entry['table_ram_config']}")
            except Exception as e:
                logger.error(f"Error adjusting per-node RAM config: {str(e)}")
        
        if ram_speeds:
            entry["table_ram_speed_grade"] = ', '.join(ram_speeds)
            logger.debug(f"Set table_ram_speed_grade: {entry['table_ram_speed_grade']}")
        
        if ram_types:
            entry["table_ram_type"] = ', '.join(ram_types)
            logger.debug(f"Set table_ram_type: {entry['table_ram_type']}")
        
        # Continue with regular extractors for any missed fields
        tokens = tokenize_with_slash_splitting(entry['table_ram'], logger)
        consumed = set()
        for extractor in ram_extractors:
            try:
                # Skip extractors we've already handled
                if extractor.name in ["ram_size", "ram_config", "ram_speed_grade", "ram_type"]:
                    if f"table_{extractor.name}" in entry:
                        continue
                        
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned: {matches}")
                for match_indices in matches:
                    extracted = extractor.process_match(tokens, match_indices)
                    for key, value in extracted.items():
                        if f"table_{key}" not in entry:  # Don't overwrite
                            entry[f"table_{key}"] = value
                            logger.debug(f"Added enhanced RAM field to table: table_{key} = {value}")
                        for idx in match_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
            except Exception as e:
                logger.error(f"Error in RAM extractor {extractor.name}: {str(e)}")
                
def process_multi_line_value(value: str, key: str) -> str:
    """Process multi-line values that may contain multiple instances separated by newlines."""
    if not value:
        return value
    
    # For technical specs, check if the value contains patterns that indicate multiple instances
    if key in ['table_cpu', 'table_ram', 'table_ssd', 'table_hard_drive', 'table_videocard']:
        # Look for patterns that indicate multiple instances in a single line
        # For CPU: "Intel Core i7-1065G7 1.30GHz Intel Core i7-1185G7 3.00GHz"
        if key == 'table_cpu':
            # Split on CPU model patterns
            cpu_pattern = r'(Intel\s+Core\s+i[3579]-\d+[A-Z0-9]*\s+[\d.]+GHz)'
            matches = re.findall(cpu_pattern, value, re.IGNORECASE)
            if len(matches) > 1:
                return '\n'.join(matches)
        
        # For RAM: "16GB 3733MHz LPDDR4 16GB 4267MHz LPDDR4"
        elif key == 'table_ram':
            # Split on complete RAM configurations
            ram_pattern = r'(\d+GB\s+(?:\([^)]+\)\s+)?\d+MHz\s+[A-Z0-9]+)'
            matches = re.findall(ram_pattern, value, re.IGNORECASE)
            if len(matches) > 1:
                return '\n'.join(matches)
        
        # For Storage: "256GB NVMe 256GB NVMe"
        elif key in ['table_hard_drive', 'table_ssd', 'table_storage']:
            # Split on storage capacity + type patterns
            storage_pattern = r'(\d+(?:GB|TB)\s+[A-Z0-9]+)'
            matches = re.findall(storage_pattern, value, re.IGNORECASE)
            if len(matches) > 1:
                return '\n'.join(matches)
        
        # For GPU: "Intel Iris Plus Intel Iris Xe"
        elif key == 'table_videocard':
            # Split on GPU brand + series patterns
            gpu_pattern = r'(Intel\s+[A-Za-z\s]+?)(?=\s+Intel\s+|$)'
            matches = re.findall(gpu_pattern, value, re.IGNORECASE)
            if len(matches) > 1:
                cleaned_matches = [match.strip() for match in matches if match.strip()]
                return '\n'.join(cleaned_matches)
    
    # For descriptive fields, join with spaces
    if key in ['table_defects', 'table_notes', 'table_missing_components']:
        # Split by newlines and clean each line, then join with spaces
        lines = [line.strip() for line in value.split('\n') if line.strip()]
        if len(lines) > 1:
            return ' '.join(lines)
    
    # Default: return as-is
    return value
    
def parse_basic_table_structure(text: str, logger: logging.Logger) -> List[Dict]:
    """Parse the basic table structure without enhancement processing."""
    entries = []
    current_entry = {}
    key_order = []
    in_table = False
    current_key = None
    accumulated_value = []
    
    key_mapping = {
        'make': 'table_brand',
        'model': 'table_model',
        'processor (cpu)': 'table_cpu',
        'cpu': 'table_cpu',
        'memory (ram)': 'table_ram',
        'ram': 'table_ram',
        'storage': 'table_ssd',
        'ssd': 'table_ssd',
        'hard drive': 'table_hard_drive',
        'screen size': 'table_screen_composite',
        'screensize': 'table_screen_composite',
        'display': 'table_screen_composite',
        'resolution': 'table_screen_resolution',
        'video card': 'table_videocard',
        'graphics card': 'table_videocard',
        'gpu': 'table_videocard',
        'battery': 'table_battery',
        'webcam': 'table_webcam',
        'ethernet': 'table_ethernet',
        'wifi': 'table_wifi',
        'operating system': 'table_os',
        'os': 'table_os',
        'defects': 'table_defects',
        'missing components': 'table_missing_components',
        'notes': 'table_notes',
        'test result': 'table_test_result',
        'network / carrier': 'table_network_carrier',
        'network carrier': 'table_network_carrier',
        'battery health': 'table_battery_health',
        'battery health as is': 'table_battery_health',
        'version': 'table_version',
        'ios version': 'table_version',
        'missing': 'table_missing',
        'serial number / imei': 'table_serial_number_imei',
        'ports': 'table_ports',
        'poe specs': 'table_poe_specs',
        'rack size': 'table_rack_size',
        'power supply': 'table_power_supply',
        'form factor': 'table_form_factor',
        'device type': 'table_device_type',
        'touch screen': 'table_screen_touch',
        'panel type': 'table_screen_panel_type',
        'switch': 'table_switch',
        'network switch': 'table_switch',
        'adapter': 'table_adapter',
        'network adapter': 'table_adapter',
        'nic': 'table_adapter',
        'network interface': 'table_adapter',
    }

    def store_accumulated_value():
        """Helper function to store accumulated value with proper multi-line handling."""
        if current_key and accumulated_value:
            mapped_key = key_mapping.get(current_key.lower(), f"table_{standardize_key(current_key)}")
            if mapped_key == 'table_notes':
                value = ', '.join([v.strip() for v in accumulated_value if v.strip()])
            else:
                # Join accumulated lines and then process for multi-line handling
                joined_value = ' '.join(accumulated_value)
                value = clean_text(joined_value)
                value = process_multi_line_value(value, mapped_key)
            current_entry[mapped_key] = value
            if mapped_key not in [k for k, _ in key_order]:
                key_order.append((mapped_key, current_key))
                logger.debug(f"Stored table data key-value pair: '{mapped_key}' = '{value}'")

    for line in text.split('\n'):
        line = line.strip().lstrip('\ufeff')
        if not line:
            continue
        if "=== TABLE DATA ===" in line or line.startswith('[table_entry_count_key]'):
            in_table = True
            logger.debug("Entered table data section")
            continue
        elif in_table and "===" in line:
            store_accumulated_value()
            if current_entry:
                current_entry['__key_order__'] = key_order
                entries.append(current_entry)
                logger.debug(f"Completed table entry {len(entries)}")
                current_entry = {}
                key_order = []
            in_table = False
            current_key = None
            accumulated_value = []
            logger.debug("Exited table data section")
            continue
        if in_table:
            if line.startswith("Entry "):
                store_accumulated_value()
                if current_entry:
                    current_entry['__key_order__'] = key_order
                    entries.append(current_entry)
                    logger.debug(f"Completed table entry {len(entries)}")
                current_entry = {}
                key_order = []
                current_key = None
                accumulated_value = []
                logger.debug(f"Starting new table entry: Entry {len(entries) + 1}")
            else:
                tokens = tokenize(line, logger)
                colon_index = -1
                key_tokens = []
                for i, token in enumerate(tokens):
                    if ':' in token:
                        colon_index = i
                        if token != ':':
                            key_part = token[:token.index(':')]
                            if key_part:
                                key_tokens.append(key_part)
                        break
                    key_tokens.append(token)
                if colon_index != -1:
                    store_accumulated_value()
                    current_key = ' '.join(key_tokens).strip()
                    accumulated_value = tokens[colon_index+1:] if colon_index + 1 < len(tokens) else []
                    logger.debug(f"Identified table data key: '{current_key}'")
                elif current_key:
                    accumulated_value.append(line)
                    logger.debug(f"Accumulating value for table data key '{current_key}': '{line}'")

    store_accumulated_value()
    if current_entry:
        current_entry['__key_order__'] = key_order
        entries.append(current_entry)
        logger.debug(f"Completed table entry {len(entries)}")

    return entries
    
def process_multi_line_cpu_data(entry: Dict, cpu_extractors: List, logger: logging.Logger) -> None:
    """Process CPU data that may contain multiple CPUs separated by newlines."""
    if "table_cpu" not in entry:
        return
        
    logger.debug(f"Enhancing CPU data for table entry: '{entry['table_cpu']}'")
    cpu_value = entry['table_cpu']
    
    # Split by newlines to handle multiple CPU entries
    cpu_lines = [line.strip() for line in cpu_value.split('\n') if line.strip()]
    
    if len(cpu_lines) > 1:
        # Process each CPU line separately and create numbered fields
        for i, cpu_line in enumerate(cpu_lines, 1):
            clean_cpu_text = clean_text(cpu_line)
            tokens = tokenize_with_slash_splitting(clean_cpu_text, logger)
            consumed = set()
            for extractor in cpu_extractors:
                logger.debug(f"Applying CPU extractor: {extractor.name} to line {i}")
                try:
                    matches = extractor.extract(tokens, consumed)
                    logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                    for match_indices in matches:
                        extracted = extractor.process_match(tokens, match_indices)
                        for key, value in extracted.items():
                            # Create numbered fields for multiple CPUs
                            entry[f"table_{key}{i}"] = value
                            # Also set base field for first CPU for compatibility
                            if i == 1:
                                entry[f"table_{key}"] = value
                            logger.debug(f"Added enhanced CPU field to table: table_{key}{i} = {value}")
                        for idx in match_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                except Exception as e:
                    logger.error(f"Error in CPU extractor {extractor.name}: {str(e)}")
    else:
        # Single CPU entry - process normally
        clean_cpu_text = clean_text(entry['table_cpu'])
        tokens = tokenize_with_slash_splitting(clean_cpu_text, logger)
        consumed = set()
        avoid_overwrite_for_slash = '/' in clean_cpu_text
        for extractor in cpu_extractors:
            logger.debug(f"Applying CPU extractor: {extractor.name}")
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned matches: {matches}")
                if matches:
                    # If multiple matches were found on a single line, create numbered fields (1, 2, ...)
                    if len(matches) > 1:
                        for i, match_indices in enumerate(matches, start=1):
                            # Flatten indices and consume
                            flat_indices = [idx for idx in match_indices if isinstance(idx, int)]
                            for idx in flat_indices:
                                consumed.add(idx)
                            extracted = extractor.process_match(tokens, flat_indices)
                            for key, value in extracted.items():
                                numbered_key = f"table_{key}{i}"
                                entry[numbered_key] = value
                                # Also set base field for the first CPU for compatibility
                                base_key = f"table_{key}"
                                if i == 1 and ((not avoid_overwrite_for_slash) or (base_key not in entry or not entry[base_key])):
                                    entry[base_key] = value
                                logger.debug(f"Added enhanced CPU field to table: {numbered_key} = {value}")
                    else:
                        # Single match - use base keys (with optional no-overwrite on slash lines)
                        match_indices = matches[0]
                        flat_indices = [idx for idx in match_indices if isinstance(idx, int)]
                        for idx in flat_indices:
                            consumed.add(idx)
                        extracted = extractor.process_match(tokens, flat_indices)
                        for key, value in extracted.items():
                            table_key = f"table_{key}"
                            if (not avoid_overwrite_for_slash) or (table_key not in entry or not entry[table_key]):
                                entry[table_key] = value
                                logger.debug(f"Added enhanced CPU field to table: {table_key} = {value}")
                            else:
                                logger.debug(
                                    f"Preserving existing {table_key}={entry[table_key]} (skipping later value '{value}')"
                                )
            except Exception as e:
                logger.error(f"Error in CPU extractor {extractor.name}: {str(e)}")

def process_multi_line_ram_data(entry: Dict, ram_extractors: List, logger: logging.Logger) -> None:
    """Process RAM data that may contain multiple RAM configurations separated by newlines."""
    if "table_ram" not in entry:
        return
        
    logger.debug(f"Enhancing RAM data for table entry: '{entry['table_ram']}'")
    ram_value = entry['table_ram']

    # First, attempt a full-text parse on the multi-line RAM block, but only if it
    # does NOT clearly represent multiple separate RAM entries (e.g., comma or slash
    # separated with multiple sizes). This preserves multi-entry parsing like
    # "8GB (2x4GB), 12GB (1x8GB) (1x4GB)".
    looks_like_multiple = False
    try:
        norm_val_for_detect = normalize_units(ram_value)
        trailing_ctx_match = re.match(r'(.+?)\s*(ram|memory)\s*$', norm_val_for_detect, re.IGNORECASE)
        base_text_detect = trailing_ctx_match.group(1) if trailing_ctx_match else ram_value
        if '/' in base_text_detect:
            candidate_parts_detect = [part.strip() for part in base_text_detect.split('/') if part.strip()]
        else:
            candidate_parts_detect = [part.strip() for part in re.split(r',(?![^()]*\))', base_text_detect) if part.strip()]
        num_with_sizes_detect = sum(1 for p in candidate_parts_detect if re.search(r'\d+(?:\.\d+)?\s*(gb|tb|mb)', p, re.IGNORECASE))
        looks_like_multiple = len(candidate_parts_detect) > 1 and num_with_sizes_detect >= 2
        # Additional detection: repeated "Size (NxY)" groups without explicit delimiters
        if not looks_like_multiple:
            try:
                repeated_pattern_detect = re.compile(r'(\d+(?:\.\d+)?\s*(?:GB|TB|MB))\s*\(\s*\d+\s*[xX]\s*\d+\s*(?:GB|TB|MB)\s*\)', re.IGNORECASE)
                matches = list(repeated_pattern_detect.finditer(base_text_detect))
                if len(matches) >= 2:
                    looks_like_multiple = True
            except Exception:
                pass
    except Exception:
        looks_like_multiple = False

    if not looks_like_multiple:
        try:
            temp_full = { 'table_ram': ram_value }
            process_table_ram_data(temp_full, ram_extractors, logger)
            if any(k in temp_full for k in ("table_ram_config", "table_ram_size", "table_ram_description")):
                # If parsing succeeded at full-block level, propagate results and return
                for k in ("table_ram_size", "table_ram_config", "table_ram_speed_grade", "table_ram_type", "table_ram_description"):
                    if k in temp_full and temp_full[k]:
                        entry[k] = temp_full[k]
                logger.debug("RAM: Full-block parse succeeded; using combined results before line splitting")
                return
        except Exception as e:
            logger.debug(f"RAM: Full-block parse attempt failed or yielded nothing useful: {e}")
    
    # Treat slash- or comma-separated single-line RAM as multiple entries when they denote distinct configs
    ram_lines = None
    if '\n' not in ram_value and ('/' in ram_value or ',' in ram_value):
        norm_val = normalize_units(ram_value)
        trailing_ctx_match = re.match(r'(.+?)\s*(ram|memory)\s*$', norm_val, re.IGNORECASE)
        base_text = trailing_ctx_match.group(1) if trailing_ctx_match else ram_value

        if '/' in base_text:
            candidate_parts = [part.strip() for part in base_text.split('/') if part.strip()]
        else:
            # Split on commas that are not inside parentheses to avoid breaking descriptions
            candidate_parts = [part.strip() for part in re.split(r',(?![^()]*\))', base_text) if part.strip()]

        # Validate that this looks like multiple standalone RAM configs (each with a size)
        num_with_sizes = sum(1 for p in candidate_parts if re.search(r'\d+(?:\.\d+)?\s*(gb|tb|mb)', p, re.IGNORECASE))
        if len(candidate_parts) > 1 and num_with_sizes >= 2:
            if trailing_ctx_match:
                ctx = trailing_ctx_match.group(2)
                ram_lines = [f"{part} {ctx}" for part in candidate_parts]
            else:
                ram_lines = candidate_parts

    # NEW: Detect repeated groups like "8GB (1x8GB) 1600MHz DDR3  8GB (2x4GB) 1600MHz DDR3" and split into lines
    if ram_lines is None and ('\n' not in ram_value):
        try:
            base_text_for_split = normalize_units(ram_value)
            repeated_pattern = re.compile(r'(\d+(?:\.\d+)?\s*(?:GB|TB|MB))\s*\(\s*(\d+)\s*[xX]\s*(\d+)\s*(GB|TB|MB)\s*\)\s*(\d+\s*MHz)?\s*(DDR[0-9]x?)?', re.IGNORECASE)
            groups = list(repeated_pattern.finditer(base_text_for_split))
            if len(groups) >= 2:
                candidate_lines = []
                for g in groups:
                    size = g.group(1).upper().replace(' ', '')
                    count = g.group(2)
                    per = g.group(3)
                    unit = g.group(4).upper()
                    speed = (g.group(5) or '').strip()
                    ddr = (g.group(6) or '').upper().strip()
                    cfg = f"{count}x{per}{unit}"
                    parts = [size, f"({cfg})"]
                    if speed:
                        sp = re.sub(r'\s+', '', speed)
                        sp = re.sub(r'mhz$', 'MHz', sp, flags=re.IGNORECASE)
                        parts.append(sp)
                    if ddr:
                        parts.append(ddr)
                    candidate_lines.append(' '.join(parts))
                if candidate_lines:
                    ram_lines = candidate_lines
        except Exception:
            pass

    if ram_lines is None:
        # Split by newlines to handle multiple RAM entries
        ram_lines = [line.strip() for line in ram_value.split('\n') if line.strip()]
    
    if len(ram_lines) > 1:
        # Process each RAM line separately and create numbered fields
        collected_configs = []
        for i, ram_line in enumerate(ram_lines, 1):
            # Create a temporary entry for this RAM line
            temp_entry = {'table_ram': ram_line}
            process_table_ram_data(temp_entry, ram_extractors, logger)
            # Ensure a RAM size exists for this line; derive if missing
            if not temp_entry.get('table_ram_size'):
                try:
                    # 1) Try to parse explicit leading total like "12GB"
                    size_match = re.search(r'(\d+(?:\.\d+)?)(gb|tb|mb)', ram_line, re.IGNORECASE)
                    if size_match:
                        size_val = float(size_match.group(1))
                        unit = size_match.group(2).upper()
                        # Normalize to GB if TB/MB encountered
                        if unit == 'TB':
                            size_val *= 1024.0
                            unit = 'GB'
                        elif unit == 'MB':
                            size_val /= 1024.0
                            unit = 'GB'
                        temp_entry['table_ram_size'] = f"{int(size_val) if abs(size_val - int(size_val)) < 1e-6 else size_val}{unit}"
                    # 2) Derive from config(s) like "1x8GB, 1x4GB"
                    if not temp_entry.get('table_ram_size') and temp_entry.get('table_ram_config'):
                        total_gb = 0.0
                        for cfg_part in [p.strip() for p in temp_entry['table_ram_config'].split(',') if p.strip()]:
                            m = re.search(r'^(\d+)x(\d+)(GB|TB|MB)$', cfg_part.strip(), re.IGNORECASE)
                            if m:
                                count = int(m.group(1))
                                per = int(m.group(2))
                                u = m.group(3).upper()
                                if u == 'TB':
                                    total_gb += count * per * 1024.0
                                elif u == 'MB':
                                    total_gb += count * per / 1024.0
                                else:
                                    total_gb += count * per
                        if total_gb > 0:
                            temp_entry['table_ram_size'] = f"{int(total_gb) if abs(total_gb - int(total_gb)) < 1e-6 else total_gb}GB"
                except Exception as e:
                    logger.debug(f"RAM: Failed to derive size for part '{ram_line}': {e}")
            
            # Collect configs for a combined description/config on the base entry
            if 'table_ram_config' in temp_entry and temp_entry['table_ram_config']:
                collected_configs.append(temp_entry['table_ram_config'])
            
            # Copy extracted fields to main entry with numbering
            for key, value in temp_entry.items():
                if key.startswith('table_ram') and key != 'table_ram':
                    # Add number suffix to the key
                    numbered_key = f"{key}{i}"
                    entry[numbered_key] = value
                    # Also set base field for first RAM for compatibility
                    if i == 1:
                        entry[key] = value
                    logger.debug(f"Added enhanced RAM field to table: {numbered_key} = {value}")

        # Preserve the original combined config/description behavior for display/compatibility
        if collected_configs:
            combined = ', '.join(collected_configs)
            # Do not overwrite if already set by earlier parsing
            if 'table_ram_config' not in entry:
                entry['table_ram_config'] = combined
            if 'table_ram_description' not in entry:
                entry['table_ram_description'] = combined
    else:
        # Single RAM entry - process normally
        process_table_ram_data(entry, ram_extractors, logger)

def process_multi_line_storage_data(entry: Dict, storage_extractors: List, logger: logging.Logger) -> None:
    """Process storage data that may contain multiple storage devices separated by newlines."""
    storage_key = next((k for k in entry if k in ["table_ssd", "table_storage", "table_hard_drive"]), None)
    if not storage_key:
        return
        
    logger.debug(f"Enhancing storage data for table entry: '{entry[storage_key]}'")
    storage_value = entry[storage_key]
    
    # Split by newlines to handle multiple storage entries
    storage_lines = [line.strip() for line in storage_value.split('\n') if line.strip()]
    
    if len(storage_lines) > 1:
        # Process each storage line separately and create numbered fields
        for i, storage_line in enumerate(storage_lines, 1):
            # Special handling for mixed storage types like "1TB HDD/120GB SSD"
            mixed_storage_pattern = re.search(r'(\d+(?:\.\d+)?)(gb|tb)\s+(hdd|ssd|nvme|m\.2|emmc)\s*/\s*(\d+(?:\.\d+)?)(gb|tb)\s+(hdd|ssd|nvme|m\.2|emmc)', storage_line.lower())
            if mixed_storage_pattern:
                # Extract first storage device
                cap1 = f"{mixed_storage_pattern.group(1)}{mixed_storage_pattern.group(2)}"
                type1 = mixed_storage_pattern.group(3)
                # Extract second storage device  
                cap2 = f"{mixed_storage_pattern.group(4)}{mixed_storage_pattern.group(5)}"
                type2 = mixed_storage_pattern.group(6)
                
                entry[f"table_storage_capacity{i}_1"] = cap1
                entry[f"table_storage_capacity{i}_2"] = cap2
                entry[f"table_storage_type{i}"] = f"{type1}/{type2}"
                logger.debug(f"Extracted mixed storage for line {i}: {cap1} {type1}, {cap2} {type2}")
                
                # Also set individual types if different
                if type1 != type2:
                    entry[f"table_storage_type{i}_1"] = type1
                    entry[f"table_storage_type{i}_2"] = type2
            else:
                # Include field name as context for better storage detection
                field_name = ""
                if storage_key == "table_ssd":
                    field_name = "SSD"
                elif storage_key == "table_storage":
                    field_name = "Storage"
                elif storage_key == "table_hard_drive":
                    field_name = "HDD"
                
                # Combine field name with value to provide context
                storage_text = f"{field_name} {storage_line}" if field_name else storage_line
                tokens = tokenize_with_slash_splitting(storage_text, logger)
                consumed = set()
                for extractor in storage_extractors:
                    try:
                        matches = extractor.extract(tokens, consumed)
                        logger.debug(f"Extractor {extractor.name} returned: {matches}")
                        for match_indices in matches:
                            extracted = extractor.process_match(tokens, match_indices)
                            for key, value in extracted.items():
                                # Handle the key mapping more carefully
                                if key.startswith("storage_"):
                                    # Check if the key already has a number at the end
                                    if re.search(r'\d+$', key):
                                        # Key already has a number (like storage_capacity1), replace it with our line number
                                        base_key = re.sub(r'\d+$', '', key)
                                        new_key = f"table_{base_key}{i}"
                                    else:
                                        # Key has no number, add our line number
                                        new_key = f"table_{key}{i}"
                                    
                                    entry[new_key] = value
                                    # Also set base field for first storage for compatibility
                                    if i == 1:
                                        base_table_key = f"table_{key}" if not re.search(r'\d+$', key) else f"table_{re.sub(r'\d+$', '', key)}"
                                        entry[base_table_key] = value
                                    logger.debug(f"Added enhanced storage field to table: {new_key} = {value}")
                                else:
                                    # Non-storage key, handle normally
                                    new_key = f"table_{key}{i}"
                                    entry[new_key] = value
                                    if i == 1:
                                        entry[f"table_{key}"] = value
                                    logger.debug(f"Added enhanced field to table: {new_key} = {value}")
                            for idx in match_indices:
                                if isinstance(idx, int):
                                    consumed.add(idx)
                    except Exception as e:
                        logger.error(f"Error in storage extractor {extractor.name}: {str(e)}")
    else:
        # Single storage entry - process using existing logic
        mixed_storage_pattern = re.search(r'(\d+(?:\.\d+)?)(gb|tb)\s+(hdd|ssd|nvme|m\.2|emmc)\s*/\s*(\d+(?:\.\d+)?)(gb|tb)\s+(hdd|ssd|nvme|m\.2|emmc)', storage_value.lower())
        if mixed_storage_pattern:
            # Extract first storage device
            cap1 = f"{mixed_storage_pattern.group(1)}{mixed_storage_pattern.group(2)}"
            type1 = mixed_storage_pattern.group(3)
            # Extract second storage device  
            cap2 = f"{mixed_storage_pattern.group(4)}{mixed_storage_pattern.group(5)}"
            type2 = mixed_storage_pattern.group(6)
            
            entry["table_storage_capacity1"] = cap1
            entry["table_storage_capacity2"] = cap2
            entry["table_storage_type"] = f"{type1}/{type2}"
            logger.debug(f"Extracted mixed storage: {cap1} {type1}, {cap2} {type2}")
            
            # Also set individual types if different
            if type1 != type2:
                entry["table_storage_type1"] = type1
                entry["table_storage_type2"] = type2
        else:
            # Include field name as context for better storage detection
            field_name = ""
            if storage_key == "table_ssd":
                field_name = "SSD"
            elif storage_key == "table_storage":
                field_name = "Storage"
            elif storage_key == "table_hard_drive":
                field_name = "HDD"
            
            # Combine field name with value to provide context
            storage_text = f"{field_name} {entry[storage_key]}" if field_name else entry[storage_key]
            tokens = tokenize_with_slash_splitting(storage_text, logger)
            consumed = set()
            for extractor in storage_extractors:
                try:
                    matches = extractor.extract(tokens, consumed)
                    logger.debug(f"Extractor {extractor.name} returned: {matches}")
                    for match_indices in matches:
                        extracted = extractor.process_match(tokens, match_indices)
                        for key, value in extracted.items():
                            # Handle single storage entry key mapping
                            if key.startswith("storage_"):
                                # Check if the key already has a number at the end
                                if re.search(r'\d+$', key):
                                    # Key already has a number, use it as-is but with table_ prefix
                                    new_key = f"table_{key}"
                                else:
                                    # Key has no number, just add table_ prefix
                                    new_key = f"table_{key}"
                                entry[new_key] = value
                                logger.debug(f"Added enhanced storage field to table: {new_key} = {value}")
                            else:
                                # Non-storage key
                                new_key = f"table_{key}"
                                entry[new_key] = value
                                logger.debug(f"Added enhanced field to table: {new_key} = {value}")
                        for idx in match_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                except Exception as e:
                    logger.error(f"Error in storage extractor {extractor.name}: {str(e)}")
                    
def process_multi_line_gpu_data(entry: Dict, gpu_extractors: List, logger: logging.Logger) -> None:
    """Process GPU data that may contain multiple GPUs separated by newlines."""
    gpu_key = next((k for k in entry if k in ["table_videocard", "table_gpu"]), None)
    if not gpu_key:
        return
        
    logger.debug(f"Enhancing GPU data for table entry: '{entry[gpu_key]}'")
    gpu_value = entry[gpu_key]
    
    # Split by newlines to handle multiple GPU entries
    gpu_lines = [line.strip() for line in gpu_value.split('\n') if line.strip()]
    
    if len(gpu_lines) > 1:
        # Process each GPU line separately and create numbered fields
        for i, gpu_line in enumerate(gpu_lines, 1):
            tokens = tokenize_with_slash_splitting(gpu_line, logger)
            consumed = set()
            for extractor in gpu_extractors:
                try:
                    matches = extractor.extract(tokens, consumed)
                    logger.debug(f"Extractor {extractor.name} returned: {matches}")
                    for match_indices in matches:
                        extracted = extractor.process_match(tokens, match_indices)
                        for key, value in extracted.items():
                            entry[f"table_{key}{i}"] = value
                            # Also set base field for first GPU for compatibility
                            if i == 1:
                                entry[f"table_{key}"] = value
                            logger.debug(f"Added enhanced GPU field to table: table_{key}{i} = {value}")
                        for idx in match_indices:
                            if isinstance(idx, int):
                                consumed.add(idx)
                except Exception as e:
                    logger.error(f"Error in GPU extractor {extractor.name}: {str(e)}")
    else:
        # Single GPU entry - process normally
        tokens = tokenize_with_slash_splitting(entry[gpu_key], logger)
        consumed = set()
        for extractor in gpu_extractors:
            try:
                matches = extractor.extract(tokens, consumed)
                logger.debug(f"Extractor {extractor.name} returned: {matches}")
                for match_indices in matches:
                    extracted = extractor.process_match(tokens, match_indices)
                    for key, value in extracted.items():
                        entry[f"table_{key}"] = value
                        logger.debug(f"Added enhanced GPU field to table: table_{key} = {value}")
                    for idx in match_indices:
                        if isinstance(idx, int):
                            consumed.add(idx)
            except Exception as e:
                logger.error(f"Error in GPU extractor {extractor.name}: {str(e)}")

def enhance_server_memory_table_data(entries: List[Dict], description_text: str, logger: logging.Logger) -> List[Dict]:
    """Enhance server memory table data using comprehensive description information."""
    
    # Only apply to server memory/RAM devices
    device_indicators = ['server ram', 'server memory', 'ecc', 'registered', 'ddr3 reg ecc', 'pc3-8500r', 'pc3l-8500r']
    if not any(indicator in description_text.lower() for indicator in device_indicators):
        return entries
    
    logger.debug("Enhancing server memory table data from description plaintext")
    
    # Extract manufacturer part numbers and their details
    part_number_pattern = r'MFR#:\s*([A-Z0-9-]+)\s+[A-Z0-9\s]+\((\d+)\s*x\s*(\d+GB)\)'
    part_matches = re.findall(part_number_pattern, description_text, re.IGNORECASE)
    
    # Extract voltage information from technical specs tables
    voltage_pattern = r'Voltage\s+(\d+\.?\d*)'
    voltage_matches = re.findall(voltage_pattern, description_text)
    
    # Extract ALL technical specifications from description
    shared_specs = {}
    tech_specs_patterns = {
        'table_manufacturer': r'Manufacturer\s+([A-Za-z]+)',
        'table_memory_type': r'Memory Type\s+(DDR\d+\s+SDRAM)',
        'table_data_transfer_rate': r'Data Transfer Rate\s+(\d+Mhz)',
        'table_pins': r'Pins\s+(\d+\s+Pin)', 
        'table_error_correction': r'Error Correction\s+(Registered\s+ECC)',
        'table_cycle_time': r'Cycle Time\s+([\d.]+ns)',
        'table_cas': r'Cas\s+(CL\d+)',
        'table_memory_clock': r'Memory Clock\s+(\d+Mhz)',
        'table_rank': r'Rank\s+(Rank\s+\d+)',
        'table_capacity': r'Capacity\s+(\d+GB)'
    }
    
    for field, pattern in tech_specs_patterns.items():
        match = re.search(pattern, description_text, re.IGNORECASE)
        if match:
            shared_specs[field] = match.group(1)
    
    # Extract additional description-only information
    total_config_match = re.search(r'Qty:\s*(\d+GB)\s*\((\d+)\s*x\s*(\d+GB)\)', description_text, re.IGNORECASE)
    if total_config_match:
        shared_specs['table_total_capacity'] = total_config_match.group(1)
        shared_specs['table_modules'] = total_config_match.group(2)
        shared_specs['table_config'] = f"{total_config_match.group(2)}x{total_config_match.group(3)}"
    
    # Extract additional features from description
    if 'mixed lot' in description_text.lower():
        shared_specs['table_details'] = 'Mixed Lot'
    
    if 'server ram' in description_text.lower():
        shared_specs['table_application'] = 'Server'
    
    if 'shielded' in description_text.lower():
        shared_specs['table_features'] = 'Shielded'
    
    # Extract speed grade range from description and normalize format
    bus_type_patterns = [
        r'(PC3-8500R and PC3L-8500R)',
        r'(PC3L-8500R and PC3-8500R)',
        r'(PC3-8500R\s+and\s+PC3L-8500R)',
        r'(PC3L-8500R\s+and\s+PC3-8500R)'
    ]
    
    for pattern in bus_type_patterns:
        bus_type_match = re.search(pattern, description_text, re.IGNORECASE)
        if bus_type_match:
            # Normalize the format to use " / " separator and consistent order
            shared_specs['table_speed_grade_range'] = 'PC3-8500R / PC3L-8500R'
            logger.debug(f"Extracted and normalized speed grade range: {shared_specs['table_speed_grade_range']}")
            break
    
    # If we have detailed part information, create comprehensive entries
    if part_matches and len(part_matches) >= 2:
        logger.debug(f"Found {len(part_matches)} server memory parts, creating comprehensive table data")
        new_entries = []
        
        for i, (part_number, quantity, capacity) in enumerate(part_matches):
            new_entry = shared_specs.copy()
            
            # Add specific details for this part
            new_entry['table_manufacturer_part'] = part_number
            new_entry['table_quantity'] = quantity
            new_entry['table_capacity'] = capacity
            
            # Add voltage and derive bus type
            if i < len(voltage_matches):
                voltage_val = voltage_matches[i]
                new_entry['table_voltage'] = voltage_val
                
                # Determine PC3 vs PC3L based on voltage
                if voltage_val == '1.35':
                    new_entry['table_bus_type'] = 'PC3L-8500'
                elif voltage_val == '1.5':
                    new_entry['table_bus_type'] = 'PC-8500'
            
            # Add key order for consistent output
            key_order = []
            for key in new_entry.keys():
                original_key = key.replace('table_', '').replace('_', ' ').title()
                key_order.append((key, original_key))
            new_entry['__key_order__'] = key_order
            
            new_entries.append(new_entry)
            logger.debug(f"Created comprehensive entry {i+1}: Part={part_number}, Qty={quantity}, Voltage={voltage_val if i < len(voltage_matches) else 'N/A'}")
        
        return new_entries
    
    return entries
    
def apply_table_extractors(entries: List[Dict], logger: logging.Logger) -> None:
   """Apply all extractors to enhance table data."""
   extractors = load_extractors(logger)
   cpu_extractors = [ext for ext in extractors if ext.name.startswith("cpu_")]
   ram_extractors = [ext for ext in extractors if ext.name.startswith("ram_")]
   storage_extractors = [ext for ext in extractors if ext.name.startswith("storage_")]
   screen_extractors = [ext for ext in extractors if ext.name.startswith("screen_")]
   gpu_extractors = [ext for ext in extractors if ext.name == "gpu" or ext.name.startswith("gpu_")]
   os_extractors = [ext for ext in extractors if ext.name.startswith("os_")]
   device_extractors = [ext for ext in extractors if ext.name.startswith("device_") or ext.name.startswith("form_factor")]
   battery_extractors = [ext for ext in extractors if ext.name.startswith("battery_")]
   switch_extractors = [ext for ext in extractors if ext.name.startswith("switch_")]
   adapter_extractors = [ext for ext in extractors if ext.name.startswith("adapter_")]
   hdd_extractors = [ext for ext in extractors if ext.name.startswith("hdd_")]

   # Process network carrier mapping to network status
   network_carriers = [
       "Verizon", "AT&T", "T-Mobile", "Dish Network", "Dish Mobile", "US Cellular", "Cricket", 
       "MetroPCS", "Metro", "Metro by T-Mobile", "Boost Mobile", "Mint Mobile", "Google Fi", 
       "Xfinity Mobile", "Spectrum Mobile", "Consumer Cellular", "Straight Talk", "Total by Verizon", 
       "Ting Mobile", "Republic Wireless", "H2O Wireless", "PureTalk", "Red Pocket Mobile", 
       "Ultra Mobile", "Tello Mobile", "Twigby", "TextNow", "Good2Go Mobile", "FreedomPop", 
       "Net10 Wireless", "Page Plus Cellular", "Simple Mobile"
   ]

   for entry in entries:
       if 'table_network_carrier' in entry:
           raw_value = entry['table_network_carrier']
           network_value = raw_value.lower()
           logger.debug(f"Processing network carrier value: {network_value}")
           
           if 'unlocked' in network_value:
               if 'network unlocked' in network_value:
                   entry['table_network_status'] = 'Network Unlocked'
               elif 'carrier unlocked' in network_value:
                   entry['table_network_status'] = 'Carrier Unlocked'
               else:
                   # FIXED: Map standalone "unlocked" to "Network Unlocked"
                   entry['table_network_status'] = 'Network Unlocked'
               logger.debug(f"Mapped to network_status: {entry['table_network_status']}")
           elif 'wifi only' in network_value or 'wi-fi only' in network_value:
               entry['table_network_status'] = 'WiFi Only'
               logger.debug(f"Mapped to network_status: WiFi Only")
           elif 'wifi' in network_value or 'wi-fi' in network_value:
               # Treat standalone WiFi/Wi-Fi as WiFi Only for tablets/phones in table data
               entry['table_network_status'] = 'WiFi Only'
               logger.debug(f"Mapped to network_status: WiFi Only (from WiFi)")
           else:
               carrier_found = False
               for carrier in network_carriers:
                    if carrier.lower() in network_value:
                        entry['table_network_status'] = 'Locked'
                        carrier_found = True
                        logger.debug("Mapped to network_status: Locked (carrier present)")
                        break
               
               if not carrier_found and 'locked' in network_value:
                   entry['table_network_status'] = 'Locked'
                   logger.debug(f"Mapped to network_status: Locked")

   for entry in entries:
       # Process screen composite data
       if 'table_screen_composite' in entry:
           value = entry['table_screen_composite']
           logger.debug(f"Processing composite screen value: '{value}'")
           tokens = tokenize_with_slash_splitting(value, logger)
           consumed = set()
           for extractor in screen_extractors:
               try:
                   matches = extractor.extract(tokens, consumed)
                   logger.debug(f"Screen extractor {extractor.name} returned: {matches}")
                   for match_indices in matches:
                       extracted = extractor.process_match(tokens, match_indices)
                       for key, value in extracted.items():
                           if value:
                               entry[f"table_{key}"] = value
                               logger.debug(f"Extracted {key}: {value} from composite screen value")
                       for idx in match_indices:
                           if isinstance(idx, int):
                               consumed.add(idx)
               except Exception as e:
                   logger.error(f"Error in screen extractor {extractor.name}: {str(e)}")

       # Process multi-line data for technical components
       process_multi_line_cpu_data(entry, cpu_extractors, logger)
       process_multi_line_ram_data(entry, ram_extractors, logger)
       process_multi_line_storage_data(entry, storage_extractors, logger)
       process_multi_line_gpu_data(entry, gpu_extractors, logger)

       # Process single-value fields
       os_key = next((k for k in entry if k in ["table_os", "table_operating_system"]), None)
       if os_key:
           logger.debug(f"Enhancing OS data for table entry: '{entry[os_key]}'")
           tokens = tokenize_with_slash_splitting(entry[os_key], logger)
           consumed = set()
           for extractor in os_extractors:
               try:
                   matches = extractor.extract(tokens, consumed)
                   logger.debug(f"Extractor {extractor.name} returned: {matches}")
                   for match_indices in matches:
                       extracted = extractor.process_match(tokens, match_indices)
                       for key, value in extracted.items():
                           entry[f"table_{key}"] = value
                           logger.debug(f"Added enhanced OS field to table: table_{key} = {value}")
                       for idx in match_indices:
                           if isinstance(idx, int):
                               consumed.add(idx)
               except Exception as e:
                   logger.error(f"Error in OS extractor {extractor.name}: {str(e)}")

       # Process other extractors similarly...
       for extractor_group, field_patterns in [
           (device_extractors, ["table_device_type", "table_form_factor"]),
           (battery_extractors, ["table_battery", "table_battery_health"]),
           (switch_extractors, ["table_switch", "table_network_switch"]),
           (adapter_extractors, ["table_adapter", "table_network_adapter", "table_nic"])
       ]:
           field_key = next((k for k in entry if k in field_patterns), None)
           if field_key:
               logger.debug(f"Enhancing data for table entry: '{entry[field_key]}'")
               tokens = tokenize_with_slash_splitting(entry[field_key], logger)
               consumed = set()
               for extractor in extractor_group:
                   try:
                       matches = extractor.extract(tokens, consumed)
                       logger.debug(f"Extractor {extractor.name} returned: {matches}")
                       for match_indices in matches:
                           extracted = extractor.process_match(tokens, match_indices)
                           for key, value in extracted.items():
                               entry[f"table_{key}"] = value
                               logger.debug(f"Added enhanced field to table: table_{key} = {value}")
                           for idx in match_indices:
                               if isinstance(idx, int):
                                   consumed.add(idx)
                   except Exception as e:
                       logger.error(f"Error in extractor {extractor.name}: {str(e)}")

       # Process HDD extractors for Internal Hard Disk Drives
       hdd_interface_key = next((k for k in entry if k in ["table_interface"]), None)
       if hdd_interface_key:
           logger.debug(f"Enhancing HDD data for table entry: '{entry[hdd_interface_key]}'")
           tokens = tokenize_with_slash_splitting(entry[hdd_interface_key], logger)
           consumed = set()
           for extractor in hdd_extractors:
               if extractor.name in ["hdd_interface", "hdd_form_factor"]:  # Apply both interface and form factor extractors
                   try:
                       matches = extractor.extract(tokens, consumed)
                       logger.debug(f"HDD extractor {extractor.name} returned: {matches}")
                       for match_indices in matches:
                           extracted = extractor.process_match(tokens, match_indices)
                           for key, value in extracted.items():
                               if key == "hdd_interface":
                                   entry["table_interface"] = value
                                   logger.debug(f"Updated table interface to clean value: {value}")
                               elif key == "hdd_form_factor":
                                   entry["table_form_factor"] = value
                                   logger.debug(f"Added table form factor: {value}")
                               for idx in match_indices:
                                   if isinstance(idx, int):
                                       consumed.add(idx)
                   except Exception as e:
                       logger.error(f"Error in HDD extractor {extractor.name}: {str(e)}")
                       
def detect_plaintext_table_in_description(description_text: str) -> bool:
    """Detect if the description contains plaintext tabular data."""
    if not description_text or len(description_text.strip()) < 50:
        return False
    
    # Look for table header keywords
    table_keywords = ['make', 'model', 'test result', 'storage', 'network', 'carrier', 
                     'version', 'missing', 'serial number', 'imei', 'notes', 'battery health']
    
    lines = description_text.split('\n')
    
    # Count lines that look like headers
    header_like_lines = 0
    data_like_lines = 0
    
    for line in lines:
        line_clean = line.strip().lower()
        if not line_clean:
            continue
            
        # Check for header patterns
        keyword_count = sum(1 for keyword in table_keywords if keyword in line_clean)
        if keyword_count >= 3:
            header_like_lines += 1
            continue
        
        # Check for data patterns (brand names at start, tab-separated values)
        if '\t' in line and any(line.strip().startswith(brand) for brand in 
                               ['Samsung', 'Apple', 'Google', 'OnePlus', 'Motorola', 'Nokia', 'LG', 'HTC', 'Sony']):
            data_like_lines += 1
    
    # If we have headers and data rows, likely a table
    return header_like_lines >= 1 and data_like_lines >= 2

def parse_tabular_data(text: str, logger: logging.Logger) -> List[Dict]:
    """Parse tab-separated tabular data with repeating headers and multi-line content."""
    
    def clean_header_text(header: str) -> str:
        """Clean header text by removing asterisks, underscores, and normalizing."""
        # Remove asterisks and underscores used for emphasis
        cleaned = re.sub(r'[*_]+', '', header)
        # Clean up extra spaces
        cleaned = re.sub(r'\s+', ' ', cleaned.strip())
        return cleaned
    
    def detect_header_line(line: str) -> bool:
        """Detect if a line is likely a header based on content and formatting."""
        line_clean = line.strip()
        if not line_clean:
            return False
            
        # Check for common header keywords
        header_keywords = ['make', 'model', 'test result', 'storage', 'network', 'carrier', 
                           'version', 'missing', 'serial number', 'imei', 'notes', 'battery health']
        
        line_lower = line_clean.lower()
        keyword_count = sum(1 for keyword in header_keywords if keyword in line_lower)
        
        # If multiple header keywords are present, likely a header
        return keyword_count >= 3
    
    def parse_header_section(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
        """Parse a header section that might span multiple lines."""
        headers = []
        current_idx = start_idx
        
        # First line is definitely a header
        first_line = lines[current_idx].strip()
        if first_line:
            first_headers = [clean_header_text(h) for h in first_line.split('\t')]
            headers.extend(first_headers)
            current_idx += 1
        
        # Check if next line is also a header (continuation)
        if current_idx < len(lines):
            next_line = lines[current_idx].strip()
            if next_line and detect_header_line(next_line):
                next_headers = [clean_header_text(h) for h in next_line.split('\t')]
                
                # If first line ends with "Network" and second starts with "Carrier", merge them
                if headers and headers[-1].lower() == 'network' and next_headers and next_headers[0].lower() == 'carrier':
                    headers[-1] = 'Network Carrier'
                    headers.extend(next_headers[1:])  # Skip first item as it's merged
                else:
                    headers.extend(next_headers)
                current_idx += 1
        
        # Clean up headers
        cleaned_headers = []
        for header in headers:
            if header and header.strip():
                # Map common variations
                header_clean = header.strip()
                if header_clean.lower() in ['network / carrier', 'network carrier', 'network/carrier']:
                    header_clean = 'Network Carrier'
                elif 'serial number' in header_clean.lower() and 'imei' in header_clean.lower():
                    header_clean = 'Serial Number IMEI'
                elif 'battery health' in header_clean.lower():
                    header_clean = 'Battery Health'
                
                cleaned_headers.append(header_clean)
        
        return cleaned_headers, current_idx
    
    def parse_data_row(line: str, headers: List[str]) -> Dict[str, str]:
        """Parse a data row and map to headers."""
        values = line.split('\t')
        data = {}
        
        for i, header in enumerate(headers):
            if i < len(values):
                value = values[i].strip()
                if value:
                    data[header] = value
            else:
                data[header] = ""
        
        return data
    
    def is_continuation_line(line: str) -> bool:
        """Check if line is a continuation of the previous row (notes, etc.)."""
        line_clean = line.strip()
        if not line_clean:
            return False
        
        # If line doesn't start with a brand name, likely a continuation
        brand_patterns = [r'^(Samsung|Apple|Google|OnePlus|Motorola|Nokia|LG|HTC|Sony)', 
                         r'^[A-Z][a-z]+\s+[A-Z]']  # Generic brand pattern
        
        return not any(re.match(pattern, line_clean) for pattern in brand_patterns)
    
    # Main parsing logic
    lines = text.split('\n')
    all_entries = []
    current_headers = []
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
        
        # Check if this is a header line
        if detect_header_line(line):
            logger.debug(f"Found header line at {i}: {line}")
            current_headers, new_i = parse_header_section(lines, i)
            logger.debug(f"Parsed headers: {current_headers}")
            i = new_i
            continue
        
        # If we have headers and this looks like a data row
        if current_headers and line and not detect_header_line(line):
            data = parse_data_row(line, current_headers)
            
            # Check for continuation lines (multi-line notes)
            notes_content = []
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if not next_line:
                    j += 1
                    continue
                    
                if detect_header_line(next_line) or not is_continuation_line(next_line):
                    break
                    
                notes_content.append(next_line)
                j += 1
            
            # Add continuation content to notes field
            if notes_content:
                notes_field = None
                for header in current_headers:
                    if 'notes' in header.lower():
                        notes_field = header
                        break
                
                if notes_field:
                    existing_notes = data.get(notes_field, "")
                    additional_notes = " ".join(notes_content)
                    if existing_notes:
                        data[notes_field] = f"{existing_notes} {additional_notes}"
                    else:
                        data[notes_field] = additional_notes
            
            if any(data.values()):  # Only add if there's actual data
                all_entries.append(data)
                logger.debug(f"Added entry: {data}")
            
            i = j if notes_content else i + 1
        else:
            i += 1
    
    return all_entries


def drop_phantom_placeholder_entries(entries: List[Dict], logger: logging.Logger) -> List[Dict]:
    """Detect the phantom placeholder entry and drop it and all subsequent entries.

    Context: Some captures include a bogus row like:
        Make: _Make_
        Model: _Model_
        CPU: __CPU__
        RAM: _RAM_
        Storage: Storage
        Video Card: Video_Card
        WiFi: WiFi
        Notes: __________Notes___________

    The presence of this row indicates everything after it is not real table data.
    We conservatively detect it by requiring at least three placeholder-style values.
    """
    if not entries:
        return entries

    def _norm(val: Any) -> str:
        return str(val).strip() if isinstance(val, str) else ""

    for idx, entry in enumerate(entries):
        try:
            matches = 0

            brand = _norm(entry.get('table_brand', ''))
            if re.match(r'^_+\s*make\s*_+$', brand, re.IGNORECASE):
                matches += 1

            model = _norm(entry.get('table_model', ''))
            if re.match(r'^_+\s*model\s*_+$', model, re.IGNORECASE):
                matches += 1

            cpu = _norm(entry.get('table_cpu', ''))
            if re.match(r'^_+\s*cpu\s*_+$', cpu, re.IGNORECASE):
                matches += 1

            ram = _norm(entry.get('table_ram', ''))
            if re.match(r'^_+\s*ram\s*_+$', ram, re.IGNORECASE):
                matches += 1

            storage_val = _norm(entry.get('table_storage', '') or entry.get('table_ssd', '') or entry.get('table_hard_drive', ''))
            if re.match(r'^storage$', storage_val, re.IGNORECASE):
                matches += 1

            gpu = _norm(entry.get('table_videocard', ''))
            if re.match(r'^video[_\s]*card$', gpu, re.IGNORECASE) or gpu == 'Video_Card':
                matches += 1

            wifi = _norm(entry.get('table_wifi', ''))
            if re.match(r'^wifi$', wifi, re.IGNORECASE):
                matches += 1

            notes = _norm(entry.get('table_notes', ''))
            if re.match(r'^_+\s*notes\s*_+$', notes, re.IGNORECASE) or re.match(r'^_+notes_+$', notes, re.IGNORECASE):
                matches += 1

            # Require multiple placeholder hits to avoid false positives
            if matches >= 3:
                if logger:
                    logger.debug(f"Detected phantom placeholder entry at index {idx}; dropping this and all subsequent entries")
                return entries[:idx]
        except Exception as e:
            if logger:
                logger.debug(f"Error while checking for phantom entry at index {idx}: {e}")

    return entries


def merge_degenerate_model_header_entries(entries: List[Dict], logger: logging.Logger) -> List[Dict]:
    """Merge sequences of entries where each entry only has 'table_model' with a header-like value.

    Some listings render a vertical list of attribute labels as separate entries like:
        Entry 1: Model: Manufacturer
        Entry 2: Model: Manufacturer Part #
        ...
    This is not a list of separate products, but a schema for a single product/spec table.

    Detection heuristic:
    - Entries have only one meaningful field besides '__key_order__'
    - That field is exactly 'table_model'
    - The value of 'table_model' matches a known set of spec header labels

    If a contiguous run of at least 6 such entries is found, merge that run into a single entry
    where each header label becomes an empty field (to be filled by other parsers if present),
    preserving key order for downstream display.
    """
    if not entries:
        return entries

    # Whitelist of header labels observed in this alternate table format
    header_labels_whitelist = {
        'manufacturer',
        'manufacturer part #',
        'performance',
        'product type',
        'form factor',
        'cpu socket type',
        'supported cpu(s)',
        'supported cpu technologies',
        'chipset',
        'number of memory slot(s)',
        'supported memory standard(s)',
        'maximum installed memory',
        'expansion slots',
        'storage controller',
        'audio controller',
        'network',
        'connectors/ports',
    }

    def _normalize_model_value(value: str) -> str:
        v = value.strip()
        # Handle cases like "Model: Manufacturer" -> "Manufacturer"
        if ':' in v:
            parts = v.split(':', 1)
            v = parts[1].strip() if len(parts) > 1 else v
        return v

    def is_model_header_only_entry(entry: Dict) -> Optional[str]:
        # Return the header label if this is a model-only header entry, else None
        keys = [k for k in entry.keys() if k != '__key_order__']
        if len(keys) != 1 or keys[0] != 'table_model':
            return None
        raw_value = str(entry.get('table_model', '')).strip()
        normalized_value = _normalize_model_value(raw_value)
        value_norm = normalized_value.lower()
        return normalized_value if value_norm in header_labels_whitelist else None

    merged_entries: List[Dict] = []
    i = 0
    n = len(entries)
    while i < n:
        # Attempt to detect a contiguous run starting at i
        run_labels: List[str] = []
        j = i
        while j < n:
            header_label = is_model_header_only_entry(entries[j])
            if not header_label:
                break
            run_labels.append(header_label)
            j += 1

        if len(run_labels) >= 6:  # heuristic threshold to avoid false positives
            # Merge the run [i, j) into a single entry
            merged_entry: Dict[str, Any] = {}
            key_order: List[Tuple[str, str]] = []
            seen_table_keys: Set[str] = set()

            for label in run_labels:
                table_key = f"table_{standardize_key(label)}"
                if table_key not in seen_table_keys:
                    merged_entry[table_key] = ''
                    key_order.append((table_key, label))
                    seen_table_keys.add(table_key)

            merged_entry['__key_order__'] = key_order
            merged_entries.append(merged_entry)
            logger.debug(
                f"Merged {len(run_labels)} model-only header entries into a single schema entry with keys: "
                + ", ".join(k for k, _ in key_order)
            )
            i = j  # Skip the run
        else:
            # No run; keep original entry
            merged_entries.append(entries[i])
            i += 1

    return merged_entries


def fill_merged_schema_entry_from_description(entries: List[Dict], description_text: str, logger: logging.Logger) -> List[Dict]:
    """Fill values for a merged schema-style entry using lines from description.

    Looks for lines that start with one of the known header labels and captures the remainder
    of the line plus any continuation lines until the next header. Maps these to the corresponding
    `table_` keys on the detected merged entry.
    """
    if not entries or not description_text:
        return entries

    header_labels = [
        'manufacturer',
        'manufacturer part #',
        'performance',
        'product type',
        'form factor',
        'cpu socket type',
        'supported cpu(s)',
        'supported cpu technologies',
        'chipset',
        'number of memory slot(s)',
        'supported memory standard(s)',
        'maximum installed memory',
        'expansion slots',
        'storage controller',
        'audio controller',
        'network',
        'connectors/ports',
    ]

    # Identify the merged schema entry by presence of many of the expected keys
    expected_keys = {f"table_{standardize_key(label)}" for label in header_labels}
    candidate_index = None
    best_overlap = 0
    for idx, entry in enumerate(entries):
        keys_set = set(entry.keys())
        overlap = len(keys_set & expected_keys)
        if overlap > best_overlap and 'table_model' not in entry:
            best_overlap = overlap
            candidate_index = idx

    if candidate_index is None or best_overlap < 6:
        return entries

    # Build a map from header label -> collected value from description
    lines = [ln.strip() for ln in description_text.split('\n')]
    header_to_value_lines: Dict[str, List[str]] = {}

    # Sort labels by length to match the longest first (avoid partial matches)
    labels_sorted = sorted(header_labels, key=len, reverse=True)

    current_label: Optional[str] = None

    def match_label(line_lower: str) -> Optional[str]:
        for label in labels_sorted:
            lab = label.lower()
            if line_lower == lab or line_lower.startswith(lab + ' ') or line_lower.startswith(lab + ':'):
                return label
        return None

    for raw_line in lines:
        if not raw_line:
            continue
        line_lower = raw_line.lower()
        label = match_label(line_lower)
        if label:
            # Start a new header capture
            # Extract remainder after label if present on same line
            remainder = raw_line[len(label):].lstrip(' :\t')
            header_to_value_lines[label] = []
            if remainder:
                header_to_value_lines[label].append(remainder)
            current_label = label
            continue

        # Continuation line
        if current_label:
            # Stop if this line actually looks like a new header (defensive)
            maybe_new = match_label(line_lower)
            if maybe_new:
                current_label = maybe_new
                header_to_value_lines.setdefault(current_label, [])
                continue
            header_to_value_lines[current_label].append(raw_line)

    # Join lines into values with reasonable separators
    header_to_value: Dict[str, str] = {}
    for label, vals in header_to_value_lines.items():
        cleaned_parts = [v.strip() for v in vals if v.strip()]
        if not cleaned_parts:
            continue
        if label.lower() in ['connectors/ports', 'supported cpu technologies']:
            value_text = ', '.join(cleaned_parts)
        else:
            value_text = ' '.join(cleaned_parts)
        header_to_value[label] = value_text

    if not header_to_value:
        return entries

    # Apply values to the candidate entry
    entry = entries[candidate_index]
    for label, value in header_to_value.items():
        key = f"table_{standardize_key(label)}"
        if value:
            entry[key] = value

    logger.debug(
        "Filled merged schema entry from description for keys: " + ", ".join(sorted(f"table_{standardize_key(k)}" for k in header_to_value.keys()))
    )
    return entries

def parse_table_data(text: str, logger: logging.Logger) -> List[Dict]:
   """Main table data parsing function with plaintext fallback."""
   logger.debug("Starting table data parsing")
   
   # Extract description text for enhancement purposes
   description_text = ""
   in_desc = False
   for line in text.split('\n'):
       line = line.strip()
       if "=== ITEM DESCRIPTION ===" in line:
           in_desc = True
           continue
       elif ("===" in line or "Disclaimer:" in line) and in_desc:
           break
       elif in_desc:
           description_text += line + "\n"
   
   # Step 1: Try the existing structured parsing first
   entries = parse_basic_table_structure(text, logger)
   
   # Step 2: Check if we need to fall back to plaintext parsing
   needs_fallback = False
   
   if not entries:
       logger.debug("No structured table entries found")
       needs_fallback = True
   elif len(entries) < 2:
       logger.debug("Very few structured entries found, checking for plaintext table")
       needs_fallback = True
   
   # Step 3: If fallback needed, check if there's actually plaintext table data in description
   if needs_fallback:
       if detect_plaintext_table_in_description(description_text):
           logger.debug("Plaintext table detected in description, using fallback parser")
           plaintext_entries = parse_tabular_data(description_text, logger)
           
           # Convert to the expected format with table_ prefixes
           converted_entries = []
           for entry in plaintext_entries:
               converted_entry = {}
               key_order = []
               
               for key, value in entry.items():
                   # Map common fields to expected table format
                   table_key = None
                   if key.lower() == 'make':
                       table_key = 'table_brand'
                   elif key.lower() == 'model':
                       table_key = 'table_model'
                   elif key.lower() == 'storage':
                       table_key = 'table_storage'
                   elif 'network' in key.lower() and 'carrier' in key.lower():
                       table_key = 'table_network_carrier'
                   elif key.lower() == 'version':
                       table_key = 'table_version'
                   elif 'battery health' in key.lower():
                       table_key = 'table_battery_health'
                   elif key.lower() == 'missing':
                       table_key = 'table_missing'
                   elif 'serial number' in key.lower() or 'imei' in key.lower():
                       table_key = 'table_serial_number_imei'
                   elif key.lower() == 'notes':
                       table_key = 'table_notes'
                   elif 'test result' in key.lower():
                       table_key = 'table_test_result'
                   else:
                       table_key = f"table_{standardize_key(key)}"
                   
                   if table_key and value:
                       converted_entry[table_key] = value
                       key_order.append((table_key, key))
               
               if converted_entry:
                   converted_entry['__key_order__'] = key_order
                   converted_entries.append(converted_entry)
           
           if converted_entries:
               logger.debug(f"Successfully parsed {len(converted_entries)} entries from plaintext table")
               entries = converted_entries
           else:
               logger.debug("Plaintext parsing failed, keeping original entries")
       else:
           logger.debug("No plaintext table detected in description")
  
   # Step 3.4: Drop phantom placeholder entry ("Make: _Make_", etc.) and everything after it
   entries_before = len(entries)
   entries = drop_phantom_placeholder_entries(entries, logger)
   if len(entries) != entries_before:
       logger.debug("Dropped phantom placeholder entry and subsequent rows from table_data")
   
   # Step 3.5: Merge alternate-format model-only header entries into a single entry if detected
   entries = merge_degenerate_model_header_entries(entries, logger)

   # Step 4: Apply extractors to enhance the data
   apply_table_extractors(entries, logger)

   # Step 4.5: If we created a merged schema-style entry, try to fill from description
   entries = fill_merged_schema_entry_from_description(entries, description_text, logger)
   
   # Step 5: Apply server memory enhancement if applicable - this may replace entries entirely
   enhanced_entries = enhance_server_memory_table_data(entries, description_text, logger)
   if enhanced_entries != entries:
       logger.debug("Server memory table data was replaced with enhanced version")
       entries = enhanced_entries
   
   logger.debug(f"Completed table data parsing with {len(entries)} entries")
   return entries
    
def extract_fallback_conditions_from_description(description_text: str, logger: logging.Logger) -> Dict:
    """Extract condition information from description text as fallback when other methods fail."""
    conditions = {}
    
    if not description_text:
        return conditions
    
    # Patterns to match condition information in various formats
    patterns = [
        # Format: ?? Cosmetic Condition: C4  Used Good (Minor scratches and scuffs present)
        # Format: ?? Functional Condition: F3  Key Functions Working
        r'(?:[\?\*\-\u2022]+\s*)?(?:Cosmetic\s*Condition)\s*:?\s*([CF]\d+)?\s*[-:]?\s*(.+?)(?:\s*\([^)]*\))?$',
        r'(?:[\?\*\-\u2022]+\s*)?(?:Functional\s*Condition)\s*:?\s*([CF]\d+)?\s*[-:]?\s*(.+?)(?:\s*\([^)]*\))?$',
        
        # Format: Cosmetic Condition C4: Used Good
        # Format: Functional Condition F3: Key Functions Working  
        r'(?:Cosmetic\s*Condition)\s+([CF]\d+)\s*:?\s*(.+?)$',
        r'(?:Functional\s*Condition)\s+([CF]\d+)\s*:?\s*(.+?)$',
        
        # Format: C4 - Used Good (cosmetic condition)
        # Format: F3 - Key Functions Working (functional condition)
        r'^([CF]\d+)\s*[-:]\s*(.+?)(?:\s*\(.*cosmetic.*condition.*\))?$',
        r'^([CF]\d+)\s*[-:]\s*(.+?)(?:\s*\(.*functional.*condition.*\))?$',
    ]
    
    # First, check for standalone condition codes anywhere in the text (F4-Hardware Functional, C6-Used Excellent)
    # This handles comma-separated condition codes that can appear anywhere in the description
    # Enhanced pattern to stop at common delimiters like ??, *, bullets, etc.
    # Removed \b to handle codes at start of lines better
    standalone_pattern = r'(?:^|\s)([FC]\d+)\s*-\s*([^,\n\r\?]+?)(?=\s*[,\n\r\?\*\u2022]|\s+\?\?|$)'
    standalone_matches = re.findall(standalone_pattern, description_text, re.IGNORECASE | re.MULTILINE)
    
    for code, cond_text in standalone_matches:
        code = code.upper()
        cond_text = cond_text.strip()
        
        # Clean up the condition text
        cond_text = re.sub(r'\s+', ' ', cond_text)  # Normalize whitespace
        cond_text = cond_text.rstrip('.,;?!')  # Remove trailing punctuation
        
        if cond_text:
            condition_type = 'Functional Condition' if code.startswith('F') else 'Cosmetic Condition'
            if condition_type not in conditions:
                final_value = f"{code}-{cond_text}"
                conditions[condition_type] = final_value
                logger.debug(f"Fallback extracted {condition_type}: '{final_value}' from standalone pattern")
    
    lines = description_text.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check for cosmetic condition patterns
        if 'cosmetic' in line.lower() and 'condition' in line.lower():
            for pattern in patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    code = match.group(1) if match.group(1) else ''
                    value_text = match.group(2).strip() if match.group(2) else ''
                    
                    # Clean up the value text
                    value_text = re.sub(r'\s+', ' ', value_text)  # Normalize whitespace
                    value_text = value_text.rstrip('.,;')  # Remove trailing punctuation
                    
                    if code and value_text:
                        final_value = f"{code}: {value_text}"
                        conditions['Cosmetic Condition'] = final_value
                        logger.debug(f"Fallback extracted Cosmetic Condition: '{final_value}' from line: '{line}'")
                        break
                    elif value_text:
                        conditions['Cosmetic Condition'] = value_text
                        logger.debug(f"Fallback extracted Cosmetic Condition: '{value_text}' from line: '{line}'")
                        break
        
        # Check for functional condition patterns
        elif 'functional' in line.lower() and 'condition' in line.lower():
            for pattern in patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    code = match.group(1) if match.group(1) else ''
                    value_text = match.group(2).strip() if match.group(2) else ''
                    
                    # Clean up the value text
                    value_text = re.sub(r'\s+', ' ', value_text)  # Normalize whitespace
                    value_text = value_text.rstrip('.,;')  # Remove trailing punctuation
                    
                    if code and value_text:
                        final_value = f"{code}: {value_text}"
                        conditions['Functional Condition'] = final_value
                        logger.debug(f"Fallback extracted Functional Condition: '{final_value}' from line: '{line}'")
                        break
                    elif value_text:
                        conditions['Functional Condition'] = value_text
                        logger.debug(f"Fallback extracted Functional Condition: '{value_text}' from line: '{line}'")
                        break
        
        # Also check for standalone condition codes (C4, F3, etc.) with descriptions
        elif re.match(r'^[\?\*\-\u2022]*\s*[CF]\d+', line):
            # This could be a standalone condition code line
            code_match = re.match(r'^[\?\*\-\u2022]*\s*([CF]\d+)\s*[-:]?\s*(.+?)$', line)
            if code_match:
                code = code_match.group(1)
                description = code_match.group(2).strip()
                
                # Clean up the description
                description = re.sub(r'\s+', ' ', description)
                description = description.rstrip('.,;')
                
                condition_type = 'Functional Condition' if code.startswith('F') else 'Cosmetic Condition'
                if condition_type not in conditions and description:
                    final_value = f"{code}: {description}"
                    conditions[condition_type] = final_value
                    logger.debug(f"Fallback extracted {condition_type}: '{final_value}' from standalone line: '{line}'")
    
    return conditions


def parse_description(text: str, logger: logging.Logger) -> Dict:
    logger.debug("Starting description parsing")
    desc = {'description_text': ''}
    key_order = []
    in_desc = False
    fields = ["Cosmetic Condition", "Functional Condition", "Data Sanitization"]
    pattern = re.compile(r'^(' + '|'.join(fields) + r')\s*:?\s*(.*)$', re.IGNORECASE)
    description_lines = []
    exited = False

    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        if "=== ITEM DESCRIPTION ===" in line:
            in_desc = True
            logger.debug("Entered description section")
            continue
        if ("===" in line or "Disclaimer:" in line) and not exited:
            if description_lines:
                desc['description_text'] = '\n'.join(description_lines).strip()
                if 'description_text' not in [k for k, _ in key_order]:
                    key_order.append(('description_text', 'Description Text'))
                    logger.debug(f"Stored description text: '{desc['description_text']}'")
            in_desc = False
            exited = True
            logger.debug("Exited description section")
            continue
        if in_desc:
            # Skip informational message lines from AHK scripts (INFO:, WARNING:, etc.)
            if re.match(r'^\s*(INFO|WARNING|ERROR)\s*:', line, re.IGNORECASE):
                logger.debug(f"Skipping informational message line: '{line}'")
                continue
                
            # NEW: Detect 'R2' certification lines (e.g., "R2v3 Certification: F4-Hardware Functional, C6-Used Excellent")
            if re.search(r'\bR2\w*\s*Certification\b', line, re.IGNORECASE):
                condition_matches = re.findall(r'\b([FC]\d+)\s*-\s*([^,]+)', line, re.IGNORECASE)
                for code, cond_text in condition_matches:
                    code = code.upper()
                    value = f"{code}-{cond_text.strip()}" if cond_text.strip() else code
                    key = 'Functional Condition' if code.startswith('F') else 'Cosmetic Condition'
                    if key not in desc:
                        desc[key] = value
                        if key not in [k for k, _ in key_order]:
                            key_order.append((key, key))
                            logger.debug(f"Stored {key} from R2 certification: '{value}'")
                # Skip further processing for this line since we've already handled it
                continue
            match = pattern.match(line)
            if match:
                key = match.group(1).strip()
                value = clean_text(match.group(2).strip())
                desc[key] = value
                if key not in [k for k, _ in key_order]:
                    key_order.append((key, key))
                    logger.debug(f"Stored description key-value pair: '{key}' = '{value}'")
            else:
                description_lines.append(line)

    if in_desc and description_lines:
        desc['description_text'] = '\n'.join(description_lines).strip()
        if 'description_text' not in [k for k, _ in key_order]:
            key_order.append(('description_text', 'Description Text'))
            logger.debug(f"Stored description text: '{desc['description_text']}'")

    # Structured enhancement (non-destructive): parse description_text with tolerant parser
    try:
        from description_parsing import parse_description_structured
        structured = parse_description_structured(desc.get('description_text', ''), logger)
        # Merge only when the key is not present yet to avoid changing existing behavior
        for k, v in structured.items():
            if k == 'bullets':
                # Append bullet lines into description_text so downstream logic that reads lines can use them
                if v:
                    existing_text = desc.get('description_text', '')
                    existing_lines = [ln.strip() for ln in existing_text.split('\n') if ln.strip()]
                    existing_set = set(existing_lines)
                    new_lines = [ln for ln in v if ln and ln.strip() and ln.strip() not in existing_set]
                    if new_lines:
                        combined = (existing_lines + new_lines)
                        desc['description_text'] = "\n".join(combined)
                        # Ensure key order includes description_text label
                        if 'description_text' not in [ko for ko, _ in key_order]:
                            key_order.append(('description_text', 'Description Text'))
                continue
            if k not in desc and v:
                desc[k] = v
                if k not in [ko for ko, _ in key_order] and k not in ('bullets',):
                    key_order.append((k, k))
                    if logger:
                        logger.debug(f"Structured parser added '{k}': '{v}'")
    except Exception as e:
        if logger:
            logger.debug(f"Structured description parsing skipped due to error: {e}")

    # Fallback condition extraction from description text if not found yet
    if 'Cosmetic Condition' not in desc or 'Functional Condition' not in desc:
        fallback_conditions = extract_fallback_conditions_from_description(desc.get('description_text', ''), logger)
        for condition_key, condition_value in fallback_conditions.items():
            if condition_key not in desc:
                desc[condition_key] = condition_value
                if condition_key not in [k for k, _ in key_order]:
                    key_order.append((condition_key, condition_key))
                    logger.debug(f"Extracted {condition_key} from fallback: '{condition_value}'")

    # Mirror description text into the key variant some downstream code expects
    if 'description_text_key' not in desc:
        desc['description_text_key'] = desc.get('description_text', '')

    desc['__key_order__'] = key_order
    logger.debug(f"Completed description parsing with {len(desc)} entries")
    return desc

def remove_base_keys_with_numbered_variants(entries: List[Dict], logger: logging.Logger) -> None:
    """Remove base keys when numbered variants exist to avoid duplicates, with whitelist exceptions."""
    # Whitelist: keep base for these when numbered variants exist
    table_whitelist_bases = {"table_storage_capacity", "table_network_status", "table_network_carrier", "table_ram_size", "table_ram_config"}
    for entry in entries:
        keys_to_remove = []
        
        # Group keys by their base name (without numbers)
        key_groups = {}
        for key in entry.keys():
            if key.startswith('table_') and key != '__key_order__':
                # Extract base key name - be more precise about the pattern
                base_match = re.match(r'(table_\w+?)(\d+)$', key)
                if base_match:
                    base_name = base_match.group(1)
                    number = base_match.group(2)
                    
                    if base_name not in key_groups:
                        key_groups[base_name] = {'base': None, 'numbered': []}
                    
                    key_groups[base_name]['numbered'].append(key)
                else:
                    # This might be a base key - check if it has numbered variants
                    potential_base = key
                    if potential_base not in key_groups:
                        key_groups[potential_base] = {'base': key, 'numbered': []}
                    else:
                        key_groups[potential_base]['base'] = key
        
        # Now check for numbered variants of each potential base key
        for potential_base, group in key_groups.items():
            if group['base']:
                # Look for numbered variants of this base key
                for other_key in entry.keys():
                    if other_key.startswith(f"{potential_base}") and re.match(rf'{re.escape(potential_base)}\d+$', other_key):
                        group['numbered'].append(other_key)
        
        # Remove base keys if numbered variants exist
        for base_name, group in key_groups.items():
            if group['base'] and group['numbered'] and base_name not in table_whitelist_bases:
                keys_to_remove.append(group['base'])
                logger.debug(f"Removing base key {group['base']} as numbered variants exist: {group['numbered']}")
        
        # Actually remove the keys
        for key in keys_to_remove:
            if key in entry:
                entry.pop(key)

def normalize_table_numbered_fields(entries: List[Dict]) -> None:
    """Promote 1st numbered variant to base for specific table keys and drop the '1' variant."""
    whitelist_attrs = ["storage_capacity", "network_status", "network_carrier"]
    for entry in entries:
        for attr in whitelist_attrs:
            base = f"table_{attr}"
            first = f"{base}1"
            if first in entry and base not in entry:
                entry[base] = entry[first]
            # Remove the explicit '1' variant to avoid duplication
            if first in entry:
                entry.pop(first, None)

def process_storage_fields(entries: List[Dict]) -> None:
    """Process storage fields to handle slash-separated format and remap to numbered format."""
    for entry in entries:
        storage_type_key = 'table_storage_type'
        
        # Check for slash-separated format in original storage field
        for storage_field in ['table_ssd', 'table_storage', 'table_hard_drive']:
            if storage_field in entry:
                value = entry[storage_field]
                # Try to parse potential slash-separated values (e.g., "24GB/1TB SSD")
                slash_pattern = re.search(r'(\d+(?:gb|tb|mb))\s*\/\s*(\d+(?:gb|tb|mb))', value, re.IGNORECASE)
                if slash_pattern:
                    # Found slash-separated capacities - convert to numbered format
                    cap1 = slash_pattern.group(1)
                    cap2 = slash_pattern.group(2)
                    
                    # Add as separate capacity fields for consistency with title format
                    entry['table_storage_capacity1'] = cap1
                    entry['table_storage_capacity2'] = cap2
                    
                    # Remove base storage capacity key if it exists
                    if 'table_storage_capacity' in entry:
                        entry.pop('table_storage_capacity')
                    
                    # Add type if found in string (e.g. "SSD" at the end)
                    type_match = re.search(r'(ssd|hdd|nvme)', value, re.IGNORECASE)
                    if type_match:
                        entry[storage_type_key] = type_match.group(1).lower()

def get_shared_values(entries: List[Dict]) -> Dict[str, str]:
    """Extract values that are shared across all entries."""
    shared_values = {}
    primary_storage_fields = ['table_ssd', 'table_storage', 'table_hard_drive']
    composite_keys = ['table_cpu', 'table_ram'] + primary_storage_fields
    
    # A key is only 'shared' if *every* entry contains a non-empty value and they
    # are all identical (case-insensitive). This prevents a value that appears in
    # only the first row from being incorrectly hoisted into the shared section.

    potential_shared_keys = set(entries[0].keys()) - set(composite_keys) - {'__key_order__'}
    for key in potential_shared_keys:
        all_present = all(key in entry and entry[key] for entry in entries)
        if not all_present:
            continue

        values = {entry[key].strip().lower() for entry in entries}
        if len(values) == 1:
            shared_values[key] = next(entry[key] for entry in entries)
    
    return shared_values

def remove_base_keys_from_shared_values(shared_values: Dict[str, str], logger: logging.Logger) -> Dict[str, str]:
    """Remove base keys from shared values if numbered variants exist, with whitelist exceptions."""
    table_whitelist_bases = {"table_storage_capacity", "table_network_status", "table_network_carrier", "table_ram_size", "table_ram_config"}
    # Find groups of related keys (base key + numbered variants) in shared values
    key_groups = {}
    for key in shared_values.keys():
        # Skip non-table keys or keys that don't start with 'table_'
        if not key.startswith('table_'):
            continue
            
        # Check if this is a numbered variant
        match = re.search(r'^(table_\w+?)(\d+)$', key)
        if match:
            base_key = match.group(1)
            if base_key not in key_groups:
                key_groups[base_key] = []
            key_groups[base_key].append(key)
            
    # Remove base keys if numbered variants exist in shared values
    for base_key, numbered_keys in key_groups.items():
        if numbered_keys and base_key in shared_values and base_key not in table_whitelist_bases:
            logger.debug(f"Removing base key {base_key} from shared values as numbered variants exist")
            shared_values.pop(base_key)
    
    return shared_values

def format_shared_values_output(shared_values: Dict[str, str], first_entry: Dict) -> List[str]:
    """Format shared values for output."""
    output = []
    
    # Group related keys together for more organized output
    grouped_keys = {}
    ungrouped_keys = []
    
    for key in shared_values.keys():
        if key.startswith('table_'):
            # Extract the attribute name without the table_ prefix and any numbers
            match = re.match(r'table_(\w+?)(\d*)$', key)
            if match:
                attr_name = match.group(1)
                if attr_name not in grouped_keys:
                    grouped_keys[attr_name] = []
                grouped_keys[attr_name].append(key)
            else:
                ungrouped_keys.append(key)
        else:
            ungrouped_keys.append(key)
    
    # Output ungrouped keys first
    for key in sorted(ungrouped_keys):
        original_key = next((ok for mk, ok in first_entry.get('__key_order__', []) if mk == key), 
                            key.replace('table_', '').replace('_', ' ').title())
        # Handle "No" values for OS in shared values
        if key == 'table_os' and shared_values[key].lower() in ['no', 'n/a']:
            output.append(f"[{key}_key] {original_key}: Not Included")
        else:
            output.append(f"[{key}_key] {original_key}: {shared_values[key]}")
    
    # Then output grouped keys in a logical order
    whitelist_attr_names = {"storage_capacity", "network_status", "network_carrier"}
    for attr_name, keys in sorted(grouped_keys.items()):
        sorted_keys = sorted(keys, key=lambda k: int(re.search(r'\d+$', k).group()) if re.search(r'\d+$', k) else 0)
        
        for key in sorted_keys:
            original_key = next((ok for mk, ok in first_entry.get('__key_order__', []) if mk == key), 
                                key.replace('table_', '').replace('_', ' ').title())
            
            # Handle "No" values for OS in shared values
            value = shared_values[key]
            if key.startswith('table_os') and value.lower() in ['no', 'n/a']:
                value = "Not Included"
            
            # For storage capacity, use a consistent label
            if key.startswith('table_storage_capacity'):
                # Extract the number from the key for display
                number_match = re.search(r'(\d+)$', key)
                if number_match:
                    num = number_match.group(1)
                    output.append(f"[{key}_key] Storage Capacity {num}: {value}")
                else:
                    output.append(f"[{key}_key] Storage Capacity: {value}")
            else:
                output.append(f"[{key}_key] {original_key}: {value}")
    
    return output

def format_entry_values_output(entry: Dict, entry_num: int, shared_keys_case_insensitive: set) -> List[str]:
    """Format individual entry values for output."""
    output = []
    composite_keys = ['table_cpu', 'table_ram', 'table_ssd', 'table_storage', 'table_hard_drive']
    
    # Find all keys in this entry that aren't shared values or composite keys
    entry_keys = {k for k in entry.keys() 
               if k != '__key_order__' and 
               k not in composite_keys and
               k.lower() not in shared_keys_case_insensitive}
    
    # Group related keys together for more organized output
    grouped_keys = {}
    ungrouped_keys = []
    
    for key in entry_keys:
        if key.startswith('table_'):
            # Extract the attribute name without the table_ prefix and any numbers
            match = re.match(r'table_(\w+?)(\d*)$', key)
            if match:
                attr_name = match.group(1)
                if attr_name not in grouped_keys:
                    grouped_keys[attr_name] = []
                grouped_keys[attr_name].append(key)
            else:
                ungrouped_keys.append(key)
        else:
            ungrouped_keys.append(key)
    
    # Process each group of related keys
    whitelist_attr_names = {"storage_capacity", "network_status", "network_carrier"}
    for attr_name, keys in sorted(grouped_keys.items()):
        # Sort keys with numbers naturally (1, 2, 10 instead of 1, 10, 2)
        sorted_keys = sorted(keys, key=lambda k: int(re.search(r'\d+$', k).group()) if re.search(r'\d+$', k) else 0)
        
        # Skip base key if there are numbered variants
        base_key = f"table_{attr_name}"
        has_numbered = any(re.search(r'\d+$', k) for k in sorted_keys)
        
        for key in sorted_keys:
            # Skip the base key if we have numbered variants
            if key == base_key and has_numbered and attr_name not in whitelist_attr_names:
                continue
                
            value = entry[key]
            original_key = next((ok for mk, ok in entry.get('__key_order__', []) if mk == key), 
                                key.replace('table_', '').replace('_', ' ').title())
            
            # Handle "No" values for OS in table entries
            if key.startswith('table_os') and value.lower() in ['no', 'n/a']:
                value = "Not Included"
            
            # For storage capacity, use a consistent label
            if key.startswith('table_storage_capacity'):
                # Extract the number from the key for display
                number_match = re.search(r'(\d+)$', key)
                if number_match:
                    num = number_match.group(1)
                    output.append(f"[{key}_key] Storage Capacity {num}: {value}")
                else:
                    output.append(f"[{key}_key] Storage Capacity: {value}")
            else:
                output.append(f"[{key}_key] {original_key}: {value}")
    
    # Process remaining ungrouped keys
    for key in sorted(ungrouped_keys):
        value = entry[key]
        original_key = next((ok for mk, ok in entry.get('__key_order__', []) if mk == key), 
                            key.replace('table_', '').replace('_', ' ').title())
        
        # Handle "No" values for OS in table entries
        if key.startswith('table_os') and value.lower() in ['no', 'n/a']:
            value = "Not Included"
        
        output.append(f"[{key}_key] {original_key}: {value}")
    
    return output

def remove_base_keys_when_numbered_exist(data_dict: Dict, key_prefix: str = "") -> Dict:
    """Remove base keys when numbered variants exist in a dictionary."""
    keys_to_remove = []
    
    # Group keys by their base name
    key_groups = {}
    for key in data_dict.keys():
        if key_prefix and not key.startswith(key_prefix):
            continue
            
        # Extract base key name
        base_match = re.match(r'^(.+?)(\d+)$', key)
        if base_match:
            base_name = base_match.group(1)
            if base_name not in key_groups:
                key_groups[base_name] = {'base': None, 'numbered': []}
            key_groups[base_name]['numbered'].append(key)
        else:
            # This might be a base key
            potential_base = key
            if potential_base not in key_groups:
                key_groups[potential_base] = {'base': key, 'numbered': []}
            else:
                key_groups[potential_base]['base'] = key
    
    # Check for numbered variants of each potential base key
    for potential_base, group in key_groups.items():
        if group['base']:
            # Look for numbered variants
            for other_key in data_dict.keys():
                if other_key.startswith(f"{potential_base}") and re.match(rf'^{re.escape(potential_base)}\d+$', other_key):
                    group['numbered'].append(other_key)
    
    # Remove base keys if numbered variants exist
    for base_name, group in key_groups.items():
        if group['base'] and group['numbered']:
            keys_to_remove.append(group['base'])
    
    # Create new dict without the base keys
    cleaned_dict = {k: v for k, v in data_dict.items() if k not in keys_to_remove}
    return cleaned_dict

def normalize_title_numbered_fields(data_dict: Dict) -> Dict:
    """Normalize numbered fields in title so the first instance uses the base key.

    Rules:
    - For whitelisted fields: 'storage_capacity', 'network_status', 'network_carrier'
      - If only numbered variants exist, promote 'field1' to 'field' (base) and remove 'field1'.
      - If both base and numbered variants exist, keep the base and remove 'field1' to avoid duplicates.
    - For all other fields, remove the base key when numbered variants exist (original behavior).
    """
    # Shallow copy to avoid mutating the original
    result: Dict = dict(data_dict)

    # FIX: Correct malformed CPU generation keys like 'cpu_generation23' → 'cpu_generation2'
    try:
        malformed_keys = [k for k in list(result.keys()) if re.match(r'^cpu_generation\d{2,}$', k)]
        for bad_key in malformed_keys:
            # Keep only the first index digit; discard accidental concatenations
            fixed_key = f"cpu_generation{re.search(r'(\d)', bad_key).group(1)}"
            if fixed_key not in result:
                result[fixed_key] = result[bad_key]
            # Remove the malformed key
            result.pop(bad_key, None)
    except Exception:
        pass

    whitelist_bases = {"storage_capacity", "network_status", "network_carrier", "cpu_suffix"}

    # Build mapping from base -> list of numbered variant keys present
    def _build_base_map(src: Dict) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for k in list(src.keys()):
            m = re.match(r"^(.+?)(\d+)$", k)
            if m:
                b = m.group(1)
                mapping.setdefault(b, []).append(k)
            else:
                mapping.setdefault(k, mapping.get(k, []))
        return mapping

    base_to_numbered_keys: Dict[str, List[str]] = _build_base_map(result)

    # Special-case CPU: if we only have numbered '1' variants and no '2', promote to base and remove '1'
    try:
        has_cpu2 = any(re.match(r"^cpu_\w+2$", k) for k in result.keys())
        if not has_cpu2:
            cpu_bases = {"cpu_brand", "cpu_family", "cpu_generation", "cpu_model", "cpu_speed", "cpu_suffix"}
            for base in cpu_bases:
                base1 = f"{base}1"
                if base not in result and base1 in result:
                    result[base] = result[base1]
                # Remove explicit "1" variant in single-CPU case
                if base1 in result:
                    result.pop(base1, None)
    except Exception:
        pass

    # Recompute mapping after CPU normalization so subsequent logic reflects removals
    base_to_numbered_keys = _build_base_map(result)

    for base, numbered_keys in base_to_numbered_keys.items():
        if not numbered_keys:
            continue

        if base in whitelist_bases:
            base1 = f"{base}1"
            # Promote field1 to base if base missing
            if base not in result and base1 in result:
                result[base] = result[base1]
            # Always remove the explicit "1" variant for these fields
            if base1 in result:
                result.pop(base1, None)
            # Keep other numbered variants (2, 3, ...)
            continue

        # Default behavior for non-whitelisted fields: remove base when numbered variants exist
        if base in result:
            result.pop(base, None)

    return result

def format_output(listing: 'ListingData', logger: logging.Logger) -> str:
    logger.debug("Starting output formatting")
    output = []
    cpu_detail_keys = ['brand', 'family', 'model', 'suffix', 'speed', 'quantity', 'cores', 'generation', 'gen_other']
    ram_detail_keys = ['size', 'type', 'speed_grade', 'modules', 'rank', 'brand', 'ecc', 'registered', 'unbuffered', 'details', 'range', 'config']
    storage_detail_keys = ['storage_capacity', 'storage_type', 'storage_status', 'storage_drive_count', 'storage_individual_capacity', 'storage_drive_size']
    screen_detail_keys = ['screen_size', 'screen_resolution_type', 'screen_resolution', 'screen_panel_type', 'screen_touch']
    gpu_detail_keys = ['gpu_brand', 'gpu_series', 'gpu_model', 'gpu_ram_size', 'gpu_memory_type', 'gpu_type', 'gpu_spec']
    os_detail_keys = ['os_type', 'os_version', 'os_edition', 'os_status']
    device_detail_keys = ['device_type', 'form_factor', 'rack_units']
    battery_detail_keys = ['battery_status', 'battery_health', 'battery_condition']
    switch_detail_keys = ['switch_brand', 'switch_series', 'switch_ports', 'switch_speed', 'switch_interface', 'switch_model', 'switch_type']
    adapter_detail_keys = ['adapter_brand', 'adapter_series', 'adapter_speed', 'adapter_ports', 'adapter_form_factor', 'adapter_interface', 'adapter_model', 'adapter_type']
    hdd_detail_keys = ['hdd_interface', 'hdd_form_factor', 'hdd_rpm', 'hdd_transfer_rate', 'hdd_model_number', 'hdd_part_number', 'hdd_usage_hours']

    multi_instance_components = {
        'cpu': {
            'model_key': 'cpu_model',
            'detail_keys': cpu_detail_keys,
            'shared_before': ['cpu_brand', 'cpu_family'],
            'shared_after': ['cpu_suffix', 'cpu_speed', 'cpu_quantity', 'cpu_cores', 'cpu_gen_other']
        },
        'ram': {
            'model_key': 'ram_size',
            'detail_keys': ram_detail_keys,
            'shared_before': ['ram_type', 'ram_brand'],
            'shared_after': ['ram_speed_grade', 'ram_modules', 'ram_rank']
        },
        'storage': {
            'model_key': 'storage_capacity',
            'default_factory': list,
            'detail_keys': storage_detail_keys,
            'shared_before': ['storage_type'],
            'shared_after': ['storage_drive_count']
        }
    }

    custom_key_mapping = {
        'Title': 'title',
        'Custom Label': 'customlabel',
        'Listing Info': 'listinginfo',
        'Item Number': 'itemnumber',
    }

    def is_apple_silicon_cpu(cpu_num, data_dict):
        """Check if a CPU is Apple Silicon based on brand and family."""
        brand_key = f'cpu_brand{cpu_num}' if cpu_num else 'cpu_brand'
        family_key = f'cpu_family{cpu_num}' if cpu_num else 'cpu_family'
        
        cpu_brand = data_dict.get(brand_key, '')
        cpu_family = data_dict.get(family_key, '')
        
        return (cpu_brand == 'Apple' or 'Apple M' in cpu_family)

    if listing.title.get("Full Title"):
        output.append("====== TITLE DATA ======")
        output.append(f"Full Title: {listing.title['Full Title']}")
        if "lot" in listing.title:
            output.append(f"[title_lot_key] Lot: {listing.title['lot']}")

        # Clean title data with title-aware numbering rules:
        # - Keep base for storage_capacity, network_status, network_carrier (and promote X1 -> X)
        # - Remove base for other attributes when numbered variants exist
        cleaned_title = normalize_title_numbered_fields(listing.title)
        
        phone_detail_keys = ['series', 'phone_model', 'color', 'network_status', 'network_carrier', 'battery_health', 'storage_size']
        title_keys = ['brand', 'model'] + phone_detail_keys + \
                     [f"{comp}_{k}" for comp, config in multi_instance_components.items() for k in config['detail_keys']] + \
                     screen_detail_keys + gpu_detail_keys + os_detail_keys + device_detail_keys + battery_detail_keys + \
                     switch_detail_keys + adapter_detail_keys + hdd_detail_keys

        # Track which keys we've already output to avoid duplicates
        output_keys = set()
        
        # ENHANCED: Special handling for GPU details to ensure gpu_brand and gpu_series are output
        if 'gpu_model' in cleaned_title or 'gpu_ram_size' in cleaned_title:
            # Make sure gpu_brand and gpu_series are set if gpu_model is present
            if 'gpu_model' in cleaned_title and 'gpu_brand' not in cleaned_title:
                cleaned_title['gpu_brand'] = "NVIDIA"
            if 'gpu_model' in cleaned_title and 'gpu_series' not in cleaned_title:
                cleaned_title['gpu_series'] = "GEFORCE"
        
        # ENHANCED: Special handling for model and form factor
        # For HP ProDesk 600 G3 SFF, ensure model is set correctly
        if 'brand' in cleaned_title and cleaned_title['brand'].upper() == 'HP':
            # Check if we have ProDesk in the title
            title_lower = listing.title.get('Full Title', '').lower()
            if 'prodesk' in title_lower and '600' in title_lower and 'g3' in title_lower and 'sff' in title_lower:
                cleaned_title['model'] = 'ProDesk 600 G3'
                cleaned_title['form_factor'] = 'Small Form Factor (SFF)'
        
        # Ensure CPU speeds follow the exact pair in the Full Title when two CPUs are present
        try:
            full_title_raw = listing.title.get('Full Title', '')
            has_multi_cpu = (
                ('cpu_model1' in cleaned_title and 'cpu_model2' in cleaned_title) or
                ('cpu_family1' in cleaned_title and 'cpu_family2' in cleaned_title)
            )
            if isinstance(full_title_raw, str) and full_title_raw:
                m_all = list(re.finditer(r'(\d+(?:\.\d+)?)\s*[Gg][Hh][Zz]', full_title_raw))
                if len(m_all) >= 2:
                    end1 = m_all[0].end()
                    start2 = m_all[1].start()
                    between = full_title_raw[end1:start2]
                    if '/' in between:
                        s1 = f"{m_all[0].group(1)}GHz"
                        s2 = f"{m_all[1].group(1)}GHz"
                        if cleaned_title.get('cpu_speed1') != s1:
                            cleaned_title['cpu_speed1'] = s1
                        if cleaned_title.get('cpu_speed2') != s2:
                            cleaned_title['cpu_speed2'] = s2

                # Always align CPU generations based on slash order in the Full Title
                # Examples: "7th/8th Gen", "11th / 12th Gen"
                gen_pair = re.search(r'(\d+(?:st|nd|rd|th))\s*/\s*(\d+(?:st|nd|rd|th))\s*Gen\.?', full_title_raw, re.IGNORECASE)
                if gen_pair:
                    g1 = gen_pair.group(1)
                    g2 = gen_pair.group(2)
                    cleaned_title['cpu_generation'] = f"{g1} Gen"
                    cleaned_title['cpu_generation2'] = f"{g2} Gen"
        except Exception:
            pass
        
        # Output base keys first (un-numbered), then numbered variants
        # Ensure RAM sizes are in ascending order when two values exist
        try:
            if 'ram_size1' in cleaned_title and 'ram_size2' in cleaned_title:
                def _to_mb(val: str) -> float:
                    m = re.match(r'^(\d+(?:\.\d+)?)(MB|GB|TB)$', val.strip(), re.IGNORECASE)
                    if not m:
                        return float('inf')
                    num = float(m.group(1))
                    unit = m.group(2).upper()
                    if unit == 'TB':
                        return num * 1024 * 1024
                    if unit == 'GB':
                        return num * 1024
                    return num
                v1 = cleaned_title['ram_size1']
                v2 = cleaned_title['ram_size2']
                if _to_mb(v1) > _to_mb(v2):
                    cleaned_title['ram_size1'], cleaned_title['ram_size2'] = v2, v1
        except Exception:
            pass
        base_first_keys = set()
        for key in title_keys:
            if key in cleaned_title and not any(key.startswith(f"{comp}_") for comp in multi_instance_components) and key not in ['additional_info']:
                if key == 'ram' and ('ram_size' in cleaned_title or 'ram_config' in cleaned_title):
                    continue
                safe_key = standardize_key(key)
                output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                output_keys.add(key)
                base_first_keys.add(key)

        # ENHANCED: Handle numbered phone extractor keys
        phone_numbered_patterns = [
            (r'phone_model\d+$', 'phone_model'),
            (r'color\d+$', 'color'),
            (r'network_status\d+$', 'network_status'),
            (r'network_carrier\d+$', 'network_carrier')
        ]
        
        for pattern, base_name in phone_numbered_patterns:
            # Emit base first if present and not yet emitted
            if base_name in cleaned_title and base_name not in output_keys:
                safe_key = standardize_key(base_name)
                output.append(f"[title_{safe_key}_key] {base_name}: {cleaned_title[base_name]}")
                output_keys.add(base_name)

            numbered_keys = [key for key in cleaned_title if re.match(pattern, key)]
            numbered_keys.sort(key=lambda x: int(re.search(r'\d+$', x).group()))
            
            for key in numbered_keys:
                if key not in output_keys:
                    safe_key = standardize_key(key)
                    output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                    output_keys.add(key)

        for key in cleaned_title:
            if re.match(r'network_status\d+$', key) and key not in output_keys:
                safe_key = standardize_key(key)
                output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                output_keys.add(key)

        for key in cleaned_title:
            if re.match(r'network_carrier\d+$', key) and key not in output_keys:
                safe_key = standardize_key(key)
                output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                output_keys.add(key)

        # Guarantee base CPU family and generation are emitted for CPU 1
        # even when numbered variants exist and there are no cpu_model keys.
        try:
            if 'cpu_family' not in output_keys:
                base_family = cleaned_title.get('cpu_family') or cleaned_title.get('cpu_family1')
                if base_family:
                    safe = standardize_key('cpu_family')
                    output.append(f"[title_{safe}_key] cpu_family: {base_family}")
                    output_keys.add('cpu_family')
            if 'cpu_generation' not in output_keys:
                base_gen = cleaned_title.get('cpu_generation') or cleaned_title.get('cpu_generation1')
                if base_gen:
                    safe = standardize_key('cpu_generation')
                    output.append(f"[title_{safe}_key] cpu_generation: {base_gen}")
                    output_keys.add('cpu_generation')
            # Also ensure base CPU brand is emitted once
            if 'cpu_brand' not in output_keys:
                base_brand = (
                    cleaned_title.get('cpu_brand') or
                    cleaned_title.get('cpu_brand1') or
                    cleaned_title.get('cpu_brand2')
                )
                if base_brand:
                    safe = standardize_key('cpu_brand')
                    output.append(f"[title_{safe}_key] cpu_brand: {base_brand}")
                    output_keys.add('cpu_brand')
        except Exception:
            pass

        # Handle numbered CPU families, brands, and generations when we have sequential CPU specs
        cpu_family_numbers = [key for key in cleaned_title if re.match(r'cpu_family\d+$', key)]
        cpu_generation_numbers = [key for key in cleaned_title if re.match(r'cpu_generation\d+$', key)]
        cpu_brand_numbers = [key for key in cleaned_title if re.match(r'cpu_brand\d+$', key)]
        
        # Check if we have numbered CPU models - if so, skip standalone generations
        has_numbered_cpu_models = any(re.match(r'cpu_model\d+$', key) for key in cleaned_title)
        
        if cpu_family_numbers or cpu_generation_numbers or cpu_brand_numbers:
            # Sort numbered keys
            cpu_family_numbers.sort(key=lambda x: int(re.search(r'\d+', x).group()))
            cpu_generation_numbers.sort(key=lambda x: int(re.search(r'\d+', x).group()))
            cpu_brand_numbers.sort(key=lambda x: int(re.search(r'\d+', x).group()))
            
            # Output numbered CPU brands (skip if base brand already emitted)
            for key in cpu_brand_numbers:
                # Skip first CPU's numbered brand and suppress numbered brands if base exists
                if re.search(r'1$', key) or 'cpu_brand' in output_keys:
                    continue
                if key not in output_keys:
                    safe_key = standardize_key(key)
                    output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                    output_keys.add(key)
            
            # Output numbered CPU families (skip the first CPU; it will be shown as base)
            for key in cpu_family_numbers:
                if re.search(r'1$', key):
                    continue
                if key not in output_keys:
                    safe_key = standardize_key(key)
                    output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                    output_keys.add(key)
            
            # Output numbered CPU generations (skip the first CPU; base will be shown for CPU 1)
            # (to avoid duplicates when CPU models have their own generations)
            if not has_numbered_cpu_models:
                for key in cpu_generation_numbers:
                    if re.search(r'1$', key):
                        continue
                    if key not in output_keys:
                        safe_key = standardize_key(key)
                        output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                        output_keys.add(key)
            else:
                # If we have numbered CPU models, still output generation keys that don't conflict
                for key in cpu_generation_numbers:
                    if re.search(r'1$', key):
                        continue
                    if key not in output_keys:
                        safe_key = standardize_key(key)
                        output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                        output_keys.add(key)
        
        for comp, config in multi_instance_components.items():
            model_key = config['model_key']
            model_keys = [key for key in cleaned_title if re.match(rf'^{model_key}(\d+)?$', key)]
            all_comp_keys = {key for key in cleaned_title if key.startswith(f"{comp}_")}

            if all_comp_keys:
                # Only output shared keys if they haven't been output already and don't have numbered variants
                for attr in config['shared_before']:
                    if attr in cleaned_title and attr not in output_keys:
                        # Check if this attribute has numbered variants
                        has_numbered_variants = any(re.match(rf'^{attr}\d+$', key) for key in cleaned_title)
                        if not has_numbered_variants:
                            safe_key = standardize_key(attr)
                            output.append(f"[title_{safe_key}_key] {attr}: {cleaned_title[attr]}")
                            output_keys.add(attr)
                        all_comp_keys.discard(attr)

                def sort_model_keys(key):
                    match = re.match(rf'{model_key}(\d+)', key)
                    # Base key (no number) first, then numbered keys ascending
                    return 0 if not match else int(match.group(1))
                sorted_model_keys = sorted(model_keys, key=sort_model_keys)

                for key in sorted_model_keys:
                    if key not in output_keys:
                        match = re.match(r'cpu_model(\d+)', key)
                        if match:
                            num = match.group(1)
                        else:
                            num = '1'
                        
                        # Skip Apple Silicon CPU models since family+generation is sufficient
                        if comp == 'cpu' and is_apple_silicon_cpu(num, cleaned_title):
                            output_keys.add(key)
                            all_comp_keys.discard(key)
                            continue
                        
                        value = cleaned_title[key]
                        # For CPU 1, display base key (cpu_model) instead of numbered variant
                        if comp == 'cpu' and num == '1':
                            display_field = 'cpu_model'
                            safe_display = standardize_key(display_field)
                            output.append(f"[title_{safe_display}_key] {display_field}: {value}")
                            output_keys.add(display_field)
                            # Also mark numbered key as emitted to prevent duplicates
                            output_keys.add(key)
                        else:
                            safe_key = standardize_key(key)
                            output.append(f"[title_{safe_key}_key] {key}: {value}")
                            output_keys.add(key)
                        all_comp_keys.discard(key)
                        if comp == 'cpu':
                            # Handle numbered CPU attributes for this specific CPU (but avoid duplicates)
                            for attr in ['generation', 'family', 'brand', 'speed']:
                                # Prefer base keys for the first CPU; use numbered for CPU 2+
                                attr_key = f"cpu_{attr}{num}"
                                attr_key_no_num = f"cpu_{attr}"
                                if num == '1':
                                    # Get value from numbered or base and emit as base
                                    if attr_key in cleaned_title or attr_key_no_num in cleaned_title:
                                        val = cleaned_title.get(attr_key, cleaned_title.get(attr_key_no_num))
                                        if attr_key_no_num not in output_keys:
                                            safe_attr = standardize_key(attr_key_no_num)
                                            output.append(f"[title_{safe_attr}_key] {attr_key_no_num}: {val}")
                                            output_keys.add(attr_key_no_num)
                                        # Prevent later emission of the numbered variant
                                        output_keys.add(attr_key)
                                        all_comp_keys.discard(attr_key)
                                        all_comp_keys.discard(attr_key_no_num)
                                else:
                                    if attr_key in cleaned_title and attr_key not in output_keys:
                                        safe_attr_key = standardize_key(attr_key)
                                        output.append(f"[title_{safe_attr_key}_key] {attr_key}: {cleaned_title[attr_key]}")
                                        output_keys.add(attr_key)
                                        all_comp_keys.discard(attr_key)

                for attr in config['shared_after']:
                    if attr in cleaned_title and attr not in output_keys:
                        # Always output shared_after keys when they exist (they are shared values)
                        safe_key = standardize_key(attr)
                        output.append(f"[title_{safe_key}_key] {attr}: {cleaned_title[attr]}")
                        output_keys.add(attr)
                        all_comp_keys.discard(attr)

                for key in sorted(all_comp_keys):
                    if not re.match(rf'{comp}_\w+\d+', key) and key not in output_keys:
                        safe_key = standardize_key(key)
                        output.append(f"[title_{safe_key}_key] {key}: {cleaned_title[key]}")
                        output_keys.add(key)

        if 'additional_info' in cleaned_title:
            safe_key = standardize_key('additional_info')
            output.append(f"[title_{safe_key}_key] additional_info: {cleaned_title['additional_info']}")

    if listing.metadata:
        output.append("\n====== METADATA ======")
        for key, value in listing.metadata.items():
            # FIXED: Check if key already has meta_*_key format
            if key.startswith('meta_') and key.endswith('_key'):
                # Key already has the correct format, use it as-is WITHOUT adding _key
                output.append(f"[{key}] {key}: {value}")
            else:
                # Key needs the meta_*_key format added
                safe_key = custom_key_mapping.get(key, standardize_key(key))
                output.append(f"[meta_{safe_key}_key] {key}: {value}")
                
    if listing.category:
        output.append("\n====== CATEGORY ======")
        if "category_path" in listing.category:
            output.append(f"[category_path_key] Category Path: {listing.category['category_path']}")
        if "leaf_category" in listing.category:
            output.append(f"[leaf_category_key] Category: {listing.category['leaf_category']}")

    if listing.specifics:
        output.append("\n====== SPECIFICS ======")
        
        # Normalize specifics so first numbered variant becomes base for certain keys
        def normalize_specifics_numbered_fields(specs: Dict) -> Dict:
            result = dict(specs)
            whitelist = {"specs_storage_capacity", "specs_network_status", "specs_network_carrier"}
            for base in whitelist:
                first = f"{base}1"
                if first in result and base not in result:
                    result[base] = result[first]
                if first in result:
                    result.pop(first, None)
            return result

        cleaned_specifics = normalize_specifics_numbered_fields(
            remove_base_keys_when_numbered_exist(listing.specifics, "specs_")
        )
        
        key_order = cleaned_specifics.get('__key_order__', [])
        # Detect if any CPU detail fields exist (base or numbered)
        has_any_cpu_details = any(
            re.match(r'^specs_cpu_(brand|family|model|suffix|speed|quantity|cores|generation|gen_other)(\d+)?$', k)
            for k in cleaned_specifics.keys()
        )
        for mapped_key, original_key in key_order:
            if mapped_key in cleaned_specifics and mapped_key != '__key_order__':
                base_key = mapped_key.replace('specs_', '', 1) if mapped_key.startswith('specs_') else mapped_key
                # Hide raw CPU/Processor fields when unfolded CPU detail fields exist
                if base_key in ['cpu', 'processor', 'processor_model'] and has_any_cpu_details:
                    continue
                if base_key == 'ram' and any(f'specs_ram_{k}' in cleaned_specifics for k in ram_detail_keys):
                    continue
                if base_key == 'storage' and any(k in cleaned_specifics for k in storage_detail_keys):
                    continue
                if base_key == 'screen_size' and any(f'specs_{k}' in cleaned_specifics for k in screen_detail_keys):
                    continue
                if base_key == 'videocard' and any(f'specs_{k}' in cleaned_specifics for k in gpu_detail_keys):
                    continue
                if base_key == 'os' and any(f'specs_{k}' in cleaned_specifics for k in os_detail_keys):
                    continue
                if base_key == 'device_type' and any(f'specs_{k}' in cleaned_specifics for k in device_detail_keys):
                    continue
                if base_key == 'battery' and any(f'specs_{k}' in cleaned_specifics for k in battery_detail_keys):
                    continue
                if base_key == 'switch' and any(f'specs_{k}' in cleaned_specifics for k in switch_detail_keys):
                    continue
                if base_key == 'adapter' and any(f'specs_{k}' in cleaned_specifics for k in adapter_detail_keys):
                    continue
                if base_key == 'os' and cleaned_specifics[mapped_key].lower() in ['no', 'n/a']:
                    output.append(f"[specs_{base_key}_key] {original_key}: Not Included")
                else:
                    _val = cleaned_specifics[mapped_key]
                    if isinstance(_val, str) and mapped_key in ('specs_ram_size', 'specs_storage_capacity'):
                        _val = _val.replace('&', '/')
                        _val = re.sub(r'\s*/\s*', '/', _val)
                    output.append(f"[specs_{base_key}_key] {original_key}: {_val}")
        
        # Handle CPU detail keys including numbered ones
        # First, check if we have numbered CPU models
        numbered_cpu_models = [k for k in cleaned_specifics.keys() if re.match(r'specs_cpu_model\d+$', k)]
        
        if numbered_cpu_models:
            # Handle numbered CPU models like title parsing does
            # Extract numbers and sort them
            cpu_numbers = []
            for key in numbered_cpu_models:
                match = re.match(r'specs_cpu_model(\d+)$', key)
                if match:
                    cpu_numbers.append(int(match.group(1)))
            
            cpu_numbers.sort()
            
            # Then output numbered CPU attributes
            for num in cpu_numbers:
                for detail_key in cpu_detail_keys:
                    specs_key = f'specs_cpu_{detail_key}{num}'
                    if specs_key in cleaned_specifics:
                        # Skip Apple Silicon CPU models
                        if detail_key == 'model' and is_apple_silicon_cpu(str(num), {f'cpu_brand{num}': cleaned_specifics.get(f'specs_cpu_brand{num}', ''), f'cpu_family{num}': cleaned_specifics.get(f'specs_cpu_family{num}', '')}):
                            continue
                        display_key = detail_key.replace('cpu_', '').capitalize()
                        output.append(f"[{specs_key}_key] CPU {display_key} {num}: {cleaned_specifics[specs_key]}")
        else:
            # Handle regular CPU detail keys
            for detail_key in cpu_detail_keys:
                full_key = f'specs_cpu_{detail_key}'
                if full_key in cleaned_specifics:
                    # Skip Apple Silicon CPU models
                    if detail_key == 'model' and is_apple_silicon_cpu('', {'cpu_brand': cleaned_specifics.get('specs_cpu_brand', ''), 'cpu_family': cleaned_specifics.get('specs_cpu_family', '')}):
                        continue
                    output.append(f"[{full_key}_key] CPU {detail_key.capitalize()}: {cleaned_specifics[full_key]}")
        
        for detail_key in ram_detail_keys:
            full_key = f'specs_ram_{detail_key}'
            if full_key in cleaned_specifics:
                _val = cleaned_specifics[full_key]
                if isinstance(_val, str) and full_key == 'specs_ram_size':
                    _val = _val.replace('&', '/')
                    _val = re.sub(r'\s*/\s*', '/', _val)
                output.append(f"[{full_key}_key] RAM {detail_key.capitalize()}: {_val}")
        
        # FIXED: Handle both base storage keys and numbered storage capacity keys
        for detail_key in storage_detail_keys:
            if detail_key in cleaned_specifics:
                base_key = detail_key.replace('storage_', '')
                _val = cleaned_specifics[detail_key]
                if isinstance(_val, str) and detail_key == 'specs_storage_capacity':
                    _val = _val.replace('&', '/')
                    _val = re.sub(r'\s*/\s*', '/', _val)
                output.append(f"[{detail_key}_key] Storage {base_key.capitalize()}: {_val}")
        
        # Handle numbered storage capacity keys (after normalization base + 2,3,... remain)
        storage_capacity_numbered_keys = [k for k in cleaned_specifics.keys() if re.match(r'specs_storage_capacity\d+$', k)]
        if storage_capacity_numbered_keys:
            # Sort by number
            storage_capacity_numbered_keys.sort(key=lambda x: int(re.search(r'\d+$', x).group()))
            for key in storage_capacity_numbered_keys:
                number = re.search(r'(\d+)$', key).group(1)
                output.append(f"[{key}_key] Storage Capacity {number}: {cleaned_specifics[key]}")
        
        for detail_key in screen_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('screen_', '').capitalize()
                output.append(f"[{full_key}_key] Screen {display_key}: {cleaned_specifics[full_key]}")
        
        for detail_key in gpu_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('gpu_', '').capitalize()
                output.append(f"[{full_key}_key] GPU {display_key}: {cleaned_specifics[full_key]}")
        
        for detail_key in os_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('os_', '').capitalize()
                output.append(f"[{full_key}_key] OS {display_key}: {cleaned_specifics[full_key]}")
        
        for detail_key in device_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                if detail_key == 'device_type':
                    output.append(f"[{full_key}_key] Device Type: {cleaned_specifics[full_key]}")
                elif detail_key == 'form_factor':
                    output.append(f"[{full_key}_key] Form Factor: {cleaned_specifics[full_key]}")
                elif detail_key == 'rack_units':
                    output.append(f"[{full_key}_key] Rack Units: {cleaned_specifics[full_key]}")
        
        for detail_key in battery_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('battery_', '').capitalize()
                output.append(f"[{full_key}_key] Battery {display_key}: {cleaned_specifics[full_key]}")

        for detail_key in switch_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('switch_', '').capitalize()
                output.append(f"[{full_key}_key] Switch {display_key}: {cleaned_specifics[full_key]}")

        for detail_key in adapter_detail_keys:
            full_key = f'specs_{detail_key}'
            if full_key in cleaned_specifics:
                display_key = detail_key.replace('adapter_', '').capitalize()
                output.append(f"[{full_key}_key] Adapter {display_key}: {cleaned_specifics[full_key]}")

        # Post-process: remove exact duplicate lines within the SPECIFICS section only
        try:
            # Find the start of the latest SPECIFICS section
            spec_header_idx = None
            for idx in range(len(output) - 1, -1, -1):
                if output[idx].strip() == "====== SPECIFICS ======":
                    spec_header_idx = idx
                    break
            if spec_header_idx is not None:
                # Find the next section header or end of output
                next_section_idx = None
                for j in range(spec_header_idx + 1, len(output)):
                    line_j = output[j].strip()
                    if line_j.startswith("====== ") and line_j != "====== SPECIFICS ======":
                        next_section_idx = j
                        break
                if next_section_idx is None:
                    next_section_idx = len(output)

                seen_signatures = set()
                deduped_lines = []
                for line in output[spec_header_idx + 1:next_section_idx]:
                    # Build a signature based on the bracketed key and the value after the colon
                    # This treats lines like "[key] Processor Speed: X" and "[key] CPU Speed: X" as duplicates
                    m = re.match(r'^\[(.*?)\]\s*[^:]*:\s*(.*)$', line)
                    signature = (m.group(1).strip(), m.group(2).strip()) if m else line
                    if signature not in seen_signatures:
                        deduped_lines.append(line)
                        seen_signatures.add(signature)

                # Rebuild output with deduplicated SPECIFICS content
                output = output[:spec_header_idx + 1] + deduped_lines + output[next_section_idx:]
        except Exception:
            # Non-fatal; if anything goes wrong, proceed without deduplication
            pass

    if listing.table_data:
        all_entries = listing.table_data
        if all_entries:
            output.append("\n====== TABLE DATA ======")
            output.append(f"[table_entry_count_key] Total Entries: {len(all_entries)}")
            
            # Enhanced pre-processing using helper functions
            process_storage_fields(all_entries)
            normalize_table_numbered_fields(all_entries)
            remove_base_keys_with_numbered_variants(all_entries, logger)
            
            # Process shared values
            shared_values = get_shared_values(all_entries)
            shared_values = remove_base_keys_from_shared_values(shared_values, logger)
            
            # Output shared values
            if shared_values:
                output.append("\nShared Values:")
                first_entry = all_entries[0]
                shared_output = format_shared_values_output(shared_values, first_entry)
                output.extend(shared_output)
            
            # Get a case-insensitive set of shared keys
            shared_keys_case_insensitive = set(k.lower() for k in shared_values.keys())
            
            # Process individual entries
            for i, entry in enumerate(all_entries, 1):
                output.append(f"\nEntry {i}:")
                entry_output = format_entry_values_output(entry, i, shared_keys_case_insensitive)
                output.extend(entry_output)

    if listing.description:
        output.append("\n====== DESCRIPTION ======")
        key_order = listing.description.get('__key_order__', [])
        custom_key_mapping = {
            'Cosmetic Condition': 'cosmeticcondition',
            'Functional Condition': 'functionalcondition',
            'Data Sanitization': 'datasanitization',
            'description_text': 'descriptiontext'
        }
        for mapped_key, original_key in key_order:
            if mapped_key in listing.description and mapped_key != '__key_order__':
                safe_key = custom_key_mapping.get(original_key, standardize_key(original_key))
                output.append(f"[desc_{safe_key}_key] {original_key}: {listing.description[mapped_key]}")

    return "\n".join(output)
    
def process_file(filepath: str, logger: logging.Logger) -> str:
    logger.info(f"Processing file: {os.path.basename(filepath)}")
    try:
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
        content = None
        for encoding in encodings_to_try:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    content = f.read()
                logger.debug(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            logger.warning(f"Fell back to utf-8 with errors='replace' for {filepath}")
        if not content:
            logger.error(f"No valid content read from {filepath}")
            return f"Error: No valid content read from {filepath}"
            
        listing = ListingData()
        listing.metadata = parse_metadata(content, logger)
        logger.debug(f"Parsed metadata: {listing.metadata}")
        
        title_text = listing.metadata.get('Title', '')
        title_text = title_text.replace('*', '')
        if not title_text:
            logger.warning("No 'Title' found in metadata; using 'Unknown Title'")
        print(f"Title from metadata: '{title_text or 'Unknown Title'}'")
        
        listing.title = parse_title_components(title_text, logger)
        logger.debug(f"Parsed title: {listing.title}")
        
        listing.category = parse_category(content, logger)
        logger.debug(f"Parsed category: {listing.category}")
        
        # NEW: Override category if needed based on device type and brand
        device_type = listing.title.get("device_type")
        brand = listing.title.get("brand")
        if device_type and brand:
            listing.category = override_category_for_device_type(listing.category, device_type, brand, logger)
            logger.debug(f"Category after override check: {listing.category}")
        
        listing.specifics = parse_item_specifics(content, listing.category, logger, listing)
        logger.debug(f"Parsed specifics: {listing.specifics}")
        
        listing.table_data = parse_table_data(content, logger)
        logger.debug(f"Parsed table_data: {listing.table_data}")
        
        listing.description = parse_description(content, logger)
        logger.debug(f"Parsed description: {listing.description}")
        
        # Apply post-processing with full context for ambiguous storage/RAM detection
        listing.title = check_and_reassign_ambiguous_storage_to_ram(
            listing.title, 
            listing.specifics, 
            listing.table_data, 
            logger
        )
        logger.debug(f"Applied ambiguous storage/RAM reassignment: {listing.title}")
        
        return format_output(listing, logger)
    except Exception as e:
        import traceback
        logger.error(f"Error processing file: {str(e)}")
        logger.error(f"Stack trace:\n{traceback.format_exc()}")
        return f"Error processing file: {str(e)}"
        
        
def process_file_to_listing_data(filepath: str, logger: logging.Logger) -> Tuple['ListingData', str]:
    """
    Process file and return ListingData object instead of formatted string
    Returns (ListingData object, error_message or empty string)
    """
    logger.info(f"Processing file: {os.path.basename(filepath)}")
    try:
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
        content = None
        for encoding in encodings_to_try:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    content = f.read()
                logger.debug(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            logger.warning(f"Fell back to utf-8 with errors='replace' for {filepath}")
        if not content:
            logger.error(f"No valid content read from {filepath}")
            return None, f"Error: No valid content read from {filepath}"
            
        listing = ListingData()
        listing.metadata = parse_metadata(content, logger)
        logger.debug(f"Parsed metadata: {listing.metadata}")
        
        title_text = listing.metadata.get('Title', '')
        title_text = title_text.replace('*', '')
        if not title_text:
            logger.warning("No 'Title' found in metadata; using 'Unknown Title'")
        print(f"Title from metadata: '{title_text or 'Unknown Title'}'")
        
        listing.title = parse_title_components(title_text, logger)
        logger.debug(f"Parsed title: {listing.title}")
        
        listing.category = parse_category(content, logger)
        logger.debug(f"Parsed category: {listing.category}")
        
        # Override category if needed based on device type and brand
        device_type = listing.title.get("device_type")
        brand = listing.title.get("brand")
        if device_type and brand:
            listing.category = override_category_for_device_type(listing.category, device_type, brand, logger)
            logger.debug(f"Category after override check: {listing.category}")
        
        listing.specifics = parse_item_specifics(content, listing.category, logger, listing)
        logger.debug(f"Parsed specifics: {listing.specifics}")
        
        listing.table_data = parse_table_data(content, logger)
        logger.debug(f"Parsed table_data: {listing.table_data}")
        
        listing.description = parse_description(content, logger)
        logger.debug(f"Parsed description: {listing.description}")
        
        # Apply post-processing with full context for ambiguous storage/RAM detection
        listing.title = check_and_reassign_ambiguous_storage_to_ram(
            listing.title, 
            listing.specifics, 
            listing.table_data, 
            logger
        )
        logger.debug(f"Applied ambiguous storage/RAM reassignment: {listing.title}")
        
        return listing, ""
        
    except Exception as e:
        import traceback
        logger.error(f"Error processing file: {str(e)}")
        logger.error(f"Stack trace:\n{traceback.format_exc()}")
        return None, f"Error processing file: {str(e)}"
        
        
def append_title_to_tools_titles_extracted(title_value: str) -> None:
    """Append a single title line to tools/titles_extracted.txt with simple de-duplication.
    Intent: keep an ever-growing corpus of titles for ongoing dev/training utilities.
    """
    try:
        if not isinstance(title_value, str):
            return
        normalized = title_value.strip()
        if not normalized:
            return
        repo_root = os.path.dirname(os.path.abspath(__file__))
        tools_dir = os.path.join(repo_root, 'tools')
        out_file = os.path.join(tools_dir, 'titles_extracted.txt')
        try:
            os.makedirs(tools_dir, exist_ok=True)
        except Exception:
            pass
        # Check if the title already exists (avoid duplicates)
        exists_already = False
        try:
            with open(out_file, 'r', encoding='utf-8', errors='ignore') as rf:
                for existing_line in rf:
                    if existing_line.rstrip('\n') == normalized:
                        exists_already = True
                        break
        except FileNotFoundError:
            exists_already = False
        except Exception:
            # On read error, fall back to appending
            exists_already = False
        if not exists_already:
            with open(out_file, 'a', encoding='utf-8', newline='\n') as af:
                af.write(normalized + '\n')
    except Exception:
        # Silent on purpose; callers handle logging
        pass


def process_and_write_file(filename, process_path):
    """
    ENHANCED: Process file with dual-mode support (files + database)
    """
    input_path = os.path.join(process_path, filename)
    item_number = filename.replace('_description.txt', '')
    output_filename = f"python_parsed_{item_number}.txt"
    output_path = os.path.join(process_path, output_filename)
    logger = setup_logging(item_number)
    logger.info(f"🔄 Processing file: {filename}")
    
    # Database storage status tracking
    database_success = False
    file_success = False
    
    try:
        # Process file to get ListingData object
        listing_data, error_message = process_file_to_listing_data(input_path, logger)
        
        if listing_data is None:
            logger.error(f"❌ Failed to process file: {error_message}")
            return f"Error: {error_message}", item_number
        
        # Store in database (if enabled)
        if ENABLE_DATABASE_STORAGE:
            try:
                database_success = store_listing_in_database(listing_data, item_number, logger)
                if database_success:
                    logger.info(f"💾 Database storage: SUCCESS")
                else:
                    logger.warning(f"⚠️ Database storage: FAILED")
            except Exception as e:
                logger.error(f"❌ Database storage error: {e}", exc_info=True)
                database_success = False
        else:
            database_success = True  # Consider success if disabled
        
        # Create file output (if enabled)
        if KEEP_FILE_OUTPUT:
            try:
                # Generate formatted output using existing function
                formatted_result = format_output(listing_data, logger)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(formatted_result)
                
                file_success = True
                logger.info(f"📁 File storage: SUCCESS - {output_filename}")

                # NEW: Write title-only file containing just Full Title and [title_*_key] lines
                try:
                    import re
                    title_only_filename = f"title_only_{item_number}.txt"
                    title_only_path = os.path.join(process_path, title_only_filename)
                    # Extract block under TITLE DATA up to next section header
                    m = re.search(r"^======\s*TITLE\s*DATA\s*======\s*\n([\s\S]*?)(?=^====== |\Z)", formatted_result, flags=re.MULTILINE)
                    title_block = m.group(1) if m else ""
                    if not title_block and getattr(listing_data, 'title', None):
                        lines = []
                        full_title = listing_data.title.get('Full Title')
                        if isinstance(full_title, str) and full_title:
                            lines.append(f"Full Title: {full_title}")
                        try:
                            normalized = normalize_title_numbered_fields(listing_data.title)
                            for key, value in normalized.items():
                                if key == 'Full Title':
                                    continue
                                safe_key = standardize_key(key)
                                lines.append(f"[title_{safe_key}_key] {key}: {value}")
                        except Exception:
                            pass
                        title_block = "\n".join(lines)
                    out_lines = []
                    for line in title_block.splitlines():
                        ls = line.strip()
                        if not ls:
                            continue
                        if ls.startswith('Full Title:') or ls.startswith('[title_'):
                            out_lines.append(ls)
                    with open(title_only_path, 'w', encoding='utf-8') as tf:
                        tf.write("\n".join(out_lines) + ("\n" if out_lines else ""))
                    logger.info(f"📄 Title-only file written: {title_only_filename}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to write title-only file for {item_number}: {e}")

                # NEW: Append the Full Title to tools/titles_extracted.txt for ongoing dev
                try:
                    full_title_value = None
                    if getattr(listing_data, 'title', None):
                        ft = listing_data.title.get('Full Title')
                        if isinstance(ft, str) and ft.strip():
                            full_title_value = ft.strip()
                    if full_title_value:
                        append_title_to_tools_titles_extracted(full_title_value)
                        logger.debug("📥 Appended Full Title to tools/titles_extracted.txt")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to append title to tools/titles_extracted.txt for {item_number}: {e}")
                
                # NEW: Auto-fix missing storage keys
                try:
                    import subprocess
                    fix_script = os.path.join(os.path.dirname(__file__), 'fix_missing_storage_keys.py')
                    if os.path.exists(fix_script):
                        result = subprocess.run([sys.executable, fix_script, output_path], 
                                              capture_output=True, text=True, check=False)
                        if result.returncode == 0:
                            logger.debug(f"🔧 Storage fix: SUCCESS - {output_filename}")
                        else:
                            logger.debug(f"🔧 Storage fix: SKIPPED - {output_filename} (no fixes needed)")
                    else:
                        logger.debug(f"🔧 Storage fix script not found: {fix_script}")
                except Exception as fix_error:
                    logger.debug(f"🔧 Storage fix error: {fix_error}")
                
            except Exception as e:
                logger.error(f"❌ File storage error: {e}", exc_info=True)
                file_success = False
        else:
            file_success = True  # Consider success if disabled
        
        # Always attempt to append the Full Title for ongoing dev, regardless of KEEP_FILE_OUTPUT
        try:
            full_title_value_always = None
            if getattr(listing_data, 'title', None):
                ft = listing_data.title.get('Full Title')
                if isinstance(ft, str) and ft.strip():
                    full_title_value_always = ft.strip()
            if full_title_value_always:
                append_title_to_tools_titles_extracted(full_title_value_always)
                logger.debug("📥 Appended Full Title to tools/titles_extracted.txt (post-processing)")
        except Exception as e:
            logger.debug(f"⚠️ Post-processing append to titles_extracted failed for {item_number}: {e}")

        # Determine overall success
        if database_success and file_success:
            logger.info(f"✅ COMPLETE SUCCESS: {filename}")
            return "SUCCESS", item_number
        elif database_success or file_success:
            # Partial success
            status = []
            if database_success:
                status.append("DB:OK")
            if file_success:
                status.append("FILE:OK")
            if not database_success:
                status.append("DB:FAILED")
            if not file_success:
                status.append("FILE:FAILED")
            
            logger.warning(f"⚠️ PARTIAL SUCCESS: {filename} - {','.join(status)}")
            return f"PARTIAL SUCCESS: {','.join(status)}", item_number
        else:
            logger.error(f"❌ COMPLETE FAILURE: {filename}")
            return "Error: Both database and file storage failed", item_number
            
    except Exception as e:
        logger.error(f"❌ Unexpected error processing {filename}: {e}", exc_info=True)
        return f"Error: Unexpected error: {str(e)}", item_number

def listing_data_to_dict(listing: 'ListingData', item_number: str) -> Dict:
    """Convert ListingData object to dictionary format for database storage with correct key names"""
    try:
        # Convert title data to use proper key names (as they appear in python_parsed files)
        title_dict = {}
        if listing.title:
            # Apply same normalization used for output so DB keys match expected format
            normalized_title = normalize_title_numbered_fields(listing.title)
            for key, value in normalized_title.items():
                if key == "Full Title":
                    title_dict["Full Title"] = value
                else:
                    # Convert internal keys to file format key names
                    if key in ["brand", "model", "device_type", "cpu_brand", "cpu_family", "cpu_model", 
                              "cpu_generation", "cpu_speed", "cpu_suffix", "ram_size", "storage_type", 
                              "storage_status", "battery_status", "additional_info"]:
                        title_dict[f"title_{key}_key"] = value
                    else:
                        # Handle numbered fields (cpu_brand1, cpu_family2, etc.)
                        title_dict[f"title_{key}_key"] = value
        
        # Convert metadata to use proper key names
        metadata_dict = {}
        if listing.metadata:
            for key, value in listing.metadata.items():
                # Convert clean keys to prefixed format
                if key in ["Title", "Custom Label", "Listing Info", "Item Number"]:
                    safe_key = key.lower().replace(' ', '')
                    metadata_dict[f"meta_{safe_key}_key"] = value
                elif key.startswith("meta_"):
                    # Already properly formatted
                    metadata_dict[key] = value
                else:
                    # Generic metadata fields
                    safe_key = key.lower().replace(' ', '_').replace('-', '_')
                    metadata_dict[f"meta_{safe_key}_key"] = value
        
        # Convert specifics to use proper key names
        specifics_dict = {}
        if listing.specifics:
            for key, value in listing.specifics.items():
                if key.startswith("specs_"):
                    # Already properly formatted
                    specifics_dict[key] = value
                else:
                    # Convert clean keys to prefixed format
                    safe_key = key.lower().replace(' ', '_').replace('-', '_')
                    specifics_dict[f"specs_{safe_key}_key"] = value
        
        # Convert description to use proper key names
        description_dict = {}
        if listing.description:
            for key, value in listing.description.items():
                if key.startswith("desc_"):
                    # Already properly formatted
                    description_dict[key] = value
                else:
                    # Convert clean keys to prefixed format
                    safe_key = key.lower().replace(' ', '_').replace('-', '_')
                    description_dict[f"desc_{safe_key}_key"] = value
        
        return {
            'title': title_dict,
            'metadata': metadata_dict,
            'category': listing.category or '',
            'specifics': specifics_dict,
            'description': description_dict,
            'table_data': list(listing.table_data) if listing.table_data else []
        }
    except Exception as e:
        logging.getLogger(__name__).error(f"Error converting ListingData to dict for {item_number}: {e}")
        return {
            'title': {},
            'metadata': {},
            'category': '',
            'specifics': {},
            'description': {},
            'table_data': []
        }


def store_listing_in_database(listing: 'ListingData', item_number: str, logger: logging.Logger) -> bool:
    """Store processed listing data in SQLite database"""
    if not ENABLE_DATABASE_STORAGE:
        return True  # Skip if database storage is disabled
    
    try:
        # Convert ListingData to dictionary format
        listing_dict = listing_data_to_dict(listing, item_number)
        
        # Insert into database
        db = get_database()
        success = db.insert_listing(item_number, listing_dict)
        
        if success:
            logger.info(f"✅ Successfully stored listing {item_number} in database")
        else:
            logger.error(f"❌ Failed to store listing {item_number} in database")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Error storing listing {item_number} in database: {e}", exc_info=True)
        return False


def process_and_write_file_wrapper(args):
    """Wrapper function for multiprocessing - unpacks arguments"""
    filename, process_path = args
    return process_and_write_file(filename, process_path)

def main():
    configure_root_logger()
    logger = setup_logging()
    logger.debug("Starting main processing loop")
    process_path = "item_contents"
    parser = argparse.ArgumentParser(description="Process item description files")
    parser.add_argument('item_number', nargs='?', help='Specific item number to process (e.g., 297222787777)')
    parser.add_argument('--log', action='store_true', help='Enable detailed logging')
    parser.add_argument('--skip-runit', action='store_true', help='Skip calling runit.py after processing')
    args = parser.parse_args()
    
    specific_item_number = args.item_number
    
    # DYNAMICALLY DETECT MAXIMUM SYSTEM RESOURCES
    cpu_cores = multiprocessing.cpu_count()
    logical_cores = os.cpu_count() or cpu_cores  # Fallback to multiprocessing count
    
    # For processes: Use all available CPU cores
    max_processes = cpu_cores
    
    # For threads: Use 2x CPU cores (good for mixed I/O and CPU work)
    # You can adjust this multiplier based on your workload
    max_threads = cpu_cores * 2
    
    # Always use multiprocessing for CPU-bound text processing
    use_multiprocessing = True
    
    logger.info(f"🖥️  SYSTEM DETECTION:")
    logger.info(f"   CPU Cores: {cpu_cores}")
    logger.info(f"   Logical Cores: {logical_cores}")
    logger.info(f"🚀 DYNAMIC MAX SETTINGS:")
    logger.info(f"   Max Processes: {max_processes}")
    logger.info(f"   Max Threads: {max_threads}")
    logger.info(f"   Using Multiprocessing: {use_multiprocessing}")
    
    # Database status information
    logger.info(f"💾 DATABASE CONFIGURATION:")
    logger.info(f"   Database Available: {DATABASE_AVAILABLE}")
    logger.info(f"   Database Storage: {'ENABLED' if ENABLE_DATABASE_STORAGE else 'DISABLED'}")
    logger.info(f"   File Output: {'ENABLED' if KEEP_FILE_OUTPUT else 'DISABLED'}")
    
    if ENABLE_DATABASE_STORAGE:
        try:
            db = get_database()
            stats = db.get_database_stats()
            if stats:
                total_records = sum(s.get('count', 0) for s in stats.get('status_breakdown', []))
                logger.info(f"   Existing Records: {total_records}")
                logger.info(f"   Database Size: {stats.get('database_size_mb', 0):.1f} MB")
        except Exception as e:
            logger.warning(f"   Database Status: Error connecting - {e}")
    
    # Storage mode summary
    if ENABLE_DATABASE_STORAGE and KEEP_FILE_OUTPUT:
        logger.info(f"🔄 DUAL-MODE OPERATION: Writing to both DATABASE and FILES")
    elif ENABLE_DATABASE_STORAGE:
        logger.info(f"💾 DATABASE-ONLY MODE: Writing only to database")
    else:
        logger.info(f"📁 FILE-ONLY MODE: Writing only to files")
    
    files = []
    is_single_item_mode = False
    
    caps_lock_on = win32api.GetKeyState(0x14) & 0x0001
    if caps_lock_on:
        try:
            clipboard_content = pyperclip.paste().strip()
            if clipboard_content.isdigit():
                specific_item_number = clipboard_content
                logger.info(f"Using clipboard item number: {specific_item_number}")
        except Exception as e:
            logger.error(f"Failed to read clipboard: {str(e)}")
    
    if specific_item_number:
        is_single_item_mode = True
        if not specific_item_number.isdigit():
            logger.error(f"Invalid item number: '{specific_item_number}'. Must be numeric.")
            sys.exit(1)
        target_filename = f"{specific_item_number}_description.txt"
        found_files = [f for f in os.listdir(process_path) if f.lower() == target_filename.lower()]
        if not found_files:
            logger.error(f"File for item number {specific_item_number} not found.")
            sys.exit(1)
        files = [found_files[0]]
    else:
        files = [f for f in os.listdir(process_path) if f.lower().endswith("_description.txt")]
    
    logger.info(f"Found {len(files)} files to process")
    # Guard against zero-file scenario to avoid creating a process pool with 0 workers
    if len(files) == 0:
        logger.warning("⚠️ No files found to process in the target folder. Exiting without starting workers.")
        return
    
    # Single item mode or single file - process without threading/multiprocessing
    if is_single_item_mode or len(files) == 1:
        for filename in files:
            result, item_number = process_and_write_file(filename, process_path)
            if is_single_item_mode and "Error" not in result and not args.skip_runit:
                try:
                    subprocess.run(['python', 'runit.py', item_number], check=True)
                    logger.info(f"Successfully executed 'runit.py' with item number: {item_number}")
                except Exception as e:
                    logger.error(f"Error running 'runit.py': {str(e)}")
    else:
        # DYNAMIC PERFORMANCE OPTIMIZATION
        # Choose the best approach based on file count and system resources
        optimal_workers = min(max_processes, len(files))  # Don't create more workers than files
        if optimal_workers <= 0:
            logger.warning("⚠️ Computed 0 worker processes; skipping parallel execution.")
            return
        
        logger.info(f"🔥 MAXIMUM PERFORMANCE MODE: Using {optimal_workers} processes across {cpu_cores} CPU cores")
        logger.info(f"💪 System will be fully utilized for fastest processing!")
        
        # Track processed files to avoid duplicates
        processed_files = set()
        successful_items = []
        failed_items = []
        
        # Use ProcessPoolExecutor for maximum CPU utilization
        with ProcessPoolExecutor(max_workers=optimal_workers) as executor:
            # Submit all file processing tasks
            future_to_filename = {}
            for filename in files:
                if filename not in processed_files:
                    future = executor.submit(process_and_write_file_wrapper, (filename, process_path))
                    future_to_filename[future] = filename
                    processed_files.add(filename)
            
            logger.info(f"🚀 Submitted {len(future_to_filename)} files for parallel processing")
            logger.info(f"⚡ Using {optimal_workers} worker processes on {cpu_cores}-core system")
            
            # Process completed tasks as they finish
            completed_count = 0
            import time
            start_time = time.time()
            
            for future in as_completed(future_to_filename):
                filename = future_to_filename[future]
                completed_count += 1
                elapsed = time.time() - start_time
                
                try:
                    result, item_number = future.result()
                    if "Error processing file" in result:
                        failed_items.append((filename, item_number, result))
                        logger.error(f"❌ [{completed_count}/{len(files)}] Failed: {filename}")
                    else:
                        successful_items.append((filename, item_number))
                        # Calculate rate and ETA
                        rate = completed_count / elapsed if elapsed > 0 else 0
                        eta = (len(files) - completed_count) / rate if rate > 0 else 0
                        logger.info(f"✅ [{completed_count}/{len(files)}] Completed: {filename} | Rate: {rate:.1f}/sec | ETA: {eta:.0f}s")
                except Exception as e:
                    failed_items.append((filename, "unknown", str(e)))
                    logger.error(f"💥 [{completed_count}/{len(files)}] Exception: {filename}: {str(e)}")
        
        # Final performance summary
        total_time = time.time() - start_time
        files_per_second = len(successful_items) / total_time if total_time > 0 else 0
        
        logger.info(f"🏁 PROCESSING COMPLETE!")
        logger.info(f"⏱️  Total Time: {total_time:.2f} seconds")
        logger.info(f"🚄 Processing Rate: {files_per_second:.2f} files/second")
        logger.info(f"✅ Successfully processed: {len(successful_items)}")
        logger.info(f"❌ Failed: {len(failed_items)}")
        logger.info(f"🔥 CPU Utilization: {optimal_workers}/{cpu_cores} cores used")
        
        if failed_items:
            logger.error("❌ Failed files:")
            for filename, item_number, error in failed_items[:5]:  # Show first 5 failures
                logger.error(f"  {filename} (Item: {item_number})")
            if len(failed_items) > 5:
                logger.error(f"  ... and {len(failed_items) - 5} more failures")
        
        if successful_items:
            logger.info(f"✅ Sample of processed files:")
            for filename, item_number in successful_items[:3]:  # Show first 3
                logger.info(f"  {filename} -> python_parsed_{item_number}.txt")
            if len(successful_items) > 3:
                logger.info(f"  ... and {len(successful_items) - 3} more files")
                
if __name__ == "__main__":
    # This is required for multiprocessing on Windows
    multiprocessing.freeze_support()
    main()