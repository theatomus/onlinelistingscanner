# extractor_ram.py
from configs.parser import RAMExtractor
from typing import Dict, List, Set, Any, Optional
import re

# Helper function to convert size strings to MB for comparison
def to_mb(size_str):
    size_str = size_str.lower()
    if 'gb' in size_str:
        return float(size_str.replace('gb', '')) * 1024
    elif 'tb' in size_str:
        return float(size_str.replace('tb', '')) * 1024 * 1024
    elif 'mb' in size_str:
        return float(size_str.replace('mb', ''))
    return 0

# Helper functions for concise, readable pattern definitions
def str_pat(value, optional=False, show=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

# Helper to check if a token is a storage indicator (used across RAM extraction rules)
def _token_is_storage(token: str) -> bool:
    """Return True if the cleaned token represents a storage-related keyword.

    This function has been enhanced to be more tolerant of punctuation or
    numeric suffixes that are frequently attached to storage keywords in
    listing titles (e.g. ``"HDD#2"``, ``"SSD,"``, ``"NVMe-1"``). The
    logic now:

    1. Performs fast exact-match checks.
    2. Strips common non-alphanumeric characters (except for the forward
       slash which appears in patterns like ``"os/ssd"``) and re-checks.
    3. Falls back to a regex *contains* search so that variants like
       ``"hdds"`` or ``"localStorage"`` are still detected.
    """
    cleaned_original = token.lower()
    storage_terms = {
        "ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives",
        "harddrive", "hard", "local", "locstorage", "hd", "os/ssd", "ssd/os",
        "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks", "read"
    }

    # 1) Quick exact match on the raw lowercase token
    if cleaned_original in storage_terms:
        return True

    # 2) Strip punctuation (except "/") and retry exact match
    cleaned_stripped = re.sub(r"[^a-z0-9/.]", "", cleaned_original)
    if cleaned_stripped in storage_terms:
        return True
    # Handle simple plural forms after stripping (e.g., "hdds" -> "hdd")
    if cleaned_stripped.endswith("s") and cleaned_stripped[:-1] in storage_terms:
        return True

    # 3) Fallback: substring check for any storage term inside the cleaned token
    for term in storage_terms:
        if term in cleaned_stripped:
            return True

    return False

def parse_complex_ram_format(text: str, logger) -> Dict:
    """Parse complex RAM formats like '240Gb 1866 MHz64Gb (8Gb x 8) per node, 3 nodes 48Gb (8Gb x 6), 1 node'"""
    result = {}
    
    # Extract total RAM size (first number)
    total_match = re.search(r'^(\d+)(gb|tb|mb)', text.lower())
    if total_match:
        result["ram_size"] = f"{total_match.group(1)}{total_match.group(2)}"
        logger.debug(f"Extracted total RAM size: {result['ram_size']}")
    
    # Extract speed (look for pattern like "1866 MHz" or "1866MHz")
    speed_match = re.search(r'(\d+)\s*mhz', text.lower())
    if speed_match:
        result["ram_speed_grade"] = f"{speed_match.group(1)}MHz"
        logger.debug(f"Extracted RAM speed: {result['ram_speed_grade']}")
    
    # Parse node configurations and calculate total modules
    node_configs = []
    
    # Pattern: (XGb x Y) per node, Z nodes or (XGb x Y), Z nodes
    node_pattern = re.finditer(r'\((\d+)gb\s*x\s*(\d+)\)[^,]*?(\d+)\s*nodes?', text.lower())
    for match in node_pattern:
        module_size = int(match.group(1))
        modules_per_node = int(match.group(2))
        num_nodes = int(match.group(3))
        total_modules = modules_per_node * num_nodes
        node_configs.append((module_size, total_modules))
        logger.debug(f"Node config: {module_size}GB x {modules_per_node} per node, {num_nodes} nodes = {module_size}GB x {total_modules}")
    
    # Pattern: (XGb x Y), Z node (single node)
    single_node_pattern = re.finditer(r'\((\d+)gb\s*x\s*(\d+)\)[^,]*?(\d+)\s*node(?!s)', text.lower())
    for match in single_node_pattern:
        module_size = int(match.group(1))
        modules_per_node = int(match.group(2))
        num_nodes = int(match.group(3))
        total_modules = modules_per_node * num_nodes
        node_configs.append((module_size, total_modules))
        logger.debug(f"Single node config: {module_size}GB x {modules_per_node}, {num_nodes} node = {module_size}GB x {total_modules}")
    
    if node_configs:
        logger.debug(f"Found {len(node_configs)} node configurations: {node_configs}")
        
        # Check if all modules are the same size
        module_sizes = [config[0] for config in node_configs]
        if len(set(module_sizes)) == 1:
            # All same size, sum up the modules
            total_modules = sum(config[1] for config in node_configs)
            result["ram_config"] = f"{module_sizes[0]}gb x {total_modules}"
            logger.debug(f"Combined RAM config (same size): {result['ram_config']}")
        else:
            # Different sizes, list them separately
            config_parts = [f"{config[0]}gb x {config[1]}" for config in node_configs]
            result["ram_config"] = ", ".join(config_parts)
            logger.debug(f"Combined RAM config (different sizes): {result['ram_config']}")
    else:
        logger.debug("No node configurations found in complex format")
    
    return result
    
def step_0_handle_single_token_ram_format(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle single-token "32GBRAM" format"""
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        # Accept formats like '32GBRAM', '32GB-RAM', '32GB_RAM'
        if re.search(r'^\d+gb[-_]?\s*(ram|memory)$', token.lower(), re.IGNORECASE):
            results.append([i])
            consumed.add(i)
            already_matched_indices.add(i)
            if logger:
                logger.debug(f"RAM: Found single token RAM: {token}")
            continue

def step_0_5_handle_ampersand_patterns(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle "&" patterns like "8 & 16GB" """
    for i in range(len(tokens) - 2):
        if i in consumed or i in already_matched_indices:
            continue
            
        # Pattern: [number] & [number]GB or [number] & [number] GB
        if (tokens[i].isdigit() and 
            tokens[i + 1] == '&' and
            (re.search(r'^\d+(gb|tb|mb)$', tokens[i + 2], re.IGNORECASE) or
             (i + 3 < len(tokens) and tokens[i + 2].isdigit() and 
              re.search(r'^(gb|tb|mb)$', tokens[i + 3], re.IGNORECASE)))):
            
            # Check for RAM context nearby
            has_ram_context = False
            for j in range(max(0, i-3), min(len(tokens), i+6)):
                if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                    has_ram_context = True
                    break
            
            if has_ram_context:
                if i + 3 < len(tokens) and tokens[i + 2].isdigit():
                    # Pattern: 8 & 16 GB
                    results.append([i, i + 1, i + 2, i + 3])
                    consumed.add(i)
                    consumed.add(i + 1)
                    consumed.add(i + 2)
                    consumed.add(i + 3)
                    already_matched_indices.update([i, i + 1, i + 2, i + 3])
                else:
                    # Pattern: 8 & 16GB
                    results.append([i, i + 1, i + 2])
                    consumed.add(i)
                    consumed.add(i + 1)
                    consumed.add(i + 2)
                    already_matched_indices.update([i, i + 1, i + 2])
                if logger:
                    logger.debug(f"RAM: Found & pattern: {' '.join(tokens[j] for j in range(i, min(i+4, len(tokens))))}")
                continue

def step_1_handle_complete_ram_specifications(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle complete RAM specifications like "8GB DDR4 RAM" (size + type + RAM)"""
    for i in range(len(tokens) - 2):
        if i in consumed or i in already_matched_indices:
            continue
            
        # Pattern: [size]GB [DDR/type] RAM
        if (re.search(r'^\d+(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
            tokens[i + 1].lower() in ["ddr", "ddr2", "ddr3", "ddr4", "ddr5", "lpddr3", "lpddr4", "gddr5", "gddr6"] and
            tokens[i + 2].lower() in ["ram", "memory"]):
            results.append([i, i + 1, i + 2])
            consumed.add(i)
            consumed.add(i + 1)
            consumed.add(i + 2)
            already_matched_indices.update([i, i + 1, i + 2])
            if logger:
                logger.debug(f"RAM: Found complete specification: {tokens[i]} {tokens[i + 1]} {tokens[i + 2]}")
            continue

def step_1_5_handle_size_plus_ddr_type(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle size + DDR type without RAM word (like "16GB DDR4")"""
    for i in range(len(tokens) - 1):
        if i in consumed or i in already_matched_indices:
            continue
            
        # Pattern: [size]GB [DDR/type] (no RAM word needed)
        if (re.search(r'^\d+(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
            tokens[i + 1].lower() in ["ddr", "ddr2", "ddr3", "ddr4", "ddr5", "lpddr3", "lpddr4", "gddr5", "gddr6"]):
            
            # Check for immediate storage or RAID context to avoid false positives
            has_immediate_storage_or_raid = False
            if i + 2 < len(tokens):
                next_token = tokens[i + 2].lower()
                if next_token in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives", "hd", "raid", "sas", "sata", "ssd/os", "m.2", "m2", "msata", "scsi", "disk", "disks"]:
                    has_immediate_storage_or_raid = True
            
            if not has_immediate_storage_or_raid:
                results.append([i, i + 1])
                consumed.add(i)
                consumed.add(i + 1)
                already_matched_indices.update([i, i + 1])
                if logger:
                    logger.debug(f"RAM: Found size + DDR type: {tokens[i]} {tokens[i + 1]}")
                continue

def step_2_handle_dynamic_slash_patterns(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle dynamic slash patterns like "4/8GB RAM", "4/8/16GB RAM", "32/64/128/256GB RAM" etc."""
    i = 0
    while i < len(tokens):
        if i in consumed or i in already_matched_indices:
            i += 1
            continue
            
        # Look for start of slash pattern: either [number] or [numberGB]
        if tokens[i].isdigit() or re.search(r'^\d+(gb|tb|mb)$', tokens[i], re.IGNORECASE):
            # Scan forward to find the complete slash-separated sequence
            sequence_indices = [i]
            current_pos = i + 1
            
            # Continue while we see alternating slashes and numbers/sizes
            while (current_pos < len(tokens) - 1 and 
                   tokens[current_pos] == '/' and 
                   current_pos + 1 < len(tokens) and
                   (tokens[current_pos + 1].isdigit() or 
                    re.search(r'^\d+(gb|tb|mb)$', tokens[current_pos + 1], re.IGNORECASE))):
                sequence_indices.extend([current_pos, current_pos + 1])  # Add slash and next number
                current_pos += 2
            
            # Check if we have a valid slash sequence (at least 3 tokens: num/num or num/numGB)
            if len(sequence_indices) >= 3:
                # PRIORITY 1: Check immediate context (what directly follows the pattern)
                immediate_ram_follows = (current_pos < len(tokens) and 
                                       tokens[current_pos].lower() in ["ram", "memory"])
                immediate_storage_follows = (current_pos < len(tokens) and 
                                           tokens[current_pos].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"])
                
                # PRIORITY 2: Check immediate preceding context  
                immediate_ram_precedes = (i > 0 and 
                                        tokens[i - 1].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"])
                immediate_storage_precedes = (i > 0 and 
                                            tokens[i - 1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"])
                
                # Apply strict immediate context rules
                should_extract_as_ram = False
                
                if immediate_storage_follows or immediate_storage_precedes:
                    # If immediate storage context, definitely NOT RAM
                    if logger:
                        logger.debug(f"RAM: Skipping slash pattern due to immediate storage context: {' '.join(tokens[idx] for idx in sequence_indices)}")
                    should_extract_as_ram = False
                elif immediate_ram_follows or immediate_ram_precedes:
                    # If immediate RAM context, definitely RAM
                    should_extract_as_ram = True
                    if logger:
                        logger.debug(f"RAM: Found slash pattern with immediate RAM context: {' '.join(tokens[idx] for idx in sequence_indices)}")
                else:
                    # FALLBACK: Check broader context only if no immediate context
                    has_ram_context = False
                    for j in range(max(0, i-3), min(len(tokens), current_pos + 4)):
                        if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                            has_ram_context = True
                            break
                    
                    if has_ram_context:
                        should_extract_as_ram = True
                        if logger:
                            logger.debug(f"RAM: Found slash pattern with distant RAM context: {' '.join(tokens[idx] for idx in sequence_indices)}")
                
                if should_extract_as_ram:
                    # Extract individual numbers/sizes from the sequence
                    ram_values = []
                    for idx in sequence_indices:
                        if tokens[idx] != '/':  # Skip slash tokens
                            ram_values.append(idx)
                    
                    # Add each RAM value as a separate result
                    for value_idx in ram_values:
                        results.append([value_idx])
                    
                    # Mark all tokens in sequence as consumed
                    for idx in sequence_indices:
                        consumed.add(idx)
                        already_matched_indices.add(idx)
                    
                    # Also consume RAM indicator if present
                    if immediate_ram_follows:
                        consumed.add(current_pos)
                        already_matched_indices.add(current_pos)
                    
                    i = current_pos + (1 if immediate_ram_follows else 0)
                    continue
                    
        i += 1
        
def step_3_handle_ram_ranges(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle RAM ranges like "16GB-32GB RAM" """
    for i in range(len(tokens) - 1):
        if i in consumed or i in already_matched_indices:
            continue
            
        if (re.match(r"\d+(gb|tb|mb)-\d+(gb|tb|mb)", tokens[i], re.IGNORECASE) and
            i + 1 < len(tokens) and tokens[i + 1].lower() in ["ram", "memory"]):
            results.append([i, i + 1])
            consumed.add(i)
            consumed.add(i + 1)
            already_matched_indices.update([i, i + 1])
            if logger:
                logger.debug(f"RAM: Found range with explicit context: {tokens[i]} {tokens[i + 1]}")
            continue

def step_4_handle_simple_patterns(tokens, consumed, already_matched_indices, results, logger=None):
    """Handle simple patterns like "8GB RAM" """
    for i in range(len(tokens) - 1):
        if i in consumed or i in already_matched_indices:
            continue
            
        # Pattern: [size]GB RAM
        if (re.search(r'^\d+(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
            tokens[i + 1].lower() in ["ram", "memory"]):
            
            # FIXED: Only skip if this is actually a misplaced RAM indicator or GPU VRAM
            should_skip = False
            
            # Check for GPU context before the size token
            gpu_indicators = ["gpu", "graphics", "video", "vram", "quadro", "gtx", "rtx", "gt", "geforce", "radeon", "rx", "arc", "iris", "uhd", "hd"]
            if i > 0:
                prev_tok = tokens[i - 1].lower()
                if any(keyword in prev_tok for keyword in gpu_indicators) or \
                   re.search(r"(gt|gtx|rtx)\d{3,4}", prev_tok):
                    should_skip = True
                    if logger:
                        logger.debug(f"RAM: Skipping GPU VRAM in simple pattern: {tokens[i-1]} {tokens[i]} {tokens[i+1]}")
            
            # Check for GeForce + GPU model pattern
            if not should_skip and i > 1:
                prev2_tok = tokens[i - 2].lower()
                prev1_tok = tokens[i - 1].lower()
                if prev2_tok == "geforce" and re.search(r"(gt|gtx|rtx)\d{3,4}", prev1_tok):
                    should_skip = True
                    if logger:
                        logger.debug(f"RAM: Skipping GPU VRAM in GeForce pattern: {tokens[i-2]} {tokens[i-1]} {tokens[i]} {tokens[i+1]}")
            
            # Only skip if we detect this is likely a mislabeled storage pattern
            # Look for specific problematic patterns like "RAM [size] [storage_type]"
            if not should_skip and (i > 0 and 
                tokens[i - 1].lower() in ["ram", "memory"] and 
                i + 2 < len(tokens) and
                tokens[i + 2].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives", "hd", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"]):
                should_skip = True
                if logger:
                    logger.debug(f"RAM: Skipping potential mislabeled storage: {tokens[i-1]} {tokens[i]} {tokens[i+1]} {tokens[i+2]}")
            
            if not should_skip:
                results.append([i, i + 1])
                consumed.add(i)
                consumed.add(i + 1)
                already_matched_indices.update([i, i + 1])
                if logger:
                    logger.debug(f"RAM: Found simple pattern: {tokens[i]} {tokens[i + 1]}")
                continue
        
        # Pattern: RAM [size]GB - but check context carefully
        if (tokens[i].lower() in ["ram", "memory"] and
            re.search(r'^\d+(gb|tb|mb)$', tokens[i + 1], re.IGNORECASE)):
            
            # Check if this RAM token was already consumed as part of a previous pattern
            if i in already_matched_indices:
                continue
            
            # Check if this is actually GPU VRAM mislabeled as RAM
            is_gpu_vram = False
            gpu_indicators = ["gpu", "graphics", "video", "vram", "quadro", "gtx", "rtx", "gt", "geforce", "radeon", "rx", "arc", "iris", "uhd", "hd"]
            
            # Check previous tokens for GPU context
            if i > 0:
                prev_tok = tokens[i - 1].lower()
                if any(keyword in prev_tok for keyword in gpu_indicators) or \
                   re.search(r"(gt|gtx|rtx)\d{3,4}", prev_tok):
                    is_gpu_vram = True
                    if logger:
                        logger.debug(f"RAM: Skipping GPU VRAM mislabeled as RAM: {prev_tok} {tokens[i]} {tokens[i+1]}")
            
            if not is_gpu_vram and i > 1:
                prev2_tok = tokens[i - 2].lower()
                prev1_tok = tokens[i - 1].lower()
                if prev2_tok == "geforce" and re.search(r"(gt|gtx|rtx)\d{3,4}", prev1_tok):
                    is_gpu_vram = True
                    if logger:
                        logger.debug(f"RAM: Skipping GPU VRAM mislabeled as RAM in GeForce pattern: {tokens[i-2]} {tokens[i-1]} {tokens[i]} {tokens[i+1]}")
            # New: consider forward GPU markers like 'GPU' after the size (e.g., '4GB GPU')
            if not is_gpu_vram and i + 1 < len(tokens):
                next_tok = tokens[i + 1].lower()
                if next_tok in {"gpu", "graphics", "video"}:
                    is_gpu_vram = True
                    if logger:
                        logger.debug(f"RAM: Skipping size treated as GPU VRAM due to forward GPU marker: {tokens[i]} {tokens[i+1]}")
                
            # Only skip if storage type immediately follows (indicating this is storage, not RAM)
            storage_follows = False
            if not is_gpu_vram and i + 2 < len(tokens):
                next_token = tokens[i + 2]
                if next_token.lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "drives", "hd", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"]:
                    storage_follows = True
                    if logger:
                        logger.debug(f"RAM: Skipping 'RAM [size]' pattern with storage type following: {tokens[i]} {tokens[i+1]} {next_token}")
            
            if not storage_follows and not is_gpu_vram:
                results.append([i, i + 1])
                consumed.add(i)
                consumed.add(i + 1)
                already_matched_indices.update([i, i + 1])
                if logger:
                    logger.debug(f"RAM: Found simple pattern: {tokens[i]} {tokens[i + 1]}")
                continue

def step_5_handle_standalone_sizes(tokens, consumed, already_matched_indices, results, storage_not_included, logger=None):
    """Handle standalone sizes with nearby RAM context (fallback for cases like standalone "16GB" near DDR context)"""
    for i, token in enumerate(tokens):
        if i in consumed or i in already_matched_indices:
            continue
            
        # Look for standalone size tokens
        if re.search(r'^\d+(gb|tb|mb)$', token, re.IGNORECASE):
            # Check for RAM context nearby (within 3 tokens in either direction)
            has_ram_context = False
            has_storage_context = False
            has_raid_context = False
            
            for j in range(max(0, i-3), min(len(tokens), i+4)):
                if j != i and j < len(tokens):
                    if tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                        has_ram_context = True
                    elif _token_is_storage(tokens[j].lower()):
                        has_storage_context = True
                    elif tokens[j].lower() in ["raid", "sas", "sata", "interface", "controller", "array"]:
                        has_raid_context = True
            
            # Check that this isn't immediately adjacent to storage or RAID indicators
            immediate_exclusion = False
            if ((i > 0 and (_token_is_storage(tokens[i-1].lower()) or tokens[i-1].lower() in ["raid", "sas", "sata"])) or
                (i + 1 < len(tokens) and (_token_is_storage(tokens[i+1].lower()) or tokens[i+1].lower() in ["raid", "sas", "sata", "interface", "controller"]))):
                immediate_exclusion = True
                if logger:
                    logger.debug(f"RAM: Skipping size with immediate storage/RAID context: {token}")
            
            # ENHANCED: If storage is explicitly not included, be more aggressive about capturing standalone sizes
            should_extract = False
            
            if storage_not_included and not immediate_exclusion and not has_raid_context:
                # When storage is explicitly not included, capture reasonable RAM sizes
                size_match = re.search(r'^(\d+)(gb|tb|mb)$', token.lower())
                if size_match:
                    size_value = int(size_match.group(1))
                    size_unit = size_match.group(2).lower()
                    
                    # Reasonable RAM sizes: 1GB-128GB, or any TB value
                    if ((size_unit == 'gb' and 1 <= size_value <= 128) or 
                        (size_unit == 'tb' and size_value <= 2) or
                        (size_unit == 'mb' and size_value >= 512)):  # 512MB+ for older systems
                        should_extract = True
                        if logger:
                            logger.debug(f"RAM: Storage not included - extracting reasonable RAM size: {token}")
            elif has_ram_context and not immediate_exclusion and not has_storage_context and not has_raid_context:
                should_extract = True
                if logger:
                    logger.debug(f"RAM: Found standalone size with RAM context: {token}")
            
            # Additional rule: skip if the size token appears to be GPU VRAM (e.g. immediately after a GPU model)
            if should_extract:
                gpu_indicators = [
                    "gpu", "graphics", "video", "vram",
                    "quadro", "gtx", "rtx", "gt", "geforce", "radeon", "rx", "firepro", "tesla", "m1000m",
                    "m2000m", "m3000m", "m4000m", "m5000m", "p1000", "p2000", "p400", "p600", "p620",
                    "p1000", "p2000", "p4000", "p5000", "rtx_a", "gt730", "gt710", "gt1030", "gt1050",  # NVIDIA GT series
                    "arc", "iris", "uhd", "hd"  # Intel GPUs
                ]

                is_gpu_vram = False
                
                # Check previous token for GPU indicators
                if i > 0:
                    prev_tok = tokens[i-1].lower()
                    # Simple heuristics: previous token contains a known GPU keyword or pattern
                    if any(keyword in prev_tok for keyword in gpu_indicators):
                        is_gpu_vram = True
                        if logger:
                            logger.debug(f"RAM: Detected GPU VRAM pattern - skipping size after GPU indicator: {prev_tok} {token}")
                    # Pattern like 'quadrom1000m' without space
                    elif re.search(r"(quadro|gtx|rtx|gt|geforce|radeon|tesla)[a-z0-9]*", prev_tok):
                        is_gpu_vram = True
                        if logger:
                            logger.debug(f"RAM: Detected GPU VRAM pattern - skipping size after GPU model: {prev_tok} {token}")
                    # Check for GPU model numbers like GT730, GTX1050, etc.
                    elif re.search(r"(gt|gtx|rtx)\d{3,4}", prev_tok):
                        is_gpu_vram = True
                        if logger:
                            logger.debug(f"RAM: Detected GPU VRAM pattern - skipping size after GPU model number: {prev_tok} {token}")
                
                # ENHANCED: Check previous 2 tokens for GeForce + GT/GTX/RTX patterns
                if not is_gpu_vram and i > 1:
                    prev2_tok = tokens[i-2].lower()
                    prev1_tok = tokens[i-1].lower()
                    if (prev2_tok == "geforce" and re.search(r"(gt|gtx|rtx)\d{3,4}", prev1_tok)) or \
                       (prev2_tok == "geforce" and prev1_tok in ["gt", "gtx", "rtx"]):
                        is_gpu_vram = True
                        if logger:
                            logger.debug(f"RAM: Detected GPU VRAM pattern - skipping size after GeForce GPU: {prev2_tok} {prev1_tok} {token}")
                # Forward check: size followed by 'GPU' should be VRAM
                if not is_gpu_vram and i + 1 < len(tokens):
                    next_tok = tokens[i+1].lower()
                    if next_tok in {"gpu", "graphics", "video"}:
                        is_gpu_vram = True
                        if logger:
                            logger.debug(f"RAM: Detected forward GPU marker - skipping size as system RAM: {token} {tokens[i+1]}")
                


                # If identified as GPU VRAM, skip treating as system RAM
                if is_gpu_vram:
                    continue

            if should_extract:
                results.append([i])
                consumed.add(i)
                already_matched_indices.add(i)
                continue

# Enhanced RAMExtractor with multi-instance capability
class EnhancedRAMExtractor(RAMExtractor):
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        self.logger = logger
        
    def _extract_server_ram_patterns(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract server RAM patterns with large capacities and module configurations."""
        results = []
        
        # Server RAM context indicators
        server_ram_indicators = [
            "server ram", "server memory", "ecc", "reg", "registered", "rdimm", "lrdimm", 
            "pc3", "pc4", "ddr3", "ddr4", "ddr5", "8500r", "10600r", "12800r", "14900r",
            "pc3l", "pc4l", "reg ecc", "registered ecc", "dimm", "so-dimm", "sodimm"
        ]
        
        # Check for server RAM context in the full text
        full_text = ' '.join(tokens).lower()
        has_server_context = any(indicator in full_text for indicator in server_ram_indicators)
        
        if not has_server_context:
            return results
        
        if self.logger:
            self.logger.debug(f"RAM: Server RAM context detected in: {full_text}")
        
        # Pattern 1: Large capacity + module configuration like "960GB (60 x 16GB)"
        for i in range(len(tokens)):
            if i in consumed:
                continue
                
            # Look for large capacity (typically 64GB+ for servers)
            capacity_match = re.search(r'^(\d+)(gb|tb)$', tokens[i].lower())
            if capacity_match:
                capacity_value = int(capacity_match.group(1))
                capacity_unit = capacity_match.group(2)
                
                # For server RAM, look for larger capacities (64GB+) or any TB values
                is_large_server_capacity = (
                    (capacity_unit == 'gb' and capacity_value >= 64) or
                    (capacity_unit == 'tb')
                )
                
                if is_large_server_capacity:
                    # Look for module configuration in next few tokens
                    config_found = False
                    config_indices = []
                    
                    for j in range(i+1, min(i+15, len(tokens))):
                        if j in consumed:
                            continue
                            
                        # Pattern: (60 x 16GB) or (60x16GB) or similar
                        module_config_pattern = re.search(r'^\((\d+)\s*x\s*(\d+)(gb|mb)\)$', tokens[j].lower())
                        if module_config_pattern:
                            module_count = int(module_config_pattern.group(1))
                            module_size = int(module_config_pattern.group(2))
                            module_unit = module_config_pattern.group(3)
                            
                            # Verify the math makes sense (total should match or be close)
                            if module_unit == 'gb':
                                calculated_total = module_count * module_size
                            else:  # mb
                                calculated_total = (module_count * module_size) // 1024
                            
                            # Allow some tolerance for mixed lots
                            if abs(calculated_total - capacity_value) <= capacity_value * 0.1:  # 10% tolerance
                                config_indices = [i, j]
                                config_found = True
                                if self.logger:
                                    self.logger.debug(f"RAM: Found server RAM pattern: {tokens[i]} {tokens[j]} (calculated: {calculated_total}GB)")
                                break
                    
                    if config_found:
                        results.append(config_indices)
                        for idx in config_indices:
                            consumed.add(idx)
                        continue
                    
                    # Even without explicit config, large capacities in server context are likely RAM
                    # But be more conservative - look for additional RAM indicators nearby
                    has_nearby_ram_indicators = False
                    for j in range(max(0, i-5), min(len(tokens), i+10)):
                        if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "dimm", "modules", "sticks"]:
                            has_nearby_ram_indicators = True
                            break
                    
                    # NEW: Detect storage context around the capacity to avoid false RAM extraction
                    storage_indicators = {"ssd", "ssds", "hdd", "hard", "drive", "drives", "disk", "nvme", "emmc", "sas", "sata", "scsi", "ssd/os", "m.2", "m2", "msata"}
                    has_nearby_storage_indicators = False
                    for j in range(max(0, i-3), min(len(tokens), i+4)):
                        if j == i or j in consumed:
                            continue
                        # Remove punctuation from comparison
                        token_clean = tokens[j].lower().rstrip(',.;:')
                        if token_clean in storage_indicators:
                            has_nearby_storage_indicators = True
                            break
                    
                    # Only treat as RAM if RAM indicators exist and no storage indicators nearby
                    if has_nearby_ram_indicators and not has_nearby_storage_indicators:
                        results.append([i])
                        consumed.add(i)
                        if self.logger:
                            self.logger.debug(f"RAM: Found large server RAM capacity: {tokens[i]}")
                        continue
        
        # Pattern 2: Module configuration without explicit total like "(8 x 16GB)"
        for i in range(len(tokens)):
            if i in consumed:
                continue
                
            # Look for module configuration patterns
            module_config_pattern = re.search(r'^\((\d+)\s*x\s*(\d+)(gb|mb)\)$', tokens[i].lower())
            if module_config_pattern:
                module_count = int(module_config_pattern.group(1))
                module_size = int(module_config_pattern.group(2))
                module_unit = module_config_pattern.group(3)
                
                # Calculate total capacity
                if module_unit == 'gb':
                    total_capacity = module_count * module_size
                else:  # mb
                    total_capacity = (module_count * module_size) // 1024
                
                # For server configurations, look for reasonable module counts and sizes
                is_reasonable_server_config = (
                    (module_count >= 4 and module_size >= 4) or  # At least 4 modules of 4GB+
                    (total_capacity >= 32)  # Or total of 32GB+
                )
                
                if is_reasonable_server_config:
                    results.append([i])
                    consumed.add(i)
                    if self.logger:
                        self.logger.debug(f"RAM: Found server RAM module config: {tokens[i]} (total: {total_capacity}GB)")
                    continue
        
        # Pattern 3: Mixed lot patterns with server indicators
        for i in range(len(tokens)):
            if i in consumed:
                continue
                
            # Look for "mixed lot" or similar patterns combined with RAM sizes
            if tokens[i].lower() in ["mixed", "lot", "qty"]:
                # Look for RAM sizes in nearby tokens
                for j in range(max(0, i-3), min(len(tokens), i+8)):
                    if j != i and j not in consumed:
                        size_match = re.search(r'^(\d+)(gb|mb)$', tokens[j].lower())
                        if size_match:
                            size_value = int(size_match.group(1))
                            size_unit = size_match.group(2)
                            
                            # For mixed lots, even smaller individual sizes can add up
                            if ((size_unit == 'gb' and size_value >= 8) or 
                                (size_unit == 'mb' and size_value >= 512)):
                                results.append([j])
                                consumed.add(j)
                                if self.logger:
                                    self.logger.debug(f"RAM: Found server RAM in mixed lot: {tokens[j]}")
                                break
        
        return results

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract RAM information by identifying complete RAM patterns in context."""
        
        # PRIORITY 0: Check for server RAM patterns first
        server_ram_results = self._extract_server_ram_patterns(tokens, consumed)
        if server_ram_results:
            return server_ram_results
        
        # Check if this looks like the complex format first
        full_text = ' '.join(tokens)
        if re.search(r'\d+gb.*?mhz.*?\(.*?x.*?\).*?node', full_text.lower()):
            return []
        
        # EARLY CHECK: Determine if storage is explicitly not included
        storage_not_included = False
        for i in range(len(tokens)):
            tok_lower = tokens[i].lower()
            # Pattern: 'no' followed by storage word within next 5 tokens
            if tok_lower in ["no", "none", "n/a", "without"]:
                for j in range(i+1, min(i+6, len(tokens))):
                    if tokens[j].lower() in ["ssd", "ssds", "hdd", "hdds", "emmc", "storage", "drive", "drives", "harddrive", "hard", "local", "locstorage", "hd", "os/ssd", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"]:
                        storage_not_included = True
                        if self.logger:
                            self.logger.debug(f"RAM: Storage explicitly not included - found pattern: {tokens[i]} ... {tokens[j]}")
                        break
            # Directionality chosen: we ignore patterns where the storage keyword
            # precedes the negation (e.g., 'SSD No') because sellers typically
            # write negation first.
            elif False:
                pass
            # NEW: Composite token like 'NoSSD' or 'No_SSD'
            elif tok_lower.startswith("no") and any(k in tok_lower for k in ["ssd", "ssds", "hdd", "hdds", "emmc", "storage", "drive", "drives", "harddrive", "hard", "hd", "locstorage", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"]):
                storage_not_included = True
                if self.logger:
                    self.logger.debug(f"RAM: Storage explicitly not included - composite token detected: {tokens[i]}")
        
        # Additional safety: detect full-text phrases like 'No SSD', 'No Storage'
        if not storage_not_included:
            full_text_lower = ' '.join(tokens).lower()
            if re.search(r"\bno\s+(ssd|ssds|hdd|hdds|storage|drive|drives|hard\s*drive|hd|locstorage|ssd/os|m\.2|m2|msata|sata|sas|scsi|disk|disks)\b", full_text_lower):
                storage_not_included = True
                if self.logger:
                    self.logger.debug("RAM: Storage explicitly not included - phrase detected in full title")

        if self.logger:
            self.logger.debug(f"RAM: Storage not included flag: {storage_not_included}")
        
        # SIMPLIFIED: Handle two separate capacity tokens (not slash-related)
        # Only applies when we have exactly 2 separate capacity tokens that aren't part of slash patterns
        separate_capacities = []
        for i, token in enumerate(tokens):
            if i not in consumed and re.search(r'^\d+(gb|tb|mb)$', token.lower(), re.IGNORECASE):
                # Check if this is part of a slash pattern by looking at adjacent tokens
                is_part_of_slash_pattern = False
                if ((i > 0 and tokens[i-1] == '/') or 
                    (i + 1 < len(tokens) and tokens[i+1] == '/')):
                    is_part_of_slash_pattern = True
                
                # Only consider for disambiguation if it's NOT part of a slash pattern
                if not is_part_of_slash_pattern:
                    capacity_match = re.search(r'^(\d+)(gb|tb|mb)$', token.lower())
                    if capacity_match:
                        capacity_value = int(capacity_match.group(1))
                        capacity_unit = capacity_match.group(2).lower()
                        capacity_mb = capacity_value * (1024 if capacity_unit == 'gb' else 1024*1024 if capacity_unit == 'tb' else 1)
                        
                        # Check if this capacity has clear storage context (adjacent storage type)
                        has_storage_type = False
                        if ((i + 1 < len(tokens) and tokens[i+1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "local", "locstorage", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"]) or
                            (i > 0 and tokens[i-1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "emmc", "storage", "drive", "local", "locstorage", "ssd/os", "m.2", "m2", "msata", "sata", "sas", "scsi", "disk", "disks"])):
                            has_storage_type = True
                        
                        separate_capacities.append({
                            'index': i,
                            'value': capacity_value,
                            'unit': capacity_unit,
                            'mb': capacity_mb,
                            'token': token,
                            'has_storage_type': has_storage_type
                        })
        
        # Apply disambiguation only for separate (non-slash) capacity tokens
        if len(separate_capacities) == 2:
            larger_cap = max(separate_capacities, key=lambda x: x['mb'])
            smaller_cap = min(separate_capacities, key=lambda x: x['mb'])
            
            # If larger capacity has clear storage context, smaller is likely RAM
            if larger_cap['has_storage_type'] and not smaller_cap['has_storage_type']:
                if self.logger:
                    self.logger.debug(f"RAM: Two separate capacities - larger: {larger_cap['token']} (storage context), smaller: {smaller_cap['token']} (RAM)")
                results = [[smaller_cap['index']]]
                consumed.add(smaller_cap['index'])
                return results
            else:
                if self.logger:
                    self.logger.debug(f"RAM: Two separate capacities without clear storage context - will process normally")
        
        results = []
        already_matched_indices = set()
        
        # Execute all steps - slash patterns will be handled properly by step 2
        step_0_handle_single_token_ram_format(tokens, consumed, already_matched_indices, results)
        step_0_5_handle_ampersand_patterns(tokens, consumed, already_matched_indices, results)
        step_1_handle_complete_ram_specifications(tokens, consumed, already_matched_indices, results)
        step_1_5_handle_size_plus_ddr_type(tokens, consumed, already_matched_indices, results)
        step_2_handle_dynamic_slash_patterns(tokens, consumed, already_matched_indices, results)
        step_3_handle_ram_ranges(tokens, consumed, already_matched_indices, results)
        step_4_handle_simple_patterns(tokens, consumed, already_matched_indices, results)
        step_5_handle_standalone_sizes(tokens, consumed, already_matched_indices, results, storage_not_included)
        
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        result = {}
        if not match_indices:
            return result
        
        configurations = []  # List to collect all configs
        for idx in match_indices:
            if idx < len(tokens):
                token = tokens[idx]
                # Look for broader module config patterns like XxYGB, even without parentheses
                config_match = re.search(r'^(\d+)x(\d+)(gb|tb|mb)$', token.lower())  # Removed parentheses requirement
                if config_match:
                    module_count = config_match.group(1)
                    module_size = config_match.group(2)
                    module_unit = config_match.group(3).upper()  # Capitalize unit
                    configurations.append(f"{module_count}x{module_size}{module_unit}")  # e.g., '2x2GB'
        
        if configurations:  # If configs found, aggregate them
            result["ram_config"] = ', '.join(configurations)  # e.g., '2x2GB, 2x8GB'
            # Add total size if available from context
            for idx in match_indices:
                if re.search(r'^(\d+)(gb|tb|mb)(?:[-]?ram|memory)?$', tokens[idx].lower()):
                    m=re.search(r'^(\d+)(gb|tb|mb)', tokens[idx], re.IGNORECASE)
                    if m:
                        result["ram_size"] = f"{m.group(1)}{m.group(2).upper()}"
                    else:
                        result["ram_size"] = tokens[idx].upper()
                    break
        
        # Fallback: handle simple patterns without explicit config (e.g., "16GB RAM")
        # If no configuration pattern was detected, attempt to capture a standalone RAM size token.
        if not result.get("ram_size"):
            for idx in match_indices:
                if re.search(r'^(\d+)(gb|tb|mb)(?:[-]?ram|memory)?$', tokens[idx].lower()):
                    m=re.search(r'^(\d+)(gb|tb|mb)', tokens[idx], re.IGNORECASE)
                    if m:
                        result["ram_size"] = f"{m.group(1)}{m.group(2).upper()}"
                    else:
                        result["ram_size"] = tokens[idx].upper()
                    break

        # NEW LOGIC: Handle digit-only tokens that rely on a nearby unit token (e.g., "8 / 16 GB RAM")
        # This is especially useful for slash-separated capacity lists where the unit appears once at the end.
        if not result.get("ram_size"):
            for idx in match_indices:
                token = tokens[idx]
                # Proceed only if the token is purely numeric (e.g., '8', '16').
                if token.isdigit():
                    unit_token = None
                    # Look forward up to three tokens for a unit (GB/TB/MB) OR a size token containing the unit (e.g., '16GB').
                    for offset in range(1, 4):
                        forward_index = idx + offset
                        if forward_index >= len(tokens):
                            break
                        f_tok = tokens[forward_index]
                        # Case A: token is just the unit ("GB")
                        m_unit_only = re.match(r'^(gb|tb|mb)$', f_tok, re.IGNORECASE)
                        if m_unit_only:
                            unit_token = m_unit_only.group(1).upper()
                            break
                        # Case B: token is another size with unit ("16GB") – extract the unit part
                        m_size_unit = re.match(r'^\d+(gb|tb|mb)$', f_tok, re.IGNORECASE)
                        if m_size_unit:
                            unit_token = m_size_unit.group(1).upper()
                            break
                    # If still not found ahead, look backward up to two tokens with the same two cases.
                    if not unit_token:
                        for offset in range(1, 3):
                            backward_index = idx - offset
                            if backward_index < 0:
                                break
                            b_tok = tokens[backward_index]
                            m_unit_only = re.match(r'^(gb|tb|mb)$', b_tok, re.IGNORECASE)
                            if m_unit_only:
                                unit_token = m_unit_only.group(1).upper()
                                break
                            m_size_unit = re.match(r'^\d+(gb|tb|mb)$', b_tok, re.IGNORECASE)
                            if m_size_unit:
                                unit_token = m_size_unit.group(1).upper()
                                break
                    # Once we have a unit, build the RAM size string.
                    if unit_token:
                        result["ram_size"] = f"{token}{unit_token}"
                        break

        return result
        
# Extractor for RAM configuration patterns
class EnhancedRAMConfigExtractor(RAMExtractor):
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        results = []
        
        # First, look for parenthesized configurations like "(1x8GB, 1x16GB)"
        for i in range(len(tokens)):
            if i in consumed:
                continue
            
            # Look for opening parenthesis
            if tokens[i] == '(' or tokens[i].startswith('('):
                config_end = i
                paren_depth = tokens[i].count('(') - tokens[i].count(')')

                # If the parentheses are balanced within this same token (e.g., "(1x16GB)"),
                # do NOT expand to include the next token. Otherwise, scan forward to find the matcher.
                if paren_depth > 0:
                    # Find the closing parenthesis across subsequent tokens
                    for j in range(i + 1, len(tokens)):
                        paren_depth += tokens[j].count('(') - tokens[j].count(')')
                        config_end = j
                        if paren_depth <= 0:
                            break
                
                # Extract content between parentheses and check for config patterns
                config_indices = list(range(i, config_end + 1))
                config_content = ' '.join(tokens[idx] for idx in config_indices)
                
                # Check if this contains RAM configuration patterns
                if re.search(r'\d+x\d+(gb|tb|mb)', config_content.lower()):
                    # Check for RAM context
                    has_ram_context = False
                    for j in range(max(0, i-5), min(len(tokens), config_end + 6)):
                        if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                            has_ram_context = True
                            break
                    
                    if has_ram_context:
                        results.append(config_indices)
                        for idx in config_indices:
                            consumed.add(idx)
                        continue
        
        # Then look for individual patterns like "2 x 32GB"
        for i in range(len(tokens) - 2):
            if i in consumed:
                continue
            if (tokens[i].isdigit() and 
                tokens[i+1].lower() == 'x' and
                re.search(r'^\d+(gb|tb|mb)$', tokens[i+2].lower(), re.IGNORECASE)):
                has_ram_context = False
                for j in range(max(0, i-5), min(len(tokens), i+7)):
                    if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5", "config", "configuration"]:
                        has_ram_context = True
                        break
                if has_ram_context:
                    match_indices = [i, i+1, i+2]
                    results.append(match_indices)
                    for idx in match_indices:
                        consumed.add(idx)
        
        # Finally look for single-token patterns like "1x8GB"
        for i, token in enumerate(tokens):
            if i in consumed:
                continue
            if re.match(r"\d+x\d+(gb|tb|mb)", token.lower(), re.IGNORECASE):
                has_ram_context = False
                for j in range(max(0, i-3), min(len(tokens), i+4)):
                    if j < len(tokens) and tokens[j].lower() in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]:
                        has_ram_context = True
                        break
                if has_ram_context:
                    results.append([i])
                    consumed.add(i)
        
        return results
        
    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process RAM configuration matches to extract module count and size."""
        result = {}
        
        if not match_indices:
            return result
        
        # Handle parenthesized configurations with 2 tokens like ['(1x8GB,', '1x16GB)']
        if len(match_indices) == 2:
            # Extract the content and clean it up
            config_content = ' '.join(tokens[idx] for idx in match_indices)
            
            # Remove parentheses and clean up spacing
            config_content = re.sub(r'[(),]', '', config_content)
            config_content = re.sub(r'\s+', ' ', config_content)
            
            # Split by spaces and rejoin with commas
            parts = config_content.strip().split()
            if len(parts) > 1:
                result["ram_config"] = ', '.join(parts)
            else:
                result["ram_config"] = config_content.strip()
            return result
        
        # If we have multiple indices (likely parenthesized content)
        if len(match_indices) > 3:
            # Extract the content and clean it up
            config_content = ' '.join(tokens[idx] for idx in match_indices)
            
            # Remove parentheses and clean up spacing
            config_content = re.sub(r'[()]', '', config_content)
            config_content = re.sub(r'\s*,\s*', ', ', config_content)
            config_content = re.sub(r'\s+', ' ', config_content)
            
            result["ram_config"] = config_content.strip()
            return result
        
        # Handle the existing cases for simpler patterns
        if len(match_indices) == 3:
            module_count = tokens[match_indices[0]]
            module_size = tokens[match_indices[2]]
            result["ram_modules"] = module_count
            result["ram_module_size"] = module_size
            result["ram_config"] = f"{module_count}x{module_size}"
        elif len(match_indices) == 1:
            token = tokens[match_indices[0]]
            # Remove any leading or trailing parentheses
            clean_token = re.sub(r'^\(+', '', token)
            clean_token = re.sub(r'\)+$', '', clean_token)
            match = re.match(r"(\d+)x(\d+(gb|tb|mb))", clean_token.lower(), re.IGNORECASE)
            if match:
                module_count = match.group(1)
                module_size = match.group(2).upper()
                result["ram_config"] = clean_token
                if int(module_count) == 1:
                    result["ram_size"] = module_size
        
        return result

# New extractor for RAM range patterns
class RAMRangeExtractor(RAMExtractor):
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        results = []
        for i in range(len(tokens)):
            if i in consumed:
                continue
            # Single token like "16GB-32GB" (FIXED PATTERN)
            if re.match(r"\d+(gb|tb|mb)-\d+(gb|tb|mb)", tokens[i], re.IGNORECASE):
                if i + 1 < len(tokens) and tokens[i + 1].lower() in ["ram", "memory"] and i + 1 not in consumed:
                    results.append([i, i + 1])
            # Single token like "4-16GB" (original pattern)
            elif re.match(r"\d+-\d+(gb|tb|mb)", tokens[i], re.IGNORECASE):
                if i + 1 < len(tokens) and tokens[i + 1].lower() in ["ram", "memory"] and i + 1 not in consumed:
                    results.append([i, i + 1])
            # Three-token sequence like "4GB - 16GB"
            elif (i + 3 < len(tokens) and
                  re.match(r"\d+(gb|tb|mb)", tokens[i], re.IGNORECASE) and
                  tokens[i + 1] == "-" and
                  re.match(r"\d+(gb|tb|mb)", tokens[i + 2], re.IGNORECASE) and
                  tokens[i + 3].lower() in ["ram", "memory"] and
                  all(j not in consumed for j in [i, i + 1, i + 2, i + 3])):
                results.append([i, i + 1, i + 2, i + 3])
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        if len(match_indices) == 2:
            range_token = tokens[match_indices[0]]
            # Handle both "16GB-32GB" and "4-16GB" patterns
            if re.match(r"\d+(gb|tb|mb)-\d+(gb|tb|mb)", range_token, re.IGNORECASE):
                # Pattern like "16GB-32GB"
                parts = range_token.split("-")
                first = parts[0]
                second = parts[1]
                range_str = f"{first}-{second}"
            elif re.match(r"\d+-\d+(gb|tb|mb)", range_token, re.IGNORECASE):
                # Pattern like "4-16GB"
                parts = range_token.split("-")
                first = parts[0]
                second = parts[1]
                unit_match = re.search(r"(gb|tb|mb)", second, re.IGNORECASE)
                if unit_match:
                    unit = unit_match.group(1).upper()
                    if not re.search(r"(gb|tb|mb)", first, re.IGNORECASE):
                        first += unit
                range_str = f"{first}-{second}"
            else:
                range_str = range_token
        elif len(match_indices) == 4:
            first = tokens[match_indices[0]]
            second = tokens[match_indices[2]]
            range_str = f"{first}-{second}"
        else:
            return {}
        return {"ram_range": range_str}
        
# Configuration for RAM extractors
extractor_config = [
    {
        "name": "ram_range",
        "patterns": [],  # Custom logic in extract method
        "multiple": False,
        "class": RAMRangeExtractor,
    },
    {
        "name": "ram_size",
        "patterns": [
            [regex_pat(r"\b\d+gb(ram|memory)\b", show=True)],  # Added to handle "32GBRAM" format
            [regex_pat(r"\b[0-9]+(gb|tb|mb)\b", show=True),
             list_pat(["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"], optional=False, show=False)],
            [list_pat(["ram", "memory"], optional=False, show=False),
             regex_pat(r"\b[0-9]+(gb|tb|mb)\b", show=True)],
            [regex_pat(r"\b[0-9]+(gb|tb|mb)(ram|memory|ddr)\b", show=True)],
            [regex_pat(r"\b(ram|memory|ddr)[0-9]+(gb|tb|mb)\b", show=True)],
            [regex_pat(r"\b[0-9]+(gb|tb|mb)\b", show=True)],
            [regex_pat(r"\b[0-9]+x[0-9]+(gb|tb|mb)\b", show=True)],
            [regex_pat(r"\b[0-9]+(mb|gb|tb)\s*-\s*[0-9]+\\1\b", show=True),
             list_pat(["ram", "memory"], optional=False, show=False)],
            [regex_pat(r"\b[0-9]+\s*pc\s*[=-]\s*[0-9]+\s*(mb|gb|tb)\b", show=True)],
            [regex_pat(r"lot\s*of\s*\(?[0-9]+\s*(mb|gb|tb)\)?", show=True)]
        ],
        "multiple": False,
        "class": EnhancedRAMExtractor,
        "output_options": {
            "include_unit": True
        }
    },
    {
        "name": "ram_config",
        "patterns": [
            [regex_pat(r"\d+\s*x\s*\d+(gb|tb|mb)", show=True)]
        ],
        "multiple": False,
        "class": EnhancedRAMConfigExtractor,
    },
    {
        "name": "ram_type",
        "patterns": [
            [regex_pat(r"(ddr|lpddr|gddr|hbm)[0-5][x]?", show=True)],
            [list_pat(["ecc", "reg", "udimm", "rdimm", "lrdimm", "sodimm", "dimm", "sdram"], show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_speed_grade",
        "patterns": [
            [regex_pat(r"[0-9]+\s*mhz", show=True)],
            [regex_pat(r"PC-?\d{2,4}(?:[A-Z])?", show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_modules",
        "patterns": [
            [regex_pat(r"\(\d+\s*x\s*\d+\s*[mgt]b\)", show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_rank",
        "patterns": [
            [regex_pat(r"\d+Rx\d+", show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_brand",
        "patterns": [
            [list_pat(["samsung", "kingston", "hynix", "micron", "crucial", "corsair", "g.skill"], show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_ecc",
        "patterns": [
            [str_pat("ecc", show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_registered",
        "patterns": [
            [list_pat(["reg", "rdimm"], show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_unbuffered",
        "patterns": [
            [list_pat(["unbuffered", "udimm"], show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    },
    {
        "name": "ram_details",
        "patterns": [
            [regex_pat(r"\(\d+\s*x\s*\d+\s*[mgt]b(?:\s*&\s*\d+\s*x\s*\d+\s*[mgt]b)*\)", show=True)]
        ],
        "multiple": False,
        "class": RAMExtractor,
    }
]