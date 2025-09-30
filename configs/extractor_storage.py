from configs.parser import StorageExtractor
from typing import Dict, List, Set, Any, Optional
import re

def str_pat(value, optional=False, show=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

class EnhancedStorageExtractor(StorageExtractor):
    """Enhanced storage extractor with capitalization for storage terms."""
    KNOWN_STORAGE_TYPES = {'ssd', 'hdd', 'nvme', 'emmc', 'storage', 'local', 'locstorage'}

    def __init__(self, config: Dict[str, Any], logger=None):
        """Initialize the extractor with a set to track processed capacities."""
        super().__init__(config, logger)
        self.processed_capacities = set()
        self.found_clear_patterns = False  # Track if we found unambiguous storage patterns
        # Flag that is set to True once a "No storage" pattern (e.g. "No SSD")
        # has been detected. When this flag is True, the extractor will skip any
        # further capacity extraction attempts for the current text to ensure we
        # don't incorrectly capture RAM capacities as storage.
        self.storage_excluded: bool = False

    def capitalize_units(self, s: str) -> str:
        """Capitalize storage units (e.g., 'gb' to 'GB') in a string."""
        return re.sub(r'\b(\d+)(gb|tb|mb)\b', lambda m: m.group(1) + m.group(2).upper(), s, flags=re.IGNORECASE)

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract storage-related matches from tokenized text."""
        return enhanced_storage_extract(self, tokens, consumed)

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process matched tokens into a structured dictionary with capitalized terms."""
        return enhanced_storage_process_match(self, tokens, match_indices)

def _title_indicates_no_storage_via_power_cord_group(tokens: List[str]) -> bool:
    """Post-process helper: detect titles like 'No Power Cord/HardDrive/SSD'.

    Intent: Without touching any existing patterns, recognize when a "No Power"
    group explicitly lists storage (HDD/SSD) as not included via a slash group,
    so any lone "GB" capacity should not be treated as storage.
    """
    try:
        full_text = " ".join(tokens).lower()
        # Match either spaced or compact 'no power cord/adapter' then '/hdd|ssd|hard drive'.
        # Allow small annotations like '(a)' after the storage term.
        pattern = re.compile(
            r"\bno\s*(?:power\s*(?:cord|adapter)|powercord|poweradapter)\b[^,;:()]{0,120}(?:/|\|)\s*(?:hard\s*drive|hdd|ssd)(?![a-z])",
            re.IGNORECASE,
        )
        return bool(pattern.search(full_text))
    except Exception:
        return False

# New explicit pattern extractor: detect titles like "32GB RAM 512GB" and emit the
# trailing capacity as storage without touching existing regex patterns.
def extract_ram_then_storage_from_context(tokens: List[str]) -> List[List[int]]:
    try:
        results: List[List[int]] = []
        n = len(tokens)
        for i in range(n):
            tok_i = tokens[i]
            # First capacity (likely RAM)
            if not re.match(r'^\d+(?:\.\d+)?(gb|tb)$', tok_i, re.IGNORECASE):
                continue
            # Look ahead for RAM/MEMORY token within a short window
            ram_idx = -1
            for k in range(i + 1, min(n, i + 6)):
                if re.match(r'(?i)\bram\b|\bmemory\b', tokens[k]):
                    ram_idx = k
                    break
            if ram_idx == -1:
                continue
            # After RAM token, look ahead for a larger (or clearly storage) capacity
            for j in range(ram_idx + 1, min(n, ram_idx + 8)):
                tok_j = tokens[j]
                m2 = re.match(r'^(\d+(?:\.\d+)?)(gb|tb)$', tok_j, re.IGNORECASE)
                if not m2:
                    continue
                val2 = float(m2.group(1))
                unit2 = m2.group(2).upper()
                # Only treat as storage when clearly large (>=256GB) or any TB
                if (unit2 == 'GB' and val2 >= 256) or (unit2 == 'TB'):
                    # Emit a single-index match for the storage capacity token
                    results.append([j])
                    break
        return results
    except Exception:
        return []

# ADDITIONAL POST-PROCESS HELPER (non-invasive): detect compact patterns like
# "NoPowerCord/HardDrive/SSD(a)" where spaces are omitted. This helper does not
# modify any existing logic; it merely provides another signal for post-process.
def _title_indicates_no_storage_compact_powercord_group(tokens: List[str]) -> bool:
    try:
        full_text = " ".join(tokens).lower()
        # Compact variant: 'NoPowerCord' (no spaces) followed by '/hdd|ssd|hard drive'.
        # Accept small annotations after the storage term.
        pattern = re.compile(
            r"\bnopower(?:cord|adapter)\b[^,;:()]{0,120}(?:/|\|)\s*(?:hard\s*drive|hdd|ssd)(?![a-z])",
            re.IGNORECASE,
        )
        return bool(pattern.search(full_text))
    except Exception:
        return False

def extract_standalone_storage_sizes_with_clear_pattern_awareness(self, tokens: List[str], consumed: Set[int], context: Dict, markers: Dict, results: List[List[int]]) -> List[List[int]]:
    """Extract standalone storage sizes with awareness of whether clear patterns were already found."""
    standalone_results = []
    
    for i in range(len(tokens)):
        if i in consumed:
            continue
            
        # Match any number + storage unit pattern but exclude range patterns
        if (re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
            not re.match(r'^\d+(?:\.\d+)?-\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE)):
            
            if hasattr(self, 'logger') and self.logger:
                self.logger.debug(f"Storage: Evaluating standalone storage token {i}: {tokens[i]}")
            
            # Priority checks for immediate RAM/GPU/RAID context
            is_immediate_ram = (i + 1 < len(tokens) and is_ram_context_token(tokens[i + 1]))
            is_immediate_gpu = False
            is_immediate_raid = (i + 1 < len(tokens) and is_raid_or_network_context_token(tokens[i + 1]))
            
            # Check for GPU model patterns
            if (i >= 2 and
                is_gpu_context_token(tokens[i-2]) and
                re.match(r'^\d{3,4}[a-z]*$', tokens[i-1], re.IGNORECASE)):
                is_immediate_gpu = True
            elif (i > 0 and is_gpu_context_token(tokens[i-1])):
                is_immediate_gpu = True
            elif (i > 0 and 
                  re.search(r'(gtx|rtx)\s*\d+', tokens[i-1].lower(), re.IGNORECASE)):
                is_immediate_gpu = True
            
            if is_immediate_ram:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Skipping {tokens[i]} - immediately followed by RAM: {tokens[i+1]}")
                continue
                
            if is_immediate_gpu:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Skipping {tokens[i]} - GPU context detected")
                continue
                
            if is_immediate_raid:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Skipping {tokens[i]} - RAID/network context detected: {tokens[i+1]}")
                continue
            
            # Context-based extraction logic
            has_storage_context = any(j in markers['storage_markers'] for j in range(max(0, i-2), min(len(tokens), i+3)))
            has_ram_context = any(j in markers['ram_markers'] for j in range(max(0, i-2), min(len(tokens), i+3)))
            
            should_extract = False
            
            # ENHANCED LOGIC: Be more conservative if we already found clear patterns
            if hasattr(self, 'found_clear_patterns') and self.found_clear_patterns:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Clear patterns already found - applying stricter criteria for {tokens[i]}")
                # Only extract if there's explicit storage context or phone context
                if context['is_phone_context'] or context['device_type'] in ['Tablets & eReaders', 'Cell Phones & Smartphones']:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Phone/tablet context - extracting {tokens[i]}")
                elif has_storage_context:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Explicit storage context detected - extracting {tokens[i]}")
                else:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} - no explicit storage context and clear patterns already found")
            else:
                # Original logic when no clear patterns found yet
                if context['is_phone_context'] or context['device_type'] in ['Tablets & eReaders', 'Cell Phones & Smartphones']:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Phone/tablet context - extracting {tokens[i]}")
                elif has_storage_context:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Storage context detected - extracting {tokens[i]}")
                elif not has_ram_context:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: No RAM context - extracting {tokens[i]}")
                else:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} due to RAM context in computer device")
            
            if should_extract:
                standalone_results.append([i])
                consumed.add(i)
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Found standalone storage size at {i}: {tokens[i]}")
    
    return standalone_results
    
def setup_storage_context(self, tokens: List[str]) -> Dict:
    """Setup context and helper functions for storage extraction."""
    device_context = getattr(self, 'device_context', {})
    is_phone_context = device_context.get('is_phone_device', False) or device_context.get('has_phone_context', False)
    device_type = device_context.get('device_type', '')
    
    if hasattr(self, 'logger') and self.logger:
        self.logger.debug(f"Storage extraction context: phone={is_phone_context}, device_type={device_type}")
    
    return {
        'is_phone_context': is_phone_context,
        'device_type': device_type
    }
    
def clean_token_for_context(token):
    """Remove punctuation and normalize token for context checking."""
    return re.sub(r'[^\w]', '', token.lower())

def is_ram_context_token(token):
    """Check if a token (cleaned of punctuation) indicates RAM context."""
    cleaned = clean_token_for_context(token)
    return cleaned in ["ram", "memory", "ddr", "ddr2", "ddr3", "ddr4", "ddr5"]

def is_gpu_context_token(token):
    """Check if a token indicates GPU context."""
    cleaned = clean_token_for_context(token)
    gpu_terms = [
        "gtx", "rtx", "radeon", "rx", "vega", "fury", "titan", "quadro", "tesla", 
        "geforce", "nvidia", "amd", "ati", "graphics", "gpu", "video", "1050", "1060", "1070", "1080", 
        "1650", "1660", "2060", "2070", "2080", "3060", "3070", "3080", "3090", "4060", 
        "4070", "4080", "4090", "580", "590", "5700", "6600", "6700", "6800", "6900", "7600", "7700", "7800", "7900"
    ]
    return any(gpu_term in cleaned for gpu_term in gpu_terms)

def check_storage_explicitly_not_included(tokens: List[str], consumed: Set[int]) -> Optional[List[List[int]]]:
    """Check if storage is explicitly not included and return 'No' patterns if found."""
    storage_not_included = False
    results = []
    
    # Check for patterns like "no SSD", "without HDD", etc.
    for i in range(len(tokens)):
        if i in consumed:
            continue
            
        if tokens[i].lower() in ["no", "none", "n/a", "without"]:
            for j in range(i+1, min(i+6, len(tokens))):
                if j in consumed:
                    continue
                if tokens[j].lower() in ["ssd", "ssds", "hdd", "hdds", "storage", "drive", "drives", "harddrive", "hard", "local", "locstorage", "hd", "os/ssd"]:
                    storage_not_included = True
                    # CRITICAL: Consume both tokens immediately to prevent HDDs from becoming a storage marker
                    results.append([i, j])
                    consumed.add(i)
                    consumed.add(j)
                    print(f"Storage explicitly not included - found pattern: {tokens[i]} ... {tokens[j]} (consumed both tokens)")
                    break
        # NEW PATTERN: Reversed order in table-like text -> "SSD No" / "HDD None"
        # This occurs when a field label is followed by a value. Treat it as "Not Included".
        if tokens[i].lower() in [
            "ssd", "ssds", "hdd", "hdds", "storage", "drive", "drives",
            "harddrive", "hard", "local", "locstorage", "hd", "os/ssd"
        ]:
            for j in range(i + 1, min(i + 4, len(tokens))):
                if tokens[j].lower() in ["no", "none", "n/a", "without"] and j not in consumed:
                    results.append([i, j])
                    consumed.add(i)
                    consumed.add(j)
                    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                        print(f"Storage explicitly not included - reversed order: {tokens[i]} {tokens[j]} (consumed both tokens)")
                    storage_not_included = True
                    break
        # NEW PATTERN: Capacity token BEFORE the "no" keyword -> "8GB No SSD"
        # We scan for a numeric capacity with unit, followed within 1..3 tokens by "no/none/n/a"
        # followed within next 1..3 tokens by a storage-type word.
        if re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE):
            # lookahead window up to 3 tokens for "no" and another 3 for storage type
            cap_idx = i
            # first find the "no" token within 1..3 positions after cap_idx
            for j in range(cap_idx + 1, min(cap_idx + 4, len(tokens))):
                if tokens[j].lower() in ["no", "none", "n/a", "without"]:
                    # after finding "no", search for storage indicator within next 1..3 tokens
                    for k in range(j + 1, min(j + 4, len(tokens))):
                        if tokens[k].lower() in [
                            "ssd", "ssds", "hdd", "hdds", "storage", "drive", "drives", "harddrive", "hard", "local", "locstorage", "hd", "os/ssd"
                        ]:
                            # Found "capacity no storage" pattern
                            storage_not_included = True
                            # only consume the negation word and storage keyword – leave
                            # the capacity token untouched so the RAM extractor can still
                            # consider it.
                            pattern_indices = [j, k]
                            for idx in pattern_indices:
                                if idx not in consumed:
                                    consumed.add(idx)
                            results.append(pattern_indices)
                            print(
                                f"Storage explicitly not included - found pattern: {tokens[cap_idx]} ... {tokens[j]} ... {tokens[k]} (consumed negation + storage keyword only)"
                            )
                            break
                    break
    
    # Return results regardless of storage_not_included flag - we want to extract the "No" patterns
    return results if results else None
    
def extract_slash_separated_patterns(tokens: List[str], consumed: Set[int], context: Dict) -> List[List[int]]:
    """Extract slash-separated patterns like '8/16/32/64/128/256GB SSD' or '128GB SSD/1TB HDD'."""
    results = []
    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
        print("Scanning for slash-separated patterns first (highest priority)")
    
    # Check for dual storage configuration with different types (e.g., "128GB SSD/1TB HDD")
    for i in range(len(tokens) - 4):
        if i in consumed or i+1 in consumed or i+2 in consumed or i+3 in consumed:
            continue
            
        # Pattern: <SIZE1><UNIT> <TYPE1> / <SIZE2><UNIT> <TYPE2>
        # e.g.: "128GB SSD / 1TB HDD"
        if (re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
            tokens[i+1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"] and
            tokens[i+2] == '/' and
            re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i+3], re.IGNORECASE) and
            i+4 < len(tokens) and 
            tokens[i+4].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]):
            
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Found dual storage configuration: {tokens[i]} {tokens[i+1]} / {tokens[i+3]} {tokens[i+4]}")
            sequence_indices = [i, i+1, i+2, i+3, i+4]
            results.append(sequence_indices)
            for idx in sequence_indices:
                consumed.add(idx)
            return results
            
        # Pattern: <SIZE1><UNIT><TYPE1>/<SIZE2><UNIT><TYPE2>
        # e.g.: "128GBSSD/1TBHDD"
        capacity1_type1_pattern = re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[i], re.IGNORECASE)
        if (capacity1_type1_pattern and
            tokens[i+1] == '/' and
            re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[i+2], re.IGNORECASE)):
            
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Found compact dual storage configuration: {tokens[i]} / {tokens[i+2]}")
            sequence_indices = [i, i+1, i+2]
            results.append(sequence_indices)
            for idx in sequence_indices:
                consumed.add(idx)
            return results
            
    # First, try to find the longest continuous sequence of numbers separated by slashes
    # This helps with cases like 16/32/64/128GB where we want to capture the whole pattern
    longest_sequence = None
    longest_length = 0
    
    i = 0
    while i < len(tokens):
        
        # Check if this token is a number (standalone or with unit)
        if tokens[i].isdigit() or re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE):
            # Try to build a sequence from this position
            sequence_indices = [i]
            j = i + 1
            
            # Continue as long as we see the pattern "/ number"
            while j + 1 < len(tokens) and tokens[j] == '/':
                next_token = tokens[j + 1]
                if next_token.isdigit() or re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', next_token, re.IGNORECASE):
                    sequence_indices.extend([j, j + 1])  # Add both slash and number
                    j += 2  # Move past this pair
                else:
                    break
            
            # Check if this is a valid pattern (at least one number, one slash, and one more number)
            if len(sequence_indices) >= 3:
                # Check if we need to add a unit token that follows
                if j < len(tokens) and re.match(r'^(gb|tb|mb)$', tokens[j], re.IGNORECASE):
                    sequence_indices.append(j)
                
                # Keep track of the longest sequence
                if len(sequence_indices) > longest_length:
                    longest_sequence = sequence_indices.copy()
                    longest_length = len(sequence_indices)
        
        i += 1
    
    # If we found a multi-part sequence, process it
    if longest_sequence and len(longest_sequence) >= 3:
        # RAM guard: if the sequence is immediately followed by RAM/MEMORY (even if those
        # tokens were previously consumed by the RAM extractor), treat this as RAM and skip.
        end_idx = longest_sequence[-1]
        follow_idx = end_idx + 1
        if follow_idx < len(tokens):
            next_tok_clean = re.sub(r"[^a-z]", "", tokens[follow_idx]).lower()
            if next_tok_clean in {"ram", "memory"}:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print("Skipping slash-separated capacity sequence due to immediate RAM context after sequence")
                return results  # return current (possibly empty) results without adding this sequence

        # As an additional safeguard, scan a short window ahead for RAM context
        window_end = min(len(tokens), end_idx + 4)
        for j in range(end_idx + 1, window_end):
            tok_clean = re.sub(r"[^a-z]", "", tokens[j]).lower()
            if tok_clean in {"ram", "memory"}:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print("Skipping slash-separated capacity sequence due to nearby RAM context after sequence")
                return results

        # Treat as storage pattern if it contains an explicit storage type
        # OR if a capacity unit (GB/TB/MB) is present in the sequence.
        has_storage_type = any(
            (idx < len(tokens) and tokens[idx].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"])
            for idx in longest_sequence
        )
        has_capacity_unit = any(
            (idx < len(tokens) and (re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[idx], re.IGNORECASE) or re.match(r'^(gb|tb|mb)$', tokens[idx], re.IGNORECASE)))
            for idx in longest_sequence
        )
        if not (has_storage_type or has_capacity_unit):
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print("Skipping slash-separated capacity sequence without explicit storage type or unit")
            return results

        # Treat as storage pattern
        results.append(longest_sequence)
        for idx in longest_sequence:
            consumed.add(idx)
        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
            print(f"Found complete slash-separated pattern: {[tokens[k] for k in longest_sequence]}")
        return results
    
    # If no multi-part sequence found, fall back to original logic
    i = 0
    while i < len(tokens):
        if i in consumed:
            i += 1
            continue
            
        # Look for patterns starting with: number (with or without unit) followed by slash
        if (i + 1 < len(tokens) and
            (tokens[i].isdigit() or re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE)) and
            tokens[i+1] == '/'):
            
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Starting slash-separated sequence detection at token {i}: {tokens[i]}")
            
            sequence_indices = [i]  # Start with the first number
            j = i + 1
            
            # Continue until we can't find more slash patterns
            while j < len(tokens):
                current_token = tokens[j]
                
                # If we see a slash, add it and look for the next number
                if current_token == '/':
                    sequence_indices.append(j)
                    j += 1
                    
                    # The next token should be a number (with or without unit)
                    if j < len(tokens):
                        next_token = tokens[j]
                        sequence_indices.append(j)
                        j += 1
                        
                        # CRITICAL: Check if there's ANOTHER slash after this token
                        # If so, continue the pattern regardless of whether current token has unit
                        if (j < len(tokens) and tokens[j] == '/'):
                            continue  # Keep going - more numbers follow
                        else:
                            # No more slashes - CHECK FOR STORAGE TYPE IMMEDIATELY AFTER
                            if (j < len(tokens) and 
                                tokens[j].lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]):
                                sequence_indices.append(j)
                                print(f"Added storage type to end of sequence: {tokens[j]}")
                            break
                    else:
                        break
                        
                # If we see a storage type after the sequence, add it
                elif (current_token.lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"] and
                      len(sequence_indices) >= 3):
                    sequence_indices.append(j)
                    print(f"Added storage type to sequence: {current_token}")
                    break
                    
                else:
                    # End of pattern
                    break
            
            # Process the sequence if valid
            if len(sequence_indices) >= 3:
                # NEW SAFEGUARD: Detect and skip CPU generation sequences (e.g., "2/3/4th Gen.")
                next_idx = sequence_indices[-1] + 1
                if next_idx < len(tokens):
                    # Strip punctuation to standardize the comparison (e.g., 'Gen.' -> 'gen')
                    next_tok_clean = re.sub(r'[^a-zA-Z]', '', tokens[next_idx]).lower()
                    if next_tok_clean in ["gen", "generation", "generations"]:
                        print(f"Skipping slash-separated sequence {sequence_indices} – appears to be CPU generation info near token: {tokens[next_idx]}")
                        i = sequence_indices[-1] + 1
                        continue
                
                # PRIORITY CHECK: Does sequence contain explicit storage indicator?
                has_explicit_storage_in_sequence = False
                for idx in sequence_indices:
                    if idx < len(tokens) and tokens[idx].lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                        has_explicit_storage_in_sequence = True
                        break
                
                # If sequence contains explicit storage indicator, process it immediately
                if has_explicit_storage_in_sequence:
                    results.append(sequence_indices)
                    for idx in sequence_indices:
                        consumed.add(idx)
                    print(f"Found complete slash-separated storage pattern: {[tokens[k] for k in sequence_indices]}")
                    i = sequence_indices[-1] + 1
                    continue
                
                # For sequences without explicit storage indicators, apply context rules
                is_followed_by_ram = is_sequence_followed_by_ram(tokens, i, sequence_indices[-1], is_ram_context_token, print)
                
                if not is_followed_by_ram:
                    # Additional context checks only for ambiguous cases
                    skip_due_to_context = False
                    if not context.get('is_phone_context', False):
                        search_start = max(0, i-2)
                        search_end = min(len(tokens), sequence_indices[-1]+3)
                        
                        has_ram_context = any(j in range(search_start, search_end) and j < len(tokens) and 
                                            is_ram_context_token(tokens[j]) for j in range(search_start, search_end))
                        
                        if has_ram_context:
                            skip_due_to_context = True
                            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                                print(f"Skipping ambiguous slash-separated sequence due to RAM context")
                    
                    if not skip_due_to_context:
                        results.append(sequence_indices)
                        for idx in sequence_indices:
                            consumed.add(idx)
                        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                            print(f"Found slash-separated storage pattern at {sequence_indices}: {' '.join(tokens[k] for k in sequence_indices)}")
                        i = sequence_indices[-1] + 1
                        continue
                else:
                    print(f"Skipping slash-separated sequence - followed by RAM: {' '.join(tokens[k] for k in sequence_indices)}")
        
        i += 1
    
    return results
    
def extract_range_patterns(tokens: List[str], consumed: Set[int]) -> List[List[int]]:
    """Extract range patterns like '250GB-1TB' and '250-500GB'."""
    results = []
    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
        print("Scanning for range patterns (e.g., '250GB-1TB' and '250-500GB')")
    
    for i, token in enumerate(tokens):
        if i in consumed:
            print(f"Skipping token {i}: {token} (already consumed)")
            continue
        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
            print(f"Checking token {i}: {token} for range pattern")
        
        # Pattern 1: Both numbers have units (like "250GB-500GB")
        range_match = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)-(\d+(?:\.\d+)?)(gb|tb|mb)$', token, re.IGNORECASE)
        if range_match:
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Found range match (both units) in token {i}: {token}")
            sequence = [i]
            if i + 1 < len(tokens) and tokens[i + 1].lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage", "hd"]:
                sequence.append(i + 1)
                consumed.add(i + 1)
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Added storage type token {i+1}: {tokens[i+1]}")
            results.append(sequence)
            consumed.add(i)
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Consumed token {i} for range (both units)")
            continue
        
        # Pattern 2: Only second number has unit (like "250-500GB")
        range_match2 = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)(gb|tb|mb)$', token, re.IGNORECASE)
        if range_match2:
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Found range match (second unit only) in token {i}: {token}")
            sequence = [i]
            if i + 1 < len(tokens) and tokens[i + 1].lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage", "hd"]:
                sequence.append(i + 1)
                consumed.add(i + 1)
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Added storage type token {i+1}: {tokens[i+1]}")
            results.append(sequence)
            consumed.add(i)
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Consumed token {i} for range (second unit only)")
            continue
    
    return results

def identify_context_markers(tokens: List[str], consumed: Set[int]) -> Dict[str, List[int]]:
    """Identify RAM, storage, and GPU context markers in tokens that are NOT consumed."""
    # IMPORTANT: RAM markers should be detected regardless of consumption so that
    # downstream ambiguity checks (e.g., deciding whether a slash-separated
    # capacity sequence is RAM vs Storage) still see nearby "RAM" tokens even if
    # the RAM extractor has already consumed them.
    ram_markers = [i for i, t in enumerate(tokens) if is_ram_context_token(t)]
    
    # FIXED: Only mark as storage markers if not part of a "No" pattern that was already consumed
    storage_markers = []
    for i, t in enumerate(tokens):
        if i not in consumed and t.lower() in [
            "ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "drive", "drives", 
            "harddrive", "hard", "disk", "disks", "local", "locstorage", "hd", "ssd/os"
        ]:
            storage_markers.append(i)
    
    gpu_markers = [i for i, t in enumerate(tokens) if i not in consumed and is_gpu_context_token(t)]
    
    print(f"RAM markers at indices: {ram_markers}")
    print(f"Storage markers at indices: {storage_markers} (after consuming 'No' patterns)")
    print(f"GPU markers at indices: {gpu_markers}")
    
    return {
        'ram_markers': ram_markers,
        'storage_markers': storage_markers,
        'gpu_markers': gpu_markers
    }
    
def is_raid_or_network_context_token(token):
    """Check if a token indicates RAID or network speed context."""
    cleaned = clean_token_for_context(token)
    raid_network_terms = [
        "raid", "sas", "fc", "fiber", "fibre", "ethernet", "network", 
        "controller", "adapter", "hba", "nic", "switch", "port", "ports",
        "speed", "transfer", "rate", "bandwidth", "connection", "interface"
    ]
    return any(term in cleaned for term in raid_network_terms)

def extract_clear_storage_patterns(tokens: List[str], consumed: Set[int]) -> List[Dict]:
    """STEP 1: Extract all clear storage patterns first (SIZE + TYPE combinations)."""
    clear_storage_patterns = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        
        # Skip speed/rate patterns like "6Gb/s", "6GB/s"
        if re.match(r"\d+(?:\.\d+)?[GMK]?[Bb]/s$", token, re.IGNORECASE):
            print(f"Skipping speed/rate pattern: {token}")
            continue
        
        # PATTERN 1: Combined token patterns (like "512GB" + "SSD") - HIGHEST PRIORITY
        if (re.search(r'^\d+(?:\.\d+)?(gb|tb)$', token.lower(), re.IGNORECASE) and
            i+1 < len(tokens) and i+1 not in consumed and
            tokens[i+1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage", "hd"]):
            
            capacity_match = re.search(r'^(\d+(?:\.\d+)?)(gb|tb)$', token.lower())
            if capacity_match:
                capacity_value = int(float(capacity_match.group(1)))
                clear_storage_patterns.append({
                    'indices': [i, i+1],
                    'capacity': capacity_value,
                    'tokens': f"{token} {tokens[i+1]}",
                    'type': 'size_type'
                })
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Found clear SIZE+TYPE pattern: {token} {tokens[i+1]} (capacity: {capacity_value})")
        
        # PATTERN 2: TYPE + SIZE patterns (like "SSD" + "512GB")
        elif (token.lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage", "hd"] and 
              i+1 < len(tokens) and i+1 not in consumed and
              re.search(r'^\d+(?:\.\d+)?(gb|tb)$', tokens[i+1].lower(), re.IGNORECASE)):
            
            capacity_match = re.search(r'^(\d+(?:\.\d+)?)(gb|tb)$', tokens[i+1].lower())
            if capacity_match:
                capacity_value = int(float(capacity_match.group(1)))
                clear_storage_patterns.append({
                    'indices': [i, i+1],
                    'capacity': capacity_value,
                    'tokens': f"{token} {tokens[i+1]}",
                    'type': 'type_size'
                })
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Found clear TYPE+SIZE pattern: {token} {tokens[i+1]} (capacity: {capacity_value})")
        
        # PATTERN 3: Separated token patterns (like "512 GB" + "SSD") 
        elif (re.match(r'^\d+(?:\.\d+)?$', token) and 
              i+1 < len(tokens) and i+1 not in consumed and
              re.match(r'^(gb|tb)$', tokens[i+1].lower(), re.IGNORECASE) and
              i+2 < len(tokens) and i+2 not in consumed and
              tokens[i+2].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage", "hd"]):
            
            capacity_value = int(float(token))
            clear_storage_patterns.append({
                'indices': [i, i+1, i+2],
                'capacity': capacity_value,
                'tokens': f"{token} {tokens[i+1]} {tokens[i+2]}",
                'type': 'num_unit_type'
            })
            if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                print(f"Found clear NUM+UNIT+TYPE pattern: {token} {tokens[i+1]} {tokens[i+2]} (capacity: {capacity_value})")
    
    return clear_storage_patterns

def process_clear_storage_patterns(clear_storage_patterns: List[Dict], consumed: Set[int]) -> List[List[int]]:
    """STEP 2: If we found clear storage patterns, extract the largest one and return."""
    results = []
    
    if clear_storage_patterns:
        # Sort by capacity (largest first)
        clear_storage_patterns.sort(key=lambda x: x['capacity'], reverse=True)
        
        # Extract the largest clear storage pattern
        largest_pattern = clear_storage_patterns[0]
        results.append(largest_pattern['indices'])
        for idx in largest_pattern['indices']:
            consumed.add(idx)
        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
            print(f"Selected largest clear storage pattern: {largest_pattern['tokens']} (capacity: {largest_pattern['capacity']})")
            print("Leaving smaller capacity for RAM extractor: 32GB (capacity: 32)")
    
    return results

def extract_standalone_capacities_with_lookahead(tokens: List[str], consumed: Set[int], context: Dict, markers: Dict) -> List[List[int]]:
    """STEP 3: No clear storage patterns found - use look-ahead logic for standalone capacities."""
    results = []
    
    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
        print("No clear storage patterns found - using look-ahead logic")
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        
        # Look for standalone capacity tokens
        if re.search(r'^\d+(?:\.\d+)?(gb|tb)$', token.lower(), re.IGNORECASE):
            # RAM-SEQUENCE GUARD: If this capacity is part of a slash-separated sequence
            # that is followed shortly by a RAM/MEMORY indicator, skip treating it as storage.
            in_slash_context = ((i + 1 < len(tokens) and tokens[i + 1] == '/') or (i > 0 and tokens[i - 1] == '/'))
            if in_slash_context:
                # Scan ahead a small window for RAM/MEMORY
                window_end = min(len(tokens), i + 10)
                for j in range(i + 1, window_end):
                    tok_clean = re.sub(r'[^a-z]', '', tokens[j]).lower()
                    if tok_clean in {"ram", "memory"}:
                        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                            print(f"Skipping standalone capacity at {i} due to slash+RAM context ahead: {token}")
                        # Do not extract this capacity as storage
                        # Also do not consume; let RAM extractor handle
                        break
                else:
                    pass  # No RAM found; continue with storage checks
                # If we broke out due to RAM, continue to next token
                if any(re.sub(r'[^a-z]', '', tokens[j]).lower() in {"ram", "memory"} for j in range(i + 1, window_end)):
                    continue
            
            # HIGHEST PRIORITY: Check for immediate RAID/network context FIRST
            is_immediate_raid = (i + 1 < len(tokens) and is_raid_or_network_context_token(tokens[i + 1]))
            if is_immediate_raid:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Skipping token {i}: {token} - RAID/network context detected: {tokens[i+1]}")
                continue
            
            # Check for immediate context exclusions
            is_immediate_ram = (i + 1 < len(tokens) and is_ram_context_token(tokens[i + 1]))
            is_immediate_gpu = False
            
            # Check for GPU model patterns
            if (i >= 2 and
                is_gpu_context_token(tokens[i-2]) and
                re.match(r'^\d{3,4}[a-z]*$', tokens[i-1], re.IGNORECASE)):
                is_immediate_gpu = True
            elif (i > 0 and is_gpu_context_token(tokens[i-1])):
                is_immediate_gpu = True
            elif (i > 0 and 
                  re.search(r'(gtx|rtx)\s*\d+', tokens[i-1].lower(), re.IGNORECASE)):
                is_immediate_gpu = True
            
            if is_immediate_ram:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Skipping token {i}: {token} - immediately followed by RAM context: {tokens[i+1]}")
                continue
                
            if is_immediate_gpu:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Skipping token {i}: {token} - GPU context detected")
                continue
            
            # NEW: Check if we already have storage in consumed tokens FIRST
            already_found_storage = False
            largest_found_storage = 0
            
            for consumed_idx in consumed:
                if consumed_idx < len(tokens):
                    consumed_token = tokens[consumed_idx]
                    if re.search(r'^\d+(?:\.\d+)?(gb|tb)$', consumed_token.lower(), re.IGNORECASE):
                        # Check if next token is storage type
                        if (consumed_idx + 1 < len(tokens) and 
                            consumed_idx + 1 in consumed and
                            tokens[consumed_idx + 1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "storage", "local", "locstorage"]):
                            already_found_storage = True
                            capacity_match = re.search(r'^(\d+(?:\.\d+)?)(gb|tb)$', consumed_token.lower())
                            if capacity_match:
                                capacity_value = int(float(capacity_match.group(1)))
                                largest_found_storage = max(largest_found_storage, capacity_value)
            
            if already_found_storage:
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Already found clear storage pattern with capacity {largest_found_storage}GB - being conservative with remaining capacities")
                # Only extract if there's explicit storage context
                skip_due_to_ram = False
                skip_due_to_gpu = False
                has_storage_marker = False
                for j in range(max(0, i-1), min(len(tokens), i+2)):
                    if j in markers['ram_markers']:
                        skip_due_to_ram = True
                    if j in markers['gpu_markers']:
                        skip_due_to_gpu = True
                    if j in markers['storage_markers']:
                        has_storage_marker = True
                
                # Only extract if explicit storage context OR phone context
                if context['is_phone_context'] or has_storage_marker:
                    results.append([i])
                    consumed.add(i)
                    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                        print(f"Found standalone storage capacity at {i}: {token} (explicit context)")
                else:
                    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                        print(f"Skipping {token} - no explicit storage context and clear storage already found")
                continue
            
            # Look ahead for multiple capacity values and storage context
            look_ahead_results = []
            scan_distance = 8  # Look ahead up to 8 tokens
            
            for j in range(i, min(len(tokens), i + scan_distance)):
                if j in consumed:
                    continue
                    
                current_token = tokens[j]
                
                # Found another capacity
                if re.search(r'^\d+(?:\.\d+)?(gb|tb)$', current_token.lower(), re.IGNORECASE):
                    capacity_match = re.search(r'^(\d+(?:\.\d+)?)(gb|tb)$', current_token.lower())
                    if capacity_match:
                        capacity_value = int(float(capacity_match.group(1)))
                        
                        # Check if this capacity is near storage indicators that are NOT consumed
                        has_storage_nearby = any(k in markers['storage_markers'] for k in range(max(0, j-2), min(len(tokens), j+3)))
                        
                        look_ahead_results.append({
                            'index': j,
                            'capacity': capacity_value,
                            'unit': capacity_match.group(2),
                            'has_storage_nearby': has_storage_nearby
                        })
                        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                            print(f"Found capacity in look-ahead at {j}: {current_token} (capacity: {capacity_value}, storage_nearby: {has_storage_nearby})")
            
            # Process look-ahead results
            if len(look_ahead_results) >= 2:
                # Sort by capacity (largest first)
                look_ahead_results.sort(key=lambda x: x['capacity'], reverse=True)
                
                # If the largest has storage context, use it
                largest = look_ahead_results[0]
                if largest['has_storage_nearby']:
                    results.append([largest['index']])
                    consumed.add(largest['index'])
                    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                        print(f"Selected largest capacity with storage context: {tokens[largest['index']]} (capacity: {largest['capacity']})")
                    
                    # Skip all smaller capacities
                    if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                        print("Skipping all smaller capacities - clear storage pattern found")
                    continue
                
                # If largest is significantly bigger than smallest, prefer largest
                smallest = look_ahead_results[-1]
                if largest['capacity'] >= smallest['capacity'] * 2:  # At least 2x larger
                    # Check broader context for largest
                    has_storage_context = any(j in markers['storage_markers'] for j in range(max(0, largest['index']-2), min(len(tokens), largest['index']+3)))
                    has_ram_context = any(j in markers['ram_markers'] for j in range(max(0, largest['index']-2), min(len(tokens), largest['index']+3)))
                    
                    if not has_ram_context or has_storage_context:
                        results.append([largest['index']])
                        consumed.add(largest['index'])
                        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                            print(f"Selected largest capacity (size heuristic): {tokens[largest['index']]} (capacity: {largest['capacity']})")
                        
                        # Skip all smaller capacities
                        if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                            print("Skipping all smaller capacities - size heuristic applied")
                        continue
            
            # Fallback: single capacity or no clear winner - apply context rules
            skip_due_to_ram = False
            skip_due_to_gpu = False
            has_storage_marker = False
            for j in range(max(0, i-1), min(len(tokens), i+2)):
                if j in markers['ram_markers']:
                    skip_due_to_ram = True
                if j in markers['gpu_markers']:
                    skip_due_to_gpu = True
                if j in markers['storage_markers']:
                    has_storage_marker = True
            
            if ((skip_due_to_ram or skip_due_to_gpu) and 
                not has_storage_marker and not context['is_phone_context']):
                context_type = "RAM" if skip_due_to_ram else "GPU"
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Skipping token {i}: {token} due to {context_type} context (non-phone device)")
                continue
            
            # Extract if passes context checks
            if context['is_phone_context'] or has_storage_marker or (not skip_due_to_ram and not skip_due_to_gpu):
                results.append([i])
                consumed.add(i)
                if hasattr(globals(), 'DEBUG_STORAGE') and globals()['DEBUG_STORAGE']:
                    print(f"Found standalone storage capacity at {i}: {token}")
    
    return results

def extract_separated_patterns_with_units(tokens: List[str], consumed: Set[int], context: Dict) -> List[List[int]]:
    """STEP 4: Handle remaining separated patterns with units."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        if (re.match(r'^\d+(?:\.\d+)?$', token) and 
            i+1 < len(tokens) and i+1 not in consumed and
            re.match(r'^(gb|tb)$', tokens[i+1].lower(), re.IGNORECASE)):
            
            # Check if this GB unit is immediately followed by RAM
            is_gb_ram = (i+2 < len(tokens) and is_ram_context_token(tokens[i+2]))
            
            # Check if this GB unit is immediately followed by RAID/network context
            is_gb_raid = (i+2 < len(tokens) and is_raid_or_network_context_token(tokens[i+2]))
            
            if is_gb_ram:
                print(f"Skipping NUMBER + UNIT at {i},{i+1}: {token} {tokens[i+1]} - followed by RAM: {tokens[i+2]}")
                continue
                
            if is_gb_raid:
                print(f"Skipping NUMBER + UNIT at {i},{i+1}: {token} {tokens[i+1]} - followed by RAID/network: {tokens[i+2]}")
                continue
            
            # Just NUMBER + UNIT without clear storage type - allow in phone context only
            if context['is_phone_context']:
                results.append([i, i+1])
                consumed.add(i)
                consumed.add(i+1)
                print(f"Found NUMBER + UNIT at {i},{i+1}: {token} {tokens[i+1]} (phone/tablet context)")
    
    return results

def extract_explicit_storage_patterns(tokens: List[str], consumed: Set[int], context: Dict, markers: Dict) -> List[List[int]]:
    """Extract explicit storage patterns - SIZE + TYPE combinations with smart look-ahead."""
    results = []
    
    # STEP 1: HIGHEST PRIORITY - Extract all clear storage patterns first (SIZE + TYPE combinations)
    # RAM context guard: If a SIZE token is immediately followed by RAM/MEMORY, do not treat
    # it as a clear storage pattern. This prevents "8GB/12GB RAM" from being captured by storage.
    clear_storage_patterns = []
    raw_clear = extract_clear_storage_patterns(tokens, consumed)
    for pat in raw_clear:
        idxs = pat.get('indices', [])
        if not idxs:
            continue
        last = idxs[-1]
        next_idx = last + 1
        if next_idx < len(tokens):
            nxt_clean = re.sub(r'[^a-z]', '', tokens[next_idx]).lower()
            if nxt_clean in {"ram", "memory"}:
                print("Skipping clear storage pattern due to immediate RAM context after pattern")
                continue
        clear_storage_patterns.append(pat)
    
    # STEP 2: If we found clear storage patterns, extract the largest one and return
    clear_results = process_clear_storage_patterns(clear_storage_patterns, consumed)
    if clear_results:
        results.extend(clear_results)
        return results
    
    # STEP 3: No clear storage patterns found - use look-ahead logic for standalone capacities
    standalone_results = extract_standalone_capacities_with_lookahead(tokens, consumed, context, markers)
    results.extend(standalone_results)
    
    # STEP 4: Handle remaining separated patterns with units
    separated_results = extract_separated_patterns_with_units(tokens, consumed, context)
    results.extend(separated_results)
    
    return results
    
def extract_standalone_storage_sizes(self, tokens: List[str], consumed: Set[int], context: Dict, markers: Dict, results: List[List[int]]) -> List[List[int]]:
    """Extract standalone storage sizes with context awareness."""
    standalone_results = []
    
    # Check if we already found multi-capacity patterns
    multi_capacity_found = any(len(result) > 2 for result in results if any(tokens[idx] == '/' for idx in result if idx < len(tokens)))
    
    if not multi_capacity_found:
        for i in range(len(tokens)):
            if i in consumed:
                continue
            
            # Detect composite tokens that encode the negation and the storage
            # keyword in a single token, e.g. 'No SSD', 'NoSSD', 'No_HDD'.
            composite_lower = tokens[i].lower()
            if composite_lower.startswith("no") and any(kw in composite_lower for kw in ["ssd", "hdd", "drive", "drives", "harddrive", "hard", "storage", "hd", "locstorage"]):
                results.append([i])
                consumed.add(i)
                print(f"Storage explicitly not included - composite token: {tokens[i]} (consumed)")

                # If a capacity token (e.g. '8GB') appears immediately before this
                # composite token, consume it as well to prevent later extraction.
                prev_idx = i - 1
                if prev_idx >= 0 and prev_idx not in consumed and re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[prev_idx], re.IGNORECASE):
                    consumed.add(prev_idx)
                    results[-1].insert(0, prev_idx)
                    print(f"Also consumed preceding capacity token due to composite 'No storage' token: {tokens[prev_idx]}")

                # Proceed to next iteration since we've fully handled this token.
                continue
            
            # Match any number + storage unit pattern but exclude range patterns
            if (re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE) and
                not re.match(r'^\d+(?:\.\d+)?-\d+(?:\.\d+)?(gb|tb|mb)$', tokens[i], re.IGNORECASE)):
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Evaluating standalone storage token {i}: {tokens[i]}")
                
                # HIGHEST PRIORITY: Check for immediate RAID/network context FIRST
                is_immediate_raid = (i + 1 < len(tokens) and is_raid_or_network_context_token(tokens[i + 1]))
                if is_immediate_raid:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} - RAID/network context detected: {tokens[i+1]}")
                    continue
                
                # Priority checks for immediate RAM/GPU context
                is_immediate_ram = (i + 1 < len(tokens) and is_ram_context_token(tokens[i + 1]))
                is_immediate_gpu = False
                
                # Check for GPU model patterns
                if (i >= 2 and
                    is_gpu_context_token(tokens[i-2]) and
                    re.match(r'^\d{3,4}[a-z]*$', tokens[i-1], re.IGNORECASE)):
                    is_immediate_gpu = True
                elif (i > 0 and is_gpu_context_token(tokens[i-1])):
                    is_immediate_gpu = True
                elif (i > 0 and 
                      re.search(r'(gtx|rtx)\s*\d+', tokens[i-1].lower(), re.IGNORECASE)):
                    is_immediate_gpu = True
                
                if is_immediate_ram:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} - immediately followed by RAM: {tokens[i+1]}")
                    continue
                    
                if is_immediate_gpu:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} - GPU context detected")
                    continue
                
                # Context-based extraction logic
                has_storage_context = any(j in markers['storage_markers'] for j in range(max(0, i-2), min(len(tokens), i+3)))
                has_ram_context = any(j in markers['ram_markers'] for j in range(max(0, i-2), min(len(tokens), i+3)))
                
                should_extract = False
                if context['is_phone_context'] or context['device_type'] in ['Tablets & eReaders', 'Cell Phones & Smartphones']:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Phone/tablet context - extracting {tokens[i]}")
                elif has_storage_context:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Storage context detected - extracting {tokens[i]}")
                elif not has_ram_context:
                    should_extract = True
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: No RAM context - extracting {tokens[i]}")
                else:
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Skipping {tokens[i]} due to RAM context in computer device")
                
                if should_extract:
                    standalone_results.append([i])
                    consumed.add(i)
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Found standalone storage size at {i}: {tokens[i]}")
    
    return standalone_results
    
def enhanced_storage_process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
    result = {}
    if not match_indices:
        return result
    
    flat_indices = []
    for idx in match_indices:
        if isinstance(idx, int) and 0 <= idx < len(tokens):
            flat_indices.append(idx)
        elif isinstance(idx, list):
            for x in idx:
                if isinstance(x, int) and 0 <= x < len(tokens):
                    flat_indices.append(x)
    
    if not flat_indices:
        return result
    
    match_text = " ".join(tokens[idx].lower() for idx in flat_indices)
    if hasattr(self, 'logger') and self.logger:
        self.logger.debug(f"Storage: Processing match text: {match_text}")

    # SPECIAL POST-PROCESS: Handle titles like "No Power Cord/HardDrive/SSD"
    # without altering existing pattern sets. If detected, force storage_status
    # to Not Included so that lone capacities (e.g., "8GB") are not treated as storage.
    if _title_indicates_no_storage_via_power_cord_group(tokens) or _title_indicates_no_storage_compact_powercord_group(tokens):
        if hasattr(self, 'logger') and self.logger:
            self.logger.debug("Storage: Detected 'No Power Cord/.../SSD|HDD' group – setting storage_status='Not Included'")
        return {"storage_status": "Not Included"}

    # Determine phone/tablet context once for stricter handling below
    try:
        device_ctx = getattr(self, 'device_context', {})
        device_type = device_ctx.get('device_type', '')
        is_phone_context = (
            device_ctx.get('is_phone_device', False)
            or device_ctx.get('has_phone_context', False)
            or device_type in ['Cell Phones & Smartphones', 'Tablets & eReaders']
        )
    except Exception:
        is_phone_context = False

    # RAM CONTEXT GUARD (GLOBAL): If a matched capacity sequence is immediately
    # followed by RAM/MEMORY (or such appears within a short window), treat it
    # as RAM and return no storage fields.
    try:
        end_idx = max(flat_indices)
        # Look ahead up to 4 tokens for RAM/MEMORY regardless of consumption
        lookahead_end = min(len(tokens), end_idx + 5)
        for j in range(end_idx + 1, lookahead_end):
            tok_clean = re.sub(r'[^a-z]', '', tokens[j]).lower()
            if tok_clean in {"ram", "memory"}:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug("Storage: Skipping match due to RAM context after sequence")
                return {}
        # Also look a small window before the sequence for explicit RAM markers that
        # indicate the numbers belong to RAM (e.g., "RAM 8/12GB")
        start_idx = min(flat_indices)
        lookback_start = max(0, start_idx - 3)
        for j in range(lookback_start, start_idx):
            tok_clean = re.sub(r'[^a-z]', '', tokens[j]).lower()
            if tok_clean in {"ram", "memory"}:
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug("Storage: Skipping match due to RAM context before sequence")
                return {}
    except Exception:
        # Be permissive on guard failures
        pass
    
    # Handle "No" patterns first
    if (any(tokens[idx].lower() in ["n/a", "no", "none"] for idx in flat_indices) or
        "none included" in match_text or
        "no hdd" in match_text or
        "no ssd" in match_text or
        "no hard drive" in match_text or
        match_text == "no" or
        ("none" in match_text and "included" in match_text)):
        result = {"storage_status": "Not Included"}
        # Ensure we do NOT retain any capacity or type keys when storage is absent
        # (redundant safety – they shouldn't exist yet, but this guarantees it)
        if hasattr(self, 'logger') and self.logger:
            self.logger.debug("Storage: Set storage_status to 'Not Included'; clearing any capacity/type fields present")
        for key in list(result.keys()):
            if key.startswith("storage_capacity") or key in {"storage_type", "storage_range"}:
                result.pop(key, None)
        return result
    
    # Handle range patterns like "250GB-1TB" and "250-500GB"
    if len(flat_indices) >= 1:
        first_token = tokens[flat_indices[0]].lower()
        
        # Pattern 1: Both numbers have units (like "250GB-500GB")
        range_match = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)-(\d+(?:\.\d+)?)(gb|tb|mb)$', first_token, re.IGNORECASE)
        if range_match:
            start_size = range_match.group(1) + range_match.group(2).upper()
            end_size = range_match.group(3) + range_match.group(4).upper()
            result["storage_range"] = f"{start_size}-{end_size}"
            if hasattr(self, 'logger') and self.logger:
                self.logger.debug(f"Storage: Set storage_range to {result['storage_range']}")
            
            if len(flat_indices) > 1 and flat_indices[1] == flat_indices[0] + 1:
                type_token = tokens[flat_indices[1]].lower()
                if type_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                    if type_token in ["local", "locstorage"]:
                        result["storage_type"] = "LOCAL STORAGE"
                    else:
                        result["storage_type"] = type_token.upper()
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Set storage_type to {result['storage_type']}")
            return result
        
        # Pattern 2: Only second number has unit (like "250-500GB")
        range_match2 = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)(gb|tb|mb)$', first_token, re.IGNORECASE)
        if range_match2:
            unit = range_match2.group(3).upper()
            start_size = range_match2.group(1) + unit
            end_size = range_match2.group(2) + unit
            result["storage_range"] = f"{start_size}-{end_size}"
            if hasattr(self, 'logger') and self.logger:
                self.logger.debug(f"Storage: Set storage_range to {result['storage_range']}")
            
            if len(flat_indices) > 1 and flat_indices[1] == flat_indices[0] + 1:
                type_token = tokens[flat_indices[1]].lower()
                if type_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                    if type_token in ["local", "locstorage"]:
                        result["storage_type"] = "LOCAL STORAGE"
                    else:
                        result["storage_type"] = type_token.upper()
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Set storage_type to {result['storage_type']}")
            return result
    
    # Check for dual storage configuration with different types (e.g., "128GB SSD/1TB HDD")
    # This pattern has higher priority than regular slash-separated patterns
    if len(flat_indices) >= 5:
        first_idx, second_idx, slash_idx, third_idx, fourth_idx = flat_indices[:5]
        
        # Check for pattern: <SIZE1><UNIT> <TYPE1> / <SIZE2><UNIT> <TYPE2>
        if (slash_idx < len(tokens) and tokens[slash_idx] == '/' and
            first_idx < len(tokens) and re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[first_idx], re.IGNORECASE) and
            second_idx < len(tokens) and tokens[second_idx].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"] and
            third_idx < len(tokens) and re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[third_idx], re.IGNORECASE) and
            fourth_idx < len(tokens) and tokens[fourth_idx].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]):
            
            # First storage capacity and type
            first_capacity = re.search(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', tokens[first_idx], re.IGNORECASE)
            first_capacity_value = first_capacity.group(1) + first_capacity.group(2).upper()
            first_type = tokens[second_idx].upper() if tokens[second_idx].lower() not in ["local", "locstorage"] else "LOCAL STORAGE"
            
            # Second storage capacity and type
            second_capacity = re.search(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', tokens[third_idx], re.IGNORECASE)
            second_capacity_value = second_capacity.group(1) + second_capacity.group(2).upper()
            second_type = tokens[fourth_idx].upper() if tokens[fourth_idx].lower() not in ["local", "locstorage"] else "LOCAL STORAGE"
            
            # Store both capacities and types
            result["storage_capacity1"] = first_capacity_value
            result["storage_type1"] = first_type
            result["storage_capacity2"] = second_capacity_value
            result["storage_type2"] = second_type
            
            print(f"Processed dual storage: {first_capacity_value} {first_type} and {second_capacity_value} {second_type}")
            return result
            
    # Check for compact dual storage configuration (e.g., "128GBSSD/1TBHDD")
    if len(flat_indices) >= 3:
        first_idx, slash_idx, second_idx = flat_indices[:3]
        
        if (slash_idx < len(tokens) and tokens[slash_idx] == '/' and
            first_idx < len(tokens) and second_idx < len(tokens)):
            
            # Check first token for SIZE+UNIT+TYPE pattern
            first_pattern = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[first_idx], re.IGNORECASE)
            # Check second token for SIZE+UNIT+TYPE pattern
            second_pattern = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[second_idx], re.IGNORECASE)
            
            if first_pattern and second_pattern:
                # First storage capacity and type
                first_capacity_value = first_pattern.group(1) + first_pattern.group(2).upper()
                first_type = first_pattern.group(3).upper()
                
                # Second storage capacity and type
                second_capacity_value = second_pattern.group(1) + second_pattern.group(2).upper()
                second_type = second_pattern.group(3).upper()
                
                # Store both capacities and types
                result["storage_capacity1"] = first_capacity_value
                result["storage_type1"] = first_type
                result["storage_capacity2"] = second_capacity_value
                result["storage_type2"] = second_type
                
                print(f"Processed compact dual storage: {first_capacity_value} {first_type} and {second_capacity_value} {second_type}")
                return result
    
    # PHONE-ONLY: Handle simple separated NUMBER + UNIT (e.g., "64 GB") with no explicit storage type
    # This supports phone titles like "iPhone 8 64 GB ..."
    if is_phone_context and not result:
        # Look for any adjacent pair in the original token order that is number followed by unit
        try:
            # Work with the contiguous range covering the match
            start_idx = min(flat_indices)
            end_idx = max(flat_indices)
            for i in range(start_idx, min(len(tokens) - 1, end_idx + 1)):
                if re.match(r'^\d+(?:\.\d+)?$', tokens[i]) and re.match(r'^(gb|tb|mb)$', tokens[i + 1], re.IGNORECASE):
                    number = tokens[i]
                    unit = tokens[i + 1].upper()
                    result["storage_capacity1"] = f"{number}{unit}"
                    # Do not set storage_type here; phones often omit it
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Phone context NUMBER+UNIT -> storage_capacity1={result['storage_capacity1']}")
                    return result
        except Exception:
            pass

    # ENHANCED: Handle multi-capacity slash-separated patterns like "16/32/64/128/256 GB"
    # Check if we have a sequence with multiple slashes
    slash_count = sum(1 for idx in flat_indices if idx < len(tokens) and tokens[idx] == '/')
    if slash_count >= 1:
        if hasattr(self, 'logger') and self.logger:
            self.logger.debug(f"Storage: Processing slash-separated sequence with {slash_count} slashes")
        
        # Extract all numbers and find the unit
        numbers = []
        unit = None
        
        # Go through indices in order and collect numbers and unit
        for idx in flat_indices:
            if idx < len(tokens):
                token = tokens[idx]
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Processing token at index {idx}: '{token}'")
                
                # Skip slashes
                if token == '/':
                    continue
                    
                # Check if it's a standalone number
                if token.isdigit():
                    numbers.append(token)
                    print(f"Found standalone number: {token}")
                
                # Check if it's a number with unit (final token in sequence)
                elif re.search(r'^\d+(gb|tb|mb)$', token.lower(), re.IGNORECASE):
                    unit_match = re.search(r'^(\d+)(gb|tb|mb)$', token.lower(), re.IGNORECASE)
                    if unit_match:
                        numbers.append(unit_match.group(1))
                        unit = unit_match.group(2).upper()
                        print(f"Found number with unit: {unit_match.group(1)} + {unit}")
                
                # Check if it's a standalone unit
                elif re.search(r'^(gb|tb|mb)$', token.lower(), re.IGNORECASE):
                    unit = token.upper()
                    print(f"Found standalone unit: {unit}")
                
                # ENHANCED: Check if it's a number with unit (for each-has-unit pattern like "16GB/32GB/64GB")
                elif re.search(r'^\d+(gb|tb|mb)$', token.lower(), re.IGNORECASE):
                    unit_match = re.search(r'^(\d+)(gb|tb|mb)$', token.lower(), re.IGNORECASE)
                    if unit_match:
                        numbers.append(unit_match.group(1))
                        # Set unit from first occurrence, but verify all units are the same
                        current_unit = unit_match.group(2).upper()
                        if unit is None:
                            unit = current_unit
                        elif unit != current_unit:
                            print(f"Warning: Mixed units found ({unit} vs {current_unit})")
                        print(f"Found number with unit: {unit_match.group(1)} + {current_unit}")
        
        print(f"Extracted numbers: {numbers}, unit: {unit}")
        
        # FALLBACK: If we didn't find enough numbers, try using full title pattern matching
        if len(numbers) < 2:
            # Look at the whole tokens list to see if there's a larger pattern we missed
            full_text = " ".join(tokens)
            print(f"Trying fallback pattern matching on full text")
            
            # Look for patterns like "16/32/64/128 GB" even if they're split up
            fallback_pattern = re.search(r'(\d+)\s*/\s*(\d+)(?:\s*/\s*(\d+))?(?:\s*/\s*(\d+))?(?:\s*/\s*(\d+))?\s*(?:gb|tb|mb)?', full_text, re.IGNORECASE)
            
            if fallback_pattern:
                matched_text = fallback_pattern.group(0)
                new_numbers = []
                for i in range(1, 6):  # Check all capture groups
                    if fallback_pattern.group(i):
                        new_numbers.append(fallback_pattern.group(i))
                # In phone/tablet context, be stricter: skip if percent appears right after the slash numbers
                # Handle both inside the matched segment and immediately following it in the original text
                percent_immediately_after = False
                try:
                    following_text = full_text[fallback_pattern.end():]
                    if re.match(r'\s*%+', following_text):
                        percent_immediately_after = True
                except Exception:
                    percent_immediately_after = False
                if is_phone_context and ('%' in matched_text or percent_immediately_after):
                    print("Skipping fallback number extraction in phone context due to percent in slash segment")
                else:
                    if len(new_numbers) > len(numbers):
                        print(f"Found better pattern via fallback: {new_numbers}")
                        numbers = new_numbers
                        
                        # If we don't have a unit yet, try to find it
                        if not unit:
                            # In phone context, only accept a unit that is part of the matched segment,
                            # not borrowed from elsewhere in the title (prevents 64GB from leaking into 100/74%)
                            search_text = matched_text if is_phone_context else full_text
                            unit_match = re.search(r'(?:gb|tb|mb)', search_text, re.IGNORECASE)
                            if unit_match:
                                unit = unit_match.group(0).upper()
                                print(f"Found unit via fallback: {unit}")
        
        # If we found multiple numbers and a unit, create numbered storage capacity fields
        if len(numbers) >= 2 and unit:
            print(f"Creating {len(numbers)} storage capacity fields with unit {unit}")
            for i, number in enumerate(numbers, 1):
                capacity_key = f"storage_capacity{i}"
                result[capacity_key] = f"{number}{unit}"
                print(f"Set {capacity_key} to {result[capacity_key]}")
            return result
        
        # Fallback for simple two-number slash pattern like "64/256GB"
        elif len(numbers) == 2 and unit:
            result["storage_capacity1"] = f"{numbers[0]}{unit}"
            result["storage_capacity2"] = f"{numbers[1]}{unit}"
            print(f"Set storage_capacity1 to {result['storage_capacity1']}")
            print(f"Set storage_capacity2 to {result['storage_capacity2']}")
            return result
        
        # If we only found one number with unit, treat as single capacity
        elif len(numbers) == 1 and unit:
            result["storage_capacity1"] = f"{numbers[0]}{unit}"
            print(f"Set single storage_capacity1 to {result['storage_capacity1']}")
            return result
    
    # Find storage type and capitalize it
    storage_type = None
    for idx in flat_indices:
        if idx < len(tokens):
            lower_token = tokens[idx].lower()
            if lower_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                if lower_token in ["local", "locstorage"]:
                    storage_type = "LOCAL STORAGE"
                else:
                    storage_type = lower_token.upper()
                break
            elif lower_token in ["ssds", "hdds"]:
                storage_type = lower_token[:-1].upper()
                break
            elif lower_token in ["ide", "sata", "scsi", "ata"]:
                storage_type = "HDD"
                break
    
    # Handle simple patterns - single capacity + type
    for idx in flat_indices:
        if idx < len(tokens) and re.search(r'^\d+(?:\.\d+)?(gb|tb)$', tokens[idx].lower(), re.IGNORECASE):
            value = re.sub(r'(gb|tb)$', lambda m: m.group(1).upper(), tokens[idx].lower())
            result["storage_capacity1"] = value
            print(f"Set storage_capacity1 to {value}")
            if storage_type:
                result["storage_type"] = storage_type
                print(f"Set storage_type to {storage_type}")
            break
    
    return result
    
def is_sequence_followed_by_ram(tokens: List[str], start_idx: int, end_idx: int, is_ram_context_token, logger) -> bool:
    """Dynamically check if a slash-separated sequence is followed by RAM/MEMORY keywords."""
    current_idx = end_idx + 1
    max_scan_distance = 10  # Reasonable upper bound to avoid scanning entire token list
    
    while current_idx < len(tokens) and (current_idx - end_idx) <= max_scan_distance:
        if current_idx >= len(tokens):
            break
            
        token = tokens[current_idx].lower()
        
        # Found RAM/MEMORY context - this sequence is RAM-related
        if is_ram_context_token(tokens[current_idx]):
            if hasattr(logger, 'debug'):
                logger.debug(f"Found RAM context at distance {current_idx - end_idx}: {tokens[current_idx]}")
            else:
                logger(f"Found RAM context at distance {current_idx - end_idx}: {tokens[current_idx]}")
            return True
        
        # Check if we encounter storage-related tokens
        # This indicates the sequence is storage-related, not RAM
        storage_indicators = ['ssd', 'hdd', 'nvme', 'emmc', 'storage', 'drive', 'disk', 'ssd/os']
        if any(indicator in token for indicator in storage_indicators):
            if hasattr(logger, 'debug'):
                logger.debug(f"Found storage indicator at distance {current_idx - end_idx}: {token} - sequence is storage-related")
            else:
                logger(f"Found storage indicator at distance {current_idx - end_idx}: {token} - sequence is storage-related")
            return False  # Explicitly not RAM
        
        # Continue if we see patterns that could be part of the same capacity sequence
        if (token == '/' or  # Slash separator
            token.isdigit() or  # Standalone number
            re.match(r'^\d+(gb|tb|mb)$', token) or  # Number with storage unit
            re.match(r'^(gb|tb|mb)$', token)):  # Standalone storage unit
            if hasattr(logger, 'debug'):
                logger.debug(f"Continuing scan past capacity-related token: {token}")
            else:
                logger(f"Continuing scan past capacity-related token: {token}")
            current_idx += 1
            continue
        
        # Stop scanning if we hit tokens that clearly aren't part of a capacity sequence
        stop_tokens = [
            'cpu', 'processor', 'intel', 'amd', 'core',  # CPU-related
            'os', 'windows', 'linux', 'macos',  # OS-related
            'wifi', 'bluetooth', 'ethernet',  # Connectivity
            'hdmi', 'usb', 'displayport',  # Ports
            'battery', 'power', 'adapter',  # Power-related
            'read', 'note', 'see', 'description',  # Descriptive text
            '!', '?', 'condition', 'tested', 'working'  # Status indicators
        ]
        
        if any(stop_word in token for stop_word in stop_tokens):
            if hasattr(logger, 'debug'):
                logger.debug(f"Stopping scan at non-capacity token: {token}")
            else:
                logger(f"Stopping scan at non-capacity token: {token}")
            break
        
        # For tokens we don't recognize, continue for a bit but with caution
        if len(token) <= 3 or token.isalnum():
            if hasattr(logger, 'debug'):
                logger.debug(f"Continuing scan past short/alphanumeric token: {token}")
            else:
                logger(f"Continuing scan past short/alphanumeric token: {token}")
            current_idx += 1
            continue
        
        # Stop at longer unrecognized tokens
        if hasattr(logger, 'debug'):
            logger.debug(f"Stopping scan at unrecognized token: {token}")
        else:
            logger(f"Stopping scan at unrecognized token: {token}")
        break
    
    return False
    
def enhanced_storage_extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
    """Context-aware storage extraction without size restrictions."""
    results = []
    
    # EARLY OUT: If title indicates compact or spaced "No Power Cord/HardDrive/SSD" group,
    # mark storage as excluded and trigger a minimal match so process_match can set status.
    try:
        if _title_indicates_no_storage_via_power_cord_group(tokens) or _title_indicates_no_storage_compact_powercord_group(tokens):
            self.storage_excluded = True
            # Return a dummy match to invoke process_match, which will set storage_status
            return [[0]] if tokens else []
    except Exception:
        pass

    # If we have already encountered a "No storage" pattern in a previous pass,
    # immediately return an empty result so that no capacities are extracted on
    # subsequent iterations.
    if getattr(self, "storage_excluded", False):
        return []

    # Step 1: Setup context
    # ADDITIONAL early-out: If the full title contains phrases like 'No SSD',
    # 'No HDD', 'No Storage', etc., then storage is explicitly absent. We
    # simply mark `storage_excluded` and return without extracting any
    # capacities.
    full_text_lower = ' '.join(tokens).lower()
    if re.search(r"\bno\s+(ssd|ssds|hdd|hdds|storage|drive|drives|hard\s*drive|hd|locstorage|ssd/os)\b", full_text_lower):
        self.storage_excluded = True
        # Attempt to consume any tokens containing the negation phrase so
        # that subsequent extractors (e.g., RAM) don't treat them as
        # context markers.
        for idx, tok in enumerate(tokens):
            tok_lower = tok.lower()
            if re.search(r"\bno\s+(ssd|ssds|hdd|hdds|storage|drive|drives|hard\s*drive|hd|locstorage|ssd/os)\b", tok_lower):
                consumed.add(idx)
        print("Skipping storage extraction – 'No storage' phrase detected in full text")
        return []

    context = setup_storage_context(self, tokens)

    # EARLY HARD GUARD: RAM list like "8GB/12GB RAM" should never be treated as storage.
    try:
        full_text_lower = ' '.join(tokens).lower()
        if re.search(r"\b\d+(?:\.\d+)?(?:gb|tb)(?:\s*/\s*\d+(?:\.\d+)?(?:gb|tb))+\s+(?:ram|memory)\b", full_text_lower):
            print("Skipping storage extraction – detected slash-separated capacity list followed by RAM/MEMORY")
            return []
    except Exception:
        pass
    
    # CRITICAL NEW CHECK: Skip storage extraction entirely for Server Memory (RAM)
    device_type = context.get('device_type', '')
    if device_type == "Server Memory (RAM)":
        print(f"Skipping storage extraction - device type is Server Memory (RAM)")
        return results
    
    # ADDITIONAL CHECK: Detect server RAM patterns in tokens and skip if found
    full_text = ' '.join(tokens).lower()
    server_ram_indicators = [
        "server ram", "server memory", "ecc", "reg", "registered", "rdimm", "lrdimm", 
        "pc3", "pc4", "ddr3", "ddr4", "ddr5", "8500r", "10600r", "12800r", "14900r",
        "pc3l", "pc4l", "reg ecc", "registered ecc"
    ]
    
    has_server_ram_context = any(indicator in full_text for indicator in server_ram_indicators)
    
    # Check for module configuration patterns that indicate RAM
    has_ram_module_config = bool(re.search(r'\(\d+\s*x\s*\d+gb\)', full_text))
    
    if has_server_ram_context and has_ram_module_config:
        print(f"Skipping storage extraction - detected server RAM with module configuration")
        return results
    
    # CRITICAL FIX: Check if we already found clear storage in consumed tokens
    already_found_clear_storage = False
    for consumed_idx in consumed:
        if consumed_idx < len(tokens):
            consumed_token = tokens[consumed_idx]
            if re.search(r'^\d+(?:\.\d+)?(gb|tb)$', consumed_token.lower(), re.IGNORECASE):
                # Check if next token is storage type
                if (consumed_idx + 1 < len(tokens) and 
                    consumed_idx + 1 in consumed and
                    tokens[consumed_idx + 1].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "storage", "local", "locstorage"]):
                    already_found_clear_storage = True
                    print(f"Already found clear storage pattern - skipping all extraction in this call")
                    break
    
    if already_found_clear_storage:
        return results  # Return empty - don't extract anything
    
    # Step 2: Check if storage is explicitly not included
    not_included_results = check_storage_explicitly_not_included(tokens, consumed)
    if not_included_results is not None:
        # Mark that storage is explicitly not included so that subsequent
        # extractor passes short-circuit immediately.
        self.storage_excluded = True
        return not_included_results
    
    # Step 3: Extract slash-separated patterns (highest priority)
    # Guard: If the slash-separated pattern is followed by RAM/MEMORY, skip.
    slash_results = extract_slash_separated_patterns(tokens, consumed, context)
    guarded_slash_results = []
    for seq in slash_results:
        if not seq:
            continue
        end_idx = max([idx for idx in seq if isinstance(idx, int)] or [seq[-1]])
        next_idx = end_idx + 1
        skip_seq = False
        if next_idx < len(tokens):
            nxt_clean = re.sub(r'[^a-z]', '', tokens[next_idx]).lower()
            if nxt_clean in {"ram", "memory"}:
                print("Skipping slash-separated storage sequence due to immediate RAM context")
                skip_seq = True
        if not skip_seq:
            guarded_slash_results.append(seq)
    results.extend(guarded_slash_results)

    # Step 3.5: NEW pattern layer – "XGB RAM YGB" -> take YGB as storage (Y >= 256GB or any TB)
    try:
        ram_then_storage = extract_ram_then_storage_from_context(tokens)
        if ram_then_storage:
            results.extend(ram_then_storage)
    except Exception:
        pass
    
    # Step 4: Extract range patterns
    range_results = extract_range_patterns(tokens, consumed)
    results.extend(range_results)
    
    # Step 5: Identify context markers
    markers = identify_context_markers(tokens, consumed)
    
    # Step 6: Extract explicit storage patterns (with RAM guard applied inside)
    explicit_results = extract_explicit_storage_patterns(tokens, consumed, context, markers)
    results.extend(explicit_results)
    
    # Step 7: If we found explicit patterns, we're done
    if explicit_results or results:
        print("Explicit storage patterns found, skipping standalone extraction to avoid conflicts")
        return results
    
    # Step 8: Extract standalone storage sizes ONLY if no explicit patterns found
    standalone_results = extract_standalone_storage_sizes(self, tokens, consumed, context, markers, results)
    results.extend(standalone_results)
    
    return results
    
# -----------------------------------------------------------------------------
# Duplicate helper below renamed to avoid re-definition linter error.
# -----------------------------------------------------------------------------

def enhanced_storage_process_match_duplicate(self, tokens: List[str], match_indices: List[int]) -> Dict:
    result = {}
    if not match_indices:
        return result
    
    flat_indices = []
    for idx in match_indices:
        if isinstance(idx, int) and 0 <= idx < len(tokens):
            flat_indices.append(idx)
        elif isinstance(idx, list):
            for x in idx:
                if isinstance(x, int) and 0 <= x < len(tokens):
                    flat_indices.append(x)
    
    if not flat_indices:
        return result
    
    match_text = " ".join(tokens[idx].lower() for idx in flat_indices)
    if hasattr(self, 'logger') and self.logger:
        self.logger.debug(f"Storage: Processing match text: {match_text}")
    
    # Handle "No" patterns first
    if (any(tokens[idx].lower() in ["n/a", "no", "none"] for idx in flat_indices) or
        "none included" in match_text or
        "no hdd" in match_text or
        "no ssd" in match_text or
        "no hard drive" in match_text or
        match_text == "no" or
        ("none" in match_text and "included" in match_text)):
        result = {"storage_status": "Not Included"}
        print(f"Set storage_status to 'Not Included'")
        return result
    
    # Handle range patterns like "250GB-1TB" and "250-500GB"
    if len(flat_indices) >= 1:
        first_token = tokens[flat_indices[0]].lower()
        
        # Pattern 1: Both numbers have units (like "250GB-500GB")
        range_match = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)-(\d+(?:\.\d+)?)(gb|tb|mb)$', first_token, re.IGNORECASE)
        if range_match:
            start_size = range_match.group(1) + range_match.group(2).upper()
            end_size = range_match.group(3) + range_match.group(4).upper()
            result["storage_range"] = f"{start_size}-{end_size}"
            if hasattr(self, 'logger') and self.logger:
                self.logger.debug(f"Storage: Set storage_range to {result['storage_range']}")
            
            if len(flat_indices) > 1 and flat_indices[1] == flat_indices[0] + 1:
                type_token = tokens[flat_indices[1]].lower()
                if type_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                    if type_token in ["local", "locstorage"]:
                        result["storage_type"] = "LOCAL STORAGE"
                    else:
                        result["storage_type"] = type_token.upper()
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Set storage_type to {result['storage_type']}")
            return result
        
        # Pattern 2: Only second number has unit (like "250-500GB")
        range_match2 = re.match(r'^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)(gb|tb|mb)$', first_token, re.IGNORECASE)
        if range_match2:
            unit = range_match2.group(3).upper()
            start_size = range_match2.group(1) + unit
            end_size = range_match2.group(2) + unit
            result["storage_range"] = f"{start_size}-{end_size}"
            if hasattr(self, 'logger') and self.logger:
                self.logger.debug(f"Storage: Set storage_range to {result['storage_range']}")
            
            if len(flat_indices) > 1 and flat_indices[1] == flat_indices[0] + 1:
                type_token = tokens[flat_indices[1]].lower()
                if type_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                    if type_token in ["local", "locstorage"]:
                        result["storage_type"] = "LOCAL STORAGE"
                    else:
                        result["storage_type"] = type_token.upper()
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.debug(f"Storage: Set storage_type to {result['storage_type']}")
            return result
    
    # Check for dual storage configuration with different types (e.g., "128GB SSD/1TB HDD")
    # This pattern has higher priority than regular slash-separated patterns
    if len(flat_indices) >= 5:
        first_idx, second_idx, slash_idx, third_idx, fourth_idx = flat_indices[:5]
        
        # Check for pattern: <SIZE1><UNIT> <TYPE1> / <SIZE2><UNIT> <TYPE2>
        if (slash_idx < len(tokens) and tokens[slash_idx] == '/' and
            first_idx < len(tokens) and re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[first_idx], re.IGNORECASE) and
            second_idx < len(tokens) and tokens[second_idx].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"] and
            third_idx < len(tokens) and re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', tokens[third_idx], re.IGNORECASE) and
            fourth_idx < len(tokens) and tokens[fourth_idx].lower() in ["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]):
            
            # First storage capacity and type
            first_capacity = re.search(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', tokens[first_idx], re.IGNORECASE)
            first_capacity_value = first_capacity.group(1) + first_capacity.group(2).upper()
            first_type = tokens[second_idx].upper() if tokens[second_idx].lower() not in ["local", "locstorage"] else "LOCAL STORAGE"
            
            # Second storage capacity and type
            second_capacity = re.search(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', tokens[third_idx], re.IGNORECASE)
            second_capacity_value = second_capacity.group(1) + second_capacity.group(2).upper()
            second_type = tokens[fourth_idx].upper() if tokens[fourth_idx].lower() not in ["local", "locstorage"] else "LOCAL STORAGE"
            
            # Store both capacities and types
            result["storage_capacity1"] = first_capacity_value
            result["storage_type1"] = first_type
            result["storage_capacity2"] = second_capacity_value
            result["storage_type2"] = second_type
            
            print(f"Processed dual storage: {first_capacity_value} {first_type} and {second_capacity_value} {second_type}")
            return result
            
    # Check for compact dual storage configuration (e.g., "128GBSSD/1TBHDD")
    if len(flat_indices) >= 3:
        first_idx, slash_idx, second_idx = flat_indices[:3]
        
        if (slash_idx < len(tokens) and tokens[slash_idx] == '/' and
            first_idx < len(tokens) and second_idx < len(tokens)):
            
            # Check first token for SIZE+UNIT+TYPE pattern
            first_pattern = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[first_idx], re.IGNORECASE)
            # Check second token for SIZE+UNIT+TYPE pattern
            second_pattern = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)(ssd|hdd|nvme|emmc|storage)$', tokens[second_idx], re.IGNORECASE)
            
            if first_pattern and second_pattern:
                # First storage capacity and type
                first_capacity_value = first_pattern.group(1) + first_pattern.group(2).upper()
                first_type = first_pattern.group(3).upper()
                
                # Second storage capacity and type
                second_capacity_value = second_pattern.group(1) + second_pattern.group(2).upper()
                second_type = second_pattern.group(3).upper()
                
                # Store both capacities and types
                result["storage_capacity1"] = first_capacity_value
                result["storage_type1"] = first_type
                result["storage_capacity2"] = second_capacity_value
                result["storage_type2"] = second_type
                
                print(f"Processed compact dual storage: {first_capacity_value} {first_type} and {second_capacity_value} {second_type}")
                return result
    
    # ENHANCED: Handle multi-capacity slash-separated patterns with improved logic
    slash_count = sum(1 for idx in flat_indices if idx < len(tokens) and tokens[idx] == '/')
    if slash_count >= 1:
        if hasattr(self, 'logger') and self.logger:
            self.logger.debug(f"Storage: Processing slash-separated sequence with {slash_count} slashes")
        
        # IMPROVED: Collect numbers and determine unit pattern more carefully
        numbers = []
        unit = None
        storage_type = None
        
        # STEP 1: Go through tokens in order and collect all data
        for idx in flat_indices:
            if idx < len(tokens):
                token = tokens[idx]
                if hasattr(self, 'logger') and self.logger:
                    self.logger.debug(f"Storage: Processing token at index {idx}: '{token}'")
                
                # Skip slashes
                if token == '/':
                    continue
                    
                # PATTERN A: Standalone number
                if re.match(r'^\d+(?:\.\d+)?$', token):
                    numbers.append(token)
                    print(f"Found standalone number: {token}")
                
                # PATTERN B: Number with unit (each number has its own unit)
                elif re.match(r'^\d+(?:\.\d+)?(gb|tb|mb)$', token, re.IGNORECASE):
                    unit_match = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', token, re.IGNORECASE)
                    if unit_match:
                        numbers.append(unit_match.group(1))
                        current_unit = unit_match.group(2).upper()
                        if unit is None:
                            unit = current_unit
                        elif unit != current_unit:
                            print(f"Warning: Mixed units found ({unit} vs {current_unit})")
                        print(f"Found number with unit: {unit_match.group(1)} + {current_unit}")
                
                # PATTERN C: Standalone unit (shared unit for all numbers)
                elif re.match(r'^(gb|tb|mb)$', token, re.IGNORECASE):
                    unit = token.upper()
                    print(f"Found shared unit: {unit}")
                
                # PATTERN D: Storage type
                elif token.lower() in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                    if token.lower() in ["local", "locstorage"]:
                        storage_type = "LOCAL STORAGE"
                    else:
                        storage_type = token.upper()
                    print(f"Found storage type: {storage_type}")
        
        print(f"Collected: numbers={numbers}, unit={unit}, storage_type={storage_type}")
        
        # STEP 2: Handle special case where unit is attached to last number
        # This handles patterns like "8/16/32/128/256GB" where GB is attached to the last number
        if not unit and numbers:
            # Check if any of the original tokens has a number+unit pattern
            for idx in flat_indices:
                if idx < len(tokens):
                    token = tokens[idx]
                    # Look for pattern where unit is attached to a number
                    attached_unit_match = re.match(r'^(\d+(?:\.\d+)?)(gb|tb|mb)$', token, re.IGNORECASE)
                    if attached_unit_match:
                        # This number already has the unit attached
                        attached_number = attached_unit_match.group(1)
                        unit = attached_unit_match.group(2).upper()
                        print(f"Found unit attached to number: {attached_number}{unit}")
                        
                        # Make sure this number is in our numbers list, if not add it
                        if attached_number not in numbers:
                            numbers.append(attached_number)
                            print(f"Added attached number to list: {attached_number}")
                        break
        
        print(f"Final extracted: numbers={numbers}, unit={unit}, storage_type={storage_type}")
        
        # FALLBACK: If we didn't find enough numbers, try using full title pattern matching
        if len(numbers) < 2:
            # Look at the whole tokens list to see if there's a larger pattern we missed
            full_text = " ".join(tokens)
            print(f"Trying fallback pattern matching on full text")
            
            # Look for patterns like "16/32/64/128 GB" even if they're split up
            fallback_pattern = re.search(r'(\d+)\s*/\s*(\d+)(?:\s*/\s*(\d+))?(?:\s*/\s*(\d+))?(?:\s*/\s*(\d+))?\s*(?:gb|tb|mb)?', full_text, re.IGNORECASE)
            
            if fallback_pattern:
                new_numbers = []
                for i in range(1, 6):  # Check all capture groups
                    if fallback_pattern.group(i):
                        new_numbers.append(fallback_pattern.group(i))
                
                if len(new_numbers) > len(numbers):
                    print(f"Found better pattern via fallback: {new_numbers}")
                    numbers = new_numbers
                    
                    # If we don't have a unit yet, try to find it
                    if not unit:
                        unit_match = re.search(r'(?:gb|tb|mb)', full_text, re.IGNORECASE)
                        if unit_match:
                            unit = unit_match.group(0).upper()
                            print(f"Found unit via fallback: {unit}")
                            
        # STEP 3: Create numbered storage capacity fields for all numbers found
        if len(numbers) >= 1:
            if unit:
                print(f"Creating {len(numbers)} storage capacity fields with unit {unit}")
                for i, number in enumerate(numbers, 1):
                    capacity_key = f"storage_capacity{i}"
                    result[capacity_key] = f"{number}{unit}"
                    print(f"Set {capacity_key} to {result[capacity_key]}")
            else:
                print(f"Creating {len(numbers)} storage capacity fields without unit")
                for i, number in enumerate(numbers, 1):
                    capacity_key = f"storage_capacity{i}"
                    result[capacity_key] = number
                    print(f"Set {capacity_key} to {result[capacity_key]} (no unit found)")
            
            if storage_type:
                result["storage_type"] = storage_type
                print(f"Set storage_type to {storage_type}")
            
            return result
    
    # Find storage type and capitalize it
    storage_type = None
    for idx in flat_indices:
        if idx < len(tokens):
            lower_token = tokens[idx].lower()
            if lower_token in ["ssd", "hdd", "nvme", "m.2", "emmc", "storage", "local", "locstorage"]:
                if lower_token in ["local", "locstorage"]:
                    storage_type = "LOCAL STORAGE"
                else:
                    storage_type = lower_token.upper()
                break
            elif lower_token in ["ssds", "hdds"]:
                storage_type = lower_token[:-1].upper()
                break
            elif lower_token in ["ide", "sata", "scsi", "ata"]:
                storage_type = "HDD"
                break
    
    # Handle simple patterns - single capacity + type
    for idx in flat_indices:
        if idx < len(tokens) and re.search(r'^\d+(?:\.\d+)?(gb|tb)$', tokens[idx].lower(), re.IGNORECASE):
            value = re.sub(r'(gb|tb)$', lambda m: m.group(1).upper(), tokens[idx].lower())
            result["storage_capacity1"] = value
            print(f"Set storage_capacity1 to {value}")
            if storage_type:
                result["storage_type"] = storage_type
                print(f"Set storage_type to {storage_type}")
            break
    
    return result
    
# Configuration for Storage extractors
extractor_config = [
    {
        "name": "storage_capacity2",
        "patterns": [
            # Extract second storage capacity in dual-storage setups
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=False),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=False),
             str_pat("/", optional=False, show=False),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=False)]
        ],
        "multiple": False,
        "class": EnhancedStorageExtractor,
        "output_options": {
            "include_unit": True
        }
    },
    {
        "name": "storage_capacity",
        "patterns": [
            # Dual storage patterns - these have highest priority
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True),
             str_pat("/", optional=False, show=True),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True)],
            
            [regex_pat(r"\b[0-9]+\.?\d*(mb|gb|tb)(ssd|hdd|nvme|storage|local|locstorage)\b", show=True),
             str_pat("/", optional=False, show=True),
             regex_pat(r"\b[0-9]+\.?\d*(mb|gb|tb)(ssd|hdd|nvme|storage|local|locstorage)\b", show=True)],
             
            # NEW: Handle patterns like "128/256GB SSD" where first number has no unit
            [regex_pat(r"\b[0-9]+\b", show=True),
             str_pat("/", optional=False, show=True),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True)],
             
            # NEW: Handle patterns like "128GB/256GB SSD" where both numbers have units
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             str_pat("/", optional=False, show=True),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True)],
             
            # Standard single storage patterns
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True)],
            [list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=True),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=True)],
            [regex_pat(r"\b[0-9]+\.?\d*(mb|gb|tb)(ssd|hdd|nvme|storage|local|locstorage)\b", show=True)],
            [regex_pat(r"\b(ssd|hdd|nvme|storage|local|locstorage)[0-9]+\.?\d*(mb|gb|tb)\b", show=True)],
            [regex_pat(r"\b[2-9][0-9]{2,}\s*(mb|gb|tb)\b", show=True)],
            [regex_pat(r"\b1[0-9]{2,}\s*(mb|gb|tb)\b", show=True)],
            [regex_pat(r"(?:\[\d+\]|\d+\s*x\s*)\d+\.?\d*\s*(mb|gb|tb)(?!e)", show=True),
             list_pat(["ssd", "hdd", "nvme", "m.2", "local", "locstorage"], optional=False, show=True)],
            [regex_pat(r"\d+\.?\d*\s*(mb|gb|tb)(?!e)", show=True),
             str_pat("storage", optional=False, show=True)],
            [regex_pat(r"\d+\.?\d*\s*(mb|gb|tb)(?!e)", show=True),
             str_pat("nvme", optional=False, show=True)],
            [regex_pat(r"\d+\.?\d*\s*(mb|gb|tb)(?!e)", show=True),
             list_pat(["ssds", "hdds"], optional=False, show=True)],
            [str_pat("N/A", show=True)],
            [str_pat("No", show=True)],
            [list_pat(["ssd", "hdd", "storage", "local", "locstorage"], show=True), 
             str_pat("No", show=True)],
            [list_pat(["ssd", "hdd", "storage", "local", "locstorage"], show=True), 
             str_pat("None", show=True)]
        ],
        "multiple": True,
        "class": EnhancedStorageExtractor,
        "output_options": {
            "include_unit": True
        }
    },
    {
        "name": "storage_type",
        "patterns": [
            [list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "local", "locstorage"], show=True)]
        ],
        "multiple": False,
        "consume_on_match": True,
        "class": StorageExtractor  # CHANGED: Use base StorageExtractor instead of EnhancedStorageExtractor
    },
    {
        "name": "storage_type1",
        "patterns": [
            # Extract storage type for first storage device in dual-storage setup
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=False),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "local", "locstorage"], optional=False, show=True),
             str_pat("/", optional=False, show=False),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=False),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=False)]
        ],
        "multiple": False,
        "class": EnhancedStorageExtractor
    },
    {
        "name": "storage_type2",
        "patterns": [
            # Extract storage type for second storage device in dual-storage setup
            [regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=False),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "storage", "local", "locstorage"], optional=False, show=False),
             str_pat("/", optional=False, show=False),
             regex_pat(r"\b[0-9]+\.?\d*\s*(mb|gb|tb)\b", show=False),
             list_pat(["ssd", "ssds", "hdd", "hdds", "nvme", "m.2", "emmc", "local", "locstorage"], optional=False, show=True)]
        ],
        "multiple": False,
        "class": EnhancedStorageExtractor
    },
    {
        "name": "storage_drive_count",
        "patterns": [
            [regex_pat(r"(?:\[\d+\]|\d+\s*x\s*)", show=True),
             regex_pat(r"\d+\.?\d*(mb|gb|tb)", show=False)]
        ],
        "multiple": False,
        "class": StorageExtractor  # CHANGED: Use base StorageExtractor
    },
    {
        "name": "storage_individual_capacity",
        "patterns": [
            [regex_pat(r"(?:\[\d+\]|\d+\s*x\s*)(\d+\.?\d*(mb|gb|tb))", show=True)]
        ],
        "multiple": True,
        "class": StorageExtractor  # CHANGED: Use base StorageExtractor
    },
    {
        "name": "storage_drive_size",
        "patterns": [
            [list_pat(["2.5in", "3.5in"], show=True)]
        ],
        "multiple": False,
        "class": StorageExtractor  # CHANGED: Use base StorageExtractor
    },
    {
        "name": "storage_range", 
        "patterns": [
            # Pattern for both units: "250GB-500GB"
            [regex_pat(r"\d+(?:\.\d+)?(gb|tb|mb)-\d+(?:\.\d+)?(gb|tb|mb)", show=True)],
            # Pattern for second unit only: "250-500GB"
            [regex_pat(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?(gb|tb|mb)", show=True)]
        ],
        "multiple": False,
        "class": EnhancedStorageExtractor,
    }
]