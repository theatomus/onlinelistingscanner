from configs.parser import BaseExtractor
from typing import Dict, List, Set, Any, Optional
import re

def str_pat(value, optional=False, show=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

class HDDExtractor(BaseExtractor):
    """Extractor specifically for Internal Hard Disk Drives."""
    
    def __init__(self, config, logger=None):
        super().__init__(config, logger)
        self.name = config["name"]
        self.multiple = config.get("multiple", False)
        self.consume_on_match = config.get("consume_on_match", True)
        self.device_types = config.get("device_types", [])
        self.logger = logger

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract HDD-related matches from tokenized text."""
        return hdd_extract(self, tokens, consumed)

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process matched tokens into a structured dictionary."""
        return hdd_process_match(self, tokens, match_indices)

def hdd_extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
    """Extract HDD-specific patterns."""
    results = []
    
    if hasattr(self, 'logger') and self.logger:
        self.logger.debug(f"HDD: Extractor '{self.name}' processing tokens: {tokens}")
        self.logger.debug(f"HDD: Already consumed: {consumed}")
    
    # Based on extractor name, extract different patterns
    if self.name == "hdd_interface":
        results.extend(extract_hdd_interface(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_form_factor":
        results.extend(extract_hdd_form_factor(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_rpm":
        results.extend(extract_hdd_rpm(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_transfer_rate":
        results.extend(extract_hdd_transfer_rate(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_model_number":
        results.extend(extract_hdd_model_number(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_part_number":
        results.extend(extract_hdd_part_number(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    elif self.name == "hdd_usage_hours":
        results.extend(extract_hdd_usage_hours(tokens, consumed, self.logger if hasattr(self, 'logger') else None))
    
    if hasattr(self, 'logger') and self.logger:
        self.logger.debug(f"HDD: Extractor '{self.name}' found {len(results)} matches: {results}")
    return results

def extract_hdd_interface(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD interface like 'SATA', 'IDE', 'SCSI', 'SAS'."""
    results = []
    interfaces = ['SATA', 'IDE', 'SCSI', 'SAS', 'PATA', 'ATA']
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Check for exact interface match
        if token.upper() in interfaces:
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found interface: {token} at index {i}")
        # Also check for interface embedded in longer strings like "SATA6Gb/s"
        elif any(interface in token.upper() for interface in interfaces):
            for interface in interfaces:
                if interface in token.upper():
                    results.append([i])
                    if logger:
                        logger.debug(f"HDD: Found interface in compound token: {interface} from {token} at index {i}")
                    break
    
    return results

def extract_hdd_form_factor(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD form factor like '3.5"', '2.5"'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Match form factor patterns
        if (re.match(r'^[23]\.5["\′]?$', token) or 
            re.match(r'^[23]\.5\s*inch$', token, re.IGNORECASE) or
            token.endswith('in') and re.match(r'^[23]\.5in$', token)):
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found form factor: {token} at index {i}")
    
    return results

def extract_hdd_rpm(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD rotational speed like '7200RPM', '5400 RPM'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Single token RPM (like "7200RPM")
        if re.match(r'^\d{4,5}RPM$', token, re.IGNORECASE):
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found RPM (single token): {token} at index {i}")
            continue
            
        # Two token RPM (like "7200 RPM")
        if (re.match(r'^\d{4,5}$', token) and 
            i + 1 < len(tokens) and 
            i + 1 not in consumed and
            tokens[i + 1].upper() == 'RPM'):
            results.append([i, i + 1])
            if logger:
                logger.debug(f"HDD: Found RPM (two tokens): {token} {tokens[i + 1]} at indices {i},{i+1}")
    
    return results

def extract_hdd_transfer_rate(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD transfer rate like '6Gb/s', '3Gb/s'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Match transfer rate patterns
        if re.match(r'^\d+(?:\.\d+)?[GM]?b/s$', token, re.IGNORECASE):
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found transfer rate: {token} at index {i}")
    
    return results

def extract_hdd_model_number(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD model numbers like 'HUS728T8TALN6L0'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        gpu_patterns = [
            r'^(quadro|gtx|rtx|geforce|radeon|rx)[a-z0-9]*$',
            r'^m\d{3,4}m$',  # mobile Quadro e.g. M1000M
            r'^p\d{3,4}$',    # NVIDIA P series
            r'^rtx_a\d{3,4}$'
        ]

        is_gpu = any(re.match(pat, token.lower()) for pat in gpu_patterns)

        if not is_gpu and (
            len(token) >= 8 and
            re.match(r'^[A-Z0-9]{8,}$', token, re.IGNORECASE) and
            re.search(r'[A-Z]', token, re.IGNORECASE) and
            re.search(r'[0-9]', token) and
            # Exclude obvious part numbers that start with 0
            not token.startswith('0')):
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found model number: {token} at index {i}")
    
    return results

def extract_hdd_part_number(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract HDD part numbers like '0B36431'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Match part number patterns (often start with 0, have mix of letters/numbers)
        if (re.match(r'^0[A-Z0-9]{5,}$', token, re.IGNORECASE) or
            re.match(r'^[A-Z]\d{5,}$', token, re.IGNORECASE)):
            results.append([i])
            if logger:
                logger.debug(f"HDD: Found part number: {token} at index {i}")
    
    return results

def extract_hdd_usage_hours(tokens: List[str], consumed: Set[int], logger=None) -> List[List[int]]:
    """Extract usage hours like '19 HRS', '1000 hrs'."""
    results = []
    
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
            
        # Match number + hours pattern
        if (re.match(r'^\d+$', token) and 
            i + 1 < len(tokens) and 
            i + 1 not in consumed and
            re.match(r'^(hours?|hrs?)$', tokens[i + 1], re.IGNORECASE)):
            results.append([i, i + 1])
            if logger:
                logger.debug(f"HDD: Found usage hours: {token} {tokens[i + 1]} at indices {i},{i+1}")
    
    return results

def hdd_process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
    logger = self.logger if hasattr(self, 'logger') else None
    """Process HDD matches into structured data."""
    result = {}
    
    if not match_indices:
        return result
    
    # Get the matched tokens
    matched_tokens = [tokens[i] for i in match_indices if i < len(tokens)]
    match_text = " ".join(matched_tokens)
    
    if logger:
        logger.debug(f"HDD: Processing match for {self.name}: {match_text}")
    
    if self.name == "hdd_interface":
        if matched_tokens:
            token = matched_tokens[0]
            # Extract just the interface type, cleaning any extra info
            interfaces = ['SATA', 'IDE', 'SCSI', 'SAS', 'PATA', 'ATA']
            for interface in interfaces:
                if interface in token.upper():
                    result["hdd_interface"] = interface
                    if logger:
                        logger.debug(f"HDD: Set hdd_interface to {interface}")
                    break
    
    elif self.name == "hdd_form_factor":
        if matched_tokens:
            form_factor = matched_tokens[0]
            # Normalize form factor
            if re.match(r'^[23]\.5', form_factor):
                if '3.5' in form_factor:
                    result["hdd_form_factor"] = '3.5"'
                elif '2.5' in form_factor:
                    result["hdd_form_factor"] = '2.5"'
            if logger:
                logger.debug(f"HDD: Set hdd_form_factor to {result.get('hdd_form_factor', form_factor)}")
    
    elif self.name == "hdd_rpm":
        if matched_tokens:
            # Extract RPM number
            rpm_text = "".join(matched_tokens).upper()
            rpm_match = re.search(r'(\d{4,5})', rpm_text)
            if rpm_match:
                result["hdd_rpm"] = f"{rpm_match.group(1)}RPM"
                if logger:
                    logger.debug(f"HDD: Set hdd_rpm to {result['hdd_rpm']}")
    
    elif self.name == "hdd_transfer_rate":
        if matched_tokens:
            transfer_rate = matched_tokens[0]
            result["hdd_transfer_rate"] = transfer_rate
            if logger:
                logger.debug(f"HDD: Set hdd_transfer_rate to {transfer_rate}")
    
    elif self.name == "hdd_model_number":
        if matched_tokens:
            model_number = matched_tokens[0]
            result["hdd_model_number"] = model_number
            if logger:
                logger.debug(f"HDD: Set hdd_model_number to {model_number}")
    
    elif self.name == "hdd_part_number":
        if matched_tokens:
            part_number = matched_tokens[0]
            result["hdd_part_number"] = part_number
            if logger:
                logger.debug(f"HDD: Set hdd_part_number to {part_number}")
    
    elif self.name == "hdd_usage_hours":
        if matched_tokens:
            # Extract just the number and format consistently
            hours_match = re.search(r'(\d+)', matched_tokens[0])
            if hours_match:
                result["hdd_usage_hours"] = f"{hours_match.group(1)} Hours"
                if logger:
                    logger.debug(f"HDD: Set hdd_usage_hours to {result['hdd_usage_hours']}")
    
    return result

# Configuration for HDD extractors
extractor_config = [
    {
        "name": "hdd_interface",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_form_factor",
        "patterns": [],
        "multiple": False, 
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_rpm",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_transfer_rate",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_model_number",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_part_number",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    },
    {
        "name": "hdd_usage_hours",
        "patterns": [],
        "multiple": False,
        "class": HDDExtractor,
        "device_types": ["Internal Hard Disk Drives"],
        "consume_on_match": True
    }
]