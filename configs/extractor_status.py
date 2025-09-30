from configs.parser import BaseExtractor
import re

# Helper functions for pattern definitions
def str_pat(value, optional=False, show=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

class StatusExtractor(BaseExtractor):
    """Unified extractor for handling 'no/without' cases for various components."""
    
    def __init__(self, config, logger=None):
        super().__init__(config, logger)
        # Don't consume tokens so multiple status extractors can work on same tokens
        self.consume_on_match = False
        self.logger = logger
    
    def extract(self, tokens: list, consumed: set) -> list:
        """Override extract to handle parenthetical patterns and slash-separated missing components."""
        matches = []
        
        # Get component type from extractor name
        component_type = self.name.split('_')[0]  # storage, battery, os, etc.
        
        if self.logger:
            self.logger.debug(f"Status: Extracting status for component type: {component_type}")
        
        # Define component terms for each type
        component_terms = {
            # Added 'hard' so phrases like 'No Hard Drive' are recognised.
            "storage": ["ssd", "hdd", "hd", "storage", "drive", "hard", "m.2", "nvme", "emmc", "harddrive"],
            "battery": ["battery", "batt", "bat"],
            "os": ["os"]
        }
        
        terms = component_terms.get(component_type, [])
        if not terms:
            if self.logger:
                self.logger.debug(f"Status: No terms defined for component type: {component_type}")
            return matches
        
        # Join all tokens to look for patterns across token boundaries
        full_text = " ".join(tokens).lower()

        # SPECIAL-CASE DIRECTIVE: "No Primary Battery" should imply one battery is still included
        # Rationale: ThinkPad/EliteBook lines can have dual-battery setups; listings that say
        # "No Primary Battery" generally still include the secondary/internal battery,
        # so we normalize this case to "One Battery Included" instead of treating it as fully missing.
        # This must be detected before generic "no battery" rules to avoid overriding.
        if component_type == "battery":
            try:
                special_case_regex = re.compile(r"\b(?:no|without|missing)\b[^\w\n]{0,3}[^.,;:()]{0,80}\b(?:primary|main)\s+battery\b", re.IGNORECASE)
                sc_match = special_case_regex.search(full_text)
                if sc_match:
                    # Map character span back to token indices
                    start_pos = sc_match.start()
                    end_pos = sc_match.end()
                    current_pos = 0
                    start_token = None
                    end_token = None
                    for i, token in enumerate(tokens):
                        token_start = current_pos
                        token_end = current_pos + len(token)
                        if start_token is None and token_end > start_pos:
                            start_token = i
                        if token_start < end_pos:
                            end_token = i
                        current_pos = token_end + 1  # +1 for space
                    if start_token is not None and end_token is not None:
                        if start_token == end_token:
                            matches.append([start_token])
                            if self.logger:
                                self.logger.debug(f"Status: Special-case battery match (single token): '{tokens[start_token]}'")
                        else:
                            match_range = list(range(start_token, end_token + 1))
                            matches.append(match_range)
                            if self.logger:
                                self.logger.debug(
                                    f"Status: Special-case battery match (range): '{' '.join(tokens[i] for i in match_range)}'"
                                )
                        # Early return so generic rules don't also fire
                        return matches
            except Exception as e:
                if self.logger:
                    self.logger.debug(f"Status: Special-case 'no primary battery' detection skipped due to error: {e}")
        
        # Look for various "no" patterns
        patterns_to_check = [
            rf'\bno\s+({"|".join(terms)})\b',                    # "no ssd", "no os"
            rf'\bwithout\s+({"|".join(terms)})\b',               # "without ssd"
            rf'\bmissing\s+({"|".join(terms)})\b',               # "missing ssd"
            rf'\bno\s*\([^)]*({"|".join(terms)})[^)]*\)',        # "no (ssd/os)"
            rf'\([^)]*no[^)]*({"|".join(terms)})[^)]*\)',        # "(no ssd/os)"
            rf'\bno({"|".join(terms)})\b',                       # "nossd", "noos"
        ]
        
        for pattern in patterns_to_check:
            if re.search(pattern, full_text):
                # Find which tokens this spans
                match_obj = re.search(pattern, full_text)
                if match_obj:
                    if self.logger:
                        self.logger.debug(f"Status: Found pattern match: '{match_obj.group(0)}'")
                    start_pos = match_obj.start()
                    end_pos = match_obj.end()
                    
                    # Find token indices that correspond to this match
                    current_pos = 0
                    start_token = None
                    end_token = None
                    
                    for i, token in enumerate(tokens):
                        token_start = current_pos
                        token_end = current_pos + len(token)
                        
                        if start_token is None and token_end > start_pos:
                            start_token = i
                        if token_start < end_pos:
                            end_token = i
                        
                        current_pos = token_end + 1  # +1 for space
                    
                    if start_token is not None and end_token is not None:
                        if start_token == end_token:
                            matches.append([start_token])
                            if self.logger:
                                self.logger.debug(f"Status: Matched single token: '{tokens[start_token]}'")
                        else:
                            match_range = list(range(start_token, end_token + 1))
                            matches.append(match_range)
                            if self.logger:
                                self.logger.debug(f"Status: Matched token range: '{' '.join(tokens[i] for i in match_range)}'")
                        break  # Only match once per extractor
        
        # Additional handling for slash-separated negatives like: "No OS/SSD/Battery"
        # This ensures each component extractor (os/storage/battery) can match its term within the group.
        if not matches:
            slash_pat = re.compile(r"\bno\s+([^\s,;:]+)\b", re.IGNORECASE)
            for match_obj in slash_pat.finditer(full_text):
                group_text = match_obj.group(1)  # e.g., "os/ssd/battery"
                # Split on common separators used inline
                parts = re.split(r"[\/|]", group_text)
                # Normalize parts
                parts = [p.strip().lower() for p in parts if p.strip()]
                if any(p in terms for p in parts):
                    if self.logger:
                        self.logger.debug(f"Status: Slash-group matched terms {parts} for component '{component_type}'")

                    start_pos = match_obj.start()
                    end_pos = match_obj.end()

                    current_pos = 0
                    start_token = None
                    end_token = None
                    for i, token in enumerate(tokens):
                        token_start = current_pos
                        token_end = current_pos + len(token)
                        if start_token is None and token_end > start_pos:
                            start_token = i
                        if token_start < end_pos:
                            end_token = i
                        current_pos = token_end + 1  # +1 for space

                    if start_token is not None and end_token is not None:
                        if start_token == end_token:
                            matches.append([start_token])
                            if self.logger:
                                self.logger.debug(f"Status: Matched single token (slash-group): '{tokens[start_token]}'")
                        else:
                            match_range = list(range(start_token, end_token + 1))
                            matches.append(match_range)
                            if self.logger:
                                self.logger.debug(f"Status: Matched token range (slash-group): '{' '.join(tokens[i] for i in match_range)}'")
                        break  # Only match once per extractor

        return matches
    
    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process 'no/without' component matches with standardized output."""
        result = {}
        component_type = self.name.split('_')[0]  # Extract component type (storage, battery, os, etc.)
        
        # Combine all matched tokens to analyze
        match_text = " ".join(tokens[i] for i in match_indices).lower()
        
        if self.logger:
            self.logger.debug(f"Status: Processing match text: '{match_text}'")
        
        # Extract component subtype for storage
        if component_type == "storage":
            # Look for storage type in the matched text
            storage_type = None
            
            subtype_match = re.search(r'(ssd|hdd|storage|drive|m\.2|nvme|emmc)', match_text, re.IGNORECASE)
            if subtype_match:
                storage_type = subtype_match.group(0).upper()
            elif "harddrive" in match_text or "hard" in match_text:
                storage_type = "HDD"
            
            if storage_type:
                result["storage_type"] = storage_type
                if self.logger:
                    self.logger.debug(f"Status: Detected storage type: {storage_type}")
        
        # Battery special-case: phrases like "No Primary/Main Battery" imply a secondary/internal battery is present
        if component_type == "battery":
            if re.search(r"\b(?:no|without|missing)\b[^\w\n]{0,3}[^.,;:()]{0,80}\b(?:primary|main)\s+battery\b", match_text, re.IGNORECASE):
                result["battery_status"] = "One Battery Included"
                if self.logger:
                    self.logger.debug("Status: Special-case applied -> battery_status set to 'One Battery Included'")
                return result

        # Set the standard status for any component (default)
        result[f"{component_type}_status"] = "Not Included"
        if self.logger:
            self.logger.debug(f"Status: Set {component_type}_status to 'Not Included'")
        
        return result

# Configuration for Status extractors (handles no/without detection for all components)
extractor_config = [
    {
        "name": "storage_status",
        "patterns": [],  # We'll use custom logic instead of patterns
        "multiple": True,
        "class": StatusExtractor
    },
    {
        "name": "battery_status",
        "patterns": [],  # We'll use custom logic instead of patterns
        "multiple": False,
        "class": StatusExtractor
    },
    {
        "name": "os_status",
        "patterns": [],  # We'll use custom logic instead of patterns
        "multiple": False,
        "class": StatusExtractor
    },
    {
        "name": "bios_status",
        "patterns": [
            [regex_pat(r"bios\s*lock(?:ed)?", show=True)],
            [str_pat("BIOSLOCK", show=True)]
        ],
        "multiple": False,
        "class": BaseExtractor,
        "process_match": lambda tokens, indices: {"bios_status": "Locked BIOS"}
    }
]