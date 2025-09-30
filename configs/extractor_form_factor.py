"""
Form factor extractor for PC desktops, laptops, and servers.
"""
import re
from typing import List, Dict, Any

class FormFactorExtractor:
    """Extractor for form factor information."""
    
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        self.name = config["name"]
        self.patterns = config.get("patterns", [])
        self.multiple = config.get("multiple", False)
        self.output_options = config.get("output_options", {"include_unit": True})
        self.logger = logger
        
    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict[str, Any]:
        """Process form factor matches, extracting detailed information."""
        result = {}
        matched_text = " ".join([tokens[i] for i in match_indices]).lower()
        
        if self.logger:
            self.logger.debug(f"FormFactor: Processing match text: '{matched_text}'")
        
        # Handle common form factor abbreviations
        form_factor_mapping = {
            "sff": "Small Form Factor (SFF)",
            "usff": "Ultra Small Form Factor (USFF)",
            "ssf": "Small Form Factor (SFF)",  # Common misspelling
            "usf": "Ultra Small Form Factor (USFF)",  # Common misspelling
            "atx": "ATX",
            "micro atx": "Micro ATX",
            "matx": "Micro ATX",
            "mini atx": "Mini ATX",
            "mini itx": "Mini ITX",
            "mitx": "Mini ITX",
            "eatx": "Extended ATX",
            "btx": "BTX",
            "micro btx": "Micro BTX",
            "mbtx": "Micro BTX",
            "mini btx": "Mini BTX",
            "dtx": "DTX",
            "mini dtx": "Mini DTX",
            "mdtx": "Mini DTX",
            "flex atx": "Flex ATX",
            "htpx": "HTPX",
            "tower": "Tower",
            "mini tower": "Mini Tower",
            "mid tower": "Mid Tower",
            "full tower": "Full Tower",
            "desktop": "Desktop",
            "slim": "Slim",
            "ultra slim": "Ultra Slim",
            "all in one": "All-In-One",
            "aio": "All-In-One",
            "1u": "1U Rack",
            "2u": "2U Rack",
            "3u": "3U Rack",
            "4u": "4U Rack",
            "5u": "5U Rack",
            "6u": "6U Rack",
            "blade": "Blade",
            "pizza box": "Pizza Box",
            "thin client": "Thin Client",
            "zero client": "Zero Client",
            "nuc": "NUC",
        }
        
        # Check for exact matches in our mapping
        for abbr, full_name in form_factor_mapping.items():
            if matched_text == abbr:
                result["form_factor"] = full_name
                if self.logger:
                    self.logger.debug(f"FormFactor: Exact match found: '{abbr}' -> '{full_name}'")
                return result
        
        # Check for form factor with parentheses: "Small Form Factor (SFF)"
        sff_match = re.match(r"(.*?)\s*\((.*?)\)", matched_text)
        if sff_match:
            full_name = sff_match.group(1).strip()
            abbr = sff_match.group(2).strip()
            
            # Verify both parts are valid
            if full_name in [v.lower() for v in form_factor_mapping.values()] or abbr.upper() in [k.upper() for k in form_factor_mapping.keys()]:
                if len(full_name) > len(abbr):
                    result["form_factor"] = full_name.title() + " (" + abbr.upper() + ")"
                    if self.logger:
                        self.logger.debug(f"FormFactor: Parentheses match found: '{full_name}' with abbreviation '{abbr}'")
                else:
                    # Find the full name for this abbreviation
                    for k, v in form_factor_mapping.items():
                        if k.upper() == abbr.upper():
                            result["form_factor"] = v
                            if self.logger:
                                self.logger.debug(f"FormFactor: Abbreviation match found: '{abbr}' -> '{v}'")
                            break
                return result
        
        # If no exact match, try partial matches (for cases like "Small Form Factor")
        for abbr, full_name in form_factor_mapping.items():
            if full_name.lower() in matched_text:
                result["form_factor"] = full_name
                if self.logger:
                    self.logger.debug(f"FormFactor: Partial match found: '{matched_text}' contains '{full_name.lower()}'")
                return result
        
        # If still no match, use the matched text as is
        result["form_factor"] = matched_text.title()
        if self.logger:
            self.logger.debug(f"FormFactor: No mapping found, using raw text: '{matched_text}'")
        return result

# Define patterns for form factor extraction
extractors = [
    {
        "name": "form_factor",
        "patterns": [
            # Common desktop form factors
            [{"value": "SFF", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "USFF", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "SSF", "optional": False, "show": True, "case_sensitive": False}],  # Common misspelling
            [{"value": "USF", "optional": False, "show": True, "case_sensitive": False}],  # Common misspelling
            [{"value": "Small Form Factor", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Ultra Small Form Factor", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Small", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Form", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Factor", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Ultra", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Small", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Form", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Factor", "optional": False, "show": True, "case_sensitive": False}],
            
            # Motherboard form factors
            [{"value": "ATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Micro", "optional": False, "show": True, "case_sensitive": False},
             {"value": "ATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "mATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mini", "optional": False, "show": True, "case_sensitive": False},
             {"value": "ATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mini", "optional": False, "show": True, "case_sensitive": False},
             {"value": "ITX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "mITX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Extended", "optional": False, "show": True, "case_sensitive": False},
             {"value": "ATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "EATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "BTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Micro", "optional": False, "show": True, "case_sensitive": False},
             {"value": "BTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "mBTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mini", "optional": False, "show": True, "case_sensitive": False},
             {"value": "BTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "DTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mini", "optional": False, "show": True, "case_sensitive": False},
             {"value": "DTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "mDTX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Flex", "optional": False, "show": True, "case_sensitive": False},
             {"value": "ATX", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "HTPX", "optional": False, "show": True, "case_sensitive": False}],
            
            # Case form factors
            [{"value": "Tower", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mini", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Tower", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Mid", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Tower", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Full", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Tower", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Desktop", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Slim", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Ultra", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Slim", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "All", "optional": False, "show": True, "case_sensitive": False},
             {"value": "In", "optional": False, "show": True, "case_sensitive": False},
             {"value": "One", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "All-In-One", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "AIO", "optional": False, "show": True, "case_sensitive": False}],
            
            # Server form factors
            [{"value": "1U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "2U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "3U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "4U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "5U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "6U", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Blade", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Pizza", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Box", "optional": False, "show": True, "case_sensitive": False}],
            
            # Other form factors
            [{"value": "Thin", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Client", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "Zero", "optional": False, "show": True, "case_sensitive": False},
             {"value": "Client", "optional": False, "show": True, "case_sensitive": False}],
            [{"value": "NUC", "optional": False, "show": True, "case_sensitive": False}],
        ],
        "multiple": False,
        "class": FormFactorExtractor,
    }
]