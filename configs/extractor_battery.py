from configs.parser import BaseExtractor
import re

class BatteryExtractor(BaseExtractor):
    """Extractor for battery information, handling health and condition."""
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        
    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process battery-specific matches."""
        result = {}
        matched_text = " ".join([tokens[i] for i in match_indices])
        
        if self.logger:
            self.logger.debug(f"Battery: Processing match text: '{matched_text}'")
                
        # Extract battery health percentage
        if self.name == "battery_health":
            health_text = matched_text.lower()
            
            # Look for percentage indicators
            health_match = re.search(r'(\d+)%', health_text)
            if health_match:
                result["battery_health"] = f"{health_match.group(1)}%"
                if self.logger:
                    self.logger.debug(f"Battery: Extracted health percentage: {result['battery_health']}")
            else:
                result["battery_health"] = health_text
                if self.logger:
                    self.logger.debug(f"Battery: Using raw health text: {health_text}")
                
        # Extract battery condition qualitative assessment
        elif self.name == "battery_condition":
            condition_text = matched_text.lower()
            
            # Map condition terms to standardized assessments
            condition_mapping = {
                "good": "Good",
                "excellent": "Excellent",
                "fair": "Fair", 
                "poor": "Poor",
                "bad": "Bad",
                "replace": "Needs Replacement",
                "worn": "Worn",
                "new": "New"
            }
            
            for key, value in condition_mapping.items():
                if key in condition_text:
                    result["battery_condition"] = value
                    if self.logger:
                        self.logger.debug(f"Battery: Mapped condition '{key}' to '{value}'")
                    break
            else:
                result["battery_condition"] = matched_text
                if self.logger:
                    self.logger.debug(f"Battery: Using raw condition text: {matched_text}")
                
        # Handle battery presence indicators (positive only - negative moved to StatusExtractor)
        elif self.name == "battery_presence":
            # Standardize all matches to "With Battery" since patterns indicate presence
            result["battery_status"] = "With Battery"
            if self.logger:
                self.logger.debug(f"Battery: Setting battery_status to 'With Battery' from '{matched_text}'")
                
        return result

# Helper functions for pattern definitions
def str_pat(value, optional=False, show=True):
    """Defines a string pattern for exact text matching."""
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    """Defines a regex pattern for flexible matching."""
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    """Defines a list pattern to match any one of several values."""
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

# Configuration for battery extractors
extractor_config = [
    {
        "name": "battery_presence",
        "patterns": [
            [regex_pat(r"(?i)w/?\s*battery")],  # Updated pattern with case insensitivity
            [regex_pat(r"(?i)with\s+battery")],
            [regex_pat(r"(?i)battery\s+included")],
            [regex_pat(r"(?i)battery\s+present")]
        ],
        "multiple": False,
        "class": BatteryExtractor,
    },
    {
        "name": "battery_health",
        "patterns": [
            [regex_pat(r"(?i)battery\s*health\s*\d+%")],
            [regex_pat(r"(?i)\d+%\s*battery\s*health")],
            [regex_pat(r"(?i)battery\s*at\s*\d+%")]
        ],
        "multiple": False,
        "class": BatteryExtractor,
    },
    {
        "name": "battery_condition",
        "patterns": [
            [list_pat(["battery good", "good battery", "battery in good condition"])],
            [list_pat(["battery excellent", "excellent battery"])],
            [list_pat(["battery fair", "fair battery"])],
            [list_pat(["battery poor", "poor battery", "battery in poor condition"])],
            [list_pat(["battery bad", "bad battery"])],
            [list_pat(["battery needs replacement", "replace battery"])],
            [list_pat(["battery worn", "worn battery"])],
            [list_pat(["battery new", "new battery"])]
        ],
        "multiple": False,
        "class": BatteryExtractor,
    }
]