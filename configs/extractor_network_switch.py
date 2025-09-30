# extractor_network_switch.py
from configs.parser import BaseExtractor
import re

class NetworkSwitchExtractor(BaseExtractor):
    """Extractor for network switch information, parsing brand, series, ports, speed, and interface type."""
    
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        self.logger = logger
    
    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process network switch matches, extracting detailed information."""
        result = {}
        matched_text = " ".join([tokens[i] for i in match_indices])
        
        if self.logger:
            self.logger.debug(f"NetworkSwitch: Processing match text: '{matched_text}'")
        
        # Strict context validation
        context_window = 10
        switch_required_keywords = ["switch", "switching", "managed", "unmanaged", "layer", "ethernet switch", "network switch"]
        exclude_keywords = ["cpu", "processor", "memory", "ram", "storage", "ssd", "hdd", "display", "graphics", "gpu", "motherboard", "power"]
        
        # Build context string
        start_idx = max(0, min(match_indices) - context_window)
        end_idx = min(len(tokens), max(match_indices) + context_window)
        context_tokens = tokens[start_idx:end_idx]
        context_str = " ".join(context_tokens).lower()
        
        # Check for switch context
        has_switch_context = any(keyword in context_str for keyword in switch_required_keywords)
        has_exclude_context = any(keyword in context_str for keyword in exclude_keywords)
        
        # Strict validation
        if not has_switch_context or has_exclude_context:
            if self.name not in ["switch_spec", "switch_type"]:
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting match due to missing context or exclusion context")
                return {}
        
        # Extract switch brand
        if self.name == "switch_brand":
            # Must have switch context
            if "switch" not in context_str and "switching" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_brand due to missing switch context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract switch series/model
        elif self.name == "switch_series":
            # Must have brand context
            brand_keywords = ["juniper", "cisco", "arista", "netgear", "hp", "hpe", "dell", "mellanox", "brocade"]
            if not any(brand in context_str for brand in brand_keywords):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_series due to missing brand context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract port count
        elif self.name == "switch_ports":
            # Must be switch ports, not other ports
            if "switch" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_ports due to missing switch context")
                return {}
            # Exclude USB/display/other ports
            if re.search(r"(usb|hdmi|displayport|vga|audio)\s*port", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_ports due to non-network port context")
                return {}
            port_match = re.search(r"(\d+)", matched_text)
            if port_match:
                result[self.name] = port_match.group(1)
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract switch speed
        elif self.name == "switch_speed":
            # Exclude RAM/storage patterns
            if re.search(r"\d+\s*[GT]B\s*(RAM|Memory|DDR|storage|SSD|HDD)", matched_text, re.I):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_speed due to RAM/storage pattern match")
                return {}
            # Must have network speed indicator
            if not re.search(r"(gbe|gigabit|ethernet|mbps|gbps)", matched_text.lower()):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_speed due to missing speed indicator")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract interface type
        elif self.name == "switch_interface":
            # Must be network interface
            if not re.search(r"(ethernet|fiber|copper|sfp|qsfp|rj45)", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_interface due to missing interface context")
                return {}
            result[self.name] = matched_text.strip().upper()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract full model number
        elif self.name == "switch_model":
            # Must have switch and brand context
            if "switch" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_model due to missing switch context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract switch type
        elif self.name == "switch_type":
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Extract managed/unmanaged
        elif self.name == "switch_managed":
            if "switch" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_managed due to missing switch context")
                return {}
            if "managed" in matched_text.lower():
                result[self.name] = "Managed" if "unmanaged" not in matched_text.lower() else "Unmanaged"
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        # Handle full switch specification
        elif self.name == "switch_spec":
            # Must have comprehensive switch context
            if not re.search(r"(juniper|cisco|arista).*(switch).*(ex\d{4}|catalyst|nexus)", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkSwitch: Rejecting switch_spec due to missing comprehensive context")
                return {}
            result[self.name] = matched_text
            if self.logger:
                self.logger.debug(f"NetworkSwitch: Extracted {self.name}: {result[self.name]}")
            
        return result

# Helper functions with 'consume' parameter
def str_pat(value, optional=False, show=True, consume=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show, "consume": consume}

def regex_pat(pattern, optional=False, show=True, consume=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show, "consume": consume}

def list_pat(values, optional=False, show=True, consume=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show, "consume": consume}

# Configuration for network switch extractors with 'consume' parameter
extractor_config = [
    {
        "name": "switch_brand",
        "patterns": [
            # Require explicit switch context, but do not consume context tokens
            [list_pat(["Network", "Ethernet", "Managed", "Unmanaged"], optional=False, show=False, consume=False),
             str_pat("Switch", optional=False, show=False, consume=False),
             list_pat(["Juniper", "Cisco", "Arista", "Netgear", "HP", "HPE", "Dell", "Mellanox", "Brocade"], optional=False, show=True, consume=True)],
            [list_pat(["Juniper", "Cisco", "Arista", "Netgear"], optional=False, show=True, consume=True),
             list_pat(["Switch", "Switching"], optional=False, show=False, consume=False)]
        ],
        "multiple": False,
        "class": NetworkSwitchExtractor,
    },
    {
        "name": "switch_series",
        "patterns": [
            # Juniper series - must follow brand
            [list_pat(["Juniper"], optional=False, show=False, consume=False),
             regex_pat(r"[EQ]FX\d{4}", optional=False, show=True, consume=True)],
            # Cisco series - must follow brand
            [list_pat(["Cisco"], optional=False, show=False, consume=False),
             regex_pat(r"(Catalyst|Nexus)\s*\d+", optional=False, show=True, consume=True)]
        ],
        "multiple": False,
        "class": NetworkSwitchExtractor,
    },
    {
        "name": "switch_ports",
        "patterns": [
            # Must be in switch context, consume only the port number
            [list_pat(["Switch", "Ethernet", "Network"], optional=False, show=False, consume=False),
             regex_pat(r"\d+\s*Port", optional=False, show=True, consume=True)],
            [regex_pat(r"\d+-Port", optional=False, show=True, consume=True),
             str_pat("Switch", optional=False, show=False, consume=False)]
        ],
        "multiple": False,
        "class": NetworkSwitchExtractor,
    },
    {
        "name": "switch_speed",
        "patterns": [
            # Network-specific speeds only, consume the speed value
            [regex_pat(r"\d+\s*GbE", optional=False, show=True, consume=True),
             list_pat(["Switch", "Ethernet", "Port"], optional=False, show=False, consume=False)],
            [regex_pat(r"\d+\s*Gigabit", optional=False, show=True, consume=True),
             str_pat("Ethernet", optional=False, show=False, consume=False)],
            [regex_pat(r"10\/100\/1000", optional=False, show=True, consume=True)]
        ],
        "multiple": False,
        "class": NetworkSwitchExtractor,
    },
    {
        "name": "switch_interface",
        "patterns": [
            # Network interfaces only, consume the interface type
            [list_pat(["Switch", "Port"], optional=False, show=False, consume=False),
             list_pat(["SFP+", "SFP", "QSFP+", "QSFP", "RJ45", "Fiber", "Copper"], optional=False, show=True, consume=True)]
        ],
        "multiple": True,
        "class": NetworkSwitchExtractor,
    }
]