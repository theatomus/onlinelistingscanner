# extractor_network_adapter.py
from configs.parser import BaseExtractor
import re

class NetworkAdapterExtractor(BaseExtractor):
    """Extractor for network adapter information, parsing brand, series, speed, ports, and form factor."""
    
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        self.logger = logger
    
    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process network adapter matches, extracting detailed information."""
        result = {}
        matched_text = " ".join([tokens[i] for i in match_indices])
        
        if self.logger:
            self.logger.debug(f"NetworkAdapter: Processing match text: '{matched_text}'")
        
        # Strict context validation - require explicit network adapter context
        context_window = 10
        adapter_required_keywords = ["adapter", "nic", "network", "ethernet", "infiniband", "smartnic", "dpu", "hba", "cna"]
        exclude_keywords = ["cpu", "processor", "core", "memory", "ram", "storage", "ssd", "hdd", "display", "graphics", "gpu"]
        
        # Build context string
        start_idx = max(0, min(match_indices) - context_window)
        end_idx = min(len(tokens), max(match_indices) + context_window)
        context_tokens = tokens[start_idx:end_idx]
        context_str = " ".join(context_tokens).lower()
        
        # Check for adapter context
        has_adapter_context = any(keyword in context_str for keyword in adapter_required_keywords)
        has_exclude_context = any(keyword in context_str for keyword in exclude_keywords)
        
        # Strict validation - reject if no adapter context or has exclude context
        if not has_adapter_context or has_exclude_context:
            if self.name not in ["adapter_spec", "adapter_type"]:  # These may have their own context
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting match due to missing context or exclusion context")
                return {}
        
        # Extract adapter brand
        if self.name == "adapter_brand":
            # Additional validation for brands
            if "controller" not in context_str and "adapter" not in context_str and "nic" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_brand due to missing controller/adapter/nic context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract adapter series
        elif self.name == "adapter_series":
            # Must have brand context
            brand_keywords = ["mellanox", "intel", "broadcom", "chelsio", "solarflare"]
            if not any(brand in context_str for brand in brand_keywords):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_series due to missing brand context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract adapter technology/speed
        elif self.name == "adapter_speed":
            # Exclude RAM/storage patterns
            if re.search(r"\d+\s*[GT]B\s*(RAM|Memory|DDR|storage|SSD|HDD)", matched_text, re.I):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_speed due to RAM/storage pattern match")
                return {}
            # Must have speed context
            if "gbe" not in matched_text.lower() and "gigabit" not in matched_text.lower() and \
               not re.search(r"[EQF]DR", matched_text):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_speed due to missing speed context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract port configuration
        elif self.name == "adapter_ports":
            # Must be in adapter context
            if "adapter" not in context_str and "nic" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_ports due to missing adapter/nic context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract form factor
        elif self.name == "adapter_form_factor":
            # Must have PCIe or form factor context
            if not re.search(r"(pcie|pci\s*express|low\s*profile|full\s*height|ocp|mezzanine)", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_form_factor due to missing PCIe/form factor context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract interface type
        elif self.name == "adapter_interface":
            # Must be network interface
            if not re.search(r"(sfp|qsfp|rj45|pcie|ethernet|fiber|copper)", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_interface due to missing interface context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract model number
        elif self.name == "adapter_model":
            # Must have brand and adapter context
            if "adapter" not in context_str and "nic" not in context_str:
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_model due to missing adapter/nic context")
                return {}
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Extract adapter type
        elif self.name == "adapter_type":
            result[self.name] = matched_text.strip()
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        # Handle full adapter specification
        elif self.name == "adapter_spec":
            # Must have comprehensive adapter context
            if not re.search(r"(mellanox|intel|broadcom).*(adapter|nic).*(connectx|x\d{3})", context_str):
                if self.logger:
                    self.logger.debug(f"NetworkAdapter: Rejecting adapter_spec due to missing comprehensive context")
                return {}
            result[self.name] = matched_text
            if self.logger:
                self.logger.debug(f"NetworkAdapter: Extracted {self.name}: {result[self.name]}")
            
        return result

# Helper functions
def str_pat(value, optional=False, show=True):
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

# Configuration for network adapter extractors
extractor_config = [
    {
        "name": "adapter_brand",
        "patterns": [
            # Require explicit adapter context
            [list_pat(["Network", "Ethernet", "InfiniBand", "NIC"], optional=False, show=False),
             list_pat(["Adapter", "Controller", "Card"], optional=False, show=False),
             list_pat(["Mellanox", "Intel", "Broadcom", "Chelsio", "Solarflare", "Xilinx", "Marvell", "Realtek", "Aquantia", "Qualcomm", "NVIDIA"], optional=False, show=True)],
            [list_pat(["Mellanox", "Intel", "Broadcom", "Chelsio", "Solarflare"], optional=False, show=True),
             list_pat(["NIC", "Network", "Ethernet", "InfiniBand"], optional=False, show=False),
             list_pat(["Adapter", "Controller", "Card"], optional=False, show=False)]
        ],
        "multiple": False,
        "class": NetworkAdapterExtractor,
    },
    {
        "name": "adapter_series",
        "patterns": [
            # Mellanox ConnectX series - must follow brand
            [list_pat(["Mellanox"], optional=False, show=False),
             regex_pat(r"ConnectX-\d+", optional=False, show=True)],
            
            # Intel series - must follow brand
            [list_pat(["Intel"], optional=False, show=False),
             regex_pat(r"[XIE]\d{3}", optional=False, show=True)],
            
            # Broadcom series - must follow brand
            [list_pat(["Broadcom"], optional=False, show=False),
             regex_pat(r"NetXtreme", optional=False, show=True)]
        ],
        "multiple": False,
        "class": NetworkAdapterExtractor,
    },
    {
        "name": "adapter_speed",
        "patterns": [
            # InfiniBand speeds - require IB context
            [list_pat(["InfiniBand", "IB"], optional=False, show=False),
             regex_pat(r"[EQF]DR", optional=False, show=True)],
            
            # Ethernet speeds - require network context
            [list_pat(["Ethernet", "Network", "NIC"], optional=False, show=False),
             regex_pat(r"\d+GbE", optional=False, show=True)],
            [regex_pat(r"\d+\s*Gigabit", optional=False, show=True),
             list_pat(["Ethernet", "Network"], optional=False, show=False)]
        ],
        "multiple": True,
        "class": NetworkAdapterExtractor,
    },
    {
        "name": "adapter_ports",
        "patterns": [
            # Must be in adapter context
            [list_pat(["Adapter", "NIC", "Network"], optional=False, show=False),
             list_pat(["Single Port", "Dual Port", "Quad Port", "Octal Port"], optional=False, show=True)],
            [regex_pat(r"\d+\s*Port", optional=False, show=True),
             list_pat(["Adapter", "NIC", "Network"], optional=False, show=False)]
        ],
        "multiple": False,
        "class": NetworkAdapterExtractor,
    },
    {
        "name": "adapter_form_factor",
        "patterns": [
            # PCIe context required
            [regex_pat(r"PCIe\s*x\d+", optional=False, show=True),
             list_pat(["Adapter", "NIC", "Card"], optional=False, show=False)],
            [list_pat(["Low Profile", "Full Height", "Half Height"], optional=False, show=True),
             list_pat(["Adapter", "NIC", "Bracket"], optional=False, show=False)]
        ],
        "multiple": True,
        "class": NetworkAdapterExtractor,
    }
]