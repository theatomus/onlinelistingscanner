import re
import logging
from typing import List, Set, Dict
from .parser import BaseExtractor  # Import BaseExtractor from parser.py in the same configs directory

# Comprehensive series list for Samsung, Apple, Google, OnePlus
brand_series = {
    "Samsung": [
        "Galaxy S", "Galaxy Note", "Galaxy A", "Galaxy Z", "Galaxy Tab", 
        "Galaxy M", "Galaxy J", "Galaxy F", "Galaxy Xcover", "Galaxy Fold"
    ],
    "Apple": ["iPhone", "iPad"],
    "Google": ["Pixel", "Pixel Tablet"],
    "OnePlus": ["OnePlus", "Nord"]
}

# Comprehensive color list across Samsung, Apple, Google, OnePlus
phone_colors = [
    "Black", "White", "Silver", "Gold", "Blue", "Red", "Green", "Purple", "Yellow", 
    "Rose Gold", "Phantom Black", "Phantom Silver", "Phantom Gray", "Phantom Grey", "Phantom White", 
    "Phantom Violet", "Phantom Pink", "Phantom Green", "Phantom Gold", "Mystic Bronze", 
    "Mystic Black", "Mystic White", "Mystic Gray", "Mystic Grey", "Mystic Green", "Aura Black", 
    "Aura White", "Aura Blue", "Aura Red", "Aura Glow", "Prism Black", "Prism White", 
    "Prism Blue", "Prism Green", "Cloud Blue", "Cloud Pink", "Cloud White", 
    "Cosmic Gray", "Cosmic Grey", "Cosmic Black", "Burgundy Red", "Lilac Purple", "Sky Blue", 
    "Graphite", "Titanium", "Amber Brown", "Crystal Blue", "Matte Black", 
    "Ceramic White", "Ceramic Black", "Oh So Orange", "Flamingo Pink", "Canary Yellow",
    "Space Gray", "Space Grey", "Midnight Black", "Jet Black", "Midnight", "Starlight", 
    "Product Red", "Pink", "Alpine Green", "Coral", "Deep Purple", "Space Black",
    "Just Black", "Clearly White", "Not Pink", "Kinda Blue", "Really Blue", 
    "Sorta Sage", "Barely Blue", "Stormy Black", "Sorta Sunny", 
    "Cloudy White", "Stormy Sky", "Sage", "Chalk", "Charcoal", "Hazel",
    "Frosted Silver", "Nebula Blue", "Glacial Green", "Marble White", 
    "Stellar Gray", "Stellar Grey", "Lunar Silver", "Astral Black", "Morning Mist", "Pine Green", 
    "Sandstone Black", "Jade Green", "Emerald Green", "Twilight", "Ocean Blue", 
    "Polar White", "Interstellar Glow", "Slate"
]

# Expanded list of U.S. network carriers
network_carriers = [
    "Verizon", "AT&T", "T-Mobile", "Dish Network", "Dish Mobile", "US Cellular", "Cricket", 
    "MetroPCS", "Metro", "Metro by T-Mobile", "Boost Mobile", "Mint Mobile", "Google Fi", 
    "Xfinity Mobile", "Spectrum Mobile", "Consumer Cellular", "Straight Talk", "Total by Verizon", 
    "Ting Mobile", "Republic Wireless", "H2O Wireless", "PureTalk", "Red Pocket Mobile", 
    "Ultra Mobile", "Tello Mobile", "Twigby", "TextNow", "Good2Go Mobile", "FreedomPop", 
    "Net10 Wireless", "Page Plus Cellular", "Simple Mobile"
]

def phone_extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
    """Extract phone information from tokens based on patterns defined in extractor_config."""
    results = []
    for pattern_set in self.patterns:
        match = self.match_pattern(tokens, pattern_set, consumed)
        if match:
            match_indices, consumed_indices = match
            results.append(match_indices)
            consumed.update(consumed_indices)
            if self.logger:
                self.logger.debug(f"PhoneExtractor matched indices: {match_indices}, consumed: {consumed_indices}")
            if not self.multiple:
                break
        else:
            if self.logger:
                self.logger.debug(f"PhoneExtractor pattern {pattern_set} did not match tokens: {tokens}")
    return results

def phone_extract_from_additional_info(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
    """Extract phone information from additional_info tokens without requiring brand/series."""
    results = []
    
    # Look for patterns that indicate phone/tablet info without requiring brand
    indicators = []
    
    # Look for WiFi
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        if token.lower() in ["wifi", "wi-fi"]:
            indicators.append(i)
            # Check if "Only" follows WiFi
            if i + 1 < len(tokens) and tokens[i + 1].lower() == "only":
                indicators.append(i + 1)
    
    # Look for unlocked patterns
    for i in range(len(tokens) - 1):
        if i in consumed or i + 1 in consumed:
            continue
        two_token = f"{tokens[i]} {tokens[i+1]}".lower()
        if two_token in ["network unlocked", "net unlocked", "carrier unlocked"]:
            indicators.extend([i, i+1])
        elif tokens[i].lower() == "unlocked":
            indicators.append(i)
    
    # Look for Apple model numbers
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        if re.match(r'^A\d{4}$', token):
            indicators.append(i)
    
    # Look for storage capacities (phone context allows smaller sizes)
    for i, token in enumerate(tokens):
        if i in consumed:
            continue
        if re.match(r"\d+(?:\.\d+)?(GB|TB|gb|tb)", token, re.IGNORECASE):
            storage_match = re.match(r"(\d+(?:\.\d+)?)(GB|TB|gb|tb)", token, re.IGNORECASE)
            number = int(float(storage_match.group(1)))
            unit = storage_match.group(2).upper()
            # Phone context allows smaller storage sizes (8GB+)
            if unit == "TB" or (unit == "GB" and number >= 8):
                indicators.append(i)
    
    # Look for phone colors
    for color in phone_colors:
        color_lower = color.lower()
        color_words = color_lower.split()
        
        if len(color_words) == 1:
            # Single word color
            for i, token in enumerate(tokens):
                if i in consumed:
                    continue
                if token.lower() == color_lower:
                    indicators.append(i)
        else:
            # Multi-word color
            for i in range(len(tokens) - len(color_words) + 1):
                if any(j in consumed for j in range(i, i + len(color_words))):
                    continue
                token_sequence = " ".join(tokens[i:i + len(color_words)]).lower()
                if token_sequence == color_lower:
                    indicators.extend(range(i, i + len(color_words)))
                    break
    
    if indicators:
        # Remove duplicates and sort
        indicators = sorted(list(set(indicators)))
        results.append(indicators)
        if self.logger:
            self.logger.debug(f"PhoneExtractor additional_info found indicators at: {indicators}")
    
    return results

def phone_process_additional_info_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
    """Process additional_info tokens into structured phone attribute data, focusing on phone-specific attributes only."""
    output = {}
    consumed = set()
    if self.logger:
        self.logger.debug(f"Processing additional_info match with tokens: {tokens}, match_indices: {match_indices}")

    # Extract Apple model numbers (A####) 
    apple_models = []
    for i in range(len(tokens)):
        if i in consumed:
            continue
        if re.match(r'^A\d{4}$', tokens[i]):
            apple_models.append(tokens[i])
            consumed.add(i)
            if self.logger:
                self.logger.debug(f"Extracted Apple model number: {tokens[i]}")
    
    if apple_models:
        if len(apple_models) == 1:
            output["phone_model"] = apple_models[0]
        else:
            for idx, model in enumerate(apple_models, 1):
                output[f"phone_model{idx}"] = model

    # ENHANCED: Extract storage capacities with numbered keys (ONLY if no storage already extracted)
    # Check if this function has access to previously extracted data to avoid duplication
    # For now, we'll extract storage but be conservative about when to do it
    storage_capacities_found = []
    
    # Only extract storage if we don't see evidence of slash-separated storage patterns
    # which should be handled by the main storage extractor
    has_slash_pattern = any('/' in token for token in tokens)
    
    if not has_slash_pattern:
        # Look for combined format like "32GB"
        for i in range(len(tokens)):
            if i in consumed:
                continue
            if re.match(r"(\d+(?:\.\d+)?)(GB|TB|gb|tb)", tokens[i], re.IGNORECASE):
                storage_match = re.match(r"(\d+(?:\.\d+)?)(GB|TB|gb|tb)", tokens[i], re.IGNORECASE)
                number = int(float(storage_match.group(1)))
                unit = storage_match.group(2).upper()
                # Phone context allows smaller storage sizes (8GB+)
                if unit == "TB" or (unit == "GB" and number >= 8):
                    capacity_value = f"{storage_match.group(1)}{unit}"
                    storage_capacities_found.append((capacity_value, [i]))
                    consumed.add(i)
                    if self.logger:
                        self.logger.debug(f"Extracted storage capacity from additional_info: {capacity_value}")
        
        # Look for separate format like "32 GB"
        for i in range(len(tokens) - 1):
            if i in consumed or i + 1 in consumed:
                continue
            if re.match(r"\d+(\.\d+)?", tokens[i]) and re.match(r"(GB|TB|gb|tb)", tokens[i + 1], re.IGNORECASE):
                number = int(float(tokens[i]))
                unit = tokens[i + 1].upper()
                if unit == "TB" or (unit == "GB" and number >= 8):
                    capacity_value = f"{tokens[i]}{unit}"
                    storage_capacities_found.append((capacity_value, [i, i + 1]))
                    consumed.add(i)
                    consumed.add(i + 1)
                    if self.logger:
                        self.logger.debug(f"Extracted storage capacity from additional_info: {capacity_value}")
        
        # Assign numbered storage capacity keys
        if storage_capacities_found:
            if len(storage_capacities_found) == 1:
                output["storage_size"] = storage_capacities_found[0][0]
            else:
                for idx, (capacity_text, indices) in enumerate(storage_capacities_found, 1):
                    output[f"storage_size{idx}"] = capacity_text
    else:
            if self.logger:
                self.logger.debug(f"Skipping storage extraction in phone extractor due to slash pattern detected (should be handled by storage extractor)")

    # ENHANCED: Extract multiple colors with numbered keys
    colors_found = []
    for color in phone_colors:
        color_lower = color.lower()
        color_words = color_lower.split()
        
        if len(color_words) == 1:
            # Single word color
            for i in range(len(tokens)):
                if i not in consumed and tokens[i].lower() == color_lower:
                    colors_found.append((tokens[i], [i]))
                    for idx in [i]:
                        consumed.add(idx)
                    if self.logger:
                        self.logger.debug(f"Extracted color from additional_info: {tokens[i]}")
        else:
            # Multi-word color
            for i in range(len(tokens) - len(color_words) + 1):
                if all(j not in consumed for j in range(i, i + len(color_words))):
                    token_sequence = " ".join(tokens[i:i + len(color_words)]).lower()
                    if token_sequence == color_lower:
                        color_text = " ".join(tokens[i:i + len(color_words)])
                        colors_found.append((color_text, list(range(i, i + len(color_words)))))
                        for j in range(i, i + len(color_words)):
                            consumed.add(j)
                        if self.logger:
                            self.logger.debug(f"Extracted color from additional_info: {color_text}")
                        break
    
    # Assign numbered color keys
    if colors_found:
        if len(colors_found) == 1:
            output["color"] = colors_found[0][0]
        else:
            for idx, (color_text, indices) in enumerate(colors_found, 1):
                output[f"color{idx}"] = color_text
    
    # ENHANCED: Extract multiple network statuses with numbered keys
    network_status_count = 0
    
    # Look for WiFi (enhanced to detect "WiFi Only" and treat standalone "WiFi" as "WiFi Only" in phone/tablet context)
    for i in range(len(tokens)):
        if i in consumed:
            continue
        if tokens[i].lower() in ["wifi", "wi-fi"]:
            # Check if "Only" follows WiFi
            if i + 1 < len(tokens) and tokens[i + 1].lower() == "only":
                network_status_count += 1
                output[f"network_status{network_status_count}"] = "WiFi Only"
                consumed.add(i)
                consumed.add(i + 1)
                if self.logger:
                    self.logger.debug(f"Extracted network_status{network_status_count} from additional_info: WiFi Only")
            else:
                # FIXED: In phone/tablet context, standalone "WiFi" typically means "WiFi Only"
                network_status_count += 1
                output[f"network_status{network_status_count}"] = "WiFi Only"
                consumed.add(i)
                if self.logger:
                    self.logger.debug(f"Extracted network_status{network_status_count} from additional_info: WiFi Only")
            break
    
    # Look for unlocked patterns
    unlocked_patterns = [
        ("network unlocked", 2),
        ("net unlocked", 2),
        ("carrier unlocked", 2),
        ("unlocked", 1)
    ]
    
    for pattern, token_count in unlocked_patterns:
        pattern_words = pattern.split()
        found = False
        
        for i in range(len(tokens) - token_count + 1):
            if all(j not in consumed for j in range(i, i + token_count)):
                token_sequence = " ".join(tokens[i:i + token_count]).lower()
                if token_sequence == pattern:
                    network_status_count += 1
                    if pattern == "network unlocked":
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    elif pattern == "net unlocked":
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    elif pattern == "carrier unlocked":
                        output[f"network_status{network_status_count}"] = "Carrier Unlocked"
                    else:
                        # FIXED: Map standalone "unlocked" to "Network Unlocked"
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    
                    for j in range(i, i + token_count):
                        consumed.add(j)
                    if self.logger:
                        self.logger.debug(f"Extracted network_status{network_status_count} from additional_info: {output[f'network_status{network_status_count}']}")
                    found = True
                    break
        
        if found:
            break

    if self.logger:
        self.logger.debug(f"Final additional_info output: {output}")
    return output
    
def phone_process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
    """Process matched tokens into structured phone attribute data, focusing on phone-specific attributes only."""
    output = {}
    consumed = set(match_indices)
    if self.logger:
        self.logger.debug(f"Processing match with tokens: {tokens}, match_indices: {match_indices}")

    # Extract brand and series from matched tokens (if found in patterns)
    if len(match_indices) >= 1:
        output["brand"] = tokens[match_indices[0]]  # e.g., "Samsung"
        if len(match_indices) >= 2:
            output["series"] = tokens[match_indices[1]]  # e.g., "Galaxy"
    if self.logger:
        self.logger.debug(f"Extracted brand: {output.get('brand')}, series: {output.get('series')}")

    # Extract Apple model numbers (A####) 
    apple_models = []
    for i in range(len(tokens)):
        if i in consumed:
            continue
        if re.match(r'^A\d{4}$', tokens[i]):
            apple_models.append(tokens[i])
            consumed.add(i)
            if self.logger:
                self.logger.debug(f"Extracted Apple model number: {tokens[i]}")
    
    if apple_models:
        if len(apple_models) == 1:
            output["phone_model"] = apple_models[0]
        else:
            for idx, model in enumerate(apple_models, 1):
                output[f"phone_model{idx}"] = model

    # ENHANCED: Extract multiple colors with numbered keys
    colors_found = []
    for color in phone_colors:
        color_lower = color.lower()
        color_words = color_lower.split()
        
        if len(color_words) == 1:
            # Single word color
            for i in range(len(tokens)):
                if i not in consumed and tokens[i].lower() == color_lower:
                    colors_found.append((tokens[i], [i]))
                    for idx in [i]:
                        consumed.add(idx)
                    if self.logger:
                        self.logger.debug(f"Extracted color: {tokens[i]}")
        else:
            # Multi-word color
            for i in range(len(tokens) - len(color_words) + 1):
                if all(j not in consumed for j in range(i, i + len(color_words))):
                    token_sequence = " ".join(tokens[i:i + len(color_words)]).lower()
                    if token_sequence == color_lower:
                        color_text = " ".join(tokens[i:i + len(color_words)])
                        colors_found.append((color_text, list(range(i, i + len(color_words)))))
                        for j in range(i, i + len(color_words)):
                            consumed.add(j)
                        if self.logger:
                            self.logger.debug(f"Extracted color: {color_text}")
                        break
    
    # Assign numbered color keys
    if colors_found:
        if len(colors_found) == 1:
            output["color1"] = colors_found[0][0]
        else:
            for idx, (color_text, indices) in enumerate(colors_found, 1):
                output[f"color{idx}"] = color_text

    # ENHANCED: Extract multiple network statuses with numbered keys
    network_status_count = 0
    
    # Look for WiFi (enhanced to detect "WiFi Only" and treat standalone "WiFi" as "WiFi Only" in phone/tablet context)
    for i in range(len(tokens)):
        if i in consumed:
            continue
        if tokens[i].lower() in ["wifi", "wi-fi"]:
            # Check if "Only" follows WiFi
            if i + 1 < len(tokens) and tokens[i + 1].lower() == "only":
                network_status_count += 1
                output[f"network_status{network_status_count}"] = "WiFi Only"
                consumed.add(i)
                consumed.add(i + 1)
                if self.logger:
                    self.logger.debug(f"Extracted network_status{network_status_count}: WiFi Only")
            else:
                # FIXED: In phone/tablet context, standalone "WiFi" typically means "WiFi Only"
                network_status_count += 1
                output[f"network_status{network_status_count}"] = "WiFi Only"
                consumed.add(i)
                if self.logger:
                    self.logger.debug(f"Extracted network_status{network_status_count}: WiFi Only")
            break
    
    # Look for unlocked patterns
    unlocked_patterns = [
        ("network unlocked", 2),
        ("net unlocked", 2),
        ("carrier unlocked", 2),
        ("unlocked", 1)
    ]
    
    for pattern, token_count in unlocked_patterns:
        pattern_words = pattern.split()
        found = False
        
        for i in range(len(tokens) - token_count + 1):
            if all(j not in consumed for j in range(i, i + token_count)):
                token_sequence = " ".join(tokens[i:i + token_count]).lower()
                if token_sequence == pattern:
                    network_status_count += 1
                    if pattern == "network unlocked":
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    elif pattern == "net unlocked":
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    elif pattern == "carrier unlocked":
                        output[f"network_status{network_status_count}"] = "Carrier Unlocked"
                    else:
                        # FIXED: Map standalone "unlocked" to "Network Unlocked"
                        output[f"network_status{network_status_count}"] = "Network Unlocked"
                    
                    for j in range(i, i + token_count):
                        consumed.add(j)
                    if self.logger:
                        self.logger.debug(f"Extracted network_status{network_status_count}: {output[f'network_status{network_status_count}']}")
                    found = True
                    break
        
        if found:
            break

    # Extract battery health
    for i in range(len(tokens) - 1):
        if i in consumed or i + 1 in consumed:
            continue
        if tokens[i].lower() == "battery" and tokens[i+1].lower() == "health":
            if i + 2 < len(tokens) and i + 2 not in consumed:
                output["battery_health"] = tokens[i + 2]
                consumed.add(i)
                consumed.add(i + 1)
                consumed.add(i + 2)
                if self.logger:
                    self.logger.debug(f"Extracted battery_health: {output['battery_health']}")
                break

    # Model is all remaining unconsumed tokens (excluding slashes and common separators)
    model_tokens = []
    for i in range(len(tokens)):
        if i not in consumed and tokens[i] not in ["/", "-", "|", ":", ";", "&"]:
            model_tokens.append(tokens[i])
    
    if model_tokens:
        output["model"] = " ".join(model_tokens)
        if self.logger:
            self.logger.debug(f"Extracted model: {output['model']}")

    if self.logger:
        self.logger.debug(f"Final output: {output}, consumed tokens: {consumed}")
    return output
    
class PhoneExtractor(BaseExtractor):
    """Extractor for phone attributes, handling brand, series, storage, color, network status, and battery health."""
    
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        self.logger = logger

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        return phone_extract(self, tokens, consumed)

    def extract_from_additional_info(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        return phone_extract_from_additional_info(self, tokens, consumed)

    def process_additional_info_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        return phone_process_additional_info_match(self, tokens, match_indices)

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        return phone_process_match(self, tokens, match_indices)

# Configuration for phone extractor
extractor_config = [
    {
        "name": "phone",
        "patterns": [
            [
                {"type": "regex", "pattern": r"\b(Samsung|Apple|Google|OnePlus)\b", "include_in_output": True},
                {"type": "regex", "pattern": r"\b(Galaxy|iPhone|iPad|Pixel|Nord|OnePlus)\b", "include_in_output": True}
            ],
            # ENHANCED: Additional pattern for Apple devices without explicit series match
            [
                {"type": "string", "value": "Apple", "include_in_output": True}
            ]
        ],
        "multiple": False,
        "class": PhoneExtractor
    }
]