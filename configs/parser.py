import re
from typing import Dict, List, Any, Optional, Set, Tuple

class BaseExtractor:
    """Base class for extractors, handling pattern matching and output processing."""
    def __init__(self, config: Dict[str, Any], logger=None):
        """Initialize with a configuration dictionary.
        
        Args:
            config: Configuration dictionary with patterns and options
            logger: Optional logger instance for logging messages
        """
        self.name = config["name"]
        self.patterns = config["patterns"]
        self.multiple = config.get("multiple", False)
        self.output_options = config.get("output_options", {"include_unit": True})
        self.logger = logger
        # Prefer non-destructive parsing by default
        self.consume_on_match = config.get("consume_on_match", False)

    def match_pattern(self, tokens: List[str], pattern_set: List[Dict], consumed: Set[int]) -> Optional[Tuple[List[int], List[int]]]:
        """Find the first match for the pattern set in tokens, respecting consumed indices.
        
        Returns:
            Tuple of (match_indices, consumed_indices) where:
            - match_indices: Indices of tokens to include in the output.
            - consumed_indices: Indices of all tokens matched by the pattern.
        """
        for start in range(len(tokens)):
            if start in consumed:
                continue
            pos = start
            match_indices = []  # Tokens to include in output
            consumed_indices = []  # All matched tokens
            for pattern in pattern_set:
                while pos < len(tokens) and pos in consumed:
                    pos += 1
                if pos >= len(tokens):
                    if pattern.get("optional", False):
                        continue
                    return None
                token = tokens[pos].lower()
                matched = False
                if pattern["type"] == "string":
                    if token == pattern["value"].lower():
                        matched = True
                elif pattern["type"] == "regex":
                    if re.match(pattern["pattern"], token, re.IGNORECASE):
                        matched = True
                elif pattern["type"] == "list":
                    if any(value.lower() == token for value in pattern["values"]):
                        matched = True
                if matched:
                    if pattern.get("include_in_output", True):
                        match_indices.append(pos)
                    consumed_indices.append(pos)
                    pos += 1
                elif pattern.get("optional", False):
                    continue
                else:
                    break
            else:
                if match_indices or consumed_indices:
                    return match_indices, consumed_indices
        return None

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract matches from tokens based on patterns, updating consumed indices.
        
        Returns:
            List of match_indices for each successful match.
        """
        results = []
        for pattern_set in self.patterns:
            condition = pattern_set[-1].get("condition") if isinstance(pattern_set[-1], dict) else None
            patterns_to_match = pattern_set[:-1] if condition else pattern_set
            
            if self.logger:
                self.logger.debug(f"{self.name}: Applying pattern set to tokens")
                
            while True:
                match = self.match_pattern(tokens, patterns_to_match, consumed)
                if match is None:
                    break
                match_indices, consumed_indices = match
                
                if self.logger:
                    matched_tokens = [tokens[i] for i in match_indices]
                    self.logger.debug(f"{self.name}: Found match {matched_tokens} at indices {match_indices}")
                # Handle RAM/storage differentiation
                if condition in ["smaller_first", "larger_second"]:
                    capacities = [tokens[i] for i in match_indices if re.match(r"[0-9]+(gb|tb)", tokens[i], re.IGNORECASE)]
                    if len(capacities) == 2:
                        num1, unit1 = re.match(r"([0-9]+)(gb|tb)", capacities[0], re.IGNORECASE).groups()
                        num2, unit2 = re.match(r"([0-9]+)(gb|tb)", capacities[1], re.IGNORECASE).groups()
                        if unit1.lower() == unit2.lower():
                            num1, num2 = int(num1), int(num2)
                            if condition == "smaller_first" and num1 <= num2:
                                results.append([match_indices[0]])  # Take first capacity
                            elif condition == "larger_second" and num2 >= num1:
                                results.append([match_indices[1]])  # Take second capacity
                            consumed.update(consumed_indices)
                            if not self.multiple:
                                break
                            continue
                results.append(match_indices)
                # Only consume tokens if explicitly enabled on the extractor
                if getattr(self, 'consume_on_match', False):
                    consumed.update(consumed_indices)
                if not self.multiple:
                    break
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Convert matched indices into a dictionary."""
        value = " ".join(tokens[i] for i in match_indices)
        # Apply output options (e.g., remove unit if include_unit is False)
        if not self.output_options.get("include_unit", True):
            value = re.sub(r"(gb|tb|mhz|ghz|in)$", "", value, flags=re.IGNORECASE)
        return {self.name: value}

class CPUExtractor(BaseExtractor):
    """Extractor for CPU attributes, handling tokenized input."""
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract CPU matches, with special handling for generation."""
        results = super().extract(tokens, consumed)
        if self.name == "cpu_generation":
            for match_indices in results[:]:
                cpu_text = " ".join(tokens[i] for i in match_indices)
                model_match = re.match(r"i[3579]-([0-1]?[0-9])([0-9]{3,4})", cpu_text, re.IGNORECASE)
                if model_match:
                    gen = model_match.group(1)
                    results.append(match_indices)  # Retain original for context
                    consumed.update(match_indices)
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process CPU-specific matches."""
        value = " ".join(tokens[i] for i in match_indices)
        if not self.output_options.get("include_unit", True):
            value = re.sub(r"(ghz|mhz)$", "", value, flags=re.IGNORECASE)
        return {self.name: value}

class RAMExtractor(BaseExtractor):
    """Extractor for RAM attributes, handling capacity calculations and ranges."""
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract RAM matches, handling modules and ranges."""
        results = super().extract(tokens, consumed)
        processed_results = []
        for match_indices in results:
            value = " ".join(tokens[i] for i in match_indices)
            module_match = re.match(r"(\d+)x(\d+)(gb|tb)", value, re.IGNORECASE)
            if module_match:
                count = int(module_match.group(1))
                size = int(module_match.group(2))
                unit = module_match.group(3)
                total = count * size
                processed_results.append(match_indices)
                if getattr(self, 'consume_on_match', False):
                    consumed.update(match_indices)
            elif re.match(r"\d+(mb|gb|tb)\s*-\s*\d+\1", value, re.IGNORECASE):
                start, end, unit = re.match(r"(\d+)(mb|gb|tb)\s*-\s*(\d+)\2", value, re.IGNORECASE).groups()
                start, end = int(start), int(end)
                powers = [start]
                while start * 2 <= end:
                    start *= 2
                    powers.append(start)
                processed_results.append(match_indices)
                if getattr(self, 'consume_on_match', False):
                    consumed.update(match_indices)
            else:
                processed_results.append(match_indices)
        return processed_results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process RAM-specific matches."""
        value = " ".join(tokens[i] for i in match_indices)
        module_match = re.match(r"(\d+)x(\d+)(gb|tb)", value, re.IGNORECASE)
        if module_match:
            count = int(module_match.group(1))
            size = int(module_match.group(2))
            unit = module_match.group(3)
            total = count * size
            value = f"{total}{unit}"
        elif re.match(r"\d+(mb|gb|tb)\s*-\s*\d+\1", value, re.IGNORECASE):
            start, end, unit = re.match(r"(\d+)(mb|gb|tb)\s*-\s*(\d+)\2", value, re.IGNORECASE).groups()
            start, end = int(start), int(end)
            powers = [start]
            while start * 2 <= end:
                start *= 2
                powers.append(start)
            value = f"[{', '.join(map(str, powers))}]"
        if not self.output_options.get("include_unit", True):
            value = re.sub(r"(gb|tb|mhz|ghz|in)$", "", value, flags=re.IGNORECASE)
        return {self.name: value}

class StorageExtractor(BaseExtractor):
    """Extractor for Storage attributes, handling multi-drive calculations."""
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract storage matches, handling multi-drive setups."""
        results = super().extract(tokens, consumed)
        processed_results = []
        for match_indices in results:
            value = " ".join(tokens[i] for i in match_indices)
            multi_match = re.match(r"(\d+)\s*x\s*([0-9]+)(gb|tb)", value, re.IGNORECASE)
            if multi_match:
                count = int(multi_match.group(1))
                size = int(multi_match.group(2))
                unit = multi_match.group(3)
                total = count * size
                processed_results.append(match_indices)
                if getattr(self, 'consume_on_match', False):
                    consumed.update(match_indices)
            else:
                processed_results.append(match_indices)
        return processed_results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process storage-specific matches."""
        value = " ".join(tokens[i] for i in match_indices)
        multi_match = re.match(r"(\d+)\s*x\s*([0-9]+)(gb|tb)", value, re.IGNORECASE)
        if multi_match:
            count = int(multi_match.group(1))
            size = int(multi_match.group(2))
            unit = multi_match.group(3)
            total = count * size
            value = f"{total}{unit}"
        if not self.output_options.get("include_unit", True):
            value = re.sub(r"(gb|tb|mhz|ghz|in)$", "", value, flags=re.IGNORECASE)
        return {self.name: value}