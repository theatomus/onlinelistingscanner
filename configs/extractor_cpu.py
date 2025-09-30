import re
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
import logging
# when you have a solution, post the entire updated function in a python codeblock.
# Define the cpu_generations map for Intel Core i-series CPUs
cpu_generations = {
    "1st Gen": {
        "i3-3xx": ["", "M", "UM"],
        "i3-5xx": ["", "M", "UM"],
        "i5-5xx": ["", "M", "UM"],
        "i5-6xx": ["", "K", "S", "T", "M", "LM", "UM"],
        "i5-7xx": ["", "S", "T", "M", "LM", "UM"],
        "i7-6xx": ["", "M", "LM", "UM"],
        "i7-7xx": ["", "K", "S", "T", "M", "LM", "UM"],
        "i7-8xx": ["", "S", "T", "M", "LM", "UM"],
        "i7-9xx": ["", "XM"]
    },
    "2nd Gen": {
        "i3-21xx": ["", "T", "M"],
        "i3-23xx": ["", "T", "M", "UM"],
        "i5-23xx": ["", "K", "S", "T", "M"],
        "i5-24xx": ["", "K", "S", "T", "M"],
        "i5-25xx": ["", "K", "S", "T", "M"],
        "i7-26xx": ["", "K", "S", "T", "M", "QM"],
        "i7-27xx": ["", "K", "M", "QM"],
        "i7-28xx": ["", "QM", "XM"]
    },
    "3rd Gen": {
        "i3-31xx": ["", "T", "U", "M"],
        "i3-32xx": ["", "T", "M"],
        "i5-32xx": ["", "T", "M", "U"],
        "i5-33xx": ["", "K", "S", "T", "P", "M"],
        "i5-34xx": ["", "K", "S", "T", "M"],
        "i5-35xx": ["", "K", "S", "T", "M"],
        "i7-35xx": ["", "K", "U", "M", "QM"],
        "i7-36xx": ["", "K", "M", "QM"],
        "i7-37xx": ["", "K", "M", "QM"],
        "i7-38xx": ["", "QM", "XM"]
    },
    "4th Gen": {
        "i3-41xx": ["", "T", "U", "M"],
        "i3-43xx": ["", "T", "U", "M"],
        "i5-42xx": ["", "U", "M"],
        "i5-43xx": ["", "U", "M"],
        "i5-44xx": ["", "K", "S", "T"],
        "i5-45xx": ["", "K", "S", "T"],
        "i7-45xx": ["", "U", "M"],
        "i7-47xx": ["", "K", "M", "QM"],
        "i7-48xx": ["", "K", "M", "QM"],
        "i7-49xx": ["", "K", "M", "QM"]
    },
    "5th Gen": {
        "i3-50xx": ["", "U"],
        "i5-52xx": ["", "U"],
        "i5-53xx": ["", "T", "U"],
        "i7-55xx": ["", "U"],
        "i7-57xx": ["", "T", "S", "K"]
    },
    "6th Gen": {
        "i3-60xx": ["", "T", "U"],
        "i3-61xx": ["", "T", "U"],
        "i5-62xx": ["", "U"],
        "i5-63xx": ["", "U"],
        "i5-64xx": ["", "K", "T"],
        "i5-65xx": ["", "K", "T"],
        "i5-66xx": ["", "K"],
        "i7-65xx": ["", "U"],
        "i7-66xx": ["U", "HQ", "HK"],
        "i7-67xx": ["", "K", "HQ", "HK"],
        "i7-68xx": ["", "K"]
    },
    "7th Gen": {
        "i3-71xx": ["", "T", "U"],
        "i3-73xx": ["", "T", "U"],
        "i5-72xx": ["", "U"],
        "i5-73xx": ["", "U"],
        "i5-74xx": ["", "K", "T"],
        "i5-75xx": ["", "K", "T"],
        "i7-75xx": ["", "U"],
        "i7-77xx": ["", "K", "HQ", "HK"],
        "i7-78xx": ["", "K"]
    },
    "8th Gen": {
        "i3-81xx": ["", "U"],
        "i3-83xx": ["", "T", "U"],
        "i5-82xx": ["", "U"],
        "i5-83xx": ["", "T", "U", "H"],
        "i5-84xx": ["", "K", "T"],
        "i5-85xx": ["", "K"],
        "i7-85xx": ["", "U"],
        "i7-86xx": ["", "U"],
        "i7-87xx": ["", "K", "H"],
        "i7-88xx": ["", "X"]
    },
    "9th Gen": {
        "i3-91xx": ["", "F"],
        "i5-93xx": ["", "T", "U"],
        "i5-94xx": ["", "K", "F", "T"],
        "i5-95xx": ["", "K", "F"],
        "i7-97xx": ["", "K", "F"],
        "i7-98xx": ["", "K", "H", "HK"],
        "i9-99xx": ["", "K", "KF", "X"]
    },
    "10th Gen": {
        "i3-100xx": ["", "G1", "G4"],
        "i3-101xx": ["", "T", "U"],
        "i5-102xx": ["", "U"],
        "i5-103xx": ["", "G1", "G4", "G7"],
        "i5-104xx": ["", "K", "F", "T"],
        "i5-105xx": ["", "K", "F"],
        "i7-105xx": ["", "U"],
        "i7-106xx": ["", "G4", "G7"],
        "i7-107xx": ["", "K", "F"],
        "i7-108xx": ["", "H", "HK"]
    },
    "11th Gen": {
        "i3-111xx": ["", "G4"],
        "i3-113xx": ["", "G4"],
        "i5-112xx": ["", "G4", "G7"],
        "i5-113xx": ["", "G7"],
        "i5-114xx": ["", "K", "F", "T"],
        "i5-115xx": ["", "K", "G7"],
        "i7-116xx": ["", "G7"],
        "i7-117xx": ["", "K", "F"],
        "i7-118xx": ["", "H"]
    },
    "12th Gen": {
        "i3-121xx": ["", "U", "P"],
        "i3-123xx": ["", "T"],
        "i5-123xx": ["", "U", "P"],
        "i5-124xx": ["", "K", "F", "T", "U", "P"],
        "i5-125xx": ["", "K", "F", "U", "P"],
        "i5-126xx": ["", "H", "HX"],
        "i7-125xx": ["", "U", "P"],
        "i7-126xx": ["", "K", "F", "KS"],
        "i7-127xx": ["", "K", "F", "H", "HK", "HX"],
        "i9-129xx": ["", "K", "KF", "KS", "H", "HX"]
    },
    "13th Gen": {
        "i3-131xx": ["", "U", "P"],
        "i3-133xx": ["", "T"],
        "i3-134xx": ["", "F"],
        "i5-133xx": ["", "U", "P"],
        "i5-134xx": ["", "K", "F", "T", "U", "P"],
        "i5-135xx": ["", "K", "F", "H", "HX", "U", "P"],
        "i7-135xx": ["", "U", "P"],
        "i7-136xx": ["", "K", "F", "KS"],
        "i7-137xx": ["", "K", "F", "H", "HK", "HX"],
        "i9-139xx": ["", "K", "KF", "KS", "H", "HX"]
    },
    "14th Gen": {
        "i3-141xx": ["", "U"],
        "i3-143xx": ["", "F"],
        "i5-144xx": ["", "F", "T"],
        "i5-145xx": ["", "K", "F", "H", "HX", "U"],
        "i7-145xx": ["", "U"],
        "i7-146xx": ["", "K", "F", "KS"],
        "i7-147xx": ["", "K", "F", "H", "HK", "HX"],
        "i9-149xx": ["", "K", "KF", "KS", "H", "HX"]
    }
}

apple_m_generations = {
    "M1": "1st Gen Apple Silicon",
    "M2": "2nd Gen Apple Silicon",
    "M3": "3rd Gen Apple Silicon",
    "M1 Pro": "1st Gen Apple Silicon",
    "M1 Max": "1st Gen Apple Silicon",
    "M1 Ultra": "1st Gen Apple Silicon",
    "M2 Pro": "2nd Gen Apple Silicon",
    "M2 Max": "2nd Gen Apple Silicon",
    "M2 Ultra": "2nd Gen Apple Silicon",
    "M3 Pro": "3rd Gen Apple Silicon",
    "M3 Max": "3rd Gen Apple Silicon",
    "M3 Ultra": "3rd Gen Apple Silicon"
}


def get_apple_m_generation(chip_name):
    """Get generation for Apple M-series chips."""
    chip_upper = chip_name.upper()
    for m_chip, generation in apple_m_generations.items():
        if m_chip.upper() == chip_upper:
            return generation
    return None


# Helper functions for generation detection
def extract_gen_number(gen_str):
    """Extract the numeric part from a generation string (e.g., '6' from '6th Gen')."""
    match = re.match(r"(\d+)(?:st|nd|rd|th)?\s*Gen", gen_str, re.IGNORECASE)
    return match.group(1) if match else None


def get_generation_from_map(model_token, family):
    """Determine the generation of a CPU model token using the cpu_generations map."""
    if not family.startswith("Core i"):
        return None
    ix = family.split(' ')[1].lower()  # e.g., 'i7'
    for gen, models in cpu_generations.items():
        for model_pattern, suffixes in models.items():
            if not model_pattern.startswith(ix):
                continue
            # Replace each 'x' with '\d' to match exactly one digit
            regex_pattern = model_pattern
            x_count = regex_pattern.count('x')
            if x_count > 0:
                # Replace 'xx' with '\d{2}', 'xxx' with '\d{3}', etc.
                regex_pattern = regex_pattern.replace(
                    'x' * x_count, r'\d{' + str(x_count) + '}')

            non_empty_suffixes = [s for s in suffixes if s != ""]
            if non_empty_suffixes:
                suffix_pattern = "(" + "|".join(re.escape(s)
                                   for s in non_empty_suffixes) + ")?"
            else:
                suffix_pattern = ""
            full_pattern = rf"{regex_pattern}{suffix_pattern}$"
            if re.match(full_pattern, model_token, re.IGNORECASE):
                return gen
    return None


def get_generation_from_standalone_model(model_token):
    """Get generation from standalone model numbers like '3210M' by inferring the i5 family."""
    # For 4-5 digit models starting with specific digits, infer the generation
    if re.match(r'^\d{4,5}[a-zA-Z]*$', model_token):
        first_digit = int(model_token[0])

        # Map first digit to generation (common Intel pattern)
        if first_digit == 2:
            return "2nd Gen"
        elif first_digit == 3:
            return "3rd Gen"
        elif first_digit == 4:
            return "4th Gen"
        elif first_digit == 5:
            return "5th Gen"
        elif first_digit == 6:
            return "6th Gen"
        elif first_digit == 7:
            return "7th Gen"
        elif first_digit == 8:
            return "8th Gen"
        elif first_digit == 9:
            return "9th Gen"
        else:
            # For 10th+ gen, try first two digits
            if len(model_token) >= 2:
                first_two = model_token[:2]
                if first_two.isdigit():
                    gen_num = int(first_two)
                    if 10 <= gen_num <= 14:
                        return f"{gen_num}th Gen"

    return None

# Update the has_cpu_context function to include Apple M-series detection


def has_cpu_context(tokens: List[str]) -> bool:
    """Check if there are any CPU-related tokens anywhere in the token list without consuming them."""
    cpu_brands = {'intel', 'amd', 'arm', 'apple', 'qualcomm', 'mediatek', 'samsung', 'ibm', 'via',
        'cyrix', 'transmeta', 'fujitsu', 'motorola', 'risc-v', 'huawei', 'rockchip', 'allwinner'}

    cpu_families = {'core', 'ryzen', 'xeon', 'pentium', 'celeron',
        'atom', 'athlon', 'phenom', 'epyc', 'threadripper'}

    # Check for CPU brands
    for token in tokens:
        if token.lower() in cpu_brands:
            return True

    # Check for CPU families
    for token in tokens:
        if token.lower() in cpu_families:
            return True

    # Check for Intel Core Ultra patterns
    for i, token in enumerate(tokens):
        if (token.lower() == "core" and
            i + 1 < len(tokens) and
            tokens[i + 1].lower() == "ultra"):
            return True

    # Check for i-series patterns (i3, i5, i7, i9)
    for token in tokens:
        if re.match(r'i[3579](?:-|$)', token.lower()):
            return True

                  # Check for existing i-series CPU models
        for token in tokens:
            if re.match(r'i[3579]-[0-9]{3,4}[a-zA-Z0-9]*', token.lower()):
                return True

    # Check for Apple M-series
    for token in tokens:
        if re.match(r'm[123](?:\s+(?:pro|max|ultra))?$', token.lower()):
            return True

    # Check for CPU-related terms
    cpu_terms = {'processor', 'cpu', 'ghz', 'mhz', '@'}
    for token in tokens:
        if token.lower() in cpu_terms or token.lower().startswith('@'):
            return True

    return False


class BaseExtractor:
    """Base class for extractors, handling pattern matching and output processing."""

    def __init__(self, config: Dict[str, Any], logger=None):
        """Initialize with a configuration dictionary and optional logger."""
        self.name = config["name"]
        self.patterns = config.get("patterns", [])
        self.multiple = config.get("multiple", False)
        self.output_options = config.get(
            "output_options", {"include_unit": True})
        self.logger = logger

    def match_pattern(self, tokens: List[str], pattern_set: List[Dict], consumed: Set[int]) -> Optional[List[int]]:
        """Find the first match for the pattern set in tokens, respecting consumed indices."""
        for start in range(len(tokens)):
            if start in consumed:
                continue
            pos = start
            match_indices = []
            for pattern in pattern_set:
                while pos < len(tokens) and pos in consumed:
                    pos += 1
                if pos >= len(tokens):
                    if pattern.get("optional", False):
                        continue
                    return None
                token = tokens[pos].lower()
                if pattern["type"] == "string":
                    if token == pattern["value"].lower():
                        if pattern.get("include_in_output", True):
                            match_indices.append(pos)
                        pos += 1
                    elif pattern.get("optional", False):
                        continue
                    else:
                        break
                elif pattern["type"] == "regex":
                    if re.match(pattern["pattern"], token, re.IGNORECASE):
                        if pattern.get("include_in_output", True):
                            match_indices.append(pos)
                        pos += 1
                    elif pattern.get("optional", False):
                        continue
                    else:
                        break
                elif pattern["type"] == "list":
                    if any(value.lower() == token for value in pattern["values"]):
                        if pattern.get("include_in_output", True):
                            match_indices.append(pos)
                        pos += 1
                    elif pattern.get("optional", False):
                        continue
                    else:
                        break
            else:
                return match_indices
        return None

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract matches from tokens based on patterns, updating consumed indices."""
        results = []
        for pattern_set in self.patterns:
            while True:
                match_indices = self.match_pattern(
                    tokens, pattern_set, consumed)
                if match_indices is None:
                    break
                results.append(match_indices)
                consumed.update(match_indices)
                if not self.multiple:
                    break
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Convert matched indices into a dictionary."""
        value = " ".join(tokens[i] for i in match_indices)
        if not self.output_options.get("include_unit", True):
            value = re.sub(r"(ghz|mhz)$", "", value, flags=re.IGNORECASE)
        return {self.name: value}


class CPUGenerationMultiSlashExtractor(BaseExtractor):
    """NEW extractor specifically for handling slash-separated generation patterns like '11th/12th Gen'"""

    def extract(self, tokens, consumed):
        """Extract slash-separated CPU generation patterns that follow Intel Core context."""
        results = []
        # Reset sequence map for this extraction run
        self._sequence_map = {}

        # Only extract if there's CPU context
        if not has_cpu_context(tokens):
            return []

        i = 0
        while i < len(tokens):
            if i in consumed:
                i += 1
                continue

            # Look for patterns like "11th/12th" that are followed by "Gen"
            if (re.match(r"\d+(?:st|nd|rd|th)$", tokens[i], re.IGNORECASE) and
                i + 1 < len(tokens) and tokens[i + 1] == "/" and
                i + 2 < len(tokens) and re.match(r"\d+(?:st|nd|rd|th)$", tokens[i + 2], re.IGNORECASE) and
                i + 3 < len(tokens) and re.match(r"Gen\.?", tokens[i + 3], re.IGNORECASE)):

                # Check if this is preceded by "Intel Core" or "Core"
                is_preceded_by_core = False
                if i >= 1 and tokens[i - 1].lower() == "core":
                    is_preceded_by_core = True
                elif (i >= 2 and tokens[i - 2].lower() == "intel" and
                      tokens[i - 1].lower() == "core"):
                    is_preceded_by_core = True

                if is_preceded_by_core:
                    # Create separate results for each generation
                    first_gen_indices = [i, i + 3]  # "11th Gen"
                    second_gen_indices = [i + 2, i + 3]  # "12th Gen"

                    results.append(first_gen_indices)
                    results.append(second_gen_indices)

                    # Record ordering for numbered key generation
                    self._sequence_map[i] = 1
                    self._sequence_map[i + 2] = 2

                    # Mark all tokens as consumed
                    consumed.add(i)      # "11th"
                    consumed.add(i + 1)  # "/"
                    consumed.add(i + 2)  # "12th"
                    consumed.add(i + 3)  # "Gen"

                    i += 4
                    continue

            i += 1

        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process CPU generation matches from slash patterns."""
        if len(match_indices) == 2:
            # Two tokens: generation number and "Gen"
            first_token = tokens[match_indices[0]]
            second_token = tokens[match_indices[1]]

            if (re.match(r"\d+(?:st|nd|rd|th)", first_token, re.IGNORECASE) and
                second_token.lower().startswith("gen")):
                # Use numbered keys if part of a detected sequence
                key_suffix = ""
                if hasattr(self, "_sequence_map") and match_indices[0] in getattr(self, "_sequence_map", {}):
                    order = self._sequence_map[match_indices[0]]
                    key_suffix = "" if order == 1 else str(order)
                key = f"cpu_generation{key_suffix}"
                return {key: f"{first_token} Gen"}

        # Fallback
        value = " ".join(tokens[i] for i in match_indices)
        return {"cpu_generation": value}


class CPUGenericCoreExtractor(BaseExtractor):
    """NEW extractor for handling 'Intel Core' without specific family when followed by generations."""

    def extract(self, tokens, consumed):
        """Extract Intel Core patterns that are followed by generation info but no i-series family."""
        results = []

        i = 0
        while i < len(tokens):
            if i in consumed:
                i += 1
                continue

            # Look for "Intel Core" or "Core" followed by generation patterns
            is_intel_core_pattern = False
            core_start_idx = -1

            if (tokens[i].lower() == "intel" and
                i + 1 < len(tokens) and tokens[i + 1].lower() == "core"):
                is_intel_core_pattern = True
                core_start_idx = i
            elif tokens[i].lower() == "core":
                is_intel_core_pattern = True
                core_start_idx = i

            if is_intel_core_pattern:
                # Check what follows the "Core" token
                next_idx = core_start_idx + \
                    (2 if tokens[core_start_idx].lower() == "intel" else 1)

                # Skip any consumed tokens
                while next_idx < len(tokens) and next_idx in consumed:
                    next_idx += 1

                # Check if followed by generation patterns like "11th/12th Gen" or "11th Gen"
                if (next_idx < len(tokens) and
                    re.match(r"\d+(?:st|nd|rd|th)$", tokens[next_idx], re.IGNORECASE)):

                    # Check if this is a slash-separated generation pattern
                    is_slash_gen_pattern = (
                        next_idx + 1 < len(tokens) and tokens[next_idx + 1] == "/" and
                        next_idx + 2 < len(tokens) and re.match(r"\d+(?:st|nd|rd|th)$", tokens[next_idx + 2], re.IGNORECASE) and
                        next_idx +
                            3 < len(tokens) and re.match(
                                r"Gen\.?", tokens[next_idx + 3], re.IGNORECASE)
                    )

                    # Or single generation pattern
                    is_single_gen_pattern = (
                        next_idx +
                            1 < len(tokens) and re.match(
                                r"Gen\.?", tokens[next_idx + 1], re.IGNORECASE)
                    )

                    if is_slash_gen_pattern or is_single_gen_pattern:
                        # This is a generic "Intel Core" or "Core" without specific i-series family
                        # Return the Core pattern without the generation part
                        if tokens[core_start_idx].lower() == "intel":
                            result_indices = [core_start_idx,
                                core_start_idx + 1]  # "Intel Core"
                        else:
                            result_indices = [core_start_idx]  # "Core"

                        results.append(result_indices)

                        # Mark Core tokens as consumed
                        for idx in result_indices:
                            consumed.add(idx)

                        # Skip past the entire pattern
                        if is_slash_gen_pattern:
                            i = next_idx + 4
                        else:
                            i = next_idx + 2
                        continue

            i += 1

        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process generic Intel Core matches."""
        result = {}

        if len(match_indices) == 2:
            # "Intel Core" pattern
            result["cpu_brand"] = "Intel"
            result["cpu_family"] = "Core"  # Generic Core family
        elif len(match_indices) == 1 and tokens[match_indices[0]].lower() == "core":
            # "Core" pattern
            result["cpu_family"] = "Core"  # Generic Core family

        return result


class CPUGenerationExtractor(BaseExtractor):
    """
    Numbering convention for all extractors:
    - Base key is unnumbered for the first occurrence
    - Second and onward occurrences use numeric suffixes (e.g., key2, key3, ...)
    This file follows the above rule when emitting fields like cpu_generation.
    """
    def extract(self, tokens, consumed):
        """Extract CPU generation patterns like '8th/6th Gen' from tokenized text."""
        # Only extract generations if there's CPU context
        if not has_cpu_context(tokens):
            return []

        # Reset sequence map for this extraction run
        self._sequence_map = {}

        # Don't extract CPU generations from compatibility descriptions
        compatibility_phrases = [
            'supports', 'support', 'compatible', 'compatibility', 'designed for',
            'optimized for', 'works with', 'fits', 'motherboard', 'socket',
            'chipset', 'platform', 'family processors', 'processor family'
        ]

        tokens_lower = [t.lower() for t in tokens]
        has_compatibility_context = any(phrase in ' '.join(
            tokens_lower) for phrase in compatibility_phrases)

        if has_compatibility_context:
            return []

        # Additional check: Don't extract if this is clearly about a system/motherboard
        system_indicators = [
            'motherboard', 'server', 'system', 'board', 'chassis', 'poweredge',
            'proliant', 'thinkserver', 'supermicro', 'dell', 'hp', 'lenovo',
            'rack', 'tower', 'workstation'
        ]

        has_system_context = any(
            token.lower() in system_indicators for token in tokens)

        if has_system_context:
            # Check for CPU family indicators (Core i5, Ryzen 5, etc.) which should allow generation extraction
            tokens_text = ' '.join(tokens)
            tokens_text_lower = ' '.join(tokens_lower)
            has_cpu_family = (
                bool(re.search(r'\bcore\s+i[3579]\b', tokens_text_lower)) or
                bool(re.search(r'\bcore\s+ultra\s+[579]\b', tokens_text_lower)) or
                bool(re.search(r'\bryzen\s+[3579]\b', tokens_text_lower)) or
                bool(re.search(r'\bxeon\b', tokens_text_lower)) or
                any(token.lower() in ['i3', 'i5', 'i7', 'i9'] for token in tokens)
            )

            # Only proceed if there's a clear CPU model OR CPU family in the same text
            has_explicit_cpu_model = any(
                re.match(r'i[3579]-[0-9]{3,4}[a-zA-Z]*', token.lower()) or
                # Short generation patterns like i7-6
                re.match(r'i[3579]-[0-9]{1,2}$', token.lower()) or
                re.match(r'(E[357]|W)-[0-9]{4}[a-zA-Z]*', token) or
                # AMD Ryzen pattern
                re.match(r'[3579][0-9]{3}[a-zA-Z]*', token) or
                # Core Ultra pattern (135U, 165H, etc.)
                re.match(r'[125]\d{2}[a-zA-Z]*', token)
                for token in tokens
            )

            # Minimal relaxation: allow explicit "Intel Core Xth[/Yth] Gen" patterns to pass
            allow_core_gen_pattern = (
                ('intel' in tokens_lower and 'core' in tokens_lower) and (
                    bool(re.search(r'\b\d+(?:st|nd|rd|th)\s*/\s*\d+(?:st|nd|rd|th)\s*Gen\.?', tokens_text, re.IGNORECASE)) or
                    bool(re.search(r'\b\d+(?:st|nd|rd|th)\s*Gen\b', tokens_text, re.IGNORECASE))
                )
            )

            if not has_explicit_cpu_model and not has_cpu_family and not allow_core_gen_pattern:
                return []

        results = []
        i = 0
        while i < len(tokens):
            # ENHANCED: Don't skip tokens that contain generation information, even if consumed
            # This allows generation extractor to work with full context
            token = tokens[i]
            is_generation_relevant = (
                # i7-6 patterns
                re.match(r"i[3579]-(\d{1,2})$", token, re.IGNORECASE) or
                # 8th, 9th patterns
                re.match(r"\d+(?:st|nd|rd|th)$", token, re.IGNORECASE) or
                token.lower() == "gen"
            )

            if i in consumed and not is_generation_relevant:
                i += 1
                continue

            # NEW: Handle embedded short generation tokens like 'i7-6' or 'i5-8'
            if re.match(r"i[3579]-(\d{1,2})$", tokens[i], re.IGNORECASE):
                results.append([i])
                consumed.add(i)
                i += 1
                continue

            # NEW: Check for standalone generation numbers that are followed by "/" or adjacent to CPU tokens
            if re.match(r"\d+(?:st|nd|rd|th)$", tokens[i], re.IGNORECASE):
                # Check if this generation number is adjacent to CPU-related content
                is_cpu_adjacent = False

                # Check if preceded by CPU family/model (like "Core i7 10th")
                if i > 0:
                    prev_token = tokens[i - 1].lower()
                    if (prev_token.startswith(("i3", "i5", "i7", "i9")) or
                        prev_token in ["core", "ryzen", "xeon", "pentium", "celeron", "athlon", "5", "7", "9"]):
                        is_cpu_adjacent = True
                    # Check if preceded by "Core iX" pattern
                    elif (i > 1 and tokens[i - 2].lower() == "core" and
                          tokens[i - 1].lower().startswith(("i3", "i5", "i7", "i9"))):
                        is_cpu_adjacent = True
                    # Check if preceded by "Core Ultra X" pattern
                    elif (i > 2 and tokens[i - 3].lower() == "core" and
                          tokens[i - 2].lower() == "ultra" and
                          tokens[i - 1] in ["5", "7", "9"]):
                        is_cpu_adjacent = True

                # Check if followed by "/" (indicating it's part of a sequence like "10th/Apple")
                is_followed_by_slash = (
                    i + 1 < len(tokens) and tokens[i + 1] == "/")

                # Check if followed by "Gen" (traditional pattern)
                is_followed_by_gen = (i + 1 < len(tokens) and
                                     re.match(r"Gen\.?", tokens[i + 1], re.IGNORECASE))

                if is_cpu_adjacent and (is_followed_by_slash or is_followed_by_gen or i == len(tokens) - 1):
                    if is_followed_by_gen:
                        # Traditional "10th Gen" pattern
                        results.append([i, i + 1])
                        consumed.add(i)
                        consumed.add(i + 1)
                    else:
                        # Standalone "10th" pattern (like "10th/Apple" or "10th" at end)
                        results.append([i])
                        consumed.add(i)
                        # Don't consume the "/" - let other extractors handle it
                    i += 1
                    continue

            # Original logic: Check for 'Gen' or 'gen' (case-insensitive)
            if re.match(r"Gen\.?", tokens[i], re.IGNORECASE):
                gen_index = i
                generation_numbers = []

                j = i - 1  # Look backward
                while j >= 0:
                    # Match generation numbers with or without ordinal suffix (e.g. '4th', '10th', '2')
                    if re.match(r"\d+(?:st|nd|rd|th)?$", tokens[j], re.IGNORECASE):
                        generation_numbers.append(j)
                        # Walk backward past any repeated '/ <number>' pairs
                        while j - 1 >= 0 and tokens[j - 1] == '/':
                            prev_idx = j - 2
                            # Also detect hyphen-embedded generation tokens (e.g., 'i5-8th')
                            if prev_idx >= 0 and (
                                re.match(r"\d+(?:st|nd|rd|th)?$",
                                         tokens[prev_idx], re.IGNORECASE)
                                or re.search(r"i[3579]-(\d+(?:st|nd|rd|th))", tokens[prev_idx], re.IGNORECASE)
                            ):
                                generation_numbers.append(prev_idx)
                                j = prev_idx  # continue scanning further left
                            else:
                                break
                        break  # finished gathering contiguous sequence
                    # FIXED: Also check for standalone numbers (without ordinal suffix)
                    # that might be part of a slash-separated sequence
                    elif (re.match(r"^\d+$", tokens[j]) and
                          j + 1 < len(tokens) and tokens[j + 1] == '/' and
                          j + 2 < len(tokens) and re.match(r"\d+(?:st|nd|rd|th)", tokens[j + 2], re.IGNORECASE)):
                        # This is a number followed by slash and ordinal number (like "6/7th")
                        generation_numbers.append(j)
                        break  # Don't continue looking beyond this pattern
                    # FIXED: Also check for generation numbers embedded in CPU model tokens like "i7-11th"
                    elif re.search(r'i[3579]-(\d+(?:st|nd|rd|th))', tokens[j], re.IGNORECASE):
                        generation_numbers.append(j)
                        break  # Don't continue looking if we found an embedded generation
                    # NEW: Handle cases where multiple generations are packed into a single token such as "2/3/4th"
                    elif '/' in tokens[j] and re.match(r'^(\d+(?:st|nd|rd|th)?)(/\d+(?:st|nd|rd|th)?)+$', tokens[j], re.IGNORECASE):
                        # Treat the entire token as a generation list. Detailed parsing will happen in process_match.
                        generation_numbers.append(j)
                        break
                    else:
                        break

                # Create separate results for each generation found
                if generation_numbers:
                    # Reverse to get them in the original order (left to right)
                    generation_numbers.reverse()

                    # Record ordering for numbered key generation
                    for position, gen_num_idx in enumerate(generation_numbers, start=1):
                        self._sequence_map[gen_num_idx] = position

                    for gen_num_idx in generation_numbers:
                        # Check if this is an embedded generation in a CPU model token
                        embedded_match = re.search(
                            r'i[3579]-(\d+(?:st|nd|rd|th))', tokens[gen_num_idx], re.IGNORECASE)
                        if embedded_match:
                            # For embedded generations, we need to create a virtual result
                            # since we can't point to just the generation part of the token
                            # We'll include both the CPU model token and the Gen token
                            result_indices = [gen_num_idx, gen_index]
                            results.append(result_indices)
                            # Consume the CPU model token
                            consumed.add(gen_num_idx)

                            # NEW: If there is a slash-separated previous embedded generation like
                            # i5-11th / i7-13th Gen, capture the left side as well so we emit two generations
                            prev_slash_idx = gen_num_idx - 1
                            prev_embedded_idx = gen_num_idx - 2
                            if (
                                prev_embedded_idx >= 0 and prev_slash_idx >= 0 and
                                tokens[prev_slash_idx] == '/' and
                                re.search(r'i[3579]-(\d+(?:st|nd|rd|th))', tokens[prev_embedded_idx], re.IGNORECASE)
                            ):
                                # Avoid duplicates if already present
                                if [prev_embedded_idx, gen_index] not in results:
                                    results.append([prev_embedded_idx, gen_index])
                                    consumed.add(prev_embedded_idx)
                        else:
                            # Normal case: separate generation number and "Gen"
                            result_indices = [gen_num_idx, gen_index]
                            results.append(result_indices)
                            consumed.add(gen_num_idx)

                    # Mark "Gen" as consumed after processing all generations
                    consumed.add(gen_index)

                    # FIXED: Also consume any slash tokens that were part of the pattern
                    for gen_num_idx in generation_numbers:
                        if gen_num_idx + 1 < len(tokens) and tokens[gen_num_idx + 1] == '/':
                            consumed.add(gen_num_idx + 1)

            i += 1
        return results

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process CPU generation matches, handling both separate and embedded generation patterns."""
        if len(match_indices) == 2:
            # Two tokens: either "11th Gen" or "i7-11th Gen" or "6 Gen"
            first_token = tokens[match_indices[0]]
            second_token = tokens[match_indices[1]]

            # Check if the first token has an embedded generation pattern
            embedded_match = re.search(
                r'i[3579]-(\d+(?:st|nd|rd|th))', first_token, re.IGNORECASE)
            if embedded_match and second_token.lower() == "gen":
                # Extract just the generation part from the embedded token
                generation_part = embedded_match.group(1)
                # Use numbered keys if part of a detected sequence
                key_suffix = ""
                if hasattr(self, "_sequence_map") and match_indices[0] in getattr(self, "_sequence_map", {}):
                    order = self._sequence_map[match_indices[0]]
                    key_suffix = "" if order == 1 else str(order)
                key = f"cpu_generation{key_suffix}"
                return {key: f"{generation_part} Gen"}

            # Normal case: generation number with ordinal suffix and "Gen"
            elif re.match(r"\d+(?:st|nd|rd|th)", first_token, re.IGNORECASE) and second_token.lower().startswith("gen"):
                key_suffix = ""
                if hasattr(self, "_sequence_map") and match_indices[0] in getattr(self, "_sequence_map", {}):
                    order = self._sequence_map[match_indices[0]]
                    key_suffix = "" if order == 1 else str(order)
                key = f"cpu_generation{key_suffix}"
                return {key: f"{first_token} Gen"}

            # Handle case where first token is just a number ("2" "Gen." etc.)
            elif re.match(r"^\d+$", first_token) and second_token.lower().startswith("gen"):
                num = int(first_token)
                if 10 <= num % 100 <= 20:
                    suffix = "th"
                else:
                    suffix = {1: "st", 2: "nd", 3: "rd"}.get(num % 10, "th")
                key_suffix = ""
                if hasattr(self, "_sequence_map") and match_indices[0] in getattr(self, "_sequence_map", {}):
                    order = self._sequence_map[match_indices[0]]
                    key_suffix = "" if order == 1 else str(order)
                key = f"cpu_generation{key_suffix}"
                return {key: f"{num}{suffix} Gen"}

        elif len(match_indices) == 1:
            # Single token case
            token = tokens[match_indices[0]]
            if re.match(r"\d+(?:st|nd|rd|th)", token, re.IGNORECASE):
                # Single-token generation not tied to a sequence
                return {"cpu_generation": f"{token} Gen"}
            # FIXED: Handle standalone numbers
            elif re.match(r"^\d+$", token):
                num = int(token)
                if num % 100 in [11, 12, 13]:
                    suffix = "th"
                elif num % 10 == 1:
                    suffix = "st"
                elif num % 10 == 2:
                    suffix = "nd"
                elif num % 10 == 3:
                    suffix = "rd"
                else:
                    suffix = "th"
                return {"cpu_generation": f"{num}{suffix} Gen"}
            # NEW: Core i-series with short number (e.g. "i7-6")
            elif re.match(r"i[3579]-(\d{1,2})$", token, re.IGNORECASE):
                match = re.match(r"i[3579]-(\d{1,2})$", token, re.IGNORECASE)
                num = int(match.group(1))
                if num % 100 in [11, 12, 13]:
                    suffix = "th"
                elif num % 10 == 1:
                    suffix = "st"
                elif num % 10 == 2:
                    suffix = "nd"
                elif num % 10 == 3:
                    suffix = "rd"
                else:
                    suffix = "th"
                return {"cpu_generation": f"{num}{suffix} Gen"}

        # Fallback: join all tokens
        value = " ".join(tokens[i] for i in match_indices)
        return {"cpu_generation": value}


class CPUQuantityExtractor(BaseExtractor):
    """CPU quantity extractor that looks for quantities adjacent to CPU tokens."""

    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract CPU quantity patterns that are adjacent to CPU-related tokens."""
        results = []

        # Look for quantity patterns that are adjacent to CPU tokens
        for i in range(len(tokens)):
            if i in consumed:  # Skip already consumed tokens
                continue

            token = tokens[i]

            # Check if this token matches any quantity pattern
            if self._is_quantity_pattern(token):
                # FIXED: Check if it's part of a model name context
                if self._is_part_of_model_name(tokens, i):
                    continue  # Skip if it's part of a model name

                # Check if it's adjacent to any CPU token (immediate neighbors only)
                if self._is_adjacent_to_cpu_tokens(tokens, i):
                    results.append([i])
                    if not self.multiple:
                        break

        return results

    def _is_quantity_pattern(self, token: str) -> bool:
        """Check if a token matches a CPU quantity pattern."""
        token_lower = token.lower()

        # Exact word matches (not followed by "core")
        if token_lower in ['single', 'dual', 'triple', 'quad']:
            return True

        # Pattern matches
        quantity_patterns = [
            r'^\d+x$',           # 2x, 4x
            r'^x\d+$',           # x2, x4
            r'^\(\d+x\)$',       # (2x)
            r'^\(x\d+\)$',       # (x2)
            r'^\(\d+\)$',        # (2)
        ]

        return any(re.match(pattern, token, re.IGNORECASE) for pattern in quantity_patterns)

    def _is_part_of_model_name(self, tokens: List[str], index: int) -> bool:
        """Check if the quantity pattern is actually part of a model name like 'X230', 'T450s'."""
        token = tokens[index]

        # Check if this token starts with a letter followed by digits (like X230, T450)
        if re.match(r'^[A-Za-z]\d+', token):
            return True

        # Check if the previous token is a model prefix and current token is just digits
        if index > 0:
            prev_token = tokens[index - 1]
            # Check for patterns like "ThinkPad" followed by model number
            if (prev_token.lower() in ['thinkpad', 'latitude', 'inspiron', 'pavilion', 'elitebook', 'probook'] and
                re.match(r'^\d+', token)):
                return True

            # Check if previous token ends with a letter and current starts with digits (split model names)
            if (re.search(r'[A-Za-z]$', prev_token) and re.match(r'^\d+', token)):
                return True

        # Check context around the token for model name indicators
        context_start = max(0, index - 2)
        context_end = min(len(tokens), index + 3)
        context_tokens = tokens[context_start:context_end]

        # Model name indicators
        model_indicators = [
            'thinkpad', 'latitude', 'inspiron', 'pavilion', 'elitebook', 'probook',
            'macbook', 'imac', 'surface', 'yoga', 'ideapad', 'vostro', 'precision',
            'omen', 'envy', 'spectre', 'zbook', 'chromebook'
        ]

        # If we find model indicators in the context, this might be a model number
        for context_token in context_tokens:
            if context_token.lower() in model_indicators:
                return True

        return False

    def _is_adjacent_to_cpu_tokens(self, tokens: List[str], index: int) -> bool:
        """Check if the token at index is immediately adjacent to any CPU token."""
        # Check immediate neighbors only (before and after)
        adjacent_indices = []
        if index > 0:
            adjacent_indices.append(index - 1)
        if index < len(tokens) - 1:
            adjacent_indices.append(index + 1)

        # Check if any adjacent token is CPU-related (skip over a '/' delimiter)
        for adj_idx in adjacent_indices:
            tok = tokens[adj_idx]
            if self._is_cpu_related_token(tok):
                return True
            # If the adjacent token is just a slash, look one more step in that direction
            if tok == "/":
                # Look two steps away depending on direction
                if adj_idx < index and adj_idx - 1 >= 0:
                    if self._is_cpu_related_token(tokens[adj_idx - 1]):
                        return True
                elif adj_idx > index and adj_idx + 1 < len(tokens):
                    if self._is_cpu_related_token(tokens[adj_idx + 1]):
                        return True

        return False

    def _is_cpu_related_token(self, token: str) -> bool:
        """Check if a token is CPU-related with enhanced detection."""
        token_lower = token.lower()

        # CPU brands
        cpu_brands = {'intel', 'amd', 'arm', 'apple', 'qualcomm', 'mediatek',
                     'samsung', 'ibm', 'via', 'cyrix', 'transmeta', 'fujitsu',
                     'motorola', 'risc-v', 'huawei', 'rockchip', 'allwinner'}

        # CPU families
        cpu_families = {'core', 'ryzen', 'xeon', 'pentium', 'celeron', 'atom',
                       'athlon', 'phenom', 'epyc', 'threadripper', 'silver', 'gold',
                       'bronze', 'platinum'}

        # CPU terms
        cpu_terms = {'processor', 'cpu'}

        # Check for CPU brands
        if token_lower in cpu_brands:
            return True

        # Check for CPU families
        if token_lower in cpu_families:
            return True

        # Check for CPU terms
        if token_lower in cpu_terms:
            return True

        # Check for i-series patterns (i3, i5, i7, i9)
        if re.match(r'i[3579](?:-|$)', token_lower):
            return True

        # Check for CPU model patterns
        cpu_model_patterns = [
            r'i[3579]-[0-9]{3,4}[a-zA-Z0-9]*',  # i5-8250U, i7-10700K
            r'[0-9]{4}[a-zA-Z]+',                # 4214R, 5600X
            r'[JNG][0-9]{3,4}',                  # J4105, N5105
            r'[a-zA-Z0-9]+-[0-9]{4}[a-zA-Z]*',   # E5-2623, E3-1535M
            r'v[0-9]+',                          # v3, v6 (Xeon versions)
        ]

        if any(re.match(pattern, token, re.IGNORECASE) for pattern in cpu_model_patterns):
            return True

        # Check for CPU speed patterns
        speed_patterns = [
            r'[0-9]+\.[0-9]+[gG][hH][zZ]',      # 2.4GHz, 3.0GHz
            r'[0-9]+[gG][hH][zZ]',              # 3GHz
            r'@[0-9]+\.[0-9]+[gG][hH][zZ]',     # @2.4GHz
            r'@[0-9]+[gG][hH][zZ]',             # @3GHz
            r'[0-9]+[mM][hH][zZ]',              # 1000MHz
        ]

        if any(re.match(pattern, token, re.IGNORECASE) for pattern in speed_patterns):
            return True

        return False

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process CPU quantity matches and normalize to Nx format."""
        token = tokens[match_indices[0]]
        token_lower = token.lower()

        # Handle different quantity formats and normalize to Nx format
        if re.match(r'^\d+x$', token, re.IGNORECASE):
            # Format like "2x", "4x" - already in desired format
            return {"cpu_quantity": token_lower}
        elif re.match(r'^x\d+$', token, re.IGNORECASE):
            # Format like "x2", "x4" - convert to "2x" format
            match = re.search(r'\d+', token)
            if match:
                num = match.group()
                return {"cpu_quantity": f"{num}x"}
        elif re.match(r'^\(\d+x\)$', token, re.IGNORECASE):
            # Format like "(2x)" - extract and normalize
            match = re.search(r'\((\d+)x\)', token, re.IGNORECASE)
            if match:
                return {"cpu_quantity": f"{match.group(1)}x"}
        elif re.match(r'^\(x\d+\)$', token, re.IGNORECASE):
            # Format like "(x2)" - extract and convert
            match = re.search(r'\d+', token)
            if match:
                num = match.group()
                return {"cpu_quantity": f"{num}x"}
        elif re.match(r'^\(\d+\)$', token, re.IGNORECASE):
            # Format like "(2)" - extract number and convert
            match = re.search(r'\d+', token)
            if match:
                num = match.group()
                return {"cpu_quantity": f"{num}x"}
        elif token_lower == "single":
            # Convert "single" to "1x"
            return {"cpu_quantity": "1x"}
        elif token_lower == "dual":
            # Convert "dual" to "2x"
            return {"cpu_quantity": "2x"}
        elif token_lower == "triple":
            # Convert "triple" to "3x"
            return {"cpu_quantity": "3x"}
        elif token_lower == "quad":
            # Convert "quad" to "4x"
            return {"cpu_quantity": "4x"}

        # Fallback - return as is
        return {"cpu_quantity": token}


def extract_xeon_processors(tokens: List[str], consumed: Set[int], results: List[List[int]], i: int, logger=None) -> bool:
    """Extract Xeon processors with proper handling for 'CPU' keyword between 'Xeon' and model."""
    if tokens[i].lower() == "xeon" and i + 1 < len(tokens):
        # Look for model patterns starting from the next token
        indices = [i]  # Start with "Xeon"

        model_idx = i + 1
        while model_idx < len(tokens) and model_idx in consumed:
            model_idx += 1  # Skip consumed tokens

        # Skip over "CPU" if it's the next token
        if model_idx < len(tokens) and tokens[model_idx].lower() == "cpu":
            model_idx += 1
            while model_idx < len(tokens) and model_idx in consumed:
                model_idx += 1  # Skip consumed tokens

        if model_idx < len(tokens):
            # UPDATED: More restrictive pattern for series-model format
            # Only match legitimate Xeon series: E3, E5, E7, W
            if re.match(r"(E[357]|W)-[0-9]{4}[a-zA-Z]*", tokens[model_idx], re.IGNORECASE):
                indices.append(model_idx)
                consumed.add(model_idx)

                # Look for version (v6) in next token
                version_idx = model_idx + 1
                while version_idx < len(tokens) and version_idx in consumed:
                    version_idx += 1

                if version_idx < len(tokens) and re.match(r"[vV]\d+|0", tokens[version_idx], re.IGNORECASE):
                    indices.append(version_idx)
                    consumed.add(version_idx)

            # UPDATED: More restrictive pattern for series only
            # Only match legitimate Xeon series prefixes
            elif re.match(r"(E[357]|E|W)$", tokens[model_idx], re.IGNORECASE):
                # Check if next token is the model number
                num_idx = model_idx + 1
                while num_idx < len(tokens) and num_idx in consumed:
                    num_idx += 1

                # Only proceed if we find a complete 4-digit model number
                if num_idx < len(tokens) and re.match(r"[0-9]{4}[a-zA-Z]*", tokens[num_idx], re.IGNORECASE):
                    indices.append(model_idx)  # Add series (e.g., "E5")
                    consumed.add(model_idx)
                    # Add model number (e.g., "2630")
                    indices.append(num_idx)
                    consumed.add(num_idx)

                    # Check for version in next token
                    version_idx = num_idx + 1
                    while version_idx < len(tokens) and version_idx in consumed:
                        version_idx += 1

                    if version_idx < len(tokens) and re.match(r"[vV]\d+|0", tokens[version_idx], re.IGNORECASE):
                        indices.append(version_idx)
                        consumed.add(version_idx)

            # NEW: Handle generic Xeon E-2xxx models (e.g., "E-2176M", "E-2288G")
            elif re.match(r"E-[0-9]{4}[a-zA-Z]*", tokens[model_idx], re.IGNORECASE):
                indices.append(model_idx)
                consumed.add(model_idx)
                # No additional version token expected for these models

            # NEW: Handle named-series Xeon (Gold/Silver/Bronze/Platinum) followed by 4-digit model
            elif tokens[model_idx].lower() in {"gold", "silver", "bronze", "platinum"}:
                series_idx = model_idx
                num_idx = series_idx + 1
                while num_idx < len(tokens) and num_idx in consumed:
                    num_idx += 1

                # Require a 4-digit number after the series word
                if num_idx < len(tokens) and re.match(r"[0-9]{4}[A-Za-z]*", tokens[num_idx]):
                    indices.extend([series_idx, num_idx])
                    consumed.update({series_idx, num_idx})

        # Only add to results if we found more than just "Xeon" (i.e., we have a complete model)
        if len(indices) > 1:
            results.append(indices)
            consumed.add(i)  # Mark "Xeon" as consumed
            return True

    return False


def extract_apple_m_processors(tokens: List[str], consumed: Set[int], results: List[List[int]], i: int, context_available: bool, logger=None) -> bool:
    """Extract Apple M-series processors (M1, M2, M3, M1 Pro, M1 Max, etc.)."""
    token = tokens[i]

    # Check if there's explicit Apple brand mentioned near this token
    apple_brand_present = False

    # Check for Apple brand in a reasonable range around this token
    check_start = max(0, i - 5)  # Look 5 tokens before
    check_end = min(len(tokens), i + 5)  # Look 5 tokens after

    for j in range(check_start, check_end):
        if tokens[j].lower() == 'apple':
            apple_brand_present = True
            break

    # If Apple brand is explicitly mentioned nearby, allow Apple M-series detection
    if apple_brand_present:
        # Check for M1, M2, M3 patterns
        if re.match(r'^[mM][123]$', token):
            indices = [i]
            consumed.add(i)

            # Look for Pro/Max/Ultra variants in next token
            if i + 1 < len(tokens):
                next_token = tokens[i + 1].lower()
                if next_token in ['pro', 'max', 'ultra']:
                    indices.append(i + 1)
                    consumed.add(i + 1)

            results.append(indices)
            return True

        return False

    # If no explicit Apple brand, use the original logic with Intel context checking
    # Check if this might be Intel Core M series or another Intel processor context
    # Look for "Intel" or "Core" ANYWHERE in the token list (not just before)
    intel_context = False
    for j in range(len(tokens)):
        if tokens[j].lower() in ['intel', 'core']:
            intel_context = True
            break

    # Also check for explicit Intel processor models in the token list
    intel_cpu_present = False
    for j in range(len(tokens)):
        if re.match(r'i[3579]-\d{3,4}[a-zA-Z]*', tokens[j], re.IGNORECASE):
            intel_cpu_present = True
            break

    # If we found Intel context OR an Intel processor, this is likely Intel Core M series or part of model name
    if intel_context or intel_cpu_present:
        return False

    # Check if this appears to be part of a model name by looking at adjacent tokens
    # Look for model name patterns like "x360 m3" or "Pavilion m3"
    if i > 0:
        prev_token = tokens[i - 1]
        # Common model name patterns that might precede m1/m2/m3
        model_patterns = [
            r'x\d+',      # x360, x220, etc.
            r'pavilion',  # HP Pavilion
            r'elite',     # HP Elite
            r'envy',      # HP Envy
            r'spectre',   # HP Spectre
            r'latitude',  # Dell Latitude
            r'inspiron',  # Dell Inspiron
            r'thinkpad',  # Lenovo ThinkPad
            r'yoga',      # Lenovo Yoga
            r'surface',   # Microsoft Surface
            r'\d+[a-z]*',  # Various numeric model prefixes
        ]

        if any(re.match(pattern, prev_token.lower()) for pattern in model_patterns):
            return False

    # Check if there's no explicit Apple brand mentioned anywhere
    apple_brand_present = any(token.lower() == 'apple' for token in tokens)

    # Only proceed with Apple M-series detection if:
    # 1. No Intel context is found AND
    # 2. No Intel processors are present AND
    # 3. Either Apple brand is explicitly mentioned OR there are no other CPU brands
    if not apple_brand_present:
        # Check for other CPU brands
        other_cpu_brands = ['amd', 'qualcomm', 'mediatek', 'samsung']
        other_brands_present = any(
            token.lower() in other_cpu_brands for token in tokens)

        # If other CPU brands are present but no Apple brand, don't detect as Apple M-series
        if other_brands_present:
            return False

        # If Intel is present but no Apple brand, don't detect as Apple M-series
        if intel_context or intel_cpu_present:
            return False

    # Check for M1, M2, M3 patterns
    if re.match(r'^[mM][123]$', token):
        indices = [i]
        consumed.add(i)

        # Look for Pro/Max/Ultra variants in next token
        if i + 1 < len(tokens):
            next_token = tokens[i + 1].lower()
            if next_token in ['pro', 'max', 'ultra']:
                indices.append(i + 1)
                consumed.add(i + 1)

        results.append(indices)
        return True

    # Check for full patterns like "M1 Pro", "M2 Max", etc.
    elif re.match(r'^[mM][123]$', token) and i + 1 < len(tokens):
        next_token = tokens[i + 1].lower()
        if next_token in ['pro', 'max', 'ultra']:
            indices = [i, i + 1]
            consumed.add(i)
            consumed.add(i + 1)
            results.append(indices)
            return True

    return False


def extract_other_cpu_models(tokens: List[str], consumed: Set[int], results: List[List[int]], i: int, context_available: bool, logger=None) -> bool:
    """Extract non-Xeon CPU models including Core i-series, Celeron, and standalone models."""

    def _is_preceded_by_gpu_keywords(tokens: List[str], index: int) -> bool:
        """Check if a token is preceded by GPU-related keywords within a reasonable distance."""
        gpu_keywords = ['geforce', 'gtx', 'rtx', 'radeon',
            'rx', 'quadro', 'tesla', 'arc', 'iris', 'uhd', 'hd']

        # Look back up to 3 tokens for GPU keywords
        for i in range(max(0, index - 3), index):
            if tokens[i].lower() in gpu_keywords:
                return True
        return False

    def _is_adjacent_to_cpu_context(tokens: List[str], index: int) -> bool:
        """Check if a token is immediately adjacent to CPU-related context."""
        # Check immediate neighbors only (before and after)
        adjacent_indices = []
        if index > 0:
            adjacent_indices.append(index - 1)
        if index < len(tokens) - 1:
            adjacent_indices.append(index + 1)

        # Check if any adjacent token is CPU-related (skip over a '/' delimiter)
        for adj_idx in adjacent_indices:
            tok = tokens[adj_idx]
            if _is_cpu_related_token(tok):
                return True
            # If the adjacent token is just a slash, look one more step in that direction
            if tok == "/":
                # Look two steps away depending on direction
                if adj_idx < index and adj_idx - 1 >= 0:
                    if _is_cpu_related_token(tokens[adj_idx - 1]):
                        return True
                elif adj_idx > index and adj_idx + 1 < len(tokens):
                    if _is_cpu_related_token(tokens[adj_idx + 1]):
                        return True

        return False

    def _is_cpu_related_token(token: str) -> bool:
        """Check if a token is CPU-related."""
        token_lower = token.lower()

        # CPU brands
        cpu_brands = {'intel', 'amd', 'arm', 'apple', 'qualcomm', 'mediatek',
                     'samsung', 'ibm', 'via', 'cyrix', 'transmeta', 'fujitsu',
                     'motorola', 'risc-v', 'huawei', 'rockchip', 'allwinner'}

        # CPU families
        cpu_families = {'core', 'ryzen', 'xeon', 'pentium', 'celeron', 'atom',
                       'athlon', 'phenom', 'epyc', 'threadripper', 'silver', 'gold',
                       'bronze', 'platinum'}

        # CPU terms
        cpu_terms = {'processor', 'cpu'}

        # Check for CPU brands
        if token_lower in cpu_brands:
            return True

        # Check for CPU families
        if token_lower in cpu_families:
            return True

        # Check for CPU terms
        if token_lower in cpu_terms:
            return True

        # Check for i-series patterns (i3, i5, i7, i9)
        if re.match(r'i[3579](?:-|$)', token_lower):
            return True

        # Check for CPU model patterns
        cpu_model_patterns = [
            r'i[3579]-[0-9]{3,4}[a-zA-Z0-9]*',  # i5-8250U, i7-10700K
            # 4214R, 5600X (but will be filtered by adjacency)
            r'[0-9]{4}[a-zA-Z]+',
            r'[JNG][0-9]{3,4}',                  # J4105, N5105
            r'[a-zA-Z0-9]+-[0-9]{4}[a-zA-Z]*',   # E5-2623, E3-1535M
            r'v[0-9]+',                          # v3, v6 (Xeon versions)
        ]

        if any(re.match(pattern, token, re.IGNORECASE) for pattern in cpu_model_patterns):
            return True

        # Check for CPU speed patterns
        speed_patterns = [
            r'[0-9]+\.[0-9]+[gG][hH][zZ]',      # 2.4GHz, 3.0GHz
            r'[0-9]+[gG][hH][zZ]',              # 3GHz
            r'@[0-9]+\.[0-9]+[gG][hH][zZ]',     # @2.4GHz
            r'@[0-9]+[gG][hH][zZ]',             # @3GHz
            r'[0-9]+[mM][hH][zZ]',              # 1000MHz
        ]

        if any(re.match(pattern, token, re.IGNORECASE) for pattern in speed_patterns):
            return True

        return False

    def _comes_after_cpu_brand(tokens: List[str], index: int) -> bool:
        """Check if a token comes AFTER any CPU brand in the token sequence."""
        cpu_brands = ['intel', 'amd', 'apple']

        # Find all CPU brand positions
        cpu_brand_positions = []
        for i, token in enumerate(tokens):
            if token.lower() in cpu_brands:
                cpu_brand_positions.append(i)

        # If no CPU brands found, return False (require explicit CPU brands for standalone models)
        if not cpu_brand_positions:
            return False

        # Check if current index comes after ANY CPU brand position
        return any(index > brand_pos for brand_pos in cpu_brand_positions)

    token = tokens[i]

    if logger:
            logger.debug(
                f"CPU: extract_other_cpu_models called for token at {i}: {token}")

    # Check for Apple M-series processors first
    if extract_apple_m_processors(tokens, consumed, results, i, context_available, logger):
        if logger:
            logger.debug(
                f"CPU: extract_apple_m_processors returned True for token at {i}")
        return True

    # Check for Pentium processors
    if token.lower() == "pentium" and i + 1 < len(tokens):
        if logger:
            logger.debug(f"CPU: Found Pentium processor at {i}")
        # Look for model patterns starting from the next token
        indices = [i]  # Start with "Pentium"

        model_idx = i + 1
        while model_idx < len(tokens) and model_idx in consumed:
            model_idx += 1  # Skip consumed tokens

        if model_idx < len(tokens):
            # Check for Pentium model pattern (3-4 digits followed by optional letters)
            if re.match(r"[0-9]{3,4}[a-zA-Z]*", tokens[model_idx], re.IGNORECASE):
                indices.append(model_idx)
                consumed.add(model_idx)
                results.append(indices)
                consumed.add(i)  # Mark "Pentium" as consumed
                return True

    # Check for Core i-series with sequence pattern
    if token.lower().startswith(("i3-", "i5-", "i7-", "i9-")):
        # Skip very short post-dash fragments (≤2 digits) – these are generation numbers
        short_part = token.split("-")[1]
        if len(short_part) <= 2 and short_part.isdigit():
            return False  # leave for CPUGenerationExtractor
        if logger:
            logger.debug(
                f"CPU: Found Core i-series with sequence pattern at {i}")
        seq_indices = [i]
        consumed.add(i)
        # Look for slash-separated sequence
        j = i + 1
        while j + 1 < len(tokens):
            if tokens[j] == '/' and tokens[j + 1].lower().startswith(("i3-", "i5-", "i7-", "i9-")):
                seq_indices.append(j + 1)
                consumed.add(j)
                consumed.add(j + 1)
                j += 2
            else:
                break
        results.append(seq_indices)
        return True

    # Check for Core M series processors (m3-, m5-, m7-)
    elif token.lower().startswith(("m3-", "m5-", "m7-")):
        if logger:
            logger.debug(f"CPU: Found Core M series at {i}")
        # Only match if there are CPU-related tokens somewhere in the title AND adjacent to this token
        if context_available and _is_adjacent_to_cpu_context(tokens, i):
            results.append([i])
            consumed.add(i)
            return True

    # Check for Celeron J/N/G-series processors
    elif re.match(r"[JNG][0-9]{4}", token, re.IGNORECASE):
        if logger:
            logger.debug(f"CPU: Found Celeron J/N/G-series at {i}")
        # Only match if there are CPU-related tokens somewhere in the title AND adjacent to this token
        if context_available and _is_adjacent_to_cpu_context(tokens, i):
            results.append([i])
            consumed.add(i)
            return True

    # Check for standalone model (without i-prefix) - REQUIRES ADJACENT CPU CONTEXT
    elif re.match(r"\d{4}[a-zA-Z]+", token):
        if logger:
            logger.debug(f"CPU: Found standalone model at {i}")
        # Only match if:
        # 1. There are CPU-related tokens somewhere in the title AND
        # 2. This token is not preceded by GPU-related keywords AND
        # 3. This token comes AFTER any CPU brand (Intel/AMD/Apple) in the sequence AND
        # 4. This token is immediately adjacent to CPU-related context
        if (context_available and
            not _is_preceded_by_gpu_keywords(tokens, i) and
            _comes_after_cpu_brand(tokens, i) and
            _is_adjacent_to_cpu_context(tokens, i)):
            results.append([i])
            consumed.add(i)
            return True

    if logger:
            logger.debug(f"CPU: No match found for token at {i}")
    return False


def cpu_extract(tokens: List[str], consumed: Set[int], name: str, base_extract_func=None, logger=None) -> List[List[int]]:
    """Find CPU models with special handling for slash-separated sequences and sequential CPU patterns."""
    results = []

    if name == "cpu_model":
        if logger:
            logger.debug(f"CPU: cpu_extract called with tokens: {tokens}")
            logger.debug(f"CPU: consumed at start: {consumed}")

        # Check if there's a multiple cpu_generation extractor configured
        has_multiple_generation_extractor = any(
            config["name"] == "cpu_generation" and config.get(
                "multiple", False)
            for config in extractor_config
        )

        if logger:
            logger.debug(
                f"CPU: has_multiple_generation_extractor: {has_multiple_generation_extractor}")

        # Check for word "Processor" which should be ignored
        processor_indices = [i for i, t in enumerate(
            tokens) if t.lower() == "processor"]
        for idx in processor_indices:
            consumed.add(idx)  # Mark "Processor" tokens as consumed

        # Check if we have CPU context available
        context_available = has_cpu_context(tokens)
        if logger:
            logger.debug(f"CPU: context_available: {context_available}")

        # Enhanced logic to detect mixed CPU types (Intel + Apple)
        i = 0
        while i < len(tokens):
            if i in consumed:
                if logger:
                    logger.debug(
                        f"CPU: Skipping consumed token at index {i}: {tokens[i]}")
                i += 1
                continue

            # NEW: Skip short generation patterns entirely - let CPU generation extractor handle them
            if re.match(r"i[3579]-\d{1,2}$", tokens[i], re.IGNORECASE):
                if logger:
                    logger.debug(
                        f"CPU: Skipping short generation pattern token at index {i}: {tokens[i]} - leaving for CPU generation extractor")
                i += 1
                continue

            if logger:
                logger.debug(
                    f"CPU: Processing token at index {i}: {tokens[i]}")

            # NEW: Handle incomplete slash-separated CPU models like "i5-8250U/7200U"
            if (tokens[i].lower().startswith(("i3-", "i5-", "i7-", "i9-")) and
                i + 2 < len(tokens) and tokens[i + 1] == "/" and
                re.match(r'\d{2,4}[a-zA-Z]*$', tokens[i + 2])):

                if logger:
                    logger.debug(
                        f"CPU: Found incomplete slash-separated CPU pattern at {i}")

                # Extract the family prefix from the first CPU
                first_cpu = tokens[i]
                family_prefix = first_cpu.split('-')[0]  # e.g., "i5"
                incomplete_model = tokens[i + 2]  # e.g., "7200U"

                # Build a virtual complete second CPU model by intelligently filling in any
                # missing leading digits taken from the first CPU's numeric portion.  For
                # example, 'i7-2640/20M' becomes the pair 2640M and 2620M.

                original_token = tokens[i + 2]

                first_digits_match = re.match(
                    r'[iI][3579]-([0-9]{3,4})([A-Za-z]*)', first_cpu)
                second_digits_match = re.match(
                    r'([0-9]+)([A-Za-z]*)', incomplete_model)

                if first_digits_match and second_digits_match:
                    leading_digits = first_digits_match.group(
                        1)   # e.g., '2640'
                    second_digits = second_digits_match.group(
                        1)    # e.g., '20'
                    second_suffix = second_digits_match.group(
                        2)    # e.g., 'M' (may be empty)

                    # Prefix missing leading digits when the second fragment is shorter
                    if len(second_digits) < len(leading_digits):
                        missing_prefix = leading_digits[:len(
                            leading_digits) - len(second_digits)]
                        reconstructed_digits = f"{missing_prefix}{second_digits}"
                    else:
                        reconstructed_digits = second_digits

                    complete_model_body = f"{reconstructed_digits}{second_suffix}"
                    complete_model = f"{family_prefix}-{complete_model_body}"
                else:
                    # Fallback – simply prefix the family if pattern matching fails
                    complete_model = f"{family_prefix}-{incomplete_model}"

                # Add both CPUs as separate results
                results.append([i])  # First CPU (complete)
                consumed.add(i)

                # Don't consume slash if there's a generation extractor
                if not has_multiple_generation_extractor:
                    consumed.add(i + 1)

                # Temporarily replace the incomplete token with the complete model for processing
                tokens[i + 2] = complete_model
                results.append([i + 2])  # Second CPU (now complete)
                consumed.add(i + 2)

                # NOTE: Do NOT restore the original token – we want downstream processing
                # (process_match) to see the fully reconstructed model.
                i += 3
                continue

            # NEW: Handle Core M series incomplete patterns like "m3-7Y30/6Y54"
            elif (tokens[i].lower().startswith(("m3-", "m5-", "m7-")) and
                  i + 2 < len(tokens) and tokens[i + 1] == "/" and
                  re.match(r'[a-zA-Z0-9]+$', tokens[i + 2])):

                if logger:
                    logger.debug(
                        f"CPU: Found Core M series incomplete pattern at {i}")

                # Extract the family prefix from the first CPU
                first_cpu = tokens[i]
                family_prefix = first_cpu.split('-')[0]  # e.g., "m3"
                incomplete_model = tokens[i + 2]  # e.g., "6Y54"

                # Create a virtual complete second CPU model
                original_token = tokens[i + 2]
                complete_model = f"{family_prefix}-{incomplete_model}"

                # Add both CPUs as separate results
                results.append([i])  # First CPU (complete)
                consumed.add(i)

                if not has_multiple_generation_extractor:
                    consumed.add(i + 1)

                # Temporarily replace the incomplete token with the complete model for processing
                tokens[i + 2] = complete_model
                results.append([i + 2])  # Second CPU (now complete)
                consumed.add(i + 2)

                # NOTE: Do NOT restore the original token – we want downstream processing
                # (process_match) to see the fully reconstructed model.
                i += 3
                continue

            # Handle standalone i-series in slash-separated sequences like "i9/i7"
            if (tokens[i].lower() in ["i3", "i5", "i7", "i9"] and
                i + 1 < len(tokens) and tokens[i + 1] == "/" and
                i + 2 < len(tokens) and tokens[i + 2].lower() in ["i3", "i5", "i7", "i9"]):

                if logger:
                    logger.debug(
                        f"CPU: Found standalone i-series slash sequence at {i}")

                # Found slash-separated i-series sequence
                results.append([i])  # First CPU (e.g., "i9")
                consumed.add(i)

                # Skip the slash (don't consume if there's a generation extractor)
                if not has_multiple_generation_extractor:
                    consumed.add(i + 1)

                # Add second CPU (e.g., "i7")
                results.append([i + 2])
                consumed.add(i + 2)

                i += 3
                continue

            # Handle standalone i-series tokens when CPU context is available
            elif (tokens[i].lower() in ["i3", "i5", "i7", "i9"] and
                  (context_available or any(t.lower() in ["intel", "core", "apple"] for t in tokens))):
                if logger:
                    logger.debug(
                        f"CPU: Found standalone i-series token at {i}")
                results.append([i])
                consumed.add(i)
                i += 1
                continue

            # NEW: Check for Intel Core Ultra patterns like "Core Ultra 5 135U"
            if (tokens[i].lower() == "core" and
                i + 1 < len(tokens) and tokens[i + 1].lower() == "ultra" and
                i + 2 < len(tokens) and tokens[i + 2] in ["5", "7", "9"]):

                if logger:
                    logger.debug(f"CPU: Found Core Ultra pattern at index {i}")

                # Look for model number after the series number
                model_idx = i + 3
                while model_idx < len(tokens) and model_idx in consumed:
                    model_idx += 1

                if model_idx < len(tokens) and re.match(r'\d{3,4}[a-zA-Z]*$', tokens[model_idx]):
                    # Found complete Core Ultra pattern: Core Ultra 5 135U
                    first_cpu_indices = [i, i + 1, i + 2, model_idx]
                    for idx in first_cpu_indices:
                        consumed.add(idx)

                    if logger:
                        logger.debug(
                            f"CPU: Found complete Core Ultra: {[tokens[idx] for idx in first_cpu_indices]}")

                    # Look for slash-separated sequence
                    next_idx = model_idx + 1
                    while next_idx < len(tokens) and next_idx in consumed:
                        next_idx += 1

                    if (next_idx < len(tokens) and tokens[next_idx] == "/" and
                        next_idx + 1 < len(tokens)):

                        # Check for different patterns after slash
                        sep_idx = next_idx
                        after_slash_idx = next_idx + 1

                        # Pattern 1: Complete Core Ultra (Core Ultra 7 165H)
                        if (after_slash_idx + 3 < len(tokens) and
                            tokens[after_slash_idx].lower() == "core" and
                            tokens[after_slash_idx + 1].lower() == "ultra" and
                            tokens[after_slash_idx + 2] in ["5", "7", "9"] and
                            re.match(r'\d{3,4}[a-zA-Z]*$', tokens[after_slash_idx + 3])):

                            second_cpu_indices = [
                                after_slash_idx, after_slash_idx + 1, after_slash_idx + 2, after_slash_idx + 3]
                            for idx in second_cpu_indices:
                                consumed.add(idx)
                            if not has_multiple_generation_extractor:
                                consumed.add(sep_idx)
                            results.append(first_cpu_indices)
                            results.append(second_cpu_indices)
                            if logger:
                                logger.debug(
                                    f"CPU: Found slash-separated Core Ultra: {[tokens[idx] for idx in second_cpu_indices]}")
                            i = after_slash_idx + 4
                            continue

                        # Pattern 2: Short form (Ultra 7 165H)
                        elif (after_slash_idx + 2 < len(tokens) and
                              tokens[after_slash_idx].lower() == "ultra" and
                              tokens[after_slash_idx + 1] in ["5", "7", "9"] and
                              re.match(r'\d{3,4}[a-zA-Z]*$', tokens[after_slash_idx + 2])):

                            # Reuse "Core" from first
                            second_cpu_indices = [
                                i, after_slash_idx, after_slash_idx + 1, after_slash_idx + 2]
                            for idx in [after_slash_idx, after_slash_idx + 1, after_slash_idx + 2]:
                                consumed.add(idx)
                            if not has_multiple_generation_extractor:
                                consumed.add(sep_idx)
                            results.append(first_cpu_indices)
                            results.append(second_cpu_indices)
                            if logger:
                                logger.debug(
                                    f"CPU: Found slash-separated Core Ultra short form: {[tokens[idx] for idx in second_cpu_indices]}")
                            i = after_slash_idx + 3
                            continue

                        # Pattern 3: Just series and model (7 165H)
                        elif (after_slash_idx + 1 < len(tokens) and
                              tokens[after_slash_idx] in ["5", "7", "9"] and
                              re.match(r'\d{3,4}[a-zA-Z]*$', tokens[after_slash_idx + 1])):

                            # Reuse "Core Ultra" from first
                            second_cpu_indices = [
                                i, i + 1, after_slash_idx, after_slash_idx + 1]
                            for idx in [after_slash_idx, after_slash_idx + 1]:
                                consumed.add(idx)
                            if not has_multiple_generation_extractor:
                                consumed.add(sep_idx)
                            results.append(first_cpu_indices)
                            results.append(second_cpu_indices)
                            if logger:
                                logger.debug(
                                    f"CPU: Found slash-separated Core Ultra minimal form: {[tokens[idx] for idx in second_cpu_indices]}")
                            i = after_slash_idx + 2
                            continue

                        # Pattern 4: Just model number (165H)
                        elif re.match(r'\d{3,4}[a-zA-Z]*$', tokens[after_slash_idx]):
                            # Reuse "Core Ultra 5" from first
                            second_cpu_indices = [
                                i, i + 1, i + 2, after_slash_idx]
                            consumed.add(after_slash_idx)
                            if not has_multiple_generation_extractor:
                                consumed.add(sep_idx)
                            results.append(first_cpu_indices)
                            results.append(second_cpu_indices)
                            if logger:
                                logger.debug(
                                    f"CPU: Found slash-separated Core Ultra model only: {[tokens[idx] for idx in second_cpu_indices]}")
                            i = after_slash_idx + 1
                            continue

                    # No slash pattern, just single Core Ultra
                    results.append(first_cpu_indices)
                    i = model_idx + 1
                    continue
                else:
                    # Core Ultra without model number, treat as family only
                    family_indices = [i, i + 1, i + 2]
                    for idx in family_indices:
                        consumed.add(idx)
                    results.append(family_indices)
                    i = i + 3
                    continue

            # NEW: Check for Pentium/Celeron patterns
            # Example: "Intel Pentium Silver/Celeron" or "Pentium/Celeron"
            if (tokens[i].lower() in ["intel", "pentium"] and
                ((tokens[i].lower() == "intel" and i + 1 < len(tokens) and tokens[i + 1].lower() == "pentium") or
                 tokens[i].lower() == "pentium")):

                start_idx = i if tokens[i].lower() == "pentium" else i + 1
                if logger:
                    logger.debug(
                        f"CPU: Found potential Pentium pattern at index {start_idx}: {tokens[start_idx]}")

                # Look for variant (Silver/Gold/Bronze) and Celeron
                variant = None
                celeron_idx = None
                slash_idx = None

                # Check next few tokens for variant and Celeron
                for j in range(start_idx + 1, min(start_idx + 5, len(tokens))):
                    if j in consumed:
                        continue
                    tok_lower = tokens[j].lower()
                    if tok_lower in ["silver", "gold", "bronze"] and variant is None:
                        variant = tokens[j].capitalize()
                    elif tok_lower == "/":
                        slash_idx = j
                    elif tok_lower == "celeron":
                        celeron_idx = j
                        break

                if celeron_idx is not None:
                    # Found Pentium/Celeron pattern
                    if logger:
                        logger.debug(
                            f"CPU: Found Pentium/Celeron pattern: Pentium at {start_idx}, variant={variant}, Celeron at {celeron_idx}")

                    # Create a single result that includes all relevant tokens
                    pentium_celeron_indices = []
                    if tokens[i].lower() == "intel":
                        pentium_celeron_indices.append(i)  # Intel
                    pentium_celeron_indices.append(start_idx)  # Pentium
                    if variant:
                        # Find the variant token index
                        for j in range(start_idx + 1, celeron_idx):
                            if tokens[j].lower() == variant.lower():
                                pentium_celeron_indices.append(j)
                                break
                    if slash_idx:
                        pentium_celeron_indices.append(slash_idx)  # /
                    pentium_celeron_indices.append(celeron_idx)  # Celeron

                    results.append(pentium_celeron_indices)
                    for idx in pentium_celeron_indices:
                        consumed.add(idx)

                    if logger:
                        logger.debug(
                            f"CPU: Added Pentium/Celeron result: {pentium_celeron_indices}")
                    i = celeron_idx + 1
                    continue
                elif variant:
                    # Found standalone Pentium variant (no Celeron)
                    if logger:
                        logger.debug(
                            f"CPU: Found standalone Pentium variant: Pentium at {start_idx}, variant={variant}")

                    pentium_variant_indices = []
                    if tokens[i].lower() == "intel":
                        pentium_variant_indices.append(i)  # Intel
                    pentium_variant_indices.append(start_idx)  # Pentium
                    # Find the variant token index
                    for j in range(start_idx + 1, min(start_idx + 3, len(tokens))):
                        if tokens[j].lower() == variant.lower():
                            pentium_variant_indices.append(j)
                            break

                    results.append(pentium_variant_indices)
                    for idx in pentium_variant_indices:
                        consumed.add(idx)

                    if logger:
                        logger.debug(
                            f"CPU: Added Pentium variant result: {pentium_variant_indices}")
                    i = max(pentium_variant_indices) + 1
                    continue

            # Check for multi-CPU patterns with different architectures
            # Example: "Core i7 10th/Apple M1" or "Intel Core i7/M1" or "Core i7 8th Gen i5 11th Gen"
            if (tokens[i].lower() == "core" and
                i + 1 < len(tokens) and
                tokens[i + 1].lower().startswith(("i3", "i5", "i7", "i9")) and
                not tokens[i + 1].lower().startswith(("i3-", "i5-", "i7-", "i9-"))):  # Not a full model

                if logger:
                    logger.debug(
                        f"CPU: Found Core i-series pattern at index {i}: {tokens[i]} {tokens[i+1]}")

                # Found first CPU family (Core iX)
                first_cpu_indices = [i, i + 1]
                consumed.add(i)
                consumed.add(i + 1)

                if logger:
                    logger.debug(f"CPU: first_cpu_indices: {first_cpu_indices}")

                # Look for generation after Core iX - but only include if no multiple generation extractor
                gen_idx = i + 2
                while gen_idx < len(tokens) and gen_idx in consumed:
                    gen_idx += 1

                if logger:
                    logger.debug(f"CPU: Looking for generation at gen_idx: {gen_idx}")

                # Check if we have a generation specification and skip past it
                generation_found = False
                if (gen_idx < len(tokens) and
                    re.match(r"\d+(?:st|nd|rd|th)", tokens[gen_idx], re.IGNORECASE)):

                    if logger:
                        logger.debug(f"CPU: Found generation token at {gen_idx}: {tokens[gen_idx]}")

                    # Only include generation in CPU model match if no dedicated generation extractor
                    if not has_multiple_generation_extractor:
                        first_cpu_indices.append(gen_idx)
                        consumed.add(gen_idx)
                        generation_found = True

                        # Check for "Gen" after the generation number
                        if (gen_idx + 1 < len(tokens) and
                            tokens[gen_idx + 1].lower() == "gen"):
                            first_cpu_indices.append(gen_idx + 1)
                            consumed.add(gen_idx + 1)
                            gen_idx += 1
                    else:
                        # Skip generation tokens even if not consuming them - let the generation extractor handle them
                        gen_idx += 1  # Skip the generation number
                        if (gen_idx < len(tokens) and tokens[gen_idx].lower() == "gen"):
                            gen_idx += 1  # Skip "Gen" too

                if logger:
                    logger.debug(f"CPU: After generation processing, first_cpu_indices: {first_cpu_indices}, generation_found: {generation_found}")
                
                # Now look for separator (/ or slash) and potential second CPU
                next_idx = gen_idx + 1 if generation_found else gen_idx
                while next_idx < len(tokens) and next_idx in consumed:
                    next_idx += 1
                
                if logger:
                    logger.debug(f"CPU: Looking for separator at next_idx: {next_idx}")
                if next_idx < len(tokens):
                    if logger:
                        logger.debug(f"CPU: Token at next_idx: {tokens[next_idx]}")
                
                # Check for separator
                separator_found = False
                if (next_idx < len(tokens) and tokens[next_idx] == '/'):
                    if logger:
                        logger.debug(f"CPU: Found separator at {next_idx}")
                    # Don't consume separator if there's a generation extractor - let it handle the pattern
                    if not has_multiple_generation_extractor:
                        consumed.add(next_idx)
                    next_idx += 1
                    separator_found = True
                
                # Look for second CPU after separator (or without separator for some patterns)
                while next_idx < len(tokens) and next_idx in consumed:
                    next_idx += 1
                
                if logger:
                    logger.debug(f"CPU: Looking for second CPU at next_idx: {next_idx}")
                if next_idx < len(tokens):
                    if logger:
                        logger.debug(f"CPU: Potential second CPU token at {next_idx}: {tokens[next_idx]}")
                
                second_cpu_found = False
                if next_idx < len(tokens):
                    # Check for Apple brand + M-series
                    if (tokens[next_idx].lower() == "apple" and 
                        next_idx + 1 < len(tokens) and
                        re.match(r'^m[123]$', tokens[next_idx + 1].lower())):
                        
                        if logger:
                            logger.debug(f"CPU: Found Apple M-series pattern at {next_idx}: {tokens[next_idx]} {tokens[next_idx + 1]}")
                        
                        second_cpu_indices = [next_idx, next_idx + 1]
                        consumed.add(next_idx)
                        consumed.add(next_idx + 1)
                        
                        # Check for M-series variants (Pro, Max, Ultra)
                        if (next_idx + 2 < len(tokens) and 
                            tokens[next_idx + 2].lower() in ['pro', 'max', 'ultra']):
                            if logger:
                                logger.debug(f"CPU: Found M-series variant: {tokens[next_idx + 2]}")
                            second_cpu_indices.append(next_idx + 2)
                            consumed.add(next_idx + 2)
                        
                        second_cpu_found = True
                    
                    # Check for standalone M-series (without Apple prefix)
                    elif re.match(r'^m[123]$', tokens[next_idx].lower()):
                        if logger:
                            logger.debug(f"CPU: Found standalone M-series at {next_idx}: {tokens[next_idx]}")
                        
                        second_cpu_indices = [next_idx]
                        consumed.add(next_idx)
                        
                        # Check for M-series variants
                        if (next_idx + 1 < len(tokens) and 
                            tokens[next_idx + 1].lower() in ['pro', 'max', 'ultra']):
                            if logger:
                                logger.debug(f"CPU: Found M-series variant: {tokens[next_idx + 1]}")
                            second_cpu_indices.append(next_idx + 1)
                            consumed.add(next_idx + 1)
                        
                        second_cpu_found = True
                    
                    # Check for another Intel CPU family (standalone i3, i5, i7, i9)
                    elif (tokens[next_idx].lower().startswith(("i3", "i5", "i7", "i9")) and
                          not tokens[next_idx].lower().startswith(("i3-", "i5-", "i7-", "i9-")) and
                          len(tokens[next_idx]) == 2):  # Just i3, i5, i7, i9 (not i3-8130U)
                        
                        if logger:
                            logger.debug(f"CPU: Found second Intel CPU family at {next_idx}: {tokens[next_idx]}")
                        
                        second_cpu_indices = [next_idx]
                        consumed.add(next_idx)
                        
                        # Look for generation after second CPU - but only include if no multiple generation extractor
                        second_gen_idx = next_idx + 1
                        while second_gen_idx < len(tokens) and second_gen_idx in consumed:
                            second_gen_idx += 1
                        
                        if (second_gen_idx < len(tokens) and 
                            re.match(r"\d+(?:st|nd|rd|th)", tokens[second_gen_idx], re.IGNORECASE)):
                            
                            # Only include generation if no dedicated generation extractor
                            if not has_multiple_generation_extractor:
                                second_cpu_indices.append(second_gen_idx)
                                consumed.add(second_gen_idx)
                                
                                if (second_gen_idx + 1 < len(tokens) and 
                                    tokens[second_gen_idx + 1].lower() == "gen"):
                                    second_cpu_indices.append(second_gen_idx + 1)
                                    consumed.add(second_gen_idx + 1)
                        
                        second_cpu_found = True
                    
                    # Check for standalone numbers that should be interpreted as Intel CPU families (e.g., "7" -> "i7")
                    elif (separator_found and 
                          tokens[next_idx] in ["3", "5", "7", "9"]):  # Just single digits that map to i3, i5, i7, i9
                        
                        if logger:
                            logger.debug(f"CPU: Found standalone CPU number at {next_idx}: {tokens[next_idx]} (interpreting as i{tokens[next_idx]})")
                        
                        second_cpu_indices = [next_idx]
                        consumed.add(next_idx)
                        
                        # Look for generation after second CPU - but only include if no multiple generation extractor
                        second_gen_idx = next_idx + 1
                        while second_gen_idx < len(tokens) and second_gen_idx in consumed:
                            second_gen_idx += 1
                        
                        if (second_gen_idx < len(tokens) and 
                            re.match(r"\d+(?:st|nd|rd|th)", tokens[second_gen_idx], re.IGNORECASE)):
                            
                            # Only include generation if no dedicated generation extractor
                            if not has_multiple_generation_extractor:
                                second_cpu_indices.append(second_gen_idx)
                                consumed.add(second_gen_idx)
                                
                                if (second_gen_idx + 1 < len(tokens) and 
                                    tokens[second_gen_idx + 1].lower() == "gen"):
                                    second_cpu_indices.append(second_gen_idx + 1)
                                    consumed.add(second_gen_idx + 1)
                        
                        second_cpu_found = True
                
                if logger:
                    logger.debug(f"CPU: second_cpu_found: {second_cpu_found}")
                if second_cpu_found:
                    if logger:
                        logger.debug(f"CPU: second_cpu_indices: {second_cpu_indices}")
                
                # Add CPUs as separate results
                results.append(first_cpu_indices)
                if second_cpu_found:
                    results.append(second_cpu_indices)
                
                if logger:
                    logger.debug(f"CPU: Added CPU results: {results}")
                
                # Continue from after the last processed token
                if second_cpu_found:
                    i = max(second_cpu_indices) + 1
                else:
                    i = max(first_cpu_indices) + 1
                continue
            
            # Try Xeon extraction
            if extract_xeon_processors(tokens, consumed, results, i):
                if logger:
                    logger.debug(f"CPU: Found Xeon processor at {i}")
                i += 1
                continue
                
            # Check for Core i-series with slash-separated sequence - RETURN SEPARATE MATCHES
            if tokens[i].lower().startswith(("i3-", "i5-", "i7-", "i9-")):
                if logger:
                    logger.debug(f"CPU: Found Core i-series with dash at {i}: {tokens[i]}")
                # First add the initial CPU as its own match
                results.append([i])
                consumed.add(i)
                
                # Look for slash-separated sequence and add each as separate match
                j = i + 1
                while j + 1 < len(tokens):
                    if (tokens[j] == '/' and 
                        (tokens[j + 1].lower().startswith(("i3-", "i5-", "i7-", "i9-")) or
                         re.match(r"\d{3,5}[a-zA-Z]*$", tokens[j + 1]))):
                        # Add each subsequent CPU as a separate result
                        results.append([j + 1])
                        # Only consume separator if no generation extractor
                        if not has_multiple_generation_extractor:
                            consumed.add(j)
                        consumed.add(j + 1)
                        j += 2
                    else:
                        break
                i = j
                continue
                
            # Try other CPU model extraction (including Apple M-series)
            if extract_other_cpu_models(tokens, consumed, results, i, context_available):
                if logger:
                    logger.debug(f"CPU: Found other CPU model at {i}")
                i += 1
                continue
            
            i += 1
        
        if logger:
            logger.debug(f"CPU: Final results: {results}")
        if logger:
            logger.debug(f"CPU: Final consumed: {consumed}")
        return results
    else:
        # For other extractors, use standard extraction
        if base_extract_func:
            return base_extract_func(tokens, consumed)
        return []
        
def clean_value(value):
    """Clean a value by removing trailing punctuation like commas."""
    return re.sub(r'[,;:\.]+$', '', value.strip())

def normalize_version(version_str):
    """Normalize version strings to ensure consistent formatting."""
    # Convert uppercase V to lowercase v in version numbers
    match = re.match(r"[Vv](\d+)", version_str, re.IGNORECASE)
    if match:
        return f"v{match.group(1)}"
    return version_str

def normalize_cpu_model(model_str: str) -> str:
    """Normalize CPU model strings to ensure consistent dash formatting and remove unwanted symbols."""
    if not model_str:
        return model_str
    
    # Remove @ symbol, TM, and other unwanted symbols
    model_str = re.sub(r'[@™®\s]+', ' ', model_str)  # Changed to replace '@' with space and handle other symbols
    
    # Normalize Xeon E-series models (E3, E5, E7, W-series)
    # Pattern: "E5 2687W v4" -> "E5-2687W v4"
    model_str = re.sub(r'\b([EWX])([357])\s+(\d{4}[A-Z]*)', r'\1\2-\3', model_str)
    
    # Normalize Core i-series models 
    # Pattern: "i5 8250U" -> "i5-8250U", "i7 10700K" -> "i7-10700K"
    model_str = re.sub(r'\b(i[3579])\s+(\d{3,4}[A-Z]*)', r'\1-\2', model_str)
    
    # Normalize Core m-series models
    # Pattern: "m3 7Y30" -> "m3-7Y30", "m5 6Y54" -> "m5-6Y54"
    model_str = re.sub(r'\b(m[357])\s+(\d[A-Z0-9]*)', r'\1-\2', model_str)
    
    # NEW: Strip generic 'E' prefix (space or dash) from newer Xeon E models (e.g., "E-2176M" -> "2176M",
    #       "E 2288G" -> "2288G")
    model_str = re.sub(r'\bE[\s-]+(\d{4}[A-Z]*)', r'\1', model_str, flags=re.IGNORECASE)
    
    return model_str
    
def is_apple_silicon_cpu(cpu_num, data_dict):
    """Check if a CPU is Apple Silicon based on brand and family."""
    brand_key = f'cpu_brand{cpu_num}' if cpu_num else 'cpu_brand'
    family_key = f'cpu_family{cpu_num}' if cpu_num else 'cpu_family'
    
    cpu_brand = data_dict.get(brand_key, '')
    cpu_family = data_dict.get(family_key, '')
    
    return (cpu_brand == 'Apple' or 'Apple M' in cpu_family)

def process_match(tokens: List[str], match_indices: List[int], base_process_match_func=None) -> Dict:
    """Process CPU matches with proper handling for both single and multi-CPU models."""
    result = {}
    

    
    # NEW: Handle Pentium/Celeron patterns first (highest priority)
    tokens_text = " ".join(tokens[i] for i in match_indices).lower()
    
    # Check for Pentium/Celeron slash patterns
    if "pentium" in tokens_text and "celeron" in tokens_text:
        # Detect variant (Silver / Gold / Bronze) within the matched slice
        variant = None
        for idx in match_indices:
            tok_low = tokens[idx].lower()
            if tok_low in {"silver", "gold", "bronze"}:
                variant = tok_low.capitalize()
                break

        pentium_family = f"Pentium {variant}" if variant else "Pentium"
        
        # Slash-separated pair – output both families
        result["cpu_brand"] = "Intel"
        result["cpu_family"] = pentium_family  # primary = Pentium variant
        result["cpu_family1"] = pentium_family
        result["cpu_family2"] = "Celeron"
        return result
    
    # Check for standalone Pentium variant (no Celeron)
    elif "pentium" in tokens_text:
        # Detect variant (Silver / Gold / Bronze) within the matched slice
        variant = None
        for idx in match_indices:
            tok_low = tokens[idx].lower()
            if tok_low in {"silver", "gold", "bronze"}:
                variant = tok_low.capitalize()
                break

        pentium_family = f"Pentium {variant}" if variant else "Pentium"
        
        result["cpu_brand"] = "Intel"
        result["cpu_family"] = pentium_family
        return result
    
    # NEW: Handle Intel Core Ultra patterns
    if len(match_indices) >= 3:
        tokens_text = " ".join(tokens[i] for i in match_indices).lower()
        
        # Check for Core Ultra patterns: "core ultra 5", "core ultra 5 135u"
        if re.match(r"core\s+ultra\s+[579](?:\s+\d{3,4}[a-z]*)?$", tokens_text):
            series_num = None
            model_num = None
            
            for i, idx in enumerate(match_indices):
                token = tokens[idx].lower()
                if token in ["5", "7", "9"]:
                    series_num = token
                elif re.match(r'\d{3,4}[a-z]*$', token):
                    model_num = token
            
            if series_num:
                result["cpu_brand"] = "Intel"
                result["cpu_family"] = f"Core Ultra {series_num}"
                
                if model_num:
                    result["cpu_model"] = model_num
                    
                    # Extract suffix if present
                    suffix_match = re.search(r"([a-zA-Z]+)$", model_num)
                    if suffix_match:
                        result["cpu_suffix"] = suffix_match.group(1).upper()
                    
                    # Determine generation based on model number (Intel Core Ultra is 1st gen for now)
                    # Core Ultra 100 series = 1st Gen Ultra (Meteor Lake)
                    # Core Ultra 200 series = 2nd Gen Ultra (future)
                    if model_num.startswith("1"):
                        result["cpu_generation"] = "1st Gen Ultra"
                    elif model_num.startswith("2"):
                        result["cpu_generation"] = "2nd Gen Ultra"
                
                return result
    
    # Handle Core i5/7 type patterns first
    if len(match_indices) == 2:
        tokens_text = " ".join(tokens[i] for i in match_indices).lower()
        
        # Check for "core i5/7" pattern
        if re.match(r"core\s+i[3579]/[3579]$", tokens_text):
            core_token = tokens[match_indices[0]]
            i_token = tokens[match_indices[1]]
            
            # Extract the two CPU families from the i5/7 pattern
            if "/" in i_token:
                first_family = i_token.split("/")[0]
                second_family = "i" + i_token.split("/")[1]
                
                # Return both CPU families
                result["cpu_family1"] = f"Core {first_family}"
                result["cpu_family2"] = f"Core {second_family}"
                result["cpu_brand"] = "Intel"
                
                # Also set the primary cpu_family to the first one
                result["cpu_family"] = f"Core {first_family}"
                return result
    
    # Check if this is a standalone i-series token (like "i5" for second CPU)
    if (len(match_indices) >= 2 and len(match_indices) <= 4):
        tokens_text = " ".join(tokens[i] for i in match_indices).lower()
        
        # Pattern for standalone i-series with generation: "i5 11th gen"
        if re.match(r"i[3579]\s+\d+(?:st|nd|rd|th)\s+gen$", tokens_text):
            for i, idx in enumerate(match_indices):
                token = tokens[idx].lower()
                if token.startswith("i") and len(token) == 2:
                    result["cpu_family"] = f"Core {token}"
                    result["cpu_brand"] = "Intel"
                elif re.match(r"\d+(?:st|nd|rd|th)", token):
                    if i + 1 < len(match_indices) and tokens[match_indices[i + 1]].lower() == "gen":
                        gen_match = re.match(r"(\d+)", token)
                        if gen_match:
                            gen_num = gen_match.group(1)
                            suffix = token[len(gen_num):]
                            result["cpu_generation"] = f"{gen_num}{suffix} Gen"
            return result
        
        # Pattern for Core i-series with generation: "core i7 8th gen" or "core i7 10th"
        elif re.match(r"core\s+i[3579](?:\s+\d+(?:st|nd|rd|th))?(?:\s+gen)?$", tokens_text):
            cpu_family_set = False
            cpu_brand_set = False
            generation_set = False
            
            for i, idx in enumerate(match_indices):
                token = tokens[idx].lower()
                if token == "core" and not cpu_family_set:
                    # Next token should be iX
                    if i + 1 < len(match_indices):
                        ix_token = tokens[match_indices[i + 1]]
                        result["cpu_family"] = f"Core {ix_token}"
                        result["cpu_brand"] = "Intel"
                        cpu_family_set = True
                        cpu_brand_set = True
                elif re.match(r"\d+(?:st|nd|rd|th)", token) and not generation_set:
                    # This is generation number
                    gen_match = re.match(r"(\d+)", token)
                    if gen_match:
                        gen_num = gen_match.group(1)
                        suffix = token[len(gen_num):]
                        result["cpu_generation"] = f"{gen_num}{suffix} Gen"
                        generation_set = True
            return result
        
        # Pattern for Intel Core M series: "intel core m3" or "core m3"
        elif re.match(r"(?:intel\s+)?core\s+m[357]$", tokens_text):
            result["cpu_brand"] = "Intel"
            for i, idx in enumerate(match_indices):
                token = tokens[idx].lower()
                if token.startswith("m") and len(token) == 2:
                    result["cpu_family"] = f"Core {token}"
                    result["cpu_model"] = token.upper()  # Store as M3, M5, M7
            return result
        
        # Pattern for Apple + M-series: "apple m1" or "apple m1 pro"
        elif re.match(r"apple\s+m[123](?:\s+(?:pro|max|ultra))?$", tokens_text):
            result["cpu_brand"] = "Apple"
            m_chip_parts = []
            for i, idx in enumerate(match_indices):
                token = tokens[idx]
                if token.lower() != "apple":
                    m_chip_parts.append(token)
            
            if m_chip_parts:
                m_chip_name = " ".join(m_chip_parts)
                result["cpu_family"] = f"Apple {m_chip_name}"
                result["cpu_model"] = m_chip_name
                
                # Get generation for Apple M-series
                generation = get_apple_m_generation(m_chip_name)
                if generation:
                    result["cpu_generation"] = generation
            
            return result
    
    # Check if this is an Apple M-series model (single or multiple tokens)
    if len(match_indices) >= 1:
        tokens_text = " ".join(tokens[i] for i in match_indices)
        
        # FIXED: More intelligent Intel/Core context detection
        # Only check for Intel context in the local vicinity of this match, not globally
        intel_core_context = False
        intel_cpu_present = False
        
        # Check for Intel/Core only in a reasonable range around this match
        match_start = min(match_indices)
        match_end = max(match_indices)
        check_start = max(0, match_start - 5)  # Look 5 tokens before
        check_end = min(len(tokens), match_end + 5)  # Look 5 tokens after
        
        for j in range(check_start, check_end):
            if j not in match_indices and tokens[j].lower() in ['intel', 'core']:
                intel_core_context = True
                break
        
        # Check for explicit Intel processor models in the local vicinity
        for j in range(check_start, check_end):
            if j not in match_indices and re.match(r'i[3579]-\d{3,4}[a-zA-Z]*', tokens[j], re.IGNORECASE):
                intel_cpu_present = True
                break
        
        # Check if this appears to be part of a model name
        is_likely_model_name = False
        if match_indices:
            first_idx = match_indices[0]
            if first_idx > 0:
                prev_token = tokens[first_idx - 1]
                model_patterns = [
                    r'x\d+',      # x360, x220, etc.
                    r'pavilion',  # HP Pavilion
                    r'elite',     # HP Elite
                    r'envy',      # HP Envy
                    r'spectre',   # HP Spectre
                    r'latitude',  # Dell Latitude
                    r'inspiron',  # Dell Inspiron
                    r'thinkpad',  # Lenovo ThinkPad
                    r'yoga',      # Lenovo Yoga
                    r'surface',   # Microsoft Surface
                    r'\d+[a-z]*', # Various numeric model prefixes
                ]
                
                if any(re.match(pattern, prev_token.lower()) for pattern in model_patterns):
                    is_likely_model_name = True
        
        # Check for explicit Apple brand in the current match
        apple_brand_in_match = any(tokens[idx].lower() == 'apple' for idx in match_indices)
        
        # CRITICAL FIX: For standalone M1/M2/M3 tokens, check if Apple brand exists ANYWHERE in the full token list
        apple_brand_in_full_tokens = any(token.lower() == 'apple' for token in tokens)
        
        # FIXED: Be more permissive for Apple M-series detection
        if (re.match(r'^(?:Apple\s+)?[mM][123](?:\s+(?:Pro|Max|Ultra))?$', tokens_text, re.IGNORECASE) and
            not is_likely_model_name and
            (apple_brand_in_match or apple_brand_in_full_tokens or (not intel_core_context and not intel_cpu_present))):
            
            result["cpu_brand"] = "Apple"
            
            # Extract the M-chip part (remove "Apple" if present)
            m_chip_parts = []
            for i, idx in enumerate(match_indices):
                token = tokens[idx]
                if token.lower() != "apple":
                    m_chip_parts.append(token)
            
            if m_chip_parts:
                m_chip_name = " ".join(m_chip_parts)
                result["cpu_family"] = f"Apple {m_chip_name}"
                result["cpu_model"] = m_chip_name
                
                # Get generation for Apple M-series
                generation = get_apple_m_generation(m_chip_name)
                if generation:
                    result["cpu_generation"] = generation
            
            return result
    
    # Check if this is a Celeron model (single token like "J4105", "N5105", etc.)
    if len(match_indices) == 1:
        token = clean_value(tokens[match_indices[0]])
        
        # NEW: Check for standalone i-series CPU family (i3, i5, i7, i9)
        if re.match(r"^i[3579]$", token, re.IGNORECASE):
            result["cpu_family"] = f"Core {token.lower()}"
            result["cpu_brand"] = "Intel"
            return result
        
        # NEW: Check for standalone numbers that should be interpreted as Intel CPU families (3, 5, 7, 9 -> i3, i5, i7, i9)
        elif token in ["3", "5", "7", "9"]:
            # Check if there's Intel/Core context in the full token list
            intel_core_context = False
            all_tokens_text = " ".join(tokens).lower()
            if 'intel' in all_tokens_text or 'core' in all_tokens_text:
                result["cpu_family"] = f"Core i{token}"
                result["cpu_brand"] = "Intel"
                return result
        
        # NEW: Check for standalone Intel Core M series (M3, M5, M7) with Intel context
        elif re.match(r"^m[357]$", token, re.IGNORECASE):
            # Check if there's Intel/Core context in the full token list
            intel_core_context = False
            all_tokens_text = " ".join(tokens).lower()
            if 'intel' in all_tokens_text or 'core' in all_tokens_text:
                # This is likely Intel Core M3/M5/M7
                result["cpu_brand"] = "Intel"
                result["cpu_family"] = f"Core {token.lower()}"
                result["cpu_model"] = token.upper()  # Store as M3, M5, M7
                return result
        
        # Handle low-power J/N/G-series processors. Some (e.g. J4105) are
        # Celeron, while the 5000-series parts (J5005, N5000, etc.) are sold
        # as Pentium Silver.
        elif re.match(r"[JNG][0-9]{3,4}", token, re.IGNORECASE):
            result["cpu_brand"] = "Intel"
            result["cpu_model"] = token

            pentium_keyword   = any(t.lower() == "pentium" for t in tokens)
            starts_with_five  = len(token) > 1 and token[1] == "5"

            if pentium_keyword or starts_with_five:
                variant = next((t.capitalize() for t in tokens if t.lower() in ["silver", "gold", "bronze"]), None)
                result["cpu_family"] = f"Pentium {variant}" if variant else "Pentium"
            else:
                result["cpu_family"] = "Celeron"

            # For these series the leading letter is part of the model so we
            # do not split suffixes here.
            return result
        
        # Check for Core M series model (m3-, m5-, m7-)
        elif token.lower().startswith(("m3-", "m5-", "m7-")):
            prefix = token.split('-')[0].lower()
            model_number = token.split('-')[1]
            
            # Set family, brand, model  
            result["cpu_family"] = f"Core {prefix}"
            result["cpu_brand"] = "Intel"
            
            # Keep the full model with suffix
            result["cpu_model"] = model_number
            
            # Extract suffix if present
            suffix_match = re.search(r"^(\d+)([a-zA-Z][a-zA-Z0-9]*)$", model_number)
            if suffix_match:
                result["cpu_suffix"] = suffix_match.group(2)
                
            # Core M series generation detection (typically 5th-8th gen)
            if model_number.startswith(("5", "6", "7", "8")):
                first_digit = int(model_number[0])
                suffix = "th"
                if first_digit == 5:
                    suffix = "th"
                elif first_digit == 6:
                    suffix = "th" 
                elif first_digit == 7:
                    suffix = "th"
                elif first_digit == 8:
                    suffix = "th"
                result["cpu_generation"] = f"{first_digit}{suffix} Gen"
                
            return result
            
        # Check for Core i-series model (with prefix)
        elif token.lower().startswith(("i3-", "i5-", "i7-", "i9-")):
            prefix = token.split('-')[0].lower()
            model_number = token.split('-')[1]
            
            # NEW: Treat one- or two-digit fragments after the dash as generation numbers,
            # _not_ as full CPU models. This handles titles like
            #   "i5/i7-6/8/9th Gen" → generation list, not model "6".
            if re.match(r'^\d{1,2}$', model_number):
                # Heuristic – a genuine model always has ≥3 digits.
                # Only return CPU family and brand, let CPU generation extractor handle the generation
                result["cpu_family"] = f"Core {prefix}"
                result["cpu_brand"] = "Intel"
                # Don't set cpu_generation here - let the CPU generation extractor handle it
                return result
            
            # FIXED: Check if the "model number" is actually a generation indicator
            if re.match(r'\d+(?:st|nd|rd|th)$', model_number, re.IGNORECASE):
                # This is a generation indicator like "11th", not a model number
                # Just set the family and brand, don't set cpu_model
                result["cpu_family"] = f"Core {prefix}"
                result["cpu_brand"] = "Intel"
                gen_match = re.match(r"(\d+)", model_number)
                if gen_match:
                    gen_num = int(gen_match.group(1))
                    # Choose correct ordinal suffix
                    if 10 <= gen_num % 100 <= 20:
                        suffix = "th"
                    else:
                        suffix = {1: "st", 2: "nd", 3: "rd"}.get(gen_num % 10, "th")
                    result["cpu_generation"] = f"{gen_num}{suffix} Gen"
                return result
            
            # Set family, brand, model
            result["cpu_family"] = f"Core {prefix}"
            result["cpu_brand"] = "Intel"
            
            # Keep the full model with suffix for generation detection
            full_model = f"{prefix}-{model_number}"
            
            # Extract suffix if present but keep it in the model too
            suffix_match = re.search(r"^(\d+)([a-zA-Z][a-zA-Z0-9]*)$", model_number)
            if suffix_match:
                result["cpu_model"] = model_number  # Keep the full model with suffix
                result["cpu_suffix"] = suffix_match.group(2)  # Also extract suffix separately
            else:
                result["cpu_model"] = model_number
            
            # Detect generation using the full model with suffix
            gen = get_generation_from_map(full_model, result["cpu_family"])
            if not gen and result.get("cpu_model", "")[0].isdigit():
                # Fallback detection - only use 2 digits for models starting with 1x
                first_digit = int(result["cpu_model"][0])
                if first_digit == 1 and len(result["cpu_model"]) >= 4:
                    # For models starting with 1, use first 2 digits (10-14th gen)
                    gen_num = int(result["cpu_model"][:2])
                    suffix = "th"
                    if gen_num % 100 not in [11, 12, 13]:
                        suffix = {1: "st", 2: "nd", 3: "rd"}.get(gen_num % 10, "th")
                    gen = f"{gen_num}{suffix} Gen"
                else:
                    # For models starting with 2-9, use first digit only
                    suffix = "th"
                    if first_digit == 1:
                        suffix = "st"
                    elif first_digit == 2:
                        suffix = "nd"
                    elif first_digit == 3:
                        suffix = "rd"
                    gen = f"{first_digit}{suffix} Gen"
            
            if gen:
                result["cpu_generation"] = gen
                
            return result
            
        # Single model without i-prefix - FIXED: Now includes generation detection for standalone models
        elif re.match(r"\d{3,5}[a-zA-Z]*", token):
            result["cpu_model"] = token
            
            # Extract suffix if present
            suffix_match = re.search(r"([a-zA-Z][a-zA-Z0-9]*)$", token)
            if suffix_match:
                result["cpu_suffix"] = suffix_match.group(1)
            
            # FIXED: Add generation detection for standalone models like "3210M" but DON'T assume family
            gen = get_generation_from_standalone_model(token)
            if gen:
                result["cpu_generation"] = gen
                result["cpu_brand"] = "Intel"
                
                # NEW: Infer CPU family for slash-separated partial models
                # Look for an i5- pattern in the token stream to infer family
                for i, t in enumerate(tokens):
                    if re.match(r"i[3579]-\d+", t, re.IGNORECASE):
                        family_prefix = t.split('-')[0].lower()  # e.g., "i5"
                        result["cpu_family"] = f"Core {family_prefix}"
                        break
                
            return result
    
    # Check for Celeron processor with multiple tokens
    elif len(match_indices) >= 2 and any(tokens[i].lower() == "celeron" for i in range(len(tokens))):
        result["cpu_brand"] = "Intel"
        result["cpu_family"] = "Celeron"  # Keep as just "Celeron"
        
        # Find the model token (not "Celeron")
        model_tokens = [clean_value(tokens[i]) for i in match_indices if tokens[i].lower() != "celeron"]
        if model_tokens:
            model = " ".join(model_tokens)
            # Apply normalization to the model
            model = normalize_cpu_model(model)
            result["cpu_model"] = model
            
            # Handle different Celeron series based on model format
            if re.match(r"[JNG][0-9]{3,4}", model, re.IGNORECASE):
                # Keep family as just "Celeron" for consistency
                pass
            elif re.match(r"D[0-9]{3}", model, re.IGNORECASE):
                result["cpu_family"] = "Celeron D"
            elif re.match(r"M [0-9]{3}", model, re.IGNORECASE):
                result["cpu_family"] = "Celeron M"
                
        return result
    
    # Check for Pentium processor with multiple tokens
    elif len(match_indices) >= 2 and any(tokens[i].lower() == "pentium" for i in range(len(tokens))):
        result["cpu_brand"] = "Intel"
        result["cpu_family"] = "Pentium"
        
        # Find the model token (not "Pentium")
        model_tokens = [clean_value(tokens[i]) for i in match_indices if tokens[i].lower() != "pentium"]
        if model_tokens:
            model = " ".join(model_tokens)
            # Apply normalization to the model
            model = normalize_cpu_model(model)
            result["cpu_model"] = model
            
            # Extract suffix if present
            suffix_match = re.search(r"([a-zA-Z][a-zA-Z0-9]*)$", model)
            if suffix_match:
                result["cpu_suffix"] = suffix_match.group(1)
                
        return result
    
    # Check for Xeon processor
    elif len(match_indices) >= 2 and tokens[match_indices[0]].lower() == "xeon":
        result["cpu_brand"] = "Intel"
        
        # Extract the model (Silver, Gold, E3-1535M, etc.)
        model_parts = []
        for i in range(1, len(match_indices)):
            model_parts.append(clean_value(tokens[match_indices[i]]))
        
        model = " ".join(model_parts)
        # Apply normalization to the model
        model = normalize_cpu_model(model)
        result["cpu_model"] = model
        
        # Extract the series from the model for family determination
        # Check for named series first (Silver, Gold, Bronze, Platinum)
        named_series_match = re.search(r'\b(Silver|Gold|Bronze|Platinum)\b', model, re.IGNORECASE)
        if named_series_match:
            series = named_series_match.group(1).capitalize()
            result["cpu_family"] = f"Xeon {series}"
            # NEW: set cpu_model to the numeric value that follows the series word (e.g. "Gold 6142" -> 6142)
            num_match = re.search(rf"{series}\s+([0-9]{{4}}[A-Z]*)", model, re.IGNORECASE)
            if num_match:
                result["cpu_model"] = num_match.group(1)
            else:
                # If no number captured, remove the series word and use the remainder
                remainder = re.sub(rf"\b{series}\b", "", model, flags=re.IGNORECASE).strip()
                if remainder:
                    result["cpu_model"] = remainder
        else:
            # Check for alphanumeric series (E3, E5, X5, W, etc.) - MADE MORE SPECIFIC
            series_match = re.match(r"([EWX][0-9]+)", model, re.IGNORECASE)
            if series_match:
                series = series_match.group(1).upper()
                # Special handling for numeric E-series like E5504 -> Xeon 5500 family
                if series.startswith("E") and len(series) > 2 and series[1:].isdigit():
                    digits = series[1:]
                    if len(digits) >= 2:
                        family_series = digits[:2] + "00"
                        result["cpu_family"] = f"Xeon {family_series}"
                        result["cpu_model"] = f"Xeon-{series}"
                    else:
                        result["cpu_family"] = f"Xeon {series}"
                else:
                    result["cpu_family"] = f"Xeon {series}"
            else:
                result["cpu_family"] = "Xeon"
        
        # Check for version suffix (v2, v3, V6, etc.) - case-insensitive
        version = None
        if len(match_indices) > 2:
            last_token = tokens[match_indices[-1]]
            # Case-insensitive match for v/V
            if re.match(r"[vV]\d+", last_token, re.IGNORECASE):
                # Normalize to lowercase v for consistent output
                version = normalize_version(last_token)
                # Update the model to include the normalized version
                model_parts[-1] = version
                result["cpu_model"] = normalize_cpu_model(" ".join(model_parts))
        
        # Try to extract version from model if it wasn't found as a separate token
        if not version and re.search(r"[vV]\d+$", model, re.IGNORECASE):
            version_match = re.search(r"([vV]\d+)$", model, re.IGNORECASE)
            if version_match:
                # Remove version from model as it will be appended in normalized form
                model = re.sub(r"[vV]\d+$", "", model, re.IGNORECASE).strip()
                version = normalize_version(version_match.group(1))
                result["cpu_model"] = normalize_cpu_model(f"{model} {version}")
        
        # For Xeon processors, use the version number directly as generation
        if version:
            v_match = re.search(r"\d+", version)
            if v_match:
                v_num = int(v_match.group())
                # v1, v2, v3... directly map to 1st Gen, 2nd Gen, 3rd Gen...
                suffix = "th"
                if v_num == 1:
                    suffix = "st"
                elif v_num == 2:
                    suffix = "nd"
                elif v_num == 3:
                    suffix = "rd"
                result["cpu_generation"] = f"{v_num}{suffix} Gen"
        else:
            # For legacy Xeon models without version suffix, try to determine generation
            # MADE MORE SPECIFIC - only match actual Xeon patterns, not motherboard models
            gen_match = re.match(r"([EWX])([3-9])-\d{4}", model, re.IGNORECASE)
            if gen_match:
                series_letter = gen_match.group(1).upper()
                gen_num = int(gen_match.group(2))
                # Only apply generation mapping for actual Xeon series
                if series_letter in ['E', 'W', 'X']:
                    gen_map = {
                        3: "1st",  # Nehalem (E3 without version = 1st gen)
                        5: "2nd",  # Sandy Bridge
                        6: "3rd",  # Ivy Bridge
                        7: "4th",  # Haswell
                    }
                    if gen_num in gen_map:
                        result["cpu_generation"] = f"{gen_map[gen_num]} Gen"
        
        return result
    
    # Handle multiple CPUs found (return numbered results)
    elif len(match_indices) > 1:
        # This should not happen in the new logic since we return separate results
        # But keeping for backwards compatibility
        for i, idx in enumerate(match_indices, 1):
            token = clean_value(tokens[idx])
            
            # Extract family and model for each individual token
            if token.lower().startswith(("i3-", "i5-", "i7-", "i9-")):
                prefix = token.split('-')[0].lower()
                family = f"Core {prefix}"
                model = token.split('-')[1]
                
                # Add model to result with underscored numbering
                result[f"cpu_model_{i}"] = model
                
                # Add family/brand for each numbered model
                result[f"cpu_family_{i}"] = family
                result[f"cpu_brand_{i}"] = "Intel"
                
                # Detect generation for this specific model
                full_model = f"{prefix}-{model}"
                gen = get_generation_from_map(full_model, family)
                if not gen:
                    # Extract just numbers for fallback
                    model_nums = re.match(r"^(\d+)", model)
                    if model_nums:
                        model_for_gen = model_nums.group(1)
                        # Only use 2 digits for models starting with 1x
                        first_digit = int(model_for_gen[0])
                        if first_digit == 1 and len(model_for_gen) >= 4:
                            # For models starting with 1, use first 2 digits (10-14th gen)
                            gen_num = int(model_for_gen[:2])
                            suffix = "th"
                            if gen_num % 100 not in [11, 12, 13]:
                                suffix = {1: "st", 2: "nd", 3: "rd"}.get(gen_num % 10, "th")
                            gen = f"{gen_num}{suffix} Gen"
                        else:
                            # For models starting with 2-9, use first digit only
                            suffix = "th"
                            if first_digit == 1:
                                suffix = "st"
                            elif first_digit == 2:
                                suffix = "nd"
                            elif first_digit == 3:
                                suffix = "rd"
                            gen = f"{first_digit}{suffix} Gen"
                
                if gen:
                    result[f"cpu_generation_{i}"] = gen
            else:
                # Handle model without i-prefix (including Apple M-series)
                # Apply normalization to standalone models too
                normalized_token = normalize_cpu_model(token)
                result[f"cpu_model_{i}"] = normalized_token
                
                # Check if this is an Apple M-series chip
                if re.match(r'^[mM][123](?:\s+(?:Pro|Max|Ultra))?$', token, re.IGNORECASE):
                    result[f"cpu_brand_{i}"] = "Apple"
                    result[f"cpu_family_{i}"] = f"Apple {token}"
                    
                    # Get generation for Apple M-series
                    generation = get_apple_m_generation(token)
                    if generation:
                        result[f"cpu_generation_{i}"] = generation
                else:
                    # FIXED: Add generation detection for standalone models in multi-CPU scenarios but DON'T assume family
                    gen = get_generation_from_standalone_model(normalized_token)
                    if gen:
                        result[f"cpu_generation_{i}"] = gen
                        result[f"cpu_brand_{i}"] = "Intel"
                        # REMOVED: Don't assume cpu_family = "Core i5"
                    
                    # Extract suffix if present for non-Apple chips
                    suffix_match = re.search(r"([a-zA-Z][a-zA-Z0-9]*)$", normalized_token)
                    if suffix_match:
                        result[f"cpu_suffix_{i}"] = suffix_match.group(1)
        
        # Set shared brand and family from first model if not set individually
        if not any(key.startswith("cpu_brand_") for key in result):
            result["cpu_brand"] = "Intel"
        
        # Only set shared family if all individual families are the same
        individual_families = [result[key] for key in result if key.startswith("cpu_family_")]
        if individual_families and len(set(individual_families)) == 1:
            result["cpu_family"] = individual_families[0]
        
        return result
    
    # Default case - use standard processing
    elif base_process_match_func:
        base_result = base_process_match_func(tokens, match_indices)
        # Clean any values that might have punctuation and apply normalization
        for key, value in base_result.items():
            if isinstance(value, str):
                cleaned_value = clean_value(value)
                # Apply normalization if this is a CPU model field
                if 'model' in key.lower():
                    cleaned_value = normalize_cpu_model(cleaned_value)
                base_result[key] = cleaned_value
        return base_result
    
    # Clean any values in result and apply normalization
    for key in list(result.keys()):
        if isinstance(result[key], str):
            cleaned_value = clean_value(result[key])
            # Apply normalization if this is a CPU model field
            if 'model' in key.lower():
                cleaned_value = normalize_cpu_model(cleaned_value)
            result[key] = cleaned_value
    
    return result
    


def extract_cpu_info(tokens: List[str], consumed: Set[int]) -> Dict[str, Any]:
    """Extract CPU information from a list of tokens, ensuring cpu_quantity is adjacent to other CPU attributes."""
    result = {}
    cpu_consumed = set()  # Track indices of tokens consumed by CPU-related extractors (except cpu_quantity)

    # Step 1: Extract all CPU-related attributes except cpu_quantity
    for config in extractor_config:
        if config["name"] != "cpu_quantity":
            extractor = config["class"](config)
            matches = extractor.extract(tokens, consumed)
            for match in matches:
                data = extractor.process_match(tokens, match)
                for key, value in data.items():
                    result[key] = value
                cpu_consumed.update(match)  # Add matched indices to cpu_consumed

    # Step 2: Extract cpu_quantity, only accepting matches adjacent to cpu_consumed tokens
    quantity_config = next(config for config in extractor_config if config["name"] == "cpu_quantity")
    quantity_extractor = quantity_config["class"](quantity_config)
    potential_matches = quantity_extractor.extract(tokens, consumed)  # Get potential cpu_quantity matches

    for match in potential_matches:
        match_indices = match  # Indices of the potential cpu_quantity token
        
        # Check if any CPU tokens were found
        if not cpu_consumed:
            # No CPU tokens found, skip all cpu_quantity matches
            continue
            
        # Check if the match is adjacent to any cpu_consumed index
        if any(
            (idx > 0 and (idx - 1) in cpu_consumed) or (idx < len(tokens) - 1 and (idx + 1) in cpu_consumed)
            for idx in match_indices
        ):
            data = quantity_extractor.process_match(tokens, match)
            for key, value in data.items():
                result[key] = value
            consumed.update(match)  # Mark the token as consumed

    return result

def str_pat(value, optional=False, show=True):
    """Define a string pattern for exact text matching."""
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show}

def regex_pat(pattern, optional=False, show=True):
    """Define a regex pattern for flexible matching."""
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

def list_pat(values, optional=False, show=True):
    """Define a list pattern to match any one of several values."""
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

class CPUFamilyExtractor(BaseExtractor):
    """Extractor for CPU family with special handling for combined patterns like 'i5/7'.

    Numbering convention:
    - First occurrence uses base key (e.g., cpu_family)
    - Second and onward use numeric suffixes (cpu_family2, cpu_family3, ...)
    """
    
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Extract CPU families ignoring previously consumed tokens to catch slash-separated pairs."""
        # Use a local consumed set so we don't skip tokens consumed by earlier extractors
        local_consumed: Set[int] = set()
        
        # IMPORTANT: Skip extraction if Pentium/Celeron tokens were already processed
        # Check if any Pentium or Celeron tokens are already consumed
        pentium_celeron_consumed = False
        for i, token in enumerate(tokens):
            if (token.lower() in ["pentium", "celeron"] and i in consumed):
                pentium_celeron_consumed = True
                break
        
        if pentium_celeron_consumed:
            # Don't extract CPU families if Pentium/Celeron was already processed
            return []
        
        # NEW GUARD: If a Pentium variant (Silver/Gold/Bronze) is present, defer to the
        # specialised Pentium/Celeron extractor so this generic extractor doesn't
        # prematurely consume the 'Pentium' token and override cpu_family.
        if any(tok.lower() == "pentium" for tok in tokens) and any(tok.lower() in ["silver", "gold", "bronze"] for tok in tokens):
            return []
        
        results = super().extract(tokens, local_consumed)
        
        # Special handling for slash-separated patterns that might have been partially consumed
        # Look for Xeon/i7 patterns even if some tokens are consumed
        for i in range(len(tokens) - 2):
            if (tokens[i].lower() == "xeon" and 
                tokens[i + 1] == "/" and 
                tokens[i + 2].lower() in ["i3", "i5", "i7", "i9"]):
                # Check if this pattern isn't already captured
                pattern_indices = [i, i + 1, i + 2]
                if not any(pattern_indices == result for result in results):
                    # Remove any single "Xeon" match and replace with full pattern
                    results = [r for r in results if r != [i]]
                    results.append(pattern_indices)
                    if logger:
                        logger.debug(f"CPU: CPUFamilyExtractor found Xeon/i7 pattern: {[tokens[idx] for idx in pattern_indices]}")
        
        # Mark matched indices in the shared consumed to avoid reuse
        # ENHANCED APPROACH: CPU extractors should only consume tokens that are purely their domain
        # Tokens like "i7-6" contain both family (i7) and generation (6) info, so don't consume them
        # Only consume pure separators like "/" that don't contain extractable information
        for match in results:
            for i in match:
                token = tokens[i]
                # Only consume separator tokens like "/" - let other extractors see the meaningful tokens
                if token == "/":
                    consumed.add(i)
        return results
    
    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process CPU family matches, handling combined patterns like 'i5/7'."""
        result = {}
        
        if logger:
            logger.debug(f"CPU: CPUFamilyExtractor processing match_indices: {match_indices}")
        if logger:
            logger.debug(f"CPU: CPUFamilyExtractor tokens at indices: {[tokens[i] for i in match_indices]}")
        
        # Handle Core i5/7 type patterns
        if len(match_indices) == 2:
            tokens_text = " ".join(tokens[i] for i in match_indices).lower()
            
            # Check for "core i5/7" pattern
            if re.match(r"(?:core\\s+|intel\\s+)?i[3579]/[3579]$", tokens_text):
                core_token = tokens[match_indices[0]]
                i_token = tokens[match_indices[1]]
                
                # Extract the two CPU families from the i5/7 pattern
                if "/" in i_token:
                    first_family = i_token.split("/")[0]
                    second_family = "i" + i_token.split("/")[1]
                    
                    # Return both CPU families
                    result["cpu_family1"] = f"Core {first_family}"
                    result["cpu_family2"] = f"Core {second_family}"
                    result["cpu_brand"] = "Intel"
                    
                    # Also set the primary cpu_family to the first one
                    result["cpu_family"] = f"Core {first_family}"
                    return result
        
        # Handle standalone slash-separated families like i7/5
        if len(match_indices) == 1:
            token = tokens[match_indices[0]]
            if re.match(r"i[3579]/[3579]$", token, re.IGNORECASE):
                i_token = token
                first_family = i_token.split("/")[0]
                second_family = "i" + i_token.split("/")[1]
                result["cpu_family1"] = f"Core {first_family}"
                result["cpu_family2"] = f"Core {second_family}"
                result["cpu_family"] = f"Core {first_family}"
                return result
        
        # Handle slash-separated families like i7 / 5
        if len(match_indices) == 3 and tokens[match_indices[1]] == "/":
            first_token = tokens[match_indices[0]]
            second_token = tokens[match_indices[2]]
            if re.match(r"i[3579]$", first_token, re.IGNORECASE) and re.match(r"^[3579]$", second_token):
                result["cpu_family1"] = f"Core {first_token}"
                result["cpu_family2"] = f"Core i{second_token}"
                result["cpu_family"] = f"Core {first_token}"
                result["cpu_brand"] = "Intel"
                return result
            # Handle slash-separated families like i5 / i7-6 (where second family has generation)
            elif re.match(r"i[3579]$", first_token, re.IGNORECASE) and re.match(r"^i[3579]-\d{1,2}$", second_token, re.IGNORECASE):
                # Extract just the i7 part from i7-6
                second_family = re.match(r"^(i[3579])-\d{1,2}$", second_token, re.IGNORECASE).group(1)
                result["cpu_family1"] = f"Core {first_token}"
                result["cpu_family2"] = f"Core {second_family}"
                result["cpu_family"] = f"Core {first_token}"
                result["cpu_brand"] = "Intel"
                return result
            # Handle Xeon/i7 pattern
            elif first_token.lower() == "xeon" and re.match(r"i[3579]$", second_token, re.IGNORECASE):
                if logger:
                    logger.debug(f"CPU: CPUFamilyExtractor detected Xeon/i7 pattern: {first_token}/{second_token}")
                result["cpu_family1"] = "Xeon"
                result["cpu_family2"] = f"Core {second_token}"
                result["cpu_family"] = "Xeon"
                result["cpu_brand"] = "Intel"
                if logger:
                    logger.debug(f"CPU: CPUFamilyExtractor returning Xeon/i7 result: {result}")
                return result
        
        # Handle Intel Xeon numeric series (e.g., E5504 -> Xeon 5500 family)
        if len(match_indices) == 2:
            first = tokens[match_indices[0]]
            second = tokens[match_indices[1]]
            # Handle Intel Xeon numeric series like E5504 or e5504 -> Xeon 5500 family
            if first.lower() == "xeon" and re.match(r"^[Ee]\d{3,4}$", second):
                digits = second[1:]
                series = digits[:2] + "00"
                return {self.name: f"Xeon {series}", "cpu_model": f"Xeon-{second.upper()}", "cpu_brand": "Intel"}

        # Default behavior: join tokens together
        raw_value = " ".join(tokens[i] for i in match_indices)
        # Strip specific model numbers for CPU family, e.g., from 'Xeon E5-1620' to 'Xeon E5'
        parts = raw_value.split()
        if len(parts) >= 2 and '-' in parts[1]:
            parts[1] = parts[1].split('-', 1)[0]
        value = " ".join(parts)
        return {self.name: value}

class CPUExtractor(BaseExtractor):
    """Extractor for CPU attributes with proper handling of both single and multi-CPU scenarios.

    Numbering convention:
    - First occurrence uses base keys (e.g., cpu_brand, cpu_model, cpu_speed)
    - Second and onward use numbered suffixes (cpu_brand2, cpu_model2, cpu_speed2, ...)
    """
    
    def extract(self, tokens: List[str], consumed: Set[int]) -> List[List[int]]:
        """Delegate to the standalone cpu_extract function.

        USER INTENT: CPU speed must be context-sensitive – only accept if it appears
        after CPU indicators and not in RAM/DDR context (e.g., ignore 'DDR4 2400MHz').
        """
        if self.name == "cpu_speed":
            # For CPU speed, scan all tokens with rules:
            # - GHz tokens: accept even without CPU context
            # - MHz tokens: require CPU context and reject if immediately after RAM context (e.g., DDR4 2400MHz)
            results: List[List[int]] = []

            ghz_regexes = [
                r"@[0-9]+\.[0-9]+[gG][hH][zZ]",
                r"[0-9]+\.[0-9]+[gG][hH][zZ]",
                r"[0-9]+[gG][hH][zZ]",
            ]
            mhz_regex = r"\d{2,4}[mM][hH][zZ]"

            cpu_context_regexes = [
                r"^(intel|amd|apple)$", r"^(cpu|processor)$", r"^core$", r"^ultra$",
                r"^i[3579]$", r"^[mM][357]$", r"^ryzen$", r"^xeon$", r"^pentium$", r"^celeron$", r"^athlon$",
                r"^i[3579]-[0-9]{3,5}[A-Za-z0-9]*$", r"^[0-9]{1,2}[A-Za-z][0-9]{2,3}$",
                r"^[EWX]-?[0-9]{3,5}[A-Za-z0-9]*$",
            ]

            def has_cpu_context_before(idx: int) -> bool:
                start = max(0, idx - 8)
                for j in range(start, idx):
                    tok = tokens[j].lower()
                    for rx in cpu_context_regexes:
                        if re.match(rx, tok):
                            return True
                return False

            def has_immediate_ram_marker_before(idx: int) -> bool:
                if idx - 1 < 0:
                    return False
                prev = tokens[idx - 1].lower()
                # Catch forms like 'ddr4', 'lpddr4', 'ddr4-2400'
                if "ddr" in prev:
                    return True
                if prev in {"ram", "memory", "sodimm", "dimm", "so-dimm"}:
                    return True
                return False

            for i, token in enumerate(tokens):
                low = token.lower()
                if any(re.match(rx, low) for rx in ghz_regexes):
                    results.append([i])
                    if self.logger:
                        self.logger.debug(f"CPU: accepted GHz speed token '{token}' at index {i}")
                    continue
                if re.match(mhz_regex, low):
                    if has_cpu_context_before(i) and not has_immediate_ram_marker_before(i):
                        results.append([i])
                        if self.logger:
                            self.logger.debug(
                                f"CPU: accepted MHz speed token '{token}' at index {i} (CPU context present; not after RAM marker)"
                            )
                    else:
                        if self.logger:
                            self.logger.debug(
                                f"CPU: rejected MHz speed token '{token}' at index {i} due to missing CPU context or RAM marker before"
                            )
            return results
        else:
            return cpu_extract(tokens, consumed, self.name, super().extract)
    
    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        """Process matched tokens and apply value cleaning."""
        if self.name == "cpu_speed":
            # For CPU speed, use the base process_match but clean the value and remove @
            result = super().process_match(tokens, match_indices)
            for key, value in result.items():
                if isinstance(value, str):
                    # Remove @ symbol if present at the beginning
                    value = re.sub(r'^@', '', value)
                    value = clean_value(value)
                    
                    # Normalize to two decimal places
                    # First, extract the numeric part and the unit
                    match = re.search(r'([\d.]+)([gGmM][hH][zZ])', value)
                    if match:
                        numeric_part = float(match.group(1))
                        unit = match.group(2)
                        # Format to always have two decimal places
                        value = f"{numeric_part:.2f}{unit}"
                    
                    result[key] = value
            return result
        else:
            # For other CPU extractors, use the custom process_match
            return process_match(tokens, match_indices, super().process_match)

class CPUPentiumCeleronExtractor(BaseExtractor):
    """Extractor for slash-separated Pentium/Celeron pairs such as
    'Intel Pentium Silver/Celeron', 'Pentium/Celeron', or even
    'Intel Celeron / Pentium Gold'.

    Behaviour:
      • Sets cpu_brand = Intel  (explicitly)
      • Sets cpu_family  (primary) to Pentium  (first Pentium encountered)
      • Adds cpu_family1 = Pentium and cpu_family2 = Celeron so callers
        can see both families just like the Core i5/i7 logic elsewhere.
    """

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        output = {"cpu_brand": "Intel"}

        # Detect variant (Silver / Gold / Bronze) within the matched slice
        variant = None
        for idx in match_indices:
            tok_low = tokens[idx].lower()
            if tok_low in {"silver", "gold", "bronze"}:
                variant = tok_low.capitalize()
                break

        pentium_family = f"Pentium {variant}" if variant else "Pentium"

        if any(tokens[i].lower() == "celeron" for i in match_indices):
            # Slash-separated pair – output both families
            output["cpu_family1"] = pentium_family
            output["cpu_family2"] = "Celeron"
            output["cpu_family"] = pentium_family  # primary = Pentium variant
        else:
            # Stand-alone Pentium variant
            output["cpu_family"] = pentium_family

        return output

class CPUCeleronNoiseExtractor(BaseExtractor):
    """Extractor that consumes standalone 'Intel Celeron' occurrences (often the second CPU in a Pentium/Celeron pair) so they don't override the primary Pentium cpu_family. It intentionally returns an empty dict."""

    def process_match(self, tokens: List[str], match_indices: List[int]) -> Dict:
        # Consume but output nothing
        return {}

extractor_config = [
    {
        "name": "cpu_generation",
        "patterns": [],  # Custom logic in CPUGenerationExtractor
        "multiple": True,
        "class": CPUGenerationExtractor
    },
    {
        "name": "cpu_model",
        "patterns": [
            # === PENTIUM/CELERON PATTERNS (HIGH PRIORITY) ===
            
            # Pentium Silver/Gold/Bronze with Celeron slash pattern
            [str_pat("Intel", optional=True, show=False),
             str_pat("Pentium", show=True),
             list_pat(["Silver", "Gold", "Bronze"], optional=True, show=True),
             str_pat("/", optional=True, show=False),
             str_pat("Celeron", show=True)],
            
            # Celeron with Pentium Silver/Gold/Bronze slash pattern (reverse order)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=True),
             str_pat("/", optional=True, show=False),
             str_pat("Pentium", show=True),
             list_pat(["Silver", "Gold", "Bronze"], optional=True, show=True)],
             
            # Stand-alone Pentium variant (no Celeron)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Pentium", show=True),
             list_pat(["Silver", "Gold", "Bronze"], optional=True, show=True)],
            
            # === INTEL CORE ULTRA SERIES ===
            
            # Intel Core Ultra with full model (e.g., "Intel Core Ultra 5 135U", "Core Ultra 7 165H")
            [str_pat("Intel", optional=True, show=False),
             str_pat("Core", show=True),
             str_pat("Ultra", show=True),
             list_pat(["5", "7", "9"], show=True),
             regex_pat(r"\d{3,4}[a-zA-Z]*", show=True)],
            
            # Intel Core Ultra without model number (e.g., "Core Ultra 5", "Intel Core Ultra 7")
            [str_pat("Intel", optional=True, show=False),
             str_pat("Core", show=True),
             str_pat("Ultra", show=True),
             list_pat(["5", "7", "9"], show=True)],
            
            # === INTEL CORE M SERIES (FIXED - include Intel in output) ===
            
            # Intel Core M series without dash (e.g., "Intel Core M3", "Core M5")
            [str_pat("Intel", optional=True, show=True),
             str_pat("Core", show=True),
             regex_pat(r"[mM][357]", show=True)],
            
            # === CELERON PROCESSORS (Updated patterns) ===
            
            # J-series Celeron processors - adjacent pattern
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"J[0-9]{4}", show=True)],
            
            # J-series Celeron processors - standalone with CPU context check
            [regex_pat(r"J[0-9]{4}", show=True)],
            
            # N-series Celeron processors - adjacent pattern
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"N[0-9]{4}", show=True)],
            
            # N-series Celeron processors - standalone with CPU context check
            [regex_pat(r"N[0-9]{4}", show=True)],
            
            # G-series Celeron processors - adjacent pattern
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"G[0-9]{3,4}", show=True)],
            
            # G-series Celeron processors - standalone with CPU context check
            [regex_pat(r"G[0-9]{3,4}", show=True)],
            
            # D-series Celeron processors (e.g., D325, D336, D355)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             str_pat("D", show=True),
             regex_pat(r"[0-9]{3}", show=True),
             regex_pat(r"J?", optional=True, show=True)],
            
            # M-series Celeron processors (e.g., M 360J, M 333, M 423)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             str_pat("M", show=True),
             regex_pat(r"[0-9]{3}", show=True),
             regex_pat(r"[A-Z]?", optional=True, show=True)],
            
            # Numbered series Celeron processors (1000-7000 series)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"[1-7][0-9]{3}", show=True),
             regex_pat(r"[A-Z]?", optional=True, show=True)],
            
            # Legacy 3-digit Celeron processors (e.g., 466, 533, 700)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"[0-9]{3}", show=True),
             regex_pat(r"[A-Z]?", optional=True, show=True)],
            
            # Frequency-based legacy Celeron (e.g., 2.0GHz, 1.8GHz)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Celeron", show=False),
             regex_pat(r"[0-9]+\.[0-9]+GHz", show=True)],
            
            # Mobile Celeron processors 
            [str_pat("Intel", optional=True, show=False),
             str_pat("Mobile", show=True),
             str_pat("Celeron", show=False),
             regex_pat(r"[0-9]+\.[0-9]+GHz|[0-9]{3,4}[A-Z]*", show=True)],
            
            # === INTEL CORE PROCESSORS ===
            
            # Intel Core i-series with optional Intel prefix
            [str_pat("Intel", optional=True, show=False),
             str_pat("Core", optional=True, show=True),
             regex_pat(r"i[3579]-[0-1]?[0-9]{3,4}[a-zA-Z0-9]*", show=True)],
            
            # Intel Core i-series without Intel or Core prefix
            [regex_pat(r"i[3579]-[0-1]?[0-9]{3,4}[a-zA-Z0-9]*", show=True)],
            
            # Intel Core M series with dash
            [str_pat("Intel", optional=True, show=False),
             str_pat("Core", optional=True, show=True),
             regex_pat(r"m[3579]-[a-zA-Z0-9]+", show=True)],
            
            # === INTEL XEON PROCESSORS ===
            
            # Intel Xeon with named series (Silver, Gold, Bronze, Platinum)
            [str_pat("Intel", optional=True, show=False),
             str_pat("Xeon", show=True),
             list_pat(["Silver", "Gold", "Bronze", "Platinum"], show=True),
             regex_pat(r"[0-9]{4}[a-zA-Z]*", show=True),
             regex_pat(r"[vV]\d+|0", optional=True, show=True)],
            
            # Intel Xeon with alphanumeric series and space before vX or 0 generation
            [str_pat("Intel", optional=True, show=False),
             str_pat("Xeon", show=True),
             regex_pat(r"[a-zA-Z0-9]+-[0-9]{4}[a-zA-Z]*", show=True),
             regex_pat(r"[vV]\d+|0", optional=True, show=True)],
            
            # Intel Xeon with attached vX or 0 generation
            [str_pat("Intel", optional=True, show=False),
             str_pat("Xeon", show=True),
             regex_pat(r"[a-zA-Z0-9]+-[0-9]{4}[a-zA-Z]*(?:[vV]\d+|0)", show=True)],
            
            # === OTHER INTEL PROCESSORS ===
            
            # Intel Atom processors
            [str_pat("Intel", optional=True, show=False),
             str_pat("Atom", show=True),
             regex_pat(r"[a-zA-Z0-9]+-[0-9]{4}[a-zA-Z]*", show=True)],
            
            # Intel Pentium processors
            [str_pat("Intel", optional=True, show=False),
             str_pat("Pentium", show=True),
             regex_pat(r"[0-9]{3,4}[a-zA-Z]*", show=True)],
            
            # === AMD PROCESSORS ===
            
            # AMD Ryzen processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("Ryzen", show=True),
             regex_pat(r"[3579]\s*[0-9]{4}[a-zA-Z]*", show=True)],
            
            # AMD A-series processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("A", show=True),
             regex_pat(r"[0-9]{4}[a-zA-Z]*", show=True)],
            
            # AMD Athlon processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("Athlon", show=True),
             regex_pat(r"[0-9]{3,4}[a-zA-Z]*", show=True)],
            
            # AMD Phenom processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("Phenom", show=True),
             regex_pat(r"[0-9]{3,4}[a-zA-Z]*", show=True)],
            
            # AMD EPYC processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("EPYC", show=True),
             regex_pat(r"[0-9]{4}[a-zA-Z]*", show=True)],
            
            # AMD Threadripper processors
            [str_pat("AMD", optional=True, show=False),
             str_pat("Threadripper", show=True),
             regex_pat(r"[0-9]{4}[a-zA-Z]*", show=True)],
            
            # === GENERIC PATTERNS ===
            
            # Single-token CPU models (e.g., 5600U, 7600U) - WITH CPU CONTEXT CHECK
            [regex_pat(r"\d{4}[a-zA-Z]+", show=True)]
        ],
        "multiple": True,
        "class": CPUExtractor
    },
    {
        "name": "cpu_brand",
        "patterns": [
            [str_pat("Intel")],
            [str_pat("AMD")],
            [list_pat(["ARM", "Apple", "Qualcomm", "Mediatek", "Samsung", "IBM", "VIA",
                       "Cyrix", "Transmeta", "Fujitsu", "Motorola", "RISC-V", "Huawei",
                       "Rockchip", "Allwinner"])]
        ],
        "multiple": False,
        "class": BaseExtractor
    },
    {
        "name": "cpu_family",
        "patterns": [
            # Intel Core Ultra patterns
            [{"type": "string", "value": "Core", "include_in_output": True},
             {"type": "string", "value": "Ultra", "include_in_output": True},
             {"type": "list", "values": ["5", "7", "9"], "include_in_output": True}],
            
            [{"type": "string", "value": "Core", "include_in_output": True},
             {"type": "regex", "pattern": r"^i[3579]$", "include_in_output": True}],
            [{"type": "string", "value": "Core", "include_in_output": True},
             {"type": "regex", "pattern": r"^i[3579]/[3579]$", "include_in_output": True}],
            [{"type": "string", "value": "Core", "include_in_output": True},
             {"type": "regex", "pattern": r"^[mM][357]$", "include_in_output": True}],
            [{"type": "string", "value": "Ryzen", "include_in_output": True},
             {"type": "regex", "pattern": r"[3579]", "include_in_output": True}],
            # Put Xeon series patterns FIRST before named series and generic Xeon
            [str_pat("Xeon", show=True),
             regex_pat(r"[EWX][0-9]*", show=True)],
            [str_pat("Xeon", show=True),
             list_pat(["Silver", "Gold", "Bronze", "Platinum"], show=True)],
            [str_pat("Xeon", show=True)],
            [str_pat("Pentium", show=True)],
            [str_pat("Celeron", show=True)],
            [str_pat("Atom", show=True)],
            [{"type": "string", "value": "A", "include_in_output": True},
             {"type": "regex", "pattern": r"[0-9]", "include_in_output": True}],
            [str_pat("Athlon", show=True)],
            [str_pat("Phenom", show=True)],
            [str_pat("EPYC", show=True)],
            [str_pat("Threadripper", show=True)],
            [regex_pat(r"^i[3579]/[3579]$", show=True)],
            # Match standalone slash-separated families like i7 / 5
            [ {"type": "regex", "pattern": r"^i[3579]$", "include_in_output": True},
              {"type": "string", "value": "/", "include_in_output": True},
              {"type": "regex", "pattern": r"^[3579]$", "include_in_output": True} ],
            # Match standalone slash-separated families like i5 / i7-6 (where second family has generation)
            [ {"type": "regex", "pattern": r"^i[3579]$", "include_in_output": True},
              {"type": "string", "value": "/", "include_in_output": True},
              {"type": "regex", "pattern": r"^i[3579]-\d{1,2}$", "include_in_output": True} ],
            # Match Xeon/i7 slash-separated families
            [ {"type": "string", "value": "Xeon", "include_in_output": True},
              {"type": "string", "value": "/", "include_in_output": True},
              {"type": "regex", "pattern": r"^i[3579]$", "include_in_output": True} ],
        ],
        "multiple": False,
        "class": CPUFamilyExtractor
    },
    {
        "name": "cpu_speed",
        "patterns": [
            # Pattern for @2.40GHz format  
            [regex_pat(r"@[0-9]+\.[0-9]+[gG][hH][zZ]", show=True)],
            # Pattern for 2.40GHz format
            [regex_pat(r"[0-9]+\.[0-9]+[gG][hH][zZ]", show=True)],
            # Pattern for 3GHz format (integer + GHz)
            [regex_pat(r"[0-9]+[gG][hH][zZ]", show=True)],
            # Pattern for 1000MHz format
            [regex_pat(r"\d{2,4}[mM][hH][zZ]", show=True)],
        ],
        "multiple": True,  # Changed from False to True
        "class": CPUExtractor
    },
    {
        "name": "cpu_quantity",
        "patterns": [
            # Enhanced patterns for better CPU quantity detection
            [regex_pat(r"\d+x", show=True)],  # Match "2x", "4x", etc.
            [regex_pat(r"\(\d+x?\)", show=True)],  # Match "(1x)", "(2x)", "(4)", etc.
            [regex_pat(r"\bsingle(?!\s+core)\b", show=True)],
            [regex_pat(r"\bdual(?!\s+core)\b", show=True)],
            [regex_pat(r"\btriple(?!\s+core)\b", show=True)],
            [regex_pat(r"\bquad(?!\s+core)\b", show=True)],
            [regex_pat(r"\b[2-9]\b", show=True)]  # Match standalone numbers 2-9
        ],
        "multiple": False,
        "class": CPUQuantityExtractor
    },
    # NEW EXTRACTORS: Add to the end to run after existing extractors
    {
        "name": "cpu_generation_slash",
        "patterns": [],  # Custom logic in CPUGenerationMultiSlashExtractor
        "multiple": True,
        "class": CPUGenerationMultiSlashExtractor
    },
    {
        "name": "cpu_core_generic", 
        "patterns": [],  # Custom logic in CPUGenericCoreExtractor
        "multiple": False,
        "class": CPUGenericCoreExtractor
    },
    {
        "name": "cpu_family",  # Use the same key so downstream expects the same field
        "patterns": [
            [
                str_pat("Intel", optional=True, show=False),
                str_pat("Pentium", show=True),
                list_pat(["Silver", "Gold", "Bronze"], optional=True, show=True),
                str_pat("/", optional=True, show=False),
                str_pat("Celeron", optional=True, show=False)
            ],
            [
                str_pat("Intel", optional=True, show=False),
                str_pat("Celeron", optional=True, show=False),
                str_pat("/", optional=True, show=False),
                str_pat("Pentium", show=True),
                list_pat(["Silver", "Gold", "Bronze"], optional=True, show=True)
            ]
        ],
        "multiple": False,
        "class": CPUPentiumCeleronExtractor
    },
    {
        "name": "cpu_celeron_noise",
        "patterns": [
            [str_pat("Intel", optional=True, show=False), str_pat("Celeron", show=False)]
        ],
        "multiple": False,
        "class": CPUCeleronNoiseExtractor
    }
]

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)