from configs.parser import BaseExtractor
import re


class LotQuantityExtractor(BaseExtractor):
    """
    Numbering convention reminder:
    - Base key remains unnumbered for the first occurrence.
    - Second and onward occurrences use numbered suffixes.
    Although this extractor typically emits a single 'lot' value, keep this
    convention in mind for consistency across extractors.
    """
    """Extractor for lot quantities, handling various lot number formats."""

    def __init__(self, config, logger=None):
        super().__init__(config, logger)
        self.logger = logger

    def extract(self, tokens: list, consumed: set) -> list:
        """Extract lot quantity information from tokens."""
        results = []

        if self.logger:
            self.logger.debug(f"Lot: Extracting lot quantity from {len(tokens)} tokens")

        for i in range(len(tokens)):
            if i in consumed:
                continue

            # "Lot of X" or "lot of (X)"
            if i + 2 < len(tokens) and tokens[i].lower() == "lot" and tokens[i + 1].lower() == "of":
                matched = False
                if re.match(r"^\(\d+\)$", tokens[i + 2]):
                    results.append([i, i + 1, i + 2])
                    matched = True
                    if self.logger:
                        self.logger.debug(f"Lot: Found 'lot of (X)' at {i}-{i+2}")
                elif tokens[i + 2].isdigit():
                    results.append([i, i + 1, i + 2])
                    matched = True
                    if self.logger:
                        self.logger.debug(f"Lot: Found 'lot of X' at {i}-{i+2}")
                if matched and not self.multiple:
                    break

            # "Lot X" or "lot (X)"
            elif i + 1 < len(tokens) and tokens[i].lower() == "lot":
                matched = False
                if re.match(r"^\(\d+\)$", tokens[i + 1]):
                    results.append([i, i + 1])
                    matched = True
                elif tokens[i + 1].isdigit():
                    results.append([i, i + 1])
                    matched = True
                # Handle compact token like "(4)Asus" immediately after 'Lot'
                elif re.match(r"^\(\d+\)[A-Za-z]", tokens[i + 1]):
                    results.append([i, i + 1])
                    matched = True
                if matched and not self.multiple:
                    break

            # "X Lot" or "(X) Lot"
            elif i + 1 < len(tokens) and tokens[i + 1].lower() == "lot":
                matched = False
                if re.match(r"^\(\d+\)$", tokens[i]):
                    results.append([i, i + 1])
                    matched = True
                elif tokens[i].isdigit():
                    results.append([i, i + 1])
                    matched = True
                if matched and not self.multiple:
                    break

            # Patterns like "5x", "x5", "(5)", "(x5)", "(5x)" (avoid CPU context)
            elif re.match(r"^(\d+x|x\d+|\(\d+\)|\(x\d+\)|\(\d+x\))$", tokens[i], re.IGNORECASE):
                is_cpu_quantity = False
                if i > 0:
                    prev_token = tokens[i - 1].lower()
                    if (
                        prev_token in {'intel', 'amd', 'arm', 'qualcomm', 'mediatek', 'samsung', 'ibm', 'via', 'cyrix', 'transmeta', 'fujitsu', 'motorola', 'risc-v', 'huawei', 'rockchip', 'allwinner'}
                        or prev_token in {'core', 'ryzen', 'xeon', 'pentium', 'celeron', 'atom', 'athlon', 'phenom', 'epyc', 'threadripper'}
                        or prev_token in {'processor', 'cpu', 'ghz', 'mhz'}
                        or re.match(r"[0-9]+\.[0-9]+[gm]hz", prev_token)
                        or re.match(r"[0-9]+[gm]hz", prev_token)
                        or re.match(r"i[3579](?:-|$)", prev_token)
                        or re.match(r"i[3579]-[0-9]{3,4}[a-zA-Z0-9]*", prev_token)
                    ):
                        is_cpu_quantity = True
                if not is_cpu_quantity and i + 1 < len(tokens):
                    next_token = tokens[i + 1].lower()
                    phone_ctx = [
                        (next_token == 'apple' and i + 2 < len(tokens) and tokens[i + 2].lower() in ['iphone', 'ipad']),
                        (next_token == 'samsung' and i + 2 < len(tokens) and 'galaxy' in tokens[i + 2].lower()),
                        (next_token == 'google' and i + 2 < len(tokens) and 'pixel' in tokens[i + 2].lower()),
                        (next_token == 'oneplus' and i + 2 < len(tokens)),
                        (next_token in ['iphone', 'ipad', 'pixel'] or 'galaxy' in next_token),
                    ]
                    if any(phone_ctx):
                        is_cpu_quantity = False
                    elif (
                        next_token in {'intel', 'amd', 'arm', 'qualcomm', 'mediatek', 'samsung', 'ibm', 'via', 'cyrix', 'transmeta', 'fujitsu', 'motorola', 'risc-v', 'huawei', 'rockchip', 'allwinner'}
                        or next_token in {'core', 'ryzen', 'xeon', 'pentium', 'celeron', 'atom', 'athlon', 'phenom', 'epyc', 'threadripper'}
                        or next_token in {'processor', 'cpu', 'ghz', 'mhz'}
                        or re.match(r"i[3579](?:-|$)", next_token)
                        or re.match(r"i[3579]-[0-9]{3,4}[a-zA-Z0-9]*", next_token)
                    ):
                        is_cpu_quantity = True
                if not is_cpu_quantity:
                    results.append([i])
                    if not self.multiple:
                        break

            # Compact plus/slash-separated models: "5420+5540" or "5420/5540"
            elif re.match(r"^\d{3,5}(?:[+/]\d{3,5})+$", tokens[i]):
                results.append([i])
                if self.logger:
                    self.logger.debug(f"Lot: Found plus/slash-separated models at index {i}: {tokens[i]}")
                if not self.multiple:
                    break

            # Spaced pattern: 5420 + 5540 (+ 5550 ...)
            elif (
                re.match(r"^\d{3,5}$", tokens[i])
                and i + 2 < len(tokens)
                and re.match(r"^[+/]$", tokens[i + 1])
                and re.match(r"^\d{3,5}$", tokens[i + 2])
            ):
                j = i + 2
                while j + 2 < len(tokens) and re.match(r"^[+/]$", tokens[j + 1]) and re.match(r"^\d{3,5}$", tokens[j + 2]):
                    j += 2
                results.append(list(range(i, j + 1)))
                if self.logger:
                    self.logger.debug(f"Lot: Found spaced plus/slash-separated models at indices {list(range(i, j+1))}: {' '.join(tokens[i:j+1])}")
                if not self.multiple:
                    break

            # Standalone digit or (digit) as first token (avoid CPU context)
            elif i == 0 and (tokens[i].isdigit() or re.match(r"^\(\d+\)$", tokens[i])):
                is_cpu_quantity = False
                if i + 1 < len(tokens):
                    next_token = tokens[i + 1].lower()
                    if next_token in {'intel', 'amd', 'arm', 'qualcomm', 'mediatek'}:
                        is_cpu_quantity = True
                if not is_cpu_quantity:
                    results.append([i])
                    if self.logger:
                        self.logger.debug(f"Lot: Found standalone digit or (digit) at index {i}: {tokens[i]}")
                    if not self.multiple:
                        break

        return results

    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process lot quantity matches into structured data."""
        result = {}
        matched_text = " ".join([tokens[i] for i in match_indices])

        if self.logger:
            self.logger.debug(f"Lot: Processing match text: '{matched_text}'")

        quantity = None

        # Plus or slash separated model numbers like "5420+5540" or "5420/5540"
        plus_slash_models = re.search(r"\d{3,5}(?:\s*[+/]\s*\d{3,5})+", matched_text)
        if plus_slash_models:
            nums = re.findall(r"\d{3,5}", matched_text)
            if len(nums) >= 2:
                quantity = len(nums)

        # Leading parenthesized quantity in compact token like "(4)Asus"
        if quantity is None:
            m = re.match(r"^\((\d+)\)", matched_text)
            if m:
                try:
                    quantity = int(m.group(1))
                except Exception:
                    pass

        # "Lot of X" or "lot of (X)"
        if quantity is None:
            m = re.match(r"lot\s+of\s+(?:(\d+)|\((\d+)\))", matched_text, re.IGNORECASE)
            if m:
                quantity = int(m.group(1) or m.group(2))

        # "Lot X" or "lot (X)"
        if quantity is None:
            m = re.match(r"lot\s+(?:(\d+)|\((\d+)\))", matched_text, re.IGNORECASE)
            if m:
                quantity = int(m.group(1) or m.group(2))

        # "X Lot" or "(X) Lot"
        if quantity is None:
            m = re.match(r"(?:(\d+)|\((\d+)\))\s+lot", matched_text, re.IGNORECASE)
            if m:
                quantity = int(m.group(1) or m.group(2))

        # "5x", "x5", "(5)", "(x5)", "(5x)"
        if quantity is None:
            m = re.match(r"(?:(\d+)x|x(\d+)|\((\d+)\)|\(x(\d+)\)|\((\d+)x\))", matched_text, re.IGNORECASE)
            if m:
                groups = m.groups()
                quantity = int(next(g for g in groups if g is not None))

        # Standalone digit or (digit)
        if quantity is None:
            m = re.match(r"^(?:(\d+)|\((\d+)\))$", matched_text)
            if m:
                quantity = int(m.group(1) or m.group(2))

        result["lot"] = quantity

        if self.logger:
            self.logger.debug(f"Lot: Extracted quantity: {quantity}")

        return result


# Configuration for lot quantity extractor
extractor_config = [
    {
        "name": "lot",
        "patterns": [
            # "Lot of X" or "Lot of (X)"
            [{"type": "string", "value": "lot"}, {"type": "string", "value": "of"}, {"type": "regex", "pattern": r"\d+"}],
            [{"type": "string", "value": "lot"}, {"type": "string", "value": "of"}, {"type": "regex", "pattern": r"\(\d+\)"}],

            # "Lot X" or "Lot (X)"
            [{"type": "string", "value": "lot"}, {"type": "regex", "pattern": r"\d+"}],
            [{"type": "string", "value": "lot"}, {"type": "regex", "pattern": r"\(\d+\)"}],

            # "X Lot" or "(X) Lot"
            [{"type": "regex", "pattern": r"\d+"}, {"type": "string", "value": "lot"}],
            [{"type": "regex", "pattern": r"\(\d+\)"}, {"type": "string", "value": "lot"}],

            # Special formats
            [{"type": "regex", "pattern": r"\d+x"}],
            [{"type": "regex", "pattern": r"x\d+"}],
            [{"type": "regex", "pattern": r"\(\d+\)"}],
            [{"type": "regex", "pattern": r"\(x\d+\)"}],
            [{"type": "regex", "pattern": r"\(\d+x\)"}],

            # Standalone digit or (digit) as first token
            [{"type": "regex", "pattern": r"^\d+$"}],
            [{"type": "regex", "pattern": r"^\(\d+\)$"}],
        ],
        "multiple": False,
        "class": LotQuantityExtractor,
    }
]