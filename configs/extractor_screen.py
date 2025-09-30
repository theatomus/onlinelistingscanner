from configs.parser import BaseExtractor
import re

class ScreenExtractor(BaseExtractor):
    """Extractor for screen attributes, handling size, resolution, hertz, and aspect ratio."""
    
    def __init__(self, config, logger=None):
        """Initialize with config and logger."""
        super().__init__(config, logger)
        self.logger = logger
    
    # Comprehensive resolution map with older and newer formats
    resolution_map = {
        # Standard resolutions
        "fhd": "1920x1080",
        "hd": "1366x768",
        "4k": "3840x2160",
        "qhd": "2560x1440",
        "uhd": "3840x2160",
        "1080p": "1920x1080",
        "720p": "1280x720",
        "1440p": "2560x1440",
        "2160p": "3840x2160",
        "full hd": "1920x1080",
        "quad hd": "2560x1440",
        "ultra hd": "3840x2160",
        
        # Older/traditional resolutions
        "svga": "800x600",
        "xga": "1024x768",
        "sxga": "1280x1024",
        "wxga": "1280x800",
        "wsxga": "1440x900",
        "uxga": "1600x1200",
        "wuxga": "1920x1200",
        "qxga": "2048x1536",
        
        # Widescreen variations
        "wxga+": "1440x900",
        "wsxga+": "1680x1050",
        "wqhd": "2560x1440",
        "wqxga": "2560x1600",
        
        # Ultrawide resolutions
        "uwfhd": "2560x1080",
        "uwqhd": "3440x1440",
        
        # Modern higher resolutions
        "hd+": "1600x900",
        "5k": "5120x2880",
        "8k": "7680x4320",
        
        # Common Apple resolutions
        "retina": "2560x1600",
        "retina15": "2880x1800"
    }

    def process_match(self, tokens: list, match_indices: list) -> dict:
        """Process matches for screen attributes based on the extractor's name."""
        matched_text = " ".join([tokens[i] for i in match_indices]).lower()
        
        if self.logger:
            self.logger.debug(f"Screen: Processing match text: '{matched_text}'")
        
        if self.name == "screen_size":
            # Extract just the numeric part using more lenient pattern
            number_match = re.search(r"(\d+\.\d+|\d+)", matched_text)
            if number_match:
                number = number_match.group(1)
                # Always normalize to number + "in" without spaces
                result = {"screen_size": number + "in"}
                if self.logger:
                    self.logger.debug(f"Screen: Extracted screen size: {result['screen_size']}")
                return result
            else:
                return {"screen_size": None}
        
        elif self.name == "screen_resolution" or self.name == "screen_resolution_split":
            # For split resolution, extract the digits and normalize to standard format
            if self.name == "screen_resolution_split":
                digits = re.findall(r"\d+", matched_text)
                if len(digits) >= 2:
                    # Normalize to standard format without spaces
                    result = {"screen_resolution": f"{digits[0]}x{digits[1]}"}
                    if self.logger:
                        self.logger.debug(f"Screen: Extracted split resolution: {result['screen_resolution']}")
                    return result
            
            # For standard resolution, return as is or map from known terms
            res = matched_text.strip()
            if res in self.resolution_map:
                result = {"screen_resolution": self.resolution_map[res]}
                if self.logger:
                    self.logger.debug(f"Screen: Mapped resolution term '{res}' to {result['screen_resolution']}")
                return result
            result = {"screen_resolution": res}
            if self.logger:
                self.logger.debug(f"Screen: Using raw resolution: {result['screen_resolution']}")
            return result
        
        elif self.name == "screen_hertz":
            hertz_match = re.search(r"(\d+)\s*Hz", matched_text, re.IGNORECASE)
            result = {"screen_hertz": hertz_match.group(0) if hertz_match else None}
            if self.logger and hertz_match:
                self.logger.debug(f"Screen: Extracted refresh rate: {result['screen_hertz']}")
            return result
        
        elif self.name == "screen_aspect_ratio":
            ar_match = re.search(r"(\d+:\d+)", matched_text)
            if ar_match:
                result = {"screen_aspect_ratio": ar_match.group(1)}
                if self.logger:
                    self.logger.debug(f"Screen: Extracted aspect ratio: {result['screen_aspect_ratio']}")
                return result
            elif "by" in matched_text:
                ar_text = matched_text.replace("by", ":").replace(" ", "")
                result = {"screen_aspect_ratio": ar_text}
                if self.logger:
                    self.logger.debug(f"Screen: Converted 'by' to aspect ratio: {result['screen_aspect_ratio']}")
                return result
            elif "widescreen" in matched_text:
                result = {"screen_aspect_ratio": "16:9"}
                if self.logger:
                    self.logger.debug(f"Screen: Detected widescreen format, using aspect ratio: {result['screen_aspect_ratio']}")
                return result
            elif "standard" in matched_text:
                result = {"screen_aspect_ratio": "4:3"}
                if self.logger:
                    self.logger.debug(f"Screen: Detected standard format, using aspect ratio: {result['screen_aspect_ratio']}")
                return result
            else:
                return {"screen_aspect_ratio": None}
        
        return {}

# Helper function for regex patterns
def regex_pat(pattern, optional=False, show=True):
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show}

# Configuration for screen extractors
extractor_config = [
    {
        "name": "screen_size",
        "patterns": [
            # Match patterns with spaces between number and unit
            [regex_pat(r"(\d+\.\d+|\d+)\s*(\"|inch(es)?|in\b)")],
            # Match hyphenated format
            [regex_pat(r"(\d+\.\d+|\d+)-inch")],
            # Match already normalized formats
            [regex_pat(r"(\d+\.\d+|\d+)in\b")],
            [regex_pat(r"(\d+\.\d+|\d+)inch")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_resolution",
        "patterns": [
            [regex_pat(r"(\d+x\d+|\bfhd\b|\bhd\b|\bhd\+\b|\b4k\b|\b5k\b|\b8k\b|\bqhd\b|\buhd\b|\b1080p\b|\b720p\b|\b1440p\b|\b2160p\b|\bsvga\b|\bxga\b|\bsxga\b|\bwsxga\b|\bwxga\b|\bwxga\+\b|\buxga\b|\bwuxga\b|\bqxga\b|\bwqhd\b|\buwfhd\b|\buwqhd\b|\bwqxga\b|\bretina\b|\bretina15\b|full hd|quad hd|ultra hd)")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_resolution_split",
        "patterns": [
            # This pattern looks for a number followed by an 'x' followed by another number
            # with possible spaces between them
            [regex_pat(r"(\d+)\s*[xX]\s*(\d+)")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_hertz",
        "patterns": [
            [regex_pat(r"(\d+)\s*Hz")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_aspect_ratio",
        "patterns": [
            [regex_pat(r"(\d+:\d+)")],
            [regex_pat(r"(\d+\s*by\s*\d+)")],
            [regex_pat(r"(widescreen|standard)")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_touch",
        "patterns": [
            [regex_pat(r"(touch(screen)?|touchscreen|non-touch|non touch)")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    },
    {
        "name": "screen_panel_type",
        "patterns": [
            [regex_pat(r"(ips|tn|va|oled|lcd|led|amoled)")]
        ],
        "multiple": False,
        "class": ScreenExtractor,
    }
]