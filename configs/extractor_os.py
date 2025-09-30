from configs.parser import BaseExtractor
import re

class OSExtractor(BaseExtractor):
   """Extractor for operating system information, handling OS type, version, and edition."""
   
   def __init__(self, config, logger=None):
       """Initialize with config and logger."""
       super().__init__(config, logger)
       self.logger = logger
   
   def process_match(self, tokens: list, match_indices: list) -> dict:
       """Process OS-specific matches, extracting type, version, and edition."""
       result = {}
       matched_text = " ".join([tokens[i] for i in match_indices])
       
       if self.logger:
           self.logger.debug(f"OS: Processing match text: '{matched_text}'")
       
       # Handle "No OS" pattern separately
       if self.name == "os_status" and matched_text.lower() in ["no", "no os", "not included", "n/a"]:
           result["os_type"] = "No OS"
           if self.logger:
               self.logger.debug(f"OS: Detected 'No OS' status from '{matched_text}'")
           return result
       
       # Check surrounding context for false positives in OS edition
       if self.name == "os_edition":
           # Get surrounding tokens for context
           start_idx = max(0, min(match_indices) - 3)
           end_idx = min(len(tokens), max(match_indices) + 4)
           context = " ".join(tokens[start_idx:end_idx]).lower()
           
           # Skip if in CPU/hardware context
           if any(term in context for term in ["cpu", "processor", "desktop", "laptop", "intel", "amd", "core", "processors"]):
               if self.logger:
                   self.logger.debug(f"OS: Rejecting os_edition due to hardware context: '{context}'")
               return {}
       
       # Extract OS type (e.g., Windows, macOS, Linux)
       if self.name == "os_type":
           os_text = matched_text.strip()
           
           # Format Windows versions properly
           if "windows" in os_text.lower() or "win" in os_text.lower():
               # Always set os_type to just "Windows"
               result["os_type"] = "Windows"
               if self.logger:
                   self.logger.debug(f"OS: Detected Windows OS from '{os_text}'")
               
               # Extract version number if present
               version_match = re.search(r'(\d{1,2}(?:\.\d)?|11|2000|xp|vista)', os_text.lower())
               if version_match:
                   version = version_match.group(1)
                   # Map old version names to standardized format
                   version_map = {
                       "xp": "XP",
                       "vista": "Vista",
                       "2000": "2000"
                   }
                   result["os_version"] = version_map.get(version, version)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted Windows version: {result['os_version']}")
           
           # Format macOS versions properly - ENHANCED to handle "OS" + version pattern
           elif any(mac_term in os_text.lower() for mac_term in ["macos", "mac os", "osx", "os x"]) or (
               "os" in os_text.lower() and any(version in os_text.lower() for version in [
                   "monterey", "big sur", "catalina", "mojave", "high sierra", "sierra",
                   "el capitan", "yosemite", "mavericks", "ventura", "sonoma", "sequoia",
                   "mountain lion", "lion", "snow leopard", "leopard", "tiger"
               ])
           ):
               # Always set os_type to just "macOS"
               result["os_type"] = "macOS"
               if self.logger:
                   self.logger.debug(f"OS: Detected macOS from '{os_text}'")
               
               # Extract version name if present
               mac_versions = {
                   "monterey": "Monterey", "big sur": "Big Sur", "catalina": "Catalina",
                   "mojave": "Mojave", "high sierra": "High Sierra", "sierra": "Sierra",
                   "el capitan": "El Capitan", "yosemite": "Yosemite", "mavericks": "Mavericks",
                   "ventura": "Ventura", "sonoma": "Sonoma", "sequoia": "Sequoia",
                   "mountain lion": "Mountain Lion", "lion": "Lion", "snow leopard": "Snow Leopard",
                   "leopard": "Leopard", "tiger": "Tiger"
               }
               
               for version_key, version_name in mac_versions.items():
                   if version_key in os_text.lower():
                       result["os_version"] = version_name
                       if self.logger:
                           self.logger.debug(f"OS: Extracted macOS version name: {version_name}")
                       break
               
               # Check for numeric versions (e.g., 10.15, 11.0)
               numeric_match = re.search(r'(\d{1,2}(?:\.\d{1,2})?)', os_text)
               if numeric_match and "os_version" not in result:
                   result["os_version"] = numeric_match.group(1)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted macOS numeric version: {result['os_version']}")
           
           # iOS
           elif any(ios_term in os_text.lower() for ios_term in ["ios", "iphone os", "ipad os", "ipados"]):
               result["os_type"] = "iOS"
               if self.logger:
                   self.logger.debug(f"OS: Detected iOS from '{os_text}'")
               # Extract iOS version
               version_match = re.search(r'(\d{1,2}(?:\.\d{1,2})?)', os_text)
               if version_match:
                   result["os_version"] = version_match.group(1)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted iOS version: {result['os_version']}")
           
           # Android
           elif "android" in os_text.lower():
               result["os_type"] = "Android"
               if self.logger:
                   self.logger.debug(f"OS: Detected Android from '{os_text}'")
               # Extract Android version or name
               version_match = re.search(r'(\d{1,2}(?:\.\d{1,2})?)', os_text)
               if version_match:
                   result["os_version"] = version_match.group(1)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted Android version: {result['os_version']}")
               else:
                   # Check for Android version names
                   android_versions = {
                       "nougat": "7.0", "oreo": "8.0", "pie": "9.0",
                       "q": "10", "r": "11", "s": "12", "tiramisu": "13"
                   }
                   for name, ver in android_versions.items():
                       if name in os_text.lower():
                           result["os_version"] = ver
                           if self.logger:
                               self.logger.debug(f"OS: Mapped Android name '{name}' to version: {ver}")
                           break
           
           # Handle Linux distributions
           elif any(linux_term in os_text.lower() for linux_term in ["linux", "ubuntu", "fedora", "debian", "centos", "mint"]):
               # Detect specific distribution
               linux_distros = {
                   "ubuntu": "Ubuntu", "fedora": "Fedora", "debian": "Debian",
                   "centos": "CentOS", "mint": "Linux Mint", "redhat": "Red Hat",
                   "arch": "Arch Linux", "gentoo": "Gentoo", "opensuse": "openSUSE",
                   "manjaro": "Manjaro", "kali": "Kali Linux", "elementary": "Elementary OS",
                   "zorin": "Zorin OS", "pop": "Pop!_OS", "mx": "MX Linux"
               }
               
               for distro_key, distro_name in linux_distros.items():
                   if distro_key in os_text.lower():
                       result["os_type"] = distro_name
                       if self.logger:
                           self.logger.debug(f"OS: Detected specific Linux distribution: {distro_name}")
                       break
               else:
                   # If no specific distro found, use generic "Linux"
                   result["os_type"] = "Linux"
                   if self.logger:
                       self.logger.debug(f"OS: Using generic Linux type for '{os_text}'")
               
               # Extract version if present
               version_match = re.search(r'(\d{2}\.\d{2}(?:\.\d{1,2})?)', os_text)
               if version_match:
                   result["os_version"] = version_match.group(1)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted Linux version: {result['os_version']}")
           
           # Chrome OS
           elif any(chrome_term in os_text.lower() for chrome_term in ["chrome os", "chromeos"]):
               result["os_type"] = "Chrome OS"
               if self.logger:
                   self.logger.debug(f"OS: Detected Chrome OS from '{os_text}'")
           
           # FreeBSD
           elif "freebsd" in os_text.lower():
               result["os_type"] = "FreeBSD"
               if self.logger:
                   self.logger.debug(f"OS: Detected FreeBSD from '{os_text}'")
               version_match = re.search(r'(\d{1,2}(?:\.\d)?)', os_text)
               if version_match:
                   result["os_version"] = version_match.group(1)
                   if self.logger:
                       self.logger.debug(f"OS: Extracted FreeBSD version: {result['os_version']}")
           
           # Unix/Other
           elif any(unix_term in os_text.lower() for unix_term in ["unix", "aix", "solaris", "hp-ux"]):
               unix_types = {
                   "aix": "AIX", "solaris": "Solaris", "hp-ux": "HP-UX"
               }
               for key, value in unix_types.items():
                   if key in os_text.lower():
                       result["os_type"] = value
                       if self.logger:
                           self.logger.debug(f"OS: Detected Unix variant: {value}")
                       break
               else:
                   result["os_type"] = "Unix"
                   if self.logger:
                       self.logger.debug(f"OS: Using generic Unix type for '{os_text}'")
           
           # Fallback
           else:
               result["os_type"] = os_text
               if self.logger:
                   self.logger.debug(f"OS: Using fallback OS type: '{os_text}'")
       
       # Extract OS edition for Windows (e.g., Home, Pro, Enterprise)
       elif self.name == "os_edition":
           edition_text = matched_text.strip()
           # Normalize common Windows editions
           edition_map = {
               "professional": "Pro",
               "ultimate": "Ultimate",
               "enterprise": "Enterprise",
               "education": "Education",
               "home premium": "Home Premium",
               "home": "Home",
               "pro": "Pro",
               "server": "Server"
           }
           
           normalized_edition = edition_map.get(edition_text.lower(), edition_text)
           result["os_edition"] = normalized_edition
           if self.logger:
               self.logger.debug(f"OS: Extracted OS edition: '{normalized_edition}'")
       
       # Extract OS version
       elif self.name == "os_version":
           version_text = matched_text.strip()
           
           # Validate Windows versions
           windows_versions = ["11", "10", "8.1", "8", "7", "vista", "xp", "2000", "me", "98", "95"]
           if version_text.lower() in windows_versions:
               result["os_version"] = version_text
               if self.logger:
                   self.logger.debug(f"OS: Extracted version: {version_text}").upper() if version_text.lower() in ["xp", "me"] else version_text
               if self.logger:
                   self.logger.debug(f"OS: Extracted Windows version: {result['os_version']}")
           
           # Validate macOS versions
           mac_versions = ["monterey", "big sur", "catalina", "mojave", "high sierra", 
                         "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                         "sonoma", "sequoia", "mountain lion", "lion", "snow leopard"]
           if version_text.lower() in mac_versions:
               result["os_version"] = version_text
               if self.logger:
                   self.logger.debug(f"OS: Extracted version: {version_text}").title()
               if self.logger:
                   self.logger.debug(f"OS: Extracted macOS version: {result['os_version']}")
           
           # Validate Linux/numeric versions
           elif re.match(r'^\d{1,2}\.\d{1,2}(?:\.\d{1,2})?$', version_text):
               result["os_version"] = version_text
               if self.logger:
                   self.logger.debug(f"OS: Extracted version: {version_text}")
           
           # iOS/Android versions
           elif re.match(r'^\d{1,2}(?:\.\d{1,2})?$', version_text):
               result["os_version"] = version_text
               if self.logger:
                   self.logger.debug(f"OS: Extracted version: {version_text}")
           
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

# Configuration for OS extractors with comprehensive patterns
extractor_config = [
   {
       "name": "os_status",
       "patterns": [
           # No OS indicators
           [str_pat("no"), str_pat("os")],
           [str_pat("not"), str_pat("included")],
           [str_pat("n/a")],
           [str_pat("without"), str_pat("os")]
       ],
       "multiple": False,
       "class": OSExtractor,
   },
   {
       "name": "os_type",
       "patterns": [
           # Windows patterns with version
           [str_pat("windows"), list_pat(["11", "10", "8.1", "8", "7", "vista", "xp", "2000", "me", "98", "95"], optional=True)],
           [regex_pat(r"win\s*(11|10|8\.1|8|7|vista|xp|2000|me|98|95)?")],
           
           # macOS patterns with specific versions - ENHANCED
           [str_pat("macos"), 
            list_pat(["monterey", "big sur", "catalina", "mojave", "high sierra", 
                      "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                      "sonoma", "sequoia", "mountain lion", "lion", "snow leopard", "leopard", "tiger"], optional=True)],
           [list_pat(["mac os", "osx", "os x"]), 
            list_pat(["monterey", "big sur", "catalina", "mojave", "high sierra", 
                      "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                      "sonoma", "sequoia", "mountain lion", "lion", "snow leopard", "leopard", "tiger"], optional=True)],
           
           # NEW: Simple "OS" + macOS version name pattern for Apple products
           [str_pat("os"), 
            list_pat(["monterey", "big sur", "catalina", "mojave", "high sierra", 
                      "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                      "sonoma", "sequoia", "mountain lion", "lion", "snow leopard", "leopard", "tiger"])],
           
           # iOS patterns
           [list_pat(["ios", "iphone os", "ipad os", "ipados"]), regex_pat(r"\d{1,2}(?:\.\d{1,2})?", optional=True)],
           
           # Android patterns
           [str_pat("android"), regex_pat(r"\d{1,2}(?:\.\d{1,2})?", optional=True)],
           [str_pat("android"), list_pat(["nougat", "oreo", "pie", "q", "r", "s", "tiramisu"], optional=True)],
           
           # Chrome OS
           [list_pat(["chrome os", "chromeos"])],
           
           # Linux distributions
           [str_pat("linux")],
           [list_pat(["ubuntu", "fedora", "debian", "centos", "redhat", "mint", 
                      "arch", "gentoo", "opensuse", "manjaro", "kali", "zorin", 
                      "elementary", "pop", "mx", "suse"])],
           
           # FreeBSD
           [str_pat("freebsd"), regex_pat(r"\d{1,2}(?:\.\d)?", optional=True)],
           
           # Unix variants
           [list_pat(["unix", "aix", "solaris", "hp-ux"])],
       ],
       "multiple": False,
       "class": OSExtractor,
   },
   {
       "name": "os_edition",
       "patterns": [
           # Windows editions with explicit Windows context
           [str_pat("windows"), list_pat(["home", "pro", "professional", "enterprise", "education", "ultimate", "server", "starter"])],
           
           # Edition followed by "edition" word
           [list_pat(["home", "pro", "professional", "enterprise", "education", "ultimate", "server", "starter"]), str_pat("edition")],
           
           # Specific Windows edition patterns with word boundaries
           [regex_pat(r"windows\s+\w*\s*(home\s*premium|pro\s*education)")],
           
           # Standalone edition patterns with word boundaries (only when clearly OS-related)
           [regex_pat(r"\b(home|pro|professional|enterprise|education|ultimate|server)\s+edition\b")],
           
           # Windows-specific edition phrases
           [str_pat("windows"), regex_pat(r"home\s*premium")],
           [str_pat("windows"), regex_pat(r"pro\s*education")],
       ],
       "multiple": False,
       "class": OSExtractor,
   },
   {
       "name": "os_version",
       "patterns": [
           # Windows versions
           [list_pat(["windows", "win"]), list_pat(["11", "10", "8.1", "8", "7", "vista", "xp", "2000", "me", "98", "95"])],
           
           # macOS version names with OS context - ENHANCED
           [list_pat(["macos", "mac os", "osx", "os x"]), 
            list_pat(["monterey", "big sur", "catalina", "mojave", "high sierra", 
                     "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                     "sonoma", "sequoia", "mountain lion", "lion", "snow leopard", "leopard", "tiger"])],
           [list_pat(["macos", "mac os", "osx", "os x"]), regex_pat(r"\d{1,2}\.\d{1,2}")],
           
           # NEW: Simple "OS" + macOS version name pattern
           [str_pat("os"), 
            list_pat(["monterey", "big sur", "catalina", "mojave", "high sierra", 
                     "sierra", "el capitan", "yosemite", "mavericks", "ventura", 
                     "sonoma", "sequoia", "mountain lion", "lion", "snow leopard", "leopard", "tiger"])],
                     
           # Linux version numbers with distribution mention
           [list_pat(["ubuntu", "fedora", "debian", "centos", "redhat", "mint", "suse"]), 
            regex_pat(r"\d{2}\.\d{2}(?:\.\d{1,2})?")],
           
           # iOS versions
           [list_pat(["ios", "iphone os", "ipad os", "ipados"]), regex_pat(r"\d{1,2}(?:\.\d{1,2})?")],
           
           # Android versions
           [str_pat("android"), regex_pat(r"\d{1,2}(?:\.\d{1,2})?")],
           
           # FreeBSD versions
           [str_pat("freebsd"), regex_pat(r"\d{1,2}(?:\.\d)?")]
       ],
       "multiple": False,
       "class": OSExtractor,
   }
]