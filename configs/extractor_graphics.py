from configs.parser import BaseExtractor
import re

# Import the GPU database if it exists, otherwise define basic K-series models
try:
    from configs.gpu_models_database import get_gpu_info, is_valid_gpu_model, find_gpu_in_tokens, ALL_GPUS
    HAS_GPU_DATABASE = True
except ImportError:
    HAS_GPU_DATABASE = False
    # Basic K-series GPU models for fallback
    QUADRO_K_SERIES = {
        "K5100M", "K4100M", "K3100M", "K2100M", "K1100M", "K610M", "K510M",
        "Quadro K5100M", "Quadro K4100M", "Quadro K3100M", "Quadro K2100M", 
        "Quadro K1100M", "Quadro K610M", "Quadro K510M"
    }

def process_match(name, tokens, match_indices, logger=None):
    """Process GPU-specific matches, extracting detailed information."""
    result = {}
    matched_text = " ".join([tokens[i] for i in match_indices]).lower()
    
    # FIRST: Check for standalone GPU pattern (e.g., "2GB GPU")
    if len(match_indices) == 2 and matched_text.endswith(" gpu"):
        if logger:
            logger.debug(f"Graphics: Detected standalone GPU pattern: '{matched_text}'")
        # Extract memory size from the first token
        memory_token = tokens[match_indices[0]]
        ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', memory_token)
        if ram_match:
            size = ram_match.group(1)
            unit = ram_match.group(0)[-2:].upper()
            # Only use smaller memory sizes for GPU (typically 1-16GB)
            if unit.upper() == "GB" and int(float(size)) <= 16:
                result["gpu_ram_size"] = f"{size}{unit}"
                if logger:
                    logger.debug(f"Graphics: Extracted GPU RAM size: {size}{unit}")
                return result
            elif unit.upper() == "MB":
                result["gpu_ram_size"] = f"{size}{unit}"
                if logger:
                    logger.debug(f"Graphics: Extracted GPU RAM size: {size}{unit}")
                return result
    
    # Define GPU context keywords to check for presence of GPU
    gpu_keywords = {"nvidia", "amd", "intel", "geforce", "radeon", "quadro", "tesla", "arc", "iris", "uhd", "hd", "gtx", "rtx", "gt", "rx", "k1100m", "k2100m", "k3100m", "k4100m", "k5100m"}
    
    # Check if any GPU-related keywords are in the full token list
    full_text = " ".join(tokens).lower()
    has_gpu_context = any(keyword in full_text for keyword in gpu_keywords)
    
    def normalize_nvidia_model(model_text, has_geforce=False):
        """Normalize NVIDIA model numbers to include appropriate series prefix."""
        if not model_text:
            return model_text
            
        # Check if model is just a number (like "1070", "2080", etc.)
        number_match = re.match(r'^(\d{3,4})([a-z]*|ti|super|mobile)?$', model_text.lower())
        if number_match and has_geforce:
            model_num = int(number_match.group(1))
            suffix = number_match.group(2) if number_match.group(2) else ""
            
            # Normalize suffix
            if suffix == "mobile":
                suffix = "M"
            elif suffix:
                suffix = suffix.upper()
            
            # Determine series based on model number
            if model_num >= 2000:  # RTX series (2000, 3000, 4000+)
                prefix = "RTX"
            elif model_num >= 600:  # GTX series (600, 700, 900, 1000)
                prefix = "GTX"
            else:  # GT series for lower numbers
                prefix = "GT"
            
            return f"{prefix} {model_num}{suffix}" if suffix else f"{prefix} {model_num}"
        
        return model_text
    
    if name == "gpu":
        # FIRST: Check for exact matches using database if available
        if HAS_GPU_DATABASE:
            # Only check the matched tokens, not all tokens
            matched_tokens = [tokens[i] for i in match_indices]
            found_gpus = find_gpu_in_tokens(matched_tokens)
            if found_gpus:
                gpu_model, _, _ = found_gpus[0]
                brand, series, normalized_model = get_gpu_info(gpu_model)
                
                if brand:
                    result["gpu_brand"] = brand
                if series:
                    result["gpu_series"] = series
                if normalized_model:
                    result["gpu_model"] = normalized_model
                
                # ENHANCED: Look for GPU RAM in the matched tokens first, then nearby
                for idx in match_indices:
                    if idx < len(tokens):
                        token = tokens[idx]
                        ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', token)
                        if ram_match:
                            size = ram_match.group(1)
                            unit = ram_match.group(0)[-2:].upper()
                            if unit.upper() == "GB" and int(float(size)) <= 16:
                                result["gpu_ram_size"] = f"{size}{unit}"
                                return result
                            elif unit.upper() == "MB":
                                result["gpu_ram_size"] = f"{size}{unit}"
                                return result
                
                # Look for GPU RAM in tokens immediately following the GPU model
                # Find the position of the GPU model in the full token list
                gpu_token_positions = []
                for i, token in enumerate(tokens):
                    if token.upper() == gpu_model.upper():
                        gpu_token_positions.append(i)
                
                # Look for memory size in the next few tokens after the GPU
                for gpu_pos in gpu_token_positions:
                    for offset in range(1, 4):  # Check next 3 tokens
                        if gpu_pos + offset < len(tokens):
                            next_token = tokens[gpu_pos + offset]
                            ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', next_token)
                            if ram_match:
                                size = ram_match.group(1)
                                unit = ram_match.group(0)[-2:].upper()
                                # Only use smaller memory sizes for GPU (typically 1-16GB)
                                if unit.upper() == "GB" and int(float(size)) <= 16:
                                    result["gpu_ram_size"] = f"{size}{unit}"
                                    break
                                elif unit.upper() == "MB":
                                    result["gpu_ram_size"] = f"{size}{unit}"
                                    break
                    if "gpu_ram_size" in result:
                        break
                
                return result
        
        # SECOND: Check for K-series mobile Quadro cards specifically
        # Only check if we have a single token that matches K-series pattern
        if len(match_indices) == 1:
            token = tokens[match_indices[0]]
            k_series_match = re.match(r'^K(\d{4})M?$', token, re.IGNORECASE)
            if k_series_match:
                model_num = k_series_match.group(1)
                full_model = f"K{model_num}M" if not token.upper().endswith('M') else token.upper()
                
                # Check if it's a known Quadro model
                if HAS_GPU_DATABASE:
                    if is_valid_gpu_model(full_model):
                        brand, series, normalized_model = get_gpu_info(full_model)
                        result["gpu_brand"] = brand or "NVIDIA"
                        result["gpu_series"] = series or "Quadro"
                        result["gpu_model"] = normalized_model or full_model
                    else:
                        result["gpu_brand"] = "NVIDIA"
                        result["gpu_series"] = "Quadro"
                        result["gpu_model"] = full_model
                else:
                    # Fallback without database
                    if full_model in QUADRO_K_SERIES:
                        result["gpu_brand"] = "NVIDIA"
                        result["gpu_series"] = "Quadro"
                        result["gpu_model"] = full_model
                
                # Look for associated memory size in the next few tokens
                gpu_index = match_indices[0]
                for offset in range(1, 4):  # Check next 3 tokens
                    if gpu_index + offset < len(tokens):
                        next_token = tokens[gpu_index + offset]
                        ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', next_token)
                        if ram_match:
                            size = ram_match.group(1)
                            unit = ram_match.group(0)[-2:].upper()
                            # For K-series, typical VRAM is 1-4GB
                            if unit.upper() == "GB" and int(float(size)) <= 8:
                                result["gpu_ram_size"] = f"{size}{unit}"
                                break
                            elif unit.upper() == "MB":
                                result["gpu_ram_size"] = f"{size}{unit}"
                                break
                
                return result
        
        # SINGLE-TOKEN Quadro/P-series/mobile GPUs (e.g., 'QuadroM1000M', 'P1000', 'M2000M')
        if len(match_indices) == 1:
            tk = tokens[match_indices[0]].upper()
            # QuadroMXXXXM
            m = re.match(r"QUADRO([A-Z]?\d{3,4}M?)", tk, re.IGNORECASE)
            if m:
                result.update({"gpu_brand":"NVIDIA","gpu_series":"QUADRO","gpu_model":m.group(1).upper()})
            else:
                # P-series or mobile M-series
                m = re.match(r"P(\d{3,4}M?)", tk, re.IGNORECASE)
                if m:
                    result.update({"gpu_brand":"NVIDIA","gpu_series":"QUADRO","gpu_model":f'P{m.group(1).upper()}'})
                else:
                    m = re.match(r"M(\d{3,4})M", tk, re.IGNORECASE)
                    if m:
                        result.update({"gpu_brand":"NVIDIA","gpu_series":"QUADRO","gpu_model":f'M{m.group(1)}M'})

            # If we identified a single-token GPU, also capture VRAM from next 3 tokens
            if "gpu_model" in result:
                idx = match_indices[0]
                for off in range(1,4):
                    if idx+off < len(tokens):
                        mem_tok = tokens[idx+off]
                        mem_m = re.search(r"(\d+(?:\.\d+)?)\s*(GB|MB)", mem_tok, re.IGNORECASE)
                        if mem_m:
                            size,unit = mem_m.groups(); unit = unit.upper()
                            if unit=="MB" or (unit=="GB" and int(float(size))<=16):
                                result["gpu_ram_size"] = f"{size}{unit}"
                                break
            if result:
                return result

        # THIRD: Extract GPU RAM size from matched tokens if present (but be selective)
        gpu_ram_size = None
        for idx in match_indices:
            if idx < len(tokens):
                token = tokens[idx]
                ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', token)
                if ram_match:
                    size = ram_match.group(1)
                    unit = ram_match.group(0)[-2:].upper()
                    # Only consider smaller sizes as GPU RAM (not system RAM)
                    if unit.upper() == "GB" and int(float(size)) <= 16:
                        gpu_ram_size = f"{size}{unit}"
                        result["gpu_ram_size"] = gpu_ram_size
                        break
                    elif unit.upper() == "MB":
                        gpu_ram_size = f"{size}{unit}"
                        result["gpu_ram_size"] = gpu_ram_size
                        break
        
        # FOURTH: AIB Partner pattern for GeForce cards (only if we have multiple tokens)
        if len(match_indices) > 1:
            aib_geforce_pattern = r"(?P<aib_brand>evga|asus|msi|gigabyte|zotac|sapphire|xfx|powercolor|pny|asrock)\s*(?P<sub_brand>geforce)\s*(?P<model>\d{3,4})\s*(?P<suffix>gs|gt|gts|gtx|le|se|gso)?"
            match = re.search(aib_geforce_pattern, matched_text, re.IGNORECASE)
            if match:
                data = match.groupdict()
                if data["aib_brand"]:
                    result["gpu_brand"] = data["aib_brand"].upper()
                if data["sub_brand"]:
                    result["gpu_series"] = data["sub_brand"].upper()
                if data["model"]:
                    model_parts = [data["model"]]
                    if data["suffix"]:
                        model_parts.append(data["suffix"].upper())
                    model_text = " ".join(model_parts)
                    result["gpu_model"] = normalize_nvidia_model(model_text, True)
                return result
        
        # FOURTH-B: Professional RTX/GTX cards (RTX 4000, GTX 4000, etc.)
        if len(match_indices) == 2:
            # Check if we have RTX/GTX + model number pattern
            series_token = tokens[match_indices[0]].upper()
            model_token = tokens[match_indices[1]]
            
            if series_token in ["RTX", "GTX", "GT"]:
                model_match = re.match(r'(\d{3,4})([a-z]*|ti|super|mx|mobile)?$', model_token, re.IGNORECASE)
                if model_match:
                    model_num = int(model_match.group(1))
                    suffix = model_match.group(2) if model_match.group(2) else ""
                    
                    # Normalize suffix
                    if suffix and suffix.lower() == "mobile":
                        suffix = "M"
                    elif suffix:
                        suffix = suffix.upper()
                    
                    # Determine if it's a professional card based on model number
                    # Professional cards: RTX 4000, RTX 5000, RTX 6000, RTX 8000, RTX A4000, etc.
                    # GTX Titan series, GTX x000 series (4000, 5000, 6000)
                    is_professional = (
                        (series_token == "RTX" and model_num in [4000, 5000, 6000, 8000]) or
                        (series_token == "RTX" and model_token.upper().startswith("A")) or  # RTX A4000, A5000, etc.
                        (series_token == "GTX" and model_num >= 4000) or
                        (series_token == "GTX" and "titan" in model_token.lower())
                    )
                    
                    result["gpu_brand"] = "NVIDIA"
                    if is_professional:
                        result["gpu_series"] = "QUADRO"
                    else:
                        result["gpu_series"] = "GEFORCE"
                    
                    full_model = f"{series_token} {model_num}{suffix}" if suffix else f"{series_token} {model_num}"
                    result["gpu_model"] = full_model
                    
                    return result
            
            # ADDED: Handle combined format like "GT730" + "2GB"
            combined_match = re.match(r'(?:GTX|RTX|GT)(\d{3,4})([a-z]*|ti|super|mx|mobile)?$', model_token, re.IGNORECASE)
            if combined_match:
                prefix = re.match(r'(GTX|RTX|GT)', model_token, re.IGNORECASE).group(1).upper()
                model_num = int(combined_match.group(1))
                suffix = combined_match.group(2) if combined_match.group(2) else ""
                
                # Normalize suffix
                if suffix and suffix.lower() == "mobile":
                    suffix = "M"
                elif suffix:
                    suffix = suffix.upper()
                
                # Set brand and series
                result["gpu_brand"] = "NVIDIA"
                result["gpu_series"] = "GEFORCE"
                
                # Format model
                full_model = f"{prefix} {model_num}{suffix}" if suffix else f"{prefix} {model_num}"
                result["gpu_model"] = full_model
                
                # Look for memory size in the next token
                if match_indices[1] + 1 < len(tokens):
                    next_token = tokens[match_indices[1] + 1]
                    ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', next_token)
                    if ram_match:
                        size = ram_match.group(1)
                        unit = ram_match.group(0)[-2:].upper()
                        if unit.upper() == "GB" and int(float(size)) <= 16:
                            result["gpu_ram_size"] = f"{size}{unit}"
                        elif unit.upper() == "MB":
                            result["gpu_ram_size"] = f"{size}{unit}"
                
                return result

        # ENHANCED: GeForce/NVIDIA pattern handling with GPU RAM extraction
        if len(match_indices) > 1:
            # Enhanced NVIDIA pattern to better capture GeForce GT/GTX/RTX cards with memory
            nvidia_pattern = r"(?P<brand>nvidia)?\s*(?P<sub_brand>geforce|quadro|tesla)?\s*((?P<series>rtx|gtx|gt)\s*(?P<model>\d{3,4}[a-z]*|\d{3,4}\s+(ti|super|mx|mobile))|(?P<series_model>[a-z]*\d{3,4}[a-z]*))"
            match = re.search(nvidia_pattern, matched_text, re.IGNORECASE)
            if match:
                data = match.groupdict()
                if data["brand"]:
                    result["gpu_brand"] = data["brand"].upper()
                elif data["sub_brand"] is not None and data["sub_brand"].lower() in ["geforce", "quadro", "tesla"]:
                    result["gpu_brand"] = "NVIDIA"
                if data["sub_brand"]:
                    result["gpu_series"] = data["sub_brand"].upper()
                if data["series"] and data["model"]:
                    model = data["model"].upper()
                    model = re.sub(r'\bMOBILE\b', 'M', model)
                    result["gpu_model"] = f"{data['series'].upper()} {model}"
                elif data["series_model"]:
                    split_match = re.match(r"([a-z]+)(\d+[a-z]*)", data["series_model"], re.IGNORECASE)
                    if split_match:
                        series, model = split_match.groups()
                        model = re.sub(r'\bmobile\b', 'M', model, flags=re.IGNORECASE)
                        result["gpu_model"] = f"{series.upper()} {model.upper()}"
                    else:
                        model = data["series_model"].upper()
                        model = re.sub(r'\bMOBILE\b', 'M', model)
                        result["gpu_model"] = model
                
                # Extract GPU RAM from the matched tokens
                for idx in match_indices:
                    if idx < len(tokens):
                        token = tokens[idx]
                        ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', token)
                        if ram_match:
                            size = ram_match.group(1)
                            unit = ram_match.group(0)[-2:].upper()
                            # Only consider smaller sizes as GPU RAM (not system RAM)
                            if unit.upper() == "GB" and int(float(size)) <= 16:
                                result["gpu_ram_size"] = f"{size}{unit}"
                                break
                            elif unit.upper() == "MB":
                                result["gpu_ram_size"] = f"{size}{unit}"
                                break
                
                return result
        
        # Continue with other patterns for multi-token matches only...
        if len(match_indices) > 1:
            # AMD pattern
            amd_pattern = r"(?P<brand>amd)?\s*(?P<sub_brand>radeon)?\s*(?P<series>rx|r9|r7|r5)?\s*(?P<model>\d{3,4})\s*(?P<suffix>xt|x2|pro|mobile)?"
            match = re.search(amd_pattern, matched_text, re.IGNORECASE)
            if match:
                data = match.groupdict()
                result["gpu_brand"] = "AMD"
                radeon_index = next((i for i, idx in enumerate(match_indices) if tokens[idx].lower() == "radeon"), None)
                if radeon_index is not None:
                    result["gpu_series"] = tokens[match_indices[radeon_index]].upper()
                    if radeon_index + 1 < len(match_indices):
                        model_tokens = [tokens[match_indices[i]] for i in range(radeon_index + 1, len(match_indices))]
                        model_tokens = [t for t in model_tokens if not re.search(r'\d+(?:GB|gb|MB|mb)', t)]
                        model_text = " ".join(model_tokens)
                        model_text = re.sub(r'\bmobile\b', 'M', model_text, flags=re.IGNORECASE)
                        result["gpu_model"] = model_text
                else:
                    model_tokens = [tokens[i] for i in match_indices]
                    model_tokens = [t for t in model_tokens if not re.search(r'\d+(?:GB|gb|MB|mb)', t)]
                    model_text = " ".join(model_tokens)
                    model_text = re.sub(r'\bmobile\b', 'M', model_text, flags=re.IGNORECASE)
                    result["gpu_model"] = model_text
                return result
            
            # Intel pattern
            intel_pattern = r"(?P<brand>intel)?\s*(?P<series>arc|iris|uhd|hd)?\s*(?P<model>a\d{3,4}|\d{3,4})?"
            match = re.search(intel_pattern, matched_text, re.IGNORECASE)
            if match:
                data = match.groupdict()
                if data["brand"]:
                    result["gpu_brand"] = data["brand"].upper()
                elif data["series"] is not None and data["series"].lower() in ["arc", "iris", "uhd", "hd"]:
                    result["gpu_brand"] = "INTEL"
                if data["series"]:
                    result["gpu_series"] = data["series"].upper()
                if data["model"]:
                    result["gpu_model"] = data["model"]
                return result
        
        # ENHANCED: Handle specific GeForce patterns that might not match above
        if len(match_indices) >= 2:
            # Check for "GeForce GT730" pattern (GeForce + combined format)
            if tokens[match_indices[0]].lower() == "geforce":
                # Check if next token is a combined format like GT730
                combined_match = re.match(r'(?:GTX|RTX|GT)(\d{3,4})([a-z]*|ti|super|mx|mobile)?$', tokens[match_indices[1]], re.IGNORECASE)
                if combined_match:
                    prefix = re.match(r'(GTX|RTX|GT)', tokens[match_indices[1]], re.IGNORECASE).group(1).upper()
                    model_num = combined_match.group(1)
                    suffix = combined_match.group(2) if combined_match.group(2) else ""
                    
                    # Normalize suffix
                    if suffix and suffix.lower() == "mobile":
                        suffix = "M"
                    elif suffix:
                        suffix = suffix.upper()
                    
                    result["gpu_brand"] = "NVIDIA"
                    result["gpu_series"] = "GEFORCE"
                    full_model = f"{prefix} {model_num}{suffix}" if suffix else f"{prefix} {model_num}"
                    result["gpu_model"] = full_model
                    
                    # Look for GPU RAM in next token
                    if len(match_indices) > 2:
                        token = tokens[match_indices[2]]
                        ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', token)
                        if ram_match:
                            size = ram_match.group(1)
                            unit = ram_match.group(0)[-2:].upper()
                            if unit.upper() == "GB" and int(float(size)) <= 16:
                                result["gpu_ram_size"] = f"{size}{unit}"
                            elif unit.upper() == "MB":
                                result["gpu_ram_size"] = f"{size}{unit}"
                    
                    return result
                
                # Check for "GeForce GT/GTX/RTX model [memory]" patterns (separate tokens)
                if len(match_indices) >= 3:
                    series_token = tokens[match_indices[1]].upper()
                    if series_token in ["GT", "GTX", "RTX"]:
                        model_token = tokens[match_indices[2]]
                        model_match = re.match(r'(\d{3,4})([a-z]*|ti|super|mx|mobile)?$', model_token, re.IGNORECASE)
                        if model_match:
                            model_num = model_match.group(1)
                            suffix = model_match.group(2) if model_match.group(2) else ""
                            
                            # Normalize suffix
                            if suffix and suffix.lower() == "mobile":
                                suffix = "M"
                            elif suffix:
                                suffix = suffix.upper()
                            
                            result["gpu_brand"] = "NVIDIA"
                            result["gpu_series"] = "GEFORCE"
                            full_model = f"{series_token} {model_num}{suffix}" if suffix else f"{series_token} {model_num}"
                            result["gpu_model"] = full_model
                            
                            # Look for GPU RAM in remaining tokens
                            for i in range(3, len(match_indices)):
                                if match_indices[i] < len(tokens):
                                    token = tokens[match_indices[i]]
                                    ram_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|gb|MB|mb)\b', token)
                                    if ram_match:
                                        size = ram_match.group(1)
                                        unit = ram_match.group(0)[-2:].upper()
                                        if unit.upper() == "GB" and int(float(size)) <= 16:
                                            result["gpu_ram_size"] = f"{size}{unit}"
                                            break
                                        elif unit.upper() == "MB":
                                            result["gpu_ram_size"] = f"{size}{unit}"
                                            break
                            
                            return result
        
        # Fallback: set gpu_spec if no specific match and we have multiple tokens
        if len(match_indices) > 1:
            spec_tokens = [tokens[i] for i in match_indices]
            spec_tokens = [t for t in spec_tokens if not re.search(r'\d+(?:GB|gb|MB|mb)', t)]
            if spec_tokens:
                result["gpu_spec"] = " ".join(spec_tokens).lower()
        
        return result
    
    # Handle single-token GPU patterns caught by the regex above
    if name == "gpu_ram_size":
        memory_text = matched_text.strip().replace(" ", "").upper()
        result[name] = memory_text
    elif name == "gpu_memory_type" and has_gpu_context:
        result[name] = matched_text.upper()
    elif name == "gpu_type":
        result[name] = "Integrated" if "integrated" in matched_text.lower() else "Dedicated"
    elif name == "gpu_spec":
        normalized_text = re.sub(r'\bmobile\b', 'M', matched_text, flags=re.IGNORECASE)
        result[name] = normalized_text
    
    return result
    
class GraphicsExtractor(BaseExtractor):
    """Extractor for GPU information, parsing brand, series, model, and other details."""
    
    def process_match(self, tokens, match_indices):
        if self.logger:
            matched_text = " ".join([tokens[i] for i in match_indices])
            self.logger.debug(f"Graphics: Processing match: '{matched_text}'")
        result = process_match(self.name, tokens, match_indices, self.logger)
        if self.logger and result:
            self.logger.debug(f"Graphics: Extracted result: {result}")
        return result

# Helper functions for pattern definitions
def str_pat(value, optional=False, show=True, case_sensitive=False):
    """Defines a string pattern for exact text matching."""
    return {"type": "string", "value": value, "optional": optional, "include_in_output": show, "case_sensitive": case_sensitive}

def regex_pat(pattern, optional=False, show=True, case_sensitive=False):
    """Defines a regex pattern for flexible matching."""
    return {"type": "regex", "pattern": pattern, "optional": optional, "include_in_output": show, "case_sensitive": case_sensitive}

def list_pat(values, optional=False, show=True):
    """Defines a list pattern to match any one of several values."""
    return {"type": "list", "values": values, "optional": optional, "include_in_output": show}

# Configuration for GPU extractors with specific K-series support
extractor_config = [
    {
        "name": "gpu",
        "patterns": [
            # HIGHEST PRIORITY: Standalone GPU with memory size (e.g., "2GB GPU")
            [regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False),
             str_pat("GPU", show=True, case_sensitive=False)],
            # HIGHEST PRIORITY: GPU keyword followed by memory size (e.g., "GPU 4GB", "Graphics 8GB", "Video 2GB")
            [str_pat("GPU", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            [str_pat("Graphics", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            [str_pat("Video", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGHEST PRIORITY: Specific K-series mobile Quadro patterns
            [regex_pat(r"K\d{4}M?", show=True, case_sensitive=False)],

            # SINGLE TOKEN Quadro/P-series/mobile patterns (e.g. 'QuadroM1000M', 'P1000', 'M2000M')
            [regex_pat(r"(?:QUADRO[A-Z0-9]+|P\d{3,4}M?|M\d{3,4}M)", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: Professional RTX/GTX cards without GeForce branding (RTX 4000, GTX 4000, etc.)
            [list_pat(["GTX", "RTX", "GT"], show=True),
             regex_pat(r"\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: GTX/RTX/GT + model number + memory size (separate tokens)
            [list_pat(["GTX", "RTX", "GT"], show=True),
             regex_pat(r"\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: Combined model format (GT730, GTX1050, RTX3060, etc.) + memory size
            [regex_pat(r"(?:GTX|RTX|GT)\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: GeForce GTX/RTX/GT + model number + memory size (separate tokens)
            [str_pat("GeForce", show=True, case_sensitive=False),
             list_pat(["GTX", "RTX", "GT"], show=True),
             regex_pat(r"\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: GeForce + combined model (GT730, GTX1050, RTX3060, etc.) + memory size
            [str_pat("GeForce", show=True, case_sensitive=False),
             regex_pat(r"(?:GTX|RTX|GT)\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: NVIDIA GeForce GTX/RTX/GT + model number + memory size (separate tokens)
            [str_pat("NVIDIA", optional=True, show=True, case_sensitive=False),
             str_pat("GeForce", show=True, case_sensitive=False),
             list_pat(["GTX", "RTX", "GT"], show=True),
             regex_pat(r"\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: NVIDIA GeForce + combined model (GT730, GTX1050, RTX3060, etc.) + memory size
            [str_pat("NVIDIA", optional=True, show=True, case_sensitive=False),
             str_pat("GeForce", show=True, case_sensitive=False),
             regex_pat(r"(?:GTX|RTX|GT)\d{3,4}(?:ti|super|MX|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # HIGH PRIORITY: AMD Radeon + series + model + memory size
            [str_pat("AMD", optional=True, show=True, case_sensitive=False),
             str_pat("Radeon", show=True, case_sensitive=False),
             list_pat(["RX", "R9", "R7", "R5"], show=True),
             regex_pat(r"\d{3,4}(?:xt|x2|pro|mobile)?", show=True, case_sensitive=False),
             regex_pat(r"\d+(?:\.\d+)?\s*(?:GB|gb|MB|mb)\b", show=True, case_sensitive=False)],
            
            # AIB Partner GeForce cards
            [list_pat(["EVGA", "ASUS", "MSI", "GIGABYTE", "ZOTAC", "Sapphire", "XFX", "PowerColor", "PNY", "ASRock", "Gainward", "Palit", "Inno3D", "Colorful", "KFA2", "Galax", "Yeston", "Biostar"], show=True),
             str_pat("GeForce", show=True, case_sensitive=False),
             regex_pat(r"\d{3,4}", show=True, case_sensitive=False),
             list_pat(["GS", "GT", "GTS", "GTX", "LE", "SE", "GSO"], optional=True, show=True)],
            
            # Older GeForce cards pattern
            [str_pat("NVIDIA", optional=True, show=True, case_sensitive=False),
             str_pat("GeForce", show=True, case_sensitive=False),
             regex_pat(r"\d{3,4}", show=True, case_sensitive=False),
             list_pat(["GS", "GT", "GTS", "GTX", "LE", "SE", "GSO"], optional=True, show=True)],
            
            # AMD patterns
            [str_pat("AMD", optional=True, show=True, case_sensitive=False), 
             str_pat("Radeon", optional=True, show=True, case_sensitive=False), 
             list_pat(["RX", "R9", "R7", "R5"], show=True), 
             regex_pat(r"\d{3,4}(?:xt|x2|pro|mobile)?", show=True, case_sensitive=False)],
            
            # Intel patterns
            [str_pat("Intel", optional=True, show=True, case_sensitive=False), 
             list_pat(["Arc", "Iris", "UHD", "HD"], show=True), 
             regex_pat(r"a\d{3,4}|\d{3,4}", optional=True, show=True, case_sensitive=False)],
        ],
        "multiple": False,
        "class": GraphicsExtractor,
    },
    {
        "name": "gpu_memory_type",
        "patterns": [
            [list_pat(["GDDR6", "GDDR5", "GDDR5X", "GDDR4", "GDDR3", "DDR3", "DDR2", "DDR4", "DDR5", "HBM", "HBM2", "HBM2e"], show=True)]
        ],
        "multiple": False,
        "class": GraphicsExtractor,
    },
    {
        "name": "gpu_type",
        "patterns": [
            [list_pat(["integrated", "onboard", "built-in"], show=True)],
            [list_pat(["dedicated", "discrete", "external"], show=True)]
        ],
        "multiple": False,
        "class": GraphicsExtractor,
    },
    {
        "name": "gpu_spec",
        "patterns": [
            # Complete GPU specifications
            [str_pat("NVIDIA", optional=False, show=True, case_sensitive=False), 
             str_pat("GeForce", optional=True, show=True, case_sensitive=False),
             list_pat(["RTX", "GTX", "GT", "Quadro", "Tesla", "NVS"], optional=False, show=True), 
             regex_pat(r"\d{3,4}(?:\s*(ti|super|MX|mobile))?", optional=False, show=True, case_sensitive=False),
             regex_pat(r"\d+\s*(?:gb|g\b)(?!e)", optional=True, show=True, case_sensitive=False),
             list_pat(["GDDR6", "GDDR5", "GDDR5X", "HBM2"], optional=True, show=True)],
        ],
        "multiple": False,
        "class": GraphicsExtractor,
    }
]