# Comprehensive GPU Models Database for Exact Matching
# This prevents false positives by using exact model matches instead of patterns

# NVIDIA GeForce Desktop GPUs
nvidia_geforce_desktop = {
    # RTX 40 Series
    "RTX 4090", "RTX 4080", "RTX 4070", "RTX 4060", "RTX 4050",
    
    # RTX 30 Series
    "RTX 3090", "RTX 3080", "RTX 3070", "RTX 3060", "RTX 3050",
    "RTX 3090 Ti", "RTX 3080 Ti", "RTX 3070 Ti", "RTX 3060 Ti",
    
    # RTX 20 Series
    "RTX 2080", "RTX 2070", "RTX 2060", "RTX 2050",
    "RTX 2080 Ti", "RTX 2070 Super", "RTX 2060 Super",
    
    # GTX 16 Series
    "GTX 1660", "GTX 1650", "GTX 1630",
    "GTX 1660 Ti", "GTX 1660 Super", "GTX 1650 Super",
    
    # GTX 10 Series
    "GTX 1080", "GTX 1070", "GTX 1060", "GTX 1050", "GTX 1030",
    "GTX 1080 Ti", "GTX 1070 Ti", "GTX 1050 Ti",
    "GTX TITAN X", "GTX TITAN Xp",
    
    # GTX 900 Series
    "GTX 980", "GTX 970", "GTX 960", "GTX 950",
    "GTX 980 Ti", "GTX TITAN X",
    
    # GTX 700 Series
    "GTX 780", "GTX 770", "GTX 760", "GTX 750", "GTX 745",
    "GTX 780 Ti", "GTX 750 Ti", "GTX TITAN", "GTX TITAN Black",
    
    # GTX 600 Series
    "GTX 690", "GTX 680", "GTX 670", "GTX 660", "GTX 650", "GTX 645", "GTX 640",
    "GTX 660 Ti", "GTX 650 Ti", "GTX 650 Ti Boost",
    
    # GTX 500 Series
    "GTX 590", "GTX 580", "GTX 570", "GTX 560", "GTX 550", "GTX 545",
    "GTX 560 Ti", "GTX 550 Ti",
    
    # GTX 400 Series
    "GTX 480", "GTX 470", "GTX 460", "GTX 450",
    "GTX 460 SE",
    
    # GT Series (older)
    "GT 1030", "GT 730", "GT 720", "GT 710", "GT 705", "GT 640", "GT 630", "GT 620", "GT 610",
    "GT 545", "GT 540", "GT 530", "GT 520", "GT 440", "GT 430", "GT 420",
    
    # 9000 Series
    "GeForce 9800", "GeForce 9600", "GeForce 9500", "GeForce 9400",
    "GeForce 9800 GTX", "GeForce 9800 GX2", "GeForce 9600 GT", "GeForce 9500 GT",
    
    # 8000 Series
    "GeForce 8800", "GeForce 8600", "GeForce 8500", "GeForce 8400",
    "GeForce 8800 GTX", "GeForce 8800 GTS", "GeForce 8600 GTS", "GeForce 8600 GT",
    "GeForce 8500 GT", "GeForce 8400 GS",
}

# NVIDIA GeForce Mobile GPUs
nvidia_geforce_mobile = {
    # RTX 40 Series Mobile
    "RTX 4090M", "RTX 4080M", "RTX 4070M", "RTX 4060M", "RTX 4050M",
    
    # RTX 30 Series Mobile
    "RTX 3080M", "RTX 3070M", "RTX 3060M", "RTX 3050M", "RTX 3050 Ti Mobile",
    "RTX 3080 Ti Mobile", "RTX 3070 Ti Mobile",
    
    # RTX 20 Series Mobile
    "RTX 2080M", "RTX 2070M", "RTX 2060M",
    "RTX 2080 Super Mobile", "RTX 2070 Super Mobile", "RTX 2060 Super Mobile",
    
    # GTX 16 Series Mobile
    "GTX 1660M", "GTX 1650M", "GTX 1650 Ti Mobile",
    "GTX 1660 Ti Mobile",
    
    # GTX 10 Series Mobile
    "GTX 1080M", "GTX 1070M", "GTX 1060M", "GTX 1050M", "GTX 1050 Ti Mobile",
    
    # GTX 900M Series
    "GTX 980M", "GTX 970M", "GTX 960M", "GTX 950M", "GTX 940M", "GTX 930M",
    "GTX 980MX", "GTX 965M",
    
    # GTX 800M Series
    "GTX 880M", "GTX 870M", "GTX 860M", "GTX 850M", "GTX 840M", "GTX 830M", "GTX 820M",
    
    # GTX 700M Series
    "GTX 780M", "GTX 770M", "GTX 765M", "GTX 760M", "GTX 750M", "GTX 745M", "GTX 740M",
    
    # GT Mobile Series
    "GT 755M", "GT 750M", "GT 745M", "GT 740M", "GT 735M", "GT 730M", "GT 720M", "GT 710M",
    "GT 650M", "GT 640M", "GT 630M", "GT 620M", "GT 610M",
    "GT 555M", "GT 550M", "GT 540M", "GT 525M", "GT 520M",
}

# NVIDIA Quadro Professional GPUs
nvidia_quadro = {
    # RTX A Series (newest)
    "RTX A6000", "RTX A5000", "RTX A4000", "RTX A2000",
    "RTX A5500", "RTX A4500",
    
    # Quadro RTX Series
    "Quadro RTX 8000", "Quadro RTX 6000", "Quadro RTX 5000", "Quadro RTX 4000",
    
    # Quadro P Series
    "Quadro P6000", "Quadro P5000", "Quadro P4000", "Quadro P2200", "Quadro P2000",
    "Quadro P1000", "Quadro P620", "Quadro P600", "Quadro P400",
    
    # Quadro M Series
    "Quadro M6000", "Quadro M5000", "Quadro M4000", "Quadro M2000", "Quadro M1200",
    "Quadro M620", "Quadro M600", "Quadro M500",
    
    # Quadro K Series Desktop
    "Quadro K6000", "Quadro K5200", "Quadro K5000", "Quadro K4000", "Quadro K2200",
    "Quadro K2000", "Quadro K1200", "Quadro K620", "Quadro K600", "Quadro K420",
    
    # Mobile Quadro K Series (these often appear without "Quadro" prefix)
    "K5100M", "K4100M", "K3100M", "K2100M", "K1100M", "K610M", "K510M",
    "Quadro K5100M", "Quadro K4100M", "Quadro K3100M", "Quadro K2100M", 
    "Quadro K1100M", "Quadro K610M", "Quadro K510M",
    
    # Quadro FX Series (older)
    "Quadro FX 5800", "Quadro FX 4800", "Quadro FX 3800", "Quadro FX 1800", "Quadro FX 580",
    "Quadro FX 5600", "Quadro FX 4600", "Quadro FX 3500", "Quadro FX 1500", "Quadro FX 540",
    
    # Legacy Quadro
    "Quadro 6000", "Quadro 5000", "Quadro 4000", "Quadro 2000", "Quadro 600", "Quadro 400",
    "Quadro NVS 420", "Quadro NVS 450", "Quadro NVS 290", "Quadro NVS 295",
}

# AMD Radeon Desktop GPUs
amd_radeon_desktop = {
    # RX 7000 Series (RDNA 3)
    "RX 7900 XTX", "RX 7900 XT", "RX 7800 XT", "RX 7700 XT", "RX 7600",
    
    # RX 6000 Series (RDNA 2)
    "RX 6950 XT", "RX 6900 XT", "RX 6800 XT", "RX 6800", "RX 6750 XT", "RX 6700 XT",
    "RX 6650 XT", "RX 6600 XT", "RX 6600", "RX 6500 XT", "RX 6400",
    
    # RX 5000 Series (RDNA)
    "RX 5700 XT", "RX 5700", "RX 5600 XT", "RX 5500 XT", "RX 5500", "RX 5300",
    
    # RX 500 Series (Polaris refresh)
    "RX 590", "RX 580", "RX 570", "RX 560", "RX 550",
    "RX 580X", "RX 570X",
    
    # RX 400 Series (Polaris)
    "RX 480", "RX 470", "RX 460",
    
    # R9 300 Series
    "R9 390X", "R9 390", "R9 380X", "R9 380", "R9 370X", "R9 370",
    
    # R9 200 Series
    "R9 295X2", "R9 290X", "R9 290", "R9 280X", "R9 280", "R9 270X", "R9 270",
    
    # R7 Series
    "R7 370", "R7 360", "R7 350", "R7 340", "R7 265", "R7 260X", "R7 260", "R7 250X", "R7 250", "R7 240",
    
    # R5 Series
    "R5 340X", "R5 340", "R5 335", "R5 330", "R5 310",
    
    # HD 7000 Series
    "HD 7990", "HD 7970", "HD 7950", "HD 7870", "HD 7850", "HD 7790", "HD 7770", "HD 7750",
    "HD 7730", "HD 7670", "HD 7650", "HD 7570", "HD 7550", "HD 7470", "HD 7450",
    
    # HD 6000 Series
    "HD 6990", "HD 6970", "HD 6950", "HD 6870", "HD 6850", "HD 6790", "HD 6770", "HD 6750",
    "HD 6670", "HD 6650", "HD 6570", "HD 6450", "HD 6350", "HD 6290", "HD 6230",
    
    # HD 5000 Series
    "HD 5970", "HD 5870", "HD 5850", "HD 5770", "HD 5750", "HD 5670", "HD 5570", "HD 5550",
    "HD 5450", "HD 5430",
}

# AMD Radeon Mobile GPUs
amd_radeon_mobile = {
    # RX 7000M Series
    "RX 7900M", "RX 7800M", "RX 7700S", "RX 7600M XT", "RX 7600M", "RX 7600S",
    
    # RX 6000M Series
    "RX 6850M XT", "RX 6800M", "RX 6700M", "RX 6650M XT", "RX 6650M", "RX 6600M",
    "RX 6500M", "RX 6300M",
    
    # RX 5000M Series
    "RX 5700M", "RX 5600M", "RX 5500M", "RX 5300M",
    
    # RX 500 Mobile
    "RX 580M", "RX 570M", "RX 560M", "RX 550M", "RX 540M",
    
    # Mobile R9/R7/R5 Series
    "R9 M485X", "R9 M470X", "R9 M390X", "R9 M385X", "R9 M380", "R9 M370X", "R9 M365X",
    "R7 M465", "R7 M460", "R7 M445", "R7 M440", "R7 M365", "R7 M360", "R7 M340",
    "R5 M435", "R5 M430", "R5 M420", "R5 M335", "R5 M330", "R5 M320", "R5 M315",
}

# AMD FirePro Professional GPUs
amd_firepro = {
    "FirePro W9100", "FirePro W8100", "FirePro W7100", "FirePro W5100", "FirePro W4300", "FirePro W4100",
    "FirePro W2100", "FirePro W600", "FirePro W500",
    "FirePro V7900", "FirePro V5900", "FirePro V4900", "FirePro V3900",
    "FirePro S10000", "FirePro S9150", "FirePro S7150",
    "FirePro M6100", "FirePro M5100", "FirePro M4150", "FirePro M4000",
}

# Intel Graphics
intel_graphics = {
    # Arc Series (discrete)
    "Arc A770", "Arc A750", "Arc A580", "Arc A380", "Arc A350",
    
    # Iris Xe (integrated)
    "Iris Xe Graphics", "Iris Xe MAX Graphics",
    
    # Iris Plus (integrated)
    "Iris Plus Graphics", "Iris Plus Graphics 645", "Iris Plus Graphics 655",
    
    # UHD Graphics (integrated)
    "UHD Graphics 770", "UHD Graphics 730", "UHD Graphics 630", "UHD Graphics 620", "UHD Graphics 617",
    "UHD Graphics 605", "UHD Graphics 600",
    
    # HD Graphics (integrated)
    "HD Graphics 6000", "HD Graphics 5500", "HD Graphics 5300", "HD Graphics 4600", "HD Graphics 4400",
    "HD Graphics 4000", "HD Graphics 3000", "HD Graphics 2500", "HD Graphics 2000",
    "HD Graphics 630", "HD Graphics 620", "HD Graphics 530", "HD Graphics 520", "HD Graphics 515",
    "HD Graphics 510", "HD Graphics 500", "HD Graphics 405", "HD Graphics 400",
}

# Comprehensive GPU database
GPU_DATABASE = {
    "nvidia_geforce_desktop": nvidia_geforce_desktop,
    "nvidia_geforce_mobile": nvidia_geforce_mobile,
    "nvidia_quadro": nvidia_quadro,
    "amd_radeon_desktop": amd_radeon_desktop,
    "amd_radeon_mobile": amd_radeon_mobile,
    "amd_firepro": amd_firepro,
    "intel_graphics": intel_graphics,
}

# Create flattened lists for easy lookup
ALL_NVIDIA_GPUS = nvidia_geforce_desktop | nvidia_geforce_mobile | nvidia_quadro
ALL_AMD_GPUS = amd_radeon_desktop | amd_radeon_mobile | amd_firepro
ALL_INTEL_GPUS = intel_graphics
ALL_GPUS = ALL_NVIDIA_GPUS | ALL_AMD_GPUS | ALL_INTEL_GPUS

# Brand mapping for exact matches
GPU_BRAND_MAP = {}
for gpu in ALL_NVIDIA_GPUS:
    GPU_BRAND_MAP[gpu] = "NVIDIA"
for gpu in ALL_AMD_GPUS:
    GPU_BRAND_MAP[gpu] = "AMD"
for gpu in ALL_INTEL_GPUS:
    GPU_BRAND_MAP[gpu] = "Intel"

# Series mapping for specific models
GPU_SERIES_MAP = {
    # NVIDIA GeForce
    **{gpu: "GeForce" for gpu in nvidia_geforce_desktop | nvidia_geforce_mobile if not gpu.startswith(("Quadro", "Tesla", "RTX A"))},
    # NVIDIA Quadro (including mobile K-series)
    **{gpu: "Quadro" for gpu in nvidia_quadro},
    # AMD Radeon
    **{gpu: "Radeon" for gpu in amd_radeon_desktop | amd_radeon_mobile},
    # AMD FirePro
    **{gpu: "FirePro" for gpu in amd_firepro},
    # Intel
    **{gpu: gpu.split()[0] if " " in gpu else "Graphics" for gpu in intel_graphics},
}

def get_gpu_info(model_string):
    """
    Get GPU brand and series for a given model string.
    Returns (brand, series, model) tuple or (None, None, None) if not found.
    """
    # Try exact match first
    if model_string in GPU_BRAND_MAP:
        brand = GPU_BRAND_MAP[model_string]
        series = GPU_SERIES_MAP.get(model_string, "")
        return brand, series, model_string
    
    # Try case-insensitive match
    model_upper = model_string.upper()
    for gpu in ALL_GPUS:
        if gpu.upper() == model_upper:
            brand = GPU_BRAND_MAP[gpu]
            series = GPU_SERIES_MAP.get(gpu, "")
            return brand, series, gpu
    
    return None, None, None

def is_valid_gpu_model(model_string):
    """Check if a string is a valid GPU model."""
    return model_string in ALL_GPUS or model_string.upper() in {gpu.upper() for gpu in ALL_GPUS}

def find_gpu_in_tokens(tokens):
    """
    Find GPU models in a list of tokens using exact matching.
    Returns list of (gpu_model, start_index, end_index) tuples.
    """
    found_gpus = []
    
    # Check single tokens first
    for i, token in enumerate(tokens):
        if is_valid_gpu_model(token):
            found_gpus.append((token, i, i))
    
    # Check combinations of tokens (up to 4 tokens for names like "GeForce 8800 GTX")
    for length in range(2, 5):
        for i in range(len(tokens) - length + 1):
            combined = " ".join(tokens[i:i+length])
            if is_valid_gpu_model(combined):
                found_gpus.append((combined, i, i+length-1))
    
    return found_gpus