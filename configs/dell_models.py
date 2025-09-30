# dell_models.py
# Comprehensive lists of Dell laptop and desktop model numbers for device type detection

# Set of known Dell laptop model numbers
dell_laptop_models = {
    # === LATITUDE LAPTOPS (ALL Latitude models are laptops) ===
    # Current 3000, 5000, 7000, 9000 series (2010-2025)
    "3120", "3189", "3190", "3300", "3301", "3310", "3320", "3330", "3340", "3350", "3379", "3380", "3390",
    "3400", "3410", "3460", "3470", "3480", "3490", "3500", "3510", "3520", "3530", "3540", "3550",
    "3560", "3570", "3580", "3590",
    
    "5175", "5179", "5280", "5285", "5289", "5290", "5300", "5310", "5320", "5330", "5340", "5400", "5401",
    "5410", "5411", "5414", "5420", "5421", "5424", "5430", "5431", "5440", "5450", "5455", "5480", "5490",
    "5500", "5501", "5510", "5511", "5520", "5521", "5530", "5531", "5540", "5541", "5550", "5551", "5580",
    "5590", "5591",
    
    "7200", "7202", "7210", "7212", "7214", "7220", "7230", "7270", "7275", "7280", "7285", "7290", "7300",
    "7310", "7320", "7330", "7350", "7370", "7380", "7390", "7400", "7410", "7412", "7414", "7420", "7424",
    "7430", "7440", "7450", "7455", "7480", "7490", "7520", "7530", "7540",
    
    "9330", "9410", "9420", "9430", "9440", "9470", "9480", "9510", "9520", "9530", "9540", "9550", "9560", "9570",
    
    # Legacy E-series Latitude (2000s-2010s)
    "E4200", "E4300", "E4310", "E5400", "E5410", "E5420", "E5430", "E5440", "E5450", "E5470", "E5480", "E5490",
    "E5500", "E5510", "E5520", "E5530", "E5540", "E5550", "E5570", "E5580", "E5590", "E6400", "E6410", "E6420",
    "E6430", "E6440", "E6500", "E6510", "E6520", "E6530", "E6540", "E7240", "E7250", "E7270", "E7280", "E7290",
    "E7440", "E7450", "E7470", "E7480", "E7490",
    
    # Legacy D, C, X series Latitude (1990s-2000s)
    "D620", "D630", "D800", "D810", "D820", "D830", "C600", "C610", "C640", "C800", "C810", "C840",
    "X200", "X300", "XT", "XT2",
    
    # === PRECISION MOBILE WORKSTATIONS (Laptops) ===
    # Current Mobile Precision (2010-2025)
    "3510", "3520", "3530", "3540", "3541", "3550", "3551", "3560", "3561", "3570", "3571", "3580", "3581", "3590", "3591",
    "5510", "5520", "5530", "5540", "5550", "5560", "5570", "5580", "5590", "5591", "5750", "5760", "5770",
    "7510", "7520", "7530", "7540", "7550", "7560", "7570", "7580", "7590", "7710", "7720", "7730", "7740", "7750", "7760", "7770", "7780", "7790",
    
    # Legacy M-series Mobile Precision (2000s-2010s)
    "M2400", "M4400", "M4500", "M4600", "M4700", "M4800", "M6500", "M6600", "M6700", "M6800",
    
    # === INSPIRON LAPTOPS ===
    # Current Inspiron Laptops (2010-2025)
    "3521", "3537", "3542", "3558", "3567", "3576", "3580", "3593", "3501", "3502", "3505", "3506", "3507",
    "3510", "3511", "3515", "3520", "3525", "3530", "3535", "3541", "3551", "3552", "3565", "3583", "3584",
    "3585", "3590", "3591", "3595",
    
    "5565", "5566", "5567", "5568", "5570", "5575", "5580", "5590", "5593", "5401", "5402", "5405", "5406",
    "5407", "5408", "5409", "5415", "5418", "5425", "5435", "5445", "5455", "5480", "5481", "5482", "5485",
    "5488", "5491", "5493", "5494", "5498", "5501", "5502", "5510", "5515", "5547", "5551", "5555", "5558",
    "5559", "5584", "5585", "5593", "5594", "5598", "5755", "5758", "5759", "5765", "5767", "5770", "5775",
    
    "7447", "7548", "7559", "7560", "7566", "7567", "7570", "7572", "7573", "7577", "7579", "7580", "7586",
    "7590", "7591", "7405", "7415", "7425", "7435", "7460", "7472", "7490", "7500", "7501", "7506", "7510",
    "7520", "7530", "7537", "7547", "7569", "7700", "7706", "7710", "7720", "7730", "7737", "7746", "7773", "7778", "7779",
    
    # Legacy Inspiron Laptops (1990s-2000s)
    "1100", "1150", "1200", "1300", "1318", "1420", "1425", "1427", "1428", "1440", "1450", "1464", "1470",
    "1501", "1502", "1503", "1505", "1520", "1521", "1525", "1526", "1535", "1536", "1545", "1546", "1564",
    "1570", "1700", "1705", "1720", "1721", "1747", "1749", "1764", "5100", "5150", "5160", "6000", "6400",
    "8500", "8600", "9200", "9300", "9400", "B120", "B130",
    
    # === XPS LAPTOPS ===
    # Current XPS Laptops (2010-2025)
    "9300", "9305", "9310", "9315", "9320", "9333", "9340", "9343", "9350", "9360", "9365", "9370", "9380",
    "9500", "9510", "9520", "9530", "9550", "9560", "9570", "9575", "9700", "9710", "9720", "9730",
    
    # Legacy XPS Laptops
    "L321X", "L322X", "1340", "1640", "1645", "1647", "1730", "M1210", "M1330", "M1530", "M1710", "M1730",
    
    # === VOSTRO LAPTOPS ===
    # Current Vostro Laptops (2010-2025)
    "3300", "3301", "3350", "3360", "3400", "3401", "3405", "3445", "3446", "3450", "3460", "3468", "3470",
    "3478", "3480", "3481", "3490", "3491", "3500", "3501", "3510", "3515", "3520", "3525", "3530", "3535",
    "3540", "3545", "3546", "3549", "3550", "3555", "3560", "3565", "3568", "3570", "3572", "3578", "3580",
    "3581", "3582", "3583", "3584", "3590", "3591",
    
    "5300", "5301", "5310", "5320", "5330", "5370", "5390", "5391", "5400", "5401", "5402", "5405", "5406",
    "5408", "5410", "5415", "5418", "5420", "5425", "5430", "5435", "5459", "5460", "5468", "5470", "5471",
    "5480", "5481", "5488", "5490", "5491", "5500", "5501", "5502", "5510", "5515", "5520", "5525", "5530",
    "5568", "5581", "5590", "5591",
    
    # Legacy Vostro Laptops
    "1014", "1015", "1088", "1220", "1310", "1320", "1510", "1520", "1710", "1720",
    
    # === ALIENWARE LAPTOPS ===
    "13", "14", "15", "17", "18", "m11x", "m13x", "m14x", "m15x", "m17x", "m18x", "x14", "x15", "x16", "x17",
    "Area-51m",
    
    # === G-SERIES GAMING LAPTOPS ===
    "G3", "G5", "G7", "G15", "G16", "G17", "3579", "3590", "5587", "5590", "5500", "5505", "5510", "5515",
    "5520", "5525", "7588", "7590", "7700",
    
    # === LEGACY LAPTOP MODELS ===
    "600m", "610m", "630m", "640m", "700m", "710m", "8100", "8200", "9100", "9150",
}

# Set of known Dell desktop model numbers
dell_desktop_models = {
    # === OPTIPLEX DESKTOPS (ALL OptiPlex models are desktops) ===
    # Current OptiPlex (2010-2025)
    "3000", "3010", "3011", "3020", "3030", "3040", "3046", "3050", "3060", "3070", "3080", "3090", "3100",
    "3240", "3250", "3280",
    
    "5000", "5010", "5020", "5030", "5040", "5050", "5055", "5060", "5070", "5080", "5090", "5100", "5250",
    "5260", "5270", "5280", "5290",
    
    "7000", "7010", "7020", "7030", "7040", "7050", "7060", "7070", "7071", "7080", "7090", "7100", "7400",
    "7410", "7440", "7450", "7460", "7470", "7480", "7490", "7500",
    
    "9010", "9020", "9030", "9320",
    
    # Legacy OptiPlex (1990s-2000s)
    "160", "170", "170L", "210L", "260", "270", "280", "320", "330", "360", "380", "390", "520", "620",
    "740", "745", "755", "760", "780", "790", "960", "980", "990", "GX50", "GX60", "GX110", "GX150",
    "GX240", "GX260", "GX270", "GX280", "GX520", "GX620", "SX260", "SX270", "SX280",
    
    # === PRECISION DESKTOP WORKSTATIONS ===
    # Current Desktop Precision with T-prefix (2010-2025)
    "T1500", "T1600", "T1650", "T1700", "T3400", "T3500", "T3600", "T3610", "T3620", "T3630", "T3640",
    "T3650", "T3660", "T5400", "T5500", "T5600", "T5610", "T5810", "T5820", "T7400", "T7500", "T7600",
    "T7610", "T7810", "T7820", "T7910", "T7920",
    
    # Precision newer numbered desktop models (Desktop/Tower/SFF/Rack)
    "3420", "3430", "3431", "3440", "3460", "3630", "3631", "3640", "3650", "3660", "3680", "5810", "5820", 
    "5860", "7820", "7875", "7920", "7960", "3450",
    
    # === INSPIRON DESKTOPS ===
    # Current Inspiron Desktops (2010-2025)
    "3000", "3010", "3020", "3030", "3040", "3050", "3060", "3070", "3080", "3090", "3250", "3268", "3470",
    "3471", "3472", "3647", "3650", "3655", "3656", "3660", "3667", "3668", "3670", "3671", "3680", "3681",
    "3847", "3880", "3881", "3888", "3891", "3910",
    
    "5300", "5310", "5400", "5410", "5675", "5676", "5680", "5700", "5720", "5775",
    
    "7700", "7777", "7790",
    
    # Legacy Inspiron Desktops (1990s-2000s)
    "200", "300", "301", "400", "410", "510", "515", "518", "519", "530", "531", "533", "534", "535",
    "536", "537", "540", "545", "546", "548", "549", "560", "570", "580", "620", "630", "660",
    
    # === XPS DESKTOPS ===
    # Current XPS Desktops
    "8000", "8100", "8200", "8300", "8400", "8500", "8700", "8900", "8910", "8920", "8930", "8940", "8950",
    
    # Legacy XPS Desktops
    "200", "300", "400", "410", "420", "430", "435", "600", "625", "630", "700", "710", "720", "730",
    
    # === ALIENWARE DESKTOPS ===
    "Aurora", "Aurora R1", "Aurora R2", "Aurora R3", "Aurora R4", "Aurora R5", "Aurora R6", "Aurora R7",
    "Aurora R8", "Aurora R9", "Aurora R10", "Aurora R11", "Aurora R12", "Aurora R13", "Aurora R14", "Aurora R15",
    "Area-51", "X51",
    
    # === VOSTRO DESKTOPS ===
    # Current Vostro Desktops
    "200", "220", "230", "260", "270", "400", "410", "420", "430", "3250", "3267", "3268", "3470", "3471",
    "3650", "3660", "3667", "3668", "3670", "3671", "3681", "3888", "3910", "5450", "5460", "5470", "5890",
    
    # === LEGACY DESKTOP MODELS ===
    "4100", "4400", "4500", "4550", "4600", "8100", "8200",
    
    # Dimension Series (Legacy)
    "1100", "2350", "2400", "3000", "4600", "4700", "8200", "8250", "8300", "9100", "9150", "9200",
}

# Additional sets for specific model prefixes that are always one type
dell_always_laptop_prefixes = {"E", "L", "M"}  # E-series Latitude, L-series XPS, M-series Precision
dell_always_desktop_prefixes = {"T", "GX", "SX"}  # T-series Precision, GX/SX OptiPlex

import os
from pathlib import Path
BASE_DIR = os.path.dirname(__file__)
CONFIG_DIR = os.path.join(BASE_DIR, 'configs')

# Set of Dell models that are known 2-in-1 convertibles/detachables (treat as tablets)
# Note: These entries are chosen to avoid ambiguous clamshell models.
# Examples include Latitude detachables and XPS 2-in-1 only SKUs.
dell_2in1_models = {
    # Latitude 2-in-1/detachables
    "5175", "5179", "5285", "5289", "5290", "7275", "7285", "7350",
    # XPS 2-in-1 only
    "9365", "9575",
}

def is_dell_laptop_model(model_string):
    """Check if a Dell model string indicates a laptop."""
    if not model_string:
        return False
    
    model_lower = model_string.lower()
    
    # Check for laptop-specific series names
    laptop_series = ["latitude", "precision mobile", "mobile workstation"]
    if any(series in model_lower for series in laptop_series):
        return True
    
    # Check prefixes that are always laptops
    for prefix in dell_always_laptop_prefixes:
        if model_lower.startswith(prefix.lower()):
            return True
    
    # Extract numeric model numbers and check against laptop set
    import re
    # Sanitize out CPU model patterns (e.g., i5-6400, i7 8700k, Xeon E5-2670) so they don't bias detection
    model_clean = re.sub(r'\bi[3579]\s*[-]?\s*\d{3,5}[a-z]*\b', ' ', model_lower)
    model_clean = re.sub(r'\bxeon\s*\w*\s*\d{3,5}[a-z]*\b', ' ', model_clean)
    model_numbers = re.findall(r'\b\d{4}\b', model_clean)
    for model_num in model_numbers:
        if model_num in dell_laptop_models:
            return True
    
    return False

def is_dell_desktop_model(model_string):
    """Check if a Dell model string indicates a desktop."""
    if not model_string:
        return False
    
    model_lower = model_string.lower()
    
    # Check for desktop-specific series names
    desktop_series = ["optiplex", "precision tower", "precision desktop", "tower", "desktop", "workstation"]
    if any(series in model_lower for series in desktop_series):
        return True
    
    # Check prefixes that are always desktops
    for prefix in dell_always_desktop_prefixes:
        if model_lower.startswith(prefix.lower()):
            return True
    
    # Extract numeric model numbers and check against desktop set
    import re
    # Sanitize out CPU model patterns (e.g., i5-6400, i7 8700k, Xeon E5-2670) so they don't bias detection
    model_clean = re.sub(r'\bi[3579]\s*[-]?\s*\d{3,5}[a-z]*\b', ' ', model_lower)
    model_clean = re.sub(r'\bxeon\s*\w*\s*\d{3,5}[a-z]*\b', ' ', model_clean)
    model_numbers = re.findall(r'\b\d{4}\b', model_clean)
    for model_num in model_numbers:
        if model_num in dell_desktop_models:
            return True
    
    # Check for T-prefix models (Precision towers)
    t_model_match = re.search(r'\bt(\d{4})\b', model_lower)
    if t_model_match:
        t_model = f"T{t_model_match.group(1)}"
        if t_model in dell_desktop_models:
            return True
    
    return False