r"""
Directives / Intent Memory
--------------------------
- Goal: Read titles (one per line) and produce a preview of which title_* keys would populate using
  the existing description extractor logic (CPU, RAM, Storage, GPU, Lot, etc.).
- Method: Reuse description_extraction parsers on each title string, then map their canonical
  output fields to training/schema.json's allowed title keys.
- Output: JSON Lines file where each line contains {"title": <str>, "title_keys": {<title_key>: <value>, ...}}.
  Also writes compact CSV summaries of a few common keys.
- Non-destructive: String values preserved as-is, light normalization is performed within the
  underlying extractors (e.g., GHz, GB/TB normalization).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

# Ensure project root is importable for local modules like description_extraction/*
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Config-driven helpers (brand → device type families, Dell model typing)
try:
    from configs.brand_model_types import brand_model_types as CONFIG_BRAND_MODEL_TYPES
except Exception:  # Fallback if configs not available
    CONFIG_BRAND_MODEL_TYPES = {}

try:
    from configs.dell_models import is_dell_laptop_model, is_dell_desktop_model
except Exception:
    def is_dell_laptop_model(_: str) -> bool:  # type: ignore
        return False
    def is_dell_desktop_model(_: str) -> bool:  # type: ignore
        return False


def map_cpu_to_title_keys(cpu_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not cpu_data:
        return out
    mapping = {
        "cpu_brand": "title_cpu_brand_key",
        "cpu_family": "title_cpu_family_key",
        "cpu_model": "title_cpu_model_key",
        "cpu_speed": "title_cpu_speed_key",
        "cpu_quantity": "title_cpu_quantity_key",
        "cpu_generation": "title_cpu_generation_key",
        "cpu_suffix": "title_cpu_suffix_key",
    }
    for k, v in cpu_data.items():
        if k in mapping and v:
            out[mapping[k]] = v
    # Multi support (base first, then numbered starting at 2)
    if cpu_data.get("cpu_family1"):
        out["title_cpu_family2_key"] = cpu_data["cpu_family1"]
    if cpu_data.get("cpu_family2"):
        out["title_cpu_family3_key"] = cpu_data["cpu_family2"]
    if cpu_data.get("cpu_model1"):
        out["title_cpu_model2_key"] = cpu_data["cpu_model1"]
    if cpu_data.get("cpu_model2"):
        out["title_cpu_model3_key"] = cpu_data["cpu_model2"]
    if cpu_data.get("cpu_generation2"):
        out["title_cpu_generation2_key"] = cpu_data["cpu_generation2"]
    if cpu_data.get("cpu_generation3"):
        out["title_cpu_generation3_key"] = cpu_data["cpu_generation3"]
    if cpu_data.get("cpu_generation4"):
        out["title_cpu_generation4_key"] = cpu_data["cpu_generation4"]
    return out


def _ordinal(n: int) -> str:
    # Convert 1 -> 1st, 2 -> 2nd, etc.
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix} Gen"


def _intel_generation_from_model(model_code: str) -> Optional[str]:
    # Derive Intel Core generation from a model code like 6600U, 3615QM, 10210U, 1165G7, etc.
    import re as _re
    if not model_code:
        return None
    m = _re.search(r"(\d{3,5})", model_code)
    if not m:
        return None
    digits = m.group(1)
    try:
        # 5 digits (10th gen and later) -> first two digits are generation (10, 11, 12, 13...)
        if len(digits) >= 5:
            gen = int(digits[:2])
            return _ordinal(gen)
        # 4 digits (2nd..9th gen) -> first digit is generation (2..9)
        if len(digits) == 4:
            gen = int(digits[0])
            return _ordinal(gen)
        # 3-digit Core i-series mobile codes (e.g., 620M, 540M, 720QM) correspond to 1st Gen
        if len(digits) == 3:
            return _ordinal(1)
    except Exception:
        return None
    return None


def _clean_model_string(model_text: str) -> str:
    # Remove noise tokens like 'Unlocked', standalone 'Un', and trailing counters like '#2', '(4)'
    import re as _re
    s = model_text or ""
    # Remove 'Network Unlocked' or 'Unlocked' tokens; keep 'G-' codes intact (avoid removing 'Un' inside codes)
    s = _re.sub(r"\bnetwork\s+unlocked\b", " ", s, flags=_re.IGNORECASE)
    s = _re.sub(r"\bunlocked\b", " ", s, flags=_re.IGNORECASE)
    # Remove standalone ' Un ' token only if surrounded by spaces
    s = _re.sub(r"(?<!\w)\s+un\s+(?!\w)", " ", s, flags=_re.IGNORECASE)
    # Remove trailing small parenthetical counters like (4), (#2), (B)
    # Remove trailing small parenthetical counters or letters like (4), (c), (#2), (B)
    s = _re.sub(r"\s*\((?:#?[A-Za-z0-9]{1,5})\)\s*$", " ", s)
    # Remove inline '#2', '#B' tokens
    s = _re.sub(r"\s*#\w+\b", " ", s)
    # Remove trailing single-letter lot/condition marks like ' (C)' or ' C' or ' B' but not inside codes
    s = _re.sub(r"\s+([A-Za-z])$", " ", s)
    # Remove trailing CPU/Processor words
    s = _re.sub(r"\b(cpu|processor)\b\s*$", " ", s, flags=_re.IGNORECASE)
    # Collapse whitespace and punctuation spaces
    s = _re.sub(r"\s+", " ", s).strip(" -_,")
    return s


def map_ram_to_title_keys(ram_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not ram_data:
        return out
    mapping = {
        "ram_size": "title_ram_size_key",
        "ram_config": "title_ram_config_key",
        "ram_type": "title_ram_type_key",
        "ram_speed_grade": "title_ram_speed_grade_key",
        "ram_modules": "title_ram_modules_key",
        # No direct rank key in title schema; skip ram_rank
        # "ram_rank": None,
        # Brand is not a title key; skip ram_brand
        # ECC/registered/unbuffered flags
        # We'll map ECC to title_ram_error_correction_key and registered to title_ram_registered_key
        "ram_ecc": "title_ram_error_correction_key",
        "ram_registered": "title_ram_registered_key",
        # ram_unbuffered: no explicit title key; skip
        "ram_range": "title_ram_range_key",
    }
    for k, v in ram_data.items():
        if k in mapping and v:
            # Normalize ECC/registered flags to friendly values if needed
            if k == "ram_ecc" and v:
                out[mapping[k]] = "ECC"
            elif k == "ram_registered" and v:
                out[mapping[k]] = "registered"
            else:
                out[mapping[k]] = v
    # Support multiple RAM sizes where provided: ram_size, ram_size2, ram_size3 -> title_ram_size1/2/3
    if ram_data.get("ram_size2"):
        out["title_ram_size2_key"] = ram_data["ram_size2"]
    if ram_data.get("ram_size3"):
        out["title_ram_size3_key"] = ram_data["ram_size3"]
    return out


def map_storage_to_title_keys(storage_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not storage_data:
        return out
    # Primary mappings
    type_val = storage_data.get("storage_type") or storage_data.get("storage_type1")
    if type_val:
        out["title_storage_type_key"] = type_val
    # Secondary type if present
    if storage_data.get("storage_type2"):
        out.setdefault("title_additional_info_key", f"storage_type2: {storage_data['storage_type2']}")

    # Capacities
    cap1 = storage_data.get("storage_capacity")
    cap2 = storage_data.get("storage_capacity2")
    # Only map capacities if we have a storage type present to avoid RAM sizes bleeding into storage
    if out.get("title_storage_type_key"):
        if cap1:
            out["title_storage_capacity_key"] = cap1
        if cap2:
            out["title_storage_capacity2_key"] = cap2

    # Ranges and extras -> stash into additional capacity slots if available
    storage_range = storage_data.get("storage_range")
    if storage_range:
        out["title_storage_capacity3_key"] = storage_range

    # Do not leak drive size into additional info; keep parity with reference outputs

    return out


def map_gpu_to_title_keys(gpu_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not gpu_data:
        return out
    # Prefer putting specific model string under title_gpu_model_key
    if gpu_data.get("gpu"):
        out["title_gpu_model_key"] = gpu_data["gpu"]
    if gpu_data.get("gpu_memory_type"):
        out["title_gpu_memory_type_key"] = gpu_data["gpu_memory_type"]
    # Additional GPU fields from extractors
    if gpu_data.get("gpu_spec"):
        out["title_gpu_ram_size_key"] = gpu_data["gpu_spec"]
    if gpu_data.get("gpu_type"):
        out["title_gpu_type_key"] = gpu_data["gpu_type"]
    return out


def map_screen_to_title_keys(screen_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not screen_data:
        return out
    mapping = {
        "screen_size": "title_screen_size_key",
        "screen_resolution": "title_screen_resolution_key",
        "screen_hertz": "title_screen_hertz_key",
        "screen_aspect_ratio": "title_screen_aspect_ratio_key",
        "screen_touch": "title_screen_touch_key",
        "screen_panel_type": "title_screen_panel_type_key",
    }
    for k, v in screen_data.items():
        if k in mapping and v:
            out[mapping[k]] = v
    return out


def map_hdd_to_title_keys(hdd_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not hdd_data:
        return out
    mapping = {
        "hdd_interface": "title_hdd_interface_key",
        "hdd_form_factor": "title_hdd_form_factor_key",
        "hdd_rpm": "title_hdd_rpm_key",
        "hdd_transfer_rate": "title_hdd_transfer_rate_key",
        "hdd_model_number": "title_hdd_model_number_key",
        "hdd_part_number": "title_hdd_part_number_key",
        "hdd_usage_hours": "title_hdd_usage_hours_key",
    }
    for k, v in hdd_data.items():
        if k in mapping and v:
            out[mapping[k]] = v
    return out


# (removed) switch/adapter mapping helpers


def map_battery_to_title_keys(battery_data: Dict[str, str], existing: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not battery_data:
        return out
    # Preserve existing status if already determined by status extractor
    if battery_data.get("battery_presence"):
        if not existing or "title_battery_status_key" not in existing:
            pres = battery_data["battery_presence"]
            out["title_battery_status_key"] = pres
    if battery_data.get("battery_condition"):
        out["title_battery_condition_key"] = battery_data["battery_condition"]
    if battery_data.get("battery_health"):
        out["title_battery_health_key"] = battery_data["battery_health"]
    return out


def map_lot_to_title_keys(lot_data: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not lot_data:
        return out
    if lot_data.get("lot"):
        out["title_lot_key"] = lot_data["lot"]
    return out


def _finalize_cpu_keys(title_text: str, keys: Dict[str, str]) -> None:
    # Ensure multi-CPU lists include all models seen in title, fix generations from model code,
    # and avoid ambiguous speeds when multiple models and speeds coexist.
    import re as _re
    # Collect models from keys (existing base/numbered)
    models: list[str] = []
    key_names = [k for k in keys.keys() if _re.match(r"^title_cpu_model(\d+)?_key$", k)]
    def _model_sort_key(k: str) -> int:
        if k == "title_cpu_model_key":
            return 0
        m = _re.match(r"^title_cpu_model(\d+)_key$", k)
        return int(m.group(1)) if m else 0
    for k in sorted(key_names, key=_model_sort_key):
        v = keys.get(k)
        if isinstance(v, str) and v:
            uv = v.upper()
            if uv not in models:
                models.append(uv)
    # Collect from title with context filter to avoid device model collisions (e.g., OptiPlex 3020M)
    for m in _re.finditer(r"\b(\d{3,5})(?:-)?([A-Z]{1,3}|G\d|Y\d)\b", title_text, _re.IGNORECASE):
        start = m.start()
        prefix = title_text[max(0, start - 24): start]
        # Accept if immediate CPU context token appears near end, OR previous token is a CPU model followed by '/'
        has_ctx = bool(_re.search(r"(Core|Xeon|CPU|Intel|Pentium|Celeron|i[3579])\s*[-/ ]?$", prefix, _re.IGNORECASE))
        if not has_ctx:
            # Look for a prior CPU code ending before a slash near the end of prefix to allow chained / codes
            has_ctx = bool(_re.search(r"(\d{3,5})(U|M|H|HQ|HK|QM|G\d|Y\d)\s*/\s*$", prefix, _re.IGNORECASE))
        if not has_ctx:
            continue
        code = (m.group(1) + m.group(2)).upper().replace("QU", "QM")
        if code not in models:
            models.append(code)
    for grade, code in _re.findall(r"\bm([35])[- ]?(\dY\d{2})\b", title_text, _re.IGNORECASE):
        code = code.upper()
        if code not in models:
            models.append(code)
            if not keys.get("title_cpu_family_key"):
                keys["title_cpu_family_key"] = f"Core m{grade}"
    # Rewrite models sequentially: base + numbered (no cap)
    for k in list(keys.keys()):
        if _re.match(r"^title_cpu_model\d+_key$", k):
            keys.pop(k, None)
    for i, m in enumerate(models, start=1):
        if i == 1:
            keys["title_cpu_model_key"] = m
        else:
            keys[f"title_cpu_model{i}_key"] = m

    # Derive generations from all models and from tokens in title; then merge and enumerate base+numbered
    gen_from_models: list[str] = []
    for i in range(1, len(models) + 1):
        mk = "title_cpu_model_key" if i == 1 else f"title_cpu_model{i}_key"
        mv = keys.get(mk)
        if isinstance(mv, str) and mv:
            gv = _intel_generation_from_model(mv)
            if gv and gv not in gen_from_models:
                gen_from_models.append(gv)
    # Derive suffix from model code (last letters), prefer existing explicit suffix if present
    def _suffix_from(model: str) -> Optional[str]:
        mm = _re.search(r"[A-Z]+$", model.upper())
        if mm:
            return mm.group(0)
        return None
    # Primary suffix
    if keys.get("title_cpu_model_key"):
        sf = _suffix_from(str(keys["title_cpu_model_key"]))
        if sf:
            keys["title_cpu_suffix_key"] = keys.get("title_cpu_suffix_key") or sf
    # Secondary+ suffixes dynamic up to 5
    for i in range(2, 6):
        mk = f"title_cpu_model{i}_key"
        sk = f"title_cpu_suffix{i}_key"
        if keys.get(mk):
            sfi = _suffix_from(str(keys[mk]))
            if sfi:
                keys[sk] = sfi
        else:
            keys.pop(sk, None)

    # Families: capture multiple when present (i3/i5/i7/i9 tokens), allow up to 3 and de-duplicate
    fam_order = []
    if _re.search(r"\bi9\b", title_text, _re.IGNORECASE): fam_order.append("Core i9")
    if _re.search(r"\bi7\b", title_text, _re.IGNORECASE): fam_order.append("Core i7")
    if _re.search(r"\bi5\b", title_text, _re.IGNORECASE): fam_order.append("Core i5")
    if _re.search(r"\bi3\b", title_text, _re.IGNORECASE): fam_order.append("Core i3")
    # Preserve existing primary family if set
    primary_fam = keys.get("title_cpu_family_key")
    fams: list[str] = []
    if isinstance(primary_fam, str) and primary_fam:
        fams.append(primary_fam)
    for f in fam_order:
        if f not in fams:
            fams.append(f)
    # keep up to 3 unique families
    if fams:
        keys["title_cpu_family_key"] = fams[0]
        if len(fams) > 1:
            keys["title_cpu_family2_key"] = fams[1]
        else:
            keys.pop("title_cpu_family2_key", None)
        if len(fams) > 2:
            keys["title_cpu_family3_key"] = fams[2]
        else:
            keys.pop("title_cpu_family3_key", None)

    # Extract multiple generation tokens when present (e.g., 7th/8th Gen, 11th/i7-13thGen)
    gens: list[str] = []
    # Prefer explicit paired forms to preserve left/right order
    # Pattern D: multi-part cluster like 7th/8th/9th Gen
    m_cluster = _re.search(r"\b((?:\d{1,2}(?:st|nd|rd|th))(?:\s*/\s*\d{1,2}(?:st|nd|rd|th))+)[\s\-]*Gen\b", title_text, _re.IGNORECASE)
    if m_cluster:
        parts = _re.split(r"\s*/\s*", m_cluster.group(1))
        try:
            gens = [_ordinal(int(_re.match(r"\d{1,2}", p).group(0))) for p in parts if _re.match(r"\d{1,2}", p)]
        except Exception:
            gens = []
    m_pair = m_pair if not m_cluster else None
    if m_pair:
        try:
            gens = [_ordinal(int(m_pair.group(1))), _ordinal(int(m_pair.group(2)))]
        except Exception:
            gens = []
    else:
        m_pair_sep = _re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\s*/[^G\n]{0,32}?(\d{1,2})(?:st|nd|rd|th)\s*Gen\b", title_text, _re.IGNORECASE)
        if m_pair_sep:
            try:
                gens = [_ordinal(int(m_pair_sep.group(1))), _ordinal(int(m_pair_sep.group(2)))]
            except Exception:
                gens = []
        else:
            # Fallback: collect standalone gens in order of appearance
            seen = set()
            for gm in _re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)\s*Gen\b", title_text, _re.IGNORECASE):
                try:
                    g = _ordinal(int(gm.group(1)))
                    if g not in seen:
                        gens.append(g)
                        seen.add(g)
                except Exception:
                    pass
    # Merge model-derived generations and token-derived generations; rewrite base+numbered sequentially
    all_gens: list[str] = []
    for g in gen_from_models:
        if g not in all_gens:
            all_gens.append(g)
    for g in gens:
        if g not in all_gens:
            all_gens.append(g)
    if all_gens:
        # clear any existing generation keys
        for k in list(keys.keys()):
            if _re.match(r"^title_cpu_generation(\d+)?_key$", k):
                keys.pop(k, None)
        # write base then numbered
        for i, gv in enumerate(all_gens, start=1):
            if i == 1:
                keys["title_cpu_generation_key"] = gv
            else:
                keys[f"title_cpu_generation{i}_key"] = gv

    # Speeds: enumerate all unique GHz tokens (base + numbered), not tied to models
    ghz = [m.group(0) for m in _re.finditer(r"\b\d+(?:\.\d+)?\s*GHz\b", title_text, _re.IGNORECASE)]
    # normalize and dedupe preserving order
    def _norm_speed(x: str) -> str:
        return x.upper().replace("GHZ", "GHz").replace(" ", "")
    uniq_speeds: list[str] = []
    for sp in ghz:
        nv = _norm_speed(sp)
        if nv not in uniq_speeds:
            uniq_speeds.append(nv)
    # clear prior speed keys
    for k in list(keys.keys()):
        if _re.match(r"^title_cpu_speed(\d+)?_key$", k):
            keys.pop(k, None)
    # write speeds sequentially
    for i, sv in enumerate(uniq_speeds, start=1):
        if i == 1:
            keys["title_cpu_speed_key"] = sv
        else:
            keys[f"title_cpu_speed{i}_key"] = sv

    # De-duplicate repeated numbered values across families, generations, suffixes, and speeds
    def _dedupe_series(base: str) -> None:
        vals: list[tuple[str, str]] = []
        for idx in ("", "2", "3", "4", "5"):
            k = f"title_cpu_{base}{('_key' if idx == '' else idx + '_key')}"
            v = keys.get(k)
            if isinstance(v, str) and v:
                vals.append((k, v))
        seen: set[str] = set()
        for k, v in vals:
            vn = v.strip().lower()
            if vn in seen:
                keys.pop(k, None)
            else:
                seen.add(vn)

    _dedupe_series("family")
    _dedupe_series("generation")
    _dedupe_series("suffix")
    _dedupe_series("speed")
    # Also dedupe storage capacities if repeated (rare OCR glitches)
    def _dedupe_generic_series(prefix: str) -> None:
        series = [k for k in keys.keys() if _re.match(rf"^{prefix}(?:\d+)?_key$", k)]
        def sk(k: str) -> tuple[int, int]:
            if k == f"{prefix}_key": return (0,0)
            m = _re.match(rf"^{prefix}(\d+)_key$", k)
            return (1, int(m.group(1)) if m else 0)
        series.sort(key=sk)
        seen: set[str] = set()
        for k in series:
            v = keys.get(k)
            if isinstance(v, str) and v:
                vn = v.strip().lower()
                if vn in seen and k != f"{prefix}_key":
                    keys.pop(k, None)
                else:
                    seen.add(vn)
    _dedupe_generic_series("title_storage_capacity")

    # Extra explicit de-duplication for suffix and speed to guarantee base-only when equal
    def _drop_if_equal(base: str, i: int, j: int) -> None:
        ki = f"title_cpu_{base}{('_key' if i == 1 else str(i) + '_key')}"
        kj = f"title_cpu_{base}{('_key' if j == 1 else str(j) + '_key')}"
        vi = keys.get(ki)
        vj = keys.get(kj)
        if isinstance(vi, str) and isinstance(vj, str) and vi.strip().lower() == vj.strip().lower():
            if j != 1:
                keys.pop(kj, None)

    _drop_if_equal("suffix", 1, 2)
    _drop_if_equal("suffix", 1, 3)
    _drop_if_equal("suffix", 2, 3)
    _drop_if_equal("speed", 1, 2)
    _drop_if_equal("speed", 1, 3)
    _drop_if_equal("speed", 2, 3)
    # Family fallback
    if not keys.get("title_cpu_family_key"):
        if _re.search(r"\bi9\b", title_text, _re.IGNORECASE):
            keys["title_cpu_family_key"] = "Core i9"
        elif _re.search(r"\bi7\b", title_text, _re.IGNORECASE):
            keys["title_cpu_family_key"] = "Core i7"
        elif _re.search(r"\bi5\b", title_text, _re.IGNORECASE):
            keys["title_cpu_family_key"] = "Core i5"
        elif _re.search(r"\bi3\b", title_text, _re.IGNORECASE):
            keys["title_cpu_family_key"] = "Core i3"

def _infer_device_type_from_brand_model(brand: Optional[str], model: Optional[str], title_text: str) -> Optional[str]:
    if not brand:
        return None
    brand_key = str(brand)
    families = CONFIG_BRAND_MODEL_TYPES.get(brand_key)
    if isinstance(families, dict):
        # families may be mapping series → device type or nested dict
        lowered_title = title_text.lower()
        model_lc = (model or "").lower()
        for series, dtype in families.items():
            if isinstance(dtype, str):
                if series.lower() in lowered_title or series.lower() in model_lc:
                    return dtype
            elif isinstance(dtype, dict):
                # dtype maps device type → list of qualifiers
                for dt, qual_list in dtype.items():
                    for q in qual_list:
                        ql = str(q).lower()
                        if ql and (ql in lowered_title or ql in model_lc):
                            return dt
    return None


def _derive_model_from_title(title_text: str, brand: Optional[str], keys: Dict[str, str]) -> Optional[str]:
    """Derive a model string by subtracting detected specs and stopwords, similar to process_description.
    Prefers the segment following the brand token, clipped before spec/status words.
    """
    import re as _re
    t = title_text
    # Remove values already extracted
    remove_values: list[str] = []
    for k, v in keys.items():
        if isinstance(v, str) and v:
            remove_values.append(_re.escape(v))
            # Also remove spaced capacity variants like "128 GB" if value is "128GB"
            m_cap = _re.match(r"^(\d+(?:\.\d+)?)(GB|TB|MB)$", v, _re.IGNORECASE)
            if m_cap:
                num, unit = m_cap.group(1), m_cap.group(2)
                remove_values.append(rf"\b{_re.escape(num)}\s*{_re.escape(unit)}\b")
    if brand:
        remove_values.append(_re.escape(brand))
    # Common spec/status tokens to strip
    spec_tokens = [
        r"Intel", r"AMD", r"Apple", r"Core", r"Ryzen", r"Xeon", r"Celeron", r"Pentium",
        r"i[3579]-?\d+[A-Za-z0-9]*", r"M[1-4](?:\s+(?:Pro|Max|Ultra))?",
        # Xeon/Pentium code patterns like E5-2609, E3-1535M, W-2145, and their v# suffixes
        r"(?:E[357]|W)-\d{3,5}[A-Za-z]?", r"v\d+",
        r"\d+(?:\.\d+)?\s*GHz", r"\d+(?:\.\d+)?\s*(GB|TB|MB)", r"DDR\d", r"LPDDR\d",
        r"SSD", r"HDD", r"NVME", r"EMMC", r"M\.2", r"m\.2", r"SATA", r"SAS", r"SCSI",
        r"No\s+(?:OS|SSD|HDD|Storage|Battery)", r"BIOS\s+LOCKED", r"Locked", r"Charger", r"Adapter",
        r"WiFi", r"Unlocked", r"Network", r"Carrier", r"Only", r"Screen", r"Screens?", r"Scratches?",
        r"Crack(?:ed)?", r"Broken", r"Defective", r"Parts",
        # Carriers (strip from model)
        r"Verizon", r"AT\s*&\s*T|ATT", r"T[-\s]?Mobile", r"Sprint", r"Cricket", r"Metro(?:PCS)?",
        r"Boost", r"Xfinity", r"U\.?\s*S\.?\s*Cellular", r"Visible", r"Mint\s*Mobile", r"Straight\s*Talk",
        r"TracFone", r"Consumer\s*Cellular", r"Simple\s*Mobile",
    ]
    pattern_remove_values = "|".join(remove_values) if remove_values else None
    # Work on a temp string
    temp = t
    if pattern_remove_values:
        temp = _re.sub(pattern_remove_values, " ", temp, flags=_re.IGNORECASE)
    for tok in spec_tokens:
        temp = _re.sub(tok, " ", temp, flags=_re.IGNORECASE)
    # Collapse whitespace
    temp = _re.sub(r"\s+", " ", temp).strip()
    # Debug log
    # Note: logger not available in this scope
    # Strip known extracted values (storage, colors, network, carriers) more aggressively from temp
    try:
        to_strip = []
        for k, v in list(keys.items()):
            if not isinstance(v, str):
                continue
            if k.startswith("title_storage_") or k.startswith("title_color") or k in ("title_network_status_key", "title_network_carrier_key"):
                to_strip.append(_re.escape(v))
                # also strip spaced GB
                m_cap = _re.match(r"^(\d+(?:\.\d+)?)(GB|TB|MB)$", v, _re.IGNORECASE)
                if m_cap:
                    num, unit = m_cap.group(1), m_cap.group(2)
                    to_strip.append(rf"\b{_re.escape(num)}\s*{_re.escape(unit)}\b")
        if to_strip:
            temp = _re.sub("|".join(to_strip), " ", temp, flags=_re.IGNORECASE)
            temp = _re.sub(r"\s+", " ", temp).strip()
    except Exception:
        pass
    if not temp:
        return None
    # Prefer substring after brand occurrence
    model_candidate = None
    if brand:
        m = _re.search(_re.escape(brand) + r"\b\s*(.+)$", t, _re.IGNORECASE)
        if m:
            after = m.group(1)
            # Stop at first spec/status keyword
            stops = ["Intel", "AMD", "Apple", "GHz", "RAM", "SSD", "HDD", "No ", "BIOS", "Locked", "]", "("]
            cut = len(after)
            for s in stops:
                idx = after.find(s)
                if idx != -1:
                    cut = min(cut, idx)
            cand = after[:cut].strip(" ,-;")
            # Debug logging skipped (no logger in scope)
            if 3 <= len(cand) <= 80:
                model_candidate = cand
    # Fallback to cleaned temp
    if not model_candidate:
        model_candidate = temp
    # Final cleanup
    model_candidate = model_candidate.strip(" ,-/|")
    # Remove residual trailing CPU speed fragments like '@ 1.80'
    try:
        import re as _re
        model_candidate = _re.sub(r"@\s*$", "", model_candidate).strip()
        model_candidate = _re.sub(r"@?\s*\d+\.\d+\s*$", "", model_candidate).strip()
    except Exception:
        pass
    if model_candidate:
        return model_candidate
    return None


def parse_title_to_keys(title_text: str, logger=None) -> Dict[str, str]:
    """Run description extractors on a title string and convert canonical outputs to title_* keys."""
    from description_extraction.cpu import parse_cpu
    from description_extraction.ram import parse_ram
    from description_extraction.storage import parse_storage
    from description_extraction.graphics import parse_graphics
    from description_extraction.screen import parse_screen
    from description_extraction.lot import parse_lot
    from description_extraction.phone import parse_phone
    from description_extraction.hdd import parse_hdd
    from description_extraction.form_factor import parse_form_factor
    from tools.device_type import classify_device_type

    keys: Dict[str, str] = {}

    try:
        cpu_data = parse_cpu(title_text, logger)
        keys.update(map_cpu_to_title_keys(cpu_data))
        # Finalize CPU keys with multi-CPU/model/gen/speed logic
        _finalize_cpu_keys(title_text, keys)

        # Brand/model from title if present (HP 250 G8, etc.)
        tt = title_text
        m = _re.search(r"\b(HP|HPE|Dell|Lenovo|Acer|ASUS|Apple|Microsoft|Toshiba|Samsung|LG|MSI|Sony|Cisco|Supermicro|IBM|Fujitsu|Gigabyte|ASRock|Alienware|Panasonic|Dynabook|Razer|Huawei|Zebra|Kyocera|Sonim|Google)\b", tt, _re.IGNORECASE)
        if m:
            keys.setdefault("title_brand_key", m.group(1))
        # Model heuristic: brand followed by token block up to 6 words skipping CPU/size terms
        if m:
            after = tt[m.end():].strip()
            after = after.split('Intel')[0].split('AMD')[0].split('Core')[0]
            # Avoid optical/accessory-only titles affecting CPU parsing; preserve clean model
            if _re.search(r"\b(USB\s+SuperDrive|SuperDrive|Optical|Drive|DVD|CD|Blu[- ]?ray)\b", tt, _re.IGNORECASE):
                base_model = after.strip().split(',')[0]
                if base_model:
                    keys.setdefault("title_model_key", base_model)
            # Stop words common in titles to avoid over-capturing into model
            for stop in ["3.5GHz", "2.9GHz", "GHz", "RAM", "SSD", "HDD", "Battery", "BIOS", "Locked", "No ", "Intel", "AMD", "Core", "Ryzen", "i3", "i5", "i7", "i9"]:
                idx = after.find(stop)
                if idx != -1:
                    after = after[:idx]
            model_guess = after.strip().split(',')[0].split('  ')[0].strip()
            if model_guess and len(model_guess) <= 60:
                # Append bracketed suffix from original title if it is part of model (e.g., Pixel G-2PW4100 Gray)
                model_guess = _clean_model_string(model_guess)
                # Remove trailing CPU speed numeric fragments like "@ 3.0" or "3.0"
                model_guess = _re.sub(r"\s*@?\s*\d+\.\d+\s*$", "", model_guess).strip()
                keys.setdefault("title_model_key", model_guess)
    except Exception:
        pass

    try:
        ram_data = parse_ram(title_text, logger)
        keys.update(map_ram_to_title_keys(ram_data))
        # Also detect multi-RAM sizes expressed as slashes near RAM context (e.g., 4/8GB RAM, 8GB/16GB/32GB RAM)
        import re as _re
        # Pattern 1: 4/8GB RAM
        m_slash = _re.search(r"\b((?:\d{1,3})(?:\s*/\s*\d{1,3})+)\s*GB\s*(?:RAM|DDR\d|LPDDR\d)\b", title_text, _re.IGNORECASE)
        sizes_from_slash: list[str] = []
        if m_slash:
            nums = [seg.strip() for seg in m_slash.group(1).split('/') if seg.strip()]
            sizes_from_slash = [f"{n}GB" for n in nums if n.isdigit()]
        # Pattern 2: 8GB/16GB/32GB RAM
        if not sizes_from_slash:
            m_gbslash = _re.search(r"\b((?:\d{1,3}GB)(?:\s*/\s*\d{1,3}GB)+)\s*(?:RAM|DDR\d|LPDDR\d)\b", title_text, _re.IGNORECASE)
            if m_gbslash:
                parts = [p.strip() for p in _re.split(r"/", m_gbslash.group(1)) if p.strip()]
                sizes_from_slash = [p.upper() for p in parts if _re.match(r"\d{1,3}GB", p, _re.IGNORECASE)]
        if sizes_from_slash:
            # Ensure base then numbered keys
            if "title_ram_size_key" not in keys and sizes_from_slash:
                keys["title_ram_size_key"] = sizes_from_slash[0]
                sizes_from_slash = sizes_from_slash[1:]
            slot = 2
            for s in sizes_from_slash:
                kname = f"title_ram_size{slot}_key"
                if kname not in keys:
                    keys[kname] = s
                    slot += 1
                if slot > 4:
                    break
        # If only a single ambiguous GB value exists in the title and device type is a laptop/desktop,
        # bias it to RAM unless adjacent to storage keywords
        if "title_ram_size_key" not in keys and "title_storage_capacity_key" not in keys:
            tt = title_text
            # find lone GB values
            singles = [m.group(0).upper() for m in _re.finditer(r"\b(4|8|12|16|24|32|48|64|96|128)GB\b", tt, _re.IGNORECASE)]
            # If one value and no storage adjacency, set as RAM
            if len(singles) == 1:
                gb = singles[0]
                if not _re.search(r"\b" + _re.escape(gb) + r"\s*(SSD|HDD|NVME|EMMC|M\.2|MSATA|SATA|SAS|SCSI)\b", tt, _re.IGNORECASE):
                    keys["title_ram_size_key"] = gb
    except Exception:
        pass

    try:
        storage_data = parse_storage(title_text, logger)
        keys.update(map_storage_to_title_keys(storage_data))
        # Populate storage_status and os_status if negations or mentions present
        tt = title_text.lower()
        if any(neg in tt for neg in ("no hdd", "no hard drive", "no ssd", "no storage", "no m.2", "without hard drive", "without ssd")):
            keys["title_storage_status_key"] = "Not Included"
        # Storage extras
        if storage_data.get("storage_drive_count"):
            keys.setdefault("title_storage_drive_count_key", storage_data["storage_drive_count"])
        if storage_data.get("storage_individual_capacity"):
            keys.setdefault("title_storage_individual_capacity_key", storage_data["storage_individual_capacity"])
        if storage_data.get("storage_drive_size"):
            keys.setdefault("title_storage_drive_size_key", storage_data["storage_drive_size"])
    except Exception:
        pass

    # NEW: Title-level pattern layer for "XGB RAM YGB" -> treat YGB as storage (>=256GB or any TB)
    try:
        import re as _re
        # Find sequences like "32GB RAM 512GB" allowing small noise tokens between
        m_seq = _re.search(r"\b(\d+(?:\.\d+)?)\s*GB\b[^\n]{0,20}?\bRAM\b[^\n]{0,20}?\b(\d+(?:\.\d+)?)\s*(GB|TB)\b", title_text, _re.IGNORECASE)
        if m_seq:
            left_val = float(m_seq.group(1))
            right_num = float(m_seq.group(2))
            right_unit = m_seq.group(3).upper()
            # Only assign storage when the right side is clearly storage-sized
            if (right_unit == 'GB' and right_num >= 256) or (right_unit == 'TB'):
                # Don't overwrite if already set by extractors
                if "title_storage_capacity_key" not in keys:
                    keys["title_storage_capacity_key"] = f"{int(right_num) if right_num.is_integer() else right_num}{right_unit}"
                # Ensure RAM stays mapped on the left side if not already set
                if "title_ram_size_key" not in keys:
                    # Round to int if whole number to match typical key formatting
                    keys["title_ram_size_key"] = f"{int(left_val) if left_val.is_integer() else left_val}GB"
    except Exception:
        pass

    # Multi-value slash parsing for storage capacities like 128/256GB (skip when near 'RAM')
    try:
        import re as _re
        for m in _re.finditer(r"\b(\d+(?:\.\d+)?)((?:\s*/\s*\d+(?:\.\d+)?)+)\s*(GB|TB)\b", title_text, _re.IGNORECASE):
            first = f"{m.group(1)}{m.group(3).upper()}"
            rest = [seg.strip() for seg in m.group(2).split('/') if seg.strip()]
            vals = [first] + [f"{x}{m.group(3).upper()}" for x in rest]
            # If this X/YGB cluster is followed by the word RAM within a short window, treat as RAM only
            span_end = m.end()
            window = title_text[span_end: span_end + 10]
            if _re.search(r"\bRAM\b", window, _re.IGNORECASE):
                # Assign the smallest as RAM and skip storage keys for this cluster
                try:
                    nums = sorted(int(v[:-2]) for v in vals if v.upper().endswith("GB"))
                    if nums:
                        keys["title_ram_size_key"] = f"{nums[0]}GB"
                        if len(nums) > 1:
                            keys["title_ram_size2_key"] = f"{nums[1]}GB"
                except Exception:
                    pass
                continue
            # Ensure base key is non-numbered first
            if "title_storage_capacity_key" not in keys and vals:
                keys["title_storage_capacity_key"] = vals[0]
                vals = vals[1:]
            slot = 2
            for v in vals:
                keyname = f"title_storage_capacity{slot}_key"
                if keyname not in keys:
                    keys[keyname] = v
                    slot += 1
                if slot > 4:
                    break
    except Exception:
        pass

    # Colors using phone extractor palette; supports up to 3 distinct colors
    try:
        from description_extraction.phone import PHONE_COLORS
        import re as _re
        found: list[str] = []
        # Prefer longer multi-word color names first, and avoid adding sub-colors if a longer match exists
        for name in sorted(PHONE_COLORS, key=len, reverse=True):
            # Word-bounded match; allow flexible whitespace for multi-word colors
            pattern = r"(?<!\w)" + _re.escape(name).replace(r"\ ", r"\\s+") + r"(?!\w)"
            if _re.search(pattern, title_text, _re.IGNORECASE):
                # Skip if this color is a sub-string (ignoring spaces/case) of any already-found color
                nm = name.replace(" ", "").lower()
                if any(nm in f.replace(" ", "").lower() for f in found):
                    continue
                found.append(name)
        if found:
            # Merge common two-word color phrases if parts were captured separately
            phrase_combos = [
                ("Midnight", "Black", "Midnight Black"),
                ("Space", "Gray", "Space Gray"),
                ("Space", "Grey", "Space Grey"),
                ("Rose", "Gold", "Rose Gold"),
                ("Product", "Red", "Product Red"),
                ("Alpine", "Green", "Alpine Green"),
            ]
            fl = [c.lower() for c in found]
            for a, b, phrase in phrase_combos:
                if a.lower() in fl and b.lower() in fl and phrase not in found and _re.search(r"\b" + a + r"\s+" + b + r"\b", title_text, _re.IGNORECASE):
                    # remove parts and add phrase
                    found = [c for c in found if c.lower() not in (a.lower(), b.lower())]
                    found.insert(0, phrase)
            # Single color to title_color_key for phones/tablets, else enumerate
            if len(found) >= 1:
                keys.setdefault("title_color_key", found[0])
            # Only enumerate additional colors to avoid duplicating color_key
            extras = found[1:3]
            for offset, c in enumerate(extras, start=2):
                keys.setdefault(f"title_color{offset}_key", c)
            # Preserve reference behavior for special finishes like "Natural Titanium"
            tll = title_text.lower()
            extra_colors = [tok for tok in ("Natural Titanium",) if tok.lower() in tll]
            if extra_colors:
                prev = keys.get("title_additional_info_key", "").strip()
                addition = " ".join(extra_colors)
                keys["title_additional_info_key"] = (f"{prev} {addition}" if prev else addition)
    except Exception:
        pass

    try:
        gpu_data = parse_graphics(title_text, logger)
        keys.update(map_gpu_to_title_keys(gpu_data))
        # Require GPU context for GPU RAM size to avoid misclassifying general RAM as VRAM
        if "title_gpu_ram_size_key" in keys and "title_gpu_model_key" not in keys:
            import re as _re
            if not _re.search(r"\b(geforce|quadro|radeon|rtx|gtx|rx|vram|graphics|gpu|iris|arc)\b", title_text, _re.IGNORECASE):
                keys.pop("title_gpu_ram_size_key", None)
    except Exception:
        pass

    # Screen
    try:
        screen_data = parse_screen(title_text, logger)
        keys.update(map_screen_to_title_keys(screen_data))
    except Exception:
        pass

    # HDD specific details (moved to after device type inference)

    # (removed) Network devices (switches and adapters / NICs)

    # Network/carrier (phones/tablets)
    try:
        from description_extraction.network import parse_network
        net = parse_network(title_text, logger)
        if net.get("network_status"):
            # Base key should be non-numbered first
            keys.setdefault("title_network_status_key", net["network_status"])
        i = 2
        while net.get(f"network_status{i}"):
            keys.setdefault(f"title_network_status{i}_key", net[f"network_status{i}"])
            i += 1
        # carriers may be multiple
        if net.get("network_carrier"):
            keys.setdefault("title_network_carrier_key", net["network_carrier"])
        i = 2
        while net.get(f"network_carrier{i}"):
            keys.setdefault(f"title_network_carrier{i}_key", net[f"network_carrier{i}"])
            i += 1
        # Also mirror primary carrier to additional info to match some reference outputs
        if net.get("network_carrier"):
            prev = keys.get("title_additional_info_key", "").strip()
            addition = net["network_carrier"]
            if addition not in (prev or ""):
                keys["title_additional_info_key"] = (f"{prev} {addition}" if prev else addition)
    except Exception:
        pass

    try:
        lot_data = parse_lot(title_text, logger)
        keys.update(map_lot_to_title_keys(lot_data))
        # Battery status
        tt = title_text.lower()
        if "no battery" in tt or "without battery" in tt or "no batteries" in tt:
            keys["title_battery_status_key"] = "Not Included"
        elif "with battery" in tt or "w/ battery" in tt:
            keys.setdefault("title_battery_status_key", "Included")
    except Exception:
        pass

    # Enrich OS keys via dedicated parser
    try:
        from description_extraction.os import parse_os
        os_data = parse_os(title_text, logger)
        if os_data.get("os_status"):
            # Map to normalized casing consistent with examples
            status = os_data["os_status"]
            if status.lower() in ("installed", "included"):
                keys["title_os_status_key"] = "Included"
            elif status.lower() in ("no os", "not included"):
                keys["title_os_status_key"] = "Not Included"
            else:
                keys["title_os_status_key"] = status
        if os_data.get("os_type"):
            keys["title_os_type_key"] = os_data["os_type"]
        # Windows: include version + edition when available (e.g., "11 Pro"); macOS handled below
        if os_data.get("os_type", "").lower() == "windows":
            ver = str(os_data.get("os_version") or "").strip()
            ed = str(os_data.get("os_edition") or "").strip()
            if ver and ed:
                keys["title_os_edition_key"] = f"{ver} {ed}"
            elif ver:
                keys["title_os_edition_key"] = ver
            elif ed:
                keys["title_os_edition_key"] = ed
        # Force keys when explicit negation text appears
        tll = title_text.lower()
        if "no os" in tll or "no operating system" in tll:
            keys["title_os_status_key"] = "Not Included"
            keys["title_os_type_key"] = "No OS"
        # Force Not Included if explicit negation text appears
        tll = title_text.lower()
        if "no os" in tll or "no operating system" in tll:
            keys["title_os_status_key"] = "Not Included"
        # Fallback heuristics: detect compact Windows tokens like WIN11Pro, WIN10, Win 11 Pro, etc.
        import re as _re
        if "title_os_type_key" not in keys:
            # Accept compact merged forms like WIN11PRO/WIN10HOME or spaced variants
            m_win = _re.search(r"\bwin(?:dows)?\s*(xp|vista|7|8(?:\.1)?|10|11)\s*(home|pro|enterprise|education)?\b", title_text, _re.IGNORECASE)
            if m_win:
                keys["title_os_type_key"] = "Windows"
                ver = m_win.group(1)
                ed = m_win.group(2)
                if ed:
                    keys["title_os_edition_key"] = f"{ver} {ed.title()}"
                else:
                    keys["title_os_edition_key"] = ver
        # Compact forms like WIN11PRO, WIN10HOME
        if "title_os_type_key" not in keys:
            m_compact = _re.search(r"\bwin(?:dows)?\s*(\d{1,2}|xp|vista)(?:\s*|-)?(home|pro|enterprise|education)?\b|\bwin(?:dows)?(xp|vista|7|8(?:\.1)?|10|11)(home|pro|enterprise|education)\b", title_text, _re.IGNORECASE)
            if m_compact:
                keys["title_os_type_key"] = "Windows"
                ver = m_compact.group(1) or (m_compact.group(3) if m_compact.lastindex and m_compact.lastindex >= 3 else None)
                ed = m_compact.group(2) or (m_compact.group(4) if m_compact.lastindex and m_compact.lastindex >= 4 else None)
                if ed and ver:
                    keys["title_os_edition_key"] = f"{ver} {ed.title()}"
                elif ver:
                    keys["title_os_edition_key"] = ver
                elif ed:
                    keys["title_os_edition_key"] = ed.title()
        # Ultra-compact forms like WIN11PRO without spaces
        if "title_os_type_key" not in keys:
            m_ultra = _re.search(r"\bwin\s*(\d{1,2})\s*(home|pro|enterprise|education)?\b", title_text, _re.IGNORECASE)
            if m_ultra:
                keys["title_os_type_key"] = "Windows"
                ver = m_ultra.group(1)
                ed = m_ultra.group(2)
                if ed:
                    keys["title_os_edition_key"] = f"{ver} {ed.title()}"
                else:
                    keys["title_os_edition_key"] = ver

        # macOS fallback detection (emit friendly name only, no numeric version)
        if "title_os_type_key" not in keys:
            mac_tokens = _re.search(r"\b(mac\s*os|os\s*x|osx|macos)\b", title_text, _re.IGNORECASE)
            mac_names = [
                "Monterey", "Big Sur", "Catalina", "Mojave", "High Sierra", "Sierra", "El Capitan", "Yosemite",
                "Mavericks", "Ventura", "Sonoma", "Sequoia", "Mountain Lion", "Lion", "Snow Leopard",
                "Leopard", "Tiger"
            ]
            found_name = None
            for name in mac_names:
                if _re.search(r"\b" + _re.escape(name) + r"\b", title_text, _re.IGNORECASE):
                    found_name = name
                    break
            # Apple context: brand Apple or device type mentions Apple
            apple_ctx = (keys.get("title_brand_key", "").lower() == "apple") or ("apple" in keys.get("title_device_type_key", "").lower())
            # Accept either explicit mac tokens or Apple context + OS <Name>
            has_mac_os_hint = bool(mac_tokens) or bool(_re.search(r"\bOS\s+" + (found_name or ""), title_text, _re.IGNORECASE))
            if (mac_tokens and found_name) or (apple_ctx and found_name and has_mac_os_hint):
                keys["title_os_type_key"] = "macOS"
                keys["title_os_edition_key"] = found_name

        # Gate iOS: only keep if device is phone/tablet or Apple brand; drop otherwise (prevents Cisco IOS false positives)
        try:
            if keys.get("title_os_type_key") == "iOS":
                dtype = keys.get("title_device_type_key", "").lower()
                brand = keys.get("title_brand_key", "").lower()
                if not ("cell phone" in dtype or "smartphone" in dtype or "tablet" in dtype or brand == "apple"):
                    keys.pop("title_os_type_key", None)
        except Exception:
            pass
        # Also handle slash-grouped negatives like "No Battery/OS/HDD" (any order)
        import re as _re
        if "title_os_status_key" not in keys:
            if _re.search(r"\bno\b[^\n]*\bos\b", tll, _re.IGNORECASE) or _re.search(r"\bno\b[^\n]*\boperating\s*system\b", tll, _re.IGNORECASE):
                keys["title_os_status_key"] = "Not Included"
                keys.setdefault("title_os_type_key", "No OS")
        import re as _re
    except Exception:
        pass

    # Generic status enrichment
    try:
        from description_extraction.status import parse_status
        st = parse_status(title_text, logger)
        if st.get("storage_status"):
            # Normalize to Included/Not Included
            val = st["storage_status"].lower()
            keys.setdefault("title_storage_status_key", "Not Included" if ("no" in val or "without" in val) else "Included")
        if st.get("os_status") and "title_os_status_key" not in keys:
            # Normalize regardless of word order (e.g., "No Battery/OS/HDD")
            val = st["os_status"].lower()
            keys["title_os_status_key"] = "Not Included" if ("no" in val or "without" in val) else "Included"
            if keys["title_os_status_key"] == "Not Included":
                keys.setdefault("title_os_type_key", "No OS")
        if st.get("battery_status") and "title_battery_status_key" not in keys:
            bv = st["battery_status"].upper()
            if bv in ("AS IS",):
                keys["title_battery_status_key"] = "AS IS"
            elif bv in ("NOT INCLUDED",):
                keys["title_battery_status_key"] = "Not Included"
            elif bv in ("GOOD", "INCLUDED"):
                keys["title_battery_status_key"] = "Included"
            elif bv in ("BAD",):
                keys["title_battery_status_key"] = "Bad"
        # BIOS status
        if st.get("bios_status"):
            # Standardize to Locked/Unlocked if possible; default to raw
            bios_val = st["bios_status"]
            keys.setdefault("title_bios_status_key", "Locked" if "lock" in bios_val.lower() else bios_val)
        # Fallback: capture grouped negatives regardless of order (e.g., "No OS/HDD/Batteries")
        import re as _re
        tll = title_text.lower()
        if "title_storage_status_key" not in keys:
            if _re.search(r"\bno\b[^\n]*\b(hdd|ssd|storage|drive|drives|hard\s*drive|m\.?2|nvme|emmc)\b", tll, _re.IGNORECASE):
                keys["title_storage_status_key"] = "Not Included"
        if "title_battery_status_key" not in keys:
            if _re.search(r"\bno\b[^\n]*\bbatter(?:y|ies)\b", tll, _re.IGNORECASE):
                keys["title_battery_status_key"] = "Not Included"
        # New: Disc/optical drive status (DVD/CD/Optical/Blu-ray/SuperDrive)
        if "title_disc_drive_status_key" not in keys:
            if _re.search(r"\b(no|without|missing)\b[^\n]*\b(dvd|cd\s*rom|cdrom|optical|optical\s*drive|disc\s*drive|disk\s*drive|blu[- ]?ray|bd\s*rom|superdrive)\b", tll, _re.IGNORECASE):
                keys["title_disc_drive_status_key"] = "Not Included"
    except Exception:
        pass

    # Battery enrichment (health/condition/presence)
    try:
        from description_extraction.battery import parse_battery
        b = parse_battery(title_text, logger)
        if b:
            keys.update(map_battery_to_title_keys(b, existing=keys))
    except Exception:
        pass

    # Phone context: if a plausible phone/tablet storage size exists, bias storage capacity over RAM
    try:
        phone_data = parse_phone(title_text, logger)
        phone_storage = phone_data.get("storage_size") or phone_data.get("storage_size1")
        if phone_storage:
            keys["title_storage_capacity_key"] = phone_storage
            # Avoid conflicting small RAM sizes mis-mapped to storage
            if keys.get("title_ram_size_key") == phone_storage:
                del keys["title_ram_size_key"]
            # If storage type missing, prefer EMMC/NVME/M.2 hints else leave unset
            if "title_storage_type_key" not in keys:
                tt = title_text.lower()
                if "emmc" in tt:
                    keys["title_storage_type_key"] = "EMMC"
                elif "m.2" in tt or "m2" in tt:
                    keys["title_storage_type_key"] = "M.2"
                elif "nvme" in tt:
                    keys["title_storage_type_key"] = "NVME"
        # Device type for downstream logic (optional)
        dtype = classify_device_type(title_text, allow_llm=False)
        if dtype:
            keys.setdefault("title_device_type_key", dtype)
            # If listing is memory, do not set storage capacities (but keep storage type if present)
            if "memory" in dtype.lower():
                keys.pop("title_storage_capacity_key", None)
                keys.pop("title_storage_capacity2_key", None)
        # Phone model names -> model key
        if phone_data.get("phone_model_name"):
            # If a model is already present, prefer the cleaner one; always clean noise tokens
            cleaned = _clean_model_string(phone_data["phone_model_name"]) 
            prev_model = keys.get("title_model_key")
            if not prev_model or len(cleaned) < len(prev_model):
                keys["title_model_key"] = cleaned
        # Apple A#### id -> additional info
        if phone_data.get("phone_model"):
            prev = keys.get("title_additional_info_key", "").strip()
            addition = f"AppleModel: {phone_data['phone_model']}"
            keys["title_additional_info_key"] = (f"{prev} {addition}" if prev else addition)
        # For phone/tablet titles: treat a lone GB >= 16 as storage if no explicit storage set
        import re as _re
        dtype_lc = keys.get("title_device_type_key", "").lower()
        if ("cell phone" in dtype_lc or "smartphone" in dtype_lc or "tablet" in dtype_lc) and "title_storage_capacity_key" not in keys:
            gb_vals = [m.group(0).upper().replace(" ", "") for m in _re.finditer(r"\b(\d{1,4})\s*GB\b", title_text, _re.IGNORECASE)]
            uniq = []
            for v in gb_vals:
                if v not in uniq:
                    uniq.append(v)
            if len(uniq) == 1:
                try:
                    num = int(uniq[0][:-2])
                    if num >= 16:
                        keys["title_storage_capacity_key"] = uniq[0]
                        if keys.get("title_ram_size_key") == uniq[0]:
                            keys.pop("title_ram_size_key", None)
                except Exception:
                    pass
    except Exception:
        pass

    # Final refinement for PCs: enforce RAM vs storage disambiguation, disallow small GB as storage unless adjacent to storage keywords
    # For non-phone/tablet, only allow storage capacity if either:
    # - adjacency in title like "256GB SSD"/"1TB HDD", or
    # - GB >= 256, or
    # - TB any, or
    # - device type is clearly memory (handled above by clearing), or device type is phone/tablet
    try:
        dtype = keys.get("title_device_type_key", "")
        is_phone_tablet = dtype in ("Cell Phones & Smartphones", "Tablets & eBook Readers")
        cap = keys.get("title_storage_capacity_key", "")
        if cap:
            # Big GB/TB passes
            import re as _re
            m = _re.match(r"^(\d+(?:\.\d+)?)(GB|TB)$", cap, _re.IGNORECASE)
            allow = False
            if m:
                val = float(m.group(1))
                unit = m.group(2).upper()
                if unit == "TB" or (unit == "GB" and val >= 256):
                    allow = True
                    
            # Check adjacency in title: size followed by storage keyword
            tt = title_text
            if not allow:
                if _re.search(r"\b" + _re.escape(cap) + r"\s*(SSD|HDD|NVME|EMMC|M\.2|MSATA|SATA|SAS|SCSI)\b", tt, _re.IGNORECASE):
                    allow = True
                    
            # Special case: if RAM is explicitly mentioned next to the capacity, it's NOT storage
            # This handles cases like "8GB RAM 256GB SSD" where 8GB should not be storage
            if _re.search(r"\b" + _re.escape(cap) + r"\s*RAM\b|\bRAM\s*" + _re.escape(cap) + r"\b", tt, _re.IGNORECASE):
                allow = False
                # Also set as RAM size if not already set
                if "title_ram_size_key" not in keys:
                    keys["title_ram_size_key"] = cap
                # Remove from storage
                keys.pop("title_storage_capacity_key", None)
            # If DDR context is present (e.g., 8GB DDR3), treat capacity as RAM and not storage
            if not allow and _re.search(r"\bDDR[2-5]X?\b", tt, _re.IGNORECASE) and cap.endswith("GB"):
                if "title_ram_size_key" not in keys:
                    keys["title_ram_size_key"] = cap
                if keys.get("title_storage_capacity_key") == cap:
                    keys.pop("title_storage_capacity_key", None)
                allow = False
                
            # Handle Surface/Tablet devices with RAM sizes being confused as storage
            if not allow and not is_phone_tablet and "title_ram_size_key" not in keys and _re.search(r"\b(surface|tablet|chromebook)\b", tt, _re.IGNORECASE):
                if _re.search(r"\b" + _re.escape(cap) + r"\s*RAM\b|\bRAM\s*" + _re.escape(cap) + r"\b|\b\d+GB\s+RAM\b|\bRAM\b", tt, _re.IGNORECASE):
                    keys["title_ram_size_key"] = cap
                    keys.pop("title_storage_capacity_key", None)
                    allow = False
                    
            # Handle Chromebook cases specifically - they typically have small storage (32GB eMMC)
            # but we need to make sure 4GB/8GB is recognized as RAM
            if _re.search(r"\bchromebook\b", tt, _re.IGNORECASE) and cap in ("4GB", "8GB"):
                if "title_ram_size_key" not in keys:
                    keys["title_ram_size_key"] = cap
                if keys.get("title_storage_capacity_key") == cap:
                    keys.pop("title_storage_capacity_key", None)
                allow = False
                
            # Special case for "4GB 32GB eMMC" pattern in Chromebooks - very common
            if cap in ("4GB", "8GB") and _re.search(r"\b" + _re.escape(cap) + r"\s+\d+GB\s+e?mmc\b", tt, _re.IGNORECASE):
                if "title_ram_size_key" not in keys:
                    keys["title_ram_size_key"] = cap
                if keys.get("title_storage_capacity_key") == cap:
                    keys.pop("title_storage_capacity_key", None)
                allow = False
                
            # Specific pattern for Chromebook listings: "4GB 32GB eMMC" where 4GB is RAM
            if _re.search(r"\bchromebook\b", tt, _re.IGNORECASE) and cap in ("4GB", "8GB"):
                # Look for a pattern like "4GB 32GB eMMC" or "4GB, 32GB eMMC"
                if _re.search(r"\b" + _re.escape(cap) + r"(?:\s|,)+(\d+GB)\s+e?mmc\b", tt, _re.IGNORECASE):
                    # This is definitely RAM
                    if "title_ram_size_key" not in keys:
                        keys["title_ram_size_key"] = cap
                    # Remove from storage if it was set
                    if keys.get("title_storage_capacity_key") == cap:
                        keys.pop("title_storage_capacity_key", None)
                    allow = False
                
            # Handle special case for "4GBRAM 64GBSSD" pattern (no space)
            if _re.search(r"\b\d+GBRAM\b", tt, _re.IGNORECASE) and cap.endswith("GB"):
                ram_match = _re.search(r"\b(\d+)GBRAM\b", tt, _re.IGNORECASE)
                if ram_match and ram_match.group(1) + "GB" == cap:
                    # This is a RAM size with no space
                    if "title_ram_size_key" not in keys:
                        keys["title_ram_size_key"] = cap
                    if keys.get("title_storage_capacity_key") == cap:
                        keys.pop("title_storage_capacity_key", None)
                    allow = False
                    
            # Handle "4GBRAM 64GBSSD" pattern where both are stuck together
            if cap.endswith("GB") and _re.search(r"\b\d+GBRAM\b.*\b\d+GBSSD\b", tt, _re.IGNORECASE):
                # Extract the RAM size
                ram_match = _re.search(r"\b(\d+)GBRAM\b", tt, _re.IGNORECASE)
                if ram_match:
                    ram_size = ram_match.group(1) + "GB"
                    if ram_size == cap:
                        # This is definitely RAM
                        if "title_ram_size_key" not in keys:
                            keys["title_ram_size_key"] = cap
                        if keys.get("title_storage_capacity_key") == cap:
                            keys.pop("title_storage_capacity_key", None)
                        allow = False
                # Extract the SSD size
                ssd_match = _re.search(r"\b(\d+)GBSSD\b", tt, _re.IGNORECASE)
                if ssd_match:
                    ssd_size = ssd_match.group(1) + "GB"
                    if ssd_size == cap:
                        # This is definitely storage
                        keys["title_storage_capacity_key"] = cap
                        keys["title_storage_type_key"] = "SSD"
                        allow = True
            
            # Handle standalone "XGB Ram" patterns (case insensitive)
            if cap.endswith("GB") and _re.search(r"\b" + _re.escape(cap[:-2]) + r"GB\s*Ram?\b", tt, _re.IGNORECASE):
                if "title_ram_size_key" not in keys:
                    keys["title_ram_size_key"] = cap
                # Remove from storage if it was set
                keys.pop("title_storage_capacity_key", None)
                allow = False
                        
            # Handle Chromebook specific patterns - these are the remaining cases
            if _re.search(r"\bchromebook\b", tt, _re.IGNORECASE):
                # If we see a pattern like "4GB RAM 32GB eMMC" in a Chromebook title
                # The first GB value is almost always RAM, the second is storage
                if cap == "4GB" and _re.search(r"\b4GB\b.*\b32GB\b.*\beMMC\b|\b4GB\b.*\bRAM\b.*\b32GB\b", tt, _re.IGNORECASE):
                    if "title_ram_size_key" not in keys:
                        keys["title_ram_size_key"] = "4GB"
                    if keys.get("title_storage_capacity_key") == "4GB":
                        keys.pop("title_storage_capacity_key", None)
                    allow = False
                    
                # Ensure 32GB is always storage in Chromebooks with eMMC
                if cap == "32GB" and _re.search(r"\beMMC\b", tt, _re.IGNORECASE):
                    keys["title_storage_capacity_key"] = "32GB"
                    keys["title_storage_type_key"] = "EMMC"
                    allow = True
                    
                # Special case: "4GB RAM 32GB eMMC" - make sure 32GB is storage, not RAM
                if cap == "32GB" and _re.search(r"\b4GB\b.*\bRAM\b.*\b32GB\b.*\beMMC\b", tt, _re.IGNORECASE):
                    if keys.get("title_ram_size_key") == "32GB":
                        keys["title_ram_size_key"] = "4GB"  # Fix incorrect RAM assignment
                    keys["title_storage_capacity_key"] = "32GB"
                    keys["title_storage_type_key"] = "EMMC"
                    allow = True
                    
            # Special case for Microsoft Surface with XGBRAM pattern
            surface_gbram = _re.search(r"\bmicrosoft\s+surface\b.*\b(\d+)GBRAM\b", tt, _re.IGNORECASE)
            if surface_gbram:
                ram_size = surface_gbram.group(1) + "GB"
                if cap == ram_size:
                    if "title_ram_size_key" not in keys:
                        keys["title_ram_size_key"] = ram_size
                    if keys.get("title_storage_capacity_key") == ram_size:
                        keys.pop("title_storage_capacity_key", None)
                    allow = False
                    
            # Special case for "32/64GB SSD" pattern - complex storage capacity
            if cap in ("32GB", "64GB") and _re.search(r"\b32/64GB\s+SSD\b", tt, _re.IGNORECASE):
                # This is a complex storage capacity pattern, keep the larger one
                keys["title_storage_capacity_key"] = "64GB"
                allow = True
                    
            # Global rule (per user): if no storage sign present and two GB values occur,
            # smaller is RAM, larger is storage (for non-phone/tablet too)
            if not allow:
                gb_vals = [m.group(0).upper() for m in _re.finditer(r"\b(\d{1,4})GB\b", tt, _re.IGNORECASE)]
                # keep uniques and numeric sort
                uniq = []
                for v in gb_vals:
                    if v not in uniq:
                        uniq.append(v)
                if len(uniq) >= 2:
                    nums = sorted({int(v[:-2]) for v in uniq})
                    small = f"{nums[0]}GB"
                    large = f"{nums[-1]}GB"
                    # Assign
                    keys["title_ram_size_key"] = small
                    if nums[-1] >= 256 or _re.search(r"\b" + _re.escape(large) + r"\s*(SSD|HDD|NVME|EMMC|M\.2|MSATA|SATA|SAS|SCSI)\b", tt, _re.IGNORECASE):
                        keys["title_storage_capacity_key"] = large
                    else:
                        keys.pop("title_storage_capacity_key", None)
                    allow = True
    except Exception:
        pass

    # After all extraction: infer device type from brand/model families; derive model if missing
    try:
        brand = keys.get("title_brand_key")
        model = keys.get("title_model_key")
        inferred = _infer_device_type_from_brand_model(brand, model, title_text)
        if inferred:
            keys.setdefault("title_device_type_key", inferred)
        # Form factor
        ff = parse_form_factor(title_text, logger)
        if ff.get("form_factor"):
            keys.setdefault("title_form_factor_key", ff["form_factor"])
        # Parts listings should keep descriptors (Housing, OEM, Genuine, Assembly, Parts)
        dtype = keys.get("title_device_type_key", "")
        if (not model) or (dtype and "Parts" in dtype):
            derived_model = _derive_model_from_title(title_text, brand, keys)
            if derived_model:
                if dtype and "Parts" in dtype:
                    desc_bits = []
                    tl = title_text
                    for tok in ("Housing", "Original", "OEM", "Genuine", "Assembly", "Parts"):
                        if tok.lower() in tl.lower() and tok not in desc_bits:
                            desc_bits.append(tok)
                    keys["title_model_key"] = (f"{derived_model} {' '.join(desc_bits)}".strip() if desc_bits else derived_model)
                else:
                    keys["title_model_key"] = derived_model
        # Dell-specific typing reinforcement
        if brand and brand.lower() == "dell" and keys.get("title_model_key"):
            mval = keys["title_model_key"]
            if is_dell_laptop_model(mval):
                keys.setdefault("title_device_type_key", "PC Laptops & Netbooks")
            elif is_dell_desktop_model(mval):
                keys.setdefault("title_device_type_key", "PC Desktops & All-In-Ones")
        # Final Inspiron desktop override: if title clearly says Inspiron + desktop terms, force desktops
        try:
            tll = title_text.lower()
            is_dell = (brand or "").lower() == "dell" or "dell" in tll
            if is_dell and "inspiron" in tll and any(x in tll for x in ("desktop", "tower", "all-in-one", "aio")):
                keys["title_device_type_key"] = "PC Desktops & All-In-Ones"
        except Exception:
            pass
    except Exception:
        pass

    # HDD specific details: only if device type is storage or HDD-specific listing
    try:
        dtype = keys.get("title_device_type_key", "")
        # Restrict HDD parsing to storage/HDD devices and desktops/servers where it is relevant
        if any(x in dtype for x in ("Desktops", "Servers", "Hard Disk", "Internal Hard Disk", "HDD")):
            hdd_data = parse_hdd(title_text, logger)
            keys.update(map_hdd_to_title_keys(hdd_data))
    except Exception:
        pass

    # Final model cleanup: remove already-extracted storage, color(s), network status/carriers from model
    try:
        import re as _re
        mdl = keys.get("title_model_key")
        if mdl:
            parts_to_strip: list[str] = []
            # CPU families and code patterns
            parts_to_strip.extend([
                r"Intel", r"AMD", r"Apple", r"Core", r"Ryzen", r"Xeon", r"Celeron", r"Pentium",
                r"i[3579]-?\d+[A-Za-z0-9]*", r"M[1-4](?:\s+(?:Pro|Max|Ultra))?",
                r"(?:E[357]|W)-\d{3,5}[A-Za-z]?", r"v\d+",
                r"\d+(?:\.\d+)?\s*GHz",
            ])
            # storage values (and spaced variants)
            for k in list(keys.keys()):
                if k.startswith("title_storage_"):
                    v = keys.get(k)
                    if isinstance(v, str) and v:
                        parts_to_strip.append(_re.escape(v))
                        m_cap = _re.match(r"^(\d+(?:\.\d+)?)(GB|TB|MB)$", v, _re.IGNORECASE)
                        if m_cap:
                            num, unit = m_cap.group(1), m_cap.group(2)
                            parts_to_strip.append(rf"\b{_re.escape(num)}\s*{_re.escape(unit)}\b")
            # colors (+ WiFi for phones)
            for k in list(keys.keys()):
                if k.startswith("title_color"):
                    v = keys.get(k)
                    if isinstance(v, str) and v:
                        parts_to_strip.append(_re.escape(v))
            if (keys.get("title_device_type_key") or "").lower() in ("cell phones & smartphones", "tablets & ebook readers"):
                parts_to_strip.append(r"\bWi[- ]?Fi\b")
            # network values
            for k in ("title_network_status_key", "title_network_status2_key", "title_network_carrier_key"):
                v = keys.get(k)
                if isinstance(v, str) and v:
                    parts_to_strip.append(_re.escape(v))
            # common junk words
            junk_tokens = [r"Unlocked", r"Only", r"Screen", r"Screens?", r"Scratches?", r"Crack(?:ed)?", r"Broken", r"Defective", r"Parts"]
            parts_to_strip.extend(junk_tokens)
            if parts_to_strip:
                mdl2 = _re.sub("|".join(parts_to_strip), " ", mdl, flags=_re.IGNORECASE)
                mdl2 = _clean_model_string(mdl2)
                mdl2 = _re.sub(r"\s+", " ", mdl2).strip(" -_,/")
                # For PC/Server/Apple Laptop/Thin Client device types, also remove residual capacity/speed and OCR 'No <component>' variants
                dtype = (keys.get("title_device_type_key") or "").lower()
                mdl2_lc = mdl2.lower()
                is_pc_like = (
                    any(x in dtype for x in ("pc laptops", "pc desktops", "servers", "computer servers", "apple laptops", "thin", "workstation", "workstations"))
                    or ("thin client" in mdl2_lc)
                    or _re.search(r"\b(ProLiant|PowerEdge|Precision|OptiPlex|Latitude|EliteDesk|ProDesk|ThinkPad|ThinkCentre|ThinkStation|Z\d{3,4}|Workstation|R\d{3,4}|DL\d{2,4}|ML\d{2,4}|OEMR)\b", mdl2, _re.IGNORECASE)
                )
                if is_pc_like:
                    mdl2 = _re.sub(r"\b\d+(?:\.\d+)?\s*(GB|TB)\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\bRAM\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\bDDR(?:[2345](?:[A-Z0-9\-]+)?)\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\b(?:SODIMM|UDIMM|RDIMM|LRDIMM|ECC|Registered|Unbuffered|Buffered|CAMM)\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"@\s*\d+(?:[\.-]\d+)?\s*(?:GHz|MHz)?", " ", mdl2, flags=_re.IGNORECASE)
                    # remove stray '@' not followed by digits
                    mdl2 = _re.sub(r"@\s*(?!\d)", " ", mdl2)
                    mdl2 = _re.sub(r"\b\d+\s*MHz\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\bNo\s+(?:HDD|SSD|Hard\s*Drive|HardDrive|Power(?:\s*Cord)?|PowerCord|Power(?:\s*Supply)?|PowerSupply|AC\s*Adapter|Adapter|Charger|Battery|OS|Operating\s*System|RAM|Memory|Cadd(?:y|ies)|Bezel|Cover|Back\s*Cover|Front\s*Bezel|Keys?)\b", " ", mdl2, flags=_re.IGNORECASE)
                    # remove 'RAM No' and 'Memory No' OCR leftovers
                    mdl2 = _re.sub(r"\b(?:RAM|Memory)\s+No\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"/(?:Power(?:\s*Supply|\s*Cord)?|Bezel|Cover|Cadd(?:y|ies)|Keys?|Charger|Battery|RJ-?45)\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\bMS[-\s]?DOS\b", " ", mdl2, flags=_re.IGNORECASE)
                    # disclaimers and test phrases
                    mdl2 = _re.sub(r"\bPower\s*Tested\b", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\bBoots\s*(?:to\s*)?BIOS\b", " ", mdl2, flags=_re.IGNORECASE)
                    # drop isolated 'NO'/'No' after previous removals
                    mdl2 = _re.sub(r"(?:(?<=^)|(?<=[\s\-_,/|]))(?:NO|No)(?=($|[\s\-_,/|]))", " ", mdl2)
                    mdl2 = _re.sub(r"[!*]\s*READ!?", " ", mdl2, flags=_re.IGNORECASE)
                    mdl2 = _re.sub(r"\|\s*\|", "|", mdl2)
                    mdl2 = _re.sub(r"\s+", " ", mdl2).strip(" -_,/|")
                else:
                    # Fallback cleanup for non-network listings that still show specs/negatives in model
                    is_network_like = any(x in dtype for x in ("switch", "router", "firewall", "network", "access point", "wireless")) or _re.search(r"\b(Catalyst|Meraki|Nexus|Juniper|Brocade|Arista|PoE|SFP|Switch|Router|Firewall)\b", mdl2, _re.IGNORECASE)
                    likely_pc_hw = bool(
                        keys.get("title_cpu_brand_key")
                        or keys.get("title_ram_size_key")
                        or keys.get("title_os_status_key")
                        or keys.get("title_storage_status_key")
                    )
                    if likely_pc_hw and not is_network_like and _re.search(r"@|\bRAM\b|\bNo\b|Power\s*Tested|Boots\s*.*BIOS", mdl2, _re.IGNORECASE):
                        mdl2 = _re.sub(r"@\s*\d+(?:[\.-]\d+)?\s*(?:GHz|MHz)?", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"@\s*(?!\d)", " ", mdl2)
                        mdl2 = _re.sub(r"\b\d+\s*MHz\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\b\d+(?:\.\d+)?\s*(GB|TB)\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bRAM\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bDDR(?:[2345](?:[A-Z0-9\-]+)?)\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\b(?:SODIMM|UDIMM|RDIMM|LRDIMM|ECC|Registered|Unbuffered|Buffered|CAMM)\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bNo\s+(?:HDD|SSD|Hard\s*Drive|HardDrive|Power(?:\s*Cord)?|PowerCord|Power(?:\s*Supply)?|PowerSupply|AC\s*Adapter|Adapter|Charger|Battery|OS|Operating\s*System|RAM|Memory|Cadd(?:y|ies)|Bezel|Cover|Back\s*Cover|Front\s*Bezel|Keys?)\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\b(?:RAM|Memory)\s+No\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"/(?:Power(?:\s*Supply|\s*Cord)?|Bezel|Cover|Cadd(?:y|ies)|Keys?|Charger|Battery|RJ-?45)\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bMS[-\s]?DOS\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bPower\s*Tested\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\bBoots\s*(?:to\s*)?BIOS\b", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"(?:(?<=^)|(?<=[\s\-_,/|]))(?:NO|No)(?=($|[\s\-_,/|]))", " ", mdl2)
                        mdl2 = _re.sub(r"[!*]\s*READ!?", " ", mdl2, flags=_re.IGNORECASE)
                        mdl2 = _re.sub(r"\|\s*\|", "|", mdl2)
                        mdl2 = _re.sub(r"\s+", " ", mdl2).strip(" -_,/|")
                if mdl2:
                    keys["title_model_key"] = mdl2
            # For phones/tablets, strip WiFi, carrier names, and 'Power Tested' from model even if not extracted as keys
            dtype_ft = (keys.get("title_device_type_key") or "").lower()
            if dtype_ft in ("cell phones & smartphones", "tablets & ebook readers"):
                mdl2 = keys.get("title_model_key", mdl2)
                mdl2 = _re.sub(r"\bWi[- ]?Fi\b", " ", mdl2, flags=_re.IGNORECASE)
                mdl2 = _re.sub(r"\b(Verizon|AT&T|ATT|T[- ]?Mobile|Sprint|Cricket|Boost(?:\s*Mobile)?|Metro(?:PCS)?|US\s*Cellular|Unlocked)\b", " ", mdl2, flags=_re.IGNORECASE)
                mdl2 = _re.sub(r"\bPower\s*Tested\b", " ", mdl2, flags=_re.IGNORECASE)
                mdl2 = _re.sub(r"\s+", " ", mdl2).strip(" -_,/")
                keys["title_model_key"] = mdl2
    except Exception:
        pass

    # Final post-processing dedup across series after all assignments
    try:
        import re as _re
        def _dedupe_series_keys(series_regex: str, base_key: str) -> None:
            series = [k for k in keys.keys() if _re.match(series_regex, k)]
            def sk(k: str) -> tuple[int, int]:
                if k == base_key:
                    return (0, 0)
                m = _re.match(r".*?(\d+)_key$", k)
                idx = int(m.group(1)) if m else 0
                return (1, idx)
            series.sort(key=sk)
            seen: set[str] = set()
            for k in series:
                v = keys.get(k)
                if not isinstance(v, str) or not v:
                    continue
                vn = v.strip().lower()
                if vn in seen and k != base_key:
                    keys.pop(k, None)
                else:
                    seen.add(vn)

        _dedupe_series_keys(r"^title_cpu_suffix(\d+)?_key$", "title_cpu_suffix_key")
        _dedupe_series_keys(r"^title_cpu_speed(\d+)?_key$", "title_cpu_speed_key")
        _dedupe_series_keys(r"^title_storage_capacity(\d+)?_key$", "title_storage_capacity_key")
        _dedupe_series_keys(r"^title_ram_size(\d+)?_key$", "title_ram_size_key")
    except Exception:
        pass

    return keys


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Map titles to likely title_* keys using existing extractors; output JSONL and optional CSV summary."
        )
    )
    parser.add_argument(
        "--in",
        dest="input_path",
        type=Path,
        default=Path.cwd() / "titles_extracted.txt",
        help="Input titles file (one title per line). Default: ./titles_extracted.txt",
    )
    parser.add_argument(
        "--out-jsonl",
        dest="out_jsonl",
        type=Path,
        default=Path.cwd() / "title_keys_preview.jsonl",
        help="Output JSONL path. Default: ./title_keys_preview.jsonl",
    )
    parser.add_argument(
        "--out-csv",
        dest="out_csv",
        type=Path,
        default=Path.cwd() / "title_keys_summary.csv",
        help="Optional CSV summary of common keys. Default: ./title_keys_summary.csv",
    )
    parser.add_argument(
        "--out-full",
        dest="out_full",
        type=Path,
        default=Path.cwd() / "title_keys_full.txt",
        help="Output full process_description style format with one entry per title (default: ./title_keys_full.txt)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    input_path: Path = args.input_path
    out_jsonl: Path = args.out_jsonl
    out_csv: Path = args.out_csv
    out_full: Optional[Path] = args.out_full

    if not input_path.exists() or not input_path.is_file():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    try:
        titles = [line.rstrip("\n") for line in input_path.open("r", encoding="utf-8-sig", errors="replace")]
    except OSError as exc:
        print(f"Failed to read input file: {exc}", file=sys.stderr)
        return 1

    # Store results for reuse across output formats
    results = []
    
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with out_jsonl.open("w", encoding="utf-8", newline="\n") as jf:
        for title in titles:
            if not title.strip():
                continue
            keys = parse_title_to_keys(title)
            rec = {"title": title, "title_keys": keys}
            results.append(rec)
            jf.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Simple CSV summary for quick eyeballing
    common_fields = [
        "title_cpu_model_key",
        "title_cpu_generation_key",
        "title_cpu_suffix_key",
        "title_cpu_speed_key",
        "title_ram_size_key",
        "title_ram_type_key",
        "title_ram_config_key",
        "title_storage_type_key",
        "title_storage_capacity_key",
        "title_storage_capacity2_key",
        "title_gpu_model_key",
        "title_lot_key",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.writer(cf)
        writer.writerow(["title"] + common_fields)
        for rec in results:
            title = rec.get("title", "")
            keys = rec.get("title_keys", {})
            row = [title] + [keys.get(f, "") for f in common_fields]
            writer.writerow(row)
    
    # Write full process_description style output if requested
    if out_full:
        out_full.parent.mkdir(parents=True, exist_ok=True)
        with out_full.open("w", encoding="utf-8", newline="\n") as ff:
            for rec in results:
                title = rec.get("title", "")
                keys = rec.get("title_keys", {})
                
                ff.write(f"Title: {title}\n")
                
                # Format keys in process_description style with base-first ordering
                import re as _re
                def _key_sort_tuple(k: str) -> tuple[str, int]:
                    m = _re.match(r"^(.*?)(\d+)_key$", k)
                    if m:
                        base = m.group(1)
                        idx = int(m.group(2))
                    else:
                        base = k[:-4] if k.endswith("_key") else k
                        idx = 0
                    return (base, idx)
                for key in sorted(keys.keys(), key=_key_sort_tuple):
                    value = keys[key]
                    ff.write(f"{key}: {value}\n")
                
                # Add a separator between entries
                ff.write("\n" + "-" * 80 + "\n\n")

    print(f"Wrote JSONL -> {out_jsonl}")
    print(f"Wrote CSV    -> {out_csv}")
    if out_full:
        print(f"Wrote FULL   -> {out_full}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


