import re
from typing import Any, Dict, Optional

CPU_BRANDS = ["Intel", "AMD", "Apple"]

def _normalize_speed_to_ghz(speed_text: str) -> str:
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(ghz|mhz)$", speed_text.strip(), re.IGNORECASE)
    if not m:
        return speed_text
    val, unit = m.groups()
    num = float(val)
    if unit.lower() == "mhz":
        num = num / 1000.0
    # Always format to 2 decimals to match reference outputs
    formatted = f"{num:.2f}"
    return f"{formatted}GHz"


def _extract_cpu_suffix(code: str) -> Optional[str]:
    """Extract a CPU suffix (e.g., U, HQ, HK, K, KF, T, S, X, Y, G7) from a model code token.

    Examples:
    - 8550U -> U
    - 1065G7 -> G7
    - 9900K -> K
    - 9900KF -> KF
    - 7Y57 -> Y
    - 1165G7 -> G7
    Returns None if no recognizable suffix is present.
    """
    if not code:
        return None
    c = code.upper()
    # Prefer explicit G + digit suffix (Intel Gen10/11 graphics tier)
    m = re.search(r"G\d$", c)
    if m:
        return m.group(0)
    # Trailing letter-only suffixes (K, KF, KS, U, H, HQ, HK, T, S, X, XM, etc.)
    m = re.search(r"[A-Z]{1,3}$", c)
    if m:
        return m.group(0)
    # Embedded letter before trailing digits (e.g., 7Y57 -> Y)
    m = re.match(r"^[0-9]{1,2}([A-Z]{1,2})[0-9]{2,3}$", c)
    if m:
        return m.group(1)
    return None

def parse_cpu(desc_text: str, logger=None) -> Dict[str, Any]:
    """Extract CPU brand/model/family/speed/quantity from description text.
    Canonical keys: cpu_brand, cpu_model, cpu_family, cpu_speed, cpu_quantity, cpu_generation, cpu_suffix.
    Non-destructive; caller merges only when missing.
    """
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # Brand detection gated by CPU context to avoid false positives (e.g., Apple accessories)
    # Prefer explicit families to pick brand correctly
    intel_context = bool(re.search(r"\b(i[3579]|core\s+(?:ultra\s+)?[3579]|xeon|pentium|celeron|core\s+m[3579])\b", t, re.IGNORECASE))
    # AMD context if explicit AMD CPU families or AMD brand appear, or classic A/E-series codes present
    amd_context = bool(
        re.search(r"\b(amd|ryzen|athlon|fx|opteron|apu)\b", t, re.IGNORECASE)
    )
    apple_silicon_context = bool(re.search(r"\b(m[1-4](?:\s+(?:pro|max|ultra))?|apple\s+silicon)\b", t, re.IGNORECASE))
    
    # Special case: "Intel" with "m3" is definitely Intel context
    if re.search(r"\bintel\b.*\bm3\b|\bm3\b.*\bintel\b", tl):
        intel_context = True
        
    # Force Intel brand for any Core m3 in Chromebooks, Surface devices, or with explicit Intel mention
    if re.search(r"\bcore\s+m3\b", tl) and re.search(r"\b(chromebook|surface|intel)\b", tl, re.IGNORECASE):
        intel_context = True
        result.setdefault("cpu_brand", "Intel")
        
    # Block false Core m3 detection in HP Pavilion x360 m3 models (model name not CPU)
    if re.search(r"\bhp\s+pavilion\s+x360\s+m3\b", tl):
        result.setdefault("cpu_brand", "Intel")  # Force Intel for these models

    if apple_silicon_context:
        result.setdefault("cpu_brand", "Apple")
    elif intel_context:
        result.setdefault("cpu_brand", "Intel")
    elif amd_context:
        result.setdefault("cpu_brand", "AMD")

    # Apple M-series
    m = re.search(r"\b(M[123])(\s+Pro|\s+Max|\s+Ultra)?\b", t)
    if m:
        model = (m.group(1) + (m.group(2) or "")).strip()
        result.setdefault("cpu_model", model)
        result.setdefault("cpu_family", "Apple Silicon")
        return result

    # Intel Core Ultra / Core m3/m5/m7 (preserve lowercase m)
    m = re.search(r"\b(Intel\s+)?Core\s+(Ultra\s+)?([579]|M[357])\b(?:\s*-?\s*([A-Za-z0-9]{3,6}))?", t, re.IGNORECASE)
    if m:
        gen_suffix = m.group(4) or ""
        fam_tail = m.group(3)
        if fam_tail and str(fam_tail).upper().startswith("M"):
            fam_tail = f"m{str(fam_tail)[1:]}"  # Core m3/m5/m7
        family = ("Core " + (m.group(2) or "") + str(fam_tail)).replace("  ", " ").strip()
        result.setdefault("cpu_family", family)
        # For non-m families, treat trailing code as model
        if gen_suffix and not (isinstance(fam_tail, str) and fam_tail.startswith("m")):
            code = gen_suffix.upper()
            result.setdefault("cpu_model", f"{family} {code}")
            suffix = _extract_cpu_suffix(code)
            if suffix and "cpu_suffix" not in result:
                result["cpu_suffix"] = suffix
        # For Core m3/m5/m7, also capture codes like 6Y75, 7Y57, 6Y54 as model codes
        if gen_suffix and isinstance(fam_tail, str) and fam_tail.startswith("m"):
            code = gen_suffix.upper()
            if re.match(r"^\d{1,2}[A-Z]\d{2,3}$", code):
                result.setdefault("cpu_model", code)
                suffix = _extract_cpu_suffix(code)
                if suffix and "cpu_suffix" not in result:
                    result["cpu_suffix"] = suffix

    # Multi CPU families like i7/i9
    fam_pair = re.search(r"\b(i[3579])\s*/\s*(i[3579])\b", t, re.IGNORECASE)
    if fam_pair:
        s1 = fam_pair.group(1).lower()
        s2 = fam_pair.group(2).lower()
        result.setdefault("cpu_family1", f"Core {s1}")
        result.setdefault("cpu_family2", f"Core {s2}")

    # Multi model forms like i7-6600U/5600U/7600U
    mm = re.search(r"\b(i[3579])\s*-\s*([0-9]{3,5}[A-Za-z0-9]{0,2})(?:/([0-9]{3,5}[A-Za-z0-9]{0,2}))+\b", t, re.IGNORECASE)
    if mm:
        base_series = mm.group(1).lower()
        rest = t[mm.start():mm.end()]
        codes_part = re.sub(r"^.*?\-", "", rest)
        codes = [mm.group(2).upper()] + [c.upper() for c in codes_part.split('/') if c]
        seen = set()
        codes = [c for c in codes if not (c in seen or seen.add(c))]
        if codes:
            result.setdefault("cpu_model1", codes[0])
        if len(codes) > 1:
            result.setdefault("cpu_model2", codes[1])
        result.setdefault("cpu_family", f"Core {base_series}")
        sfx = _extract_cpu_suffix(codes[0])
        if sfx and "cpu_suffix" not in result:
            result["cpu_suffix"] = sfx
        # Derive generations per code in order (handles slash-separated models without repeated 'i7' prefix)
        def _gen_from_code(code: str) -> Optional[str]:
            m = re.match(r"^(\d{3,5})", code)
            if not m:
                return None
            num = m.group(1)
            # Two-digit generation for 10+; one-digit for 4-9
            if len(num) >= 4 and num[:2].isdigit() and num[:2] in {"10","11","12","13","14","15","16","17","18","19","20"}:
                gen_prefix = num[:2]
            else:
                gen_prefix = num[0]
            ordinal_map = {
                "4": "4th Gen", "5": "5th Gen", "6": "6th Gen", "7": "7th Gen", "8": "8th Gen", "9": "9th Gen",
                "10": "10th Gen", "11": "11th Gen", "12": "12th Gen", "13": "13th Gen", "14": "14th Gen"
            }
            return ordinal_map.get(gen_prefix)

        gens: list[str] = []
        for code in codes:
            g = _gen_from_code(code)
            if g and g not in gens:
                gens.append(g)
        for i, g in enumerate(gens, 1):
            if i == 1 and "cpu_generation" not in result and g:
                result["cpu_generation"] = g
            elif i > 1 and g:
                result.setdefault(f"cpu_generation{i}", g)

    # Intel i3/i5/i7/i9 model forms: i7-8650U / i5 8250U / i7-1065G7 / i5-7Y57 / i9-9900KF
    # Also handle short core codes like "6Y75" that can appear without preceding family in some titles
    m = re.search(r"\b(i[3579])\s*-?\s*((?:\d{3,5}[A-Za-z0-9]{0,2})|(?:\d{1,2}[A-Za-z]\d{2,3}))\b", t, re.IGNORECASE)
    if m and "cpu_model" not in result:
        series = m.group(1).lower()
        code = m.group(2).upper()
        # Reject bogus codes where the suffix looks like capacity (e.g., 128GB)
        if not re.search(r"\d+GB$", code):
            # Emit cpu_model as bare code to match configs output style
            result.setdefault("cpu_model", code)
            result.setdefault("cpu_family", f"Core {series}")
            suffix = _extract_cpu_suffix(code)
            if suffix and not re.search(r"GB$", suffix):
                result.setdefault("cpu_suffix", suffix)

    # If we have a Core m-family above, but the model code appears standalone (e.g., "6Y75")
    # and family already set to Core m3/m5/m7, capture it as cpu_model
    if (
        result.get("cpu_family", "").startswith("Core m")
        and "cpu_model" not in result
    ):
        mm = re.search(r"\b(\d{1,2}[A-Za-z]\d{2,3})\b", t)
        if mm:
            code = mm.group(1).upper()
            # Filter out obvious non-CPU codes like storage or RAM markers
            if not re.search(r"\d+(GB|TB|MB)$", code, re.IGNORECASE):
                result.setdefault("cpu_model", code)
                sfx = _extract_cpu_suffix(code)
                if sfx and "cpu_suffix" not in result:
                    result.setdefault("cpu_suffix", sfx)

    # Odd family forms like "5/i7" -> interpret as i5/i7 in Intel Core context
    if intel_context and not result.get("cpu_family1"):
        m = re.search(r"\b([3579])\s*/\s*i([3579])\b", t, re.IGNORECASE)
        if m:
            first = m.group(1).lower()
            second = m.group(2).lower()
            # Base family stays as detected elsewhere; enumerate alternates
            result.setdefault("cpu_family", result.get("cpu_family", f"Core i{first}"))
            result.setdefault("cpu_family1", f"Core i{second}")

    # Intel Xeon (extended patterns for hyphenated models with v-suffix e.g., E5-2680 v4, E3-1535M v6, W-2145)
    if "cpu_family" not in result:
        # Allow optional 'CPU' between 'Xeon' and the code
        m = re.search(r"\bXeon\b(?:\s+CPU)?\s+((?:E[357]|W)-\d{3,5}[A-Za-z]?)(?:\s*v\d+)?\b", t, re.IGNORECASE)
        if m:
            result.setdefault("cpu_family", "Xeon")
            full = m.group(0)  # includes optional vN
            core = m.group(1).upper()
            # capture v generation if present
            mvg = re.search(r"v(\d+)", full, re.IGNORECASE)
            model_str = core + (f" v{mvg.group(1)}" if mvg else "")
            result.setdefault("cpu_model", model_str)
            # Extract cpu_suffix from the numeric block (e.g., '1535M')
            mcode = re.search(r"-(\d{3,5}[A-Za-z]?)", core)
            if mcode:
                sfx = _extract_cpu_suffix(mcode.group(1))
                if sfx and "cpu_suffix" not in result:
                    result.setdefault("cpu_suffix", sfx)
    # Fallback generic Xeon
    m = re.search(r"\bXeon\b(?:\s*([A-Z]?\d{3,5}[A-Za-z]{0,2}))?", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        result.setdefault("cpu_family", "Xeon")
        if m.group(1):
            result.setdefault("cpu_model", f"Xeon {m.group(1).upper()}")

    # Pentium / Celeron series
    m = re.search(r"\b(Pentium|Celeron)\b(?:\s*(Silver|Gold|Bronze))?\s*(?:/?\s*Celeron)?", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        fam = m.group(1).title()
        variant = m.group(2).title() if m.group(2) else ""
        result.setdefault("cpu_family", f"{fam} {variant}".strip())

    # AMD Ryzen
    m = re.search(r"\b(Ryzen)\s+([3579])(?:\s*(\d{3,4}[A-Za-z]{0,2}))?\b", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        result.setdefault("cpu_family", f"Ryzen {m.group(2)}")
        if m.group(3) and "cpu_model" not in result:
            code = m.group(3).upper()
            result["cpu_model"] = code
            suffix = _extract_cpu_suffix(code)
            if suffix and "cpu_suffix" not in result:
                result["cpu_suffix"] = suffix

    # AMD A-series / E-series APUs: e.g., AMD E2-9000e, A6-9220C, E-350
    m = re.search(r"\b(?:AMD\s+)?(E\d|A\d{1,2})\s*-\s*([0-9]{3,5}[A-Za-z]{0,2})\b", t, re.IGNORECASE)
    if m and re.search(r"\b(amd|apu)\b", tl):
        fam = m.group(1).upper()
        code = m.group(2).upper()
        result.setdefault("cpu_brand", "AMD")
        result.setdefault("cpu_family", fam)
        # Use bare code as model
        if "cpu_model" not in result:
            result.setdefault("cpu_model", code)
        sfx = _extract_cpu_suffix(code)
        if sfx and "cpu_suffix" not in result:
            result.setdefault("cpu_suffix", sfx)

    # AMD Athlon
    m = re.search(r"\b(Athlon)(?:\s+(X[2-4]))?\s*([0-9]{3,4}[A-Za-z]{0,2})\b", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        fam = m.group(1).title()
        if m.group(2):
            fam = f"{fam} {m.group(2).upper()}"
        result.setdefault("cpu_brand", "AMD")
        result.setdefault("cpu_family", fam)
        code = m.group(3).upper()
        result.setdefault("cpu_model", code)

    # AMD FX
    m = re.search(r"\bFX[- ]?([0-9]{3,4}[A-Za-z]{0,2})\b", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        result.setdefault("cpu_brand", "AMD")
        result.setdefault("cpu_family", "FX")
        result.setdefault("cpu_model", m.group(1).upper())

    # AMD Opteron
    m = re.search(r"\bOpteron\s*([A-Za-z]?\d{3,4}[A-Za-z]{0,2})\b", t, re.IGNORECASE)
    if m and "cpu_family" not in result:
        result.setdefault("cpu_brand", "AMD")
        result.setdefault("cpu_family", "Opteron")
        result.setdefault("cpu_model", m.group(1).upper())

    # Speed: require CPU context before accepting GHz/MHz, and avoid RAM context
    try:
        speed_candidates: list[tuple[int, str]] = []
        # Collect GHz tokens
        for mm in re.finditer(r"\b(\d+(?:\.\d+)?)\s*GHz\b", t, re.IGNORECASE):
            speed_candidates.append((mm.start(), mm.group(0)))
        # Odd slash form: 3.30/40GHz => synthesize second as 3.40GHz
        mslash = re.search(r"\b(\d+)\.(\d{2})\s*/\s*(\d{2})\s*GHz\b", t, re.IGNORECASE)
        if mslash:
            major = mslash.group(1)
            dec1 = mslash.group(2)
            dec2 = mslash.group(3)
            synth1 = f"{major}.{dec1}GHz"
            synth2 = f"{major}.{dec2}GHz"
            # approximate start positions near the slash pattern
            base_start = mslash.start()
            speed_candidates.append((base_start, synth1))
            speed_candidates.append((base_start + 1, synth2))

        def has_cpu_context_before(pos: int) -> bool:
            span_start = max(0, pos - 40)
            window = t[span_start:pos]
            return bool(re.search(r"\b(intel|amd|apple|cpu|processor|core|ultra|i[3579]|ryzen|xeon|pentium|celeron|athlon)\b", window, re.IGNORECASE))

        def has_ram_context_near(pos: int) -> bool:
            span_start = max(0, pos - 15)
            span_end = min(len(t), pos + 15)
            window = t[span_start:span_end]
            return bool(re.search(r"\b(ddr\d?|lpddr\d|ram|memory|dimm|sodimm|so-dimm|udimm|rdimm|lrdimm|ecc)\b", window, re.IGNORECASE))

        best_val = 0.0
        best_norm = None
        for pos, s in speed_candidates:
            mnum = re.search(r"(\d+(?:\.\d+)?)", s)
            if not mnum:
                continue
            val = float(mnum.group(1))
            if not (0.5 <= val <= 6.0):
                continue
            # Accept GHz even if no explicit CPU context, but still reject in RAM context
            if not re.search(r"ghz", s, re.IGNORECASE) and not has_cpu_context_before(pos):
                continue
            if has_ram_context_near(pos):
                continue
            if val >= best_val:
                best_val = val
                best_norm = _normalize_speed_to_ghz(s)
        if best_norm and "cpu_speed" not in result:
            result.setdefault("cpu_speed", best_norm)
    except Exception:
        pass

    # Quantity
    m = re.search(r"\b(\d+)\s*x\s*(cpu|xeon|processor)\b", t, re.IGNORECASE)
    if m:
        result.setdefault("cpu_quantity", m.group(1))
    elif re.search(r"\bdual\s+cpu\b", t, re.IGNORECASE):
        result.setdefault("cpu_quantity", "2")

    # Generation (heuristic): capture leading 2 digits from i-series model 4th+ gen
    generations = []
    models = re.findall(r"\bi[3579]\s*-?\s*((1[0-9]|[4-9])\d{2})[A-Za-z]*", t, re.IGNORECASE)
    for model_code, gen_prefix in models:
        # Map leading digits to ordinal string
        ordinal_map = {
            "4": "4th Gen", "5": "5th Gen", "6": "6th Gen", "7": "7th Gen", "8": "8th Gen", "9": "9th Gen",
            "10": "10th Gen", "11": "11th Gen", "12": "12th Gen", "13": "13th Gen", "14": "14th Gen"
        }
        gen_str = ordinal_map.get(gen_prefix, gen_prefix)
        if gen_str not in generations:
            generations.append(gen_str)
    # Textual forms like "i5-13th", "i3 3rd"
    for mt in re.finditer(r"\b(i[3579])\s*-?\s*(\d{1,2})(?:st|nd|rd|th)\b(?:\s*gen(?:eration)?)?", t, re.IGNORECASE):
        series = mt.group(1).lower()
        num = int(mt.group(2))
        if 1 <= num <= 20:
            suf = "th"
            if num % 10 == 1 and num != 11:
                suf = "st"
            elif num % 10 == 2 and num != 12:
                suf = "nd"
            elif num % 10 == 3 and num != 13:
                suf = "rd"
            gen_str = f"{num}{suf} Gen"
            if gen_str not in generations:
                generations.append(gen_str)
            # Also record families from textual form
            result.setdefault("cpu_family", result.get("cpu_family", f"Core {series}"))
            # Populate alternates if needed
            if result.get("cpu_family") != f"Core {series}":
                if not result.get("cpu_family1"):
                    result.setdefault("cpu_family1", f"Core {series}")
                elif not result.get("cpu_family2"):
                    result.setdefault("cpu_family2", f"Core {series}")
    
    # Set generation keys
    for i, gen in enumerate(generations, 1):
        if i == 1:
            result.setdefault("cpu_generation", gen)
        else:
            result.setdefault(f"cpu_generation{i}", gen)

    if logger:
        try:
            logger.debug(f"Description CPU extraction: {result}")
        except Exception:
            pass
    return result



