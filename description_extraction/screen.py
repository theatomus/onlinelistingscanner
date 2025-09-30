import re
from typing import Any, Dict

RESO_TERMS = {
    "fhd": "1920x1080",
    "full hd": "1920x1080",
    "uhd": "3840x2160",
    "4k": "3840x2160",
    "qhd": "2560x1440",
    "wqhd": "2560x1440",
    "1440p": "2560x1440",
    "1080p": "1920x1080",
    "720p": "1280x720",
}

def parse_screen(desc_text: str, logger=None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not desc_text or len(desc_text.strip()) < 3:
        return result

    t = desc_text
    tl = t.lower()

    # size
    m = re.search(r"\b(\d+(?:\.\d+)?)\s*(?:\"|inch(?:es)?|in\b)\b", t, re.IGNORECASE)
    if m:
        result.setdefault("screen_size", f"{m.group(1)}in")

    # resolution split or token
    m = re.search(r"\b(\d+)\s*[xX]\s*(\d+)\b", t)
    if m:
        result.setdefault("screen_resolution", f"{m.group(1)}x{m.group(2)}")
    else:
        for k, v in RESO_TERMS.items():
            if k in tl:
                result.setdefault("screen_resolution", v)
                break

    # hertz
    m = re.search(r"\b(\d+)\s*hz\b", t, re.IGNORECASE)
    if m:
        result.setdefault("screen_hertz", f"{m.group(1)}Hz")

    # aspect ratio
    m = re.search(r"\b(\d+\s*:\s*\d+)\b", t)
    if m:
        result.setdefault("screen_aspect_ratio", m.group(1).replace(" ", ""))
    elif re.search(r"\b(\d+)\s*by\s*(\d+)\b", tl):
        m = re.search(r"\b(\d+)\s*by\s*(\d+)\b", tl)
        result.setdefault("screen_aspect_ratio", f"{m.group(1)}:{m.group(2)}")

    # touch
    if re.search(r"\b(touchscreen|touch screen|non[- ]?touch)\b", tl):
        touch = "Touch" if "non" not in tl[re.search(r"(touchscreen|touch screen|non[- ]?touch)", tl).start():re.search(r"(touchscreen|touch screen|non[- ]?touch)", tl).end()] else "Non-Touch"
        result.setdefault("screen_touch", touch)

    # panel type
    m = re.search(r"\b(ips|tn|va|oled|lcd|led|amoled)\b", tl)
    if m:
        result.setdefault("screen_panel_type", m.group(1).upper())

    if logger:
        try:
            logger.debug(f"Description Screen extraction: {result}")
        except Exception:
            pass
    return result



