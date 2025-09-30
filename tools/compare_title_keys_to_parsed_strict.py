from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Tuple, List

ROOT = Path(__file__).resolve().parents[1]
REF_DIR = ROOT / "tools" / "New folder"
OURS_FILE = ROOT / "tools" / "title_keys_full.txt"


def parse_process_description_style(text: str) -> List[Tuple[str, Dict[str, str]]]:
    entries: List[Tuple[str, Dict[str, str]]] = []
    title = None
    kv: Dict[str, str] = {}
    for line in text.splitlines():
        if line.startswith("Title: "):
            if title is not None:
                entries.append((title, kv))
            title = line[len("Title: ") :].strip()
            kv = {}
            continue
        if not line.strip():
            continue
        if line.startswith("-") and set(line.strip()) == {"-"}:  # separator
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            kv[k.strip()] = v.strip()
    if title is not None:
        entries.append((title, kv))
    return entries


def normalize_key_value(k: str, v: str) -> Tuple[str, str]:
    # Keys are case-sensitive; normalize some value formats only
    v = v.strip()
    # GHz: enforce 2 decimals if looks like number + GHz
    m = re.match(r"^(\d+)(?:\.(\d+))?GHz$", v, re.IGNORECASE)
    if m:
        whole = int(m.group(1))
        frac = (m.group(2) or "0")[:2]
        v = f"{whole}.{frac:0<2}GHz"
    # Collapse multiple spaces
    v = re.sub(r"\s+", " ", v)
    return k, v


def load_reference() -> Dict[str, Dict[str, str]]:
    ref: Dict[str, Dict[str, str]] = {}
    for path in sorted(REF_DIR.glob("python_parsed_*.txt")):
        text = path.read_text(encoding="utf-8", errors="replace")
        entries = parse_process_description_style(text)
        for title, kv in entries:
            norm = {normalize_key_value(k, v)[0]: normalize_key_value(k, v)[1] for k, v in kv.items()}
            ref[title] = norm
    return ref


def load_ours() -> Dict[str, Dict[str, str]]:
    text = OURS_FILE.read_text(encoding="utf-8", errors="replace")
    entries = parse_process_description_style(text)
    ours: Dict[str, Dict[str, str]] = {}
    for title, kv in entries:
        norm = {normalize_key_value(k, v)[0]: normalize_key_value(k, v)[1] for k, v in kv.items()}
        ours[title] = norm
    return ours


def compare(ref: Dict[str, Dict[str, str]], ours: Dict[str, Dict[str, str]]) -> Tuple[int, List[str]]:
    missing_count = 0
    lines: List[str] = []
    for title, expected in ref.items():
        actual = ours.get(title, {})
        # Count only missing keys or value mismatches for exact keys
        for k, v in expected.items():
            av = actual.get(k)
            if av is None:
                missing_count += 1
                lines.append(f"Title: {title}\n  Missing: {k}: {v}")
            elif av != v:
                missing_count += 1
                lines.append(f"Title: {title}\n  Mismatch: {k}: expected={v} actual={av}")
    return missing_count, lines


def main() -> int:
    ref = load_reference()
    ours = load_ours()
    count, details = compare(ref, ours)
    out = ROOT / "tools" / "diagnostics" / "title_key_diffs_strict.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(details), encoding="utf-8")
    print(f"Strict compare -> missing_or_mismatch={count}")
    print(f"Report -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


