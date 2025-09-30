#!/usr/bin/env python3
"""
Extract titles that have complex or oddly formatted CPU information, such as
slash-separated multi-CPU mentions, mixed generations, or mixed families.

Default inputs:
- tools/titles_extracted.txt (one title per line)
- tools/title_keys_full.txt (lines like: "Title: ...")
- title_keys_preview.jsonl (JSONL with {"title": ...})

Output:
- tools/complex_cpu_titles.txt (one title per line, de-duplicated and sorted)

Usage:
  python tools/extract_complex_cpu_titles.py \
    --inputs tools/titles_extracted.txt tools/title_keys_full.txt title_keys_preview.jsonl \
    --output tools/complex_cpu_titles.txt

If no --inputs are provided, the defaults above are used (only existing files
are read). If no --output is provided, the default output path is used.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Iterable, List, Set


# Regex patterns that identify complex CPU expressions
PATTERNS: List[re.Pattern] = [
    # Explicit model lists like: i7-6600U/5600U/7600U
    re.compile(r"\bi[3579]-\d{3,5}[A-Za-z0-9]*\s*/\s*(?:i[3579]-)?\d{3,5}[A-Za-z0-9]*(?:\s*/\s*\d{3,5}[A-Za-z0-9]*)*\b", re.IGNORECASE),
    # Family vs family (optionally with models): Core i7-5650U/i5-5350U, Core i7/i5, i7/i5
    re.compile(r"\b(?:core\s+)?i[3579](?:-\d{3,5}[A-Za-z0-9]*)?\b\s*/\s*\b(?:core\s+)?i[3579](?:-\d{3,5}[A-Za-z0-9]*)?\b", re.IGNORECASE),
    # Generation splits like: i5-8th/i7-10th Gen (allow any trailing text before 'Gen')
    re.compile(r"\bi[3579]-\d{1,2}(?:st|nd|rd|th)?\s*/\s*i[3579]-\d{1,2}(?:st|nd|rd|th)?\b[^\n]*\bGen\b", re.IGNORECASE),
    # Model pairs where the second omits family: 6780HQ/5600U, 8365U/8665U
    re.compile(r"\b(?:i[3579]-)?\d{3,5}[A-Za-z0-9]*\s*/\s*(?:i[3579]-)?\d{3,5}[A-Za-z0-9]*\b", re.IGNORECASE),
    # Pentium/Celeron mixes
    re.compile(r"\bPentium\b[^\n]*/[^\n]*\bCeleron\b|\bCeleron\b[^\n]*/[^\n]*\bPentium\b", re.IGNORECASE),
    # Xeon vs Core mixes
    re.compile(r"\bXeon\b[^\n]*/[^\n]*\bi[3579]\b|\bi[3579]\b[^\n]*/[^\n]*\bXeon\b", re.IGNORECASE),
]


def iter_titles_from_text_file(path: str) -> Iterable[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield line


def iter_titles_from_title_keys_full(path: str) -> Iterable[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            if not raw:
                continue
            idx = raw.find("Title:")
            if idx == -1:
                continue
            title = raw[idx + len("Title:"):].strip()
            if title:
                yield title


def iter_titles_from_jsonl(path: str) -> Iterable[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and "title" in obj and isinstance(obj["title"], str):
                    yield obj["title"].strip()
            except json.JSONDecodeError:
                # non-JSON line; ignore
                continue


def title_is_complex(title: str) -> bool:
    for pat in PATTERNS:
        if pat.search(title):
            return True
    return False


def collect_complex_titles(inputs: List[str]) -> List[str]:
    seen: Set[str] = set()
    results: List[str] = []
    for path in inputs:
        name = os.path.basename(path).lower()
        if name.endswith(".jsonl"):
            iterator = iter_titles_from_jsonl(path)
        elif name == "title_keys_full.txt":
            iterator = iter_titles_from_title_keys_full(path)
        else:
            iterator = iter_titles_from_text_file(path)

        for title in iterator:
            if title and title_is_complex(title):
                if title not in seen:
                    seen.add(title)
                    results.append(title)

    # Stable sort for reproducibility
    results.sort(key=lambda s: s.lower())
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract complex CPU titles")
    parser.add_argument(
        "--inputs",
        nargs="*",
        help="Input files to scan for titles",
    )
    parser.add_argument(
        "--output",
        default=os.path.join("tools", "complex_cpu_titles.txt"),
        help="Path to write the extracted titles (default: tools/complex_cpu_titles.txt)",
    )
    args = parser.parse_args()

    default_inputs = [
        os.path.join("tools", "titles_extracted.txt"),
        os.path.join("tools", "title_keys_full.txt"),
        os.path.join("title_keys_preview.jsonl"),
    ]
    inputs = args.inputs if args.inputs else [p for p in default_inputs if os.path.isfile(p)]
    if not inputs:
        raise SystemExit("No input files found. Provide --inputs or ensure defaults exist.")

    titles = collect_complex_titles(inputs)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        for t in titles:
            f.write(t + "\n")

    print(f"Wrote {len(titles)} titles to {args.output}")


if __name__ == "__main__":
    main()
