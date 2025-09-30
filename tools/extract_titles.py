r"""
Directives / Intent Memory
--------------------------
- Goal: Extract every listing title from all description files anywhere under the user's Documents folder.
- Input scope: Recursively search:
-   * Any *.txt files whose filenames contain "description" (case-insensitive)
-   * All files inside any folder named "eBayListingData" (case-insensitive), at any depth
- Roots scanned by default:
-   1) The provided --root (default: user's Documents)
-   2) The script's directory and its parent directory (to include sibling folders like 'eBayListingData')
- Extraction rules:
  * For text files: collect every line matching /^\s*Title\s*:\s*(.+)/i
  * For JSON files: collect any string values for keys named 'title' (case-insensitive), recursively
  * For CSV files: collect the column named 'title' (case-insensitive)
  * Write titles one per line, without the "Title:" prefix
- Output: Write to a text file (default: titles_extracted.txt) with one title per line.
- Notes: Skip files without a matching title. Safe with large trees; streaming write; tolerant of encoding issues. Remove exact duplicates by default.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Generator, Iterable, Optional


TITLE_REGEX = re.compile(r"^\s*Title\s*:\s*(.+)$", re.IGNORECASE)


def iter_all_titles(root_dir: Path) -> Generator[str, None, None]:
    """Yield titles from matching files under root_dir.

    - Outside eBayListingData trees: scan only *.txt files with 'description' in filename.
    - Inside any directory path containing a component named 'eBayListingData' (case-insensitive):
      scan *.txt, *.json, *.csv files for titles.
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        path_parts_lower = {part.lower() for part in Path(dirpath).parts}
        in_ebay_tree = "ebaylistingdata" in path_parts_lower
        for filename in filenames:
            lower_name = filename.lower()
            file_path = Path(dirpath) / filename

            if in_ebay_tree:
                suffix = file_path.suffix.lower()
                if suffix == ".txt":
                    yield from extract_titles_from_txt(file_path)
                elif suffix == ".json":
                    yield from extract_titles_from_json(file_path)
                elif suffix == ".csv":
                    yield from extract_titles_from_csv(file_path)
                # ignore other file types in eBayListingData
                continue

            # Non-eBayListingData trees: only description*.txt
            if lower_name.endswith(".txt") and ("description" in lower_name):
                yield from extract_titles_from_txt(file_path)


def iter_all_titles_from_roots(roots: Iterable[Path]) -> Generator[str, None, None]:
    """Yield titles by scanning each root path with iter_all_titles."""
    for root in roots:
        yield from iter_all_titles(root)


def extract_titles_from_txt(file_path: Path) -> Generator[str, None, None]:
    """Yield all title strings from a text file by matching 'Title:' lines.

    Reads line-by-line using UTF-8 with BOM support and replaces undecodable characters.
    """
    try:
        with file_path.open("r", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                match = TITLE_REGEX.search(line)
                if match:
                    title = match.group(1).strip()
                    if title:
                        yield title
    except (OSError, UnicodeError):
        return


def extract_titles_from_json(file_path: Path) -> Generator[str, None, None]:
    """Yield title strings from JSON by collecting any string values under keys named 'title' (case-insensitive)."""
    try:
        with file_path.open("r", encoding="utf-8-sig", errors="replace") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return
    except (OSError, UnicodeError):
        return

    def _walk(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(key, str) and key.lower() == "title" and isinstance(value, str):
                    title = value.strip()
                    if title:
                        yield title
                # Recurse
                yield from _walk(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from _walk(item)

    yield from _walk(data)


def extract_titles_from_csv(file_path: Path) -> Generator[str, None, None]:
    """Yield title strings from CSV by reading the 'title' column (case-insensitive)."""
    try:
        with file_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            try:
                reader = csv.DictReader(f)
            except Exception:
                return
            if not reader.fieldnames:
                return
            title_field: Optional[str] = None
            for name in reader.fieldnames:
                if isinstance(name, str) and name.lower() == "title":
                    title_field = name
                    break
            if not title_field:
                return
            for row in reader:
                try:
                    value = row.get(title_field)
                except Exception:
                    value = None
                if isinstance(value, str):
                    title = value.strip()
                    if title:
                        yield title
    except (OSError, UnicodeError):
        return


def write_titles(titles: Iterable[str], out_path: Path, dedupe: bool) -> int:
    """Write titles to out_path, one per line. Optionally de-duplicate while preserving order.

    Returns the number of lines written.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if dedupe:
        seen = set()
        def _unique(seq: Iterable[str]) -> Iterable[str]:
            for s in seq:
                if s not in seen:
                    seen.add(s)
                    yield s
        titles = _unique(titles)

    count = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        for title in titles:
            if not title:
                continue
            out.write(title + "\n")
            count += 1
    return count


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    default_root = Path.home() / "Documents"
    parser = argparse.ArgumentParser(
        description=(
            "Extract listing titles from description files and any 'eBayListingData' folders.\n"
            "Outside 'eBayListingData': scans *.txt files whose names contain 'description'.\n"
            "Inside any 'eBayListingData' path: scans *.txt (Title: ... lines), *.json (any 'title' key), and *.csv (the 'title' column).\n"
            "By default scans both --root and the script's directory (and its parent). Writes one title per line."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=default_root,
        help=f"Root directory to scan (default: {default_root})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path.cwd() / "titles_extracted.txt",
        help="Output file path (default: ./titles_extracted.txt)",
    )
    parser.set_defaults(dedupe=True)
    parser.add_argument(
        "--no-dedupe",
        dest="dedupe",
        action="store_false",
        help="Disable de-duplication (enabled by default).",
    )
    parser.set_defaults(include_script_tree=True)
    parser.add_argument(
        "--no-script-tree",
        dest="include_script_tree",
        action="store_false",
        help="Disable scanning from the script's directory and its parent directory.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    root_dir: Path = args.root
    out_path: Path = args.out
    dedupe: bool = args.dedupe
    include_script_tree: bool = getattr(args, "include_script_tree", True)

    # Build roots list: CLI root (if valid) + script directory + script parent (optional)
    roots: list[Path] = []
    if root_dir.exists() and root_dir.is_dir():
        roots.append(root_dir)
    else:
        print(f"Root directory not found or not a directory: {root_dir}", file=sys.stderr)
    if include_script_tree:
        script_dir = Path(__file__).resolve().parent
        script_parent = script_dir.parent
        roots.extend([script_dir, script_parent])

    # Normalize, de-duplicate, and remove nested subpaths to avoid rescanning
    def _is_subpath(child: Path, parent: Path) -> bool:
        try:
            child.resolve().relative_to(parent.resolve())
            return True
        except Exception:
            return False

    normalized: list[Path] = []
    seen_norms: set[str] = set()
    for r in roots:
        try:
            resolved = r.resolve()
        except Exception:
            resolved = r
        norm = os.path.normcase(str(resolved))
        if norm not in seen_norms:
            seen_norms.add(norm)
            normalized.append(resolved)

    unique_roots: list[Path] = []
    for candidate in sorted(normalized, key=lambda p: len(str(p))):
        if any(_is_subpath(candidate, kept) for kept in unique_roots):
            continue
        unique_roots.append(candidate)

    if not unique_roots:
        print("No valid roots to scan.", file=sys.stderr)
        return 2

    def _titles() -> Generator[str, None, None]:
        yield from iter_all_titles_from_roots(unique_roots)

    written = write_titles(_titles(), out_path, dedupe)
    # Compact terminal output
    print(f"{written} titles -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


