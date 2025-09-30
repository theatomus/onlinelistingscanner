r"""
Directives / Intent Memory
--------------------------
- Goal: From an input file of lines (titles), output:
  1) all unique tokens (whitespace-separated)
  2) all unique contiguous token groups (n-grams), for n = 2..(len(title_tokens) - 1)
- Tokenization: Split strictly on whitespace. Keep tokens exactly as-is (no normalization).
- Groups: Contiguous n-grams formed by joining neighboring tokens with a single space. Do not record the full-title-length group to avoid redundancy.
- De-duplication: Global across entire input, order-preserving for first occurrence.
- Output: Two files by default:
  * tokens_extracted.txt (singles)
  * token_groups_extracted.txt (multi-token groups)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence


def iter_tokens_from_lines(lines: Iterable[str]) -> Iterator[str]:
    for line in lines:
        # Split on any whitespace; keep tokens as-is
        for token in line.split():
            if token:
                yield token


def iter_token_groups_from_lines(
    lines: Iterable[str],
    include_full_title_group: bool = False,
    max_group_size: Optional[int] = None,
) -> Iterator[str]:
    """Yield contiguous n-gram token groups from each line.

    - include_full_title_group: if True, include the full-length group; otherwise exclude it (default False)
    - max_group_size: optional cap for the group size; if None, use up to len(tokens) - 1 (or len(tokens))
    """
    for line in lines:
        tokens: list[str] = [t for t in line.split() if t]
        if not tokens:
            continue
        max_len_allowed = len(tokens) if include_full_title_group else max(len(tokens) - 1, 0)
        if max_group_size is not None:
            max_len_allowed = min(max_len_allowed, max_group_size)
        # Start from bigrams
        for n in range(2, max_len_allowed + 1):
            for i in range(0, len(tokens) - n + 1):
                group = " ".join(tokens[i : i + n])
                if group:
                    yield group


def write_unique_tokens(tokens: Iterable[str], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    seen: set[str] = set()
    count = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as out_file:
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            out_file.write(token + "\n")
            count += 1
    return count


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract unique tokens and unique contiguous token groups (n-grams) from an input text file of titles."
        )
    )
    parser.add_argument(
        "--in",
        dest="input_path",
        type=Path,
        default=Path.cwd() / "titles_extracted.txt",
        help="Input text file (default: ./titles_extracted.txt)",
    )
    parser.add_argument(
        "--out-singles",
        dest="out_singles",
        type=Path,
        default=Path.cwd() / "tokens_extracted.txt",
        help="Output file for unique single tokens (default: ./tokens_extracted.txt)",
    )
    parser.add_argument(
        "--out-groups",
        dest="out_groups",
        type=Path,
        default=Path.cwd() / "token_groups_extracted.txt",
        help="Output file for unique token groups (default: ./token_groups_extracted.txt)",
    )
    parser.add_argument(
        "--include-full",
        action="store_true",
        help="Include full-title-length group as well (default: excluded).",
    )
    parser.add_argument(
        "--max-n",
        type=int,
        default=None,
        help=(
            "Optional maximum token group size (n). If omitted, uses up to len(tokens)-1 per title (or len if --include-full)."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    input_path: Path = args.input_path
    out_singles: Path = args.out_singles
    out_groups: Path = args.out_groups
    include_full: bool = args.include_full
    max_n: Optional[int] = args.max_n

    if not input_path.exists() or not input_path.is_file():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    try:
        with input_path.open("r", encoding="utf-8-sig", errors="replace") as f:
            # We need to iterate lines twice; read once into memory-efficient list? We'll read the file into memory as lines.
            lines = list(f)
    except OSError as exc:
        print(f"Failed to read input file: {exc}", file=sys.stderr)
        return 1

    try:
        singles_written = write_unique_tokens(iter_tokens_from_lines(lines), out_singles)
        groups_written = write_unique_tokens(
            iter_token_groups_from_lines(lines, include_full_title_group=include_full, max_group_size=max_n),
            out_groups,
        )
    except OSError as exc:
        print(f"Failed to write output files: {exc}", file=sys.stderr)
        return 1

    # Compact terminal output
    print(f"{singles_written} singles -> {out_singles}")
    print(f"{groups_written} groups -> {out_groups}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


