import sys
from pathlib import Path

# Ensure project root is on sys.path for module imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.title_ngram_key_mapper import main


if __name__ == "__main__":
    # Resolve paths relative to this script's directory so it works no matter the CWD
    base_dir = Path(__file__).resolve().parent

    titles_file = base_dir / "titles_extracted.txt"
    complex_titles_file = base_dir / "complex_cpu_titles.txt"

    # Outputs side-by-side in tools/
    full_default = base_dir.parent / "title_keys_full.txt"
    jsonl_default = base_dir.parent / "title_keys_preview.jsonl"
    csv_default = base_dir.parent / "title_keys_summary.csv"

    complex_full = base_dir / "complex cpu titles full.txt"
    complex_jsonl = base_dir / "complex_cpu_keys_preview.jsonl"
    complex_csv = base_dir / "complex_cpu_keys_summary.csv"

    # Run default titles mapping
    code1 = main(["--in", str(titles_file), "--out-full", str(full_default), "--out-jsonl", str(jsonl_default), "--out-csv", str(csv_default)])

    # Also process complex CPU titles into a dedicated FULL output
    code2 = main(["--in", str(complex_titles_file), "--out-full", str(complex_full), "--out-jsonl", str(complex_jsonl), "--out-csv", str(complex_csv)])

    sys.exit(code2 or code1)


