import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple


REF_DIR = Path(__file__).resolve().parent / "New folder"
OUR_FULL = Path(__file__).resolve().parent / "title_keys_full.txt"
OUT_DIFF = Path(__file__).resolve().parent / "diagnostics" / "title_key_diffs.txt"


def parse_reference_files(ref_dir: Path) -> Dict[str, Dict[str, str]]:
    """Parse python_parsed_*.txt files produced by configs extractors.

    Returns mapping: title -> { title_key: value }
    """
    results: Dict[str, Dict[str, str]] = {}
    for f in sorted(ref_dir.glob("python_parsed_*.txt")):
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        title = None
        kv: Dict[str, str] = {}
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("Full Title:"):
                # New record starts
                if title and kv:
                    results.setdefault(title, {}).update(kv)
                title = line.split(":", 1)[1].strip()
                kv = {}
                continue
            # Lines like: [title_brand_key] brand: HP
            m = re.match(r"^\[(title_[^\]]+?)\]\s+[^:]+:\s*(.*)$", line)
            if m:
                key = m.group(1)
                val = m.group(2).strip()
                kv[key] = val
        if title and kv:
            results.setdefault(title, {}).update(kv)
    return results


def parse_our_full(path: Path) -> Dict[str, Dict[str, str]]:
    """Parse tools/title_keys_full.txt entries.

    Returns mapping: title -> { title_key: value }
    """
    results: Dict[str, Dict[str, str]] = {}
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return results
    blocks = text.split("\n--------------------------------------------------------------------------------\n")
    for block in blocks:
        lines = [l for l in (block.splitlines()) if l.strip()]
        if not lines:
            continue
        if not lines[0].startswith("Title:"):
            # Some blocks may start with garbage, try to find
            for i, l in enumerate(lines):
                if l.startswith("Title:"):
                    lines = lines[i:]
                    break
            else:
                continue
        title = lines[0].split(":", 1)[1].strip()
        kv: Dict[str, str] = {}
        for l in lines[1:]:
            m = re.match(r"^(title_[a-z0-9_]+_key):\s*(.*)$", l, flags=re.IGNORECASE)
            if m:
                key = m.group(1)
                val = m.group(2).strip()
                kv[key] = val
        if title:
            results[title] = kv
    return results


def diff_keys(ref: Dict[str, Dict[str, str]], ours: Dict[str, Dict[str, str]]) -> Tuple[int, List[str]]:
    missing_count = 0
    lines: List[str] = []
    titles = sorted(set(ref.keys()) & set(ours.keys()))
    for t in titles:
        rkv = ref[t]
        okv = ours[t]
        missing = []
        wrong = []
        for k, v in rkv.items():
            ov = okv.get(k)
            if ov is None or ov == "":
                missing.append((k, v))
            elif str(ov).strip() != str(v).strip():
                wrong.append((k, v, ov))
        if missing or wrong:
            missing_count += len(missing) + len(wrong)
            lines.append(f"Title: {t}")
            if missing:
                lines.append("  Missing:")
                for k, v in missing:
                    lines.append(f"    {k}: {v}")
            if wrong:
                lines.append("  Mismatch:")
                for k, v, ov in wrong:
                    lines.append(f"    {k}: expected={v} actual={ov}")
            lines.append("")
    return missing_count, lines


def main() -> int:
    ref = parse_reference_files(REF_DIR)
    ours = parse_our_full(OUR_FULL)
    missing_count, report_lines = diff_keys(ref, ours)
    OUT_DIFF.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIFF.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"Compared {len(ref)} reference titles vs {len(ours)} ours; missing_or_mismatch count={missing_count}")
    print(f"Report -> {OUT_DIFF}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


