from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from .ai_validator import validate_file


def pick_llm_url(primary: Optional[str], fallback: Optional[str]) -> Optional[str]:
    import requests
    for url in [primary, fallback]:
        if not url:
            continue
        try:
            # Try a quick call; llama.cpp may not expose /health, so fall back to simple GET /
            resp = requests.get(url.rstrip('/'), timeout=1)
            if resp.ok:
                return url
        except Exception:
            continue
    return primary or fallback


def main():
    ap = argparse.ArgumentParser(description="Write AI issues for a single item to training/live_issues/ITEM.txt")
    ap.add_argument("item", help="eBay item number")
    ap.add_argument("--items-dir", default="item_contents")
    ap.add_argument("--schema", default="training/schema.json")
    ap.add_argument("--lm", default="training/mini_lm.json")
    ap.add_argument("--llm-url", default=None)
    ap.add_argument("--fallback-url", default=None)
    # Web verify disabled; keep flag for compatibility but unused
    ap.add_argument("--web-verify", action="store_true")
    args = ap.parse_args()

    item_path = Path(args.items_dir) / f"python_parsed_{args.item}.txt"
    if not item_path.exists():
        return 0

    llm_url = pick_llm_url(args.llm_url, args.fallback_url)

    try:
        issues = validate_file(item_path, args.schema, args.lm, llm_url, False)
    except Exception as e:
        issues = [f"Validator error: {e}"]

    out_dir = Path("training") / "live_issues"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{args.item}.txt"
    if issues:
        out_file.write_text("\n".join(issues), encoding="utf-8")
    else:
        out_file.write_text("No AI issues found.", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


