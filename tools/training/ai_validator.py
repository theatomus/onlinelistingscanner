from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

from .parsed_reader import read_parsed_txt
from .value_normalizer import normalize_section_values
from .mini_lm import NgramLM
from .llm_suggester import suggest_with_llm
from .web_verifier import verify_with_web

from pathlib import Path


def load_instructions(path: Path = Path('training/config/instructions.yaml')):
    try:
        import yaml  # type: ignore
    except Exception:
        import subprocess, sys
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyyaml'])
        import yaml  # type: ignore
    try:
        if path.exists():
            return yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    except Exception:
        return {}
    return {}


class SimpleSchema:
    """
    A conservative, rule-based schema validator that learns what keys normally exist,
    then flags anomalies. This is intentionally deterministic to complement hard rules.
    """

    def __init__(self, allowed_keys: Dict[str, List[str]]):
        self.allowed = allowed_keys

    def validate(self, listing: Dict[str, Dict[str, str]], lm: NgramLM | None = None, llm_url: str | None = None) -> List[str]:
        issues: List[str] = []
        for section_name, keys in [
            ('title', list(listing.get('title', {}).keys())),
            ('specifics', list(listing.get('specifics', {}).keys())),
            ('metadata', list(listing.get('metadata', {}).keys())),
            ('description', list(listing.get('description', {}).keys())),
            ('table_shared', list(listing.get('table_shared', {}).keys())),
        ]:
            allowed = set(self.allowed.get(section_name, []))
            for k in keys:
                if k not in allowed:
                    issues.append(f"Unexpected key in {section_name}: {k}")
        # Value-level heuristics: normalize and (optionally) LM-based suggestion
        for section_name, section in listing.items():
            if not isinstance(section, dict):
                continue
            normalized = normalize_section_values(section_name, section)
            for k, v in section.items():
                nv = normalized.get(k, v)
                if nv != v:
                    issues.append(f"Normalize {section_name}.{k}: '{v}' -> '{nv}'")
                if lm and isinstance(v, str) and len(v) > 0:
                    suggestion, _ = lm.suggest_closest(k, v)
                    if suggestion != v:
                        issues.append(f"Suggest {section_name}.{k}: '{v}' -> '{suggestion}' (mini-LM)")
                if llm_url and isinstance(v, str) and len(v) > 0:
                    js = suggest_with_llm(section_name, k, v, llm_url)
                    if js:
                        if js.get('normalize') and js['normalize'] != v:
                            issues.append(f"LLM Normalize {section_name}.{k}: '{v}' -> '{js['normalize']}'")
                        if js.get('suggest') and js['suggest'] != v:
                            issues.append(f"LLM Suggest {section_name}.{k}: '{v}' -> '{js['suggest']}'")
        return issues


def load_schema(schema_path: str | Path) -> SimpleSchema:
    data = json.loads(Path(schema_path).read_text(encoding='utf-8'))
    return SimpleSchema(allowed_keys=data.get('allowed_keys', {}))


def save_schema(schema: SimpleSchema, schema_path: str | Path) -> None:
    Path(schema_path).write_text(
        json.dumps({'allowed_keys': schema.allowed}, indent=2),
        encoding='utf-8'
    )


def build_schema_from_training(dataset_path: str | Path) -> SimpleSchema:
    dataset = json.loads(Path(dataset_path).read_text(encoding='utf-8'))
    allowed: Dict[str, set] = {
        'title': set(),
        'specifics': set(),
        'metadata': set(),
        'description': set(),
        'table_shared': set(),
        'table_entry_union': set(),
    }
    for ex in dataset:
        keys = ex.get('keys', {})
        for sec in allowed:
            for k in keys.get(sec, []):
                allowed[sec].add(k)
    allowed_lists = {sec: sorted(list(vals)) for sec, vals in allowed.items()}
    return SimpleSchema(allowed_keys=allowed_lists)


def validate_file(parsed_txt_path: str | Path, schema_path: str | Path, lm_path: str | Path | None = None, llm_url: str | None = None, web_verify: bool = False) -> List[str]:
    listing, _ = read_parsed_txt(parsed_txt_path)
    listing_dict = {
        'title': listing.title,
        'specifics': listing.specifics,
        'metadata': listing.metadata,
        'description': listing.description,
        'table_shared': listing.table_shared,
    }
    instructions = load_instructions()
    schema = load_schema(schema_path)
    lm = NgramLM.load(lm_path) if lm_path and Path(lm_path).exists() else None
    # Overwrite feature toggles from instructions if present
    enable_llm = instructions.get('validator', {}).get('enable_llm_suggest', True)
    enable_web = False
    # Layering: deterministic schema/mini-LM first, then (optional) LLM value suggestions; web verify disabled
    issues = schema.validate(listing_dict, lm, llm_url if enable_llm else None)
    # Web verification disabled globally
    return issues


def main():
    import argparse
    ap = argparse.ArgumentParser(description='AI-lite validator for parsed files')
    sub = ap.add_subparsers(dest='cmd', required=True)

    b = sub.add_parser('build-schema', help='Build schema from training dataset')
    b.add_argument('--dataset', default='training/training_dataset.json')
    b.add_argument('--out', default='training/schema.json')

    v = sub.add_parser('validate', help='Validate a parsed txt against schema')
    v.add_argument('parsed_txt')
    v.add_argument('--schema', default='training/schema.json')
    v.add_argument('--lm', default='training/mini_lm.json')
    v.add_argument('--llm-url', default=None, help='Optional llama.cpp server base URL (e.g., http://127.0.0.1:8080)')
    v.add_argument('--web-verify', action='store_true', help='Use web verifier with LLM extraction for missing/wrong keys')

    lmp = sub.add_parser('build-lm', help='Build mini-LLM (n-gram) from training dataset')
    lmp.add_argument('--dataset', default='training/training_dataset.json')
    lmp.add_argument('--out', default='training/mini_lm.json')

    args = ap.parse_args()
    if args.cmd == 'build-schema':
        schema = build_schema_from_training(args.dataset)
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        save_schema(schema, args.out)
        print(f"Schema saved to {args.out}")
    elif args.cmd == 'validate':
        issues = validate_file(args.parsed_txt, args.schema, args.lm, args.llm_url, args.web_verify)
        if issues:
            print('\n'.join(issues))
        else:
            print('No schema issues found')
    elif args.cmd == 'build-lm':
        lm = NgramLM(n=3)
        lm.fit(args.dataset)
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        lm.save(args.out)
        print(f"Mini-LM saved to {args.out}")


if __name__ == '__main__':
    main()


