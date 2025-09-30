from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict

from .training_data_builder import build_training_examples, save_training_dataset
from .ai_validator import build_schema_from_training, save_schema, validate_file
from .mini_lm import NgramLM


def run(items_dir: str = 'item_contents', out_dir: str = 'training', backups_root: str = 'backups/itemcontents') -> Dict[str, List[str]]:
    out_base = Path(out_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    # 1) Build dataset
    examples = build_training_examples(items_dir, backups_root)
    dataset_path = out_base / 'training_dataset.json'
    save_training_dataset(examples, dataset_path)

    # 2) Build schema from dataset
    schema = build_schema_from_training(dataset_path)
    schema_path = out_base / 'schema.json'
    save_schema(schema, schema_path)

    # 3) Build mini-LM (n-gram) from dataset
    lm = NgramLM(n=3)
    lm.fit(dataset_path)
    lm_path = out_base / 'mini_lm.json'
    lm.save(lm_path)

    # 4) Validate all files with schema + mini-LM
    results: Dict[str, List[str]] = {}
    for fp in sorted(Path(items_dir).glob('python_parsed_*.txt')):
        item = fp.name.replace('python_parsed_', '').replace('.txt', '')
        issues = validate_file(fp, schema_path, lm_path)
        if issues:
            results[item] = issues

    # 5) Save report
    (out_base / 'reports').mkdir(exist_ok=True)
    report_txt = out_base / 'reports' / 'validation_report.txt'
    report_json = out_base / 'reports' / 'validation_report.json'

    with report_txt.open('w', encoding='utf-8') as f:
        if not results:
            f.write('No schema issues found across all items.\n')
        else:
            for item, issues in results.items():
                f.write(f'ITEM={item}\n')
                for issue in issues:
                    f.write(f'  - {issue}\n')
                f.write('\n')

    report_json.write_text(json.dumps(results, indent=2), encoding='utf-8')
    return results


def main():
    import argparse
    ap = argparse.ArgumentParser(description='End-to-end training + validation workflow')
    ap.add_argument('--items-dir', default='item_contents')
    ap.add_argument('--out-dir', default='training')
    ap.add_argument('--backups-root', default='backups/itemcontents')
    args = ap.parse_args()

    results = run(args.items_dir, args.out_dir, args.backups_root)
    if results:
        print(f"Found schema issues in {len(results)} item(s). See {args.out_dir}/reports/validation_report.txt")
    else:
        print("No schema issues found.")


if __name__ == '__main__':
    main()


