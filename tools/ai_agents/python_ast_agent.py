from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import List

from .common import find_paths, atomic_write_text, backup_file, json_print


class SymbolRenamer(ast.NodeTransformer):
    def __init__(self, old: str, new: str) -> None:
        self.old = old
        self.new = new
        self.changes = 0

    def visit_Name(self, node: ast.Name):
        if node.id == self.old:
            node.id = self.new
            self.changes += 1
        return self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        node = self.generic_visit(node)
        if isinstance(node.attr, str) and node.attr == self.old:
            node.attr = self.new
            self.changes += 1
        return node


def rename_symbol_in_files(root: Path, include: List[str], old: str, new: str, dry_run: bool) -> dict:
    files = find_paths(root, include)
    modified = 0
    total_changes = 0
    details = []
    for fp in files:
        if not fp.is_file():
            continue
        try:
            src = fp.read_text(encoding='utf-8', errors='ignore')
            tree = ast.parse(src)
            renamer = SymbolRenamer(old, new)
            new_tree = renamer.visit(tree)
            if renamer.changes > 0:
                new_src = ast.unparse(new_tree) if hasattr(ast, 'unparse') else src
                if not dry_run and new_src != src:
                    backup_file(fp)
                    atomic_write_text(fp, new_src)
                modified += 1
                total_changes += renamer.changes
                details.append({'file': str(fp), 'changes': renamer.changes})
        except Exception:
            continue
    return {
        'files_examined': len(files),
        'files_modified': modified,
        'modifications': total_changes,
        'details': details,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description='Python AST Agent')
    ap.add_argument('--root', default='.')
    ap.add_argument('--include', action='append', required=True)
    ap.add_argument('--rename-symbol', nargs=2, metavar=('OLD', 'NEW'))
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if not args.rename_symbol:
        ap.error('No action specified. Use --rename-symbol OLD NEW')

    old, new = args.rename_symbol
    result = rename_symbol_in_files(Path(args.root), args.include, old, new, args.dry_run)
    json_print(result)


if __name__ == '__main__':
    main()


