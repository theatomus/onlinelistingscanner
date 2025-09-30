from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional


def atomic_write_text(target: Path, content: str, encoding: str = 'utf-8') -> None:
    tmp = target.with_suffix(target.suffix + '.tmp')
    target.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open('w', encoding=encoding) as f:
        f.write(content)
    os.replace(tmp, target)


def backup_file(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    backup = path.with_suffix(path.suffix + '.bak')
    shutil.copy2(path, backup)
    return backup


def find_paths(root: Path, include: Iterable[str], exclude: Iterable[str] = ()) -> List[Path]:
    root = root.resolve()
    results: List[Path] = []
    include = list(include)
    exclude = set(exclude)
    for pattern in include:
        for p in root.rglob(pattern):
            if any(p.match(ex) for ex in exclude):
                continue
            results.append(p)
    # De-duplicate while preserving order
    seen = set()
    unique: List[Path] = []
    for p in results:
        if p in seen:
            continue
        seen.add(p)
        unique.append(p)
    return unique


def json_print(data: dict) -> None:
    print(json.dumps(data, ensure_ascii=False))


@dataclass
class EditSummary:
    files_examined: int
    files_modified: int
    modifications: int
    details: List[dict]


def read_json(path: Path, default: dict | list | None = None):
    if not path.exists():
        return {} if default is None else default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {} if default is None else default


def write_json(path: Path, data) -> None:
    text = json.dumps(data, ensure_ascii=False, indent=2)
    atomic_write_text(path, text + "\n")


