from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


class NgramLM:
    """
    A tiny character-level n-gram language model over key and value strings.
    It provides perplexity-like scores and can suggest the closest known form
    for values seen historically (e.g., formatting corrections).
    """

    def __init__(self, n: int = 3):
        self.n = n
        self.counts: Dict[str, int] = defaultdict(int)
        self.context_counts: Dict[str, int] = defaultdict(int)
        self.known_values: Dict[str, List[str]] = defaultdict(list)

    def _tokens(self, text: str) -> List[str]:
        s = f"{'^'*(self.n-1)}{text}$"
        return [s[i:i+self.n] for i in range(len(s) - self.n + 1)]

    def fit_value(self, key_name: str, value: str) -> None:
        if not isinstance(value, str) or not value:
            return
        for gram in self._tokens(value):
            ctx, ch = gram[:-1], gram[-1]
            self.counts[gram] += 1
            self.context_counts[ctx] += 1
        # Track canonical forms per key
        if len(self.known_values[key_name]) < 1000:
            self.known_values[key_name].append(value)

    def fit(self, dataset_path: str | Path) -> None:
        data = json.loads(Path(dataset_path).read_text(encoding='utf-8'))
        for ex in data:
            values = ex.get('values', {})
            for section_name, section_vals in values.items():
                for k, v in section_vals.items():
                    key_name = k
                    self.fit_value(key_name, v)

    def score(self, text: str) -> float:
        # Negative log-likelihood with add-one smoothing
        nll = 0.0
        for gram in self._tokens(text):
            ctx, ch = gram[:-1], gram[-1]
            c = self.counts.get(gram, 0) + 1
            Z = self.context_counts.get(ctx, 0) + 256  # rough alphabet size
            nll += -1.0 * (c / Z)
        return nll

    def suggest_closest(self, key_name: str, value: str) -> Tuple[str, float]:
        """
        Suggest the historically closest form for this key based on simple edit-distance
        and LM score mixture.
        """
        candidates = self.known_values.get(key_name, [])
        if not candidates:
            return value, self.score(value)

        def edit_distance(a: str, b: str) -> int:
            dp = [[i + j if i * j == 0 else 0 for j in range(len(b) + 1)] for i in range(len(a) + 1)]
            for i in range(1, len(a) + 1):
                for j in range(1, len(b) + 1):
                    dp[i][j] = min(
                        dp[i - 1][j] + 1,
                        dp[i][j - 1] + 1,
                        dp[i - 1][j - 1] + (a[i - 1] != b[j - 1])
                    )
            return dp[-1][-1]

        best = (value, float('inf'))
        for cand in candidates:
            d = edit_distance(value, cand)
            # Mix distance with LM score of candidate to prefer well-formed strings
            score = d + 0.1 * self.score(cand)
            if score < best[1]:
                best = (cand, score)
        return best

    def save(self, out_path: str | Path) -> None:
        payload = {
            'n': self.n,
            'counts': dict(self.counts),
            'context_counts': dict(self.context_counts),
            'known_values': dict(self.known_values),
        }
        Path(out_path).write_text(json.dumps(payload), encoding='utf-8')

    @staticmethod
    def load(path: str | Path) -> 'NgramLM':
        data = json.loads(Path(path).read_text(encoding='utf-8'))
        lm = NgramLM(n=data.get('n', 3))
        lm.counts.update(data.get('counts', {}))
        lm.context_counts.update(data.get('context_counts', {}))
        kv = data.get('known_values', {})
        lm.known_values.update({k: list(v) for k, v in kv.items()})
        return lm


