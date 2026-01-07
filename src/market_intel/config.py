from __future__ import annotations

from typing import Any, Dict

import yaml


class Cfg:
    def __init__(self, raw: Dict[str, Any]) -> None:
        self.raw = raw

    def get(self, path: str, default: Any = None) -> Any:
        cur: Any = self.raw
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    def as_dict(self) -> Dict[str, Any]:
        return self.raw


def load_config(path: str) -> Cfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return Cfg(raw)
