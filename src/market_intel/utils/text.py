from __future__ import annotations

import hashlib
from typing import Any


def stable_digest(*parts: Any) -> str:
    s = "|".join([str(p or "") for p in parts])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
