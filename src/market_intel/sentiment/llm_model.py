from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple


@dataclass
class LLMSentiment:
    api_key_env: str
    model: str = "gpt-4o-mini"

    def score(self, text: str) -> Tuple[float, float]:
        # Stub: keeps pipeline stable by returning neutral if not configured.
        if not os.getenv(self.api_key_env, ""):
            return 0.0, 0.3
        return 0.0, 0.3
