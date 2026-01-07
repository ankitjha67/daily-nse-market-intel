from __future__ import annotations

import logging
import requests_cache

log = logging.getLogger(__name__)


def install_cache(cache_name: str = ".cache/http_cache", backend: str = "sqlite", expire_after: int = 3600) -> None:
    """Install requests-cache with stable signature (supports expire_after)."""
    requests_cache.install_cache(cache_name=cache_name, backend=backend, expire_after=expire_after)
    log.info("HTTP cache installed: name=%s backend=%s expire_after=%s", cache_name, backend, expire_after)
