"""统一缓存层：单一目录 .v100_cache/，按命名空间+TTL管理"""
import json
import time
import hashlib
import logging
from pathlib import Path
from typing import Any, Optional

CACHE_DIR = Path(__file__).parent.parent / ".v100_cache"
CACHE_DIR.mkdir(exist_ok=True)

TTL = {
    "price":       15 * 60,
    "fundamentals": 3600,
    "news":        15 * 60,
    "ai_report":   12 * 3600,
    "market_heat": 5 * 60,
    "stock_list":  15 * 60,
}


def _key(namespace: str, ident: str) -> str:
    raw = f"{namespace}:{ident}"
    return hashlib.md5(raw.encode()).hexdigest()


def get(namespace: str, ident: str) -> Optional[Any]:
    path = CACHE_DIR / f"{_key(namespace, ident)}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ttl = TTL.get(namespace, 300)
        if time.time() - data["ts"] < ttl:
            return data["payload"]
        path.unlink(missing_ok=True)
    except Exception as e:
        logging.debug(f"cache.get error: {e}")
    return None


def set(namespace: str, ident: str, payload: Any) -> None:
    path = CACHE_DIR / f"{_key(namespace, ident)}.json"
    try:
        path.write_text(
            json.dumps({"ts": time.time(), "payload": payload}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logging.warning(f"cache.set error ({namespace}/{ident}): {e}")


def invalidate(namespace: str, ident: str) -> None:
    path = CACHE_DIR / f"{_key(namespace, ident)}.json"
    path.unlink(missing_ok=True)


def clear_expired() -> int:
    count = 0
    for path in CACHE_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            ns_hint = data.get("ns", "price")
            ttl = TTL.get(ns_hint, 300)
            if time.time() - data["ts"] >= ttl:
                path.unlink()
                count += 1
        except Exception:
            path.unlink(missing_ok=True)
            count += 1
    return count
