"""Kimi Code subscription client shared by V88 runtime modules.

The client intentionally accepts only membership keys (``sk-kimi-``) and the
Kimi Code endpoint so a Moonshot Open Platform key cannot silently re-enable
pay-as-you-go billing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

import requests

DEFAULT_BASE_URL = "https://api.kimi.com/coding/v1"
DEFAULT_MODEL = "k3-256k"


def _provider_key_from(path: Path) -> str:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return ""
    providers = ((data.get("models") or {}).get("providers") or {})
    preferred = providers.get("kimi-coding") or {}
    candidates = [preferred, *providers.values()]
    for provider in candidates:
        if not isinstance(provider, dict):
            continue
        key = str(provider.get("apiKey") or "").strip()
        base = str(provider.get("baseUrl") or provider.get("baseURL") or "").lower()
        if key.startswith("sk-kimi-") and (not base or "kimi.com/coding" in base or "agent-gw.kimi.com/coding" in base):
            return key
    return ""


def api_key(explicit: str | None = None) -> str:
    """Resolve a Kimi Code membership key without accepting platform API keys."""
    if os.getenv("V88_DISABLE_LLM") == "1":
        return ""
    candidates = [
        explicit,
        os.getenv("KIMI_CODE_API_KEY"),
        _provider_key_from(Path.home() / ".kimi" / "kimi-claw" / "openclaw.json"),
        _provider_key_from(Path.home() / ".openclaw" / "openclaw.json"),
    ]
    for value in candidates:
        key = str(value or "").strip()
        if key.startswith("sk-kimi-"):
            return key
    return ""


def configured(explicit: str | None = None) -> bool:
    return bool(api_key(explicit))


def model_name() -> str:
    return str(os.getenv("V88_KIMI_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts).strip()
    return ""


def message_text(response: dict) -> str:
    choices = response.get("choices") or []
    if not choices:
        return ""
    return _content_text((choices[0].get("message") or {}).get("content"))


def chat_completion(
    messages: Iterable[dict],
    *,
    key: str | None = None,
    model: str | None = None,
    max_tokens: int = 4096,
    temperature: float | None = None,
    reasoning_effort: str = "high",
    response_format: dict | None = None,
    timeout: int = 120,
) -> dict:
    membership_key = api_key(key)
    if not membership_key:
        raise RuntimeError("未配置Kimi Code订阅密钥（需要KIMI_CODE_API_KEY或本机Kimi Claw登录）")
    base = str(os.getenv("KIMI_CODE_BASE_URL", DEFAULT_BASE_URL) or DEFAULT_BASE_URL).rstrip("/")
    payload: dict[str, Any] = {
        "model": model or model_name(),
        "messages": list(messages),
        "max_tokens": int(max_tokens),
        "reasoning_effort": reasoning_effort,
        "stream": False,
    }
    if temperature is not None:
        # K3 currently accepts only temperature=1. Keep the legacy caller
        # argument for API compatibility, but normalize it at the boundary.
        payload["temperature"] = 1
    if response_format:
        payload["response_format"] = response_format
    response = requests.post(
        f"{base}/chat/completions",
        headers={"Authorization": f"Bearer {membership_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    try:
        body = response.json()
    except Exception as exc:
        raise RuntimeError(f"Kimi订阅接口返回非JSON（HTTP {response.status_code}）") from exc
    if response.status_code != 200 or body.get("error"):
        error = body.get("error") or {}
        message = error.get("message") if isinstance(error, dict) else str(error)
        raise RuntimeError(f"Kimi订阅接口HTTP {response.status_code}: {str(message or body)[:240]}")
    return body


def complete(prompt: str, **kwargs: Any) -> tuple[str, dict]:
    body = chat_completion([{"role": "user", "content": str(prompt)}], **kwargs)
    text = message_text(body)
    if not text:
        raise RuntimeError("Kimi K3-256K未返回正文")
    return text, body
