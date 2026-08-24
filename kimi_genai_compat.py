"""Compatibility bridge that routes legacy Gemini-shaped V88 calls to Kimi K3.

Several long-lived V88 modules still expose historical ``Gemini`` function
names.  Keeping those names avoids a broad UI/serialization migration, while
this module guarantees that every executable call uses the user's Kimi Code
subscription and the configured ``k3-256k`` model.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kimi_subscription import api_key, complete, model_name

_configured_key = ""


def configure(*, api_key: str = "", **_: Any) -> None:
    global _configured_key
    _configured_key = str(api_key or "").strip()


def _prompt_text(contents: Any) -> str:
    if isinstance(contents, str):
        return contents
    if isinstance(contents, list):
        parts: list[str] = []
        for item in contents:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                value = item.get("text") or item.get("content")
                if value:
                    parts.append(str(value))
        return "\n".join(parts)
    return str(contents or "")


def _max_tokens(config: Any) -> int:
    if isinstance(config, dict):
        return int(config.get("maxOutputTokens") or config.get("max_output_tokens") or 4096)
    return int(getattr(config, "max_output_tokens", 4096) or 4096)


@dataclass
class _Response:
    text: str
    candidates: tuple = ()


class GenerativeModel:
    """Small subset of the old Gemini SDK used by V88."""

    def __init__(self, _legacy_model: str | None = None, **_: Any):
        self.model = model_name()

    def generate_content(self, contents: Any, **kwargs: Any) -> _Response:
        request_options = kwargs.get("request_options") or {}
        timeout = int(request_options.get("timeout") or 150) if isinstance(request_options, dict) else 150
        text, _body = complete(
            _prompt_text(contents),
            key=api_key(_configured_key),
            model=self.model,
            temperature=1,
            reasoning_effort="high",
            max_tokens=_max_tokens(kwargs.get("generation_config")),
            timeout=max(timeout, 30),
        )
        return _Response(text=text)


class _Models:
    def __init__(self, key: str):
        self.key = key

    def generate_content(self, *, model: str | None = None, contents: Any = "", config: Any = None, **kwargs: Any) -> _Response:
        configure(api_key=self.key)
        return GenerativeModel(model).generate_content(contents, generation_config=config, **kwargs)


class Client:
    def __init__(self, api_key: str = "", **_: Any):
        self.models = _Models(api_key)

