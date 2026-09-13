"""GPT/Codex subscription adapter for non-trading V88 text tasks.

This module replaces the retired narrative providers.  It never
creates prices, financial facts, scores or trade actions; callers must give it
already-frozen public facts.  Failure returns ``None`` so deterministic modules
continue unchanged and no other model silently signs for GPT.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

MODEL = "gpt-6-astra"


def _binary() -> str:
    bundled = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
    if os.name != "nt" and bundled.is_file():
        return str(bundled)
    if os.name == "nt":
        shim = shutil.which("codex.cmd")
        if shim:
            try:
                vendor_root = (Path(shim).parent / "node_modules" / "@openai" /
                               "codex" / "node_modules")
                matches = list(vendor_root.glob(
                    "@openai/codex-win32-*/vendor/*/bin/codex.exe"))
                if matches:
                    return str(matches[0])
            except (NotImplementedError, OSError):
                return shim
        for name in ("codex.exe", "codex"):
            found = shutil.which(name)
            if found:
                return found
        return "codex.exe"
    resolved = shutil.which("codex")
    if resolved:
        return resolved
    candidates = sorted((Path.home() / ".nvm" / "versions" / "node").glob("*/bin/codex"), reverse=True)
    return str(candidates[0]) if candidates else str(Path.home() / ".local" / "bin" / "codex")


def configured() -> bool:
    if os.getenv("V88_DISABLE_LLM") == "1":
        return False
    binary = _binary()
    return bool(Path(binary).exists() or shutil.which(binary))


def model_name() -> str:
    return MODEL


def _subscription_env() -> dict[str, str]:
    """Return an OAuth-only environment for the ChatGPT-bound Codex CLI.

    Codex reads the user's ChatGPT subscription OAuth session from its local
    auth store.  API keys, bearer-token overrides and custom API endpoints
    would silently reroute the same command to metered or third-party service,
    so none of them may cross this subprocess boundary.
    """
    blocked_exact = {
        "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY",
        "MOONSHOT_API_KEY", "KIMI_API_KEY", "OPENAI_BASE_URL",
        "AZURE_OPENAI_ENDPOINT", "OPENAI_API_BASE",
    }
    blocked_parts = (
        "API_KEY", "ACCESS_TOKEN", "AUTH_TOKEN", "BASE_URL",
        "API_BASE", "ENDPOINT",
    )
    return {
        key: value for key, value in os.environ.items()
        if key.upper() not in blocked_exact
        and not any(part in key.upper() for part in blocked_parts)
    }


def run_prompt(prompt: str, *, reasoning_effort: str = "medium",
               timeout: int = 420, schema: dict | None = None) -> str | None:
    """Run one isolated, read-only Codex request using the existing subscription."""
    if not configured():
        return None
    effort = reasoning_effort if reasoning_effort in {"low", "medium", "high", "xhigh"} else "medium"
    try:
        with tempfile.TemporaryDirectory(prefix="v88-gpt-text-") as isolated:
            result_path = Path(isolated) / "final.txt"
            cmd = [_binary(), "exec", "--skip-git-repo-check", "--sandbox", "read-only",
                   "--ephemeral", "--ignore-user-config", "--ignore-rules",
                   "--output-last-message", str(result_path), "--color", "never",
                   "--model", MODEL, "-c", f"model_reasoning_effort={effort}"]
            if schema is not None:
                schema_path = Path(isolated) / "schema.json"
                schema_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
                cmd.extend(["--output-schema", str(schema_path)])
            cmd.append(str(prompt))
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                                  stdin=subprocess.DEVNULL, cwd=isolated,
                                  env=_subscription_env())
            if proc.returncode != 0:
                return None
            text = (result_path.read_text(encoding="utf-8")
                    if result_path.exists() else str(proc.stdout or ""))
            return text.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def api_key(explicit=None):
    """Legacy SDK compatibility: a readiness marker, never an API credential."""
    return "codex-subscription" if configured() else ""

def message_text(response):
    rows = response.get("choices") or []
    return str((rows[0].get("message") or {}).get("content") or "") if rows else ""

def chat_completion(messages, *, key=None, model=None, max_tokens=4096,
                    temperature=None, reasoning_effort="high", response_format=None,
                    timeout=420):
    if model and model != MODEL:
        raise ValueError("V88仅允许gpt-6-astra复核，无备用模型")
    prompt = "\n".join(str(m.get("content") or "") for m in messages)
    if response_format:
        prompt += "\n返回严格JSON对象，不要Markdown围栏。"
    prompt = ("只使用用户提供的事实；不要调用工具或读取工作区。不要编造行情或实时消息。"
              "分析不是正式交易认证；行动资格另由中央GPT-6+经典巨著闸核验。\n" + prompt)
    text = run_prompt(prompt, reasoning_effort=reasoning_effort, timeout=timeout)
    if not text:
        raise RuntimeError("GPT-6订阅复核未完成；请检查认证或额度")
    return {"model": MODEL, "choices": [{"message": {"content": text}}],
            "usage": {}, "usage_available": False}

def complete(prompt, **kwargs):
    body = chat_completion([{"role": "user", "content": str(prompt)}], **kwargs)
    return message_text(body), body
