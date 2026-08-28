#!/usr/bin/env python3
"""Read-only, subscription-safe OpenClaw service checks (Python stdlib only).

``--state-dir`` is the monitor's own output directory, not OpenClaw's home.
``--openclaw-home`` optionally pins config, credentials and child CLI to one home.
No model is invoked: GPT remains UNTESTED and end-to-end remains UNVERIFIED.
``--probe-model`` is deliberately unsupported and exits 2 without running a CLI.

Notifications are opt-in, direct Feishu API messages to exactly one already-paired
user in the selected account's allowFrom file. They contain fixed service-status
phrases only. No login, restart, configuration change, fallback, or trading action
is performed. Failed/ambiguous delivery is never recorded as notified.

Exit codes: 0 = channel/config checks passed (NOT end-to-end verified),
1 = degraded/unknown or notification failure, 2 = invalid/unsupported arguments,
3 = monitor-state failure, 75 = another monitor owns the state lock.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Callable


SCHEMA = "v88.openclaw.ops.v1"
LABEL = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}\Z")
USER_ID = re.compile(r"ou_[A-Za-z0-9_-]{8,128}\Z")
DIGEST = re.compile(r"[a-f0-9]{64}\Z")
MAX_BYTES = 2_000_000
CLI_TIMEOUT = 20
NOTICE_RETRY_SECONDS = 300
KNOWN_GPT_MODELS = frozenset({"openai/gpt-5.6-sol", "openai-codex/gpt-5.6-sol"})


class OpsError(Exception):
    """Only a fixed, non-sensitive code is allowed to leave this exception."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def timestamp(now: float) -> str:
    return datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink() or path.stat().st_size > MAX_BYTES:
            raise OpsError("FILE_UNSAFE")
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(value, dict):
            raise OpsError("FILE_INVALID")
        return value
    except FileNotFoundError:
        raise OpsError("FILE_MISSING") from None
    except (OSError, UnicodeError, ValueError):
        raise OpsError("FILE_INVALID") from None


def safe_json_object(output: str) -> dict[str, Any] | None:
    """Accept JSON with CLI banner noise, but never echo that noise."""
    if len(output.encode("utf-8", errors="replace")) > MAX_BYTES:
        return None
    decoder = json.JSONDecoder()
    positions = [m.start() for m in re.finditer(r"\{", output)][:256]
    for position in positions:
        try:
            value, end = decoder.raw_decode(output, position)
        except ValueError:
            continue
        if isinstance(value, dict) and not output[end:].strip():
            return value
    return None


def resolve_cli(
    which: Callable[[str], str | None] = shutil.which,
    windows: bool | None = None,
) -> list[str]:
    """Never pass an npm .cmd/.bat shim or command string to a shell.

    On Windows, invoke the same installation's JS entry with node.exe. Unknown
    wrappers fail closed instead of applying fragile cmd.exe escaping.
    """
    windows = os.name == "nt" if windows is None else windows
    location = which("openclaw")
    if not location:
        raise OpsError("CLI_NOT_FOUND")
    cli = Path(location)
    if cli.suffix.lower() not in {".cmd", ".bat"}:
        return [str(cli)]
    if not windows:
        raise OpsError("CLI_WRAPPER_UNSUPPORTED")
    bases = [cli.parent / "node_modules" / "openclaw"]
    if cli.parent.name == ".bin":
        bases.append(cli.parent.parent / "openclaw")
    entries = [base / name for base in bases for name in ("openclaw.mjs", "dist/index.js")]
    entry = next((p for p in entries if p.is_file()), None)
    node = cli.parent / "node.exe"
    if not node.is_file():
        found = which("node.exe") or which("node")
        node = Path(found) if found else Path()
    if entry is None or not node.is_file() or node.suffix.lower() not in {".exe", ".com"}:
        raise OpsError("CLI_WRAPPER_UNSUPPORTED")
    return [str(node), str(entry)]


def run_cli(argv: list[str], env: dict[str, str], timeout: int = CLI_TIMEOUT) -> tuple[str, dict[str, Any] | None]:
    try:
        process = subprocess.run(
            argv, shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, encoding="utf-8", errors="replace", env=env,
            timeout=timeout, check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
        )
    except subprocess.TimeoutExpired:
        return "CLI_TIMEOUT", None
    except (OSError, ValueError):
        return "CLI_EXECUTION_FAILED", None
    if process.returncode != 0:
        return "CLI_FAILED", None
    value = safe_json_object(process.stdout)
    return ("OK", value) if value is not None else ("CLI_OUTPUT_INVALID", None)


def boolean(value: Any) -> bool | None:
    return value if type(value) is bool else None


def channel_layers(code: str, payload: dict[str, Any] | None, account: str, now: float) -> dict[str, Any]:
    result: dict[str, Any] = {
        "gateway": {"status": "UNKNOWN", "reason": code},
        "feishu": {"status": "UNKNOWN", "reason": "NO_LIVE_ACCOUNT_STATUS",
                   "enabled": None, "running": None, "connected": None, "probe_ok": None},
    }
    if code != "OK" or payload is None:
        return result
    if payload.get("gatewayReachable") is False or payload.get("configOnly") is True:
        result["gateway"] = {"status": "DOWN", "reason": "GATEWAY_RPC_UNREACHABLE"}
        return result
    ts = payload.get("ts")
    if type(ts) not in (int, float) or not math.isfinite(ts) or abs(ts / 1000 - now) > 120:
        result["gateway"]["reason"] = "LIVE_SNAPSHOT_UNVERIFIED"
        return result
    accounts = payload.get("channelAccounts")
    channels = payload.get("channels")
    if not isinstance(accounts, dict) or not isinstance(channels, dict) or "feishu" not in channels:
        result["gateway"]["reason"] = "LIVE_RPC_SCHEMA_UNKNOWN"
        return result
    result["gateway"] = {"status": "UP", "reason": "LIVE_CHANNEL_RPC_RESPONSE"}
    candidates = accounts.get("feishu")
    if not isinstance(candidates, list):
        result["feishu"]["reason"] = "ACCOUNT_STATUS_UNKNOWN"
        return result
    matches = [x for x in candidates if isinstance(x, dict) and x.get("accountId") == account]
    if len(matches) != 1:
        result["feishu"]["reason"] = "ACCOUNT_NOT_FOUND" if not matches else "ACCOUNT_AMBIGUOUS"
        return result
    selected = matches[0]
    probe = selected.get("probe")
    layer = result["feishu"]
    layer.update({key: boolean(selected.get(key)) for key in ("enabled", "running", "connected")})
    layer["probe_ok"] = boolean(probe.get("ok")) if isinstance(probe, dict) else None
    checks = [
        ("enabled", "DISABLED", "ACCOUNT_DISABLED"),
        ("running", "STOPPED", "ACCOUNT_NOT_RUNNING"),
        ("connected", "DISCONNECTED", "CHANNEL_NOT_CONNECTED"),
        ("probe_ok", "ERROR", "CHANNEL_PROBE_FAILED"),
    ]
    for key, status, reason in checks:
        if layer[key] is False:
            layer.update(status=status, reason=reason)
            return result
    if selected.get("configured") is False:
        layer.update(status="ERROR", reason="ACCOUNT_NOT_CONFIGURED")
    elif all(layer[key] is True for key, _, _ in checks) and selected.get("configured") is True:
        layer.update(status="READY", reason="CHANNEL_READY_NOT_MODEL_VERIFIED")
    else:
        layer.update(status="UNKNOWN", reason="CHANNEL_STATUS_INCOMPLETE")
    return result


def model_layers(config: dict[str, Any] | None, home: Path, agent: str, account: str, env: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "model_policy": {"status": "UNKNOWN", "authentication": "UNKNOWN",
                         "configured_model": "UNKNOWN", "fallbacks": "UNKNOWN",
                         "reason": "CONFIG_METADATA_UNAVAILABLE"},
        "routing": {"status": "UNKNOWN", "reason": "BINDING_UNVERIFIED"},
        "gpt": {"status": "UNTESTED", "reason": "MODEL_NOT_INVOKED"},
        "business": {"status": "BUSINESS_PENDING", "blocks_channel_check": False},
    }
    if config is None:
        return result
    bindings = config.get("bindings", [])
    if isinstance(bindings, list):
        relevant = []
        for binding in bindings:
            if not isinstance(binding, dict) or not isinstance(binding.get("match"), dict):
                continue
            match = binding["match"]
            if match.get("channel") == "feishu" and match.get("accountId") in (account, "*", None):
                relevant.append(binding)
        if len(relevant) == 1 and set(relevant[0]["match"]).issubset({"channel", "accountId"}):
            ok = relevant[0].get("agentId") == agent
            result["routing"] = {"status": "MATCH" if ok else "MISMATCH",
                                 "reason": "EXPLICIT_ROUTE_MATCH" if ok else "AGENT_ROUTE_MISMATCH"}
    agents = config.get("agents", {})
    if not isinstance(agents, dict):
        return result
    agent_list = agents.get("list", [])
    selected = [x for x in agent_list if isinstance(x, dict) and x.get("id") == agent] if isinstance(agent_list, list) else []
    policy = result["model_policy"]
    if len(selected) != 1:
        policy.update(status="BLOCKED", reason="AGENT_NOT_CONFIGURED")
        return result
    defaults = agents.get("defaults", {})
    model = selected[0].get("model", defaults.get("model") if isinstance(defaults, dict) else None)
    if not isinstance(model, dict):
        policy.update(reason="MODEL_POLICY_INCOMPLETE")
        return result
    primary = model.get("primary")
    known_model = isinstance(primary, str) and primary in KNOWN_GPT_MODELS
    policy["configured_model"] = primary if known_model else "OTHER_OR_UNKNOWN"
    fallback = model.get("fallbacks")
    policy["fallbacks"] = "NONE" if fallback == [] else "PRESENT" if isinstance(fallback, list) else "UNKNOWN"
    provider = primary.split("/", 1)[0] if known_model else None
    if provider is None:
        policy.update(status="BLOCKED", reason="UNAPPROVED_MODEL_METADATA")
        return result
    auth_modes: set[str] = set()
    auth = config.get("auth", {})
    profiles = auth.get("profiles", {}) if isinstance(auth, dict) else {}
    if isinstance(profiles, dict):
        for value in profiles.values():
            if isinstance(value, dict) and value.get("provider") == provider:
                mode = value.get("mode", value.get("type"))
                if isinstance(mode, str):
                    auth_modes.add(mode.lower())
    # A selected agent's legacy auth store can supply metadata. Modern opaque
    # stores are intentionally not guessed at or dumped: UNKNOWN is honest.
    try:
        store = read_json(home / "agents" / agent / "agent" / "auth-profiles.json")
        profiles = store.get("profiles", {})
        if isinstance(profiles, dict):
            for value in profiles.values():
                if isinstance(value, dict) and value.get("provider") == provider:
                    mode = value.get("type", value.get("mode"))
                    if isinstance(mode, str):
                        auth_modes.add(mode.lower())
    except OpsError:
        pass
    providers = config.get("models", {})
    providers = providers.get("providers", {}) if isinstance(providers, dict) else {}
    provider_config = providers.get(provider, {}) if isinstance(providers, dict) else {}
    has_api_key = bool(env.get("OPENAI_API_KEY")) or (isinstance(provider_config, dict) and bool(provider_config.get("apiKey")))
    if has_api_key or auth_modes.intersection({"api_key", "api-key", "token"}):
        policy["authentication"] = "NON_OAUTH"
    elif auth_modes == {"oauth"}:
        policy["authentication"] = "OAUTH"
    if policy["fallbacks"] == "PRESENT" or policy["authentication"] == "NON_OAUTH":
        policy.update(status="BLOCKED", reason="SUBSCRIPTION_ONLY_POLICY_NOT_MET")
    elif policy["fallbacks"] == "NONE" and policy["authentication"] == "OAUTH":
        policy.update(status="PASS", reason="OAUTH_METADATA_ONLY_NOT_RUNTIME_TEST")
    else:
        policy.update(reason="AUTH_OR_FALLBACK_METADATA_UNKNOWN")
    return result


def paired_recipient(home: Path, account: str, config: dict[str, Any] | None) -> str:
    if not isinstance(config, dict):
        raise OpsError("NOTICE_CONFIG_UNKNOWN")
    channels = config.get("channels", {})
    feishu = channels.get("feishu", {}) if isinstance(channels, dict) else {}
    accounts = feishu.get("accounts", {}) if isinstance(feishu, dict) else {}
    selected = accounts.get(account, {}) if isinstance(accounts, dict) else {}
    if not isinstance(feishu, dict) or feishu.get("enabled") is not True or not isinstance(selected, dict) or selected.get("enabled") is not True:
        raise OpsError("NOTICE_ACCOUNT_NOT_ENABLED")
    try:
        store = read_json(home / "credentials" / ("feishu-" + account + "-allowFrom.json"))
    except OpsError:
        raise OpsError("NOTICE_PAIRED_USER_UNAVAILABLE") from None
    entries = store.get("allowFrom")
    if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], str) or not USER_ID.fullmatch(entries[0]):
        raise OpsError("NOTICE_REQUIRES_ONE_PAIRED_USER")
    return entries[0]


def delivery_receipt(payload: dict[str, Any] | None) -> bool:
    if not payload or payload.get("dryRun") is True or payload.get("channel") != "feishu":
        return False
    def has_id(value: Any, depth: int = 0) -> bool:
        if depth > 5 or not isinstance(value, dict):
            return False
        if any(isinstance(value.get(key), str) and value[key].startswith("om_") for key in ("messageId", "message_id")):
            return True
        return any(has_id(value.get(key), depth + 1) for key in ("payload", "result", "data", "message"))
    return has_id(payload)


def notice_text(report: dict[str, Any]) -> str:
    host = "Windows主站" if report["host_role"] == "windows-primary" else "Mac临时后端"
    gateway = "在线" if report["gateway"]["status"] == "UP" else "尚未证实在线"
    channel = {"READY": "已连接并通过通道探针", "DISABLED": "已关闭", "STOPPED": "未运行",
               "DISCONNECTED": "未连接", "ERROR": "检查失败", "UNKNOWN": "状态未知"}[report["feishu"]["status"]]
    if report["gateway"]["status"] != "UP":
        issue, action = "网关连接未验证", "检查主机在线和已有网关服务；无需修改模型或付费路线。"
    elif report["feishu"]["status"] != "READY":
        issue, action = "飞书账户连接未就绪", "核对所选机器人、账户开关和连接日志；不要重新绑定到其他账户。"
    elif report["routing"]["status"] != "MATCH":
        issue, action = "机器人与助手绑定未核实", "核对现有账户绑定与预期助手；不要自动创建或切换绑定。"
    elif report["model_policy"]["status"] != "PASS":
        issue, action = "现有订阅认证路径未确认", "检查原有OAuth认证与无付费回退配置；本检查未调用模型。"
    else:
        issue, action = "真实聊天仍待验证", "请在同一飞书机器人发一句测试；收到模型回复后再验收业务/K3会审。"
    return ("V88 服务状态变化\n检查时间（UTC）：" + report["checked_at"]
            + "\n主机：" + host + "\n网关：" + gateway + "\n飞书通道：" + channel
            + "\n问题层：" + issue + "\n下一步：" + action
            + "\nGPT：尚未做真实模型测试，不能据此宣称聊天恢复。"
            + "\n业务/K3：业务会审待完成，与基础通道检查分开。"
            + "\n本通知不包含投资建议，不代表三方推荐已经通过。")


def fingerprint(report: dict[str, Any]) -> str:
    keys = ("host_role", "account", "agent", "gateway", "feishu", "model_policy", "routing", "gpt", "business", "end_to_end")
    raw = json.dumps({key: report[key] for key in keys}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def atomic_state(path: Path, value: dict[str, Any]) -> None:
    if path.is_symlink():
        raise OpsError("STATE_UNSAFE")
    temp_name: str | None = None
    try:
        fd, temp_name = tempfile.mkstemp(prefix=".ops-", suffix=".tmp", dir=path.parent)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, 0o600)
        os.replace(temp_name, path)
        temp_name = None
    except OSError:
        raise OpsError("STATE_WRITE_FAILED") from None
    finally:
        if temp_name is not None:
            with contextlib.suppress(OSError):
                os.unlink(temp_name)


@contextlib.contextmanager
def state_lock(directory: Path, filename: str):
    if directory.is_symlink():
        raise OpsError("STATE_UNSAFE")
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock = directory / (filename + ".lock")
        if lock.is_symlink():
            raise OpsError("STATE_UNSAFE")
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except OSError:
        raise OpsError("STATE_WRITE_FAILED") from None
    locked = False
    try:
        if os.name == "nt":
            import msvcrt
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                raise OpsError("STATE_LOCK_BUSY") from None
        else:
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise OpsError("STATE_LOCK_BUSY") from None
        locked = True
        yield
    finally:
        if locked:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def previous_state(path: Path) -> dict[str, Any]:
    try:
        source = read_json(path)
    except OpsError as error:
        if error.code == "FILE_MISSING":
            return {}
        raise OpsError("STATE_INVALID") from None
    clean: dict[str, Any] = {}
    for key in ("last_notified_fingerprint", "last_attempt_fingerprint"):
        value = source.get(key)
        if isinstance(value, str) and DIGEST.fullmatch(value):
            clean[key] = value
    value = source.get("last_attempt_epoch")
    if type(value) in (int, float) and math.isfinite(value) and 0 <= value < 100_000_000_000:
        clean["last_attempt_epoch"] = value
    return clean


def check(args: argparse.Namespace, runner=run_cli, cli: list[str] | None = None, now: float | None = None) -> tuple[dict[str, Any], int]:
    now = time.time() if now is None else now
    env = dict(os.environ)
    home = Path(args.openclaw_home or env.get("OPENCLAW_STATE_DIR") or (Path.home() / ".openclaw")).expanduser().absolute()
    config_path = Path(env.get("OPENCLAW_CONFIG_PATH") or (home / "openclaw.json"))
    if args.openclaw_home:
        env["OPENCLAW_STATE_DIR"] = str(home)
        env["OPENCLAW_CONFIG_PATH"] = str(home / "openclaw.json")
        config_path = home / "openclaw.json"
    directory = Path(args.state_dir).expanduser().absolute()
    filename = "health-" + args.host_role + "-" + args.account + "-" + args.agent
    with state_lock(directory, filename):
        path = directory / (filename + ".json")
        previous = previous_state(path)
        try:
            config = read_json(config_path)
        except OpsError:
            config = None
        try:
            cli = cli or resolve_cli()
            code, payload = runner(cli + ["channels", "status", "--probe", "--json"], env, CLI_TIMEOUT)
        except OpsError as error:
            code, payload, cli = error.code, None, None
        report = {"schema": SCHEMA, "checked_at": timestamp(now), "host_role": args.host_role,
                  "account": args.account, "agent": args.agent,
                  **channel_layers(code, payload, args.account, now),
                  **model_layers(config, home, args.agent, args.account, env)}
        observed_block = (report["feishu"]["status"] in {"DISABLED", "STOPPED", "DISCONNECTED", "ERROR"}
                          or report["routing"]["status"] == "MISMATCH" or report["model_policy"]["status"] == "BLOCKED")
        report["end_to_end"] = {"status": "BLOCKED" if observed_block else "UNVERIFIED"}
        report["notification"] = {"status": "DISABLED" if not args.notify_on_change else "UNCHANGED"}
        digest = fingerprint(report)
        state = {"schema": SCHEMA, "last_checked_at": timestamp(now), "last_status": report, **previous}
        if args.notify_on_change and previous.get("last_notified_fingerprint") != digest:
            recent = previous.get("last_attempt_fingerprint") == digest and now - previous.get("last_attempt_epoch", 0) < NOTICE_RETRY_SECONDS
            if recent:
                report["notification"] = {"status": "RETRY_DELAY"}
            else:
                try:
                    target = paired_recipient(home, args.account, config)
                    if cli is None:
                        raise OpsError("NOTICE_CLI_UNAVAILABLE")
                    state.update(last_attempt_fingerprint=digest, last_attempt_epoch=now)
                    atomic_state(path, state)  # Bound crash/uncertain-send retry bursts.
                    notice_code, receipt = runner(cli + ["message", "send", "--channel", "feishu", "--account", args.account,
                                                       "--target", target, "--message", notice_text(report), "--json"], env, CLI_TIMEOUT)
                    if notice_code == "OK" and delivery_receipt(receipt):
                        state["last_notified_fingerprint"] = digest
                        report["notification"] = {"status": "SENT"}
                    else:
                        report["notification"] = {"status": "FAILED", "reason": "NOTICE_DELIVERY_NOT_CONFIRMED"}
                except OpsError as error:
                    report["notification"] = {"status": "FAILED", "reason": error.code}
        state["last_status"] = report
        atomic_state(path, state)
    complete = (report["gateway"]["status"] == "UP" and report["feishu"]["status"] == "READY"
                and report["model_policy"]["status"] == "PASS" and report["routing"]["status"] == "MATCH"
                and report["notification"]["status"] != "FAILED")
    return report, 0 if complete else 1


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        raise OpsError("ARGUMENTS_INVALID")


def valid_label(value: str) -> str:
    if not LABEL.fullmatch(value):
        raise argparse.ArgumentTypeError("invalid label")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = SafeParser(description=__doc__)
    parser.add_argument("command", choices=["check"])
    parser.add_argument("--host-role", required=True, choices=["windows-primary", "mac-temporary"])
    parser.add_argument("--account", required=True, type=valid_label)
    parser.add_argument("--agent", required=True, type=valid_label)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--openclaw-home")
    parser.add_argument("--notify-on-change", action="store_true")
    parser.add_argument("--probe-model", action="store_true", help="unsupported; exits 2 without invoking a model")
    result = parser.parse_args(argv)
    if result.probe_model:
        raise OpsError("MODEL_PROBE_NOT_SUPPORTED")
    return result


def main(argv: list[str] | None = None) -> int:
    try:
        report, exit_code = check(parse_args(argv))
    except OpsError as error:
        report = {"schema": SCHEMA, "error": error.code}
        exit_code = 75 if error.code == "STATE_LOCK_BUSY" else 2 if error.code in {"ARGUMENTS_INVALID", "MODEL_PROBE_NOT_SUPPORTED"} else 3
    except KeyboardInterrupt:
        report, exit_code = {"schema": SCHEMA, "error": "INTERRUPTED"}, 130
    except Exception:
        report, exit_code = {"schema": SCHEMA, "error": "INTERNAL_ERROR"}, 3
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
