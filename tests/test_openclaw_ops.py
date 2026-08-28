"""Isolated service-probe contract tests; never connect to a real account."""

import argparse
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "openclaw_ops", Path(__file__).resolve().parents[1] / "win" / "openclaw_ops.py"
)
ops = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ops)
NOW = 1_788_000_000.0
SECRET = "DO_NOT_EXPOSE_TEST_TOKEN_987"
PAIRED = "ou_TEST_ONLY_PAIRED_USER_0001"


def live_payload():
    return {
        "ts": NOW * 1000,
        "channels": {"feishu": {"running": True, "probe": {"ok": True}}},
        "channelAccounts": {"feishu": [{
            "accountId": "default", "enabled": True, "configured": True,
            "running": True, "connected": True, "probe": {"ok": True},
            "lastInboundAt": None, "lastOutboundAt": None,
        }]},
    }


def config_fixture():
    return {
        "agents": {"list": [{"id": "v88-gpt", "model": {
            "primary": "openai/gpt-5.6-sol", "fallbacks": [],
        }}]},
        "auth": {"profiles": {"subscription": {"provider": "openai", "mode": "oauth"}}},
        "bindings": [{"agentId": "v88-gpt", "match": {"channel": "feishu", "accountId": "default"}}],
        "channels": {"feishu": {"enabled": True, "accounts": {"default": {"enabled": True}}}},
    }


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.home = self.root / "configured-home"
        self.home.mkdir()
        self.state = self.root / "monitor-only"
        self.config = config_fixture()
        self.write_config()
        self.args = argparse.Namespace(
            command="check", host_role="windows-primary", account="default", agent="v88-gpt",
            state_dir=str(self.state), openclaw_home=str(self.home), notify_on_change=False,
            probe_model=False,
        )
        self.calls = []
        self.probe_code = "OK"
        self.payload = live_payload()
        self.send_code = "OK"
        self.receipt = {"channel": "feishu", "result": {"messageId": "om_TEST_RECEIPT_ONLY"}}
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def write_config(self):
        (self.home / "openclaw.json").write_text(json.dumps(self.config), encoding="utf-8")

    def pair(self, users=None):
        directory = self.home / "credentials"
        directory.mkdir(exist_ok=True)
        (directory / "feishu-default-allowFrom.json").write_text(
            json.dumps({"allowFrom": [PAIRED] if users is None else users}), encoding="utf-8"
        )

    def runner(self, argv, env, timeout):
        self.calls.append((argv, dict(env), timeout))
        if "message" in argv:
            return self.send_code, copy.deepcopy(self.receipt)
        return self.probe_code, copy.deepcopy(self.payload)

    def run_check(self, now=NOW):
        return ops.check(self.args, runner=self.runner, cli=["test-openclaw"], now=now)

    def state_json(self):
        paths = list(self.state.glob("*.json"))
        self.assertEqual(len(paths), 1)
        return json.loads(paths[0].read_text(encoding="utf-8"))

    def test_live_channel_never_claims_model_or_business_success(self):
        report, code = self.run_check()
        self.assertEqual(code, 0)
        self.assertEqual(report["gateway"]["status"], "UP")
        self.assertEqual(report["feishu"]["status"], "READY")
        self.assertEqual(report["gpt"]["status"], "UNTESTED")
        self.assertEqual(report["end_to_end"]["status"], "UNVERIFIED")
        self.assertEqual(report["business"]["status"], "BUSINESS_PENDING")
        self.assertFalse(report["business"]["blocks_channel_check"])
        self.assertEqual(self.calls[0][0], ["test-openclaw", "channels", "status", "--probe", "--json"])
        self.assertEqual(len(self.calls), 1)

    def test_probe_ok_does_not_mean_connected(self):
        selected = self.payload["channelAccounts"]["feishu"][0]
        for connected, expected in ((False, "DISCONNECTED"), (None, "UNKNOWN")):
            with self.subTest(connected=connected):
                selected["connected"] = connected
                report, code = self.run_check()
                self.assertEqual(code, 1)
                self.assertEqual(report["feishu"]["status"], expected)

    def test_disabled_and_not_running_are_explicit_failures(self):
        selected = self.payload["channelAccounts"]["feishu"][0]
        selected["enabled"] = False
        report, code = self.run_check()
        self.assertEqual((code, report["feishu"]["status"]), (1, "DISABLED"))
        self.assertEqual(report["end_to_end"]["status"], "BLOCKED")
        selected["enabled"], selected["running"] = True, False
        report, code = self.run_check()
        self.assertEqual((code, report["feishu"]["status"]), (1, "STOPPED"))

    def test_missing_or_duplicate_selected_account_is_not_green(self):
        selected = self.payload["channelAccounts"]["feishu"][0]
        self.payload["channelAccounts"]["feishu"] = [dict(selected, accountId="another")]
        report, code = self.run_check()
        self.assertEqual(report["feishu"]["reason"], "ACCOUNT_NOT_FOUND")
        self.assertEqual(code, 1)
        self.payload["channelAccounts"]["feishu"] = [selected, selected]
        report, code = self.run_check()
        self.assertEqual(report["feishu"]["reason"], "ACCOUNT_AMBIGUOUS")
        self.assertEqual(code, 1)

    def test_old_or_invalid_timestamps_do_not_prove_live_rpc(self):
        for ts in (None, NOW * 1000 - 121000, float("nan"), float("inf"), "2026-08-29"):
            with self.subTest(ts=ts):
                self.payload["ts"] = ts
                report, code = self.run_check()
                self.assertEqual(code, 1)
                self.assertEqual(report["gateway"]["status"], "UNKNOWN")

    def test_unknown_json_and_config_only_response_fail_closed(self):
        self.payload = {"status": "ok", "secret": SECRET}
        report, code = self.run_check()
        self.assertEqual((code, report["gateway"]["status"]), (1, "UNKNOWN"))
        self.payload = {"gatewayReachable": False, "configOnly": True, "error": SECRET}
        report, code = self.run_check()
        self.assertEqual((code, report["gateway"]["status"]), (1, "DOWN"))
        self.assertNotIn(SECRET, json.dumps(report))

    def test_cli_timeout_not_reclassified_as_model_failure(self):
        self.probe_code, self.payload = "CLI_TIMEOUT", None
        report, code = self.run_check()
        self.assertEqual(code, 1)
        self.assertEqual(report["gateway"]["reason"], "CLI_TIMEOUT")
        self.assertEqual(report["gpt"]["status"], "UNTESTED")

    def test_exact_booleans_only(self):
        self.payload["channelAccounts"]["feishu"][0]["connected"] = 1
        report, code = self.run_check()
        self.assertEqual((code, report["feishu"]["status"]), (1, "UNKNOWN"))

    def test_expired_oauth_metadata_is_not_live_authentication_failure(self):
        self.config["auth"]["profiles"]["subscription"].update(expiresAt=1, expired=True, access=SECRET)
        self.write_config()
        report, _ = self.run_check()
        self.assertEqual(report["model_policy"]["authentication"], "OAUTH")
        self.assertEqual(report["gpt"]["status"], "UNTESTED")
        self.assertNotIn(SECRET, json.dumps(report))

    def test_unknown_auth_metadata_is_unknown_not_oauth(self):
        self.config.pop("auth")
        self.write_config()
        report, code = self.run_check()
        self.assertEqual(code, 1)
        self.assertEqual(report["model_policy"]["authentication"], "UNKNOWN")

    def test_api_key_or_fallback_is_blocked_without_invoking_model(self):
        for field in ("api_key", "fallback"):
            with self.subTest(field=field):
                self.config = config_fixture()
                if field == "api_key":
                    self.config["auth"]["profiles"]["subscription"]["mode"] = "api_key"
                else:
                    self.config["agents"]["list"][0]["model"]["fallbacks"] = ["paid/secret-model"]
                self.write_config()
                report, code = self.run_check()
                self.assertEqual((code, report["model_policy"]["status"]), (1, "BLOCKED"))
                self.assertEqual(report["gpt"]["status"], "UNTESTED")
                self.assertNotIn("secret-model", json.dumps(report))

    def test_environment_api_key_is_detected_without_exposing_value(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": SECRET}):
            report, code = self.run_check()
        self.assertEqual((code, report["model_policy"]["authentication"]), (1, "NON_OAUTH"))
        self.assertNotIn(SECRET, json.dumps(report))

    def test_malformed_model_metadata_is_safely_blocked(self):
        self.config["agents"]["list"][0]["model"]["primary"] = {"secret": SECRET}
        self.write_config()
        report, code = self.run_check()
        self.assertEqual((code, report["model_policy"]["status"]), (1, "BLOCKED"))
        self.assertNotIn(SECRET, json.dumps(report))

    def test_wrong_agent_binding_blocks_and_ambiguous_is_unknown(self):
        self.config["bindings"][0]["agentId"] = "another-agent"
        self.write_config()
        report, code = self.run_check()
        self.assertEqual((code, report["routing"]["status"]), (1, "MISMATCH"))
        self.config["bindings"].append(config_fixture()["bindings"][0])
        self.write_config()
        report, code = self.run_check()
        self.assertEqual((code, report["routing"]["status"]), (1, "UNKNOWN"))

    def test_monitor_state_dir_is_not_openclaw_home(self):
        report, _ = self.run_check()
        child_env = self.calls[0][1]
        self.assertEqual(child_env["OPENCLAW_STATE_DIR"], str(self.home))
        self.assertEqual(child_env["OPENCLAW_CONFIG_PATH"], str(self.home / "openclaw.json"))
        self.assertFalse((self.state / "openclaw.json").exists())
        self.assertTrue((self.home / "openclaw.json").exists())

    def test_default_home_preserves_existing_environment(self):
        self.args.openclaw_home = None
        with patch.dict(os.environ, {"OPENCLAW_STATE_DIR": str(self.home)}):
            report, code = self.run_check()
        self.assertEqual(code, 0)
        self.assertEqual(self.calls[0][1]["OPENCLAW_STATE_DIR"], str(self.home))
        self.assertNotIn("OPENCLAW_CONFIG_PATH", self.calls[0][1])

    def test_notifications_are_opt_in(self):
        self.pair()
        report, _ = self.run_check()
        self.assertEqual(report["notification"]["status"], "DISABLED")
        self.assertEqual(len(self.calls), 1)

    def test_one_paired_user_direct_send_dedupes_on_stable_status(self):
        self.pair()
        self.args.notify_on_change = True
        report, code = self.run_check()
        self.assertEqual((code, report["notification"]["status"]), (0, "SENT"))
        self.assertEqual(self.calls[1][0][1:5], ["message", "send", "--channel", "feishu"])
        notice = self.calls[1][0][self.calls[1][0].index("--message") + 1]
        self.assertIn(report["checked_at"], notice)
        self.assertIn("问题层", notice)
        self.assertIn("下一步", notice)
        self.assertNotIn(PAIRED, notice)
        report, code = self.run_check(now=NOW + 30)
        self.assertEqual(report["notification"]["status"], "UNCHANGED")
        self.assertEqual(len(self.calls), 3)

    def test_notification_failure_is_not_deduped_as_success_and_is_rate_limited(self):
        self.pair()
        self.args.notify_on_change = True
        self.send_code, self.receipt = "CLI_TIMEOUT", None
        report, code = self.run_check()
        self.assertEqual((code, report["notification"]["status"]), (1, "FAILED"))
        self.assertNotIn("last_notified_fingerprint", self.state_json())
        report, _ = self.run_check(now=NOW + 20)
        self.assertEqual(report["notification"]["status"], "RETRY_DELAY")
        self.assertEqual(len(self.calls), 3)
        self.payload["ts"] = (NOW + 301) * 1000
        self.send_code, self.receipt = "OK", {"channel": "feishu", "messageId": "om_TEST_RETRY"}
        report, _ = self.run_check(now=NOW + 301)
        self.assertEqual(report["notification"]["status"], "SENT")
        self.assertIn("last_notified_fingerprint", self.state_json())

    def test_changed_status_sends_one_new_notice(self):
        self.pair()
        self.args.notify_on_change = True
        self.run_check()
        self.payload["channelAccounts"]["feishu"][0]["connected"] = False
        report, _ = self.run_check(now=NOW + 1)
        self.assertEqual(report["notification"]["status"], "SENT")
        self.assertEqual(len(self.calls), 4)

    def test_missing_multiple_or_untrusted_pairing_never_sends(self):
        self.args.notify_on_change = True
        for users in ([], [PAIRED, "ou_SECOND_TEST_USER_0002"], ["ou_BAD;command"], ["*"]):
            with self.subTest(users=users):
                self.pair(users)
                before = len(self.calls)
                report, code = self.run_check()
                self.assertEqual((code, report["notification"]["status"]), (1, "FAILED"))
                self.assertEqual(len(self.calls), before + 1)

    def test_never_falls_back_to_another_accounts_pairing(self):
        self.args.notify_on_change = True
        self.pair()
        (self.home / "credentials" / "feishu-default-allowFrom.json").rename(
            self.home / "credentials" / "feishu-main-allowFrom.json"
        )
        report, _ = self.run_check()
        self.assertEqual(report["notification"]["status"], "FAILED")
        self.assertEqual(len(self.calls), 1)

    def test_disabled_account_will_not_send_even_with_pairing(self):
        self.pair()
        self.args.notify_on_change = True
        self.config["channels"]["feishu"]["accounts"]["default"]["enabled"] = False
        self.write_config()
        report, _ = self.run_check()
        self.assertEqual(report["notification"]["reason"], "NOTICE_ACCOUNT_NOT_ENABLED")
        self.assertEqual(len(self.calls), 1)

    def test_ambiguous_send_success_does_not_count_as_delivered(self):
        self.pair()
        self.args.notify_on_change = True
        self.receipt = {"channel": "feishu", "ok": True}
        report, _ = self.run_check()
        self.assertEqual(report["notification"]["status"], "FAILED")
        self.assertNotIn("last_notified_fingerprint", self.state_json())

    def test_reports_state_and_notices_are_strictly_sanitized(self):
        self.pair()
        self.args.notify_on_change = True
        selected = self.payload["channelAccounts"]["feishu"][0]
        selected.update(lastError=SECRET, openId=PAIRED, holdings={"private_asset": 123456789})
        self.payload["secret"] = SECRET
        self.config["channels"]["feishu"]["accounts"]["default"]["appSecret"] = SECRET
        self.write_config()
        report, _ = self.run_check()
        output = json.dumps(report) + json.dumps(self.state_json())
        output += self.calls[1][0][self.calls[1][0].index("--message") + 1]
        for private in (SECRET, PAIRED, "private_asset", "123456789"):
            self.assertNotIn(private, output)
        self.assertEqual(set(report), {"schema", "checked_at", "host_role", "account", "agent", "gateway",
            "feishu", "model_policy", "routing", "gpt", "business", "end_to_end", "notification"})

    @unittest.skipIf(os.name == "nt", "POSIX permission assertion")
    def test_state_is_private_and_atomic_temps_are_removed(self):
        self.run_check()
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o700)
        path = next(self.state.glob("*.json"))
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(list(self.state.glob(".ops-*.tmp")), [])

    @unittest.skipIf(os.name == "nt", "portable symlink creation requires Windows privileges")
    def test_state_symlink_refused_without_writes(self):
        target = self.root / "another-state"
        target.mkdir()
        self.state.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(ops.OpsError, "STATE_UNSAFE"):
            self.run_check()
        self.assertEqual(self.calls, [])
        self.assertEqual(list(target.iterdir()), [])


class UtilityTests(unittest.TestCase):
    def test_subprocess_timeout_discards_sensitive_stdout_and_stderr(self):
        with patch.object(ops.subprocess, "run", side_effect=subprocess.TimeoutExpired(
                "test-cli", 1, output=SECRET, stderr=SECRET)) as run:
            result = ops.run_cli(["test-cli"], {}, timeout=1)
        self.assertEqual(result, ("CLI_TIMEOUT", None))
        self.assertFalse(run.call_args.kwargs["shell"])
        self.assertEqual(run.call_args.kwargs["timeout"], 1)

    def test_subprocess_failed_exit_discards_raw_error(self):
        with patch.object(ops.subprocess, "run", return_value=subprocess.CompletedProcess(
                ["test-cli"], 1, SECRET, SECRET)):
            self.assertEqual(ops.run_cli(["test-cli"], {}), ("CLI_FAILED", None))

    def test_banner_noise_is_not_reprinted(self):
        self.assertEqual(ops.safe_json_object(SECRET + "\n{\"ok\":true}\n"), {"ok": True})
        self.assertIsNone(ops.safe_json_object("not-json " + SECRET))

    def test_windows_npm_cmd_resolves_to_node_entry_without_cmd_shell(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cmd, node = root / "openclaw.cmd", root / "node.exe"
            cmd.write_text("@echo fake", encoding="ascii")
            node.touch()
            entry = root / "node_modules" / "openclaw" / "openclaw.mjs"
            entry.parent.mkdir(parents=True)
            entry.touch()
            result = ops.resolve_cli(which=lambda name: str(cmd) if name == "openclaw" else None, windows=True)
            self.assertEqual(result, [str(node), str(entry)])
            self.assertFalse(any("cmd.exe" in x or x.endswith(".cmd") for x in result))

    def test_unknown_windows_wrapper_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ops.OpsError, "CLI_WRAPPER_UNSUPPORTED"):
                ops.resolve_cli(which=lambda name: str(Path(directory) / "openclaw.cmd") if name == "openclaw" else None,
                                windows=True)

    def test_probe_model_unsupported_without_subprocess_or_raw_argument(self):
        stdout = io.StringIO()
        args = ["check", "--host-role", "windows-primary", "--account", "default", "--agent", "v88-gpt",
                "--state-dir", "/unused-test", "--probe-model"]
        with contextlib.redirect_stdout(stdout), patch.object(ops, "check") as check:
            code = ops.main(args)
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stdout.getvalue())["error"], "MODEL_PROBE_NOT_SUPPORTED")
        check.assert_not_called()

    def test_invalid_aliases_cannot_inject_commands_or_escape_paths(self):
        for alias in ("../default", "x;" + SECRET, "x&echo", "..", "x/../y", "x\\y"):
            with self.subTest(alias=alias):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = ops.main(["check", "--host-role", "windows-primary", "--account", alias,
                                     "--agent", "v88-gpt", "--state-dir", "/unused-test"])
                self.assertEqual(code, 2)
                self.assertNotIn(SECRET, stdout.getvalue())

    def test_installer_aliases_with_dot_are_supported(self):
        self.assertEqual(ops.valid_label("v88.gpt"), "v88.gpt")

    def test_dry_run_or_wrong_channel_receipt_is_not_delivery(self):
        self.assertFalse(ops.delivery_receipt({"channel": "feishu", "dryRun": True, "messageId": "om_FAKE"}))
        self.assertFalse(ops.delivery_receipt({"channel": "telegram", "messageId": "om_FAKE"}))
        self.assertTrue(ops.delivery_receipt({"channel": "feishu", "payload": {"result": {"message_id": "om_FAKE"}}}))


if __name__ == "__main__":
    unittest.main()
