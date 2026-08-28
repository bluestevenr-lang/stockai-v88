"""Static contract checks; these do not claim a Windows installation passed."""
from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "win" / "install_openclaw_watchdog.ps1"
LAUNCHER = ROOT / "win" / "安装龙虾监控-双击我.bat"
TEXT = INSTALLER.read_text(encoding="utf-8-sig")


def test_windows_script_encodings():
    assert INSTALLER.read_bytes().startswith(b"\xef\xbb\xbf")
    LAUNCHER.read_bytes().decode("ascii")


def test_monitor_is_separate_and_requires_the_current_logged_in_user():
    assert "$TaskName = 'V88 OpenClaw Health'" in TEXT
    assert "-LogonType Interactive -RunLevel Limited" in TEXT
    assert "New-ScheduledTaskTrigger -AtLogOn -User $currentSid" in TEXT
    assert "-RepetitionInterval (New-TimeSpan -Minutes 5)" in TEXT
    assert "-RepetitionDuration" not in TEXT
    assert "-MultipleInstances IgnoreNew" in TEXT
    assert "-AtStartup" not in TEXT
    assert "-LogonType S4U" not in TEXT
    assert "Register-ScheduledTask -TaskName $TaskName" in TEXT
    assert "Logged-out operation is NOT provided" in TEXT


def test_dry_run_exits_before_any_installation_write():
    dry_run_exit = TEXT.index("if ($DryRun) { $plan | ConvertTo-Json; exit 0 }")
    for operation in ("New-Item -ItemType", "Copy-Item -LiteralPath", "Set-Acl -LiteralPath",
                      "[IO.File]::WriteAllText($RunnerPath", "Register-ScheduledTask -TaskName"):
        assert TEXT.index(operation) > dry_run_exit
    assert "DryRun requires explicit -Account and -Agent" in TEXT


def test_undo_removes_only_our_owned_task_and_preserves_state():
    undo = TEXT.split("if ($Undo) {", 1)[1].split("$OpenClawHome =", 1)[0]
    assert "Unregister-ScheduledTask -TaskName $TaskName -TaskPath '\\' -Confirm:$false" in undo
    assert "Remove-Item" not in TEXT
    assert "$existing.Description -ne $TaskMarker" in TEXT
    assert "$existing.Principal.UserId -notin" in TEXT


def test_exact_existing_account_agent_binding_is_required():
    for required in ("Assert-SafeAlias $Account", "Assert-SafeAlias $Agent", "$Account -cnotin $accountNames",
                     "$Agent -cnotin $agentNames", "(Get-Property $match 'accountId') -ceq $Account",
                     "(Get-Property $_ 'agentId') -ceq $Agent", "if ($matchingBindings.Count -eq 0)"):
        assert required in TEXT
    assert "Read-Host 'Enter the Feishu account alias to monitor (not App ID)'" in TEXT


def test_fixed_core_command_and_private_outputs_only():
    for arg in ("'check'", "'--host-role'", "'windows-primary'", "'--account'", "'--agent'",
                "'--state-dir'", "'--openclaw-home'"):
        assert arg in TEXT
    assert "$StateDir = Join-Path $OpenClawHome 'ops'" in TEXT
    assert "$acl.SetAccessRuleProtection($true, $false)" in TEXT
    assert "Assert-NoReparse" in TEXT
    assert "@fixedArgs *> $null" in TEXT  # raw child stderr must not enter reports
    assert "Quote-Literal" in TEXT
    assert "--probe-model" not in TEXT


def test_notify_is_opt_in_and_never_changes_gateway_or_auth():
    assert "[switch]$Notify" in TEXT
    assert "if ($Notify) { $fixedArgs += '--notify-on-change' }" in TEXT
    assert " -Notify %*" in LAUNCHER.read_text(encoding="ascii")
    for forbidden in (r"\bgateway\s+(?:restart|start|install)\b", r"\bmodels\s+auth\s+login\b",
                      r"\bplugins\s+(?:enable|install)\b", r"\bconfig\s+(?:set|patch)\b",
                      r"Invoke-Expression", r"\bgit\s+push\b", r"Invoke-WebRequest",
                      r"Start-ScheduledTask", r"-Verb\s+RunAs"):
        assert not re.search(forbidden, TEXT, re.I), forbidden
    assert "model_test = 'UNTESTED'" in TEXT
    assert "runtime_acceptance = 'NOT_VERIFIED'" in TEXT


@pytest.mark.skipif(not shutil.which("pwsh"), reason="PowerShell parser unavailable; Windows runtime acceptance still required")
def test_powershell_syntax_only_when_parser_available():
    quoted = str(INSTALLER).replace("'", "''")
    command = (
        "$tokens=$null; $errors=$null; "
        f"[void][System.Management.Automation.Language.Parser]::ParseFile('{quoted}', [ref]$tokens, [ref]$errors); "
        "@($errors | ForEach-Object {$_.Message}) | ConvertTo-Json -Compress; if ($errors.Count) {exit 1}"
    )
    result = subprocess.run([shutil.which("pwsh"), "-NoProfile", "-Command", command],
                            text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
