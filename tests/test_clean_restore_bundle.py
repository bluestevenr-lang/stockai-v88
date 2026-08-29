import hashlib
import re
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "win" / "V88_WIN_CLEAN_RESTORE_20260830"


class CleanRestoreBundleTests(unittest.TestCase):
    def test_windows_encoding_contract(self):
        self.assertTrue((BUNDLE / "RESTORE_V88.bat").read_bytes().isascii())
        for path in BUNDLE.glob("*.ps1"):
            self.assertTrue(path.read_bytes().startswith(b"\xef\xbb\xbf"), path.name)

    def test_manifest_covers_every_runtime_input(self):
        required = {
            "AGENTS-GPT.md",
            "README.md",
            "RESTORE_V88.bat",
            "projection_runner.ps1",
            "restore_v88_win.ps1",
            "verify_v88_win.ps1",
            "../install_openclaw_watchdog.ps1",
            "../openclaw_ops.py",
            "../openclaw-v88/projection_tests.py",
            "../openclaw-v88/sync_v88_projection_win.py",
        }
        rows = {}
        for line in (BUNDLE / "MANIFEST.sha256").read_text(encoding="utf-8").splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
            self.assertIsNotNone(match, line)
            rows[match.group(2)] = match.group(1)
        self.assertEqual(required, set(rows))
        for relative, expected in rows.items():
            target = (BUNDLE / relative).resolve()
            self.assertTrue(target.is_relative_to(ROOT.resolve()))
            self.assertEqual(expected, hashlib.sha256(target.read_bytes()).hexdigest())

    def test_hard_gates_are_present(self):
        restore = (BUNDLE / "restore_v88_win.ps1").read_text(encoding="utf-8-sig")
        verify = (BUNDLE / "verify_v88_win.ps1").read_text(encoding="utf-8-sig")
        for token in (
            "managed:kimi-code",
            "kimi-code/k3-256k",
            "openai/gpt-5.6-sol",
            "--force",
            "fallbacks",
            "Assert-PackageIntegrity",
            "Privacy scan",
        ):
            self.assertIn(token, restore)
        for token in (
            "RemoteMacReadConfirmed",
            "lastInboundAt",
            "lastOutboundAt",
            "bindMode",
            "listenerPids.Count -ne 1",
            "Install-ProjectionTask",
        ):
            self.assertIn(token, verify)

    def test_no_credential_material_or_legacy_cutover(self):
        material = "\n".join(
            path.read_text(encoding="utf-8-sig", errors="replace")
            for path in BUNDLE.iterdir()
            if path.is_file()
        )
        self.assertNotRegex(material, r"\bsk-[A-Za-z0-9_-]{12,}")
        self.assertNotRegex(material, r"\bou_[A-Za-z0-9_-]{8,}")
        self.assertNotIn("v88_mobile_config_patch", material)
        self.assertNotIn("-Stage CUTOVER", material)
        self.assertNotIn("exec = @{ mode = 'full'", material)

    def test_powershell_parser_when_available(self):
        pwsh = shutil.which("pwsh")
        if not pwsh:
            self.skipTest("pwsh is not installed on this Mac")
        for path in BUNDLE.glob("*.ps1"):
            script = (
                "$e=$null;$t=$null;"
                f"[void][System.Management.Automation.Language.Parser]::ParseFile('{path}',[ref]$t,[ref]$e);"
                "if($e.Count){$e|% Message;exit 1}"
            )
            subprocess.run([pwsh, "-NoProfile", "-Command", script], check=True)


if __name__ == "__main__":
    unittest.main()
