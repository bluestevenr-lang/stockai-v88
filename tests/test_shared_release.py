"""Cloud release must work without private producer modules on sys.path."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def test_cloud_casebook_in_isolated_interpreter():
    script = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "import directive_casebook; "
        "passed,total,drift=directive_casebook.run(); "
        "assert passed == total == 23 and not drift, (passed,total,drift)"
    )
    subprocess.run([sys.executable, "-I", "-c", script, str(ROOT)], check=True, cwd=ROOT)


def test_release_manifest_covers_matching_shared_files():
    manifest = json.loads((ROOT / "decision_release.json").read_text())
    assert "promise_sentry_rules.py" in manifest["shared_modules"]
    for name, expected in {**manifest["shared_modules"], **manifest["views"]}.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, name
