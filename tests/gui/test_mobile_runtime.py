"""Execute browser client contracts against a deterministic DOM/fetch fake, without a server."""

import shutil
import subprocess
from pathlib import Path

import pytest


def test_mobile_command_and_pairing_contracts():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is optional; needed only to execute mobile browser contract tests")
    result = subprocess.run(
        [
            node,
            "--test",
            str(Path(__file__).with_name("mobile_contract.test.cjs")),
            str(Path(__file__).with_name("mobile_pwa.test.cjs")),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
