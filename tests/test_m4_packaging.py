#!/usr/bin/env python3
"""M4 packaging: `pip install .` in a clean venv yields a working `aether` command.

The metadata tests are hermetic. The install test really builds and installs the
project into a throwaway venv (needs a network or a warm pip cache to fetch the
build backend); it skips, with the reason, only when that backend is unreachable.
"""
import re
import subprocess
import sys
import tempfile
import unittest
import venv
from pathlib import Path

try:  # tomllib is 3.11+; the runtime floor stays 3.10
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
OFFLINE_MARKERS = ("Network", "Temporary failure", "Connection", "ProxyError", "Could not find a version")


@unittest.skipIf(tomllib is None, "tomllib needs Python 3.11+")
class PackagingMetadataTest(unittest.TestCase):
    def setUp(self):
        assert tomllib is not None  # class is skipped without it
        self.data = tomllib.loads(PYPROJECT.read_text())

    def test_console_script_resolves_to_callable(self):
        target = self.data["project"]["scripts"]["aether"]
        self.assertEqual(target, "aether:main")
        sys.path.insert(0, str(ROOT))
        try:
            module = __import__(target.split(":")[0])
        finally:
            sys.path.remove(str(ROOT))
        self.assertTrue(callable(getattr(module, target.split(":")[1])))

    def test_requires_python_matches_documented_floor(self):
        """pyproject's floor and README must state the same Python version."""
        floor = self.data["project"]["requires-python"]
        self.assertEqual(floor, ">=3.10")
        documented = set(re.findall(r"Python (\d+\.\d+)\+", (ROOT / "README.md").read_text()))
        self.assertEqual(documented, {floor.removeprefix(">=")},
                         f"README documents {documented or 'no floor'}, pyproject declares {floor}")

    def test_no_runtime_dependencies(self):
        self.assertEqual(self.data["project"]["dependencies"], [])

    def test_readme_test_count_matches_suite(self):
        """README's stated count is hand-written; keep it honest."""
        match = re.search(r"(\d+) end-to-end tests", (ROOT / "README.md").read_text())
        if match is None:
            self.fail("README no longer states the suite size")
        suite = unittest.TestLoader().discover(str(ROOT / "tests"))
        self.assertEqual(int(match.group(1)), suite.countTestCases())


class CleanVenvInstallTest(unittest.TestCase):
    def test_pip_install_then_aether_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_dir = Path(tmp) / "venv"
            venv.EnvBuilder(with_pip=True).create(env_dir)
            scripts = env_dir / ("Scripts" if sys.platform == "win32" else "bin")
            pip = str(scripts / "pip")
            aether = str(scripts / "aether")

            install = subprocess.run(
                [pip, "install", "--quiet", str(ROOT)],
                capture_output=True, text=True, timeout=900,
            )
            if install.returncode != 0:
                detail = install.stdout + install.stderr
                if any(marker in detail for marker in OFFLINE_MARKERS):
                    self.skipTest(f"build backend unreachable (offline): {detail.strip()[-300:]}")
                self.fail(f"pip install . failed (rc={install.returncode}):\n{detail}")

            for args in (["--help"], ["validate", str(ROOT / "examples" / "flow.json")]):
                done = subprocess.run([aether, *args], capture_output=True, text=True, timeout=120)
                self.assertEqual(done.returncode, 0, f"aether {args} rc={done.returncode}: {done.stderr}")
            self.assertIn("usage: aether", subprocess.run([aether, "--help"], capture_output=True, text=True).stdout)


if __name__ == "__main__":
    unittest.main()
