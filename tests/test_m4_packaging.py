#!/usr/bin/env python3
"""M4 packaging: `pip install .` in a clean venv yields a working `aether` command.

The metadata tests are hermetic. The install test really builds and installs the
project into a throwaway venv (needs a network or a warm pip cache to fetch the
build backend); it skips, with the reason, only when that backend is unreachable.
"""
import json
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
AETHER = ROOT / "aether.py"
CONTRACT = ROOT / "CONTRACT.md"
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

    def test_contract_states_the_declared_python_floor(self):
        """The normative document must name the floor it declares support for."""
        floor = self.data["project"]["requires-python"].removeprefix(">=")
        documented = re.findall(r"Python (\d+\.\d+)\+", (ROOT / "CONTRACT.md").read_text())
        self.assertIn(floor, documented, f"CONTRACT.md states Python {documented}, pyproject declares >={floor}")

    def test_readme_test_count_matches_suite(self):
        """README's stated count is hand-written; keep it honest."""
        match = re.search(r"(\d+) end-to-end tests", (ROOT / "README.md").read_text())
        if match is None:
            self.fail("README no longer states the suite size")
        suite = unittest.TestLoader().discover(str(ROOT / "tests"))
        self.assertEqual(int(match.group(1)), suite.countTestCases())

    def test_ci_matrix_runs_every_supported_python(self):
        """M4 acceptance: CI green on *every* supported version.

        The matrix once silently lost the declared floor, so the claim is checked,
        not trusted: the first entry must be `requires-python`'s floor and the
        versions must be contiguous from there.
        """
        floor = tuple(int(part) for part in self.data["project"]["requires-python"].removeprefix(">=").split("."))
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        block = re.search(r"python-version:\s*\[([^\]]*)\]", workflow)
        if block is None:
            self.fail("ci.yml declares no python-version matrix")
        versions = [tuple(int(part) for part in v.split(".")) for v in re.findall(r'"(\d+\.\d+)"', block.group(1))]
        expected = [(floor[0], minor) for minor in range(floor[1], versions[-1][1] + 1)] if versions else []
        self.assertEqual(versions, expected, f"ci.yml matrix {versions} must be a contiguous run from the floor {floor}")


class ContractDocsTest(unittest.TestCase):
    """CONTRACT.md calls itself normative, so every option the CLI accepts must
    appear there. It documented none of the packaging surface: `--version`, the
    `aether` console script and the supported Python floor were missing."""

    def test_every_cli_option_is_documented(self):
        options = set(re.findall(r'"(--[a-z][a-z-]*)"', AETHER.read_text()))
        self.assertTrue(options, "no CLI options found in aether.py")
        contract = CONTRACT.read_text()
        missing = sorted(option for option in options if option not in contract)
        self.assertEqual(missing, [], f"CONTRACT.md never documents {missing}")


class CiWorkflowTest(unittest.TestCase):
    """M4: "the tag exists and its commit passes the same suite", so the tag must
    trigger CI and CI must run that suite — the matrix check alone proves neither."""

    def test_ci_runs_the_suite_and_a_v_tag_triggers_it(self):
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        self.assertIn("python3 -m unittest discover -s tests", workflow,
                      "ci.yml never runs the suite")
        tags = re.search(r"tags:\s*\[([^\]]*)\]", workflow)
        self.assertTrue(tags and '"v*"' in tags.group(1), f"ci.yml never triggers on v* tags: {tags and tags.group(1)}")

    def test_ci_installs_the_project_and_runs_the_console_script(self):
        """The matrix only proves the checkout passes. M4 also claims `pip install .`
        yields a working `aether`, and the suite's install test may skip offline, so
        CI itself must install the project and invoke the installed script."""
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        install = workflow.find("pip install .")
        script = workflow.find("aether --version")
        self.assertNotEqual(install, -1, "ci.yml never installs the project")
        self.assertNotEqual(script, -1, "ci.yml never runs the installed aether")
        self.assertLess(install, script, "the console-script check must come after the install")


class CliVersionTest(unittest.TestCase):
    """No tomllib skip: `--version` must work on the 3.10 floor too."""

    def test_cli_version_flag(self):
        done = subprocess.run(
            [sys.executable, "aether.py", "--version"],
            cwd=ROOT, capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(done.returncode, 0, f"aether --version rc={done.returncode}: {done.stderr}")
        self.assertRegex(done.stdout.strip(), r"^aether \d+\.\d+\.\d+")
        if tomllib is not None:
            declared = tomllib.loads(PYPROJECT.read_text())["project"]["version"]
            self.assertEqual(done.stdout.strip(), f"aether {declared}")


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

            # Run from outside the checkout: an installed command must not need it.
            for args in (["--help"], ["validate", str(ROOT / "examples" / "flow.json")]):
                done = subprocess.run([aether, *args], cwd=tmp, capture_output=True, text=True, timeout=120)
                self.assertEqual(done.returncode, 0, f"aether {args} rc={done.returncode}: {done.stderr}")
            help_text = subprocess.run([aether, "--help"], cwd=tmp, capture_output=True, text=True).stdout
            self.assertIn("usage: aether", help_text)

            # Distribution metadata is what the console script reports: the
            # checkout fallback cannot help here, so "unknown" or a stale number
            # means the installed package is misnamed or misversioned.
            declared = re.search(r'^version = "([^"]+)"', PYPROJECT.read_text(), re.M)
            if declared is None:
                self.fail("pyproject.toml declares no version")
            reported = subprocess.run(
                [aether, "--version"], cwd=tmp, capture_output=True, text=True, timeout=120
            )
            self.assertEqual(reported.returncode, 0, f"aether --version rc={reported.returncode}: {reported.stderr}")
            self.assertEqual(reported.stdout.strip(), f"aether {declared.group(1)}")

            # `validate` only parses JSON, so an installed command that cannot
            # execute anything would still pass. Run the shipped example for
            # real from a foreign cwd: tasks must execute, state must land in
            # the given db, and the artifact must appear next to it.
            db = Path(tmp) / "installed.db"
            ran = subprocess.run(
                [aether, "run", str(ROOT / "examples" / "flow.json"), "--db", str(db)],
                cwd=tmp, capture_output=True, text=True, timeout=300,
            )
            self.assertEqual(ran.returncode, 0, f"aether run rc={ran.returncode}: {ran.stderr}")
            self.assertTrue((Path(tmp) / "demo-artifact.txt").is_file(), ran.stdout)
            status = subprocess.run(
                [aether, "status", "--db", str(db), "--json"],
                cwd=tmp, capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(status.returncode, 0, f"aether status rc={status.returncode}: {status.stderr}")
            tasks = json.loads(status.stdout)["tasks"]
            self.assertEqual({task["task_id"]: task["status"] for task in tasks},
                             {"greet": "succeeded", "write_artifact": "succeeded",
                              "verify_artifact": "succeeded"})


if __name__ == "__main__":
    unittest.main()
