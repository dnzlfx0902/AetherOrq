#!/usr/bin/env python3
"""M3: per-task execution context (cwd + environment allowlist). Real CLI, no mocks."""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AETHER = ROOT / "aether.py"


def py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "state.db"

    def tearDown(self):
        self.tmp.cleanup()

    def write_flow(self, tasks, name="t"):
        path = self.dir / "flow.json"
        path.write_text(json.dumps({"name": name, "tasks": tasks}), encoding="utf-8")
        return path

    def cli(self, *args, env=None, timeout=90):
        return subprocess.run(
            [sys.executable, str(AETHER), *args],
            cwd=self.dir,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
        )

    def run_flow(self, flow_path, env=None):
        return self.cli("run", str(flow_path), "--db", str(self.db), env=env)

    def stdout_of(self, task_id):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(
                "SELECT stdout FROM task_runs WHERE task_id=? ORDER BY attempt_id DESC LIMIT 1",
                (task_id,),
            ).fetchone()[0]
        finally:
            conn.close()

    def state_of(self, task_id):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(
                "SELECT status, exit_code, error FROM task_runs WHERE task_id=? "
                "ORDER BY attempt_id DESC LIMIT 1",
                (task_id,),
            ).fetchone()
        finally:
            conn.close()

    def test_cwd_absolute_and_relative_to_flow_file(self):
        sub = self.dir / "sub"
        sub.mkdir()
        flow = self.write_flow(
            [
                {"id": "abs", "argv": py("import os; print(os.getcwd())"), "cwd": str(sub)},
                {"id": "rel", "argv": py("import os; print(os.getcwd())"), "cwd": "sub"},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 0, res.stderr)
        for tid in ("abs", "rel"):
            self.assertEqual(
                Path(self.stdout_of(tid).strip()).resolve(), sub.resolve(), f"{tid} ran elsewhere"
            )

    def test_env_allowlist_inherits_by_name_and_hides_the_rest(self):
        env = dict(os.environ, AETHER_M3_ALLOWED="yes", AETHER_M3_HIDDEN="leak")
        probe = py(
            "import os; print(os.environ.get('AETHER_M3_ALLOWED'), "
            "os.environ.get('AETHER_M3_HIDDEN'))"
        )
        flow = self.write_flow(
            [
                {"id": "listed", "argv": probe, "env": ["AETHER_M3_ALLOWED"]},
                {"id": "absent", "argv": probe, "env": ["AETHER_M3_NEVER_SET"]},
                {"id": "noallowlist", "argv": probe},
            ]
        )
        res = self.run_flow(flow, env=env)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(self.stdout_of("listed").split(), ["yes", "None"])
        self.assertEqual(self.stdout_of("absent").split(), ["None", "None"])
        # Behaviour preserved: a task without `env` still inherits everything.
        self.assertEqual(self.stdout_of("noallowlist").split(), ["yes", "leak"])

    def test_inline_env_map_is_rejected_before_execution(self):
        marker = self.dir / "executed"
        flow = self.write_flow(
            [
                {
                    "id": "a",
                    "argv": py(f"open({str(marker)!r}, 'w').close()"),
                    "env": {"API_TOKEN": "s3cr3t-value"},
                }
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 2, res.stdout)
        self.assertIn("inline map", res.stderr)
        self.assertNotIn("s3cr3t-value", res.stderr)
        self.assertFalse(marker.exists(), "no command may run on an invalid flow")
        self.assertFalse(self.db.exists(), "no state must be written for an invalid flow")

    def test_bad_cwd_and_bad_env_types_are_rejected(self):
        flow = self.write_flow(
            [
                {"id": "a", "argv": ["true"], "cwd": 5},
                {"id": "b", "argv": ["true"], "cwd": "   "},
                {"id": "c", "argv": ["true"], "env": "PATH"},
                {"id": "d", "argv": ["true"], "env": ["A", 1]},
                {"id": "e", "argv": ["true"], "env": ["A=B"]},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 2)
        self.assertIn("'cwd' must be a non-empty string", res.stderr)
        self.assertIn("'env' must be a list of non-empty variable names", res.stderr)
        self.assertFalse(self.db.exists())

    def test_missing_cwd_is_a_recorded_failure_not_a_crash(self):
        flow = self.write_flow(
            [{"id": "gone", "argv": py("print('never')"), "cwd": "no-such-dir"}]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 1, res.stderr)
        status, code, errmsg = self.state_of("gone")
        self.assertEqual(status, "failed")
        self.assertIsNone(code)
        self.assertIn("cannot start", errmsg)
        # A second run must still work (nothing left `running`).
        again = self.run_flow(flow)
        self.assertEqual(again.returncode, 1, again.stderr)

    def test_validate_accepts_cwd_and_env(self):
        sub = self.dir / "sub"
        sub.mkdir()
        flow = self.write_flow(
            [{"id": "a", "argv": ["true"], "cwd": "sub", "env": ["HOME", "PATH"]}]
        )
        res = self.cli("validate", str(flow))
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("ok: t (1 tasks)", res.stdout)
        self.assertFalse(self.db.exists())


if __name__ == "__main__":
    unittest.main()
