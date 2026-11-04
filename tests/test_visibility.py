#!/usr/bin/env python3
"""M1 operator-visibility tests: `logs`, `runs`, and `--json` output.

Same style as tests/test_engine.py: the real CLI in a subprocess, real SQLite.
Run: python3 -m unittest discover -s tests -v
"""
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AETHER = ROOT / "aether.py"


class VisibilityTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "state.db"

    def tearDown(self):
        self.tmp.cleanup()

    def write_flow(self, tasks, name="t"):
        flow = {"name": name, "tasks": tasks}
        path = self.dir / "flow.json"
        path.write_text(json.dumps(flow), encoding="utf-8")
        return path

    def cli(self, *args, timeout=120):
        return subprocess.run(
            [sys.executable, str(AETHER), *args],
            cwd=self.dir,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def run_flow(self, flow_path, *extra):
        return self.cli("run", str(flow_path), "--db", str(self.db), *extra)

    def db_rows(self):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(
                "SELECT task_id, status FROM task_runs ORDER BY attempt_id"
            ).fetchall()
        finally:
            conn.close()


class JsonTests(VisibilityTestCase):
    def test_status_json_parses_and_reports_latest_state(self):
        flow = self.write_flow(
            [
                {"id": "ok", "argv": [sys.executable, "-c", "print('fine')"]},
                {"id": "bad", "argv": [sys.executable, "-c", "raise SystemExit(4)"]},
            ]
        )
        self.assertEqual(self.run_flow(flow).returncode, 1)

        res = self.cli("status", "--db", str(self.db), "--json")
        self.assertEqual(res.returncode, 0, res.stderr)
        payload = json.loads(res.stdout)
        self.assertEqual(payload["flow"], "t")
        self.assertEqual(payload["runs"], 1)
        states = {t["task_id"]: t for t in payload["tasks"]}
        self.assertEqual(states["ok"]["status"], "succeeded")
        self.assertEqual(states["bad"]["status"], "failed")
        self.assertEqual(states["bad"]["exit_code"], 4)
        self.assertTrue(states["bad"]["finished_at"])

    def test_runs_json_parses_run_history(self):
        flow = self.write_flow([{"id": "a", "argv": [sys.executable, "-c", "pass"]}])
        self.assertEqual(self.run_flow(flow).returncode, 0)
        self.assertEqual(self.run_flow(flow, "--force-task", "a").returncode, 0)

        res = self.cli("runs", "--db", str(self.db), "--json")
        self.assertEqual(res.returncode, 0, res.stderr)
        runs = json.loads(res.stdout)["runs"]
        self.assertEqual([r["run_id"] for r in runs], [1, 2])
        self.assertEqual({r["status"] for r in runs}, {"succeeded"})
        self.assertTrue(all(r["started_at"] and r["finished_at"] for r in runs))

    def test_json_flags_require_an_existing_db(self):
        res = self.cli("runs", "--db", str(self.dir / "missing.db"), "--json")
        self.assertEqual(res.returncode, 2)
        self.assertIn("no database", res.stderr)


class RunsTests(VisibilityTestCase):
    def test_runs_lists_every_run_with_status(self):
        flow = self.write_flow(
            [{"id": "a", "argv": [sys.executable, "-c", "raise SystemExit(2)"]}]
        )
        self.assertEqual(self.run_flow(flow).returncode, 1)
        self.assertEqual(self.run_flow(flow).returncode, 1)

        res = self.cli("runs", "--db", str(self.db))
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("run", res.stdout)
        self.assertIn("failed", res.stdout)
        self.assertEqual(len([ln for ln in res.stdout.splitlines() if ln.strip().startswith(("1", "2"))]), 2)


class LogsTests(VisibilityTestCase):
    def counting_task(self):
        counter = self.dir / "count"
        return [
            sys.executable,
            "-c",
            "import pathlib; p = pathlib.Path(%r); "
            "n = int(p.read_text()) + 1 if p.exists() else 1; p.write_text(str(n)); "
            "print('out%%d' %% n); print('err%%d' %% n, file=__import__('sys').stderr); "
            "raise SystemExit(0 if n == 3 else 6)" % str(counter),
        ]

    def test_logs_returns_the_last_stored_attempt(self):
        flow = self.write_flow([{"id": "t1", "argv": self.counting_task()}])
        for _ in range(3):
            self.assertIn(self.run_flow(flow, "--force-task", "t1").returncode, (0, 1))
        self.assertEqual(
            [s for tid, s in self.db_rows() if tid == "t1"], ["failed", "failed", "succeeded"]
        )

        res = self.cli("logs", "--db", str(self.db), "--task", "t1")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("attempt #3", res.stdout)
        self.assertIn("status=succeeded", res.stdout)
        self.assertIn("out3", res.stdout)
        self.assertIn("err3", res.stdout)
        self.assertNotIn("out2", res.stdout)
        self.assertNotIn("err1", res.stdout)

    def test_logs_reads_past_a_reused_success_row(self):
        flow = self.write_flow(
            [{"id": "t1", "argv": [sys.executable, "-c", "print('the output')"]}]
        )
        self.assertEqual(self.run_flow(flow).returncode, 0)
        self.assertEqual(self.run_flow(flow).returncode, 0)
        self.assertEqual(self.db_rows(), [("t1", "succeeded"), ("t1", "skipped")])

        res = self.cli("logs", "--db", str(self.db), "--task", "t1")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("attempt #1", res.stdout)
        self.assertIn("the output", res.stdout)

    def test_logs_reports_unknown_task(self):
        flow = self.write_flow([{"id": "t1", "argv": [sys.executable, "-c", "pass"]}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.cli("logs", "--db", str(self.db), "--task", "ghost")
        self.assertEqual(res.returncode, 1)
        self.assertIn("'ghost'", res.stderr)


if __name__ == "__main__":
    unittest.main()
