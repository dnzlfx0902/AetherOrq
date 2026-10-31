#!/usr/bin/env python3
"""M1 operator-visibility tests: `logs`, `runs`, and `--json` for status/runs.

Reuses the M0 harness, which leaves tests/test_engine.py untouched.
Run: python3 -m unittest discover -s tests -v
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_engine import FlowTestCase, py  # noqa: E402


class OperatorVisibilityTests(FlowTestCase):
    def logs(self, task, *extra):
        return self.cli("logs", "--db", str(self.db), "--task", task, *extra)

    def runs(self, *extra):
        return self.cli("runs", "--db", str(self.db), *extra)

    def test_logs_tails_the_last_stored_attempt(self):
        first = self.write_flow([{"id": "job", "argv": py("print('attempt-one')")}])
        self.assertEqual(self.run_flow(first).returncode, 0)
        second = self.write_flow([{"id": "job", "argv": py("print('attempt-two')")}])
        self.assertEqual(self.run_flow(second).returncode, 0)

        res = self.logs("job")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("attempt-two", res.stdout, "the newest attempt must win")
        self.assertNotIn("attempt-one", res.stdout, "an older attempt must not be returned")
        self.assertIn("status succeeded", res.stdout)
        self.assertIn("stdout", res.stdout)

    def test_logs_limits_to_the_tail_and_reports_missing_task(self):
        code = "print('\\n'.join(f'line{n}' for n in range(1, 21)))"
        flow = self.write_flow([{"id": "chatty", "argv": py(code)}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.logs("chatty", "--lines", "3")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("line18", res.stdout)
        self.assertIn("line20", res.stdout)
        self.assertNotIn("line17", res.stdout)

        missing = self.logs("ghost")
        self.assertEqual(missing.returncode, 2)
        self.assertIn("no stored attempt", missing.stderr)

    def test_logs_skips_rows_that_never_ran_a_process(self):
        flow = self.write_flow([{"id": "a", "argv": py("print('real-output')")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)
        self.assertEqual(self.run_flow(flow).returncode, 0)
        self.assertIn("skipped  a", self.run_flow(flow).stdout)

        res = self.logs("a")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("real-output", res.stdout, "a skipped row carries no output")

    def test_runs_lists_history_with_status_and_timing(self):
        flow = self.write_flow([{"id": "a", "argv": py("raise SystemExit(4)")}])
        self.assertEqual(self.run_flow(flow).returncode, 1)
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.runs()
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("failed", res.stdout)
        self.assertIn("succeeded", res.stdout)
        self.assertIn("started", res.stdout)

    def test_runs_json_parses_with_json_loads(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.runs("--json")
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(len(data["runs"]), 1)
        run = data["runs"][0]
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(run["tasks"], 1)
        self.assertEqual(run["succeeded"], 1)
        self.assertIsInstance(run["duration_seconds"], float)
        self.assertGreaterEqual(run["duration_seconds"], 0.0)
        self.assertTrue(run["started_at"])

    def test_status_json_parses_with_json_loads(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.cli("status", "--db", str(self.db), "--json")
        self.assertEqual(res.returncode, 0, res.stderr)
        data = json.loads(res.stdout)
        self.assertEqual(data["runs"], 1)
        self.assertEqual([t["task_id"] for t in data["tasks"]], ["a"])
        self.assertEqual(data["tasks"][0]["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
