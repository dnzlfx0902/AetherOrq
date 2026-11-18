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

    def test_logs_tail_survives_the_capture_limit(self):
        code = "for n in range(1, 20001): print(f'line{n:05d}')"
        flow = self.write_flow([{"id": "chatty", "argv": py(code)}])
        self.assertEqual(self.run_flow(flow).returncode, 0)

        res = self.logs("chatty", "--lines", "2")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("line20000", res.stdout, "logs must show the real end of a capped output")
        self.assertIn("line19999", res.stdout)
        self.assertNotIn("line00001", res.stdout, "the uncapped head is not the tail")
        self.assertIn("capture truncated", res.stdout, "a capped capture must say so")

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


class ForeignDbTests(FlowTestCase):
    """A `--db` that is not an AetherOrq database is operator error: exit 2.

    Before, `status`/`runs`/`logs` and `run` let sqlite3's DatabaseError escape,
    so a typo (`--db notes.txt`) printed a traceback and exited 1 — the code that
    means "a task failed", although nothing ran.
    """

    def foreign_db(self):
        path = self.dir / "not-aether.db"
        path.write_bytes(b"not a database")
        return path

    def test_read_only_commands_refuse_a_foreign_db(self):
        db = self.foreign_db()
        for args in (
            ("status", "--db", str(db)),
            ("runs", "--db", str(db)),
            ("logs", "--db", str(db), "--task", "job"),
        ):
            res = self.cli(*args)
            self.assertEqual(res.returncode, 2, f"{args[0]}: {res.stderr}")
            self.assertIn("as an AetherOrq database", res.stderr)
            self.assertNotIn("Traceback", res.stderr)

    def test_run_refuses_a_foreign_db_and_executes_nothing(self):
        db = self.foreign_db()
        marker = self.dir / "ran"
        flow = self.write_flow([{"id": "job", "argv": py(f"open({str(marker)!r}, 'w').close()")}])
        res = self.cli("run", str(flow), "--db", str(db))
        self.assertEqual(res.returncode, 2, res.stderr)
        self.assertIn("as an AetherOrq database", res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        self.assertFalse(marker.exists(), "no command may run when the db is unusable")


if __name__ == "__main__":
    unittest.main()
