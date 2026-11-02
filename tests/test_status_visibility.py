#!/usr/bin/env python3
"""`status` must report the task's real outcome, not the `skipped` bookkeeping row.

CONTRACT.md §2: a `skipped` row "is *not* a new success; the success it refers to
is the earlier `succeeded` row". So `status` has to look through skipped rows,
otherwise every run after the first hides the exit code and the real state.
`blocked` rows are a real state (nothing ran in this run) and stay visible.

Run: python3 -m unittest discover -s tests -v
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_engine import FlowTestCase, py  # noqa: E402


class StatusVisibilityTests(FlowTestCase):
    def status(self, *extra):
        return self.cli("status", "--db", str(self.db), *extra)

    def test_status_looks_through_skipped_rows(self):
        flow = self.write_flow([{"id": "build", "argv": py("print('built')")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)
        second = self.run_flow(flow)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("skipped  build", second.stdout)

        res = self.status()
        self.assertEqual(res.returncode, 0, res.stderr)
        line = next(l for l in res.stdout.splitlines() if l.startswith("build"))
        self.assertIn("succeeded", line, f"skipped must not hide the real state: {line!r}")
        self.assertNotIn("skipped", line)
        self.assertIn(" 0 ", line, f"the real exit code must be shown: {line!r}")

        data = json.loads(self.status("--json").stdout)
        self.assertEqual(data["tasks"][0]["status"], "succeeded")
        self.assertEqual(data["tasks"][0]["exit_code"], 0)

    def test_status_still_reports_blocked_tasks(self):
        flow = self.write_flow(
            [
                {"id": "boom", "argv": py("raise SystemExit(3)")},
                {"id": "child", "argv": py("pass"), "depends_on": ["boom"]},
            ]
        )
        self.assertEqual(self.run_flow(flow).returncode, 1)

        res = self.status()
        self.assertEqual(res.returncode, 0, res.stderr)
        lines = {l.split()[0]: l for l in res.stdout.splitlines() if l and not l[0].isspace()}
        self.assertIn("failed", lines["boom"])
        self.assertIn("blocked", lines["child"], "a blocked task is a real state")


if __name__ == "__main__":
    unittest.main()
