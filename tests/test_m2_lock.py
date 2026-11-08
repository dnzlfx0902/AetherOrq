#!/usr/bin/env python3
"""M2 step 2: single-writer lock file for `aether run`.

Two concurrent runs on the same db used to be unsupported (CONTRACT §8.2):
recovery in one process would mark the other's in-flight attempts interrupted.
These tests drive the real CLI in subprocesses and assert exclusion, release on
success/failure/crash, and the refusal message.
Run: python3 -m unittest discover -s tests -v
"""
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_engine import AETHER, FlowTestCase, py  # noqa: E402


class WriterLockTests(FlowTestCase):
    def lock_path(self):
        return Path(str(self.db) + ".lock")

    def start_run(self, flow_path, jobs=1):
        return subprocess.Popen(
            [sys.executable, str(AETHER), "run", str(flow_path), "--db", str(self.db),
             "--jobs", str(jobs)],
            cwd=self.dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def wait_until_running(self, count=1, deadline=20):
        """Block until `count` attempts are in flight (i.e. the writer holds the lock)."""
        end = time.monotonic() + deadline
        while time.monotonic() < end:
            time.sleep(0.05)
            if not self.db.exists():
                continue
            try:
                conn = sqlite3.connect(self.db)
                running = conn.execute(
                    "SELECT COUNT(*) FROM task_runs WHERE status='running'"
                ).fetchone()[0]
                conn.close()
            except sqlite3.OperationalError:
                continue  # schema still being created
            if running >= count:
                return True
        return False

    def run_count(self):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        finally:
            conn.close()

    def test_second_concurrent_run_is_refused(self):
        flow = self.write_flow([{"id": "slow", "argv": py("import time; time.sleep(3)")}])
        first = self.start_run(flow)
        try:
            self.assertTrue(self.wait_until_running(), "the first run never started a task")
            res = self.cli("run", str(flow), "--db", str(self.db))
            self.assertEqual(res.returncode, 2, res.stderr + res.stdout)
            self.assertIn("already writing", res.stderr)
            self.assertIn(str(self.db), res.stderr)
            self.assertIn("lock", res.stderr)
            self.assertEqual(self.run_count(), 1, "the refused run must not record a run row")
            self.assertFalse(
                self.lock_path().read_text().strip() == "",
                "the lock file must name its holder",
            )
        finally:
            out, err = first.communicate(timeout=60)
        self.assertEqual(first.returncode, 0, out + err)
        self.assertIn("succeeded", out)

    def test_lock_is_released_after_success(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        first = self.run_flow(flow)
        self.assertEqual(first.returncode, 0, first.stderr)
        second = self.run_flow(flow)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotIn("already writing", second.stderr)
        self.assertIn("skipped  a", second.stdout)
        self.assertEqual(self.run_count(), 2)

    def test_lock_is_released_after_a_failed_run(self):
        flow = self.write_flow([{"id": "boom", "argv": py("raise SystemExit(7)")}])
        first = self.run_flow(flow)
        self.assertEqual(first.returncode, 1, first.stderr)
        second = self.run_flow(flow)
        self.assertEqual(second.returncode, 1, second.stderr)
        self.assertNotIn("already writing", second.stderr)
        self.assertEqual(self.run_count(), 2, "a failed run must release the lock")

    def test_no_stale_lock_after_sigkill(self):
        go = self.dir / "go"
        flow = self.write_flow(
            [{"id": "slow", "argv": py(
                f"import os, time; time.sleep(0 if os.path.exists({str(go)!r}) else 30)"
            )}]
        )
        proc = self.start_run(flow)
        self.assertTrue(self.wait_until_running(), "the run never started a task")
        proc.kill()
        proc.wait(timeout=20)
        assert proc.stdout is not None and proc.stderr is not None
        proc.stdout.close()
        proc.stderr.close()

        go.touch()
        res = self.run_flow(flow)
        self.assertNotIn("already writing", res.stderr)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        self.assertIn("recovered 1 interrupted attempt", res.stderr)
        self.assertEqual(self.latest()["slow"][0], "succeeded")

    def test_lock_does_not_block_read_only_commands(self):
        flow = self.write_flow([{"id": "slow", "argv": py("import time; time.sleep(3)")}])
        first = self.start_run(flow)
        try:
            self.assertTrue(self.wait_until_running(), "the first run never started a task")
            res = self.cli("status", "--db", str(self.db))
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertIn("slow", res.stdout)
        finally:
            first.communicate(timeout=60)


if __name__ == "__main__":
    unittest.main()
