#!/usr/bin/env python3
"""Real end-to-end tests for the AetherOrq engine (stdlib unittest, no mocks).

Every test drives the actual CLI in a subprocess and inspects the real SQLite state.
Run: python3 -m unittest discover -s tests -v
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AETHER = ROOT / "aether.py"


def py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


class FlowTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "state.db"

    def tearDown(self):
        self.tmp.cleanup()

    def write_flow(self, tasks, name="t", **extra):
        flow = {"name": name, "tasks": tasks, **extra}
        path = self.dir / "flow.json"
        path.write_text(json.dumps(flow), encoding="utf-8")
        return path

    def cli(self, *args, timeout=90):
        return subprocess.run(
            [sys.executable, str(AETHER), *args],
            cwd=self.dir,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def run_flow(self, flow_path):
        return self.cli("run", str(flow_path), "--db", str(self.db))

    def rows(self):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(
                "SELECT task_id, status, attempts, exit_code FROM task_runs ORDER BY attempt_id"
            ).fetchall()
        finally:
            conn.close()

    def latest(self):
        return {tid: (status, attempts, code) for tid, status, attempts, code in self.rows()}


class ValidationTests(FlowTestCase):
    def test_cycle_is_rejected_before_any_execution(self):
        marker = self.dir / "executed"
        flow = self.write_flow(
            [
                {"id": "a", "argv": py(f"open({str(marker)!r},'w').close()"), "depends_on": ["c"]},
                {"id": "b", "argv": py("pass"), "depends_on": ["a"]},
                {"id": "c", "argv": py("pass"), "depends_on": ["b"]},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 2, res.stderr)
        self.assertIn("cycle", res.stderr)
        self.assertFalse(marker.exists(), "no command may run on an invalid flow")
        self.assertFalse(self.db.exists(), "no state must be written for an invalid flow")

    def test_unknown_dependency_and_duplicate_ids_are_rejected(self):
        flow = self.write_flow(
            [
                {"id": "a", "argv": py("pass"), "depends_on": ["ghost"]},
                {"id": "a", "argv": py("pass")},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 2)
        self.assertIn("unknown task 'ghost'", res.stderr)
        self.assertIn("duplicate task id", res.stderr)

    def test_shell_string_and_bad_timeout_are_rejected(self):
        flow = self.write_flow(
            [
                {"id": "a", "argv": "echo hi"},
                {"id": "b", "argv": ["true"], "timeout": 0},
                {"id": "c", "argv": ["true"], "retries": 99},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 2)
        self.assertIn("non-empty list", res.stderr)
        self.assertIn("timeout", res.stderr)
        self.assertIn("retries", res.stderr)

    def test_validate_subcommand_writes_nothing(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        res = self.cli("validate", str(flow))
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("ok: t (1 tasks)", res.stdout)
        self.assertFalse(self.db.exists())


class ExecutionTests(FlowTestCase):
    def test_dependencies_are_respected(self):
        log = self.dir / "order.log"
        flow = self.write_flow(
            [
                {
                    "id": "third",
                    "argv": py(f"open({str(log)!r},'a').write('third\\n')"),
                    "depends_on": ["second"],
                },
                {
                    "id": "first",
                    "argv": py(f"open({str(log)!r},'a').write('first\\n')"),
                },
                {
                    "id": "second",
                    "argv": py(f"open({str(log)!r},'a').write('second\\n')"),
                    "depends_on": ["first"],
                },
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        self.assertEqual(log.read_text().split(), ["first", "second", "third"])
        self.assertEqual({s for s, _, _ in self.latest().values()}, {"succeeded"})

    def test_failure_propagates_and_does_not_block_independent_branches(self):
        done = self.dir / "independent"
        flow = self.write_flow(
            [
                {"id": "boom", "argv": py("raise SystemExit(3)")},
                {"id": "child", "argv": py("print('should not run')"), "depends_on": ["boom"]},
                {
                    "id": "leaf",
                    "argv": py(f"open({str(done)!r},'w').close()"),
                    "depends_on": ["child"],
                },
                {"id": "other", "argv": py("print('ok')")},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 1)
        states = {tid: v[0] for tid, v in self.latest().items()}
        self.assertEqual(states["boom"], "failed")
        self.assertEqual(states["child"], "blocked")
        self.assertEqual(states["leaf"], "blocked")
        self.assertEqual(states["other"], "succeeded")
        self.assertFalse(done.exists())

    def test_retries_are_bounded_and_counted(self):
        counter = self.dir / "count"
        code = f"open({str(counter)!r},'a').write('x'); raise SystemExit(7)"
        flow = self.write_flow([{"id": "flaky", "argv": py(code), "retries": 2}])
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 1)
        self.assertEqual(counter.read_text(), "xxx", "1 attempt + 2 retries only")
        status, attempts, code_rc = self.latest()["flaky"]
        self.assertEqual((status, attempts, code_rc), ("failed", 3, 7))

    def test_success_after_retry_is_recorded(self):
        marker = self.dir / "tries"
        code = (
            f"import pathlib; p = pathlib.Path({str(marker)!r}); "
            "n = int(p.read_text()) + 1 if p.exists() else 1; p.write_text(str(n)); "
            "raise SystemExit(0 if n >= 2 else 5)"
        )
        flow = self.write_flow([{"id": "eventually", "argv": py(code), "retries": 3}])
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(self.latest()["eventually"][0], "succeeded")
        self.assertEqual(marker.read_text(), "2")

    def test_timeout_kills_the_process_tree(self):
        child = self.dir / "grandchild.pid"
        code = (
            "import subprocess, sys, time;"
            f"p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']);"
            f"open({str(child)!r},'w').write(str(p.pid));"
            "time.sleep(30)"
        )
        flow = self.write_flow([{"id": "slow", "argv": py(code), "timeout": 1}])
        started = time.monotonic()
        res = self.run_flow(flow)
        self.assertLess(time.monotonic() - started, 20)
        self.assertEqual(res.returncode, 1)
        status, _, _ = self.latest()["slow"]
        self.assertEqual(status, "timed_out")
        pid = int(child.read_text())
        time.sleep(0.3)
        self.assertFalse(Path(f"/proc/{pid}").exists(), "the whole process group must die")


class ResumeTests(FlowTestCase):
    def test_resume_skips_completed_tasks(self):
        log = self.dir / "log"
        ready = self.dir / "ready"
        flow = self.write_flow(
            [
                {"id": "a", "argv": py(f"open({str(log)!r},'a').write('a')")},
                {
                    "id": "b",
                    "argv": py(
                        f"import os,sys; open({str(log)!r},'a').write('b'); "
                        f"sys.exit(0 if os.path.exists({str(ready)!r}) else 9)"
                    ),
                    "depends_on": ["a"],
                },
            ]
        )
        first = self.run_flow(flow)
        self.assertEqual(first.returncode, 1)
        self.assertEqual(log.read_text(), "ab")
        ready.touch()
        second = self.run_flow(flow)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("skipped  a", second.stdout)
        self.assertEqual(log.read_text(), "ab" + "b", "a must not run twice")
        statuses = [(tid, s) for tid, s, _, _ in self.rows()]
        self.assertEqual(
            statuses,
            [("a", "succeeded"), ("b", "failed"), ("a", "skipped"), ("b", "succeeded")],
        )

    def test_resume_is_stable_across_repeated_runs(self):
        """D1 regression: a `skipped` row must never hide the earlier success.

        Four consecutive runs of the same flow: exactly one real execution per task,
        no duplicated external effects, and every later run skips both tasks.
        """
        log = self.dir / "effects.log"
        flow = self.write_flow(
            [
                {"id": "t1", "argv": py(f"open({str(log)!r},'a').write('t1')")},
                {
                    "id": "t2",
                    "argv": py(f"open({str(log)!r},'a').write('t2')"),
                    "depends_on": ["t1"],
                },
            ]
        )
        for run in range(1, 5):
            res = self.run_flow(flow)
            self.assertEqual(res.returncode, 0, f"run {run}: {res.stderr}")
            self.assertEqual(
                log.read_text(), "t1t2", f"run {run} repeated an external effect"
            )
            if run == 1:
                self.assertIn("running  t1", res.stdout)
            else:
                self.assertIn("skipped  t1", res.stdout, f"run {run}")
                self.assertIn("skipped  t2", res.stdout, f"run {run}")
                self.assertNotIn("running  t1", res.stdout, f"run {run}")
                self.assertNotIn("running  t2", res.stdout, f"run {run}")
        executed = [(tid, s) for tid, s, _, _ in self.rows() if s != "skipped"]
        self.assertEqual(executed, [("t1", "succeeded"), ("t2", "succeeded")])

    def test_flow_change_reruns_only_the_changed_command(self):
        log = self.dir / "log"
        flow = self.write_flow(
            [
                {"id": "a", "argv": py(f"open({str(log)!r},'a').write('A')")},
                {"id": "b", "argv": py(f"open({str(log)!r},'a').write('B')"), "depends_on": ["a"]},
            ]
        )
        self.assertEqual(self.run_flow(flow).returncode, 0)
        flow = self.write_flow(
            [
                {"id": "a", "argv": py(f"open({str(log)!r},'a').write('A2')")},
                {"id": "b", "argv": py(f"open({str(log)!r},'a').write('B')"), "depends_on": ["a"]},
            ]
        )
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(log.read_text(), "ABA2")
        self.assertIn("skipped  b", res.stdout)

    def test_force_task_overrides_a_previous_success(self):
        log = self.dir / "log"
        flow = self.write_flow(
            [
                {"id": "a", "argv": py(f"open({str(log)!r},'a').write('a')")},
                {"id": "b", "argv": py(f"open({str(log)!r},'a').write('b')"), "depends_on": ["a"]},
            ]
        )
        self.assertEqual(self.run_flow(flow).returncode, 0)
        res = self.cli("run", str(flow), "--db", str(self.db), "--force-task", "a")
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(log.read_text(), "abab")

    def test_killed_process_is_recovered_and_rerun_not_reported_as_done(self):
        started = self.dir / "started"
        go = self.dir / "go"
        code = (
            f"import os, time; open({str(started)!r},'w').close(); "
            f"time.sleep(0 if os.path.exists({str(go)!r}) else 30)"
        )
        flow = self.write_flow([{"id": "job", "argv": py(code)}])
        proc = subprocess.Popen(
            [sys.executable, str(AETHER), "run", str(flow), "--db", str(self.db)],
            cwd=self.dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 20
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(started.exists(), "the task never started")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=20)
        proc.stdout.close()
        proc.stderr.close()
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT status FROM task_runs").fetchone()[0], "running")
        conn.close()

        go.touch()
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("recovered 1 interrupted attempt", res.stderr)
        self.assertIn("interrupted", [s for _, s, _, _ in self.rows()])
        self.assertEqual(self.latest()["job"][0], "succeeded")

    def test_unstartable_command_fails_the_task_instead_of_crashing(self):
        """A missing binary is a failed attempt, never a dangling `running` row.

        `status` is the operator's only view (M1); a traceback here left the
        attempt and the run `running` forever, which reports work that never ran.
        """
        flow = self.write_flow([{"id": "ghost", "argv": ["/nonexistent/aether-probe-binary"]}])
        res = self.run_flow(flow)
        self.assertEqual(res.returncode, 1, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        conn = sqlite3.connect(self.db)
        status, code, error = conn.execute(
            "SELECT status, exit_code, error FROM task_runs WHERE task_id='ghost'"
        ).fetchone()
        run_status = conn.execute(
            "SELECT status FROM runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()[0]
        conn.close()
        self.assertEqual((status, code), ("failed", None))
        self.assertIn("cannot start", error)
        self.assertEqual(run_status, "failed")
        self.assertIn("failed", self.cli("status", "--db", str(self.db)).stdout)

    def test_status_subcommand_reports_latest_state(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        self.assertEqual(self.run_flow(flow).returncode, 0)
        res = self.cli("status", "--db", str(self.db))
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("a", res.stdout)
        self.assertIn("succeeded", res.stdout)


if __name__ == "__main__":
    unittest.main()
