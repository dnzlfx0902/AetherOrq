#!/usr/bin/env python3
"""M2 step 1: bounded parallelism via `run --jobs N`.

Reuses the M0 harness (real CLI subprocess, real SQLite) and leaves the M0+M1
tests untouched. Only execution scheduling changes: dependency, blocking and
resume semantics must be identical to the sequential path.
Run: python3 -m unittest discover -s tests -v
"""
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_engine import AETHER, FlowTestCase, py  # noqa: E402


class ParallelRunTests(FlowTestCase):
    def run_jobs(self, flow_path, jobs, timeout=90):
        return self.cli("run", str(flow_path), "--db", str(self.db), "--jobs", str(jobs), timeout=timeout)

    def statuses(self):
        return {tid: status for tid, status, _, _ in self.rows()}

    def test_jobs_runs_independent_tasks_concurrently(self):
        # Stamp start/end inside each task: overlap is asserted directly instead of
        # against a wall-clock budget, which a loaded host can blow past.
        marks = self.dir / "marks"
        marks.mkdir()
        code = (
            "import time, pathlib; p = pathlib.Path({dir!r}) / {name!r}; "
            "p.write_text(str(time.time())); time.sleep(1); "
            "p.with_suffix('.end').write_text(str(time.time()))"
        )
        tasks = [
            {"id": f"s{n}", "argv": py(code.format(dir=str(marks), name=f"s{n}.start"))}
            for n in range(4)
        ]
        flow = self.write_flow(tasks)
        res = self.run_jobs(flow, 4)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        starts = [float(p.read_text()) for p in marks.glob("*.start")]
        ends = [float(p.read_text()) for p in marks.glob("*.end")]
        self.assertEqual(len(starts), 4, f"only {len(starts)}/4 tasks stamped a start")
        self.assertLess(
            max(starts),
            min(ends),
            f"--jobs 4 did not overlap: all starts {sorted(starts)} must precede the "
            f"first end {min(ends)}",
        )
        self.assertEqual(set(self.statuses().values()), {"succeeded"})

    def test_jobs_never_starts_a_child_before_its_parent_succeeds(self):
        artifact = self.dir / "parent.out"
        child = (
            "import sys, pathlib; p = pathlib.Path(sys.argv[1]); "
            "sys.exit(0 if p.exists() and p.read_text().strip() == 'parent-done' else 3)"
        )
        flow = self.write_flow(
            [
                {
                    "id": "parent",
                    "argv": py(
                        f"import time; time.sleep(0.3); open({str(artifact)!r},'w').write('parent-done')"
                    ),
                },
                {"id": "child", "argv": py(child), "depends_on": ["parent"]},
            ]
        )
        # Pass the artifact path to the child through the flow itself.
        data = flow.read_text().replace("sys.argv[1]", repr(str(artifact)))
        flow.write_text(data)
        res = self.run_jobs(flow, 2)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        self.assertEqual(self.statuses(), {"parent": "succeeded", "child": "succeeded"})

    def test_failing_branch_blocks_only_its_descendants(self):
        done = self.dir / "independent"
        flow = self.write_flow(
            [
                {"id": "bad", "argv": py("import sys; sys.exit(5)")},
                {"id": "bad_child", "argv": py("pass"), "depends_on": ["bad"]},
                {"id": "bad_grandchild", "argv": py("pass"), "depends_on": ["bad_child"]},
                {"id": "free", "argv": py(f"open({str(done)!r},'w').close()")},
            ]
        )
        res = self.run_jobs(flow, 4)
        self.assertEqual(res.returncode, 1, res.stderr + res.stdout)
        self.assertTrue(done.exists(), "an independent branch must still run")
        self.assertEqual(
            self.statuses(),
            {
                "bad": "failed",
                "bad_child": "blocked",
                "bad_grandchild": "blocked",
                "free": "succeeded",
            },
        )

    def test_resume_with_jobs_skips_completed_tasks(self):
        log = self.dir / "ran.log"
        tasks = [
            {"id": f"t{n}", "argv": py(f"open({str(log)!r},'a').write('t{n}\\n')")}
            for n in range(2)
        ]
        flow = self.write_flow(tasks)
        first = self.run_jobs(flow, 2)
        self.assertEqual(first.returncode, 0, first.stderr + first.stdout)
        second = self.run_jobs(flow, 2)
        self.assertEqual(second.returncode, 0, second.stderr + second.stdout)
        self.assertEqual(sorted(log.read_text().split()), ["t0", "t1"], "resume re-executed tasks")
        self.assertIn("skipped  t0", second.stdout)
        self.assertIn("skipped  t1", second.stdout)
        self.assertEqual(set(self.statuses().values()), {"skipped"})

    def test_kill_recovery_with_jobs_two(self):
        started = self.dir / "started"
        go = self.dir / "go"
        slow = (
            f"import os, time; open({str(started)!r},'w').close(); "
            f"time.sleep(0 if os.path.exists({str(go)!r}) else 30)"
        )
        flow = self.write_flow(
            [
                {"id": "slow", "argv": py(slow)},
                {"id": "quick", "argv": py("pass")},
            ]
        )
        proc = subprocess.Popen(
            [sys.executable, str(AETHER), "run", str(flow), "--db", str(self.db), "--jobs", "2"],
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
        assert proc.stdout is not None and proc.stderr is not None
        proc.stdout.close()
        proc.stderr.close()
        conn = sqlite3.connect(self.db)
        self.assertIn(
            "running", [r[0] for r in conn.execute("SELECT status FROM task_runs").fetchall()]
        )
        conn.close()

        go.touch()
        res = self.run_jobs(flow, 2)
        self.assertEqual(res.returncode, 0, res.stderr + res.stdout)
        self.assertIn("recovered 1 interrupted attempt", res.stderr)
        latest = self.latest()
        self.assertEqual(latest["slow"][0], "succeeded", "the interrupted task must re-run")
        self.assertEqual(latest["quick"][0], "skipped", "an untouched success must stay skipped")

    def test_interrupt_marks_attempts_interrupted_with_jobs(self):
        flow = self.write_flow(
            [
                {"id": "a", "argv": py("import time; time.sleep(20)")},
                {"id": "b", "argv": py("import time; time.sleep(20)")},
            ]
        )
        proc = subprocess.Popen(
            [sys.executable, str(AETHER), "run", str(flow), "--db", str(self.db), "--jobs", "2"],
            cwd=self.dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 20
        running = 0
        while time.monotonic() < deadline and running < 2:
            time.sleep(0.05)
            if self.db.exists():
                try:
                    conn = sqlite3.connect(self.db)
                    running = conn.execute(
                        "SELECT COUNT(*) FROM task_runs WHERE status='running'"
                    ).fetchone()[0]
                    conn.close()
                except sqlite3.OperationalError:
                    pass  # the schema is still being created
        self.assertEqual(running, 2, "both tasks should be in flight")
        proc.send_signal(signal.SIGINT)
        try:
            out, err = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.fail("--jobs run did not stop on Ctrl-C")
        self.assertEqual(proc.returncode, 130, out + err)
        self.assertEqual(set(self.statuses().values()), {"interrupted"})
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT status FROM runs").fetchone()[0], "interrupted")
        conn.close()

    def test_jobs_rejects_non_positive_values(self):
        flow = self.write_flow([{"id": "a", "argv": py("pass")}])
        res = self.run_jobs(flow, 0)
        self.assertEqual(res.returncode, 2)
        self.assertIn("--jobs must be", res.stderr)
        self.assertFalse(self.db.exists(), "an invalid --jobs must not create state")


if __name__ == "__main__":
    unittest.main()
