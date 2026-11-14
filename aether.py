#!/usr/bin/env python3
"""AetherOrq - persistent local task-flow engine (Python stdlib only).

Runs a JSON DAG of local commands, records every attempt in SQLite, and resumes
without re-running tasks that already succeeded with the same command.
See CONTRACT.md for the resumption contract and its limits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_TIMEOUT = 300
MAX_RETRIES = 10
CAPTURE_LIMIT = 65536
FLOW_KEYS = {"name", "tasks", "description"}
TASK_KEYS = {"id", "argv", "depends_on", "timeout", "retries", "cwd", "env"}

# Children currently in flight, so an interrupt can kill them all (parallel runs
# hold one process group each; the sequential path kills its own in `execute`).
LIVE: set[subprocess.Popen] = set()
LIVE_LOCK = threading.Lock()
# Set when the operator interrupts a parallel run: children then die by our own
# SIGKILL, and their attempts must be recorded as `interrupted`, not `failed`.
INTERRUPTED = threading.Event()


class FlowError(Exception):
    """Invalid flow file: reported before anything is executed."""


class LockHeld(Exception):
    """Another process is already writing this db: a second writer is refused."""


def writer_lock(db_path: str):
    """Exclusive advisory lock on `<db>.lock`, held for the whole run (CONTRACT §8.2).

    The kernel drops the lock when the holder dies, so a crashed run never leaves
    a stale lock behind. The pid is written to the file for the refusal message.
    """
    import fcntl  # ponytail: Unix only; add msvcrt.locking if Windows is ever supported

    db = Path(db_path).resolve()
    lock_path = db.with_name(db.name + ".lock")
    try:
        handle = open(lock_path, "a+")
    except OSError as exc:
        raise LockHeld(f"cannot create lock file {lock_path}: {exc}") from exc
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.seek(0)
        holder = handle.read().strip() or "unknown"
        handle.close()
        raise LockHeld(
            f"another `aether run` is already writing {db_path} "
            f"(lock {lock_path}, pid {holder}); refusing to start a second writer"
        ) from exc
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle



def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def load_flow(path: str) -> dict:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise FlowError(f"cannot read flow file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise FlowError(f"flow file is not valid JSON: {exc}") from exc
    validate_flow(data)
    # A relative `cwd` is meant to live next to the flow file, not next to
    # whatever directory the operator happened to call `aether` from.
    base = Path(path).resolve().parent
    for task in data["tasks"]:
        if "cwd" in task:
            cwd = Path(os.path.expanduser(task["cwd"]))
            task["cwd"] = str(cwd if cwd.is_absolute() else base / cwd)
    return data


def validate_flow(data) -> None:
    """Full structural + semantic validation. Raises FlowError with every problem found."""
    errors = []
    if not isinstance(data, dict):
        raise FlowError("flow root must be a JSON object")
    extra = set(data) - FLOW_KEYS
    if extra:
        errors.append(f"unknown flow keys: {sorted(extra)}")
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append("flow 'name' must be a non-empty string")
    tasks = data.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise FlowError("flow 'tasks' must be a non-empty list")
    seen = set()
    for idx, task in enumerate(tasks):
        where = f"tasks[{idx}]"
        if not isinstance(task, dict):
            errors.append(f"{where} must be an object")
            continue
        unknown = set(task) - TASK_KEYS
        if unknown:
            errors.append(f"{where} has unknown keys {sorted(unknown)}")
        tid = task.get("id")
        if not isinstance(tid, str) or not tid.strip():
            errors.append(f"{where} 'id' must be a non-empty string")
            continue
        if tid in seen:
            errors.append(f"duplicate task id {tid!r}")
        seen.add(tid)
        argv = task.get("argv")
        if not isinstance(argv, list) or not argv:
            errors.append(f"task {tid!r} 'argv' must be a non-empty list of strings")
        elif not all(isinstance(a, str) for a in argv) or not argv[0]:
            errors.append(f"task {tid!r} 'argv' must contain only non-empty strings")
        deps = task.get("depends_on", [])
        if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
            errors.append(f"task {tid!r} 'depends_on' must be a list of task ids")
        timeout = task.get("timeout", DEFAULT_TIMEOUT)
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
            errors.append(f"task {tid!r} 'timeout' must be a positive integer")
        retries = task.get("retries", 0)
        if not isinstance(retries, int) or isinstance(retries, bool) or not 0 <= retries <= MAX_RETRIES:
            errors.append(f"task {tid!r} 'retries' must be an integer 0..{MAX_RETRIES}")
        cwd = task.get("cwd")
        if cwd is not None and (not isinstance(cwd, str) or not cwd.strip()):
            errors.append(f"task {tid!r} 'cwd' must be a non-empty string")
        env = task.get("env")
        if isinstance(env, dict):
            # Inherit-by-name only: an inline map would put values, and therefore
            # secrets, in a flow file that is committed and hashed into the db.
            errors.append(
                f"task {tid!r} 'env' must be a list of variable names, not an inline map "
                "(values and secrets never belong in a flow file)"
            )
        elif env is not None:
            if (
                not isinstance(env, list)
                or not all(isinstance(v, str) and v.strip() and "=" not in v for v in env)
            ):
                errors.append(f"task {tid!r} 'env' must be a list of non-empty variable names")

    ids = {t["id"] for t in tasks if isinstance(t, dict) and isinstance(t.get("id"), str)}
    for task in tasks:
        if not isinstance(task, dict) or not isinstance(task.get("id"), str):
            continue
        for dep in task.get("depends_on", []):
            if dep == task["id"]:
                errors.append(f"task {task['id']!r} depends on itself")
            elif dep not in ids:
                errors.append(f"task {task['id']!r} depends on unknown task {dep!r}")
    if errors:
        raise FlowError("invalid flow:\n  " + "\n  ".join(errors))
    topo_order(tasks)  # cycle check, before any execution


def topo_order(tasks: list[dict]) -> list[str]:
    deps = {t["id"]: set(t.get("depends_on", [])) for t in tasks}
    children = {tid: [] for tid in deps}
    for tid, ds in deps.items():
        for dep in ds:
            children[dep].append(tid)
    indegree = {tid: len(ds) for tid, ds in deps.items()}
    ready = sorted(t for t, n in indegree.items() if n == 0)
    order = []
    while ready:
        tid = ready.pop(0)
        order.append(tid)
        for child in sorted(children[tid]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort()
    if len(order) != len(deps):
        remaining = sorted(set(deps) - set(order))
        raise FlowError(f"dependency cycle among tasks: {', '.join(remaining)}")
    return order


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_name TEXT NOT NULL,
    flow_hash TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    task_id TEXT NOT NULL,
    command_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    exit_code INTEGER,
    started_at TEXT,
    finished_at TEXT,
    stdout TEXT,
    stderr TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS task_runs_lookup ON task_runs(task_id, command_hash, attempt_id);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # `timeout`: parallel workers each write short rows; WAL serialises writers.
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def _cut(text: str | None) -> str | None:
    # Keep both ends: the head explains how a task started, the tail is what
    # `logs` tails. Head-only truncation silently dropped the end of a long
    # output, so `logs --lines N` showed lines from the middle of the capture.
    if text is None or len(text) <= CAPTURE_LIMIT:
        return text
    keep = CAPTURE_LIMIT // 2
    return f"{text[:keep]}\n[truncated {len(text) - 2 * keep} chars]\n{text[-keep:]}"


def recover(conn: sqlite3.Connection) -> int:
    """Mark leftovers of a killed process as interrupted. Never claims they succeeded."""
    cur = conn.execute(
        "UPDATE task_runs SET status='interrupted', finished_at=?, "
        "error='process died before the task finished; external effects may have happened' "
        "WHERE status='running'",
        (now(),),
    )
    conn.execute(
        "UPDATE runs SET status='crashed', finished_at=? WHERE status='running'", (now(),)
    )
    conn.commit()
    return cur.rowcount


def previous_status(conn, task_id: str, command_hash: str) -> str | None:
    # A `skipped` row is not new evidence: it only records that this run reused an
    # earlier success. Counting it as the newest status would hide that success and
    # re-execute the task on every second run (D1). Skipped rows always point at a
    # `succeeded` row with the same identity, so excluding them is lossless.
    row = conn.execute(
        "SELECT status FROM task_runs WHERE task_id=? AND command_hash=? AND status<>'skipped' "
        "ORDER BY attempt_id DESC LIMIT 1",
        (task_id, command_hash),
    ).fetchone()
    return row[0] if row else None


def execute(
    argv: list[str], timeout: int, cwd: str | None = None, env: list[str] | None = None
) -> tuple[str, int | None, str, str, str | None]:
    """Run one attempt. No shell: argv is passed to the OS verbatim.

    `cwd` defaults to the engine's own directory; `env` is a list of variable
    names inherited from the engine's environment (a strict allowlist — anything
    else is absent in the child), `None` inherits everything (M0 behaviour).
    """
    child_env = None if env is None else {k: os.environ[k] for k in env if k in os.environ}
    try:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            start_new_session=True,  # own process group, so a timeout kills the whole tree
        )
    except OSError as exc:
        # A missing `cwd` or an unusable executable is a failed attempt, not a
        # crashed engine: record it instead of leaving a `running` row behind.
        return "failed", None, "", "", f"cannot start: {exc}"
    with LIVE_LOCK:
        LIVE.add(proc)
    try:
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            out, err = proc.communicate()
            return "timed_out", None, out or "", err or "", f"timeout after {timeout}s"
        except BaseException:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.communicate()
            raise
    finally:
        with LIVE_LOCK:
            LIVE.discard(proc)
    if INTERRUPTED.is_set() and proc.returncode is not None and proc.returncode < 0:
        return "interrupted", proc.returncode, out or "", err or "", "interrupted by the operator"
    if proc.returncode == 0:
        return "succeeded", 0, out or "", err or "", None
    return "failed", proc.returncode, out or "", err or "", f"exit code {proc.returncode}"


def kill_live() -> None:
    """Kill every child still in flight (parallel runs; each has its own group)."""
    INTERRUPTED.set()
    with LIVE_LOCK:
        procs = list(LIVE)
    for proc in procs:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def open_attempt(conn, run_id: int, task: dict, command_hash: str) -> int:
    cur = conn.execute(
        "INSERT INTO task_runs (run_id, task_id, command_hash, status, started_at) "
        "VALUES (?, ?, ?, 'running', ?)",
        (run_id, task["id"], command_hash, now()),
    )
    conn.commit()
    return cur.lastrowid


def close_attempt(conn, attempt_id: int, status: str, attempts: int, exit_code, out, err, errmsg):
    conn.execute(
        "UPDATE task_runs SET status=?, attempts=?, exit_code=?, finished_at=?, "
        "stdout=?, stderr=?, error=? WHERE attempt_id=?",
        (status, attempts, exit_code, now(), _cut(out), _cut(err), errmsg, attempt_id),
    )
    conn.commit()


def _record_blocked(conn, run_id: int, task: dict, command_hash: str) -> None:
    conn.execute(
        "INSERT INTO task_runs (run_id, task_id, command_hash, status, finished_at, error) "
        "VALUES (?, ?, ?, 'blocked', ?, ?)",
        (run_id, task["id"], command_hash, now(), "upstream task did not succeed"),
    )
    conn.commit()
    print(f"blocked  {task['id']}")


def _run_task(conn, run_id: int, task: dict, forced: set[str]) -> str:
    """Resume decision plus bounded retries for one task. Returns the final status."""
    tid = task["id"]
    command_hash = sha(task["argv"])
    prior = previous_status(conn, tid, command_hash)
    if prior == "succeeded" and tid not in forced:
        conn.execute(
            "INSERT INTO task_runs (run_id, task_id, command_hash, status, finished_at, error) "
            "VALUES (?, ?, ?, 'skipped', ?, 'already succeeded with the same command')",
            (run_id, tid, command_hash, now()),
        )
        conn.commit()
        print(f"skipped  {tid} (already succeeded with the same command)")
        return "skipped"

    timeout = task.get("timeout", DEFAULT_TIMEOUT)
    retries = task.get("retries", 0)
    attempt_id = open_attempt(conn, run_id, task, command_hash)
    status, code, out, err, errmsg = "failed", None, "", "", "not attempted"
    attempts = 0
    for attempts in range(1, retries + 2):
        print(f"running  {tid} (attempt {attempts}/{retries + 1})", flush=True)
        status, code, out, err, errmsg = execute(
            task["argv"], timeout, task.get("cwd"), task.get("env")
        )
        if status in ("succeeded", "interrupted"):
            break
        if attempts <= retries:
            print(f"retrying {tid} after {status}: {errmsg}", file=sys.stderr, flush=True)
    close_attempt(conn, attempt_id, status, attempts, code, out, err, errmsg)
    print(f"{status:<8} {tid} (exit={code})", flush=True)
    return status


def _run_sequential(conn, run_id: int, order: list[str], tasks: dict, forced: set[str]) -> bool:
    results: dict[str, str] = {}
    failed = False
    for tid in order:
        task = tasks[tid]
        dep_states = [results.get(d, "pending") for d in task.get("depends_on", [])]
        if any(s not in ("succeeded", "skipped") for s in dep_states):
            results[tid] = "blocked"
            _record_blocked(conn, run_id, task, sha(task["argv"]))
            failed = True
            continue
        results[tid] = _run_task(conn, run_id, task, forced)
        if results[tid] not in ("succeeded", "skipped"):
            failed = True
    return failed


def _task_worker(db_path: str, run_id: int, task: dict, forced: set[str]) -> str:
    # One connection per worker thread: sqlite connections are not shared across
    # threads, and the writes are short (WAL serialises the writers).
    conn = connect(db_path)
    try:
        return _run_task(conn, run_id, task, forced)
    finally:
        conn.close()


def _run_parallel(
    conn, db_path: str, run_id: int, order: list[str], tasks: dict, forced: set[str], jobs: int
) -> bool:
    """Bounded parallelism: at most `jobs` tasks in flight, deps still gate starts.

    Same semantics as the sequential loop (blocking is transitive, a failing
    branch only blocks its descendants); ready tasks are taken in id order.
    """
    children: dict[str, list[str]] = {tid: [] for tid in order}
    pending: dict[str, int] = {}
    for tid in order:
        deps = tasks[tid].get("depends_on", [])
        pending[tid] = len(deps)
        for dep in deps:
            children[dep].append(tid)
    results: dict[str, str] = {}
    failed = False
    ready = sorted(tid for tid in order if pending[tid] == 0)
    inflight = {}
    pool = ThreadPoolExecutor(max_workers=jobs)
    try:
        while ready or inflight:
            while ready and len(inflight) < jobs:
                tid = ready.pop(0)
                task = tasks[tid]
                dep_states = [results.get(d, "pending") for d in task.get("depends_on", [])]
                if any(s not in ("succeeded", "skipped") for s in dep_states):
                    results[tid] = "blocked"
                    _record_blocked(conn, run_id, task, sha(task["argv"]))
                    failed = True
                    for child in children[tid]:
                        pending[child] -= 1
                        if pending[child] == 0:
                            ready.append(child)
                    ready.sort()
                    continue
                inflight[pool.submit(_task_worker, db_path, run_id, task, forced)] = tid
            if not inflight:
                continue
            done, _ = wait(inflight, return_when=FIRST_COMPLETED)
            for fut in done:
                tid = inflight.pop(fut)
                status = fut.result()
                results[tid] = status
                if status not in ("succeeded", "skipped"):
                    failed = True
                for child in children[tid]:
                    pending[child] -= 1
                    if pending[child] == 0:
                        ready.append(child)
                ready.sort()
    except BaseException:
        # Ctrl-C (or anything else) in the main thread: the children live in their
        # own process groups, so kill them explicitly before the pool joins.
        kill_live()
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    pool.shutdown(wait=True)
    return failed


def run_flow(flow: dict, db_path: str, force_tasks: set[str] | None = None, jobs: int = 1) -> int:
    # One writer per db: the lock is taken before the db is opened, so a refused
    # run touches no state at all, and it is dropped when this process exits.
    handle = writer_lock(db_path)
    try:
        return _run_flow_locked(flow, db_path, force_tasks, jobs)
    finally:
        handle.close()


def _run_flow_locked(
    flow: dict, db_path: str, force_tasks: set[str] | None = None, jobs: int = 1
) -> int:
    force_tasks = force_tasks or set()
    order = topo_order(flow["tasks"])
    tasks = {t["id"]: t for t in flow["tasks"]}
    # Forcing a task also forces everything downstream: a stale downstream result
    # would otherwise be reported as valid after its input changed.
    dependents = {t["id"]: [] for t in flow["tasks"]}
    for t in flow["tasks"]:
        for dep in t.get("depends_on", []):
            dependents[dep].append(t["id"])
    forced = set(force_tasks)
    queue = list(forced)
    while queue:
        for child in dependents[queue.pop()]:
            if child not in forced:
                forced.add(child)
                queue.append(child)
    conn = connect(db_path)
    stale = recover(conn)
    if stale:
        print(f"recovered {stale} interrupted attempt(s) from a previous process", file=sys.stderr)
    flow_hash = sha(flow)
    run_id = conn.execute(
        "INSERT INTO runs (flow_name, flow_hash, started_at, status) VALUES (?, ?, ?, 'running')",
        (flow["name"], flow_hash, now()),
    ).lastrowid
    assert run_id is not None  # an INSERT always assigns a rowid
    conn.commit()

    try:
        if jobs > 1:
            failed = _run_parallel(conn, db_path, run_id, order, tasks, forced, jobs)
        else:
            failed = _run_sequential(conn, run_id, order, tasks, forced)
    except KeyboardInterrupt:
        recover(conn)
        conn.execute(
            "UPDATE runs SET status='interrupted', finished_at=? WHERE run_id=?", (now(), run_id)
        )
        conn.commit()
        print("interrupted; resume with the same command", file=sys.stderr)
        return 130

    final = "failed" if failed else "succeeded"
    conn.execute(
        "UPDATE runs SET status=?, finished_at=? WHERE run_id=?", (final, now(), run_id)
    )
    conn.commit()
    conn.close()
    return 1 if failed else 0


def _require_db(db_path: str) -> bool:
    if not Path(db_path).exists():
        print(f"no database at {db_path}", file=sys.stderr)
        return False
    return True


def _emitted(row) -> dict:
    keys = ("task_id", "status", "attempts", "exit_code", "started_at", "finished_at", "flow_name")
    return dict(zip(keys, row))


def show_status(db_path: str, as_json: bool = False) -> int:
    if not _require_db(db_path):
        return 2
    conn = connect(db_path)
    rows = [
        _emitted(row)
        for row in conn.execute(
            "SELECT t.task_id, t.status, t.attempts, t.exit_code, t.started_at, t.finished_at, r.flow_name "
            "FROM task_runs t JOIN runs r ON r.run_id = t.run_id "
            # `skipped` is bookkeeping, not a state: it only restates an earlier
            # success (CONTRACT §2), so counting it would hide the real status,
            # exit code and timestamps on every run after the first. `blocked` is
            # a real state and stays visible.
            "WHERE t.attempt_id IN (SELECT MAX(attempt_id) FROM task_runs "
            "WHERE status<>'skipped' GROUP BY task_id) "
            "ORDER BY t.task_id"
        ).fetchall()
    ]
    run_count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    conn.close()
    if as_json:
        print(json.dumps({"tasks": rows, "runs": run_count}, indent=2))
        return 0
    if not rows:
        print("no attempts recorded")
        return 0
    print(f"{'task':<20} {'status':<11} {'tries':>5} {'exit':>4}  finished")
    for row in rows:
        print(
            f"{row['task_id']:<20} {row['status']:<11} {row['attempts']:>5} "
            f"{str(row['exit_code']):>4}  {row['finished_at'] or row['started_at']}"
        )
    print(f"\nflow(s) in this db: {rows[0]['flow_name']!r}; runs: {run_count}")
    return 0


def show_runs(db_path: str, as_json: bool = False) -> int:
    if not _require_db(db_path):
        return 2
    conn = connect(db_path)
    rows = conn.execute(
        "SELECT r.run_id, r.flow_name, r.status, r.started_at, r.finished_at, "
        "COUNT(t.attempt_id), "
        "COALESCE(SUM(t.status='succeeded'), 0), "
        "COALESCE(SUM(t.status IN ('failed','timed_out','interrupted')), 0) "
        "FROM runs r LEFT JOIN task_runs t ON t.run_id = r.run_id "
        "GROUP BY r.run_id ORDER BY r.run_id"
    ).fetchall()
    conn.close()
    runs = [
        {
            "run_id": run_id,
            "flow_name": flow_name,
            "status": status,
            "started_at": started,
            "finished_at": finished,
            "duration_seconds": _seconds(started, finished),
            "tasks": tasks,
            "succeeded": ok,
            "failed": bad,
        }
        for run_id, flow_name, status, started, finished, tasks, ok, bad in rows
    ]
    if as_json:
        print(json.dumps({"runs": runs}, indent=2))
        return 0
    if not runs:
        print("no runs recorded")
        return 0
    print(f"{'run':>4} {'status':<11} {'tasks':>5} {'ok':>3} {'fail':>4} {'secs':>7}  started")
    for r in runs:
        secs = "-" if r["duration_seconds"] is None else f"{r['duration_seconds']:.1f}"
        print(
            f"{r['run_id']:>4} {r['status']:<11} {r['tasks']:>5} {r['succeeded']:>3} "
            f"{r['failed']:>4} {secs:>7}  {r['started_at']}  {r['flow_name']}"
        )
    return 0


def _seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        return round((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(), 3)
    except ValueError:
        return None


def _tail(text: str | None, lines: int) -> list[str]:
    stored = (text or "").splitlines()
    return stored[-lines:]


def show_logs(db_path: str, task_id: str, lines: int) -> int:
    """Tail of the most recent attempt that actually ran a process.

    `skipped` and `blocked` rows never start a process and carry no output, so they
    are not attempts for this purpose; the newest real attempt wins.
    """
    lines = max(1, lines)
    if not _require_db(db_path):
        return 2
    conn = connect(db_path)
    row = conn.execute(
        "SELECT attempt_id, run_id, status, attempts, exit_code, started_at, finished_at, "
        "stdout, stderr, error FROM task_runs "
        "WHERE task_id=? AND status NOT IN ('skipped','blocked') "
        "ORDER BY attempt_id DESC LIMIT 1",
        (task_id,),
    ).fetchone()
    conn.close()
    if row is None:
        print(f"no stored attempt for task {task_id!r}", file=sys.stderr)
        return 2
    attempt_id, run_id, status, attempts, code, started, finished, out, err, errmsg = row
    print(
        f"task {task_id}  run {run_id}  attempt #{attempt_id}  status {status}  "
        f"tries {attempts}  exit {code}"
    )
    print(f"started {started or '-'}  finished {finished or '-'}" + (f"  error: {errmsg}" if errmsg else ""))
    for name, text in (("stdout", out), ("stderr", err)):
        # The capture is capped, so the tail can sit behind dropped middle lines.
        # Say so instead of letting the operator trust an incomplete view.
        cut = "; capture truncated" if text and "[truncated " in text else ""
        print(f"--- {name} (last {lines} lines{cut}) ---" if text else f"--- {name}: empty ---")
        for line in _tail(text, lines):
            print(line)
    return 0


def _version() -> str:
    """Installed distribution version, else the version pyproject declares.

    `python3 aether.py --version` runs from a checkout where the distribution
    metadata does not exist, so PackageNotFoundError is the normal case there.
    The fallback reads pyproject.toml instead of repeating the number, so a
    version bump cannot leave the CLI reporting a stale one.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("aetherorq")
    except PackageNotFoundError:
        pass

    pyproject = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pyproject.toml")
    if os.path.exists(pyproject):
        with open(pyproject, encoding="utf-8") as fh:
            for line in fh:
                # The [project] table's `version`; build-system keys have none.
                key, sep, value = line.partition("=")
                if sep and key.strip() == "version":
                    return value.strip().strip("\"'")
    return "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aether", description="AetherOrq local task-flow engine")
    parser.add_argument(
        "--version", action="version", version=f"aether {_version()}"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="validate and execute a flow")
    p_run.add_argument("flow")
    p_run.add_argument("--db", help="SQLite state file (default: <flow>.db)")
    p_run.add_argument(
        "--force-task",
        action="append",
        default=[],
        metavar="ID",
        help="re-run this task even if it already succeeded (repeatable)",
    )

    p_run.add_argument(
        "--jobs",
        type=int,
        default=1,
        metavar="N",
        help="run up to N independent ready tasks concurrently (default 1 = sequential)",
    )

    p_val = sub.add_parser("validate", help="validate a flow without executing anything")
    p_val.add_argument("flow")

    p_status = sub.add_parser("status", help="show the last state of every task")
    p_status.add_argument("--db", required=True)
    p_status.add_argument("--json", action="store_true", help="machine-readable output")

    p_logs = sub.add_parser("logs", help="tail of the last stored attempt output for one task")
    p_logs.add_argument("--db", required=True)
    p_logs.add_argument("--task", required=True, metavar="ID")
    p_logs.add_argument("--lines", type=int, default=40, help="tail length (default 40)")

    p_runs = sub.add_parser("runs", help="run history with status and timing")
    p_runs.add_argument("--db", required=True)
    p_runs.add_argument("--json", action="store_true", help="machine-readable output")

    args = parser.parse_args(argv)
    if args.cmd == "run" and args.jobs < 1:
        print("--jobs must be an integer >= 1", file=sys.stderr)
        return 2
    if args.cmd == "status":
        return show_status(args.db, args.json)
    if args.cmd == "logs":
        return show_logs(args.db, args.task, args.lines)
    if args.cmd == "runs":
        return show_runs(args.db, args.json)
    try:
        flow = load_flow(args.flow)
    except FlowError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.cmd == "validate":
        print(f"ok: {flow['name']} ({len(flow['tasks'])} tasks)")
        return 0
    db_path = args.db or str(Path(args.flow).with_suffix(".db"))
    try:
        return run_flow(flow, db_path, set(args.force_task), args.jobs)
    except LockHeld as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
