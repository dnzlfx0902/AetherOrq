# AetherOrq

Persistent local task-flow engine. One Python file, standard library only.

AetherOrq reads a JSON flow (a DAG of tasks), validates it completely, runs each
task as an explicit `argv` list (never through a shell), and records every
attempt in SQLite. If a run is interrupted, re-running the same command resumes:
tasks that already succeeded with the same command are not executed again.

This is the first increment: the kernel, not the product. No UI, no AI, no
agent framework yet.

## Requirements

Python 3.10+ (stdlib only; developed and tested on 3.14). Nothing to install.

## Install

```bash
git clone https://github.com/dnzlfx0902/AetherOrq.git
cd AetherOrq
python3 aether.py --help        # no install step; run it from the checkout
```

Optional: `install -m755 aether.py ~/.local/bin/aether` to get `aether` on PATH.

## Flow format

```json
{
  "name": "demo",
  "tasks": [
    {"id": "compile", "argv": ["make", "build"], "timeout": 120, "retries": 1},
    {"id": "test", "argv": ["python3", "-m", "unittest", "discover"],
     "depends_on": ["compile"], "timeout": 60}
  ]
}
```

| field | required | meaning |
|---|---|---|
| `name` | yes | flow name, stored with every run |
| `tasks` | yes | non-empty list of task objects |
| `tasks[].id` | yes | unique non-empty string |
| `tasks[].argv` | yes | non-empty list of non-empty strings, executed without a shell |
| `tasks[].depends_on` | no | list of task ids that must succeed first (default `[]`) |
| `tasks[].timeout` | no | seconds per attempt, positive int (default `300`) |
| `tasks[].retries` | no | extra attempts after a failure, `0..10` (default `0`) |

Unknown keys are rejected, so typos fail loudly instead of being ignored.
Validation covers: JSON syntax, types, duplicate ids, self-dependencies,
unknown dependencies and cycles — all before any command runs. On an invalid
flow nothing is executed and no database is written (exit code `2`).

## Usage

```bash
python3 aether.py validate examples/flow.json
python3 aether.py run examples/flow.json --db demo.db
python3 aether.py run examples/flow.json --db demo.db --jobs 4
python3 aether.py status --db demo.db
python3 aether.py logs --db demo.db --task write_artifact --lines 20
python3 aether.py runs --db demo.db
python3 aether.py status --db demo.db --json
```

`status` and `runs` accept `--json`. `logs` prints the tail of the newest stored
attempt (stdout and stderr, `--lines` default 40); `skipped`/`blocked` rows never
ran a process and are not counted as attempts. A stream over 64 KiB keeps its
real end and is labelled `capture truncated`. `runs` lists each run with its
status, task/failure counts and duration.

`run` exit codes: `0` every task succeeded (or was already done), `1` at least
one task failed/blocked, `2` invalid flow, unreadable file, or another `aether
run` already writing that db, `130` interrupted by Ctrl-C.

Each run prints one line per task:

```
running  write_artifact (attempt 1/2)
succeeded write_artifact (exit=0)
skipped  greet (already succeeded with the same command)
```

`--force-task ID` re-runs a task even if it already succeeded, and re-runs
everything downstream of it (repeatable). Use it after changing something the
task depends on outside the flow file.

`--jobs N` runs up to `N` *independent* ready tasks at the same time (default
`1`, strictly sequential). Dependencies still gate starts, a failing branch still
blocks only its descendants, and the resume contract is unchanged; the output
lines of concurrent tasks interleave. Ctrl-C kills every task in flight and
exits `130`.

Only one `run` writes a db at a time: `run` holds an exclusive lock on
`<db>.lock` for its whole duration, so a second `run` on the same db exits `2`
with a clear error and records nothing. The lock dies with the process, so a
crashed run leaves no stale lock, and `status`/`logs`/`runs` stay readable while
a run is in flight.

Failed tasks block their descendants (transitively) but do not block unrelated
branches: the engine keeps going and exits `1`. Blocked tasks get a row with
status `blocked` and are never silently dropped.

## State

One SQLite database per flow (`--db`). Schema: `runs` (one row per invocation)
and `task_runs` (one row per attempt, with status, exit code, attempts, stdout,
stderr, timestamps; output capped at 64 KiB per stream, keeping the first and
last halves). WAL mode.

Skip decisions use the last attempt for the same `task_id` **and** the same
command hash (SHA-256 of the canonical `argv`), ignoring rows that only record a
skip: an unchanged flow keeps skipping on the 3rd, 4th and every later run, not
just the second. Changing a task's command makes it run again; changing an
unrelated task does not. Changing an upstream command
does **not** invalidate downstream tasks automatically — pass `--force-task` for
that. Full contract, invariants and limits: [CONTRACT.md](CONTRACT.md).

Resetting everything is `rm flow.db` (or point `--db` elsewhere); nothing else
lives outside that file.

## Security boundary

AetherOrq runs **trusted local commands that you wrote or explicitly approved**.
`argv` is passed straight to the OS with no shell, so there is no quoting or
metacharacter surface, but there is also **no sandboxing**: tasks run with your
user's permissions. Do not feed it flows from untrusted sources, and do not put
secrets in flow files or in commands — they are stored in the SQLite database
(including stdout/stderr).

## Tests

```bash
python3 -m unittest discover -s tests -v
```

36 end-to-end tests (M0's 15, M1's 9, 7 for `--jobs`, 5 for the writer lock). They
drive the real CLI in subprocesses
and inspect the real database: cycle rejected before execution, unknown/duplicate
ids, shell string rejected, dependency order, failure propagation, bounded retries,
process-tree kill on timeout, resume without re-running completed tasks across
four consecutive runs, flow change semantics, forced re-run, SIGKILL recovery,
`status` output, `logs` tail of the last attempt, `runs` history, `--json`,
concurrent wall-clock, dependency gating and Ctrl-C under `--jobs`, and the
writer lock (second run refused, release after success/failure/SIGKILL).

## Roadmap

Short, milestone-based, each with verifiable acceptance. Details in
[ROADMAP.md](ROADMAP.md). M0 is this release; work then continues in the agreed
cycle until the whole agreed scope is closed — the pause belongs at the end of
that scope, not after M0.

## License

MIT — see [LICENSE](LICENSE).
