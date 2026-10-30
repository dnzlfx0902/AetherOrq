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
python3 aether.py status --db demo.db
```

`run` exit codes: `0` every task succeeded (or was already done), `1` at least
one task failed/blocked, `2` invalid flow or unreadable file, `130` interrupted
by Ctrl-C.

Each run prints one line per task:

```
running  write_artifact (attempt 1/2)
succeeded write_artifact (exit=0)
skipped  greet (already succeeded with the same command)
```

`--force-task ID` re-runs a task even if it already succeeded, and re-runs
everything downstream of it (repeatable). Use it after changing something the
task depends on outside the flow file.

Failed tasks block their descendants (transitively) but do not block unrelated
branches: the engine keeps going and exits `1`. Blocked tasks get a row with
status `blocked` and are never silently dropped.

## State

One SQLite database per flow (`--db`). Schema: `runs` (one row per invocation)
and `task_runs` (one row per attempt, with status, exit code, attempts, stdout,
stderr, timestamps; output truncated at 64 KiB per stream). WAL mode.

Skip decisions use the last attempt for the same `task_id` **and** the same
command hash (SHA-256 of the canonical `argv`). Changing a task's command makes
it run again; changing an unrelated task does not. Changing an upstream command
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

14 end-to-end tests. They drive the real CLI in subprocesses and inspect the
real database: cycle rejected before execution, unknown/duplicate ids, shell
string rejected, dependency order, failure propagation, bounded retries,
process-tree kill on timeout, resume without re-running completed tasks, flow
change semantics, forced re-run, SIGKILL recovery, `status` output.

## Roadmap

Short, milestone-based, each with verifiable acceptance. Details in
[ROADMAP.md](ROADMAP.md). M0 is this increment; M1 is next and **awaits a
decision**.

## License

MIT — see [LICENSE](LICENSE).
