# Roadmap

Milestones, not calendar. Each one closes only when its acceptance is verifiable
by running commands. Work runs as a continuous cycle over the agreed scope: the
pause belongs at the **close of that whole agreed scope/release**, not after any
single milestone (M0 included); nothing outside the agreed scope is committed
without an explicit decision at that point.

## M0 — Kernel and persistence (this release)

Delivered: JSON flow, full validation and cycle detection before execution,
explicit `argv` without shell, deterministic sequential DAG execution, bounded
retries, per-attempt timeout killing the process group, SQLite WAL state with
per-attempt results, interruption recovery, resume without re-running completed
tasks, `validate` / `run` / `status`, `--force-task`, documented resumption
contract.

Acceptance (all runnable, all already passing):

- `python3 -m unittest discover -s tests -v` → 15 tests, `OK`.
- A flow with a cycle exits `2`, prints the cycle, executes nothing, creates no db.
- A failing task leaves `failed`; its descendants are `blocked`; independent
  branches still run; exit code `1`.
- A task with `retries: 2` that always fails runs exactly 3 times.
- A task exceeding `timeout` is `timed_out` and its whole process group dies.
- Killing the engine with `SIGKILL` mid-task is recovered on the next run: the
  old attempt becomes `interrupted`, the task re-runs, nothing is reported as
  succeeded without exit code 0.
- Re-running after a partial failure skips the completed tasks (asserted by a
  side-effect log) and finishes the rest — repeatedly: a third and fourth run
  still execute nothing new.

## M1 — Operator visibility

Delivered: `aether logs --db DB --task ID [--lines N]` (tail of the newest stored
attempt's stdout/stderr plus its status, exit code and timing; `skipped`/`blocked`
rows carry no output and are not counted as attempts) and `aether runs --db DB`
(run history with status, task/failure counts and wall-clock duration), plus
`--json` machine-readable output for `status` and `runs`. No new execution
semantics: M0's resume contract and schema are unchanged.

Acceptance (all runnable, all already passing):

- `python3 -m unittest discover -s tests -v` → 24 tests (M0's 15 unchanged), `OK`.
- `aether logs --db DB --task ID` prints the tail of the last stored attempt for a
  task with several attempts, and no older attempt's output; for output over the
  64 KiB cap it still prints the real last lines and says `capture truncated`.
- `aether runs --db DB` lists every run with its status and timing.
- `status --json` and `runs --json` parse with `json.loads`.
- `status` reports the newest attempt that is not a `skipped` row, so the real
  state and exit code survive repeated runs; `blocked` stays visible.

## M2 — Bounded parallelism (next in the cycle)

`run --jobs N` executing independent ready tasks concurrently while preserving
the same resume contract and dependency semantics.

Acceptance: with `--jobs 4` and 4 independent 1 s sleeps the wall time is
< 2.5 s; a dependency still never starts before its parent succeeds; a failing
branch still blocks only its descendants; kill-recovery and resume tests pass
with `--jobs 2`; the single-writer invariant is enforced by a lock file that
makes a second concurrent `run` fail with a clear error.

## M3 — Per-task execution context

Per-task `cwd` and an explicit environment allowlist (inherit-by-name only, no
inline values in the flow file), so tasks stop depending on the caller's shell.

Acceptance: a task with `cwd` runs there (asserted by a test reading `os.getcwd()`
output); a variable not in the allowlist is absent in the child; no secret can be
inlined in a flow file (inline maps rejected at validation).

## M4 — v1.0 packaging

`pyproject.toml` with the `aether` entry point, CI running the suite on the
supported Python versions, tagged `v1.0.0`, published docs reflecting the final
contract.

Acceptance: `pip install .` in a clean venv yields a working `aether` command;
CI green on every supported version; the tag exists and its commit passes the
same suite; CONTRACT.md updated for every behaviour added in M1–M3.

## Not planned

UI/TUI, LLM/agent integration, plugin marketplace, distributed execution,
sandbox claims. AetherOrq runs trusted local commands; it is not a security
boundary.
