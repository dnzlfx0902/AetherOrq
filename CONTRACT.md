# Resumption contract and invariants

Normative document for the M0 engine. If code and this document disagree, the
document is the specification and the code is the bug.

## 1. What is guaranteed, and what is not

**Guaranteed:** a task that has a recorded attempt with status `succeeded` for the
same `task_id` and the same command will not be executed again, unless you
explicitly pass `--force-task` for it (or for an upstream task, which forces it
too). This holds across process crashes, reboots and interruptions.

**NOT guaranteed:** *exactly-once* execution of processes. If the engine process
or a task process is killed mid-flight, the task is re-executed on the next run
because completion is unknown. Its external effects (files written, requests
sent, rows inserted elsewhere) may therefore happen more than once. There is no
transactional coupling to external systems, no idempotency keys and no
compensation/rollback of side effects. Tasks with non-idempotent effects must be
written to tolerate a repeated attempt.

**Explicitly out of scope:** atomicity across tasks, distributed execution,
sandboxing, concurrency (M0 is strictly sequential), and any claim of security
isolation.

## 2. Terms

- **flow** — the JSON file. Identity is its content, hashed as SHA-256 of
  canonical JSON (`runs.flow_hash`).
- **task identity for resume** — the pair (`task_id`, `command_hash`), where
  `command_hash = SHA-256(canonical JSON of argv)`. `timeout` and `retries` are
  *not* part of the identity: changing them does not re-run a completed task.
- **attempt** — one execution of one task. One `task_runs` row. A row is created
  with status `running` *before* the process starts and updated after it ends,
  so a killed process is always visible in the database.
- **run** — one invocation of `aether run`. One `runs` row.

Statuses: `running`, `succeeded`, `failed`, `timed_out`, `interrupted`,
`blocked`, `skipped`. `skipped` marks "not executed in this run because it had
already succeeded with this exact command" and is *not* a new success; the
success it refers to is the earlier `succeeded` row.

## 3. Pre-execution validation (hard gate)

Before any process starts and before the database is created, the flow must pass:

1. JSON parses and the root is an object with only `name`, `description`, `tasks`.
2. `name` is a non-empty string; `tasks` is a non-empty list.
3. Every task object uses only known keys, has a unique non-empty string `id`,
   a non-empty `argv` list of non-empty strings, `depends_on` as a list of
   strings, `timeout` a positive int, `retries` an int in `0..10`.
4. No self-dependency; every dependency names an existing task.
5. The dependency graph is acyclic (Kahn's algorithm).

Any violation: exit `2`, all problems reported, zero commands executed, zero
state written.

## 4. Execution semantics

- Order is a deterministic topological order; ready tasks are taken in
  lexicographic id order. Same flow ⇒ same order.
- `argv` is passed directly to the OS. **No shell**, no interpolation,
  no `eval`. A string `argv` is rejected at validation.
- Each attempt runs in its own process group; on timeout the whole group is
  killed with `SIGKILL`, status `timed_out`. Timeout is per attempt, not per task.
- Retries are bounded: at most `1 + retries` attempts per task per run, then the
  task is `failed`. `attempts` in the row counts the attempts made in this run.
- A task whose dependency did not end `succeeded`/`skipped` is `blocked` and not
  executed. Blocking is transitive. Independent branches still run.
- Exit code: `0` if every task ended `succeeded` or `skipped`; `1` otherwise;
  `2` invalid flow; `130` on Ctrl-C (current attempt marked `interrupted`, the
  run row marked `interrupted`).

## 5. Resume decision table

For each task in topological order, with `prior` = status of the most recent
attempt having the same `task_id` and `command_hash` **whose status is not
`skipped`**:

| prior | condition | action |
|---|---|---|
| `succeeded` | task not forced | insert `skipped` row, do not execute |
| `succeeded` | task forced (directly or as a dependent of a forced task) | execute |
| `failed` / `timed_out` / `interrupted` | — | execute |
| `blocked` | deps now `succeeded`/`skipped` | execute |
| none (new task) | — | execute |

The "most recent attempt" rule means: if a later attempt failed, an earlier
success does not resurrect the task — the newest evidence wins.

`skipped` rows are **not** evidence and are excluded from the lookup: a skipped
row only restates that this run reused an earlier success, and every skipped row
has a `succeeded` row with the same identity behind it. Counting it as the newest
status would hide that success and re-execute the task on every other run, so
repeated runs of an unchanged flow keep skipping indefinitely.

## 6. Interruption and recovery

- Engine killed (`SIGKILL`, power loss): the in-flight row stays `running` and
  the run row stays `running`. On the next invocation, before planning, the
  engine rewrites those rows to `interrupted` (run rows to `crashed`) with the
  error text `process died before the task finished; external effects may have
  happened`, prints `recovered N interrupted attempt(s)`, and then re-executes
  those tasks because their completion is unknown.
- Ctrl-C (`SIGINT`): the child process group is killed, the current attempt is
  marked `interrupted`, the run `interrupted`, exit `130`.
- Timeout: `timed_out`; it is a failure for dependency purposes and is retried
  within the run if `retries` allows.
- No task is ever reported `succeeded` without a zero exit code from its
  process. Recovery never converts an unknown outcome into success.

## 7. Flow change contract

| change | effect on next `run` |
|---|---|
| task added | executed; dependencies validated |
| task removed | its history stays in the DB; ignored by planning; any task still depending on it makes the flow invalid |
| `argv` of task T changed | T re-runs (new `command_hash`); downstream unchanged tasks are **skipped** |
| `timeout` / `retries` of T changed | T is skipped if it already succeeded — use `--force-task T` |
| dependency edges changed | graph re-validated; task identities are unchanged, so completed tasks stay skipped unless forced |
| `name` / `description` changed | new `flow_hash` recorded; task decisions unchanged |

Downstream freshness is **manual by design**: after changing an upstream command
run `--force-task <upstream>`, which also forces all transitive dependents. The
engine deliberately does not guess whether a downstream result is still valid.

## 8. Invariants for operators / sysadmin

1. One database per flow. Two different flows sharing a `--db` file would mix
   history; recovery and `status` are db-wide. Not supported in M0.
2. Single writer. Do not run two `aether run` processes against the same db
   simultaneously; there is no lock. Recovery would mark the other process's
   in-flight attempts as `interrupted`.
3. All state lives in the `--db` file (plus its `-wal`/`-shm` siblings). No
   daemons, no sockets, no network calls, no cron entries, no changes to
   environment or user config.
4. `succeeded` is terminal for a given (`task_id`, `command_hash`) until an
   explicit `--force-task`. Nothing in the engine flips it back.
5. Every executed process leaves a row with `started_at`, `finished_at`, exit
   code and capped output. Deleting the db deletes the audit trail.
6. stdout/stderr (up to 64 KiB each, keeping the first and last halves around a
   `[truncated N chars]` marker) and `argv` are stored in the database:
   treat the db as sensitive and never place secrets in commands or flow files.
7. Removing state = delete the db file. Code rollback = `git revert`; never
   delete the repository or rewrite published history.

## 9. Non-goals in M0

Parallel execution, sandboxing, secret management, remote execution, UI/TUI,
AI/LLM integration, plugin system, scheduling, retries with backoff, per-task
environment or working-directory configuration, `inspect` subcommands.

## 10. Read-only inspection (M1)

`logs`, `runs` and `status` only read the database. They add no execution
semantics and cannot change a task's status.

- `logs --db DB --task ID [--lines N]` reports the newest attempt that actually
  started a process (`skipped`/`blocked` rows never did) with its status, exit
  code, timing and the tail of stdout/stderr. Exit `2` if the task has no such
  attempt. A capped stream keeps its real end (§8.6) and is labelled
  `capture truncated`, so a partial tail is never mistaken for the full output.
- `runs --db DB` lists every run with status, task/success/failure counts and
  duration; a run still in flight has no duration.
- `status` and `runs` accept `--json`; the JSON is a single object
  (`{"tasks": [...], "runs": n}` and `{"runs": [...]}`) and parses with
  `json.loads`.
- `status` applies §5's rule to the read path: the reported row is the newest
  attempt whose status is not `skipped`, so a repeated run shows the `succeeded`
  attempt, its exit code and timestamps instead of the bookkeeping row.
  `blocked` is a real state and is reported as such.
