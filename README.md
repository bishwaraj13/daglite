# Daglite

<p align="center">
  <b>A lightweight Python workflow engine.</b><br>
  Simple. Observable. Installable anywhere.
</p>

---

# ✨ Overview

Daglite is a **lightweight workflow orchestration engine** designed to run **anywhere Python runs**.

It provides:

* A **state-driven workflow engine**
* **SQLite persistence**
* **Retryable task execution**
* **Full event timeline**
* **File-based logs**
* **Built-in web dashboard**

Daglite focuses on **simplicity and transparency** — no Kubernetes, no distributed brokers, no complex services. Just Python + SQLite + local filesystem.

---

# 🚀 Key Features

### Simple Workflow Definition

Write workflows as plain Python functions.

```python
from daglite import Flow, task

@task
def extract():
    return {"value": 1}

@task(deps=["extract"])
def transform(extract):
    return {"result": extract["value"] + 1}

flow = Flow(
    name="example",
    tasks={
        "extract": extract,
        "transform": transform
    },
    edges=[("extract", "transform")]
)
```

### Observable Execution

Every execution is fully tracked — runs, task runs, attempts, logs, and artifacts. You can inspect the full timeline, retry history, and dependency graph at any time.

### Built-in Dashboard

```bash
uv run python -m daglite.cli server
```

Open `http://localhost:8080` to see workflow runs, DAG visualization, task state transitions, retry history, and live logs.

### Lightweight Architecture

Core stack: Python, SQLite (WAL mode), aiohttp, plain HTML/CSS/JS. No heavy infrastructure required.

---

# 🧠 Architecture

```text
+-----------------------------+
|           UI                |
|   HTML / CSS / JS Dashboard |
+--------------▲--------------+
               |
               | HTTP API
               |
+--------------┴--------------+
|         Server / API        |
|   aiohttp + SSE streaming   |
+--------------▲--------------+
               |
               | SQLite (shared state)
               |
+-------+------┴------+-------+
|       |             |       |
| Sched |   Runner    | Worker|
| uler  |  (executor) | (loop)|
+-------+-------------+-------+
```

All components share one SQLite database. This is what allows the scheduler, workers, and server to run as separate processes — they all read and write the same `.daglite/state.db`.

---

# ⚙️ Core Concepts

### Flow
A workflow definition containing tasks, their dependencies, and versioning. Example DAG: `Task A → Task B → Task C`.

### Run
A single execution of a flow. State transitions: `CREATED → RUNNING → SUCCEEDED / FAILED`.

### TaskRun
Execution of a specific task within a run. States: `WAITING → READY → RUNNING → SUCCEEDED / FAILED`.

### Attempt
Each retry creates a new attempt (e.g., Attempt 1 → FAILED, Attempt 2 → SUCCEEDED). Attempts capture logs, errors, timing, and outputs.

### Artifact
Outputs produced by tasks. Small outputs (< 64KB) are stored inline in SQLite; large outputs are stored as files in `.daglite/artifacts/`.

### Events
An append-only event log records every state change (e.g., `run.created`, `task_run.state`). Powers debugging, observability, and live UI updates via SSE.

---

# ⚙️ How the Three Core Components Work

Understanding these three pieces is key to understanding how daglite runs your flow.

### Scheduler
The **traffic police**. It does not execute anything. Its only job is to look at the DB every 0.5 seconds and ask: *"Are all dependencies of this task done? If yes, promote it from WAITING → READY."* It also transitions the run itself — CREATED → RUNNING → SUCCEEDED/FAILED — based on task states.

### Runner
The **actual executor**. When given a task, it:
1. Atomically claims it from the DB (sets state to RUNNING)
2. Calls the Python function
3. Saves the result as an artifact
4. On failure: schedules a retry (back to WAITING with a `not_before` delay) or marks it FAILED permanently

### Worker
A **loop that drives the Runner**. It continuously calls `runner.claim_one()` every 0.2 seconds. If a READY task is available, it hands it to the runner for execution. It also holds the flow object — because the DB only stores task names, not the actual Python functions.

### How they interact:

```
Scheduler        Worker + Runner
    ↓                  ↓
WAITING → READY → claim → execute → SUCCEEDED
    ↑                                   ↓
    └──── next task becomes READY ───────┘
```

---

# 🖥 Running Commands

## Option 1: All-in-one (simplest)

Runs the scheduler, worker, and flow in a single process. Exits when the flow finishes.

```bash
uv run python -m daglite.cli run myflow:flow
```

Use this for local development or simple one-off runs.

---

## Option 2: Separate processes (for parallelism)

This is for when your flow has many tasks and you want them executed in parallel.

**Why separate?** With `daglite run`, there is only one worker — tasks execute one at a time even if multiple are READY simultaneously. With multiple worker processes, each picks up a different task and runs them truly in parallel.

**The key insight:** You only need **one scheduler**, but you can have **as many workers as you want**. The scheduler just updates DB state — it's lightweight and one is enough. Workers do the heavy lifting, so more workers = more parallelism.

**Terminal 1** — Start one scheduler + one worker:
```bash
uv run python -m daglite.cli worker myflow:flow --with-scheduler
```

**Terminal 2** — Add another worker (no scheduler needed):
```bash
uv run python -m daglite.cli worker myflow:flow
```

**Terminal 3** — Add another worker:
```bash
uv run python -m daglite.cli worker myflow:flow
```

Workers coordinate safely through SQLite — the claim operation is atomic (`BEGIN IMMEDIATE`), so two workers can never pick up the same task.

> **Why run a worker without `--with-scheduler`?**
> Because if you run `--with-scheduler` on all workers, you have multiple schedulers writing to the DB simultaneously — unnecessary contention. One scheduler is enough. All other workers just need the `--with-scheduler` flag omitted.

---

## Option 3: Start the UI server

```bash
uv run python -m daglite.cli server --host 127.0.0.1 --port 8080
```

The server reads from the same `.daglite/state.db` as your workers. It streams live updates via SSE — open `http://localhost:8080` while your flow is running to see task states update in real time.

---

# 🗄 Database

**Location:** `.daglite/state.db`

Daglite uses SQLite with WAL mode for concurrent reads. Design principles: append-only event log, indexed queries for UI, minimal writes, durable state transitions.

---

# 🔁 Retry System

Tasks define retry behavior via a `RetryPolicy`:

```python
from daglite.models import RetryPolicy

@task(
    retry=RetryPolicy(
        max_attempts=3,
        strategy="exponential",   # or "fixed"
        base_delay_seconds=1.0
    )
)
def my_task():
    ...
```

On failure, the task goes back to `WAITING` with a `not_before_utc` timestamp. The scheduler promotes it to `READY` once that time passes.

---

# 📁 Project Structure

```text
daglite/
│
├── __init__.py       # Public API (Flow, Task, task, RunState, TaskRunState)
├── models.py         # Data classes: Flow, Task, RetryPolicy
├── state.py          # Enums: RunState, TaskRunState
├── config.py         # Default paths and limits
├── db.py             # SQLite connection, schema, transactions
├── events.py         # Append-only event log writer
├── flow.py           # Serialize flow definition to JSON for DB storage
├── artifacts.py      # Store/load task outputs (inline or file-based)
├── logging_.py       # Per-attempt log file handler
├── scheduler.py      # State machine: WAITING→READY, run terminal states
├── runner.py         # Task executor: claim, run, retry, store artifact
├── worker.py         # Infinite loop driving the runner
├── cli.py            # CLI entry point: run / worker / server commands
├── server.py         # HTTP API + SSE stream for the UI
│
└── ui/
    └── static/
        ├── index.html
        ├── app.js
        └── styles.css
```

---

# 🧰 Installation

```bash
uv pip install aiohttp croniter
```

---

# 🎯 Design Philosophy

Daglite aims to be lightweight, inspectable, and production-friendly. It prioritizes clarity over complexity and avoids heavy infrastructure.

---

# 🛣 Future Improvements

### Packaging
* Proper `pip install daglite` support — `aiohttp` and `croniter` as declared dependencies in `pyproject.toml` so nothing needs to be installed manually

### Triggering
* `POST /api/runs` HTTP endpoint — so any external service can trigger a flow programmatically with params (e.g. `{"video_id": "abc123"}`)
* `params` properly passed through to task functions — currently saved in DB but tasks can't access them

### Observability & UI
* Better error visibility in the UI — which task failed, what the error was, how many retries happened, all visible at a glance
* Real-time log streaming (currently only tail)
* Search and filter on runs list
* Artifact preview in UI

### Execution
* Multiple workers actually running fan-out tasks in parallel (currently `daglite run` is single-worker sequential)
* Distributed execution with Postgres backend for multi-machine setups
* Queue-based scheduling

### Other
* Richer DAG editor
* Artifact versioning
* Cron schedule management via UI

---

# 📜 License

MIT License.

---

# 💡 Inspiration

Daglite takes inspiration from Airflow, Prefect, and Luigi — but focuses on minimal infrastructure and simplicity.