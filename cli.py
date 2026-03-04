from __future__ import annotations

import argparse
import asyncio
import importlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, Tuple
import contextlib

from aiohttp import web

from .config import DagliteConfig
from .db import connect, migrate, tx
from .events import emit, utc_now_iso
from .flow import flow_to_definition_json
from .models import Flow
from .runner import Runner
from .scheduler import Scheduler
from .server import DagliteServer
from .state import RunState, TaskRunState
from .worker import Worker


def _load_flow(ref: str) -> Tuple[Flow, str, str]:
    """
    ref: "module.sub:flow_attr"
    """
    if ":" not in ref:
        raise ValueError("Flow reference must be module:attr")
    mod, attr = ref.split(":", 1)
    m = importlib.import_module(mod)
    f = getattr(m, attr)
    if not isinstance(f, Flow):
        raise TypeError(f"{ref} is not a daglite.Flow")
    return f, mod, attr


def _persist_flow(conn: sqlite3.Connection, f: Flow) -> int:
    definition_json = flow_to_definition_json(f)
    with tx(conn) as c:
        cur = c.execute(
            """
            INSERT OR IGNORE INTO flows(name, version, definition_json, created_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            (f.name, f.version, definition_json, utc_now_iso()),
        )
        row = c.execute("SELECT id FROM flows WHERE name=? AND version=?", (f.name, f.version)).fetchone()
        flow_id = int(row["id"])

        # tasks table
        for tname, t in f.tasks.items():
            spec = {
                "name": t.name,
                "deps": list(t.deps),
                "retry": {
                    "max_attempts": t.retry.max_attempts,
                    "strategy": t.retry.strategy,
                    "base_delay_seconds": t.retry.base_delay_seconds,
                    "jitter_seconds": t.retry.jitter_seconds,
                },
                "timeout_seconds": t.timeout_seconds,
            }
            c.execute(
                """
                INSERT OR IGNORE INTO tasks(flow_id, name, spec_json)
                VALUES (?, ?, ?)
                """,
                (flow_id, tname, json.dumps(spec, ensure_ascii=False)),
            )
        return flow_id


def _create_run(conn: sqlite3.Connection, flow_id: int, *, params: Dict[str, Any] | None = None) -> int:
    with tx(conn) as c:
        cur = c.execute(
            """
            INSERT INTO runs(flow_id, state, created_at_utc, params_json)
            VALUES (?, ?, ?, ?)
            """,
            (flow_id, RunState.CREATED.value, utc_now_iso(), json.dumps(params or {}, ensure_ascii=False)),
        )
        run_id = int(cur.lastrowid)
        emit(c, actor="cli", type="run.created", run_id=run_id, message="Run created")
        return run_id


def _create_task_runs(conn: sqlite3.Connection, run_id: int) -> None:
    """
    Create task_runs for all tasks in the flow.
    Root tasks become READY; others WAITING.
    """
    with tx(conn) as c:
        flow = c.execute(
            """
            SELECT f.id AS flow_id, f.definition_json
            FROM runs r JOIN flows f ON f.id=r.flow_id
            WHERE r.id=?
            """,
            (run_id,),
        ).fetchone()
        if not flow:
            raise RuntimeError("Flow not found for run")

        definition = json.loads(flow["definition_json"])
        tasks = definition["tasks"]

        # task name -> task_id
        ids = c.execute("SELECT id, name FROM tasks WHERE flow_id=?", (int(flow["flow_id"]),)).fetchall()
        task_id_by_name = {r["name"]: int(r["id"]) for r in ids}

        for tname, spec in tasks.items():
            deps = spec.get("deps", [])
            state = TaskRunState.READY.value if not deps else TaskRunState.WAITING.value
            cur = c.execute(
                """
                INSERT INTO task_runs(run_id, task_id, state, created_at_utc, scheduled_at_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, task_id_by_name[tname], state, utc_now_iso(), utc_now_iso() if state == TaskRunState.READY.value else None),
            )
            tr_id = int(cur.lastrowid)
            emit(
                c,
                actor="cli",
                type="task_run.created",
                run_id=run_id,
                task_run_id=tr_id,
                message=f"TaskRun created: {tname}",
                detail={"task_name": tname, "initial_state": state},
            )


async def cmd_run(args: argparse.Namespace) -> None:
    cfg = DagliteConfig()
    cfg.ensure_dirs()

    flow, _, _ = _load_flow(args.flow)
    conn = connect(cfg.db_path)
    migrate(conn)

    flow_id = _persist_flow(conn, flow)
    run_id = _create_run(conn, flow_id)
    _create_task_runs(conn, run_id)

    # In-process: scheduler + single worker
    scheduler = Scheduler(conn)
    runner = Runner(conn, worker_id="local", logs_dir=cfg.logs_dir, artifacts_dir=cfg.artifacts_dir, inline_artifact_bytes=cfg.inline_artifact_bytes)
    worker = Worker(runner=runner, flow=flow, poll_seconds=cfg.worker_poll_seconds)

    async def sched_loop():
        while True:
            scheduler.tick()
            await asyncio.sleep(cfg.scheduler_tick_seconds)

    t1 = asyncio.create_task(sched_loop())
    t2 = asyncio.create_task(worker.serve())

    try:
        # run until run finishes
        while True:
            row = conn.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()
            if row and row["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            await asyncio.sleep(0.25)
    finally:
        t1.cancel()
        t2.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(t1, t2)

    final = conn.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"]
    print(f"Run {run_id} finished: {final}")


async def cmd_worker(args: argparse.Namespace) -> None:
    cfg = DagliteConfig()
    cfg.ensure_dirs()

    flow, _, _ = _load_flow(args.flow)
    conn = connect(cfg.db_path)
    migrate(conn)

    scheduler = Scheduler(conn) if args.with_scheduler else None
    runner = Runner(conn, worker_id=args.worker_id, logs_dir=cfg.logs_dir, artifacts_dir=cfg.artifacts_dir, inline_artifact_bytes=cfg.inline_artifact_bytes)
    worker = Worker(runner=runner, flow=flow, poll_seconds=cfg.worker_poll_seconds)

    async def sched_loop():
        while True:
            scheduler.tick()
            await asyncio.sleep(cfg.scheduler_tick_seconds)

    tasks = []
    if scheduler:
        tasks.append(asyncio.create_task(sched_loop()))
    tasks.append(asyncio.create_task(worker.serve()))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


async def cmd_server(args: argparse.Namespace) -> None:
    cfg = DagliteConfig()
    cfg.ensure_dirs()

    conn = connect(cfg.db_path)
    migrate(conn)

    static_dir = Path(__file__).parent / "ui" / "static"
    srv = DagliteServer(conn, static_dir=static_dir)
    app = srv.app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=args.host, port=args.port)
    await site.start()
    print(f"Daglite server running at http://{args.host}:{args.port}/")
    # run forever
    while True:
        await asyncio.sleep(3600)


def main() -> None:
    p = argparse.ArgumentParser(prog="daglite")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run a flow in-process (scheduler + local worker).")
    p_run.add_argument("flow", help="Flow ref like myflows:flow")
    p_run.set_defaults(func=cmd_run)

    p_worker = sub.add_parser("worker", help="Start a worker process.")
    p_worker.add_argument("flow", help="Flow ref like myflows:flow")
    p_worker.add_argument("--worker-id", default="worker-1")
    p_worker.add_argument("--with-scheduler", action="store_true", help="Also run scheduler loop in this process.")
    p_worker.set_defaults(func=cmd_worker)

    p_server = sub.add_parser("server", help="Start API + UI server.")
    p_server.add_argument("--host", default="127.0.0.1")
    p_server.add_argument("--port", type=int, default=8080)
    p_server.set_defaults(func=cmd_server)

    args = p.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()