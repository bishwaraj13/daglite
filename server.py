from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, Optional

from aiohttp import web

from .artifacts import load_artifact_text
from .events import emit
from .state import TaskRunState


def _json(obj: Any) -> web.Response:
    return web.json_response(obj, dumps=lambda x: json.dumps(x, ensure_ascii=False))


class DagliteServer:
    def __init__(self, conn: sqlite3.Connection, static_dir: Path) -> None:
        self.conn = conn
        self.static_dir = static_dir

    def app(self) -> web.Application:
        app = web.Application()
        app["conn"] = self.conn

        # API routes
        app.router.add_get("/api/runs", self.runs_list)
        app.router.add_get("/api/runs/{run_id}", self.run_detail)
        app.router.add_get("/api/runs/{run_id}/graph", self.run_graph)
        app.router.add_get("/api/task-runs/{task_run_id}", self.task_run_detail)
        app.router.add_get("/api/attempts/{attempt_id}/logs", self.attempt_logs)
        app.router.add_get("/api/events", self.events_list)
        app.router.add_get("/api/events/stream", self.events_stream)

        # Static UI
        app.router.add_get("/", self.index)
        app.router.add_static("/static/", path=str(self.static_dir), show_index=False)
        return app

    async def index(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "index.html")

    async def runs_list(self, request: web.Request) -> web.Response:
        rows = self.conn.execute(
            """
            SELECT r.id, r.state, r.created_at_utc, r.started_at_utc, r.finished_at_utc,
                   f.name AS flow_name, f.version AS flow_version
            FROM runs r
            JOIN flows f ON f.id = r.flow_id
            ORDER BY r.id DESC
            LIMIT 200
            """
        ).fetchall()
        return _json({"runs": [dict(x) for x in rows]})

    async def run_detail(self, request: web.Request) -> web.Response:
        run_id = int(request.match_info["run_id"])
        run = self.conn.execute(
            """
            SELECT r.*, f.name AS flow_name, f.version AS flow_version
            FROM runs r JOIN flows f ON f.id=r.flow_id
            WHERE r.id=?
            """,
            (run_id,),
        ).fetchone()
        if not run:
            raise web.HTTPNotFound()

        trs = self.conn.execute(
            """
            SELECT tr.id, tr.state, tr.task_id, tr.attempt_count, tr.started_at_utc, tr.finished_at_utc,
                   tr.last_error_json, tr.last_artifact_id, t.name AS task_name
            FROM task_runs tr
            JOIN tasks t ON t.id=tr.task_id
            WHERE tr.run_id=?
            ORDER BY tr.id ASC
            """,
            (run_id,),
        ).fetchall()

        return _json({"run": dict(run), "task_runs": [dict(x) for x in trs]})

    async def run_graph(self, request: web.Request) -> web.Response:
        run_id = int(request.match_info["run_id"])
        # Graph is flow edges + task_run states
        flow = self.conn.execute(
            """
            SELECT f.definition_json
            FROM runs r
            JOIN flows f ON f.id=r.flow_id
            WHERE r.id=?
            """,
            (run_id,),
        ).fetchone()
        if not flow:
            raise web.HTTPNotFound()
        definition = json.loads(flow["definition_json"])

        states = self.conn.execute(
            """
            SELECT t.name AS task_name, tr.id AS task_run_id, tr.state, tr.started_at_utc, tr.finished_at_utc
            FROM task_runs tr
            JOIN tasks t ON t.id=tr.task_id
            WHERE tr.run_id=?
            """,
            (run_id,),
        ).fetchall()
        by_name = {r["task_name"]: dict(r) for r in states}

        nodes = []
        for tname in definition["tasks"].keys():
            st = by_name.get(tname, {})
            nodes.append(
                {
                    "task_name": tname,
                    "task_run_id": st.get("task_run_id"),
                    "state": st.get("state", TaskRunState.PENDING.value),
                    "started_at_utc": st.get("started_at_utc"),
                    "finished_at_utc": st.get("finished_at_utc"),
                }
            )

        return _json({"nodes": nodes, "edges": definition.get("edges", [])})

    async def task_run_detail(self, request: web.Request) -> web.Response:
        task_run_id = int(request.match_info["task_run_id"])
        tr = self.conn.execute(
            """
            SELECT tr.*, t.name AS task_name
            FROM task_runs tr
            JOIN tasks t ON t.id=tr.task_id
            WHERE tr.id=?
            """,
            (task_run_id,),
        ).fetchone()
        if not tr:
            raise web.HTTPNotFound()

        attempts = self.conn.execute(
            """
            SELECT id, attempt_number, state, created_at_utc, started_at_utc, finished_at_utc, error_json, log_path
            FROM attempts
            WHERE task_run_id=?
            ORDER BY attempt_number ASC
            """,
            (task_run_id,),
        ).fetchall()

        artifact = None
        if tr["last_artifact_id"] is not None:
            ctype, text = load_artifact_text(self.conn, int(tr["last_artifact_id"]))
            artifact = {"id": int(tr["last_artifact_id"]), "content_type": ctype, "text": text[:20000]}

        return _json({"task_run": dict(tr), "attempts": [dict(x) for x in attempts], "artifact": artifact})

    async def attempt_logs(self, request: web.Request) -> web.Response:
        attempt_id = int(request.match_info["attempt_id"])
        tail = int(request.query.get("tail", "200"))
        row = self.conn.execute("SELECT log_path FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        if not row:
            raise web.HTTPNotFound()
        path = row["log_path"]
        if not path:
            return _json({"attempt_id": attempt_id, "lines": []})

        p = Path(path)
        if not p.exists():
            return _json({"attempt_id": attempt_id, "lines": []})

        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()[-tail:]
        return _json({"attempt_id": attempt_id, "lines": lines})

    async def events_list(self, request: web.Request) -> web.Response:
        since = request.query.get("since")
        limit = int(request.query.get("limit", "500"))
        if since is None:
            rows = self.conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            rows = list(reversed(rows))
        else:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT ?",
                (int(since), limit),
            ).fetchall()
        return _json({"events": [dict(x) for x in rows]})

    async def events_stream(self, request: web.Request) -> web.StreamResponse:
        """
        Server-Sent Events: emits JSON per event as:
          event: message
          data: {...}
        """
        resp = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await resp.prepare(request)

        last_id = int(request.query.get("since", "0"))
        try:
            while True:
                rows = self.conn.execute(
                    "SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT 200",
                    (last_id,),
                ).fetchall()
                for r in rows:
                    last_id = int(r["id"])
                    payload = json.dumps(dict(r), ensure_ascii=False)
                    await resp.write(f"event: event\ndata: {payload}\n\n".encode("utf-8"))
                await resp.drain()
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise
        except Exception:
            # client disconnects etc.
            return resp