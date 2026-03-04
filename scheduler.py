from __future__ import annotations

import datetime as dt
import json
import sqlite3
from typing import Dict, List, Optional, Set, Tuple

from .db import tx
from .events import emit, utc_now_iso
from .state import RunState, TaskRunState


def _utc_now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def _parse_iso(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    return dt.datetime.fromisoformat(s)


class Scheduler:
    """
    Moves task_runs through:
      WAITING -> READY when deps satisfied and not_before reached
    Also updates run state based on task_run terminal states.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def tick(self) -> int:
        """
        Returns number of state transitions performed (approx).
        """
        changed = 0
        now = _utc_now()

        with tx(self.conn) as c:
            # 1) Promote WAITING -> READY
            waiting = c.execute(
                """
                SELECT tr.id, tr.run_id
                FROM task_runs tr
                WHERE tr.state = ?
                  AND (tr.not_before_utc IS NULL OR tr.not_before_utc <= ?)
                """,
                (TaskRunState.WAITING.value, now.isoformat()),
            ).fetchall()

            for row in waiting:
                task_run_id = int(row["id"])
                run_id = int(row["run_id"])
                if self._deps_satisfied(c, task_run_id):
                    c.execute(
                        "UPDATE task_runs SET state=?, scheduled_at_utc=? WHERE id=? AND state=?",
                        (TaskRunState.READY.value, utc_now_iso(), task_run_id, TaskRunState.WAITING.value),
                    )
                    emit(
                        c,
                        actor="scheduler",
                        type="task_run.state",
                        run_id=run_id,
                        task_run_id=task_run_id,
                        message="WAITING->READY (deps satisfied)",
                        detail={"from": "WAITING", "to": "READY"},
                    )
                    changed += 1

            # 2) Ensure run state transitions
            changed += self._reconcile_runs(c)

        return changed

    def _deps_satisfied(self, c: sqlite3.Connection, task_run_id: int) -> bool:
        # Find the task + its deps via tasks.spec_json
        row = c.execute(
            """
            SELECT tr.run_id, t.spec_json, t.flow_id
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.id = ?
            """,
            (task_run_id,),
        ).fetchone()
        if row is None:
            return False
        run_id = int(row["run_id"])
        spec = json.loads(row["spec_json"])
        deps: List[str] = spec.get("deps", [])

        if not deps:
            return True

        # Map dep task names -> dep task_runs states
        q = """
        SELECT t.name AS task_name, tr.state AS state
        FROM task_runs tr
        JOIN tasks t ON t.id = tr.task_id
        WHERE tr.run_id = ?
        """
        states = {r["task_name"]: r["state"] for r in c.execute(q, (run_id,)).fetchall()}
        for d in deps:
            st = states.get(d)
            if st != TaskRunState.SUCCEEDED.value:
                return False
        return True

    def _reconcile_runs(self, c: sqlite3.Connection) -> int:
        changed = 0
        runs = c.execute("SELECT id, state, started_at_utc FROM runs").fetchall()

        for r in runs:
            run_id = int(r["id"])
            state = str(r["state"])
            started_at = r["started_at_utc"]

            # If any task running/ready/waiting => RUNNING (once it begins)
            trs = c.execute(
                "SELECT state FROM task_runs WHERE run_id=?",
                (run_id,),
            ).fetchall()
            if not trs:
                continue
            states = [str(x["state"]) for x in trs]

            any_active = any(s in {TaskRunState.READY.value, TaskRunState.RUNNING.value, TaskRunState.WAITING.value} for s in states)
            all_succeeded = all(s == TaskRunState.SUCCEEDED.value for s in states)
            any_failed = any(s == TaskRunState.FAILED.value for s in states)

            if state == RunState.CREATED.value and any_active and started_at is None:
                c.execute(
                    "UPDATE runs SET state=?, started_at_utc=? WHERE id=? AND state=?",
                    (RunState.RUNNING.value, utc_now_iso(), run_id, RunState.CREATED.value),
                )
                emit(c, actor="scheduler", type="run.state", run_id=run_id, message="CREATED->RUNNING", detail={"from": "CREATED", "to": "RUNNING"})
                changed += 1

            # Terminal reconciliation
            cur_state = c.execute("SELECT state FROM runs WHERE id=?", (run_id,)).fetchone()["state"]
            if cur_state in {RunState.SUCCEEDED.value, RunState.FAILED.value, RunState.CANCELLED.value}:
                continue

            if all_succeeded:
                c.execute(
                    "UPDATE runs SET state=?, finished_at_utc=? WHERE id=?",
                    (RunState.SUCCEEDED.value, utc_now_iso(), run_id),
                )
                emit(c, actor="scheduler", type="run.state", run_id=run_id, message="->SUCCEEDED", detail={"to": "SUCCEEDED"})
                changed += 1
            elif any_failed:
                c.execute(
                    "UPDATE runs SET state=?, finished_at_utc=? WHERE id=?",
                    (RunState.FAILED.value, utc_now_iso(), run_id),
                )
                emit(c, actor="scheduler", type="run.state", run_id=run_id, message="->FAILED", detail={"to": "FAILED"})
                changed += 1

        return changed