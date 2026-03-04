from __future__ import annotations

import asyncio
import importlib
import json
import sqlite3
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .artifacts import store_json_or_text
from .db import tx
from .events import emit, utc_now_iso
from .logging_ import configure_attempt_logger
from .models import Flow, RetryPolicy, Task
from .state import TaskRunState


def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps({"unserializable": str(obj)}, ensure_ascii=False)


class Runner:
    """
    Thin executor.
    - claims READY tasks atomically
    - executes task function
    - writes attempts, logs, artifacts, events
    - schedules retries by switching state to WAITING + not_before_utc
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        worker_id: str,
        logs_dir: Path,
        artifacts_dir: Path,
        inline_artifact_bytes: int,
    ) -> None:
        self.conn = conn
        self.worker_id = worker_id
        self.logs_dir = logs_dir
        self.artifacts_dir = artifacts_dir
        self.inline_artifact_bytes = inline_artifact_bytes

    def claim_one(self) -> Optional[Dict[str, Any]]:
        """
        Atomically claim one READY task_run.
        Returns a dict with task_run/task details or None.
        """
        with tx(self.conn) as c:
            row = c.execute(
                """
                SELECT tr.id AS task_run_id, tr.run_id, tr.task_id, tr.attempt_count,
                       t.name AS task_name, t.spec_json
                FROM task_runs tr
                JOIN tasks t ON t.id = tr.task_id
                WHERE tr.state = ?
                ORDER BY tr.scheduled_at_utc ASC NULLS LAST, tr.id ASC
                LIMIT 1
                """,
                (TaskRunState.READY.value,),
            ).fetchone()
            if row is None:
                return None

            task_run_id = int(row["task_run_id"])
            # claim if still READY
            updated = c.execute(
                """
                UPDATE task_runs
                SET state=?, claimed_by=?, claim_ts_utc=?, started_at_utc=COALESCE(started_at_utc, ?)
                WHERE id=? AND state=?
                """,
                (TaskRunState.RUNNING.value, self.worker_id, utc_now_iso(), utc_now_iso(), task_run_id, TaskRunState.READY.value),
            ).rowcount
            if updated != 1:
                return None

            emit(
                c,
                actor=f"runner:{self.worker_id}",
                type="task_run.state",
                run_id=int(row["run_id"]),
                task_run_id=task_run_id,
                message="READY->RUNNING (claimed)",
                detail={"from": "READY", "to": "RUNNING"},
            )

            return {
                "task_run_id": task_run_id,
                "run_id": int(row["run_id"]),
                "task_id": int(row["task_id"]),
                "task_name": str(row["task_name"]),
                "spec": json.loads(row["spec_json"]),
                "attempt_count": int(row["attempt_count"]),
            }

    async def run_claimed(self, task_def: Task, claimed: Dict[str, Any]) -> None:
        task_run_id = int(claimed["task_run_id"])
        run_id = int(claimed["run_id"])
        attempt_number = int(claimed["attempt_count"]) + 1

        # Create attempt row + log path
        log_path = self.logs_dir / f"run_{run_id}" / f"taskrun_{task_run_id}" / f"attempt_{attempt_number}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with tx(self.conn) as c:
            c.execute("UPDATE task_runs SET attempt_count=? WHERE id=?", (attempt_number, task_run_id))
            cur = c.execute(
                """
                INSERT INTO attempts(task_run_id, attempt_number, state, created_at_utc, started_at_utc, log_path)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (task_run_id, attempt_number, TaskRunState.RUNNING.value, utc_now_iso(), utc_now_iso(), str(log_path)),
            )
            attempt_id = int(cur.lastrowid)

            emit(
                c,
                actor=f"runner:{self.worker_id}",
                type="attempt.started",
                run_id=run_id,
                task_run_id=task_run_id,
                attempt_id=attempt_id,
                message=f"Attempt {attempt_number} started",
                detail={"attempt_number": attempt_number},
            )

        logger = configure_attempt_logger(log_path)
        try:
            logger.info("Task starting: %s", task_def.name)

            # Resolve inputs: MVP uses artifacts from deps as inputs
            inputs = self._gather_inputs(task_run_id)
            logger.info("Inputs keys: %s", list(inputs.keys()))

            result = task_def.fn(**inputs)
            if asyncio.iscoroutine(result):
                result = await result

            logger.info("Task succeeded")
            artifact_id = None
            with tx(self.conn) as c:
                artifact_id = store_json_or_text(
                    c,
                    task_run_id=task_run_id,
                    artifacts_dir=self.artifacts_dir,
                    value=result,
                    inline_limit_bytes=self.inline_artifact_bytes,
                )

                c.execute(
                    """
                    UPDATE task_runs
                    SET state=?, finished_at_utc=?, last_error_json=NULL, last_artifact_id=?
                    WHERE id=?
                    """,
                    (TaskRunState.SUCCEEDED.value, utc_now_iso(), artifact_id, task_run_id),
                )
                c.execute(
                    "UPDATE attempts SET state=?, finished_at_utc=? WHERE id=?",
                    (TaskRunState.SUCCEEDED.value, utc_now_iso(), attempt_id),
                )

                emit(
                    c,
                    actor=f"runner:{self.worker_id}",
                    type="task_run.state",
                    run_id=run_id,
                    task_run_id=task_run_id,
                    attempt_id=attempt_id,
                    message="RUNNING->SUCCEEDED",
                    detail={"from": "RUNNING", "to": "SUCCEEDED", "artifact_id": artifact_id},
                )

        except Exception as e:
            tb = traceback.format_exc()
            logger.exception("Task failed: %s", e)

            error_json = _json_dumps_safe(
                {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": tb,
                }
            )

            # Retry decision
            max_attempts = int(task_def.retry.max_attempts)
            if attempt_number < max_attempts:
                delay = float(task_def.retry.delay_for_attempt(attempt_number + 1))
                not_before = (asyncio.get_event_loop().time() + delay)  # monotonic for sleep only
                # store wall time for scheduling
                import datetime as dt
                nb_utc = (dt.datetime.now(tz=dt.timezone.utc) + dt.timedelta(seconds=delay)).isoformat()

                with tx(self.conn) as c:
                    c.execute(
                        """
                        UPDATE task_runs
                        SET state=?, not_before_utc=?, last_error_json=?, finished_at_utc=NULL
                        WHERE id=?
                        """,
                        (TaskRunState.WAITING.value, nb_utc, error_json, task_run_id),
                    )
                    c.execute(
                        "UPDATE attempts SET state=?, finished_at_utc=?, error_json=? WHERE id=?",
                        (TaskRunState.FAILED.value, utc_now_iso(), error_json, attempt_id),
                    )
                    emit(
                        c,
                        actor=f"runner:{self.worker_id}",
                        type="task_run.retry_scheduled",
                        run_id=run_id,
                        task_run_id=task_run_id,
                        attempt_id=attempt_id,
                        message=f"Retry scheduled in {delay:.2f}s",
                        detail={"attempt_number": attempt_number, "max_attempts": max_attempts, "delay_seconds": delay},
                    )
            else:
                with tx(self.conn) as c:
                    c.execute(
                        """
                        UPDATE task_runs
                        SET state=?, finished_at_utc=?, last_error_json=?
                        WHERE id=?
                        """,
                        (TaskRunState.FAILED.value, utc_now_iso(), error_json, task_run_id),
                    )
                    c.execute(
                        "UPDATE attempts SET state=?, finished_at_utc=?, error_json=? WHERE id=?",
                        (TaskRunState.FAILED.value, utc_now_iso(), error_json, attempt_id),
                    )
                    emit(
                        c,
                        actor=f"runner:{self.worker_id}",
                        type="task_run.state",
                        run_id=run_id,
                        task_run_id=task_run_id,
                        attempt_id=attempt_id,
                        message="RUNNING->FAILED (final)",
                        detail={"from": "RUNNING", "to": "FAILED", "attempt_number": attempt_number, "max_attempts": max_attempts},
                    )

    def _gather_inputs(self, task_run_id: int) -> Dict[str, Any]:
        """
        MVP: for each dependency task, take its last_artifact as input under key dep_task_name.
        """
        row = self.conn.execute(
            """
            SELECT tr.run_id, t.spec_json
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.id=?
            """,
            (task_run_id,),
        ).fetchone()
        if row is None:
            return {}
        run_id = int(row["run_id"])
        spec = json.loads(row["spec_json"])
        deps = spec.get("deps", [])

        if not deps:
            return {}

        dep_rows = self.conn.execute(
            """
            SELECT t.name AS task_name, tr.last_artifact_id
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.run_id=?
            """,
            (run_id,),
        ).fetchall()

        mapping = {r["task_name"]: r["last_artifact_id"] for r in dep_rows}
        inputs: Dict[str, Any] = {}
        for dep_name in deps:
            aid = mapping.get(dep_name)
            if aid is None:
                continue
            # Keep input as *preview text* for MVP ergonomics; user can parse JSON inside their task if needed.
            arow = self.conn.execute("SELECT inline_text, file_path, kind FROM artifacts WHERE id=?", (int(aid),)).fetchone()
            if arow is None:
                continue
            if arow["inline_text"] is not None:
                txt = str(arow["inline_text"])
            else:
                fp = arow["file_path"]
                txt = Path(fp).read_text(encoding="utf-8", errors="replace") if fp else ""
            # If JSON kind, try parse; else pass string
            if arow["kind"] == "json":
                try:
                    inputs[dep_name] = json.loads(txt)
                except Exception:
                    inputs[dep_name] = txt
            else:
                inputs[dep_name] = txt
        return inputs