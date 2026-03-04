from __future__ import annotations

import asyncio
import sqlite3
from typing import Dict, Optional

from .models import Flow, Task
from .runner import Runner


class Worker:
    """
    Runs in a loop:
      - claim READY task
      - execute it
    """

    def __init__(self, *, runner: Runner, flow: Flow, poll_seconds: float = 0.2) -> None:
        self.runner = runner
        self.flow = flow
        self.poll_seconds = poll_seconds
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def serve(self) -> None:
        while not self._stop.is_set():
            claimed = self.runner.claim_one()
            if not claimed:
                await asyncio.sleep(self.poll_seconds)
                continue

            task_name = str(claimed["task_name"])
            task_def = self.flow.tasks.get(task_name)
            if task_def is None:
                # task definition missing (flow mismatch)
                # Mark failed to avoid deadlock
                import json
                from .db import tx
                from .events import emit, utc_now_iso
                from .state import TaskRunState

                with tx(self.runner.conn) as c:
                    err = json.dumps({"type": "MissingTask", "message": f"Task '{task_name}' not found in loaded flow"})
                    c.execute(
                        "UPDATE task_runs SET state=?, finished_at_utc=?, last_error_json=? WHERE id=?",
                        (TaskRunState.FAILED.value, utc_now_iso(), err, int(claimed["task_run_id"])),
                    )
                    emit(
                        c,
                        actor=f"worker:{self.runner.worker_id}",
                        type="task_run.state",
                        run_id=int(claimed["run_id"]),
                        task_run_id=int(claimed["task_run_id"]),
                        message="RUNNING->FAILED (missing task def)",
                        detail={"to": "FAILED"},
                    )
                continue

            await self.runner.run_claimed(task_def, claimed)