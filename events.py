from __future__ import annotations

import datetime as dt
import json
import sqlite3
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def emit(
    conn: sqlite3.Connection,
    *,
    actor: str,
    type: str,
    message: str = "",
    run_id: Optional[int] = None,
    task_run_id: Optional[int] = None,
    attempt_id: Optional[int] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> int:
    ts = utc_now_iso()
    detail_json = json.dumps(detail) if detail is not None else None
    cur = conn.execute(
        """
        INSERT INTO events(ts_utc, actor, type, run_id, task_run_id, attempt_id, message, detail_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ts, actor, type, run_id, task_run_id, attempt_id, message, detail_json),
    )
    return int(cur.lastrowid)