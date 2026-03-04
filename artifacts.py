from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Optional, Tuple

from .events import utc_now_iso


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def store_json_or_text(
    conn: sqlite3.Connection,
    *,
    task_run_id: int,
    artifacts_dir: Path,
    value: Any,
    inline_limit_bytes: int,
    content_type: str = "application/json",
) -> int:
    """
    Store small outputs inline, otherwise store as content-addressed file.
    Returns artifact_id.
    """
    ts = utc_now_iso()

    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        data = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        kind = "json"
        preview = json.dumps(value, ensure_ascii=False)[:2000]
        inline_text = data.decode("utf-8") if len(data) <= inline_limit_bytes else None
    else:
        text = str(value)
        data = text.encode("utf-8")
        kind = "text"
        preview = text[:2000]
        inline_text = text if len(data) <= inline_limit_bytes else None
        content_type = "text/plain; charset=utf-8"

    file_path: Optional[str] = None
    size_bytes = len(data)

    if inline_text is None:
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        digest = _sha256_bytes(data)
        # content addressed; keep extension simple
        p = artifacts_dir / f"{digest}.artifact"
        if not p.exists():
            p.write_bytes(data)
        file_path = str(p)

    cur = conn.execute(
        """
        INSERT INTO artifacts(task_run_id, created_at_utc, kind, content_type, inline_text, file_path, size_bytes, preview_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (task_run_id, ts, kind, content_type, inline_text, file_path, size_bytes, preview),
    )
    return int(cur.lastrowid)


def load_artifact_text(conn: sqlite3.Connection, artifact_id: int) -> Tuple[str, str]:
    """
    Returns (content_type, text). For file artifacts reads file.
    """
    row = conn.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
    if row is None:
        raise KeyError(f"artifact_id={artifact_id} not found")

    ctype = row["content_type"] or "text/plain"
    inline = row["inline_text"]
    if inline is not None:
        return ctype, str(inline)

    fp = row["file_path"]
    if not fp:
        return ctype, ""
    return ctype, Path(fp).read_text(encoding="utf-8", errors="replace")