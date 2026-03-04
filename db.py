from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path
from typing import Iterator, Optional


SCHEMA_VERSION = 1


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER NOT NULL PRIMARY KEY,
  applied_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flow_id INTEGER NOT NULL,
  state TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  started_at_utc TEXT,
  finished_at_utc TEXT,
  params_json TEXT,
  FOREIGN KEY(flow_id) REFERENCES flows(id)
);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flow_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  UNIQUE(flow_id, name),
  FOREIGN KEY(flow_id) REFERENCES flows(id)
);

CREATE TABLE IF NOT EXISTS task_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  task_id INTEGER NOT NULL,
  state TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  scheduled_at_utc TEXT,
  not_before_utc TEXT,
  started_at_utc TEXT,
  finished_at_utc TEXT,
  claimed_by TEXT,
  claim_ts_utc TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error_json TEXT,
  last_artifact_id INTEGER,
  FOREIGN KEY(run_id) REFERENCES runs(id),
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE TABLE IF NOT EXISTS attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_run_id INTEGER NOT NULL,
  attempt_number INTEGER NOT NULL,
  state TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  started_at_utc TEXT,
  finished_at_utc TEXT,
  error_json TEXT,
  log_path TEXT,
  FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
  UNIQUE(task_run_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_run_id INTEGER NOT NULL,
  created_at_utc TEXT NOT NULL,
  kind TEXT NOT NULL, -- json|text|file
  content_type TEXT,
  inline_text TEXT,
  file_path TEXT,
  size_bytes INTEGER,
  preview_text TEXT,
  FOREIGN KEY(task_run_id) REFERENCES task_runs(id)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc TEXT NOT NULL,
  actor TEXT NOT NULL,
  type TEXT NOT NULL,
  run_id INTEGER,
  task_run_id INTEGER,
  attempt_id INTEGER,
  message TEXT,
  detail_json TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(id),
  FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
  FOREIGN KEY(attempt_id) REFERENCES attempts(id)
);

CREATE TABLE IF NOT EXISTS cron_schedules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flow_name TEXT NOT NULL,
  flow_ref TEXT NOT NULL, -- module:attr reference for CLI reload (MVP)
  cron_expr TEXT NOT NULL,
  timezone TEXT NOT NULL,
  last_run_at_utc TEXT,
  next_run_at_utc TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at_utc TEXT NOT NULL
);

-- Indexes (UI queries + scheduling)
CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_task_runs_state ON task_runs(state);
CREATE INDEX IF NOT EXISTS idx_task_runs_not_before ON task_runs(not_before_utc);
CREATE INDEX IF NOT EXISTS idx_attempts_task_run_id ON attempts(task_run_id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_utc);
CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_task_run_id ON events(task_run_id);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # WAL for better concurrent reads
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    row = conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
    current = int(row["v"]) if row and row["v"] is not None else 0
    if current >= SCHEMA_VERSION:
        return
    # MVP: single schema version. Future: incremental migrations.
    import datetime as _dt
    now = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO schema_migrations(version, applied_at_utc) VALUES (?, ?)",
        (SCHEMA_VERSION, now),
    )


@contextlib.contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """
    Transaction context manager.
    Uses BEGIN IMMEDIATE to reduce write contention surprises for claim/update operations.
    """
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield conn
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise