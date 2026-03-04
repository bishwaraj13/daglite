from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DagliteConfig:
    """
    Opinionated defaults:
      - SQLite file under .daglite/state.db
      - Artifacts/logs under .daglite/artifacts + .daglite/logs
    """

    root_dir: Path = Path(".daglite")
    db_path: Path = Path(".daglite/state.db")
    artifacts_dir: Path = Path(".daglite/artifacts")
    logs_dir: Path = Path(".daglite/logs")

    # Limits
    inline_artifact_bytes: int = 64 * 1024  # store small JSON/text inline

    # Scheduler/worker behavior
    scheduler_tick_seconds: float = 0.5
    worker_poll_seconds: float = 0.2

    # Claim behavior
    claim_ttl_seconds: int = 60 * 60  # 1 hour (MVP: informational; not enforced hard yet)

    def ensure_dirs(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)