from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


class TeeFileHandler(logging.Handler):
    """
    Simple file handler that writes log records to a file.
    (We keep SQLite for log metadata/events; bulk logs are file-based.)
    """

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = open(self.path, "a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        self._fp.write(msg + "\n")
        self._fp.flush()

    def close(self) -> None:
        try:
            self._fp.close()
        finally:
            super().close()


def configure_attempt_logger(log_path: Path, *, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a dedicated logger for a single attempt.
    """
    logger = logging.getLogger(f"daglite.attempt.{log_path.stem}")
    logger.setLevel(level)
    logger.propagate = False

    # Remove existing handlers (in case of reuse in long-lived processes)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    handler = TeeFileHandler(log_path)
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger