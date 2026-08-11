
from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False


def _configure_root(log_level: str, log_dir: Path) -> None:
    global _configured
    if _configured:
        return

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "ingestion.log"

    root = logging.getLogger()
    root.setLevel(log_level.upper())

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root.handlers = [console_handler, file_handler]
    _configured = True


def get_logger(name: str) -> logging.Logger:
    from ingestion.config import get_runtime_config

    runtime_cfg = get_runtime_config()
    _configure_root(runtime_cfg.log_level, runtime_cfg.log_dir)
    return logging.getLogger(name)
