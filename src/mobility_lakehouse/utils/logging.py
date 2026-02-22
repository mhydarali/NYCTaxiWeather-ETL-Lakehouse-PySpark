from __future__ import annotations

import json
import logging
from pathlib import Path

from mobility_lakehouse.config import LOG_DIR
from mobility_lakehouse.utils.paths import mkdirp


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(payload)


def setup_logging(name: str = "mobility_lakehouse", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        return logger

    mkdirp(LOG_DIR)
    log_path = Path(LOG_DIR) / "pipeline.log"

    formatter = JsonFormatter()

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger
