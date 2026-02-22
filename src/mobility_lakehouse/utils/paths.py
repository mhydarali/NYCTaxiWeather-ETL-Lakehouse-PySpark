from __future__ import annotations

from pathlib import Path
import json
import os
import tempfile
from typing import Any


def mkdirp(path: Path | str) -> Path:
    resolved = Path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def atomic_write_text(path: Path | str, content: str) -> Path:
    """Write content to a temp file first, then rename to the final path.

    Why: If the pipeline crashes mid-write, the final file won't exist
    (or still has its old content). This prevents corrupt partial files.
    """
    target = Path(path)
    mkdirp(target.parent)
    with tempfile.NamedTemporaryFile("w", dir=target.parent, delete=False, encoding="utf-8") as tmp:
        tmp.write(content)
        temp_name = tmp.name
    os.replace(temp_name, target)
    return target


def atomic_write_json(path: Path | str, payload: dict[str, Any]) -> Path:
    return atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def read_json(path: Path | str, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    return json.loads(target.read_text(encoding="utf-8"))
