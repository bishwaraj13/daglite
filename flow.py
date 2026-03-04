from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any, Dict, List, Tuple

from .models import Flow, Task, RetryPolicy, task  # re-export decorator


def flow_to_definition_json(f: Flow) -> str:
    """
    Persist a flow definition in SQLite so UI and runs remain inspectable.
    We store minimal spec (task names, deps, retry policy), not function code.
    """
    tasks_spec: Dict[str, Any] = {}
    for name, t in f.tasks.items():
        tasks_spec[name] = {
            "name": t.name,
            "deps": list(t.deps),
            "retry": asdict(t.retry),
            "timeout_seconds": t.timeout_seconds,
        }
    d = {
        "name": f.name,
        "version": f.version,
        "description": f.description,
        "tasks": tasks_spec,
        "edges": list(map(list, f.edges)),
    }
    return json.dumps(d, ensure_ascii=False, indent=2)