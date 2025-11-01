from __future__ import annotations
import json, os
from typing import Any, Dict
from flask import current_app

def _results_dir() -> str:
    base = current_app.instance_path if current_app else os.path.join(os.getcwd(), "instance")
    path = os.path.join(base, "results")
    os.makedirs(path, exist_ok=True)
    return path

def _path_for(run_id: str) -> str:
    return os.path.join(_results_dir(), f"{run_id}.json")

def save_full_result(run_id: str, payload: Dict[str, Any]) -> None:
    with open(_path_for(run_id), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def get_full_result(run_id: str) -> Dict[str, Any]:
    p = _path_for(run_id)
    if not os.path.exists(p):
        return {"run_id": run_id, "kpi": {}, "charts": {}, "downloads": {}}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)
