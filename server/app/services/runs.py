# app/services/runs.py
from typing import Dict, List, Optional
from app.dao import runs_dao

def latest_for_user(user_id: str, only_done: bool = False) -> Optional[Dict]:
    return runs_dao.latest_for_user(user_id, only_done)

def list_for_user(user_id: str, limit: int = 25, before: Optional[str] = None) -> List[Dict]:
    return runs_dao.list_for_user(user_id, limit=limit, before_run_id=before)

def get_run(run_id: str) -> Optional[Dict]:
    return runs_dao.get_by_id_compact(run_id)