# app/dao/mapper_dao.py
from __future__ import annotations

from typing import Dict, List, Any, Set

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from app.extensions import db

VALID_SOURCE = {"mail", "crm"}


# ---------------------------
# Internal helpers / contracts
# ---------------------------

def _require_source(source: str) -> str:
    s = (source or "").lower().strip()
    if s not in VALID_SOURCE:
        raise ValueError(f"invalid source: {source!r}")
    return s

def _tbl_raw(source: str) -> str:
    s = _require_source(source)
    return "staging_raw_mail" if s == "mail" else "staging_raw_crm"

def _tbl_norm(source: str) -> str:
    s = _require_source(source)
    return "staging_mail" if s == "mail" else "staging_crm"


# ---------------------------
# RAW (JSONB) ingestion / read
# ---------------------------

def insert_raw_rows(run_id: str, user_id: str, source: str, rows: List[Dict[str, Any]]) -> int:
    """
    Replace RAW rows for (run_id, source) with the given list of dicts.
    Each row is stored as JSONB under column 'data' with a 1-based 'rownum'.
    Returns number of rows inserted.
    """
    tbl = _tbl_raw(source)

    # Clear existing RAW for this run/source
    db.session.execute(text(f"DELETE FROM {tbl} WHERE run_id = :rid"), {"rid": run_id})

    if not rows:
        db.session.commit()
        return 0

    # Prepare executemany insert
    # NOTE: use a JSONB bindparam for 'data' so :data is typed correctly.
    stmt = (
        text(f"""
            INSERT INTO {tbl} (run_id, user_id, rownum, data)
            VALUES (:rid, :uid, :n, :data)
        """)
        .bindparams(bindparam("data", type_=JSONB))
    )

    payload = [{"rid": run_id, "uid": user_id, "n": i, "data": r} for i, r in enumerate(rows, start=1)]
    db.session.execute(stmt, payload)
    db.session.commit()
    return len(rows)


def get_raw_headers(run_id: str, source: str, sample: int = 25) -> Dict[str, Any]:
    """
    Return a union of keys across the first 'sample' RAW rows plus the sample rows themselves.
    { "headers": [...], "sample_rows": [...] }
    """
    tbl = _tbl_raw(source)
    res = db.session.execute(
        text(f"""
            SELECT data
            FROM {tbl}
            WHERE run_id = :rid
            ORDER BY rownum ASC
            LIMIT :lim
        """),
        {"rid": run_id, "lim": int(sample)},
    )

    sample_rows: List[Dict[str, Any]] = []
    headers: Set[str] = set()

    for row in res:
        d: Dict[str, Any] = row._mapping.get("data") or {}
        sample_rows.append(d)
        for k in d.keys():
            if isinstance(k, str) and k.strip():
                headers.add(k.strip())

    return {
        "headers": sorted(headers),
        "sample_rows": sample_rows,
    }

def get_raw_rows(run_id: str, source: str) -> List[Dict[str, Any]]:
    """
    Return all original CSV rows from RAW as plain dicts, ordered by rownum.
    Tables: staging_raw_mail / staging_raw_crm with (run_id, user_id, rownum, data jsonb)
    """
    tbl = _tbl_raw(source)
    res = db.session.execute(
        text(f"""
            SELECT data
            FROM {tbl}
            WHERE run_id = :rid
            ORDER BY rownum ASC
        """),
        {"rid": run_id},
    )
    return [row._mapping["data"] or {} for row in res]


# ---------------------------
# Mappings (per run + source)
# ---------------------------

def save_mapping(run_id: str, user_id: str, source: str, mapping: Dict[str, Any]) -> None:
    """
    Upsert the mapping JSON for (run_id, source).
    Requires a UNIQUE constraint on (run_id, source).
    """
    _require_source(source)
    stmt = (
        text("""
            INSERT INTO mappings (run_id, user_id, source, mapping, created_at)
            VALUES (:rid, :uid, :source, :mapping, NOW())
            ON CONFLICT (run_id, source)
            DO UPDATE SET
              user_id = EXCLUDED.user_id,
              mapping = EXCLUDED.mapping,
              created_at = NOW()
        """)
        .bindparams(bindparam("mapping", type_=JSONB))
    )
    db.session.execute(stmt, {"rid": run_id, "uid": user_id, "source": source, "mapping": mapping})
    db.session.commit()


def get_mapping(run_id: str, source: str) -> Dict[str, Any]:
    _require_source(source)
    row = db.session.execute(
        text("""
            SELECT mapping
            FROM mappings
            WHERE run_id = :rid AND source = :source
            LIMIT 1
        """),
        {"rid": run_id, "source": source},
    ).first()
    return (row and (row._mapping.get("mapping") or {})) or {}


# -------------------------------------------
# Normalized inserts (canonical mail / CRM CSV)
# -------------------------------------------

def clear_normalized(run_id: str, source: str) -> None:
    tbl = _tbl_norm(source)
    db.session.execute(text(f"DELETE FROM {tbl} WHERE run_id = :rid"), {"rid": run_id})
    db.session.commit()

def _extract_source_id(row: Dict[str, Any]) -> Any:
    """
    Normalize input id fields to a single 'source_id' value.
    Priority: explicit 'source_id'
    Coerce blank strings to None; leave non-blank strings as-is.
    """
    sid = row.get("source_id")

    if isinstance(sid, str):
        sid = sid.strip()
        if sid == "":
            sid = None
    return sid

def insert_normalized_mail(run_id: str, user_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        clear_normalized(run_id, "mail")
        return 0

    tbl = _tbl_norm("mail")
    stmt = text(f"""
        INSERT INTO {tbl}
          (run_id, user_id, source_id, address1, address2, city, state, zip, sent_date)
        VALUES
          (:rid, :uid, :source_id, :address1, :address2, :city, :state, :zip, :sent_date)
    """)

    payload = []
    for r in rows:
        payload.append({
            "rid": run_id,
            "uid": user_id,
            "source_id": _extract_source_id(r),  # <-- always present key, may be None
            "address1": (r.get("address1") or "").strip(),
            "address2": (r.get("address2") or "").strip(),
            "city": (r.get("city") or "").strip(),
            "state": (r.get("state") or "").strip(),
            "zip": (r.get("zip") or "").strip(),
            "sent_date": (r.get("sent_date") or None),
        })

    clear_normalized(run_id, "mail")
    db.session.execute(stmt, payload)
    db.session.commit()
    return len(rows)


def insert_normalized_crm(run_id: str, user_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        clear_normalized(run_id, "crm")
        return 0

    tbl = _tbl_norm("crm")
    stmt = text(f"""
        INSERT INTO {tbl}
          (run_id, user_id, source_id, address1, address2, city, state, zip, job_date, job_value)
        VALUES
          (:rid, :uid, :source_id, :address1, :address2, :city, :state, :zip, :job_date, :job_value)
    """)

    payload = []
    for r in rows:
        job_val = r.get("job_value")
        if isinstance(job_val, str):
            job_val = job_val.strip() or None

        payload.append({
            "rid": run_id,
            "uid": user_id,
            "source_id": _extract_source_id(r),  # <-- unified handling
            "address1": (r.get("address1") or "").strip(),
            "address2": (r.get("address2") or "").strip(),
            "city": (r.get("city") or "").strip(),
            "state": (r.get("state") or "").strip(),
            "zip": (r.get("zip") or "").strip(),
            "job_date": (r.get("job_date") or None),
            "job_value": job_val,
        })

    clear_normalized(run_id, "crm")
    db.session.execute(stmt, payload)
    db.session.commit()
    return len(rows)


# ---------------------------
# Optional small conveniences
# ---------------------------

def count_raw(run_id: str, source: str) -> int:
    tbl = _tbl_raw(source)
    n = db.session.execute(
        text(f"SELECT COUNT(*) FROM {tbl} WHERE run_id = :rid"),
        {"rid": run_id},
    ).scalar()
    return int(n or 0)

def count_norm(run_id: str, source: str) -> int:
    tbl = _tbl_norm(source)
    n = db.session.execute(
        text(f"SELECT COUNT(*) FROM {tbl} WHERE run_id = :rid"),
        {"rid": run_id},
    ).scalar()
    return int(n or 0)