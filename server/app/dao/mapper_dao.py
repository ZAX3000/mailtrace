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

    db.session.execute(text(f"DELETE FROM {tbl} WHERE run_id = :rid"), {"rid": run_id})

    if not rows:
        db.session.commit()
        return 0

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


# --- Fetch existing keys for dedupe -----------------------------------------

def fetch_mail_existing_keys(user_id: str) -> tuple[set[str], set[tuple[str, str]]]:
    """
    Return:
      - source_ids: set[str] of existing non-null source_id for this user
      - addr_dates: set[(full_address_lower, YYYY-MM-DD)] for rows with NULL source_id
    """
    sid_rows = db.session.execute(text("""
        SELECT source_id
        FROM staging_mail
        WHERE user_id = :uid AND source_id IS NOT NULL
    """), {"uid": user_id}).all()
    source_ids = {r.source_id for r in sid_rows if r.source_id}

    ad_rows = db.session.execute(text("""
        SELECT LOWER(COALESCE(full_address,'')) AS fa, sent_date
        FROM staging_mail
        WHERE user_id = :uid AND source_id IS NULL
          AND full_address IS NOT NULL AND sent_date IS NOT NULL
    """), {"uid": user_id}).all()
    addr_dates = {(r.fa, r.sent_date.isoformat()) for r in ad_rows if r.sent_date}

    return source_ids, addr_dates


def fetch_crm_existing_keys(user_id: str) -> tuple[set[str], set[tuple[str, str]]]:
    sid_rows = db.session.execute(text("""
        SELECT source_id
        FROM staging_crm
        WHERE user_id = :uid AND source_id IS NOT NULL
    """), {"uid": user_id}).all()
    source_ids = {r.source_id for r in sid_rows if r.source_id}

    ad_rows = db.session.execute(text("""
        SELECT LOWER(COALESCE(full_address,'')) AS fa, job_date
        FROM staging_crm
        WHERE user_id = :uid AND source_id IS NULL
          AND full_address IS NOT NULL AND job_date IS NOT NULL
    """), {"uid": user_id}).all()
    addr_dates = {(r.fa, r.job_date.isoformat()) for r in ad_rows if r.job_date}

    return source_ids, addr_dates


# -------------------------------------------
# Normalized inserts (canonical mail / CRM CSV)
# -------------------------------------------

def clear_normalized(run_id: str, source: str) -> None:
    tbl = _tbl_norm(source)
    db.session.execute(text(f"DELETE FROM {tbl} WHERE run_id = :rid"), {"rid": run_id})
    db.session.commit()

# MAIL
def insert_normalized_mail(run_id: str, user_id: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    # 1) in-memory de-dup by mail_key (keeps first occurrence)
    seen = set()
    dedup = []
    for r in rows:
        mk = (str(r.get("mail_key") or "").strip())
        if not mk:
            raise ValueError("mail_key is required for all mail rows")
        if mk in seen:
            continue
        seen.add(mk)
        dedup.append(r)
    rows = dedup

    # 2) upsert against (user_id, mail_key) for idempotency across runs
    stmt = text("""
        INSERT INTO staging_mail
          (run_id, user_id, mail_key, source_id,
           address1, address2, city, state, zip, full_address, sent_date)
        VALUES
          (:rid, :uid, :mail_key, :source_id,
           :address1, :address2, :city, :state, :zip, :full_address, :sent_date)
        ON CONFLICT (user_id, mail_key) DO UPDATE
        SET
          -- reattach this canonical mail row to the current run
          run_id       = EXCLUDED.run_id,
          -- prefer new non-null source_id if provided
          source_id    = COALESCE(EXCLUDED.source_id, staging_mail.source_id),
          -- keep normalized fields in sync with latest ingestion
          address1     = EXCLUDED.address1,
          address2     = EXCLUDED.address2,
          city         = EXCLUDED.city,
          state        = EXCLUDED.state,
          zip          = EXCLUDED.zip,
          full_address = EXCLUDED.full_address,
          sent_date    = EXCLUDED.sent_date
    """)

    payload = []
    for r in rows:
        src = r.get("source_id")
        addr2 = r.get("address2")
        payload.append({
            "rid": run_id,
            "uid": user_id,
            "mail_key": str(r.get("mail_key")).strip(),
            "source_id": (src.strip() if isinstance(src, str) and src.strip() else None),
            "address1": (r.get("address1") or "").strip(),
            "address2": (addr2.strip() if isinstance(addr2, str) and addr2.strip() else None),
            "city": (r.get("city") or "").strip(),
            "state": (r.get("state") or "").strip(),
            "zip": (str(r.get("zip")) if r.get("zip") is not None else "").strip(),  # keep leading zeros
            "full_address": (r.get("full_address") or "").strip(),
            "sent_date": r.get("sent_date") or None,
        })

    db.session.execute(stmt, payload)
    return len(payload)


def insert_normalized_crm(run_id: str, user_id: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    # 1) in-memory de-dup by job_index (keep first)
    seen = set()
    dedup = []
    for r in rows:
        ji = str(r.get("job_index") or "").strip()
        if not ji:
            raise ValueError("job_index is required for all CRM rows")
        if ji in seen:
            continue
        seen.add(ji)
        dedup.append(r)
    rows = dedup

    # 2) upsert for cross-run idempotency on (user_id, job_index)
    stmt = text("""
        INSERT INTO staging_crm
          (run_id, user_id, source_id, job_index,
           address1, address2, city, state, zip, full_address,
           job_date, job_value)
        VALUES
          (:rid, :uid, :source_id, :job_index,
           :address1, :address2, :city, :state, :zip, :full_address,
           :job_date, :job_value)
        ON CONFLICT (user_id, job_index) DO UPDATE
        SET
          run_id       = EXCLUDED.run_id,
          source_id    = COALESCE(EXCLUDED.source_id, staging_crm.source_id),
          address1     = EXCLUDED.address1,
          address2     = EXCLUDED.address2,
          city         = EXCLUDED.city,
          state        = EXCLUDED.state,
          zip          = EXCLUDED.zip,
          full_address = EXCLUDED.full_address,
          job_date     = EXCLUDED.job_date,
          job_value    = COALESCE(EXCLUDED.job_value, staging_crm.job_value)
    """)

    payload = []
    for r in rows:
        src = r.get("source_id")
        addr2 = r.get("address2")
        payload.append({
            "rid": run_id,
            "uid": user_id,
            "source_id": (src.strip() if isinstance(src, str) and src.strip() else None),
            "job_index": str(r.get("job_index")).strip(),
            "address1": (r.get("address1") or "").strip(),
            "address2": (addr2.strip() if isinstance(addr2, str) and addr2.strip() else None),
            "city": (r.get("city") or "").strip(),
            "state": (r.get("state") or "").strip(),
            "zip": (str(r.get("zip")) if r.get("zip") is not None else "").strip(),
            "full_address": (r.get("full_address") or "").strip(),
            "job_date": r.get("job_date"),
            "job_value": r.get("job_value"),
        })

    db.session.execute(stmt, payload)
    return len(payload)


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