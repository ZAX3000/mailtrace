# app/services/mapper.py
from __future__ import annotations

from typing import Tuple, Dict, Any, List, Optional, Set, IO
import csv
import io

from app.dao import mapper_dao

# ---------- Canonical requirements & alias maps ----------

REQUIRED_MAIL: Set[str] = {"address1", "city", "state", "zip", "sent_date"}
REQUIRED_CRM: Set[str] = {"address1", "city", "state", "zip", "job_date"}

ALIAS_MAIL: Dict[str, List[str]] = {
    "source_id": ["source_id", "source id", "id", "mail_id", "record_id"],
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "sent_date": [
        "sent_date", "sent date", "mail_date", "mail date", "date", "sent", "mailed", "mailed_at",
        "mailed at", "date mailed", "mailed date", "mailed_on", "mailed on", "postmark", "postmarked",
        "postmark date", "mailing date", "outbound date",
    ],
}

ALIAS_CRM: Dict[str, List[str]] = {
    "source_id": ["source_id", "source id", "external_id", "ext_id", "lead_id", "job_id", "id"],
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "job_date": ["job_date", "date", "created_at", "job date"],
    "job_value": ["job_value", "amount", "value", "revenue", "job value"],
}


def canon_for(source: str) -> Tuple[Set[str], Dict[str, List[str]]]:
    """
    Given a source ("mail" or "crm"), return (required_fields, alias_map).
    """
    s = (source or "").strip().lower()
    if s == "mail":
        return REQUIRED_MAIL, ALIAS_MAIL
    return REQUIRED_CRM, ALIAS_CRM


def _first_present(row: Dict[str, Any], names: List[str]) -> Optional[str]:
    """
    Return the first present non-empty value for any of the given names in the row.
    Keys in `row` are assumed already lowercase.
    """
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    return None


def apply_mapping(
    rows: List[Dict[str, Any]],
    mapping: Dict[str, Any],
    alias: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    """
    Rename columns to canonical keys using explicit mapping first, then alias fallbacks.
    This prepares rows for staging insert (normalization step later in the pipeline).
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        low: Dict[str, Any] = {(k or "").strip().lower(): v for k, v in r.items()}
        canonical: Dict[str, Any] = {}

        # 1) Explicit mapping wins
        for want, src in (mapping or {}).items():
            src_l = (str(src or "")).strip().lower()
            if src_l:
                canonical[want] = low.get(src_l)

        # 2) Fill gaps via alias fallbacks
        for want, alts in alias.items():
            if want not in canonical or canonical[want] in (None, ""):
                canonical[want] = _first_present(low, [a.lower() for a in alts] + [want])

        out.append(canonical)
    return out


def _csv_to_rows(file_stream: IO[bytes], encoding: str = "utf-8") -> List[Dict[str, Any]]:
    """
    Parse a CSV stream (bytes) into a list of dicts with original headers.
    """
    raw: bytes = file_stream.read()
    text_data: str = raw.decode(encoding, errors="replace")
    f = io.StringIO(text_data)
    reader = csv.DictReader(f)
    return [dict(row) for row in reader]


def ingest_raw_file(
    run_id: str,
    user_id: str,
    source: str,
    file_stream: IO[bytes],
    filename: str = "",
) -> Dict[str, Any]:
    """
    Upload step: store RAW only (no normalization here).
    Returns a single JSON-serializable payload.
    """
    src = (source or "").lower().strip()
    if src not in {"mail", "crm"}:
        raise ValueError(f"invalid source: {source}")

    raw_rows: List[Dict[str, Any]] = _csv_to_rows(file_stream)
    mapper_dao.insert_raw_rows(run_id, user_id, src, raw_rows)

    hdrs: Dict[str, Any] = mapper_dao.get_raw_headers(run_id, src, sample=25)

    return {
        "ok": True,
        "run_id": run_id,
        "source": src,
        "state": "raw_only",
        "raw_count": len(raw_rows),
        "sample_headers": hdrs.get("headers", []),
        "sample_rows": hdrs.get("sample_rows", []),
        "filename": filename or "",
        "message": "RAW uploaded",
    }


def get_headers(run_id: str, source: str, sample: int = 25) -> Dict[str, Any]:
    """
    Proxy DAO for the mapper UI — returns sample headers / rows from RAW.
    """
    return mapper_dao.get_raw_headers(run_id, (source or "").lower().strip(), sample)


def get_mapping(run_id: str, source: str) -> Dict[str, Any]:
    """
    Fetch the stored mapping JSON (if any) for the given run+source.
    """
    return mapper_dao.get_mapping(run_id, (source or "").lower().strip())


def save_mapping(run_id: str, user_id: str, source: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upsert the mapping JSON for (run_id, source). Requires UNIQUE(run_id, source).
    """
    mapper_dao.save_mapping(run_id, user_id, (source or "").lower().strip(), mapping)
    return {"ok": True, "run_id": run_id, "source": (source or "").lower().strip(), "mapping_saved": True}