# app/services/mapper.py
from __future__ import annotations

from typing import Tuple, Dict, Any, List, Optional, Set
import csv
import io

from app.dao import mapper_dao

# Required columns per source (canonical normalized shape)
REQUIRED_MAIL = {"address1", "city", "state", "zip", "sent_date"}
REQUIRED_CRM  = {"address1", "city", "state", "zip", "job_date"}

ALIAS_MAIL = {
    "source_id": ["source_id", "source id", "id", "mail_id", "record_id"],
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "sent_date": ["sent_date", "sent date", "mail_date", "mail date", "date", "sent", "mailed", "mailed_at",
        "mailed at", "date mailed", "mailed date", "mailed_on", "mailed on", "postmark", "postmarked",
        "postmark date", "mailing date", "outbound date"],
}
ALIAS_CRM = {
    "source_id": ["source_id", "source id", "external_id", "ext_id", "crm_id", "lead_id", "job_id", "id"],
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "job_date": ["job_date", "date", "created_at", "job date"],
    "job_value": ["job_value", "amount", "value", "revenue", "job value"],
}

def canon_for(source: str):
    if source == "mail":
        return REQUIRED_MAIL, ALIAS_MAIL
    return REQUIRED_CRM, ALIAS_CRM

def _first_present(row: dict, names: List[str]) -> Optional[str]:
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    return None

def apply_mapping(rows: List[dict], mapping: Dict[str, str], alias: Dict[str, List[str]]) -> List[dict]:
    """Rename columns to canonical keys using explicit mapping first, then alias fallbacks."""
    out: List[dict] = []
    for r in rows:
        low = { (k or "").strip().lower(): v for k, v in r.items() }
        canonical: Dict[str, Any] = {}
        # explicit mapping wins
        for want, src in (mapping or {}).items():
            src_l = (src or "").strip().lower()
            if src_l:
                canonical[want] = low.get(src_l)
        # fill gaps via alias
        for want, alts in alias.items():
            if want not in canonical or canonical[want] in (None, ""):
                canonical[want] = _first_present(low, [a.lower() for a in alts] + [want])
        out.append(canonical)
    return out

def _csv_to_rows(file_stream, encoding: str = "utf-8") -> List[dict]:
    """Parse a CSV stream into a list of dicts with original headers."""
    data = file_stream.read()
    if isinstance(data, bytes):
        data = data.decode(encoding, errors="replace")
    f = io.StringIO(data)
    reader = csv.DictReader(f)
    return [dict(row) for row in reader]

def ingest_raw_file(
    run_id: str,
    user_id: str,
    source: str,
    file_stream,
    *,
    filename: str = "",
) -> Tuple[str, Dict[str, Any]]:
    """
    Upload step: store RAW only. Never normalize here.
    Returns:
      ("ok", {run_id, source, state:'raw_only', raw_count, sample_headers, sample_rows})
    """
    source = (source or "").lower().strip()
    if source not in {"mail", "crm"}:
        raise ValueError(f"invalid source: {source}")

    raw_rows = _csv_to_rows(file_stream)

    mapper_dao.insert_raw_rows(run_id, user_id, source, raw_rows)

    hdrs = mapper_dao.get_raw_headers(run_id, source, sample=25)

    return (
        "ok",
        {
            "run_id": run_id,
            "source": source,
            "state": "raw_only",
            "raw_count": len(raw_rows),
            "sample_headers": hdrs.get("headers", []),
            "sample_rows": hdrs.get("sample_rows", []),
            "message": "RAW uploaded",
        },
    )

def get_headers(run_id: str, source: str, sample: int = 25) -> Dict[str, Any]:
    """Proxy DAO for the mapper UI."""
    return mapper_dao.get_raw_headers(run_id, source, sample)

def get_mapping(run_id: str, source: str) -> Dict[str, Any]:
    return mapper_dao.get_mapping(run_id, source)

def save_mapping(run_id: str, user_id: str, source: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
    required, alias = canon_for(source)
    mapper_dao.save_mapping(run_id, user_id, source, mapping)
    hdrs = mapper_dao.get_raw_headers(run_id, source, sample=1)  # cheap existence check
    return {"ok": True, "run_id": run_id, "source": source, "mapping_saved": True}
