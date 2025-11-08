# app/services/matching.py
from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, Iterable, Optional, List

from rapidfuzz import fuzz, process

# DB persist
from app.dao import matches_dao

# Reuse normalization utilities (already applied upstream; used here for notes logic)
from app.utils.normalize import (
    tokens,
    street_type_of,
    directional_in,
)

# -----------------------
# Small utils / constants
# -----------------------

def _mt_clean(_s: Any) -> str:
    return "" if _s is None else str(_s).strip()

# Tuning via env
MATCH_MIN_SCORE: float = 0.0
try:
    MATCH_MIN_SCORE = float((os.getenv("MATCH_MIN_SCORE") or "").strip() or "0")
except (TypeError, ValueError):
    pass

FAST_FILTERS = (os.getenv("FAST_FILTERS", "1").strip() != "0")
LIMIT_TOPK   = int((os.getenv("TOPK_RECHECK") or "1").strip())

# -----------------------
# Scoring + notes
# -----------------------

def _bonus_adjust(score_base: int, mail_row: Dict[str, Any], crm_row: Dict[str, Any]) -> int:
    """
    Lightweight deterministic bump based on exact field equality after ingestion.
    """
    score = score_base

    mz, cz = mail_row.get("_zip5", ""), crm_row.get("_zip5", "")
    if mz and cz and mz == cz:
        score = min(100, score + 5)

    if mail_row.get("_city_l") and crm_row.get("_city_l") and mail_row["_city_l"] == crm_row["_city_l"]:
        score = min(100, score + 2)

    if mail_row.get("_state_l") and crm_row.get("_state_l") and mail_row["_state_l"] == crm_row["_state_l"]:
        score = min(100, score + 2)

    return score

def _notes_for(mail_row: Dict[str, Any], crm_row: Dict[str, Any]) -> list[str]:
    """
    Human-friendly comparison notes using already-ingested raw fields.
    (Address tokenization helpers come from utils.normalize.)
    """
    a_mail = str(mail_row.get("address1", ""))
    a_crm  = str(crm_row.get("address1", ""))
    notes: list[str] = []

    ta, tb = tokens(a_crm), tokens(a_mail)
    st_a, st_b = street_type_of(ta), street_type_of(tb)
    if st_a != st_b and (st_a or st_b):
        notes.append(f"{st_b or 'none'} vs {st_a or 'none'} (street type)")

    dir_a, dir_b = directional_in(ta), directional_in(tb)
    if dir_a != dir_b and (dir_a or dir_b):
        notes.append(f"{dir_b or 'none'} vs {dir_a or 'none'} (direction)")

    unit_a = _mt_clean(crm_row.get("address2", ""))
    unit_b = _mt_clean(mail_row.get("address2", ""))
    if bool(unit_a) != bool(unit_b):
        notes.append(f"{unit_b or 'none'} vs {unit_a or 'none'} (unit)")
    elif unit_a and unit_b and unit_a.lower() != unit_b.lower():
        notes.append(f"{unit_b} vs {unit_a} (unit)")

    if not notes:
        notes.append("perfect match")
    return notes

# -----------------------
# Main (RapidFuzz bulk)
# -----------------------

# Optional artifact for dashboard panel (no-DB)
excluded_rows_collect: list[Dict[str, Any]] = []

def run_matching(
    mail_rows: Iterable[Dict[str, Any]],
    crm_rows:  Iterable[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    """
    Inputs are the *ingested + prepped* rows:
      - mail_rows/crm_rows include DB fields (address1/2, city, state, zip, dates, full_address, etc.)
      - plus ephemeral fields set by pipeline prep:
          _addr_str (normalized address1 string for RF),
          _zip5, _city_l, _state_l,
          _date (sent_date for mail, job_date for crm)
    """
    excluded_rows_collect.clear()

    # Build a fast candidate map by ZIP5; fallback to 'all mail' if needed
    mail_rows_list: List[Dict[str, Any]] = list(mail_rows)
    crm_rows_list:  List[Dict[str, Any]] = list(crm_rows)

    mail_by_zip: dict[str, list[Dict[str, Any]]] = {}
    for m in mail_rows_list:
        key = str(m.get("_zip5", "") or "")
        mail_by_zip.setdefault(key, []).append(m)

    out_rows: list[Dict[str, Any]] = []

    for c in crm_rows_list:
        # 1) Primary candidate set by _zip5; fallback to all mail if empty or missing
        czip = str(c.get("_zip5", "") or "")
        candidates = mail_by_zip.get(czip, []) or mail_rows_list

        # 2) Date window: only include mail with date <= CRM job date
        cand = candidates
        if c.get("_date"):
            dt = c["_date"]
            cand = [m for m in candidates if (m.get("_date") is None) or (m["_date"] <= dt)]
            if not cand:
                excluded_rows_collect.append({
                    "source_id": c.get("source_id", ""),
                    "reason": "no_date_window_candidates",
                    "zip5": czip,
                    "address1": c.get("address1", ""),
                    "zip": str(c.get("zip", "")),
                })
                continue

        # 3) Optional quick city/state consistency filter (FAST_FILTERS)
        if FAST_FILTERS:
            cz, cc, cs = c.get("_zip5", ""), c.get("_city_l", ""), c.get("_state_l", "")
            cand_fast: list[Dict[str, Any]] = []
            for m in cand:
                if cz and m.get("_zip5") and cz != m["_zip5"]:
                    continue
                if cc and cs and m.get("_city_l") and m.get("_state_l"):
                    if (cc != m["_city_l"]) and (cs != m["_state_l"]):
                        continue
                cand_fast.append(m)
            if cand_fast:
                cand = cand_fast

        # 4) RapidFuzz match on precomputed normalized strings
        addr_query = _mt_clean(c.get("_addr_str", ""))
        cand_strings = [_mt_clean(m.get("_addr_str", "")) for m in cand]

        if not addr_query or not cand_strings:
            excluded_rows_collect.append({
                "source_id": c.get("source_id", ""),
                "reason": "no_address_string",
                "zip5": czip,
                "address1": c.get("address1", ""),
                "zip": str(c.get("zip", "")),
            })
            continue

        best: Optional[Dict[str, Any]] = None
        best_score = -1
        best_notes: list[str] = []

        if LIMIT_TOPK <= 1:
            matched = process.extractOne(
                addr_query, cand_strings, scorer=fuzz.token_set_ratio, score_cutoff=0
            )
            if matched:
                _, base_score, idx = matched
                m = cand[idx]
                adj = _bonus_adjust(int(base_score), m, c)
                best, best_score = m, adj
                best_notes = _notes_for(m, c)
        else:
            topk = process.extract(
                addr_query, cand_strings, scorer=fuzz.token_set_ratio, limit=LIMIT_TOPK, score_cutoff=0
            )
            for _, base_score, idx in topk:
                m = cand[idx]
                adj = _bonus_adjust(int(base_score), m, c)
                # tie-breaker: pick the earlier mail date (closest to first touch)
                m_date_cmp: date = m["_date"] if isinstance(m.get("_date"), date) else date.min
                best_date_cmp: date = best["_date"] if (best and isinstance(best.get("_date"), date)) else date.max
                if adj > best_score or (adj == best_score and m_date_cmp < best_date_cmp):
                    best, best_score = m, adj
            if best:
                best_notes = _notes_for(best, c)

        if not best:
            excluded_rows_collect.append({
                "source_id": c.get("source_id", ""),
                "reason": "no_match_found",
                "zip5": czip,
                "address1": c.get("address1", ""),
                "zip": str(c.get("zip", "")),
            })
            continue

        # Build arrays from all candidates within the window (after filters)
        mail_ids: list[str] = []
        matched_mail_dates: list[date] = []
        for m in cand:
            mid = m.get("source_id")
            if mid:
                mail_ids.append(str(mid))
            d = m.get("_date")
            if isinstance(d, date):
                matched_mail_dates.append(d)

        # de-dup + sort for determinism
        mail_ids = sorted(set(mail_ids))
        matched_mail_dates = sorted(set(matched_mail_dates))

        # Prefer the full addresses already stored in staging (cleanest) for "best"
        full_mail = _mt_clean(best.get("full_address", ""))
        full_crm  = _mt_clean(c.get("full_address", ""))

        row = {
            # CRM side
            "crm_line_no": c.get("line_no", ""),
            "crm_id": c.get("source_id", ""),
            "job_index": (c.get("job_index") or None),
            "crm_address1": c.get("address1", ""),
            "crm_address2": c.get("address2", ""),
            "crm_city": c.get("city", ""),
            "crm_state": c.get("state", ""),
            "crm_zip": _mt_clean(c.get("zip", "")),
            "crm_full_address": full_crm,
            "crm_job_date": c.get("_date"),
            "job_value": c.get("job_value") or None,

            # MAIL context (winner + arrays)
            "mail_full_address": full_mail,
            "mail_ids": mail_ids,
            "matched_mail_dates": matched_mail_dates,

            # Scoring/notes (from best)
            "confidence_percent": int(best_score),
            "match_notes": ("; ".join(best_notes) if best_notes else "perfect match"),
        }

        if best_score >= MATCH_MIN_SCORE:
            out_rows.append(row)

    # Optional artifact for dashboard panel (no-DB)
    try:
        _static_dir = os.path.join(os.path.dirname(__file__), "static")
        os.makedirs(_static_dir, exist_ok=True)
        _json_path = os.path.join(_static_dir, "excluded_latest.json")
        with open(_json_path, "w", encoding="utf-8") as f:
            json.dump(excluded_rows_collect, f, ensure_ascii=False)
    except OSError:
        pass

    return out_rows


# -----------------------
# Persist to DB wrapper
# -----------------------

def persist_matches_for_run(
    run_id: str,
    user_id: str,
    mail_rows: Iterable[Dict[str, Any]],
    crm_rows:  Iterable[Dict[str, Any]],
) -> int:
    """
    Mail/CRM rows here should already include the prepped ephemeral fields.
    """
    raw_rows = run_matching(mail_rows, crm_rows)

    transformed: list[Dict[str, Any]] = []
    for r in raw_rows:
        transformed.append({
            **r,
            "zip5": str(r.get("crm_zip") or "")[:5],        # keep column contract with matches table
            "state": str(r.get("crm_state") or "")[:2],
        })

    matches_dao.delete_for_run(run_id, user_id)
    inserted = matches_dao.bulk_insert(run_id, user_id, transformed)
    return inserted