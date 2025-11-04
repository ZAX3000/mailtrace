from __future__ import annotations

import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rapidfuzz import fuzz, process

# DB persist
from app.dao import matches_dao

# -----------------------
# Small utils / constants
# -----------------------

def _mt_clean(_s: Any) -> str:
    return "" if _s is None else str(_s).strip()

MATCH_MIN_SCORE: float = 0.0
try:
    MATCH_MIN_SCORE = float((os.getenv("MATCH_MIN_SCORE") or "").strip() or "0")
except (TypeError, ValueError):
    pass

FAST_FILTERS = (os.getenv("FAST_FILTERS", "1").strip() != "0")
LIMIT_TOPK   = int((os.getenv("TOPK_RECHECK") or "1").strip())

# --- Address normalization helpers ---
STREET_TYPES: Dict[str, str] = {
    "street": "street", "st": "street", "st.": "street",
    "road": "road", "rd": "road", "rd.": "road",
    "avenue": "avenue", "ave": "avenue", "ave.": "avenue", "av": "avenue", "av.": "avenue",
    "boulevard": "boulevard", "blvd": "boulevard", "blvd.": "boulevard",
    "lane": "lane", "ln": "lane", "ln.": "lane",
    "drive": "drive", "dr": "drive", "dr.": "drive",
    "court": "court", "ct": "court", "ct.": "court",
    "circle": "circle", "cir": "circle", "cir.": "circle",
    "parkway": "parkway", "pkwy": "parkway", "pkwy.": "parkway",
    "highway": "highway", "hwy": "highway", "hwy.": "highway",
    "terrace": "terrace", "ter": "terrace", "ter.": "terrace",
    "place": "place", "pl": "place", "pl.": "place",
    "way": "way", "wy": "way", "wy.": "way",
    "trail": "trail", "trl": "trail", "trl.": "trail",
    "alley": "alley", "aly": "alley", "aly.": "alley",
    "common": "common", "cmn": "common", "cmn.": "common",
    "park": "park",
}
DIRECTIONALS: Dict[str, str] = {
    "n": "north", "n.": "north", "north": "north",
    "s": "south", "s.": "south", "south": "south",
    "e": "east",  "e.": "east",  "east": "east",
    "w": "west",  "w.": "west",  "west": "west",
    "ne": "northeast", "ne.": "northeast",
    "nw": "northwest", "nw.": "northwest",
    "se": "southeast", "se.": "southeast",
    "sw": "southwest", "sw.": "southwest",
}
UNIT_WORDS = {"apt", "apartment", "suite", "ste", "unit", "#"}

def _squash_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _norm_token(tok: str) -> str:
    t = tok.lower().strip(".,")
    if t in STREET_TYPES:   return STREET_TYPES[t]
    if t in DIRECTIONALS:   return DIRECTIONALS[t]
    return t

def normalize_address1(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace("-", " ")
    s = re.sub(r"[^\w#\s]", " ", s)
    parts = [_norm_token(p) for p in s.lower().split() if p.strip()]
    return _squash_ws(" ".join(parts))

def block_key(addr1: str) -> str:
    """
    Base block: "<first-token>|<second-initial>" to keep it tolerant.
    """
    if not isinstance(addr1, str): return ""
    toks = [t for t in _squash_ws(addr1).split() if t]
    if not toks: return ""
    first = toks[0]
    second_initial = toks[1][0] if len(toks) > 1 else ""
    return f"{first}|{second_initial}".lower()

def tokens(s: str) -> List[str]:
    return [t for t in normalize_address1(s).split() if t]

def street_type_of(tok_list: List[str]) -> Optional[str]:
    if not tok_list: return None
    last = tok_list[-1]
    return last if last in STREET_TYPES.values() else None

def directional_in(tok_list: List[str]) -> Optional[str]:
    for t in tok_list:
        if t in DIRECTIONALS.values():
            return t
    return None

def _ratio(a: str, b: str) -> float:
    return fuzz.token_set_ratio(_mt_clean(a), _mt_clean(b)) / 100.0

def address_similarity(a1: str, b1: str) -> float:
    na, nb = normalize_address1(a1), normalize_address1(b1)
    if not na or not nb:
        return 0.0
    return _ratio(na, nb)

# -----------------------
# Canonicalization
# -----------------------

MAIL_CANON_MAP: Dict[str, List[str]] = {
    "line_no": ["line_no"],
    "source_id": ["id", "mail_id"],
    "address1": ["address1", "addr1", "address", "street", "line1"],
    "address2": ["address2", "addr2", "unit", "line2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code"],
    "sent_date": ["sent_date", "date", "mail_date"],
}

CRM_CANON_MAP: Dict[str, List[str]] = {
    "line_no": ["line_no"],
    "source_id": ["crm_id", "id", "lead_id", "job_id"],
    "address1": ["address1", "addr1", "address", "street", "line1"],
    "address2": ["address2", "addr2", "unit", "line2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code"],
    "job_date": ["job_date", "date", "created_at"],
    "job_value": ["job_value", "amount", "value", "revenue"],
}

def _canonize_row(row: Dict[str, Any], mapping: Dict[str, List[str]]) -> Dict[str, Any]:
    """Return a new dict with canonical keys; if key missing, fill with ''."""
    lowered = {str(k).lower().strip(): v for k, v in row.items()}
    out: Dict[str, Any] = {}
    for want, alts in mapping.items():
        if want in lowered:
            out[want] = lowered[want]
        else:
            val = ""
            for a in alts:
                if a in lowered:
                    val = lowered[a]
                    break
            out[want] = val
    return out

def _prep_mail_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        c = _canonize_row(r, MAIL_CANON_MAP)
        c["_blk"]       = block_key(c.get("address1", ""))
        c["_date"]      = c.get("sent_date", "")
        c["_addr_norm"] = normalize_address1(str(c.get("address1", "")))
        c["_addr_str"]  = c["_addr_norm"]  # string fed to RapidFuzz
        c["_zip5"]      = str(c.get("zip", "")).strip()[:5]
        c["_city_l"]    = str(c.get("city", "")).strip().lower()
        c["_state_l"]   = str(c.get("state", "")).strip().lower()
        out.append(c)
    return out

def _prep_crm_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        c = _canonize_row(r, CRM_CANON_MAP)
        c["_blk"]       = block_key(c.get("address1", ""))
        c["_date"]      = c.get("job_date", "")
        c["_addr_norm"] = normalize_address1(str(c.get("address1", "")))
        c["_addr_str"]  = c["_addr_norm"]
        c["_zip5"]      = str(c.get("zip", "")).strip()[:5]
        c["_city_l"]    = str(c.get("city", "")).strip().lower()
        c["_state_l"]   = str(c.get("state", "")).strip().lower()
        out.append(c)
    return out

# -----------------------
# Scoring + notes
# -----------------------

def _bonus_adjust(score_base: int, mail_row: Dict[str, Any], crm_row: Dict[str, Any]) -> int:
    score = score_base

    mz, cz = mail_row.get("_zip5", ""), crm_row.get("_zip5", "")
    if mz and cz and mz == cz:
        score = min(100, score + 5)
    if mail_row.get("_city_l") and crm_row.get("_city_l") and mail_row["_city_l"] == crm_row["_city_l"]:
        score = min(100, score + 2)
    if mail_row.get("_state_l") and crm_row.get("_state_l") and mail_row["_state_l"] == crm_row["_state_l"]:
        score = min(100, score + 2)
    return score

def _notes_for(mail_row: Dict[str, Any], crm_row: Dict[str, Any]) -> List[str]:
    a_mail = str(mail_row.get("address1", ""))
    a_crm  = str(crm_row.get("address1", ""))
    notes: List[str] = []
    ta, tb = tokens(a_crm), tokens(a_mail)
    st_a, st_b = street_type_of(ta), street_type_of(tb)
    if st_a != st_b and (st_a or st_b):
        notes.append(f"{st_b or 'none'} vs {st_a or 'none'} (street type)")
    dir_a, dir_b = directional_in(ta), directional_in(tb)
    if dir_a != dir_b and (dir_a or dir_b):
        notes.append(f"{dir_b or 'none'} vs {dir_a or 'none'} (direction)")

    unit_a = str(crm_row.get("address2", "") or "").strip()
    unit_b = str(mail_row.get("address2", "") or "").strip()
    if bool(unit_a) != bool(unit_b):
        notes.append(f"{unit_b or 'none'} vs {unit_a or 'none'} (unit)")
    elif unit_a and unit_b and unit_a.lower() != unit_b.lower():
        notes.append(f"{unit_b} vs {unit_a} (unit)")

    if not notes:
        notes.append("perfect match")
    return notes

# -----------------------
# Main API (RapidFuzz bulk)
# -----------------------

excluded_rows_collect: List[Dict[str, Any]] = []

def _tight_block_key(base_blk: str, zip5: str) -> Tuple[str, str]:
    return (base_blk or ""), (zip5 or "")

def run_matching(
    mail_rows: Iterable[Dict[str, Any]],
    crm_rows:  Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    excluded_rows_collect.clear()

    mail = _prep_mail_rows(mail_rows)
    crm  = _prep_crm_rows(crm_rows)

    mail_groups: Dict[str, List[Dict[str, Any]]] = {}
    for m in mail:
        mail_groups.setdefault(m["_blk"], []).append(m)

    mail_groups_zip: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    if FAST_FILTERS:
        for blk, lst in mail_groups.items():
            for m in lst:
                key = _tight_block_key(blk, m.get("_zip5", ""))
                mail_groups_zip.setdefault(key, []).append(m)

    out_rows: List[Dict[str, Any]] = []

    for c in crm:
        blk = c["_blk"]

        candidates: Optional[List[Dict[str, Any]]] = None
        if FAST_FILTERS and c.get("_zip5"):
            candidates = mail_groups_zip.get(_tight_block_key(blk, c["_zip5"]))
            if not candidates:
                candidates = mail_groups.get(blk)
        else:
            candidates = mail_groups.get(blk)

        if not candidates:
            excluded_rows_collect.append({
                "source_id": c.get("source_id", ""),
                "reason": "no_block_candidates",
                "block": blk,
                "address1": c.get("address1", ""),
                "zip": str(c.get("zip", "")),
            })
            continue

        cand = candidates
        if c["_date"]:
            dt = c["_date"]
            cand = [m for m in candidates if (m["_date"] is None) or (m["_date"] <= dt)]
            if not cand:
                excluded_rows_collect.append({
                    "source_id": c.get("source_id", ""),
                    "reason": "no_date_window_candidates",
                    "block": blk,
                    "address1": c.get("address1", ""),
                    "zip": str(c.get("zip", "")),
                })
                continue

        if FAST_FILTERS:
            cz, cc, cs = c.get("_zip5", ""), c.get("_city_l", ""), c.get("_state_l", "")
            cand_fast: List[Dict[str, Any]] = []
            for m in cand:
                # If both have zip5 and they differ => skip
                if cz and m.get("_zip5") and cz != m["_zip5"]:
                    continue
                # If both have city+state and both differ => skip
                if cc and cs and m.get("_city_l") and m.get("_state_l"):
                    if (cc != m["_city_l"]) and (cs != m["_state_l"]):
                        continue
                cand_fast.append(m)
            if cand_fast:
                cand = cand_fast

        addr_query = c["_addr_str"]
        cand_strings = [m["_addr_str"] for m in cand]

        best: Optional[Dict[str, Any]] = None
        best_score = -1
        best_notes: List[str] = []

        if not cand_strings:
            continue

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
                m_date_cmp: date = m["_date"] if isinstance(m.get("_date"), date) else date.min
                best_date_cmp: date = best["_date"] if (best and isinstance(best.get("_date"), date)) else date.max
                if adj > best_score or (adj == best_score and m_date_cmp < best_date_cmp):
                    best, best_score = m, adj
            if best:
                best_notes = _notes_for(best, c)

        prior_dates: List[date] = sorted([d for d in (m.get("_date") for m in cand) if isinstance(d, date)])
        last_mail_dt = max(prior_dates) if prior_dates else None

        if not best:
            continue

        full_mail = " ".join([
            str(best.get("address1", "")).strip(),
            str(best.get("address2", "")).strip(),
            str(best.get("city", "")).strip(),
            str(best.get("state", "")).strip(),
            str(best.get("zip", "")).strip(),
        ]).replace("  ", " ").strip()

        full_crm = " ".join([
            str(c.get("address1", "")).strip(),
            str(c.get("address2", "")).strip(),
            str(c.get("city", "")).strip(),
            str(c.get("state", "")).strip(),
            str(c.get("zip", "")).strip(),
        ]).replace("  ", " ").strip()

        row = {
            "crm_line_no": c.get("line_no", ""),
            "crm_id": c.get("source_id", ""),
            "crm_address1": c.get("address1", ""),
            "crm_address2": c.get("address2", ""),
            "crm_city": c.get("city", ""),
            "crm_state": c.get("state", ""),
            "crm_zip": str(c.get("zip", "")),

            "crm_job_date": c.get("_date"),
            "last_mail_date": last_mail_dt,

            "job_value": c.get("job_value", ""),

            "mail_id": best.get("source_id", ""),
            "mail_line_no": best.get("line_no", ""),
            "mail_full_address": full_mail.replace(" None", "").replace(" none", ""),
            "crm_full_address": full_crm.replace(" None", "").replace(" none", ""),

            "mail_count_in_window": len(prior_dates),

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
    raw_rows = run_matching(mail_rows, crm_rows)

    transformed: List[Dict[str, Any]] = []
    for r in raw_rows:
        transformed.append({
            **r,
            "zip5": str(r.get("crm_zip") or "")[:5],
            "state": str(r.get("crm_state") or "")[:2],
        })

    matches_dao.delete_for_run(run_id, user_id)
    inserted = matches_dao.bulk_insert(run_id, user_id, transformed)
    return inserted