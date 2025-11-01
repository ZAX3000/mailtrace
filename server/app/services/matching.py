# app/services/matching.py
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rapidfuzz import fuzz

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

DATE_FORMATS: Tuple[str, ...] = (
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y",
)

def parse_date_any(s: Any) -> Optional[date]:
    if not isinstance(s, str) or not s.strip():
        return None
    z = s.strip().replace("/", "-")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(z, fmt).date()
        except ValueError:
            continue
    # last-resort: try ISO with time
    try:
        return datetime.fromisoformat(s.strip().replace("Z", "+00:00")).date()
    except Exception:
        return None

def fmt_mm_dd_yy(d: Optional[date]) -> str:
    return d.strftime("%m-%d-%y") if isinstance(d, date) else "None provided"

# --- RapidFuzz scoring (token-set is robust to order/dups) ---
def _ratio(a: str, b: str) -> float:
    return fuzz.token_set_ratio(_mt_clean(a), _mt_clean(b)) / 100.0

def address_similarity(a1: str, b1: str) -> float:
    na, nb = normalize_address1(a1), normalize_address1(b1)
    if not na or not nb:
        return 0.0
    return _ratio(na, nb)

# -----------------------
# Canonicalization (no pandas)
# -----------------------

MAIL_CANON_MAP: Dict[str, List[str]] = {
    "id": ["id", "mail_id"],
    "address1": ["address1", "addr1", "address", "street", "line1"],
    "address2": ["address2", "addr2", "unit", "line2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code"],
    "sent_date": ["sent_date", "date", "mail_date"],
}

CRM_CANON_MAP: Dict[str, List[str]] = {
    "crm_id": ["crm_id", "id", "lead_id", "job_id"],
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
            # take the first available alias
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
        c["_blk"]  = block_key(c.get("address1", ""))
        c["_date"] = parse_date_any(c.get("sent_date", ""))
        out.append(c)
    return out

def _prep_crm_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        c = _canonize_row(r, CRM_CANON_MAP)
        c["_blk"]  = block_key(c.get("address1", ""))
        c["_date"] = parse_date_any(c.get("job_date", ""))
        out.append(c)
    return out

# -----------------------
# Scoring (no pandas)
# -----------------------

def score_row(mail_row: Dict[str, Any], crm_row: Dict[str, Any]) -> Tuple[int, List[str]]:
    a_mail = str(mail_row.get("address1", ""))
    a_crm  = str(crm_row.get("address1", ""))
    sim = address_similarity(a_mail, a_crm)
    score = int(round(sim * 100))

    mz = str(mail_row.get("zip", "")).strip()
    cz = str(crm_row.get("zip", "")).strip()
    if mz[:5] and cz[:5] and mz[:5] == cz[:5]:
        score = min(100, score + 5)

    if str(mail_row.get("city", "")).strip().lower() == str(crm_row.get("city", "")).strip().lower():
        score = min(100, score + 2)
    if str(mail_row.get("state", "")).strip().lower() == str(crm_row.get("state", "")).strip().lower():
        score = min(100, score + 2)

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

    if score >= 100 and not notes:
        return 100, ["perfect match"]
    return min(100, score), notes

# -----------------------
# Main API (no pandas)
# -----------------------

# Collect “skipped” rows for optional artifact
excluded_rows_collect: List[Dict[str, Any]] = []

def run_matching(
    mail_rows: Iterable[Dict[str, Any]],
    crm_rows:  Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Pure-Python matcher:
      - Canonicalizes rows (header tolerances preserved)
      - Groups MAIL by block key
      - For each CRM row, filters MAIL by block & date window, scores candidates
      - Picks best score (tie-break by earliest mail date)
      - Emits list of dicts (former DataFrame rows)
    """
    excluded_rows_collect.clear()

    mail = _prep_mail_rows(mail_rows)
    crm  = _prep_crm_rows(crm_rows)

    # Group mail by block key
    mail_groups: Dict[str, List[Dict[str, Any]]] = {}
    for m in mail:
        mail_groups.setdefault(m["_blk"], []).append(m)

    out_rows: List[Dict[str, Any]] = []

    for c in crm:
        blk = c["_blk"]
        candidates = mail_groups.get(blk)
        if not candidates:
            excluded_rows_collect.append({
                "crm_id": c.get("crm_id", ""),
                "reason": "no_block_candidates",
                "block": blk,
                "address1": c.get("address1", ""),
                "zip": str(c.get("zip", "")),
            })
            continue

        # date window: only mail with no date or <= crm date
        cand = candidates
        if c["_date"]:
            dt = c["_date"]
            cand = [m for m in candidates if (m["_date"] is None) or (m["_date"] <= dt)]
            if not cand:
                excluded_rows_collect.append({
                    "crm_id": c.get("crm_id", ""),
                    "reason": "no_date_window_candidates",
                    "block": blk,
                    "address1": c.get("address1", ""),
                    "zip": str(c.get("zip", "")),
                })
                continue

        # Find best candidate
        best: Optional[Dict[str, Any]] = None
        best_score = -1
        best_notes: List[str] = []

        for m in cand:
            s, notes = score_row(m, c)
            # tie-breaker: prefer earliest mail date
            m_date_cmp: date = m["_date"] if isinstance(m.get("_date"), date) else date.min
            best_date_cmp: date = best["_date"] if (best and isinstance(best.get("_date"), date)) else date.max

            if s > best_score or (s == best_score and m_date_cmp < best_date_cmp):
                best, best_score, best_notes = m, s, notes

        # collect prior mail dates (sorted) for UI
        prior_dates: List[date] = sorted([d for d in (m.get("_date") for m in cand) if isinstance(d, date)])
        mail_dates_list = ", ".join(fmt_mm_dd_yy(d) for d in prior_dates) if prior_dates else "None provided"

        if not best:
            continue

        full_mail = " ".join([
            str(best.get("address1", "")).strip(),
            (str(best.get("address2", "")).strip() or ""),
            str(best.get("city", "")).strip(),
            str(best.get("state", "")).strip(),
            str(best.get("zip", "")).strip(),
        ]).replace("  ", " ").strip()

        row = {
            "crm_id": c.get("crm_id", ""),
            "crm_address1_original": c.get("address1", ""),
            "crm_address2_original": (c.get("address2", "") or ""),
            "crm_city": c.get("city", ""),
            "crm_state": c.get("state", ""),
            "crm_zip": str(c.get("zip", "")),
            "crm_job_date": fmt_mm_dd_yy(c.get("_date")),
            "job_value": c.get("job_value", ""),
            "matched_mail_id": best.get("id", ""),
            "matched_mail_full_address": full_mail.replace(" None", "").replace(" none", ""),
            "mail_dates_in_window": mail_dates_list,
            "mail_count_in_window": len(prior_dates),
            "confidence_percent": int(best_score),
            "match_notes": ("; ".join(best_notes) if best_notes else "perfect match"),
        }

        # Optional threshold gate (defaults to 0 so nothing is dropped unless set)
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