# app/utils/normalize.py
from __future__ import annotations

import re
import hashlib
from datetime import date
from typing import Any, Dict, Optional
from rapidfuzz import fuzz

# --- Public constants (exactly as in matching.py) --------------------------

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

# --- Internal helpers ------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_NON_WORD_KEEP_HASH_RE = re.compile(r"[^\w#\s]")  # keep '#' for units
_ZIP_DIGITS_ONLY = re.compile(r"\D+")

def zip5(z: Optional[str]) -> str:
    """
    Return the first 5 numeric digits of a ZIP/ZIP+4.
    '02139-4307' -> '02139'
    ' 85004 1234 ' -> '85004'
    """
    s = "" if z is None else str(z).strip()
    if not s:
        return ""
    digits = _ZIP_DIGITS_ONLY.sub("", s)
    return digits[:5]

def _squash_ws(s: str) -> str:
    return _WS_RE.sub(" ", s).strip()

def _norm_token(tok: str) -> str:
    t = tok.lower().strip(".,")
    if t in STREET_TYPES:
        return STREET_TYPES[t]
    if t in DIRECTIONALS:
        return DIRECTIONALS[t]
    return t

def _mt_clean(_s: Any) -> str:
    return "" if _s is None else str(_s).strip()

# --- Public API (moved from matching.py) -----------------------------------

def normalize_address1(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace("-", " ")
    s = _NON_WORD_KEEP_HASH_RE.sub(" ", s)
    parts = [_norm_token(p) for p in s.lower().split() if p.strip()]
    return _squash_ws(" ".join(parts))

def block_key(addr1: str) -> str:
    if not isinstance(addr1, str):
        return ""
    toks = [t for t in _squash_ws(addr1).split() if t]
    if not toks:
        return ""
    first = toks[0]
    second_initial = toks[1][0] if len(toks) > 1 else ""
    return f"{first}|{second_initial}".lower()

def tokens(s: str) -> list[str]:
    return [t for t in normalize_address1(s).split() if t]

def street_type_of(tok_list: list[str]) -> Optional[str]:
    if not tok_list:
        return None
    last = tok_list[-1]
    return last if last in STREET_TYPES.values() else None

def directional_in(tok_list: list[str]) -> Optional[str]:
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

def build_full_address(
    addr1: Optional[str],
    city: Optional[str],
    state: Optional[str],
    z: Optional[str],
    addr2: Optional[str] = None,
) -> str:
    """
    Canonical full_address used across Mail & CRM.
    - normalizes addr1 tokens (directionals, street types)
    - squashes whitespace
    - enforces ZIP-5 for stability
    """
    a1 = normalize_address1(addr1 or "")
    parts = [
        a1,
        str(addr2 or "").strip(),
        str(city  or "").strip(),
        str(state or "").strip(),
        zip5(z or ""),              # <-- use ZIP-5 here
    ]
    return _squash_ws(" ".join(parts)).lower()       # lower for stable hashing/compare


def build_mail_key(
    source_id: Optional[str],
    full_address: Optional[str],
    sent_date: Optional[date],
) -> Optional[str]:
    """
    MAIL identity:
    - If source_id present → use it (canonical).
    - Else if full_address & sent_date → stable hash mk_<16-hex>.
    - Else → None (row should be skipped upstream).
    """
    sid = (source_id or "").strip()
    if sid:
        return sid
    if full_address and sent_date:
        raw = f"{str(full_address).strip().lower()}|{sent_date.isoformat()}"
        return "mk_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return None


def build_job_index(
    source_id: Optional[str],
    full_address: Optional[str],
    job_date: Optional[date]
) -> Optional[str]:
    """
    Canonical job key (user-scoped UNIQUE):
      - If source_id present -> use it verbatim (authoritative)
      - Else require BOTH full_address AND job_date (AND-semantics)
      - Key is a short stable hash of "<full_address>|<YYYY-MM-DD>"
    """
    sid = (source_id or "").strip()
    if sid:
        return sid

    if full_address and job_date:
        base = f"{str(full_address).strip().lower()}|{job_date.isoformat()}"
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
        return f"jid_{digest}"

    return None