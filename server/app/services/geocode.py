# app/services/geocode.py
from __future__ import annotations
from typing import Iterable, Iterator, Tuple, Dict, Any
from datetime import date
import urllib.parse
import requests
from flask import current_app

def _mapbox(query: str, token: str) -> Tuple[float, float] | None:
    """Return (lat, lon) for a single-line address via Mapbox, or None."""
    if not token:
        return None
    try:
        q = urllib.parse.quote(query)
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{q}.json?limit=1&access_token={token}"
        r = requests.get(url, timeout=7)
        if r.status_code != 200:
            return None
        feats = r.json().get("features", [])
        if not feats:
            return None
        lon, lat = feats[0]["center"]
        return (lat, lon)
    except Exception:
        return None

def geocode_addresses_plain(
    addresses: Iterable[tuple[str, str, int, int | None, date | None]]
) -> Iterator[Dict[str, Any]]:
    """
    Input: iterable of (source, address, user_id, run_id, event_date)
    Yields dicts suitable for writing to the map cache (no ORM objects).
    """
    token = current_app.config.get("MAPBOX_TOKEN", "").strip()
    for source, addr, user_id, run_id, dt in addresses:
        if not addr:
            continue
        gl = _mapbox(addr, token)
        if not gl:
            continue
        lat, lon = gl
        yield {
            "lat": lat,
            "lon": lon,
            "label": (source or "match").capitalize(),
            "address": addr,
            "source": source or "match",
            "run_id": run_id,
            "event_date": dt.isoformat() if dt else None,
            # optional: include user_id if you want it in properties
            "user_id": user_id,
        }