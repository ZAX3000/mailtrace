# app/services/map_cache.py
from __future__ import annotations
import os, json, time
from typing import Dict, Any, List
from flask import current_app

_CACHE_FILENAME = "excluded_latest.json"

def _cache_path() -> str:
    root = current_app.static_folder or os.path.join(current_app.root_path, "static")
    return os.path.join(root, _CACHE_FILENAME)

def _read_cache_json() -> Dict[str, Any]:
    path = _cache_path()
    if not os.path.exists(path):
        return {"type": "FeatureCollection", "features": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # corrupt or unreadable: start fresh
        return {"type": "FeatureCollection", "features": []}

def _write_cache_json(payload: Dict[str, Any]) -> str:
    path = _cache_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.utime(path, (time.time(), time.time()))
    return path

def append_points(points: List[Dict[str, Any]], max_points: int = 5000) -> str:
    """
    Append geocoded points (lat/lon dicts) to the cached FeatureCollection
    and trim to last `max_points`.
    """
    cache = _read_cache_json()
    feats = cache.get("features", [])

    for p in points:
        lat, lon = p.get("lat"), p.get("lon")
        if lat is None or lon is None:
            continue
        props = {
            "label": p.get("label") or "",
            "address": p.get("address") or "",
            "kind": p.get("kind") or "",
            "run_id": p.get("run_id"),
            "event_date": p.get("event_date"),
            "user_id": p.get("user_id"),
        }
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    # Trim oldest if too many
    if len(feats) > max_points:
        feats = feats[-max_points:]

    cache["type"] = "FeatureCollection"
    cache["features"] = feats
    return _write_cache_json(cache)

def cached_payload_if_exists() -> bytes | None:
    path = _cache_path()
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()

def build_map_cache() -> str:
    """
    DB-less version: ensure cache file exists.
    If missing, write an empty FeatureCollection so /map/data always works.
    """
    cache = _read_cache_json()
    return _write_cache_json(cache)