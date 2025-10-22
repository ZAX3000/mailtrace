# app/services/map_cache.py
from __future__ import annotations

from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

from flask import current_app

_CACHE_FILENAME = "excluded_latest.json"


class GeoPoint(TypedDict, total=False):
    lat: float
    lon: float
    label: str
    address: str
    kind: str
    run_id: str | None
    event_date: str | None
    user_id: str | None


def _cache_path() -> Path:
    """
    Resolve the cache file path inside the app's static directory.
    """
    static_root = current_app.static_folder or str(Path(current_app.root_path) / "static")
    return Path(static_root) / _CACHE_FILENAME


def _empty_geojson() -> Dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def _read_cache_json() -> Dict[str, Any]:
    """
    Read the GeoJSON cache; on any file/parse issue, return an empty FeatureCollection.
    """
    path = _cache_path()
    if not path.exists():
        return _empty_geojson()
    try:
        text = path.read_text(encoding="utf-8")
        import json

        obj = json.loads(text)
        # Minimal schema guard
        if not isinstance(obj, dict) or "features" not in obj:
            return _empty_geojson()
        if not isinstance(obj.get("features"), list):
            obj["features"] = []
        obj["type"] = "FeatureCollection"
        return obj
    except (OSError, JSONDecodeError):
        return _empty_geojson()


def _write_cache_json(payload: Dict[str, Any]) -> str:
    """
    Write GeoJSON payload atomically where possible.
    Returns the cache path as string.
    """
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    import json
    tmp = path.with_suffix(path.suffix + ".tmp")

    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    try:
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(path)  # atomic on POSIX; safe fallback on Windows
    except OSError:
        # Fall back to direct write if atomic replace fails
        path.write_text(data, encoding="utf-8")
    return str(path)


def _coerce_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def append_points(points: List[GeoPoint], max_points: int = 5000) -> str:
    """
    Append geocoded points and trim to last `max_points`.
    Returns the cache path.
    """
    cache = _read_cache_json()
    feats = list(cache.get("features") or [])
    out_feats: List[Dict[str, Any]] = list(feats)

    for p in points:
        lat = _coerce_float(p.get("lat"))
        lon = _coerce_float(p.get("lon"))
        if lat is None or lon is None:
            continue
        props = {
            "label": p.get("label", "") or "",
            "address": p.get("address", "") or "",
            "kind": p.get("kind", "") or "",
            "run_id": p.get("run_id"),
            "event_date": p.get("event_date"),
            "user_id": p.get("user_id"),
        }
        out_feats.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )

    if len(out_feats) > max_points:
        out_feats = out_feats[-max_points:]

    cache["type"] = "FeatureCollection"
    cache["features"] = out_feats
    return _write_cache_json(cache)


def cached_payload_if_exists() -> bytes | None:
    """
    Return the raw JSON bytes if the cache exists; otherwise None.
    """
    path = _cache_path()
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except OSError:
        return None


def build_map_cache(limit: int | None = None) -> str:
    """
    Rewrites the cache file; if `limit` is provided, trims features to the last `limit`.
    Returns the cache path.
    """
    cache = _read_cache_json()
    feats = list(cache.get("features") or [])
    if isinstance(limit, int) and limit >= 0 and len(feats) > limit:
        feats = feats[-limit:]
    cache["type"] = "FeatureCollection"
    cache["features"] = feats
    return _write_cache_json(cache)