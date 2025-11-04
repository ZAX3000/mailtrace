# app/dao/result_dao.py
from __future__ import annotations

from typing import Dict, Any, List, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func

from app.extensions import db
from app.models import RunKPI, RunSeries, RunTopCity, RunTopZip, Run


# ---------- write ----------
def save_all(run_id: str, user_id: str, payload: Dict[str, Any]) -> None:
    k = payload.get("kpis", {}) or {}
    graph = payload.get("graph", {}) or {}
    tops_c = payload.get("top_cities", []) or []
    tops_z = payload.get("top_zips", []) or []

    # KPI upsert
    stmt = insert(RunKPI).values(
        run_id=run_id,
        user_id=user_id,
        total_mail=int(k.get("total_mail", 0) or 0),
        unique_mail_addresses=int(k.get("unique_mail_addresses", 0) or 0),
        total_jobs=int(k.get("total_jobs", 0) or 0),
        matches=int(k.get("matches", 0) or 0),
        match_rate=k.get("match_rate", 0) or 0,
        match_revenue=k.get("match_revenue", 0) or 0,
        revenue_per_mailer=k.get("revenue_per_mailer", 0) or 0,
        avg_ticket_per_match=k.get("avg_ticket_per_match", 0) or 0,
        median_days_to_convert=int(k.get("median_days_to_convert", 0) or 0),
        # Optional ranges if/when you add them to payload
        first_job_date=None,
        last_job_date=None,
    )

    on_conflict = stmt.on_conflict_do_update(
        index_elements=[RunKPI.run_id],
        set_={
            "user_id": user_id,
            "total_mail": stmt.excluded.total_mail,
            "unique_mail_addresses": stmt.excluded.unique_mail_addresses,
            "total_jobs": stmt.excluded.total_jobs,
            "matches": stmt.excluded.matches,
            "match_rate": stmt.excluded.match_rate,
            "match_revenue": stmt.excluded.match_revenue,
            "revenue_per_mailer": stmt.excluded.revenue_per_mailer,
            "avg_ticket_per_match": stmt.excluded.avg_ticket_per_match,
            "median_days_to_convert": stmt.excluded.median_days_to_convert,
            "updated_at": func.now(),
        },
    )

    # one atomic transaction
    with db.session.begin():
        db.session.execute(on_conflict)

        # Replace series rows for this run
        db.session.query(RunSeries).filter(RunSeries.run_id == run_id).delete(synchronize_session=False)

        months: List[str] = graph.get("months", []) or []
        for series_name, arr in (
            ("mailers", graph.get("mailers", [])),
            ("jobs",    graph.get("jobs", [])),
            ("matches", graph.get("matches", [])),
        ):
            for ym, val in zip(months, arr or []):
                db.session.add(
                    RunSeries(run_id=run_id, series=series_name, ym=str(ym), value=int(val or 0))
                )

        # Replace tops (cities)
        db.session.query(RunTopCity).filter(RunTopCity.run_id == run_id).delete(synchronize_session=False)
        for item in tops_c:
            db.session.add(
                RunTopCity(
                    run_id=run_id,
                    city=str(item.get("city", "") or ""),
                    matches=int(item.get("matches", 0) or 0),
                    match_rate=item.get("match_rate", 0) or 0,
                )
            )

        # Replace tops (zips)
        db.session.query(RunTopZip).filter(RunTopZip.run_id == run_id).delete(synchronize_session=False)
        for item in tops_z:
            db.session.add(
                RunTopZip(
                    run_id=run_id,
                    zip5=str(item.get("zip", "") or "")[:5],
                    matches=int(item.get("matches", 0) or 0),
                )
            )


# ---------- read (for dumb FE) ----------
def get_kpis(run_id: str) -> Dict[str, Any]:
    row: RunKPI | None = db.session.get(RunKPI, run_id)
    if not row:
        return {}
    return {
        "total_mail": row.total_mail,
        "unique_mail_addresses": row.unique_mail_addresses,
        "total_jobs": row.total_jobs,
        "matches": row.matches,
        "match_rate": float(row.match_rate or 0),
        "match_revenue": float(row.match_revenue or 0),
        "revenue_per_mailer": float(row.revenue_per_mailer or 0),
        "avg_ticket_per_match": float(row.avg_ticket_per_match or 0),
        "median_days_to_convert": row.median_days_to_convert,
    }


def get_series(run_id: str) -> Dict[str, List[Dict[str, int]]]:
    rows: List[RunSeries] = (
        db.session.query(RunSeries)
        .filter(RunSeries.run_id == run_id)
        .order_by(RunSeries.series, RunSeries.ym)
        .all()
    )
    out: Dict[str, List[Dict[str, int]]] = {}
    for r in rows:
        out.setdefault(r.series, []).append({"ym": r.ym, "value": r.value})
    return out


def get_top_cities(run_id: str) -> List[Dict[str, Any]]:
    rows = (
        db.session.query(RunTopCity)
        .filter(RunTopCity.run_id == run_id)
        .order_by(RunTopCity.matches.desc())
        .all()
    )
    return [{"city": r.city, "matches": r.matches, "match_rate": float(r.match_rate or 0)} for r in rows]


def get_top_zips(run_id: str) -> List[Dict[str, Any]]:
    rows = (
        db.session.query(RunTopZip)
        .filter(RunTopZip.run_id == run_id)
        .order_by(RunTopZip.matches.desc())
        .all()
    )
    return [{"zip": r.zip5, "matches": r.matches} for r in rows]


# ---------- helpers + compatibility ----------
def _resolve_user_id_for_run(run_id: str) -> Optional[str]:
    r = db.session.get(Run, run_id)
    return str(r.user_id) if r else None


def get_full_result(run_id: str) -> Dict[str, Any]:
    """
    Assemble a dumb-FE payload directly from run_kpis/run_series/run_top_*.
    Shape mirrors what summary/build_* produced.
    """
    kpis = get_kpis(run_id)
    series = get_series(run_id)
    top_cities = get_top_cities(run_id)
    top_zips = get_top_zips(run_id)

    # Rebuild the graph parallel arrays from stored points
    months = sorted({pt["ym"] for arr in series.values() for pt in (arr or [])})

    def pick(name: str) -> List[int]:
        points = {pt["ym"]: int(pt["value"]) for pt in series.get(name, [])}
        return [points.get(m, 0) for m in months]

    graph = {
        "months": months,
        "mailers": pick("mailers"),
        "jobs":    pick("jobs"),
        "matches": pick("matches"),
        "yoy": {  # optional; keep interface stable
            "mailers": {"months": [], "current": [], "prev": []},
            "jobs":    {"months": [], "current": [], "prev": []},
            "matches": {"months": [], "current": [], "prev": []},
        },
    }

    return {
        "kpis": kpis,
        "graph": graph,
        "top_cities": top_cities,
        "top_zips": top_zips,
        "run_id": run_id,
    }


def save_full_result(run_id: str, payload: Dict[str, Any]) -> None:
    """
    Compatibility: pipeline calls this without user_id. We resolve it and delegate.
    """
    user_id = _resolve_user_id_for_run(run_id)
    save_all(run_id, user_id or "", payload)