# app/dao/staging_crm.py
from __future__ import annotations

import csv
from datetime import datetime
from typing import Tuple, Iterable, List, Dict, Any

from sqlalchemy.engine import Engine
from sqlalchemy import text

from .staging_common import assert_postgres

SCHEMA = "staging"
TABLE = f"{SCHEMA}.crm"

CANON_COLS = ["crm_id","address1","address2","city","state","postal_code","job_date","job_value"]
# crm_id & job_value are optional; these are the required AFTER aliasing
REQUIRED = {"address1","city","state","postal_code","job_date"}

ALIASES = {
    "crm_id": ["crm_id","id","lead_id","job_id"],
    "address1": ["address1","addr1","address 1","address","street","line1","line 1"],
    "address2": ["address2","addr2","address 2","unit","line2","apt","apartment","suite","line 2"],
    "city": ["city","town"],
    "state": ["state","st"],
    "postal_code": ["postal_code","zip","zipcode","zip_code","zip code"],
    "job_date": ["job_date","date","created_at","job date"],
    "job_value": ["job_value","amount","value","revenue","job value"],
}

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y"
]


def _parse_date_to_iso(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    z = s.replace("/", "-")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(z, fmt).date().isoformat()
        except Exception:
            pass
    return ""


def ensure_staging_crm(engine: Engine) -> None:
    """Create schema/table/indexes used for staging CRM (idempotent)."""
    assert_postgres(engine)
    with engine.begin() as conn:
        # Schema + base table
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                crm_id      TEXT NULL,
                address1    TEXT NOT NULL,
                address2    TEXT NULL,
                city        TEXT NOT NULL,
                state       TEXT NOT NULL,
                postal_code TEXT NOT NULL,
                job_date    DATE NOT NULL,
                job_value   NUMERIC(12,2) NULL
            )
        """))

        # Generated (stored) normalized columns
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'address1_norm'
                ) THEN
                    ALTER TABLE {TABLE}
                      ADD COLUMN address1_norm TEXT GENERATED ALWAYS AS (lower(address1)) STORED;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'address2_norm'
                ) THEN
                    ALTER TABLE {TABLE}
                      ADD COLUMN address2_norm TEXT GENERATED ALWAYS AS (lower(coalesce(address2,''))) STORED;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'city_norm'
                ) THEN
                    ALTER TABLE {TABLE}
                      ADD COLUMN city_norm TEXT GENERATED ALWAYS AS (lower(city)) STORED;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'state_norm'
                ) THEN
                    ALTER TABLE {TABLE}
                      ADD COLUMN state_norm TEXT GENERATED ALWAYS AS (lower(state)) STORED;
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'zip5'
                ) THEN
                    ALTER TABLE {TABLE}
                      ADD COLUMN zip5 TEXT GENERATED ALWAYS AS (left(postal_code, 5)) STORED;
                END IF;
            END$$;
        """))

        # Lookup/filter index
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'idx_staging_crm_filters'
                      AND n.nspname = '{SCHEMA}'
                ) THEN
                    CREATE INDEX idx_staging_crm_filters
                      ON {TABLE} (address1_norm, city_norm, state_norm, zip5, job_date);
                END IF;
            END$$;
        """))

        # UNIQUE INDEX for dedupe on normalized fields + date + crm_id
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'ux_staging_crm_dedupe_idx'
                      AND n.nspname = '{SCHEMA}'
                ) THEN
                    CREATE UNIQUE INDEX ux_staging_crm_dedupe_idx
                      ON {TABLE} (address1_norm, address2_norm, city_norm, state_norm, zip5, job_date, crm_id);
                END IF;
            END$$;
        """))


def truncate_crm(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE}"))


def count_crm(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.scalar(text(f"SELECT COUNT(*) FROM {TABLE}")) or 0)


def _canon_header_map(in_headers: Iterable[str]) -> Tuple[Dict[str, str], set]:
    lower = [h.strip().lower() for h in in_headers]
    used = set()
    mapping: Dict[str, str] = {}
    for canon, alts in ALIASES.items():
        for a in alts:
            if a in lower:
                mapping[in_headers[lower.index(a)]] = canon
                used.add(canon)
                break
    missing = REQUIRED - used
    return mapping, missing


def copy_crm_csv_path(engine: Engine, csv_path: str, *, truncate: bool = False) -> int:
    """
    Ingest CSV directly into staging.crm with dedupe (ON CONFLICT DO NOTHING).
    No temp tables/files. In-memory aliasing + batched INSERTs.
    """
    assert_postgres(engine)
    ensure_staging_crm(engine)
    if truncate:
        truncate_crm(engine)

    attempted = 0
    batch: List[Dict[str, Any]] = []
    BATCH = 1000

    def flush_batch(b: List[Dict[str, Any]]) -> None:
        if not b:
            return
        placeholders = ", ".join(f":{c}" for c in CANON_COLS)
        sql = text(f"""
            INSERT INTO {TABLE} ({", ".join(CANON_COLS)})
            VALUES ({placeholders})
            ON CONFLICT (address1_norm, address2_norm, city_norm, state_norm, zip5, job_date, crm_id) DO NOTHING
        """)
        with engine.begin() as conn:
            conn.execute(sql, b)
        b.clear()

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        # delete debug
        print("DEBUG CRM HEADERS RAW:", repr(headers))
        from . import staging_crm as _sc
        print("DEBUG CRM ALIASES[job_date]:", getattr(_sc, "ALIASES", {}).get("job_date"))
        mapping, missing = _canon_header_map(headers)
        print("DEBUG CRM MAPPING:", mapping, "MISSING:", missing)
        # delete debug
        mapping, missing = _canon_header_map(headers)
        if missing:
            raise RuntimeError(f"Missing required columns after aliasing: {', '.join(sorted(missing))}")

        idx_by = {name: i for i, name in enumerate(headers)}

        def get_val(row: List[str], canon: str) -> str:
            src = next((orig for orig, c in mapping.items() if c == canon), None)
            if src is None:
                return ""
            i = idx_by.get(src)
            return (row[i] if i is not None and i < len(row) else "").strip()

        for row in reader:
            attempted += 1

            # Parse date + coerce numeric
            job_date_iso = _parse_date_to_iso(get_val(row, "job_date")) or None
            raw_val = get_val(row, "job_value")
            job_value_val = None
            if raw_val:
                try:
                    job_value_val = float(raw_val.replace(",", ""))
                except Exception:
                    job_value_val = None

            batch.append({
                "crm_id": get_val(row, "crm_id"),
                "address1": get_val(row, "address1"),
                "address2": get_val(row, "address2"),
                "city": get_val(row, "city"),
                "state": get_val(row, "state"),
                "postal_code": get_val(row, "postal_code"),
                "job_date": job_date_iso,
                "job_value": job_value_val,
            })

            if len(batch) >= BATCH:
                flush_batch(batch)

    flush_batch(batch)
    return attempted