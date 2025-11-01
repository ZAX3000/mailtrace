# app/dao/staging_crm.py
from __future__ import annotations

import csv
from datetime import datetime
from typing import Iterable, List, Dict, Any, Tuple, Optional

from sqlalchemy.engine import Engine
from sqlalchemy import text

from .staging_common import assert_postgres

SCHEMA = "staging"
TABLE = f"{SCHEMA}.crm"

# Insert columns, in order
CANON_COLS = [
    "run_id",      # NEW (NOT NULL)
    "crm_id",      # optional
    "source_id",   # NEW (optional)
    "address1",
    "address2",
    "city",
    "state",
    "zip",
    "job_date",
    "job_value",
]

# Required AFTER aliasing (crm_id/source_id are optional)
REQUIRED: set[str] = {"address1", "city", "state", "zip", "job_date"}

ALIASES: Dict[str, List[str]] = {
    "crm_id": ["crm_id", "lead_id", "job_id", "id"],  # if CSV has "id", we treat as crm_id by default
    "source_id": ["source_id", "source id", "external_id", "ext_id"],  # keep distinct from "id" to avoid collision
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "job_date": ["job_date", "date", "created_at", "job date"],
    "job_value": ["job_value", "amount", "value", "revenue", "job value"],
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%m/%d/%y",
    "%d-%m-%y",
]


def _parse_date_to_iso(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    z = s.replace("/", "-")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(z, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def ensure_staging_crm(engine: Engine) -> None:
    """Create schema/table/indexes used for staging CRM (idempotent)."""
    assert_postgres(engine)
    with engine.begin() as conn:
        # Schema + base table
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    run_id      UUID NOT NULL,
                    crm_id      TEXT NULL,
                    source_id   TEXT NULL,
                    address1    TEXT NOT NULL,
                    address2    TEXT NULL,
                    city        TEXT NOT NULL,
                    state       TEXT NOT NULL,
                    zip TEXT NOT NULL,
                    job_date    DATE NOT NULL,
                    job_value   NUMERIC(12,2) NULL
                )
                """
            )
        )

        # Ensure columns exist (robust on dev if table predated new cols)
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'run_id'
                    ) THEN
                        ALTER TABLE {TABLE} ADD COLUMN run_id UUID NOT NULL DEFAULT gen_random_uuid();
                        ALTER TABLE {TABLE} ALTER COLUMN run_id DROP DEFAULT;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'crm' AND column_name = 'source_id'
                    ) THEN
                        ALTER TABLE {TABLE} ADD COLUMN source_id TEXT NULL;
                    END IF;
                END$$;
                """
            )
        )

        # Generated (stored) normalized columns
        conn.execute(
            text(
                f"""
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
                """
            )
        )

        # Lookup/filter index
        conn.execute(
            text(
                f"""
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
                """
            )
        )

        # PARTIAL UNIQUE INDEXES for dedupe (scoped per run)
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ux_staging_crm_dedupe_with_source'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        CREATE UNIQUE INDEX ux_staging_crm_dedupe_with_source
                          ON {TABLE} (run_id, address1_norm, address2_norm, city_norm, state_norm, zip5, job_date, source_id)
                          WHERE source_id IS NOT NULL;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ux_staging_crm_dedupe_no_source'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        CREATE UNIQUE INDEX ux_staging_crm_dedupe_no_source
                          ON {TABLE} (run_id, address1_norm, address2_norm, city_norm, state_norm, zip5, job_date)
                          WHERE source_id IS NULL;
                    END IF;
                END$$;
                """
            )
        )


def truncate_crm(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE}"))


def count_crm(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.scalar(text(f"SELECT COUNT(*) FROM {TABLE}")) or 0)


def _canon_header_map(in_headers: Iterable[str]) -> Tuple[Dict[str, str], set[str]]:
    """
    Build a mapping from original CSV headers to canonical names using ALIASES.
    Returns (mapping_original_to_canonical, missing_required_after_aliasing).
    """
    headers_list = list(in_headers)  # make indexable
    lower = [h.strip().lower() for h in headers_list]
    used: set[str] = set()
    mapping: Dict[str, str] = {}
    for canon, alts in ALIASES.items():
        for a in alts:
            if a in lower:
                src = headers_list[lower.index(a)]
                mapping[src] = canon
                used.add(canon)
                break
    missing = REQUIRED - used
    return mapping, missing


def _resolve_canonical_to_source(headers: List[str], mapping_arg: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build canonical_name -> original_header mapping.
    Priority:
      1) mapping_arg (canonical->header)
      2) alias inference from ALIASES
    """
    headers_lower = [h.strip().lower() for h in headers]

    if mapping_arg:
        out: Dict[str, str] = {}
        for canon, src in mapping_arg.items():
            if not isinstance(src, str) or not src:
                continue
            # accept if present in file headers
            if src in headers:
                out[canon] = src
                continue
            # try case-insensitive match
            if src.lower() in headers_lower:
                out[canon] = headers[headers_lower.index(src.lower())]
        # we don't enforce REQUIRED here; we'll validate later
        if out:
            return out

    # fallback: alias inference (canonical <- original)
    original_to_canon, _ = _canon_header_map(headers)
    # invert to canon -> orig
    canon_to_orig: Dict[str, str] = {}
    for orig, canon in original_to_canon.items():
        # keep first seen
        canon_to_orig.setdefault(canon, orig)
    return canon_to_orig


def copy_crm_csv_path(
    engine: Engine,
    csv_path: str,
    run_id: str,
    *,
    truncate: bool = False,
    mapping: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Ingest CSV directly into staging.crm with dedupe via partial unique indexes.
    No temp tables/files. In-memory aliasing + batched INSERTs.

    `mapping` (optional): dict of canonical_name -> original_header
       canonical in: crm_id, source_id, address1, address2, city, state, zip, job_date, job_value
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
        sql = text(
            f"""
            INSERT INTO {TABLE} ({", ".join(CANON_COLS)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
            """
        )
        with engine.begin() as conn:
            conn.execute(sql, b)
        b.clear()

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        if not headers:
            return 0

        # Build canon->source mapping
        canon_to_source = _resolve_canonical_to_source(headers, mapping)
        # Check REQUIRED present after mapping/aliasing
        missing = {r for r in REQUIRED if r not in canon_to_source}
        if missing:
            raise RuntimeError(f"Missing required columns after aliasing: {', '.join(sorted(missing))}")

        # Fast header index
        idx_by = {name: i for i, name in enumerate(headers)}

        def get_val(row: List[str], canon: str) -> str:
            src = canon_to_source.get(canon)
            if not src:
                return ""
            i = idx_by.get(src)
            return (row[i] if i is not None and i < len(row) else "").strip()

        for row in reader:
            attempted += 1

            # Parse date + coerce numeric
            job_date_iso = _parse_date_to_iso(get_val(row, "job_date")) or None

            raw_val = get_val(row, "job_value")
            job_value_val: float | None = None
            if raw_val:
                try:
                    job_value_val = float(raw_val.replace(",", ""))
                except ValueError:
                    job_value_val = None

            batch.append(
                {
                    "run_id": run_id,
                    "crm_id": get_val(row, "crm_id") or None,
                    "source_id": get_val(row, "source_id") or None,
                    "address1": get_val(row, "address1"),
                    "address2": get_val(row, "address2") or None,
                    "city": get_val(row, "city"),
                    "state": get_val(row, "state"),
                    "zip": get_val(row, "postal_code"),
                    "job_date": job_date_iso,
                    "job_value": job_value_val,
                }
            )

            if len(batch) >= BATCH:
                flush_batch(batch)

    flush_batch(batch)
    return attempted