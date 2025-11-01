from __future__ import annotations

import csv
from datetime import datetime
from typing import Iterable, List, Dict, Any, Tuple, Optional

from sqlalchemy.engine import Engine
from sqlalchemy import text

from .staging_common import assert_postgres

SCHEMA = "staging"
TABLE = f"{SCHEMA}.mail"

# We INSERT these (the auto PK `id` is not part of inserts)
CANON_COLS = [
    "run_id",       # required
    "source_id",    # CSV id (nullable)
    "address1",
    "address2",
    "city",
    "state",
    "zip",
    "sent_date",
]

# Required AFTER aliasing (by column name presence in header mapping, not per-row values)
REQUIRED: set[str] = {"address1", "city", "state", "zip", "sent_date"}

ALIASES: Dict[str, List[str]] = {
    "id": ["id", "mail_id", "record_id"],  # maps to source_id
    "address1": ["address1", "addr1", "address 1", "address", "street", "line1", "line 1"],
    "address2": ["address2", "addr2", "address 2", "unit", "line2", "apt", "apartment", "suite", "line 2"],
    "city": ["city", "town"],
    "state": ["state", "st"],
    "zip": ["postal_code", "zip", "zipcode", "zip_code", "zip code"],
    "sent_date": ["sent_date", "sent date", "mail_date", "mail date", "date", "sent", "mailed", "mailed_at",
        "mailed at", "date mailed", "mailed date", "mailed_on", "mailed on", "postmark", "postmarked",
        "postmark date", "mailing date", "outbound date"],
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


def ensure_staging_mail(engine: Engine) -> None:
    """
    Create schema/table/indexes used for staging Mail (idempotent).
    Uses:
      - id BIGSERIAL PRIMARY KEY
      - run_id UUID NOT NULL
      - source_id TEXT NULL  (CSV-provided id if present)
    And two partial unique indexes to dedupe with/without source_id.
    """
    assert_postgres(engine)
    with engine.begin() as conn:
        # Schema + base table
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    id           BIGSERIAL PRIMARY KEY,
                    run_id       UUID NOT NULL,
                    source_id    TEXT NULL,
                    address1     TEXT NOT NULL,
                    address2     TEXT NULL,
                    city         TEXT NOT NULL,
                    state        TEXT NOT NULL,
                    zip  TEXT NOT NULL,
                    sent_date    DATE NOT NULL
                )
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
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'mail' AND column_name = 'address1_norm'
                    ) THEN
                        ALTER TABLE {TABLE}
                          ADD COLUMN address1_norm TEXT GENERATED ALWAYS AS (lower(address1)) STORED;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'mail' AND column_name = 'address2_norm'
                    ) THEN
                        ALTER TABLE {TABLE}
                          ADD COLUMN address2_norm TEXT GENERATED ALWAYS AS (lower(coalesce(address2,''))) STORED;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'mail' AND column_name = 'city_norm'
                    ) THEN
                        ALTER TABLE {TABLE}
                          ADD COLUMN city_norm TEXT GENERATED ALWAYS AS (lower(city)) STORED;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'mail' AND column_name = 'state_norm'
                    ) THEN
                        ALTER TABLE {TABLE}
                          ADD COLUMN state_norm TEXT GENERATED ALWAYS AS (lower(state)) STORED;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{SCHEMA}' AND table_name = 'mail' AND column_name = 'zip5'
                    ) THEN
                        ALTER TABLE {TABLE}
                          ADD COLUMN zip5 TEXT GENERATED ALWAYS AS (left(postal_code, 5)) STORED;
                    END IF;
                END$$;
                """
            )
        )

        # Lookup/filter index (include run_id first for selectivity)
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'idx_staging_mail_filters'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        CREATE INDEX idx_staging_mail_filters
                          ON {TABLE} (run_id, address1_norm, city_norm, state_norm, zip5, sent_date);
                    END IF;
                END$$;
                """
            )
        )

        # Drop legacy unique index if it exists
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ux_staging_mail_dedupe_idx'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        DROP INDEX {SCHEMA}.ux_staging_mail_dedupe_idx;
                    END IF;
                END$$;
                """
            )
        )

        # Two partial uniques to dedupe within a run:
        #  - with source_id: include it
        #  - without source_id: dedupe on the address/date only
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ux_staging_mail_dedupe_with_source'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        CREATE UNIQUE INDEX ux_staging_mail_dedupe_with_source
                          ON {TABLE} (run_id, address1_norm, address2_norm, city_norm, state_norm, zip5, sent_date, source_id)
                          WHERE source_id IS NOT NULL;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ux_staging_mail_dedupe_no_source'
                          AND n.nspname = '{SCHEMA}'
                    ) THEN
                        CREATE UNIQUE INDEX ux_staging_mail_dedupe_no_source
                          ON {TABLE} (run_id, address1_norm, address2_norm, city_norm, state_norm, zip5, sent_date)
                          WHERE source_id IS NULL;
                    END IF;
                END$$;
                """
            )
        )


def truncate_mail(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE}"))


def count_mail(engine: Engine) -> int:
    with engine.begin() as conn:
        return int(conn.scalar(text(f"SELECT COUNT(*) FROM {TABLE}")) or 0)


def _canon_header_map(in_headers: Iterable[str]) -> Tuple[Dict[str, str], set[str]]:
    """
    Build a mapping from original CSV headers to canonical names using ALIASES.
    Returns (mapping, missing_required_after_aliasing).
    """
    headers_list = list(in_headers)
    lower = [h.strip().lower() for h in headers_list]
    used: set[str] = set()
    mapping: Dict[str, str] = {}

    for canon, alts in ALIASES.items():
        for a in alts:
            if a in lower:
                src = headers_list[lower.index(a)]
                mapping[src] = "id" if canon == "id" else canon  # internal canon name "id" → will be mapped to source_id later
                used.add("sent_date" if canon == "sent_date" else canon)
                break

    missing = REQUIRED - used
    return mapping, missing


def copy_mail_csv_path(
    engine: Engine,
    csv_path: str,
    run_id: str,
    *,
    truncate: bool = False,
    mapping: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Ingest CSV directly into staging.mail with dedupe via partial unique indexes.
    No temp tables/files. In-memory aliasing + batched INSERTs.

    - `run_id` is required and stored.
    - CSV `id` (if present) is stored as `source_id` (nullable).
    - Rows missing a parseable `sent_date` are skipped to avoid NOT NULL errors.
    """
    assert_postgres(engine)
    ensure_staging_mail(engine)
    if truncate:
        truncate_mail(engine)

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

    # Build aliasing from header (unless explicit mapping is provided)
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        if mapping is None:
            header_map, missing = _canon_header_map(headers)
        else:
            # normalize provided mapping keys to our canon names where possible
            header_map = {src: dst for src, dst in mapping.items()}
            # for REQUIRED, ensure a target is present at least
            provided_targets = set(header_map.values())
            missing = REQUIRED - provided_targets

        if missing:
            raise RuntimeError(f"Missing required columns after aliasing: {', '.join(sorted(missing))}")

        idx_by = {name: i for i, name in enumerate(headers)}

        def get_val(row: List[str], canon: str) -> str:
            # Canon "id" (from ALIASES) is really CSV id → we store in source_id
            target_canon = canon
            src = next((orig for orig, c in header_map.items() if c == target_canon), None)
            if src is None:
                return ""
            i = idx_by.get(src)
            return (row[i] if i is not None and i < len(row) else "").strip()

        for row in reader:
            attempted += 1

            sent_date_iso = _parse_date_to_iso(get_val(row, "sent_date"))
            if not sent_date_iso:
                # skip rows without a parseable date to satisfy NOT NULL constraint
                continue

            source_id_val = get_val(row, "id") or None  # nullable

            batch.append(
                {
                    "run_id": run_id,
                    "source_id": source_id_val,
                    "address1": get_val(row, "address1"),
                    "address2": (get_val(row, "address2") or None),
                    "city": get_val(row, "city"),
                    "state": get_val(row, "state"),
                    "zip": get_val(row, "postal_code"),
                    "sent_date": sent_date_iso,
                }
            )

            if len(batch) >= BATCH:
                flush_batch(batch)

    flush_batch(batch)
    return attempted