from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Iterable, Mapping, List

from sqlalchemy import text
from sqlalchemy.engine import Engine


# -------------------------
# Basic guards / DDL helper
# -------------------------

def assert_postgres(engine: Engine) -> None:
    name = engine.url.get_backend_name()
    if not name.startswith("postgresql"):
        raise RuntimeError(f"Expected Postgres engine, got {name!r}")


def ensure_schema_and_table(
    engine: Engine,
    schema_sql: str,
    table_sql: str,
    indexes: List[str] | None = None,
) -> None:
    assert_postgres(engine)
    with engine.begin() as conn:
        conn.execute(text(schema_sql))
        conn.execute(text(table_sql))
        for idx_sql in (indexes or []):
            conn.execute(text(idx_sql))


# -------------------------
# COPY / fallback
# -------------------------

def _pg_copy(engine: Engine, copy_sql: str, payload: io.TextIOBase | io.BytesIO | io.StringIO) -> None:
    """
    COPY ... FROM STDIN when the DBAPI cursor supports it (psycopg2/psycopg3),
    otherwise fall back to an executemany bulk INSERT (dev-friendly).

    We normalize the payload to a text buffer so psycopg2/3 copy_expert() is happy.
    """
    assert_postgres(engine)

    # Normalize to a text buffer for both branches
    def _as_text_buffer(buf: io.TextIOBase | io.BytesIO | io.StringIO) -> io.StringIO | io.TextIOBase:
        if isinstance(buf, io.BytesIO):
            buf.seek(0)
            return io.StringIO(buf.getvalue().decode("utf-8"))
        # Text-like already
        buf.seek(0)
        return buf

    text_buf = _as_text_buffer(payload)

    # Try native COPY ... FROM STDIN via driver cursor
    with engine.begin() as conn:
        raw = conn.connection  # DBAPI connection (psycopg2/psycopg3)
        cur = raw.cursor()
        if hasattr(cur, "copy_expert"):
            # psycopg2/3 both support copy_expert(sql, file_like)
            cur.copy_expert(copy_sql, text_buf)
            return

    # ---- Fallback path: parse CSV and executemany insert ----
    # Extract table + column list from "COPY schema.table (a,b,c) FROM STDIN ..."
    m = re.search(r"COPY\s+([\w.]+)\s*\(([^)]+)\)", copy_sql, re.IGNORECASE)
    if not m:
        raise RuntimeError("Fallback COPY parser couldn't find target table/columns.")
    full_table = m.group(1).strip()
    col_list = [c.strip() for c in m.group(2).split(",")]

    # Read CSV rows to dicts
    text_buf.seek(0)
    rdr = csv.DictReader(text_buf)
    rows: List[dict[str, str | None]] = []
    for r in rdr:
        rows.append({c: r.get(c, None) for c in col_list})

    if not rows:
        return

    # Build INSERT ... VALUES bind list
    cols_sql = ", ".join(col_list)
    binds_sql = ", ".join([f":{c}" for c in col_list])
    ins_sql = f"INSERT INTO {full_table} ({cols_sql}) VALUES ({binds_sql})"

    with engine.begin() as conn:
        conn.execute(text(ins_sql), rows)


# -------------------------
# CSV remap helper
# -------------------------

def _remap_csv_to_buffer(
    file_obj: io.TextIOBase | io.StringIO,
    required: Iterable[str],
    mapping: Mapping[str, Iterable[str]],
    date_cols: Iterable[str] | None = None,
) -> io.StringIO:
    """
    Read a user CSV (file_obj), rename/alias columns to a canonical header,
    coerce date columns to YYYY-MM-DD, and emit a CSV (StringIO) with HEADER.
    Raises if any required fields are missing after aliasing.
    """
    reader = csv.reader(file_obj)
    in_headers = next(reader, [])
    lower = [h.strip().lower() for h in in_headers]
    idx_by_name = {name: i for i, name in enumerate(in_headers)}

    # Build alias map: original_header -> canonical_name
    alias_map: dict[str, str] = {}
    present: set[str] = set()
    for canon, alts in mapping.items():
        found_src = None
        for a in alts:
            if a in lower:
                src = in_headers[lower.index(a)]
                found_src = src
                break
        if found_src:
            alias_map[found_src] = canon
            present.add(canon)

    missing = set(required) - present
    if missing:
        raise RuntimeError(f"Missing required columns after aliasing: {', '.join(sorted(missing))}")

    # Canonical output header: required first (caller-defined order), then optional mapped
    canon_cols = list(required)
    for k in mapping.keys():
        if k not in canon_cols:
            canon_cols.append(k)

    # Keep only columns that are either required or successfully mapped
    canon_cols = [c for c in canon_cols if (c in present) or (c in required)]

    # Prepare writer
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(canon_cols)

    date_set = set(date_cols or ())

    def _to_iso(d: str) -> str:
        z = (d or "").strip()
        if not z:
            return ""
        z = z.replace("/", "-")
        fmts = ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y", "%d-%m-%y")
        for f in fmts:
            try:
                return datetime.strptime(z, f).date().isoformat()
            except ValueError:
                continue
        # last resort: blank (DATE column will store NULL)
        return ""

    for row in reader:
        out_row: List[str] = []
        for canon in canon_cols:
            # find original column name that maps to this canon
            src_hdr: str | None = None
            for k, v in alias_map.items():
                if v == canon:
                    src_hdr = k
                    break
            if src_hdr is None:
                out_row.append("")
                continue
            i = idx_by_name.get(src_hdr)
            val = (row[i] if (i is not None and i < len(row)) else "").strip()
            if canon in date_set:
                val = _to_iso(val)
            out_row.append(val)
        w.writerow(out_row)

    out.seek(0)
    return out