# app/dao/staging_common.py
from __future__ import annotations
import csv
import io
from datetime import datetime
from typing import Iterable, Mapping, List, Tuple, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

# -------------------------
# Basic guards / DDL helper
# -------------------------

def assert_postgres(engine: Engine) -> None:
    name = engine.url.get_backend_name()
    if not name.startswith("postgresql"):
        raise RuntimeError(f"Expected Postgres engine, got {name!r}")

def ensure_schema_and_table(engine: Engine, schema_sql: str, table_sql: str, indexes: List[str] | None = None) -> None:
    assert_postgres(engine)
    with engine.begin() as conn:
        conn.execute(text(schema_sql))
        conn.execute(text(table_sql))
        for idx_sql in (indexes or []):
            conn.execute(text(idx_sql))

# -------------------------
# COPY / fallback
# -------------------------

def _pg_copy(engine: Engine, copy_sql: str, payload: io.TextIOBase | io.BytesIO) -> None:
    """
    COPY ... FROM STDIN when the DBAPI cursor supports it (psycopg2/psycopg3),
    otherwise fall back to an executemany bulk INSERT (dev-friendly).
    """
    assert_postgres(engine)
    with engine.begin() as conn:
        raw = conn.connection  # DBAPI connection
        cur = raw.cursor()

        # Try native copy_expert (psycopg2/3)
        if hasattr(cur, "copy_expert"):
            cur.copy_expert(copy_sql, payload)  # type: ignore[attr-defined]
            return

    # ---- Fallback path: parse CSV and executemany insert ----
    # Extract table + column list from "COPY schema.table (a,b,c) FROM STDIN ..."
    import re
    m = re.search(r"COPY\s+([\w.]+)\s*\(([^)]+)\)", copy_sql, re.IGNORECASE)
    if not m:
        raise RuntimeError("Fallback COPY parser couldn't find target table/columns.")
    full_table = m.group(1).strip()
    col_list = [c.strip() for c in m.group(2).split(",")]

    # Read CSV rows to dicts
    payload.seek(0)
    if isinstance(payload, io.BytesIO):
        text_buf = io.StringIO(payload.getvalue().decode("utf-8"))
    else:
        text_buf = payload  # already text
    rdr = csv.DictReader(text_buf)
    rows = []
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
    file_obj,
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

    canon_cols = list(required)  # preserve the order caller expects
    # include optional columns present in mapping but not required (stable order)
    for k in mapping.keys():
        if k not in canon_cols:
            canon_cols.append(k)

    # ensure no dupes and keep only those actually mappable
    canon_cols = [c for c in canon_cols if c in present or c in required]

    # write output
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(canon_cols)

    date_cols = set(date_cols or [])

    def _to_iso(d: str) -> str:
        d = (d or "").strip()
        if not d:
            return ""
        z = d.replace("/", "-")
        fmts = ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y", "%d-%m-%y")
        for f in fmts:
            try:
                return datetime.strptime(z, f).date().isoformat()
            except Exception:
                pass
        # last resort: leave as blank (staging column is DATE; blank casts to NULL)
        return ""

    for row in reader:
        out_row = []
        for canon in canon_cols:
            # find original column name that maps to this canon
            src = None
            for k, v in alias_map.items():
                if v == canon:
                    src = k
                    break
            if src is None:
                out_row.append("")
                continue
            i = idx_by_name.get(src)
            val = (row[i] if i is not None and i < len(row) else "").strip()
            if canon in date_cols:
                val = _to_iso(val)
            out_row.append(val)
        w.writerow(out_row)

    out.seek(0)
    return out