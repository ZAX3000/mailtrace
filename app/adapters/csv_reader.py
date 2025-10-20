# app/adapters/csv_reader.py
import csv
from typing import Iterator
from app.types import AddressRow

def read_addresses_csv(path: str) -> Iterator[AddressRow]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            yield AddressRow(
                email=(r.get("email") or "").strip(),
                line1=(r.get("line1") or "").strip(),
                city=(r.get("city") or "").strip(),
                state=(r.get("state") or "").strip(),
                zip=(r.get("zip") or "").strip(),
            )