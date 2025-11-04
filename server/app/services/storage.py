# app/services/storage.py
from __future__ import annotations
import os
from typing import BinaryIO

class LocalStorage:
    """
    Super-simple local storage for MVP.
    Writes under <instance_path>/uploads/ and returns file:// URLs.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def put_fileobj(self, fileobj: BinaryIO, key: str) -> str:
        path = os.path.join(self.base_dir, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(fileobj.read())
        return f"file://{path}"

    def abspath(self, key: str) -> str:
        return os.path.join(self.base_dir, key)