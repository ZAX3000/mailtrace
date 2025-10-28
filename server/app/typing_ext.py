# app/typing_ext.py
from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from flask import Flask

if TYPE_CHECKING:
    # type-only import to avoid runtime cycles
    from app.services.storage import LocalStorage

class MailTraceFlask(Flask):
    storage: Optional["LocalStorage"] = None