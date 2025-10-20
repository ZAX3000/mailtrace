# app/types.py
from typing import TypedDict, NotRequired

class AddressRow(TypedDict):
    email: str
    line1: str
    city: str
    state: str
    zip: str

class GeocodeResult(TypedDict):
    email: str
    lat: float
    lon: float
    quality: NotRequired[str]