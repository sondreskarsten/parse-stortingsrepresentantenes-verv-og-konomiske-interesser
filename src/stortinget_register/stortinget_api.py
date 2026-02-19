"""Client for the Stortinget data API (data.stortinget.no).

Fetches the population in scope for the register: representatives
(including substitutes) and government members for a given parliamentary period.

API base: https://data.stortinget.no/eksport
Date format: .NET JSON dates /Date(milliseconds+timezone)/
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone

import aiohttp

API_BASE = "https://data.stortinget.no/eksport"

PERIOD_RANGES: list[tuple[str, date, date]] = [
    ("2017-2021", date(2017, 10, 1), date(2021, 9, 30)),
    ("2021-2025", date(2021, 10, 1), date(2025, 9, 30)),
    ("2025-2029", date(2025, 10, 1), date(2029, 9, 30)),
]

_DOTNET_DATE_RE = re.compile(r"/Date\((-?\d+)[+-]\d{4}\)/")


def parse_dotnet_date(s: str | None) -> str | None:
    """Parse .NET JSON date string to ISO date (YYYY-MM-DD)."""
    if not s:
        return None
    m = _DOTNET_DATE_RE.match(s)
    if not m:
        return None
    ms = int(m.group(1))
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def period_for_date(d: date) -> str:
    """Return the parliamentary period ID covering the given date."""
    for period_id, start, end in PERIOD_RANGES:
        if start <= d <= end:
            return period_id
    if d > PERIOD_RANGES[-1][2]:
        return PERIOD_RANGES[-1][0]
    return PERIOD_RANGES[0][0]


@dataclass
class PersonRecord:
    """A person in the register population snapshot."""

    etternavn: str
    fornavn: str
    foedselsdato: str | None
    id: str
    parti: str | None
    fylke: str | None
    rolle: str
    vara_representant: bool = False

    @property
    def display_name(self) -> str:
        return f"{self.etternavn}, {self.fornavn}"

    def to_dict(self) -> dict:
        return {
            "etternavn": self.etternavn,
            "fornavn": self.fornavn,
            "foedselsdato": self.foedselsdato,
            "id": self.id,
            "parti": self.parti,
            "fylke": self.fylke,
            "rolle": self.rolle,
            "vara_representant": self.vara_representant,
        }


def _extract_person(raw: dict, rolle: str) -> PersonRecord:
    parti = None
    if raw.get("parti") and isinstance(raw["parti"], dict):
        parti = raw["parti"].get("id")

    fylke = None
    if raw.get("fylke") and isinstance(raw["fylke"], dict):
        fylke = raw["fylke"].get("navn")
    elif raw.get("departement"):
        fylke = raw["departement"]

    return PersonRecord(
        etternavn=raw.get("etternavn", ""),
        fornavn=raw.get("fornavn", ""),
        foedselsdato=parse_dotnet_date(raw.get("foedselsdato")),
        id=raw.get("id", ""),
        parti=parti,
        fylke=fylke,
        rolle=rolle,
        vara_representant=raw.get("vara_representant", False),
    )


async def fetch_population(
    session: aiohttp.ClientSession,
    pdf_date: date,
) -> list[PersonRecord]:
    """Fetch all persons in register scope for a given date.

    Returns representatives (including substitutes) and government members
    for the parliamentary period covering the given date, sorted by
    etternavn then fornavn.
    """
    period_id = period_for_date(pdf_date)
    persons: list[PersonRecord] = []
    seen_ids: set[str] = set()

    reps_url = f"{API_BASE}/representanter?stortingsperiodeid={period_id}&vararepresentanter=true&format=json"
    async with session.get(reps_url) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)

    reps_list = data.get("representanter_liste", [])
    for raw in reps_list:
        person = _extract_person(raw, "representant")
        if person.id not in seen_ids:
            seen_ids.add(person.id)
            persons.append(person)

    gov_url = f"{API_BASE}/regjering?stortingsperiodeid={period_id}&format=json"
    async with session.get(gov_url) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)

    gov_list_key = next(
        (k for k in data if k.endswith("_liste") and isinstance(data[k], list)),
        None,
    )
    if gov_list_key:
        for raw in data[gov_list_key]:
            rolle = raw.get("tittel", "regjeringsmedlem")
            person = _extract_person(raw, rolle)
            if person.id not in seen_ids:
                seen_ids.add(person.id)
                persons.append(person)

    persons.sort(key=lambda p: (p.etternavn.lower(), p.fornavn.lower()))
    return persons
