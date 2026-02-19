"""Tiered URL discovery for Stortinget economic interests register PDFs.

Strategy (executed in order):
    1. SCRAPE  — Parse landing page for the current "latest" PDF link.
    2. GAPS    — Estimate expected publication dates since last manifest
                 entry using biweekly cadence. Check best-guess dates
                 (Mon-Fri of the expected week).
    3. EXHAUST — For gap windows already checked once without a hit,
                 escalate to all weekdays in the full gap range.

URL pattern:
    {BASE}/arkiv_{period}/pr-{day}-{month_no}-{year}.pdf

Known inconsistencies:
    - Folder naming: arkiv_20232024 (no hyphen) vs arkiv_2024-2025
    - Month abbreviation: "sept" used once (2023-09-27)
    - Back-to-back publications: Nov 13 and Nov 14, 2025 both exist
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, timedelta

BASE_URL = "https://www.stortinget.no/globalassets/pdf/verv-og-okonomiske-interesser-register"
LANDING_PAGE = "https://www.stortinget.no/no/stortinget-og-demokratiet/representantene/okonomiske-interesser/"

NORWEGIAN_MONTHS = {
    1: "januar",
    2: "februar",
    3: "mars",
    4: "april",
    5: "mai",
    6: "juni",
    7: "juli",
    8: "august",
    9: "september",
    10: "oktober",
    11: "november",
    12: "desember",
}

MONTHS_REVERSE = {v: k for k, v in NORWEGIAN_MONTHS.items()}
MONTHS_REVERSE["sept"] = 9

MONTH_ABBREVIATIONS = {
    9: ["sept"],
}

PUBLICATION_CADENCE_DAYS = 14

PDF_LINK_RE = re.compile(
    r"/globalassets/pdf/verv-og-okonomiske-interesser-register/"
    r"(arkiv_[^/]+)/pr-(\d{1,2})-([a-z]+)-(\d{4})\.pdf",
    re.IGNORECASE,
)


def parse_pdf_url(url: str) -> tuple[str, date, str] | None:
    """Extract (full_url, date, period_folder) from a PDF URL or href."""
    m = PDF_LINK_RE.search(url)
    if not m:
        return None
    folder = m.group(1)
    day = int(m.group(2))
    month_name = m.group(3).lower()
    year = int(m.group(4))
    month_num = MONTHS_REVERSE.get(month_name)
    if not month_num:
        return None
    d = date(year, month_num, day)
    full_url = f"{BASE_URL}/{folder}/pr-{day}-{month_name}-{year}.pdf"
    return full_url, d, folder


def get_period_folders(d: date) -> list[str]:
    y = d.year
    return [
        f"arkiv_{y - 1}-{y}",
        f"arkiv_{y}-{y + 1}",
        f"arkiv_{y - 1}{y}",
        f"arkiv_{y}{y + 1}",
    ]


def get_month_variants(month_num: int) -> list[str]:
    variants = [NORWEGIAN_MONTHS[month_num]]
    if month_num in MONTH_ABBREVIATIONS:
        variants.extend(MONTH_ABBREVIATIONS[month_num])
    return variants


def build_candidate_urls(d: date) -> list[str]:
    folders = get_period_folders(d)
    month_variants = get_month_variants(d.month)
    urls = []
    for folder in folders:
        for month_name in month_variants:
            filename = f"pr-{d.day}-{month_name}-{d.year}.pdf"
            urls.append(f"{BASE_URL}/{folder}/{filename}")
    return urls


def _weekdays_in_range(start: date, end: date) -> list[date]:
    """All Mon-Fri dates from start to end inclusive."""
    dates = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    return dates


def _week_around(d: date) -> tuple[date, date]:
    """Return (monday, friday) of the ISO week containing d."""
    monday = d - timedelta(days=d.weekday())
    friday = monday + timedelta(days=4)
    return monday, friday


def estimate_expected_dates(last_known: date, until: date) -> list[date]:
    """Estimate biweekly publication dates from last_known to until.

    Returns the midpoints of expected publication windows, spaced
    ~14 days apart. Skips July (except first week).
    """
    expected = []
    cursor = last_known + timedelta(days=PUBLICATION_CADENCE_DAYS)
    while cursor <= until:
        if cursor.month == 7 and cursor.day > 7:
            cursor += timedelta(days=PUBLICATION_CADENCE_DAYS)
            continue
        expected.append(cursor)
        cursor += timedelta(days=PUBLICATION_CADENCE_DAYS)
    return expected


def best_guess_dates(expected: date) -> list[date]:
    """Mon-Fri of the week containing the expected publication date."""
    monday, friday = _week_around(expected)
    return _weekdays_in_range(monday, friday)


def exhaustive_dates(gap_start: date, gap_end: date) -> list[date]:
    """All weekdays between gap_start and gap_end (exclusive on both ends)."""
    start = gap_start + timedelta(days=1)
    end = gap_end - timedelta(days=1)
    if start > end:
        return []
    dates = _weekdays_in_range(start, end)
    if gap_start.month <= 7 <= gap_end.month:
        dates = [d for d in dates if not (d.month == 7 and d.day > 7)]
    return dates


def initial_scan_dates(start_year: int, end_year: int) -> list[date]:
    """All weekdays for a full initial scan (no manifest exists yet).

    Skips July except first week.
    """
    start = date(start_year, 1, 1)
    today = date.today()
    end = min(date(end_year, 12, 31), today)
    dates = _weekdays_in_range(start, end)
    return [d for d in dates if not (d.month == 7 and d.day > 7)]


# --- Missed-hypothesis tracker ---


@dataclass
class GapRecord:
    """A gap window where a publication was expected but not found."""

    gap_start: str
    gap_end: str
    expected_date: str
    check_count: int = 0
    dates_checked: list[str] = field(default_factory=list)


@dataclass
class MissedHypotheses:
    """Tracks gap windows checked without finding a publication."""

    gaps: dict[str, GapRecord] = field(default_factory=dict)

    def to_json(self) -> bytes:
        data = {}
        for key, rec in self.gaps.items():
            data[key] = {
                "gap_start": rec.gap_start,
                "gap_end": rec.gap_end,
                "expected_date": rec.expected_date,
                "check_count": rec.check_count,
                "dates_checked": rec.dates_checked,
            }
        return json.dumps(data, indent=2).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes) -> MissedHypotheses:
        data = json.loads(raw)
        gaps = {}
        for key, rec in data.items():
            gaps[key] = GapRecord(**rec)
        return cls(gaps=gaps)

    def get_gap(self, key: str) -> GapRecord | None:
        return self.gaps.get(key)

    def upsert_gap(self, key: str, record: GapRecord) -> None:
        self.gaps[key] = record

    def remove_gap(self, key: str) -> None:
        self.gaps.pop(key, None)
