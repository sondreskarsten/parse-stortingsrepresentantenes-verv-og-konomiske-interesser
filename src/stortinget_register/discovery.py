"""URL pattern discovery for Stortinget economic interests register PDFs.

The register is published biweekly (except July) as PDFs at:
    https://www.stortinget.no/globalassets/pdf/verv-og-okonomiske-interesser-register/
        arkiv_{period}/pr-{day}-{month}-{year}.pdf

Known inconsistencies:
    - Folder naming: arkiv_20232024 (no hyphen) vs arkiv_2024-2025 (hyphen)
    - Month abbreviation: "sept" used once (2023-09-27) vs full "september" elsewhere
    - Back-to-back publications: Nov 13 and Nov 14, 2025 both exist
"""

from __future__ import annotations

from datetime import date, timedelta

BASE_URL = "https://www.stortinget.no/globalassets/pdf/verv-og-okonomiske-interesser-register"

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

MONTH_ABBREVIATIONS = {
    9: ["sept"],
}


def get_period_folders(d: date) -> list[str]:
    """Return candidate archive folder names for a given date.

    Stortinget sessions run roughly Oct-Sep. Both hyphenated and
    non-hyphenated variants are checked due to inconsistent naming.
    """
    y = d.year
    return [
        f"arkiv_{y - 1}-{y}",
        f"arkiv_{y}-{y + 1}",
        f"arkiv_{y - 1}{y}",
        f"arkiv_{y}{y + 1}",
    ]


def get_month_variants(month_num: int) -> list[str]:
    """Return all known spellings for a Norwegian month number."""
    variants = [NORWEGIAN_MONTHS[month_num]]
    if month_num in MONTH_ABBREVIATIONS:
        variants.extend(MONTH_ABBREVIATIONS[month_num])
    return variants


def build_candidate_urls(d: date) -> list[str]:
    """Generate all candidate PDF URLs for a given date."""
    folders = get_period_folders(d)
    month_variants = get_month_variants(d.month)
    urls = []
    for folder in folders:
        for month_name in month_variants:
            filename = f"pr-{d.day}-{month_name}-{d.year}.pdf"
            urls.append(f"{BASE_URL}/{folder}/{filename}")
    return urls


def generate_candidate_dates(start_date: date, end_date: date) -> list[date]:
    """Return plausible publication dates between start_date and end_date.

    The register publishes biweekly on weekdays (Mon-Fri). No weekend
    publications exist in the historical record. July is skipped except
    for the first week (one July leak exists historically).
    """
    dates: list[date] = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:  # Mon-Fri
            if d.month == 7:
                if d.day <= 7:
                    dates.append(d)
            else:
                dates.append(d)
        d += timedelta(days=1)
    return dates
