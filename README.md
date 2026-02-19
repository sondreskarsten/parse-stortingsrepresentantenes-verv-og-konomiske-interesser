# stortinget-register

Mirror of the Norwegian Parliament's [register of economic interests](https://www.stortinget.no/no/stortinget-og-demokratiet/representantene/okonomiske-interesser/) PDFs. Discovers all published versions via URL pattern brute-force, downloads missing PDFs to local/S3/GCS storage, and tracks state in a Parquet manifest.

## URL Pattern

```
https://www.stortinget.no/globalassets/pdf/verv-og-okonomiske-interesser-register/
    arkiv_{period}/pr-{day}-{month_norwegian}-{year}.pdf
```

| Component | Format | Example |
|-----------|--------|---------|
| `period` | `YYYY-YYYY` or `YYYYYYYY` | `2024-2025`, `20232024` |
| `day` | No zero-padding | `5`, `13` |
| `month` | Norwegian, lowercase | `januar`, `februar`, `september` |
| `year` | Four digits | `2025` |

Known inconsistencies: `arkiv_20232024` lacks the hyphen present in all other period folders. `sept` appears once (2023-09-27) vs full `september` elsewhere.

## Publication Schedule

Per [Stortingets forretningsorden § 76](https://www.stortinget.no/no/Stortinget-og-demokratiet/Lover-og-instrukser/forretningsorden/) and register §12: updated biweekly (every ~14 days), typically Fridays, with no publications in July. Session folders change around October with the parliamentary year.

## Install

```bash
# Local development
uv sync

# With GCS support
uv sync --extra gcs

# With S3 support
uv sync --extra s3
```

## Usage

```bash
# Discover and download all missing PDFs to local storage
stortinget-register sync ./data

# Sync to GCS bucket
stortinget-register sync gs://bucket-name/prefix

# Restrict scan range
stortinget-register sync ./data --start-year 2024 --end-year 2025

# With runtime limit (for CI)
stortinget-register sync gs://bucket/prefix --max-runtime 25

# Show manifest stats
stortinget-register status ./data
```

## Storage Layout

```
{storage_path}/
├── manifest.parquet          # Download tracking (date, url, hash, status, population)
├── checkpoint.json           # Resume state for interrupted runs
├── pdfs/
│   ├── pr-2022-10-18.pdf
│   ├── pr-2023-01-18.pdf
│   └── ...
└── population/
    ├── pr-2022-10-18.json    # Representatives + govt members at that date
    ├── pr-2023-01-18.json
    └── ...
```

### Population Snapshot Format

Each population JSON contains the persons in scope for that register date, fetched from the [Stortinget data API](https://data.stortinget.no/eksport). Persons are sorted by `etternavn` (last name) then `fornavn`.

```json
{
  "date": "2025-01-14",
  "period_id": "2021-2025",
  "population": [
    {
      "etternavn": "Støre",
      "fornavn": "Jonas Gahr",
      "foedselsdato": "1960-08-25",
      "id": "JGS",
      "parti": "A",
      "fylke": "Statsministerens kontor",
      "rolle": "Statsminister",
      "vara_representant": false
    }
  ]
}
```

Population includes:
- All elected representatives for the parliamentary period
- Substitute representatives (`vara_representant: true`)
- Government members (ministers) with their title as `rolle`

## Configuration

Environment variables (prefix `STORTING_`) override defaults. CLI flags override env vars.

| Variable | Default | Description |
|----------|---------|-------------|
| `STORTING_STORAGE_PATH` | `./data` | Root storage path |
| `STORTING_MAX_CONCURRENT` | `5` | Max simultaneous HTTP connections |
| `STORTING_MAX_RETRIES` | `5` | Retry attempts per failed request |
| `STORTING_MAX_RUNTIME_MINUTES` | `0` | Graceful shutdown timer (0=unlimited) |
| `STORTING_SCAN_START_YEAR` | `2021` | Earliest year to scan |
| `STORTING_SCAN_END_YEAR` | current | Latest year to scan |
| `STORTING_LOG_LEVEL` | `INFO` | Logging verbosity |

## GitHub Actions

The included workflow (`.github/workflows/sync.yml`) runs weekly on Fridays via GCS Workload Identity Federation. Required secrets:

- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT`

## Data Sources

| Source | URL |
|--------|-----|
| Register landing page | [stortinget.no/…/okonomiske-interesser](https://www.stortinget.no/no/stortinget-og-demokratiet/representantene/okonomiske-interesser/) |
| PDF archive base | `https://www.stortinget.no/globalassets/pdf/verv-og-okonomiske-interesser-register/` |
| Stortinget data API | [data.stortinget.no/eksport](https://data.stortinget.no/eksport) |
