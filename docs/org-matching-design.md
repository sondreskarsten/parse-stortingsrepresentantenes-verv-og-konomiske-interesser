# Org-Centric Parsing & Enhetsregisteret Roller Matching

## Problem Statement

The register PDFs contain free-text references to organisations across
multiple section types (§2, §3, §8, §9, §9a). These references vary
from structured (inline org number) to completely unstructured (company
name embedded in a sentence). The goal is to:

1. Extract every org-related mention per person per publication date.
2. Resolve each mention to a BRREG org number.
3. Fetch roller (roles) for that org number from the Enhetsregisteret
   API.
4. Fuzzy-match the register person against the roller response to
   produce a confirmed or candidate link.

The roller API returns **current state only**. This creates a
fundamental asymmetry: a 2022 PDF may reference roles that have since
been resigned, and those persons may no longer appear in today's roller
response. Historic and current publications must be handled differently.

---

## 1. Org Mention Extraction

### 1.1 Section Types Containing Org References

| Section | What to extract | Org number frequency |
|---------|----------------|---------------------|
| §2 Styreverv mv. | Organisation name, role, compensation | ~15% have inline org numbers |
| §3 Selvstendig næring | Company name (ENK or AS) | ~5% have org numbers |
| §8 Eiendom i næring | Holding company if property held via entity | Rare |
| §9 Selskapsinteresser | Company name, shares, ownership % | ~10% have org numbers |
| §9a Gjeld i næring | Creditor (bank), debtor company | Rare |

§4 (paid employment), §5 (former employer), §6 (future employer) also
name organisations but are employment relationships rather than
governance/ownership ties. Include them as secondary org mentions.

### 1.2 Inline Org Number Patterns

Observed patterns from the corpus:

```
985 483 345 Marine Nord AS          → org_number with spaces
Modulservice AS 998 210 585         → org_number after name
927 242 923 - Styreleder            → org_number then role
(org.nr 929360990)                  → parenthetical
Askjer job utført 926 343 796       → mid-sentence
Enkeltmannsforetak 977 105 951      → ENK with org number
```

Regex for extraction:

```
\b(\d{3}\s?\d{3}\s?\d{3})\b
```

Normalise by stripping spaces → 9-digit string. Validate with MOD 11
check digit (BRREG standard).

### 1.3 Extraction Output

```python
class OrgMention(BaseModel):
    """A single organisation reference extracted from a register entry."""
    
    section: str                          # "§2", "§3", "§9", etc.
    organisation_name: str                # as stated in text
    org_number: str | None                # 9-digit string if found inline
    role_claimed: str | None              # "Styreleder", "Daglig leder", "eier 50%"
    ownership_pct: str | None             # "100%", "under 1 pst."
    num_shares: int | None                # if §9
    compensated: bool | None              # if §2
    raw_text: str                         # verbatim section text
    
    person_name: str                      # "Astrup, Nikolai" from person header
    person_party: str                     # "H"
    person_district: str                  # "Oslo"
    publication_date: str                 # "2026-02-13"
```

One person entry can produce multiple `OrgMention` records. Astrup's §2
alone yields 5 (one per board position), §9 yields 10+.

---

## 2. Org Number Resolution

Three tiers, in order:

### Tier 1: Inline org number

Already extracted from regex. ~10-15% of mentions.

### Tier 2: Exact name search

```
GET /enhetsregisteret/api/enheter?navn={name}
```

Match criteria: response `navn` equals extracted `organisation_name`
after normalisation (uppercase, strip "AS"/"ASA"/"DA"/etc., strip
punctuation). Take the hit if exactly one active entity matches.

Observed API behaviour:
- Search is case-insensitive.
- Returns partial matches (searching "Pactum" returns Pactum AS,
  Pactum Beta AS, Pactum Capital AS, etc.).
- `_embedded.enheter[].organisasjonsnummer` is the 9-digit key.

### Tier 3: Fuzzy name search + disambiguation

When Tier 2 returns multiple candidates or zero exact matches:

- Fuzzy match using token-sort ratio on normalised names.
- Disambiguate using `organisasjonsform.kode` (prefer AS/ASA if text
  says "AS"), `forretningsadresse.kommune`, and registration status.
- Flag as `resolution_confidence: "candidate"` vs `"confirmed"`.

Companies not found in Enhetsregisteret: foreign entities (Eurovema
Mobility AB, Eden Research PLC, Rio Copenhagen MBH & CO. KG) will fail
resolution. Mark as `foreign: true` and skip roller matching.

### 2.1 Resolution Cache

Org name → org number mappings are stable. Cache in a JSON lookup file
keyed by normalised name. Avoids redundant API calls across the 89 PDFs
where the same ~500 entities recur.

```
org_resolution_cache.json
{
    "PACTUM AS": {"org_number": "929979397", "confidence": "confirmed"},
    "EGELAND & ANDERSEN HOLDING AS": {"org_number": "989459317", "confidence": "confirmed"},
    "EDEN RESEARCH PLC": {"org_number": null, "foreign": true},
    ...
}
```

---

## 3. Roller Fetch & Person Matching

### 3.1 Roller API Response Structure

```
GET /enhetsregisteret/api/enheter/{orgnr}/roller
```

Response:

```json
{
  "rollegrupper": [
    {
      "type": {"kode": "STYR", "beskrivelse": "Styre"},
      "sistEndret": "2025-01-07",
      "roller": [
        {
          "type": {"kode": "LEDE", "beskrivelse": "Styrets leder"},
          "person": {
            "fodselsdato": "1978-06-12",
            "navn": {
              "fornavn": "Nikolai",
              "mellomnavn": "Eivindssøn",
              "etternavn": "Astrup"
            },
            "erDoed": false
          },
          "fratraadt": false,
          "avregistrert": false,
          "rekkefolge": 0
        }
      ]
    }
  ]
}
```

Relevant rolle type codes:

| Code | Description | Maps to register section |
|------|------------|------------------------|
| DAGL | Daglig leder | §2, §3 |
| LEDE | Styrets leder | §2 "Styreleder" |
| MEDL | Styremedlem | §2 "Styremedlem" |
| NEST | Nestleder | §2 "Nestleder" |
| VARA | Varamedlem | §2 "Varamedlem" |
| INNH | Innehaver | §3 (ENK) |
| DTSO | Deltaker solidarisk ansvar | §3 (ANS) |
| DTPR | Deltaker proratarisk ansvar | §3 (DA) |
| KOMP | Komplementar | §9 (KS) |

### 3.2 Matching Logic

Inputs:
- **Register side**: `person_name` ("Astrup, Nikolai"), `person_party`,
  `person_district`, `person_foedselsdato` (from population JSON).
- **Roller side**: `person.navn.fornavn`, `person.navn.mellomnavn`,
  `person.navn.etternavn`, `person.fodselsdato`.

#### Step 1: DOB match (strong signal)

Population JSON provides `foedselsdato` per person. The roller API
provides `fodselsdato` per role holder. An exact DOB match is near-
deterministic — Norwegian DOB + last name is effectively unique.

```
population_dob["Astrup, Nikolai"] = "1978-06-12"
roller_dob = "1978-06-12"
→ match_confidence = "confirmed"
```

#### Step 2: Name match (fallback when DOB unavailable)

The register prints names as "Etternavn, Fornavn" but never includes
mellomnavn. The roller API includes mellomnavn. Matching strategy:

1. Exact: `etternavn` matches AND `fornavn` matches (ignoring
   mellomnavn). → `confirmed`.
2. Fuzzy: Levenshtein or token-sort on `fornavn` + `etternavn`,
   threshold ≥ 85. Handles "Bjørnar" vs "Bjørn", "Nikolai" vs
   "Nikolai Eivindssøn". → `candidate`.
3. Partial fornavn: first token of roller `fornavn` matches register
   fornavn. Handles "Hans Edvard" (register) vs "Hans Edvard" (roller
   `fornavn`="Hans Edvard"). → `confirmed` if etternavn exact.

#### Step 3: Role consistency check (optional signal)

If the register says "Styreleder, Pactum AS" and the roller API returns
`LEDE` for a person matching on DOB+name, that is a triple confirmation.
Not required for matching but useful for quality metrics.

### 3.3 Match Output

```python
class RollerMatch(BaseModel):
    """Result of matching a register org mention against BRREG roller."""
    
    org_mention: OrgMention               # from extraction
    org_number: str                       # resolved
    org_resolution_confidence: str        # "confirmed" | "candidate"
    
    roller_person_name: str | None        # from API: "Nikolai Eivindssøn Astrup"
    roller_person_dob: str | None         # "1978-06-12"
    roller_role_code: str | None          # "LEDE"
    roller_role_description: str | None   # "Styrets leder"
    roller_fratraadt: bool | None         # False = active, True = resigned
    
    match_method: str | None              # "dob_exact" | "name_exact" | "name_fuzzy"
    match_confidence: str                 # "confirmed" | "candidate" | "no_match"
    
    person_dob_from_population: str | None  # from stortinget API population
```

---

## 4. The Historic Problem

### 4.1 What the roller API exposes

The `/enheter/{orgnr}/roller` endpoint returns the **current state** of
the role register. Observed behaviour:

- Active roles: `fratraadt=false, avregistrert=false`. Always present.
- Resigned roles: sometimes appear with `fratraadt=true`. Coverage is
  **inconsistent** — BRREG does not guarantee that all historical role
  holders are returned.
- Dissolved companies: roller endpoint still responds (tested with
  Pactum Alfa AS). Returns last-known role state.
- `sistEndret` on the `rollegruppe` level reflects the most recent
  change date for that role group, not individual role history.

There is no `?dato=2022-10-18` parameter. No temporal query is
available.

### 4.2 Consequence for the 89-PDF corpus

The corpus spans Oct 2022 → Feb 2026. Role turnover in Norwegian
companies means:

- A person listed as styreleder in a 2022 PDF who resigned in 2023 may
  be absent from today's roller response.
- New roles created after a given PDF date are irrelevant noise for
  that publication.
- Company dissolutions, mergers, or name changes break org number
  continuity.

Expected match degradation: match rate will decrease for older
publications. Rough estimate based on typical board tenure (2-4 years):
- 2025-2026 PDFs: ~90%+ match rate against current roller.
- 2023-2024 PDFs: ~70-80%.
- 2022 PDFs: ~60-70%.

### 4.3 Mitigation strategies

#### Strategy A: Snapshot roller data now (baseline)

For every resolved org number in the corpus, fetch and store the roller
response today. This creates a point-in-time snapshot that is accurate
for current/recent PDFs and serves as the best-available approximation
for older ones.

Store as:

```
roller_snapshots/
├── 929979397.json      # Pactum AS
├── 989459317.json      # Egeland & Andersen Holding AS
└── ...
```

Stamp each with `snapshot_date`. This is a one-time batch job.

#### Strategy B: Accept no-match for historic, flag it

For older PDFs, when a person-org pair doesn't match in the roller
snapshot:

```python
match_confidence = "historic_no_match"
```

This is a valid output. The register itself is the authoritative record
of the claimed relationship. The roller match is a *corroboration*
signal, not a prerequisite. A `historic_no_match` means "this person
claimed this role at this time, but the current BRREG register does not
confirm it — likely due to role turnover."

#### Strategy C: Periodic roller snapshots going forward

For future publications (GitHub Actions cron), snapshot roller data
each sync run. Over time this builds a longitudinal roller dataset
that can retroactively improve matching for PDFs published near
snapshot dates.

Store with date prefix:

```
roller_snapshots/
├── 2026-02-24/
│   ├── 929979397.json
│   └── ...
├── 2026-03-10/
│   └── ...
```

#### Strategy D: BRREG historiske roller (if available)

BRREG's Rolleregisterets grunndata product (subscription-based) may
contain historical role data. This is not available through the open
API. If access is obtained, it would solve the historic problem
entirely. Until then, Strategies A-C apply.

### 4.4 Processing order

To maximise match rate on the most valuable data:

1. **Process newest PDF first** (2026-02-13). Roller API is
   near-perfectly aligned.
2. Walk backwards chronologically. Each subsequent PDF has marginally
   worse roller alignment.
3. Org resolution cache and roller snapshot cache accumulate, avoiding
   redundant API calls.
4. Final pass: compute match statistics per publication date to
   quantify degradation.

---

## 5. Full Pipeline

```
┌──────────────────────────────────────────────────────┐
│  For each PDF (newest → oldest):                     │
│                                                      │
│  1. PARSE                                            │
│     PDF → PersonEntry[] (existing schema.py)         │
│                                                      │
│  2. EXTRACT ORG MENTIONS                             │
│     PersonEntry → OrgMention[]                       │
│     - regex for inline org numbers                   │
│     - section-specific extraction per §2/§3/§9/etc.  │
│                                                      │
│  3. RESOLVE ORG NUMBERS                              │
│     OrgMention.organisation_name → org_number        │
│     - check cache                                    │
│     - Tier 1: inline                                 │
│     - Tier 2: exact name search                      │
│     - Tier 3: fuzzy + disambiguate                   │
│     - write to cache                                 │
│                                                      │
│  4. FETCH ROLLER (with snapshot cache)               │
│     org_number → roller response                     │
│     - check snapshot cache (same day = hit)          │
│     - fetch from API                                 │
│     - write to snapshot cache                        │
│                                                      │
│  5. MATCH                                            │
│     (OrgMention.person, population.foedselsdato)     │
│       ×                                              │
│     roller.person                                    │
│     → RollerMatch                                    │
│                                                      │
│  6. OUTPUT                                           │
│     Per publication date:                            │
│     - matches.jsonl  (all RollerMatch records)       │
│     - stats.json     (match rates, no-match counts)  │
└──────────────────────────────────────────────────────┘
```

### 5.1 Rate limiting

Enhetsregisteret API: no documented rate limit but practical courtesy
applies. Batch at 10 req/s with backoff on 429/503. The distinct org
count across the full corpus is estimated at ~800-1200 entities, so
resolution + roller fetch is ~2400 requests total — manageable in a
single run.

### 5.2 Edge cases

| Case | Handling |
|------|---------|
| Foreign company (PLC, AB, GmbH, KG) | Skip roller match. Flag `foreign: true`. |
| Dissolved company | Roller API still responds. Match normally. |
| Person claims role in org where they don't appear in roller | `no_match` or `historic_no_match` depending on PDF age. |
| Person appears in roller but with different role than claimed | Match on person, flag role discrepancy. |
| Same org name, multiple BRREG hits | Disambiguate on organisasjonsform, status, kommune. |
| Org name changed since PDF date | Name search may fail. Org number (if inline) still works. |
| ENK (enkeltmannsforetak) | Rolle code INNH. Typically not in Enhetsregisteret (Foretaksregisteret). Use underenheter endpoint. |
| Borettslag, sameie, stiftelse | Different org forms. Roller API works the same way. |
| "under 1 pst." / "mindre enn 1 %" holdings | §9 mentions in listed companies. Skip roller match — no governance role expected for <1% holdings in ASA. |

### 5.3 Listed company filter

§9 entries frequently reference publicly traded companies (Equinor ASA,
Aker ASA, DNB ASA, etc.) with trivial holdings. Fetching roller for
these is pointless — the representative won't appear in the board
of Equinor because they hold 500 shares.

Filter rule: if `organisasjonsform == "ASA"` AND `ownership_pct` parses
to < 1%, skip roller match. Still extract the `OrgMention` for the
holdings dataset, but don't waste an API call.

This likely eliminates 40-60% of §9 mentions.
