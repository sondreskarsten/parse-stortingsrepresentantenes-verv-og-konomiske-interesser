"""Pydantic extraction schema for Stortinget economic interests register.

Corporation-centric subset. Only sections referencing organisations
retained for BRREG Enhetsregisteret roller matching pipeline.

Retained sections (org references):

    §2   Styreverv mv.                 19,560 occurrences  -> governance roles
    §3   Selvstendig naering            6,049              -> business ownership
    §4   Loennet stilling m.v.          7,447              -> employer orgs
    §5   Tidligere arbeidsgiver         6,049              -> former employer orgs
    §8   Eiendom i naering              1,989              -> property via entities
    §9   Selskapsinteresser             8,970              -> shareholdings
    §9a  Gjeld i naeringsvirksomhet       271              -> business debt

Dropped sections (no org matching value):

    §6   Framtidig arbeidsgiver         1,422
    §7   Oekonomisk stoette               882
    §10  Utenlandsreiser                2,048
    §11  Gaver                          2,721
    §15  Andre forhold                  1,066
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- §2: Styreverv mv. (Board positions and public offices) ---


class BoardPosition(BaseModel):
    """A single board position, committee membership, or public office."""

    organisation: str = Field(description="Name of organisation, company, or public body")
    org_number: str | None = Field(
        default=None, description="Norwegian org number (9 digits) if stated"
    )
    role: str | None = Field(
        default=None,
        description=(
            "Role held: e.g. 'Styreleder', 'Styremedlem', 'Varamedlem', "
            "'Medlem', 'Nestleder', 'Leder', 'Gruppeleder', "
            "'Kommunestyrerepresentant', 'Ordfoerer'"
        ),
    )
    compensated: bool | None = Field(
        default=None,
        description=(
            "Whether compensation is received. True if 'loennet', 'godtgjoerelse', "
            "'honorar'. False if 'uloennet', 'ikke loennet', 'ikke godtgjoerelse'. "
            "None if ambiguous."
        ),
    )
    compensation_note: str | None = Field(
        default=None,
        description="Verbatim compensation detail if not a simple yes/no",
    )
    period: str | None = Field(
        default=None, description="Date range or year if stated, e.g. '2015-2019'"
    )
    leave_noted: bool = Field(
        default=False,
        description="True if entry mentions 'permisjon' or 'fritak' from this position",
    )


class BoardPositions(BaseModel):
    """§2 — Verv i styrende organer, offentlige verv."""

    positions: list[BoardPosition] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §2 entry")


# --- §3: Selvstendig naering (Self-employed / independent business) ---


class SelfEmployment(BaseModel):
    """A single self-employment or independent business activity."""

    description: str = Field(description="Nature of the business activity")
    company_name: str | None = Field(default=None, description="Company or ENK name if stated")
    org_number: str | None = Field(default=None, description="Org number if stated")
    high_value_assignment: bool = Field(
        default=False,
        description="True if §3 para 3 triggered: single assignment >50k NOK disclosed",
    )
    assignment_amount_nok: float | None = Field(
        default=None, description="Disclosed amount if high_value_assignment is True"
    )
    client: str | None = Field(
        default=None,
        description="Client/oppdragsgiver if disclosed under §3 para 3",
    )


class SelfEmploymentSection(BaseModel):
    """§3 — Selvstendig inntektsbringende virksomhet."""

    entries: list[SelfEmployment] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §3 entry")


# --- §4: Loennet stilling m.v. (Paid employment) ---


class PaidEmployment(BaseModel):
    """A single paid position held alongside the parliamentary role."""

    employer: str = Field(description="Employer name")
    org_number: str | None = Field(default=None)
    position: str | None = Field(default=None, description="Job title or role")
    percentage: str | None = Field(
        default=None, description="Employment percentage if stated, e.g. '100%', '60%'"
    )
    on_leave: bool = Field(
        default=False, description="True if 'permisjon' is mentioned for this position"
    )


class PaidEmploymentSection(BaseModel):
    """§4 — Loennet stilling eller engasjement."""

    entries: list[PaidEmployment] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §4 entry")


# --- §5: Tidligere arbeidsgiver (Former employer arrangements) ---


class FormerEmployer(BaseModel):
    """Agreement or arrangement with a former employer."""

    employer: str = Field(description="Former employer name")
    arrangement: str = Field(
        description=(
            "Nature of arrangement: e.g. 'permisjon uten loenn', "
            "'pensjonsrettigheter', 'feriepenger'"
        ),
    )
    ongoing_salary: bool = Field(
        default=False, description="True if continued salary payments mentioned"
    )
    pension_rights: bool = Field(
        default=False, description="True if pension rights preserved"
    )


class FormerEmployerSection(BaseModel):
    """§5 — Avtaler med tidligere arbeidsgiver."""

    entries: list[FormerEmployer] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §5 entry")


# --- §8: Eiendom i naering (Business real estate) ---


class BusinessProperty(BaseModel):
    """Real estate used in business activity."""

    description: str = Field(description="Property description or address")
    municipality: str | None = Field(default=None, description="Kommune if stated")
    property_id: str | None = Field(
        default=None, description="Gnr/Bnr identifier if stated"
    )
    ownership_form: str | None = Field(
        default=None, description="Ownership form if stated: 'eier', 'deleier'"
    )
    nature: str | None = Field(
        default=None,
        description=(
            "Property type: 'landbrukseiendom', 'utleieleilighet', "
            "'naeringseiendom', 'tomt'"
        ),
    )
    via_company: str | None = Field(
        default=None,
        description="Holding company name if property held via entity",
    )


class BusinessPropertySection(BaseModel):
    """§8 — Fast eiendom i naeringsvirksomhet."""

    entries: list[BusinessProperty] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §8 entry")


# --- §9: Selskapsinteresser (Company interests / shareholdings) ---


class CompanyInterest(BaseModel):
    """A single shareholding or company interest."""

    company_name: str = Field(description="Company or security name")
    org_number: str | None = Field(default=None, description="Org number if stated")
    company_form: str | None = Field(
        default=None,
        description="Legal form: 'AS', 'ASA', 'ANS', 'DA', 'KS', 'PLC', etc.",
    )
    ownership_pct: str | None = Field(
        default=None,
        description=(
            "Ownership percentage as stated. Verbatim: '100%', 'under 1 pst.', "
            "'mindre enn 1 %', '50%', '0,03%'"
        ),
    )
    num_shares: int | None = Field(default=None, description="Number of shares if stated")
    is_indirect: bool = Field(
        default=False, description="True if held indirectly through another company"
    )
    parent_company: str | None = Field(
        default=None, description="Name of intermediary company if indirect"
    )
    description: str | None = Field(
        default=None,
        description="Additional context about the holding",
    )


class ShareTransaction(BaseModel):
    """A reported share transaction under §9 para 2."""

    date: str = Field(description="Transaction date as stated, e.g. '26.01.2026'")
    direction: str = Field(description="'Kjoep' (buy) or 'Salg' (sell)")
    company_name: str = Field(description="Security/company name")
    company_form: str | None = Field(default=None)
    ownership_pct: str | None = Field(default=None)
    num_shares: int | None = Field(default=None)
    value_nok: float | None = Field(default=None, description="Transaction value in NOK")


class CompanyInterestsSection(BaseModel):
    """§9 — Selskapsinteresser (aksjer, andeler m.m.)."""

    holdings: list[CompanyInterest] = Field(default_factory=list)
    transactions: list[ShareTransaction] = Field(default_factory=list)
    portfolio_date: str | None = Field(
        default=None,
        description="Portfolio valuation date if stated: 'per 30.11.2024'",
    )
    raw_text: str = Field(description="Full verbatim text of the §9 entry")


# --- §9a: Gjeld i naeringsvirksomhet (Business debt) ---


class BusinessDebt(BaseModel):
    """Debt in business exceeding 10G or guarantee exceeding 20G."""

    creditor: str | None = Field(default=None, description="Lender/creditor name")
    amount_nok: float | None = Field(default=None, description="Debt amount if stated")
    security: str | None = Field(
        default=None, description="Collateral description if stated"
    )
    description: str = Field(description="Full debt/guarantee description")


class BusinessDebtSection(BaseModel):
    """§9a — Gjeld i naeringsvirksomhet / garantiansvar."""

    entries: list[BusinessDebt] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §9a entry")


# --- Person entry ---


class PersonEntry(BaseModel):
    """One person's corporation-related register entry for a given publication date."""

    name: str = Field(description="Full name as printed: 'Etternavn, Fornavn'")
    party: str = Field(
        description="Party abbreviation: A, H, Sp, FrP, SV, V, R, MDG, KrF, Uav, PF",
    )
    district: str = Field(
        description="Electoral district (valgdistrikt): e.g. 'Oslo', 'Hordaland'"
    )
    no_interests: bool = Field(
        default=False,
        description="True if 'Har ingen registreringspliktige interesser'",
    )
    board_positions: BoardPositions | None = Field(default=None, description="§2")
    self_employment: SelfEmploymentSection | None = Field(default=None, description="§3")
    paid_employment: PaidEmploymentSection | None = Field(default=None, description="§4")
    former_employer: FormerEmployerSection | None = Field(default=None, description="§5")
    business_property: BusinessPropertySection | None = Field(default=None, description="§8")
    company_interests: CompanyInterestsSection | None = Field(default=None, description="§9")
    business_debt: BusinessDebtSection | None = Field(default=None, description="§9a")


# --- Org-centric extraction and matching ---


class OrgMention(BaseModel):
    """A single organisation reference extracted from a register entry.

    One person entry can produce multiple OrgMention records (e.g.
    Astrup's §2 yields 5 board positions, §9 yields 10+ holdings).
    """

    section: str = Field(description="Source section: '§2', '§3', '§4', '§5', '§8', '§9', '§9a'")
    organisation_name: str = Field(description="Organisation name as stated in text")
    org_number: str | None = Field(
        default=None, description="9-digit BRREG org number if found inline via regex"
    )
    company_form: str | None = Field(
        default=None,
        description="Legal form as stated: 'AS', 'ASA', 'ANS', 'DA', 'KS', 'ENK', 'PLC', 'AB', etc.",
    )
    role_claimed: str | None = Field(
        default=None,
        description=(
            "Role or relationship claimed: 'Styreleder', 'Styremedlem', "
            "'Daglig leder', 'eier', 'innehaver', 'aksjer'"
        ),
    )
    ownership_pct: str | None = Field(
        default=None, description="Ownership percentage verbatim: '100%', 'under 1 pst.'"
    )
    num_shares: int | None = Field(default=None, description="Share count if stated")
    compensated: bool | None = Field(default=None, description="§2 compensation flag")
    is_listed_minor_holding: bool = Field(
        default=False,
        description="True if ASA with <1% ownership — skip roller match",
    )
    is_foreign: bool = Field(
        default=False,
        description="True if non-Norwegian entity (PLC, AB, GmbH, KG, etc.)",
    )
    raw_text: str = Field(description="Verbatim section text this mention was extracted from")

    person_name: str = Field(description="Person name from register: 'Etternavn, Fornavn'")
    person_party: str = Field(description="Party abbreviation")
    person_district: str = Field(description="Electoral district")
    publication_date: str = Field(description="Publication date ISO YYYY-MM-DD")


class OrgResolution(BaseModel):
    """Result of resolving an organisation name to a BRREG org number."""

    organisation_name_normalised: str = Field(description="Uppercased, stripped form")
    org_number: str | None = Field(default=None, description="Resolved 9-digit org number")
    brreg_name: str | None = Field(
        default=None, description="Official name from BRREG if resolved"
    )
    confidence: str = Field(
        description="'confirmed' (exact match or inline), 'candidate' (fuzzy), 'not_found', 'foreign'"
    )
    resolution_method: str | None = Field(
        default=None,
        description="'inline', 'exact_name', 'fuzzy_name', 'manual'",
    )


class RollerMatch(BaseModel):
    """Result of matching a register person-org pair against BRREG roller.

    The roller API returns current state only. For older PDFs the match
    may fail due to role turnover — flagged via match_confidence.
    """

    org_number: str = Field(description="Resolved BRREG org number")
    org_resolution_confidence: str = Field(description="From OrgResolution.confidence")

    roller_person_fornavn: str | None = Field(default=None)
    roller_person_mellomnavn: str | None = Field(default=None)
    roller_person_etternavn: str | None = Field(default=None)
    roller_person_dob: str | None = Field(
        default=None, description="fodselsdato from roller API: 'YYYY-MM-DD'"
    )
    roller_role_code: str | None = Field(
        default=None,
        description="BRREG rolle code: LEDE, MEDL, NEST, VARA, DAGL, INNH, DTSO, DTPR, KOMP",
    )
    roller_role_description: str | None = Field(default=None)
    roller_fratraadt: bool | None = Field(
        default=None, description="True if resigned in BRREG"
    )
    roller_snapshot_date: str = Field(
        description="Date the roller data was fetched: ISO YYYY-MM-DD"
    )

    match_method: str | None = Field(
        default=None, description="'dob_exact', 'name_exact', 'name_fuzzy'"
    )
    match_confidence: str = Field(
        description=(
            "'confirmed' (DOB+name match), 'candidate' (name-only fuzzy), "
            "'no_match' (current PDF, person not in roller), "
            "'historic_no_match' (old PDF, likely role turnover)"
        ),
    )
    role_consistent: bool | None = Field(
        default=None,
        description="True if claimed role (e.g. Styreleder) matches roller role code (LEDE)",
    )

    person_name: str = Field(description="Person name from register")
    person_dob_from_population: str | None = Field(
        default=None, description="foedselsdato from stortinget API population JSON"
    )
    publication_date: str = Field(description="Publication date of the source PDF")


# --- Document-level ---


class RegisterPublication(BaseModel):
    """One complete publication of the economic interests register."""

    publication_date: str = Field(
        description="Date from cover page: ISO format YYYY-MM-DD"
    )
    publication_date_raw: str = Field(
        description="Date as printed on cover page: 'Ajourfoert pr. 3. januar 2025'"
    )
    person_count: int = Field(description="Total number of person entries extracted")
    persons: list[PersonEntry] = Field(description="All person entries in document order")
