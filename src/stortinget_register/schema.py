"""Pydantic extraction schema for Stortinget economic interests register.

Derived from structural analysis of 89 PDFs (Oct 2022 → Feb 2026, 180–306
persons per publication). The register is governed by Stortingets
forretningsorden §76 and the register regulation §§1–16.

Section types observed across the full corpus:

    §2   Styreverv mv.                 19,560 occurrences
    §3   Selvstendig næring             6,049
    §4   Lønnet stilling m.v.           7,447
    §5   Tidligere arbeidsgiver         6,049
    §6   Framtidig arbeidsgiver         1,422
    §7   Økonomisk støtte                 882
    §8   Eiendom i næring               1,989
    §9   Selskapsinteresser             8,970
    §9a  Gjeld i næringsvirksomhet        271
    §10  Utenlandsreiser                2,048
    §11  Gaver                          2,721
    §15  Andre forhold                  1,066

Content is free-text with varying structure. Sub-models extract
recognizable patterns while preserving raw text for unstructured
portions.
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
            "'Kommunestyrerepresentant', 'Ordfører'"
        ),
    )
    compensated: bool | None = Field(
        default=None,
        description=(
            "Whether compensation is received. True if 'lønnet', 'godtgjørelse', "
            "'honorar'. False if 'ulønnet', 'ikke lønnet', 'ikke godtgjørelse'. "
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


# --- §3: Selvstendig næring (Self-employed / independent business) ---


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


# --- §4: Lønnet stilling m.v. (Paid employment) ---


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
    """§4 — Lønnet stilling eller engasjement."""

    entries: list[PaidEmployment] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §4 entry")


# --- §5: Tidligere arbeidsgiver (Former employer arrangements) ---


class FormerEmployer(BaseModel):
    """Agreement or arrangement with a former employer."""

    employer: str = Field(description="Former employer name")
    arrangement: str = Field(
        description=(
            "Nature of arrangement: e.g. 'permisjon uten lønn', "
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


# --- §6: Framtidig arbeidsgiver (Future employer) ---


class FutureEmployer(BaseModel):
    """Agreement with a future employer or engagement."""

    employer: str = Field(description="Future employer or engagement partner")
    description: str = Field(description="Nature of the future arrangement")


class FutureEmployerSection(BaseModel):
    """§6 — Avtaler med fremtidige arbeidsgivere."""

    entries: list[FutureEmployer] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §6 entry")


# --- §7: Økonomisk støtte (Financial support) ---


class FinancialSupport(BaseModel):
    """Financial support from external sources."""

    source: str = Field(description="Name of supporting entity or person")
    description: str = Field(description="Nature of support")
    amount_nok: float | None = Field(
        default=None, description="Amount if >50k NOK threshold triggered"
    )
    exceeds_threshold: bool = Field(
        default=False,
        description="True if support from same source exceeds 50k NOK per calendar year",
    )


class FinancialSupportSection(BaseModel):
    """§7 — Økonomisk støtte eller vederlag."""

    entries: list[FinancialSupport] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §7 entry")


# --- §8: Eiendom i næring (Business real estate) ---


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
            "'næringseiendom', 'tomt'"
        ),
    )


class BusinessPropertySection(BaseModel):
    """§8 — Fast eiendom i næringsvirksomhet."""

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
    direction: str = Field(description="'Kjøp' (buy) or 'Salg' (sell)")
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


# --- §9a: Gjeld i næringsvirksomhet (Business debt) ---


class BusinessDebt(BaseModel):
    """Debt in business exceeding 10G or guarantee exceeding 20G."""

    creditor: str | None = Field(default=None, description="Lender/creditor name")
    amount_nok: float | None = Field(default=None, description="Debt amount if stated")
    security: str | None = Field(
        default=None, description="Collateral description if stated"
    )
    description: str = Field(description="Full debt/guarantee description")


class BusinessDebtSection(BaseModel):
    """§9a — Gjeld i næringsvirksomhet / garantiansvar."""

    entries: list[BusinessDebt] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §9a entry")


# --- §10: Utenlandsreiser (Foreign travel) ---


class ForeignTravel(BaseModel):
    """A foreign trip where costs were externally covered."""

    date_range: str = Field(description="Trip dates as stated")
    destination: str = Field(description="Country or city visited")
    sponsor: str = Field(description="Who covered the expenses")
    purpose: str | None = Field(default=None, description="Trip purpose if stated")


class ForeignTravelSection(BaseModel):
    """§10 — Utenlandsreiser med ekstern finansiering."""

    trips: list[ForeignTravel] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §10 entry")


# --- §11: Gaver (Gifts) ---


class Gift(BaseModel):
    """A gift or economic benefit exceeding 2,000 NOK."""

    description: str = Field(description="What was given")
    giver: str = Field(description="Who gave it")
    date: str | None = Field(default=None, description="Date received if stated")
    value_nok: float | None = Field(default=None, description="Value in NOK if stated")


class GiftSection(BaseModel):
    """§11 — Gaver eller økonomiske fordeler >2000 NOK."""

    gifts: list[Gift] = Field(default_factory=list)
    raw_text: str = Field(description="Full verbatim text of the §11 entry")


# --- §15: Andre forhold (Other matters) ---


class OtherMattersSection(BaseModel):
    """§15 — Andre forhold av berettiget interesse for allmennheten."""

    raw_text: str = Field(description="Full verbatim text of the §15 entry")
    summary: str | None = Field(
        default=None, description="Brief characterization of the disclosure type"
    )


# --- Person entry ---


class PersonEntry(BaseModel):
    """One person's complete register entry for a given publication date."""

    name: str = Field(description="Full name as printed: 'Etternavn, Fornavn'")
    party: str = Field(
        description=(
            "Party abbreviation: A, H, Sp, FrP, SV, V, R, MDG, KrF, Uav, PF"
        ),
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
    future_employer: FutureEmployerSection | None = Field(default=None, description="§6")
    financial_support: FinancialSupportSection | None = Field(
        default=None, description="§7"
    )
    business_property: BusinessPropertySection | None = Field(
        default=None, description="§8"
    )
    company_interests: CompanyInterestsSection | None = Field(
        default=None, description="§9"
    )
    business_debt: BusinessDebtSection | None = Field(default=None, description="§9a")
    foreign_travel: ForeignTravelSection | None = Field(default=None, description="§10")
    gifts: GiftSection | None = Field(default=None, description="§11")
    other_matters: OtherMattersSection | None = Field(default=None, description="§15")


# --- Document-level ---


class RegisterPublication(BaseModel):
    """One complete publication of the economic interests register."""

    publication_date: str = Field(
        description="Date from cover page: ISO format YYYY-MM-DD"
    )
    publication_date_raw: str = Field(
        description="Date as printed on cover page: 'Ajourført pr. 3. januar 2025'"
    )
    person_count: int = Field(description="Total number of person entries extracted")
    persons: list[PersonEntry] = Field(description="All person entries in document order")
