"""Pure minute-boundary scenario funding; no venue payment or filesystem authority."""

import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, fields
from decimal import Decimal

from omega.application.margin_execution import (
    _digest,
    account_prefix_checksum,
    advance_prefix_checksum,
)
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import (
    FUNDING_QUANTUM,
    MAX_POSITIONS,
    FundingCashflow,
    MarginAccount,
    _instrument,
    _time,
    apply_funding,
    funding_amount,
)
from omega.domain.models import Instrument, Market, decimal

MODEL = "linear_usdt_scenario_rate_v1"
MAX_POINTS = 10000
POLICY_SHA256 = _digest(
    (MODEL, FUNDING_QUANTUM, "ROUND_HALF_EVEN", "previous_close_funding_current_open")
)


def _money(value):
    if type(value) is not str or not 1 <= len(value) <= 1024:
        raise ValidationError("funding amounts require bounded Decimal strings")
    return decimal(value)


def _parse_instrument(value):
    if type(value) is not str:
        raise ValidationError("funding instrument must be a native key string")
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (ValueError, TypeError):
        raise ValidationError("invalid funding instrument key") from None
    _instrument(result)
    return result


@dataclass(frozen=True, slots=True)
class FundingPoint:
    instrument: Instrument
    event_time_ms: int
    source_available_at_ms: int
    reference_time_ms: int
    rate: Decimal
    reference_price: Decimal

    def __post_init__(self):
        _instrument(self.instrument)
        for value in (self.event_time_ms, self.source_available_at_ms, self.reference_time_ms):
            _time(value)
        _time(self.event_time_ms + 60000)
        if (
            self.event_time_ms % 60000
            or self.reference_time_ms != self.event_time_ms
            or not self.event_time_ms <= self.source_available_at_ms <= self.event_time_ms + 60000
        ):
            raise ValidationError("funding source must be usable within its settlement minute")
        funding_amount(Decimal(0), self.reference_price, self.rate)

    def document(self):
        return {
            "instrument": self.instrument.key,
            "event_time_ms": self.event_time_ms,
            "source_available_at_ms": self.source_available_at_ms,
            "reference_time_ms": self.reference_time_ms,
            "rate": str(self.rate),
            "reference_price": str(self.reference_price),
        }

    @property
    def source_sha256(self):
        # The exact normalized scenario record, not a claim of raw venue provenance.
        return _digest(("scenario_rate", self.document()))

    @classmethod
    def parse(cls, value):
        if type(value) is not dict or set(value) != {item.name for item in fields(cls)}:
            raise ValidationError("invalid scenario funding point fields")
        return cls(
            _parse_instrument(value["instrument"]),
            value["event_time_ms"],
            value["source_available_at_ms"],
            value["reference_time_ms"],
            _money(value["rate"]),
            _money(value["reference_price"]),
        )


@dataclass(frozen=True, slots=True)
class FundingSchedule:
    points: tuple[FundingPoint, ...]

    def __post_init__(self):
        if (
            type(self.points) is not tuple
            or not 1 <= len(self.points) <= MAX_POINTS
            or any(not isinstance(point, FundingPoint) for point in self.points)
        ):
            raise ValidationError("funding needs a bounded nonempty immutable schedule")
        keys = [(point.event_time_ms, point.instrument.key) for point in self.points]
        if keys != sorted(keys) or len(set(keys)) != len(keys):
            raise ValidationError("funding schedule must have unique ordered settlements")

    def document(self):
        return {"schema_version": 1, "model": MODEL, "points": [p.document() for p in self.points]}

    @property
    def sha256(self):
        return _digest(self.document())

    def at(self, settlement_time_ms):
        _time(settlement_time_ms)
        if settlement_time_ms % 60000:
            raise ValidationError("funding lookup requires an exact minute boundary")

        def key(point):
            return point.event_time_ms

        return self.points[
            bisect_left(self.points, settlement_time_ms, key=key) : bisect_right(
                self.points, settlement_time_ms, key=key
            )
        ]

    def validate_for(self, instruments, start_ms):
        _time(start_ms)
        if any(
            point.instrument not in instruments or point.event_time_ms < start_ms
            for point in self.points
        ):
            raise ValidationError("funding schedule conflicts with session instruments/start")

    @classmethod
    def parse(cls, value):
        if (
            type(value) is not dict
            or set(value) != {"schema_version", "model", "points"}
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or value["model"] != MODEL
            or type(value["points"]) is not list
            or not 1 <= len(value["points"]) <= MAX_POINTS
        ):
            raise ValidationError("invalid versioned scenario funding schedule")
        return cls(tuple(FundingPoint.parse(point) for point in value["points"]))


@dataclass(frozen=True, slots=True)
class PreparedFundingBatch:
    batch_id: str
    checksum: str
    settlement_time_ms: int
    as_of_ms: int
    points: tuple[FundingPoint, ...]
    facts: tuple[FundingCashflow, ...]
    prefix_accounts: tuple[MarginAccount, ...]
    prefix_checksums: tuple[str, ...]
    model: str = MODEL
    policy_sha256: str = POLICY_SHA256

    def __post_init__(self):
        _time(self.settlement_time_ms)
        _time(self.as_of_ms)
        if (
            self.model != MODEL
            or self.policy_sha256 != POLICY_SHA256
            or self.settlement_time_ms % 60000
            or type(self.points) is not tuple
            or not 1 <= len(self.points) <= MAX_POSITIONS
            or any(not isinstance(point, FundingPoint) for point in self.points)
            or type(self.facts) is not tuple
            or len(self.facts) != len(self.points)
            or any(not isinstance(fact, FundingCashflow) for fact in self.facts)
            or type(self.prefix_accounts) is not tuple
            or len(self.prefix_accounts) != len(self.facts) + 1
            or any(not isinstance(account, MarginAccount) for account in self.prefix_accounts)
            or type(self.prefix_checksums) is not tuple
            or len(self.prefix_checksums) != len(self.prefix_accounts)
            or any(
                type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None
                for value in (self.batch_id, self.checksum, *self.prefix_checksums)
            )
        ):
            raise ValidationError("invalid typed scenario funding batch")

    @property
    def candidate_account(self):
        return self.prefix_accounts[-1]


def funding_fact_document(fact):
    if not isinstance(fact, FundingCashflow):
        raise ValidationError("typed funding fact required")
    return {
        item.name: value.key
        if isinstance(value := getattr(fact, item.name), Instrument)
        else str(value)
        if isinstance(value, Decimal)
        else value
        for item in fields(fact)
    }


def funding_fact_from_document(value):
    if type(value) is not dict or set(value) != {item.name for item in fields(FundingCashflow)}:
        raise ValidationError("invalid funding fact fields")
    result = dict(value)
    result["instrument"] = _parse_instrument(result["instrument"])
    for key in ("amount", "rate", "reference_price", "signed_quantity", "quantum"):
        result[key] = _money(result[key])
    return FundingCashflow(**result)


def _identity(account, point):
    owner = next((p for p in account.positions if p.instrument == point.instrument), None)
    if owner is None:
        raise ProtocolError("funding settlement has no configured position owner")
    return owner, "funding:" + _digest(
        (account.account_id, point.instrument.key, owner.owner_id, point.event_time_ms, "cycle1")
    )


def _assemble(account, points, facts, as_of_ms):
    if (
        not isinstance(account, MarginAccount)
        or type(points) is not tuple
        or not 1 <= len(points) <= MAX_POSITIONS
        or any(not isinstance(point, FundingPoint) for point in points)
        or type(facts) is not tuple
        or len(facts) != len(points)
        or any(not isinstance(fact, FundingCashflow) for fact in facts)
    ):
        raise ValidationError("invalid bounded funding batch inputs")
    _time(as_of_ms)
    if any(entry.fact.available_at_ms > as_of_ms for entry in account.ledger):
        raise ProtocolError("funding baseline contains future account facts")
    settlement = points[0].event_time_ms
    keys = [point.instrument.key for point in points]
    if (
        any(point.event_time_ms != settlement for point in points)
        or keys != sorted(keys)
        or len(set(keys)) != len(keys)
        or as_of_ms < settlement + 60000
    ):
        raise ValidationError("funding batch must settle one ordered closed minute")
    prefixes = [account]
    checksums = [account_prefix_checksum(account)]
    for point, fact in zip(points, facts, strict=True):
        owner, identity = _identity(account, point)
        if (
            fact.funding_id != identity
            or fact.instrument != point.instrument
            or fact.owner_id != owner.owner_id
            or fact.rate != point.rate
            or fact.reference_price != point.reference_price
            or fact.event_time_ms != point.event_time_ms
            or fact.source_available_at_ms != point.source_available_at_ms
            or fact.available_at_ms != as_of_ms
            or fact.source_sha256 != point.source_sha256
            or fact.policy_sha256 != POLICY_SHA256
        ):
            raise ProtocolError("frozen funding fact differs from its original scenario source")
        transition = apply_funding(prefixes[-1], fact)
        if transition.entry is None:
            raise ProtocolError("already settled funding requires its original committed batch")
        prefixes.append(transition.account)
        checksums.append(advance_prefix_checksum(checksums[-1], transition.account))
    identity = _digest((MODEL, account.account_id, settlement, tuple(keys)))
    checksum = _digest(
        (MODEL, POLICY_SHA256, identity, settlement, as_of_ms, points, facts, checksums)
    )
    return PreparedFundingBatch(
        identity, checksum, settlement, as_of_ms, points, facts, tuple(prefixes), tuple(checksums)
    )


def prepare_funding_batch(account, points, *, as_of_ms):
    if not isinstance(account, MarginAccount) or type(points) is not tuple:
        raise ValidationError("funding preparation needs a typed account and immutable points")
    if not 1 <= len(points) <= MAX_POSITIONS or any(
        not isinstance(point, FundingPoint) for point in points
    ):
        raise ValidationError("funding preparation needs bounded typed points")
    facts = []
    for point in points:
        owner, identity = _identity(account, point)
        facts.append(
            FundingCashflow(
                funding_id=identity,
                instrument=point.instrument,
                owner_id=owner.owner_id,
                amount=funding_amount(owner.quantity, point.reference_price, point.rate),
                rate=point.rate,
                reference_price=point.reference_price,
                signed_quantity=owner.quantity,
                event_time_ms=point.event_time_ms,
                source_available_at_ms=point.source_available_at_ms,
                available_at_ms=as_of_ms,
                source_kind="scenario_rate",
                source_sha256=point.source_sha256,
                policy_sha256=POLICY_SHA256,
            )
        )
    return _assemble(account, points, tuple(facts), as_of_ms)


def validate_funding_batch(plan, expected):
    if not isinstance(plan, PreparedFundingBatch):
        raise ValidationError("typed prepared funding batch required")
    rebuilt = _assemble(expected, plan.points, plan.facts, plan.as_of_ms)
    if rebuilt != plan:
        raise ProtocolError("funding batch differs from its exact original account prefixes")
    return rebuilt.candidate_account


def frozen_funding_document(plan):
    if not isinstance(plan, PreparedFundingBatch):
        raise ValidationError("typed prepared funding batch required")
    validate_funding_batch(plan, plan.prefix_accounts[0])
    return {
        "schema_version": 1,
        "model": MODEL,
        "policy_sha256": POLICY_SHA256,
        "batch_id": plan.batch_id,
        "checksum": plan.checksum,
        "settlement_time_ms": plan.settlement_time_ms,
        "as_of_ms": plan.as_of_ms,
        "baseline_sequence": len(plan.prefix_accounts[0].ledger),
        "baseline_sha256": plan.prefix_checksums[0],
        "points": [point.document() for point in plan.points],
        "facts": [funding_fact_document(fact) for fact in plan.facts],
        "prefix_checksums": list(plan.prefix_checksums),
    }


def restore_funding_batch(document, baseline):
    keys = {
        "schema_version",
        "model",
        "policy_sha256",
        "batch_id",
        "checksum",
        "settlement_time_ms",
        "as_of_ms",
        "baseline_sequence",
        "baseline_sha256",
        "points",
        "facts",
        "prefix_checksums",
    }
    if (
        not isinstance(baseline, MarginAccount)
        or type(document) is not dict
        or set(document) != keys
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["model"] != MODEL
        or document["policy_sha256"] != POLICY_SHA256
        or type(document["baseline_sequence"]) is not int
        or document["baseline_sequence"] != len(baseline.ledger)
        or document["baseline_sha256"] != account_prefix_checksum(baseline)
        or type(document["points"]) is not list
        or not 1 <= len(document["points"]) <= MAX_POSITIONS
        or type(document["facts"]) is not list
        or len(document["facts"]) != len(document["points"])
    ):
        raise ProtocolError("frozen funding document conflicts with its original baseline")
    _time(document["settlement_time_ms"])
    plan = _assemble(
        baseline,
        tuple(FundingPoint.parse(point) for point in document["points"]),
        tuple(funding_fact_from_document(fact) for fact in document["facts"]),
        document["as_of_ms"],
    )
    if frozen_funding_document(plan) != document:
        raise ProtocolError("frozen funding document was changed or is not canonical")
    return plan
