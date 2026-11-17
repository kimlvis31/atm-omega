"""Immutable provider-neutral static order rules and explicit reference provenance."""

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market, require_decimal

ZERO = Decimal(0)
MAX_TIME = 253_402_300_799_000
REFERENCE_KINDS = frozenset({"last_trade", "provider_average", "mark_price"})


def bounded_decimal(value):
    require_decimal(value)
    parts = value.as_tuple()
    if len(parts.digits) > 80 or not -60 <= parts.exponent <= 60:
        raise ValidationError("execution rule decimal exceeds its exact calculation bound")


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid execution reference observation time")


def _source(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_:/.-]{1,160}", value) is None:
        raise ValidationError("invalid public execution reference source")


@dataclass(frozen=True, slots=True)
class DecimalRange:
    minimum: Decimal = ZERO
    maximum: Decimal = ZERO
    step: Decimal = ZERO
    grid_origin: Decimal = ZERO

    def __post_init__(self):
        for value in (self.minimum, self.maximum, self.step, self.grid_origin):
            bounded_decimal(value)
            if value < 0:
                raise ValidationError("negative execution range")
        if self.maximum and self.minimum > self.maximum:
            raise ValidationError("execution range minimum exceeds maximum")

    def document(self):
        return {
            name: str(getattr(self, name)) for name in ("minimum", "maximum", "step", "grid_origin")
        }


@dataclass(frozen=True, slots=True)
class NotionalRule:
    minimum: Decimal = ZERO
    maximum: Decimal = ZERO
    apply_min_to_market: bool = False
    apply_max_to_market: bool = False
    reference_kind: str = "last_trade"
    reference_window_ms: int = 0
    minimum_reduction_exempt: bool = False

    def __post_init__(self):
        for value in (self.minimum, self.maximum):
            bounded_decimal(value)
            if value < 0:
                raise ValidationError("negative execution notional range")
        if (
            self.maximum
            and self.minimum > self.maximum
            or any(
                type(value) is not bool
                for value in (
                    self.apply_min_to_market,
                    self.apply_max_to_market,
                    self.minimum_reduction_exempt,
                )
            )
            or type(self.reference_kind) is not str
            or self.reference_kind not in REFERENCE_KINDS
            or type(self.reference_window_ms) is not int
            or not 0 <= self.reference_window_ms <= 86400000
            or self.reference_kind != "provider_average"
            and self.reference_window_ms != 0
        ):
            raise ValidationError("invalid notional applicability or reference policy")

    def document(self):
        return {
            "minimum": str(self.minimum),
            "maximum": str(self.maximum),
            "apply_min_to_market": self.apply_min_to_market,
            "apply_max_to_market": self.apply_max_to_market,
            "reference_kind": self.reference_kind,
            "reference_window_ms": self.reference_window_ms,
            "minimum_reduction_exempt": self.minimum_reduction_exempt,
        }


@dataclass(frozen=True, slots=True)
class NotionalReference:
    instrument: Instrument
    price: Decimal
    kind: str
    window_ms: int
    observed_at_ms: int
    source: str

    def __post_init__(self):
        bounded_decimal(self.price)
        _time(self.observed_at_ms)
        _source(self.source)
        if (
            not isinstance(self.instrument, Instrument)
            or self.price <= 0
            or type(self.kind) is not str
            or self.kind not in REFERENCE_KINDS
            or type(self.window_ms) is not int
            or not 0 <= self.window_ms <= 86400000
            or self.kind != "provider_average"
            and self.window_ms != 0
        ):
            raise ValidationError("invalid provider notional reference")


@dataclass(frozen=True, slots=True)
class ExecutionRulesSnapshot:
    instrument: Instrument
    observed_at_ms: int
    source: str
    status: str
    quantity: DecimalRange
    market_quantity: DecimalRange | None
    price: DecimalRange
    notionals: tuple[NotionalRule, ...] = ()
    unsupported_filters: tuple[str, ...] = ()
    reductions_allowed: bool = False
    schema_version: int = 1

    def __post_init__(self):
        _time(self.observed_at_ms)
        _source(self.source)
        if (
            not isinstance(self.instrument, Instrument)
            or self.instrument.market not in {Market.SPOT, Market.USDM}
            or type(self.status) is not str
            or re.fullmatch("[A-Z][A-Z0-9_]{0,47}", self.status) is None
            or not isinstance(self.quantity, DecimalRange)
            or not isinstance(self.price, DecimalRange)
            or self.market_quantity is not None
            and not isinstance(self.market_quantity, DecimalRange)
            or type(self.notionals) is not tuple
            or len(self.notionals) > 8
            or any(not isinstance(rule, NotionalRule) for rule in self.notionals)
            or type(self.unsupported_filters) is not tuple
            or len(self.unsupported_filters) > 64
            or any(
                type(name) is not str or re.fullmatch("[A-Z][A-Z0-9_]{0,63}", name) is None
                for name in self.unsupported_filters
            )
            or tuple(sorted(set(self.unsupported_filters))) != self.unsupported_filters
            or type(self.reductions_allowed) is not bool
            or type(self.schema_version) is not int
            or self.schema_version != 1
        ):
            raise ValidationError("invalid normalized execution rules snapshot")

    def _content(self):
        return {
            "schema_version": self.schema_version,
            "instrument": self.instrument.key,
            "source": self.source,
            "status": self.status,
            "quantity": self.quantity.document(),
            "market_quantity": self.market_quantity.document() if self.market_quantity else None,
            "price": self.price.document(),
            "notionals": [rule.document() for rule in self.notionals],
            "unsupported_filters": list(self.unsupported_filters),
            "reductions_allowed": self.reductions_allowed,
        }

    @property
    def content_hash(self):
        return sha256(
            json.dumps(
                self._content(), sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()

    def document(self):
        return {
            **self._content(),
            "observed_at_ms": self.observed_at_ms,
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_document(cls, value):
        """Strict detached normalized schema, never a raw provider filter decoder."""
        expected = {
            "schema_version",
            "instrument",
            "source",
            "status",
            "quantity",
            "market_quantity",
            "price",
            "notionals",
            "unsupported_filters",
            "reductions_allowed",
            "observed_at_ms",
            "content_hash",
        }

        def amount(raw):
            if type(raw) is not str or not 1 <= len(raw) <= 160:
                raise ValidationError("execution document requires decimal strings")
            try:
                result = Decimal(raw)
            except InvalidOperation:
                raise ValidationError("invalid execution document decimal") from None
            bounded_decimal(result)
            if str(result) != raw:
                raise ValidationError("noncanonical execution document decimal")
            return result

        def range_value(raw):
            if type(raw) is not dict or set(raw) != {"minimum", "maximum", "step", "grid_origin"}:
                raise ValidationError("invalid execution range document")
            return DecimalRange(
                *(amount(raw[name]) for name in ("minimum", "maximum", "step", "grid_origin"))
            )

        if (
            type(value) is not dict
            or set(value) != expected
            or type(value["instrument"]) is not str
            or type(value["content_hash"]) is not str
            or re.fullmatch("[a-f0-9]{64}", value["content_hash"]) is None
            or type(value["notionals"]) is not list
            or len(value["notionals"]) > 8
            or type(value["unsupported_filters"]) is not list
            or len(value["unsupported_filters"]) > 64
        ):
            raise ValidationError("invalid normalized execution document fields")
        try:
            provider, market, symbol = value["instrument"].split(":")
            instrument = Instrument(provider, Market(market), symbol)
            notionals = []
            names = {
                "minimum",
                "maximum",
                "apply_min_to_market",
                "apply_max_to_market",
                "reference_kind",
                "reference_window_ms",
                "minimum_reduction_exempt",
            }
            for rule in value["notionals"]:
                if type(rule) is not dict or set(rule) != names:
                    raise ValidationError("invalid notional document")
                notionals.append(
                    NotionalRule(
                        amount(rule["minimum"]),
                        amount(rule["maximum"]),
                        rule["apply_min_to_market"],
                        rule["apply_max_to_market"],
                        rule["reference_kind"],
                        rule["reference_window_ms"],
                        rule["minimum_reduction_exempt"],
                    )
                )
            result = cls(
                instrument,
                value["observed_at_ms"],
                value["source"],
                value["status"],
                range_value(value["quantity"]),
                range_value(value["market_quantity"])
                if value["market_quantity"] is not None
                else None,
                range_value(value["price"]),
                tuple(notionals),
                tuple(value["unsupported_filters"]),
                value["reductions_allowed"],
                value["schema_version"],
            )
        except (ValueError, TypeError, KeyError):
            raise ValidationError("invalid normalized execution document values") from None
        if result.document() != value:
            raise ValidationError("execution document checksum or canonical representation changed")
        return result
