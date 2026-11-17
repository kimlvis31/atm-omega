"""Read-only endpoint inventories; neither atomic account truth nor trading authority."""

import re
from dataclasses import dataclass, replace
from decimal import Decimal
from urllib.parse import urlsplit

from omega.domain.errors import ValidationError
from omega.domain.execution_rules import MAX_TIME, bounded_decimal
from omega.domain.models import Instrument, Market, OrderObservation, OrderStatus, Side

MAX_INVENTORY_ROWS = 1000


def _text(value, maximum=128):
    if type(value) is not str or not 1 <= len(value) <= maximum or not value.isprintable():
        raise ValidationError("inventory identity requires bounded native text")


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("inventory timestamp requires bounded native milliseconds")


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValidationError("inventory provenance requires a SHA256")


def _instrument(value):
    if type(value) is not Instrument or type(value.market) is not Market:
        raise ValidationError("inventory requires a native instrument/market")
    replace(value)


@dataclass(frozen=True, slots=True)
class InventoryProvenance:
    account_id: str
    market: Market
    origin: str
    endpoint: str
    requested_at_ms: int
    received_at_ms: int
    completed_at_ms: int
    response_sha256: str
    provider: str
    endpoint_kind: str

    def __post_init__(self):
        _text(self.account_id)
        if type(self.market) is not Market:
            raise ValidationError("unsupported inventory market")
        _text(self.provider, 32)
        if type(self.endpoint_kind) is not str or self.endpoint_kind not in (
            "ordinary",
            "conditional",
        ):
            raise ValidationError("inventory requires an explicit endpoint kind")
        _text(self.origin, 1024)
        _text(self.endpoint, 256)
        try:
            origin, path = urlsplit(self.origin), urlsplit(self.endpoint)
            valid = (
                origin.scheme == "https"
                and origin.hostname
                and origin.username is None
                and origin.password is None
                and origin.path in ("", "/")
                and not origin.query
                and not origin.fragment
                and (origin.port is None or 1 <= origin.port <= 65535)
                and not any(c.isspace() or c in "?#" for c in self.origin)
                and self.endpoint.startswith("/")
                and not path.scheme
                and not path.netloc
                and path.path == self.endpoint
                and not path.query
                and not path.fragment
                and not any(c.isspace() or c in "?#" for c in self.endpoint)
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValidationError("inventory requires a bounded HTTPS origin and endpoint path")
        for value in (self.requested_at_ms, self.received_at_ms, self.completed_at_ms):
            _time(value)
        if not self.requested_at_ms <= self.received_at_ms <= self.completed_at_ms:
            raise ValidationError("inventory receipt interval is unavailable or reversed")
        _digest(self.response_sha256)


@dataclass(frozen=True, slots=True)
class OpenOrder:
    observation: OrderObservation
    side: Side
    order_type: str  # Native venue type, never coerced into an executable OrderKind.
    original_quantity: Decimal
    price: Decimal
    created_at_ms: int
    updated_at_ms: int
    reduce_only: bool | None
    position_side: str | None
    close_position: bool | None
    source_row_index: int

    def __post_init__(self):
        if type(self.observation) is not OrderObservation or type(self.side) is not Side:
            raise ValidationError("inventory order requires native read-only observation/side")
        observation = replace(self.observation)
        _instrument(observation.instrument)
        _text(observation.client_order_id)
        _text(observation.provider_order_id)
        _text(self.order_type, 64)
        for value in (
            self.original_quantity,
            self.price,
            observation.executed_quantity,
            observation.cumulative_quote,
        ):
            if type(value) is not Decimal:
                raise ValidationError("inventory amounts require native Decimals")
            bounded_decimal(value)
        if (
            self.original_quantity < 0
            or self.price < 0
            or observation.executed_quantity > self.original_quantity
            or self.original_quantity == 0
            and self.close_position is not True
            or observation.status
            not in {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING_CANCEL}
            or (observation.executed_quantity == 0) != (observation.cumulative_quote == 0)
            or observation.status is OrderStatus.NEW
            and observation.executed_quantity != 0
            or observation.status is OrderStatus.PARTIALLY_FILLED
            and not 0 < observation.executed_quantity < self.original_quantity
        ):
            raise ValidationError("inventory order amounts/status are inconsistent")
        if any(
            value is not None and type(value) is not bool
            for value in (self.reduce_only, self.close_position)
        ):
            raise ValidationError("inventory controls require native booleans when reported")
        if self.position_side is not None:
            _text(self.position_side, 64)
        _time(self.created_at_ms)
        _time(self.updated_at_ms)
        if self.updated_at_ms < self.created_at_ms:
            raise ValidationError("inventory order timestamps regressed")
        if (
            type(self.source_row_index) is not int
            or not 0 <= self.source_row_index < MAX_INVENTORY_ROWS
        ):
            raise ValidationError("inventory source row index is invalid")


@dataclass(frozen=True, slots=True)
class ExternalConditionalOrder:
    instrument: Instrument
    algo_id: str
    client_algo_id: str
    algo_type: str
    order_type: str
    native_status: str
    source_row_index: int

    def __post_init__(self):
        _instrument(self.instrument)
        for value in (
            self.algo_id,
            self.client_algo_id,
            self.algo_type,
            self.order_type,
            self.native_status,
        ):
            _text(value)
        if (
            type(self.source_row_index) is not int
            or not 0 <= self.source_row_index < MAX_INVENTORY_ROWS
        ):
            raise ValidationError("inventory source row index is invalid")


@dataclass(frozen=True, slots=True)
class OpenOrderInventory:
    provenance: InventoryProvenance
    orders: tuple[OpenOrder, ...]

    def __post_init__(self):
        _inventory(self.provenance, self.orders, OpenOrder)
        if self.provenance.endpoint_kind != "ordinary":
            raise ValidationError("conditional source cannot masquerade as ordinary inventory")

    @property
    def scope(self):
        return "all_symbols_open_orders_endpoint_only"


@dataclass(frozen=True, slots=True)
class ConditionalOrderInventory:
    provenance: InventoryProvenance
    orders: tuple[ExternalConditionalOrder, ...]

    def __post_init__(self):
        _inventory(self.provenance, self.orders, ExternalConditionalOrder)
        if self.provenance.endpoint_kind != "conditional":
            raise ValidationError("external algo inventory requires its separate endpoint")

    @property
    def scope(self):
        return "all_symbols_external_conditional_endpoint_only"


def _inventory(provenance, rows, row_type):
    if type(provenance) is not InventoryProvenance:
        raise ValidationError("inventory requires actual response provenance")
    replace(provenance)
    if type(rows) is not tuple or len(rows) > MAX_INVENTORY_ROWS:
        raise ValidationError("inventory exceeds its bounded row capacity")
    provider_ids, client_ids = set(), set()
    for index, row in enumerate(rows):
        if type(row) is not row_type:
            raise ValidationError("inventory row has another schema")
        replace(row)
        if row.source_row_index != index:
            raise ValidationError("inventory reordered or omitted original source rows")
        instrument = row.observation.instrument if row_type is OpenOrder else row.instrument
        if instrument.market is not provenance.market or instrument.provider != provenance.provider:
            raise ValidationError("inventory row belongs to another provider or market")
        provider = row.observation.provider_order_id if row_type is OpenOrder else row.algo_id
        client = row.observation.client_order_id if row_type is OpenOrder else row.client_algo_id
        key, client_key = (instrument.key, provider), (instrument.key, client)
        if key in provider_ids or client_key in client_ids:
            raise ValidationError("inventory contains duplicate order identities")
        provider_ids.add(key)
        client_ids.add(client_key)
