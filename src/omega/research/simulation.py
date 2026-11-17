"""Causal CPU reference replay with explicit intrabar and cost assumptions.

This initial fill engine is a linear-PnL research model, not a leveraged
exchange margin/liquidation emulator. Strategies receive closed bars only.
"""

# Imports
import random
import re
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Protocol

from omega.research.artifacts import check_cancel
from omega.research.datasets import INTERVAL_MS, MarketRow, decimal_value


@dataclass(frozen=True)
class SimulationSpec:
    profile: str = "validation"
    initial_wallet: Decimal = Decimal("10000")
    path: str = "O-H-L-C"
    routes: dict[str, str] = field(default_factory=dict)
    seed: int = 0
    fee_rate: Decimal = Decimal("0.0004")
    slippage_rate: Decimal = Decimal("0")
    latency_ms: int = 0
    backend: str = "auto"
    funding: tuple[dict, ...] = ()
    history_rows: int = 4096
    quote_asset: str = "USDT"
    quote_assets: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.profile not in {"validation", "legacy_costs"}:
            raise ValueError("profile must be validation or legacy_costs")
        if self.path not in {"O-H-L-C", "O-L-H-C", "random"}:
            raise ValueError("invalid intrabar path")
        if any(key not in {"bullish", "bearish", "doji"} for key in self.routes):
            raise ValueError("routes only supports bullish, bearish and doji")
        if any(route not in {"O-H-L-C", "O-L-H-C"} for route in self.routes.values()):
            raise ValueError("case route must be O-H-L-C or O-L-H-C")
        if self.path == "random" and self.routes:
            raise ValueError("random path and deterministic case routes are mutually exclusive")
        if type(self.seed) is not int or type(self.latency_ms) is not int or self.latency_ms < 0:
            raise ValueError("seed/latency must be integers; latency nonnegative")
        decimal_value(self.initial_wallet, "initial_wallet", positive=True)
        for name in ("fee_rate", "slippage_rate"):
            value = decimal_value(getattr(self, name), name)
            if value >= 1:
                raise ValueError(f"{name} must be less than one")
        if self.backend not in {"cpu", "auto", "gpu"}:
            raise ValueError("backend must be cpu, auto or gpu")
        if type(self.history_rows) is not int or not 1 <= self.history_rows <= 65536:
            raise ValueError("history_rows must be within [1, 65536]")
        if not _valid_asset(self.quote_asset):
            raise ValueError("quote_asset must be an uppercase currency identifier")
        if not isinstance(self.quote_assets, dict) or any(
            not isinstance(instrument, str) or not instrument or not _valid_asset(asset)
            for instrument, asset in self.quote_assets.items()
        ):
            raise ValueError(
                "quote_assets must map nonempty instrument IDs to currency identifiers"
            )
        object.__setattr__(self, "quote_assets", dict(self.quote_assets))

    @classmethod
    def parse(cls, raw: dict) -> "SimulationSpec":
        raw = dict(raw)
        for name in ("initial_wallet", "fee_rate", "slippage_rate"):
            if name in raw:
                raw[name] = decimal_value(raw[name], name, positive=name == "initial_wallet")
        if "funding" in raw:
            raw["funding"] = tuple(raw["funding"])
        return cls(**raw)


def _valid_asset(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Z][A-Z0-9]{0,19}", value) is not None


def _resolve_quote_assets(instruments: set[str], spec: SimulationSpec) -> dict[str, str]:
    if spec.quote_assets.keys() - instruments:
        raise ValueError("quote_assets contains an unknown dataset instrument")
    result = {}
    for instrument in sorted(instruments):
        quote = spec.quote_assets.get(instrument)
        if ":" in instrument:
            parts = instrument.split(":")
            if len(parts) != 3 or not all(parts):
                raise ValueError("structured instrument ID must be provider:market:symbol")
            provider, market, symbol = parts
            inferred = (
                "USDT"
                if provider == "binance"
                and market in {"spot", "usdm"}
                and symbol.endswith("USDT")
                and len(symbol) > 4
                else None
            )
            if inferred and quote is not None and quote != inferred:
                raise ValueError("quote metadata contradicts the Binance USDT instrument")
            if quote is None:
                if inferred is None:
                    raise ValueError(f"explicit quote_assets metadata required for {instrument}")
                quote = inferred
        else:
            # Unqualified A/B-like IDs denote synthetic research series, not verified markets.
            quote = quote or spec.quote_asset
        if quote != spec.quote_asset:
            raise ValueError("mixed quote currencies require FX conversion, which is unsupported")
        result[instrument] = quote
    return result


@dataclass(frozen=True)
class OrderRequest:
    instrument: str
    quantity: Decimal
    kind: str = "market"
    price: Decimal | None = None
    reduce_only: bool = False
    tag: str = ""
    oco_group: str = ""
    entry_budget: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.instrument or not self.quantity.is_finite() or self.quantity == 0:
            raise ValueError("order instrument and finite nonzero signed quantity required")
        if self.kind not in {"market", "limit", "stop"}:
            raise ValueError("order kind must be market, limit or stop")
        if self.kind != "market" and (
            self.price is None or not self.price.is_finite() or self.price <= 0
        ):
            raise ValueError("limit/stop order requires positive price")
        if self.entry_budget is not None and (
            not self.entry_budget.is_finite() or self.entry_budget <= 0
        ):
            raise ValueError("entry_budget must be finite and positive")


@dataclass(frozen=True)
class CancelPendingRequest:
    """Cancel this CPU executor's still-pending orders, never retroactive fills."""

    instrument: str


@dataclass(frozen=True)
class SimulationView:
    timestamp_ms: int
    candles: tuple[MarketRow, ...]
    history: tuple[MarketRow, ...]
    positions: dict[str, Decimal]
    wallet: Decimal
    entry_prices: dict[str, Decimal] = field(default_factory=dict)
    position_revisions: dict[str, int] = field(default_factory=dict)
    position_changed_ms: dict[str, int] = field(default_factory=dict)
    pending_instruments: frozenset[str] = frozenset()


class StrategyCallback(Protocol):
    def __call__(self, view: SimulationView) -> list[OrderRequest | CancelPendingRequest]: ...


def candle_path(row: MarketRow, spec: SimulationSpec) -> tuple[tuple[int, Decimal], ...]:
    case = "bullish" if row.close > row.open else "bearish" if row.close < row.open else "doji"
    route = spec.routes.get(case, spec.path)
    if route == "random":
        rng = random.Random(f"intrabar-extrema-v1:{spec.seed}:{row.instrument}:{row.open_time_ms}")
        route = rng.choice(("O-H-L-C", "O-L-H-C"))
        middle = sorted(rng.sample(range(1, INTERVAL_MS - 1), 2))
    else:
        middle = [20_000, 40_000]
    prices = (
        (row.open, row.high, row.low, row.close)
        if route == "O-H-L-C"
        else (row.open, row.low, row.high, row.close)
    )
    return tuple(
        zip((row.open_time_ms + offset for offset in (0, *middle, 59_999)), prices, strict=True)
    )


@dataclass
class _Pending:
    request: OrderRequest
    active_at: int
    sequence: int


def _crossing(
    order: _Pending, segment: tuple, spec: SimulationSpec
) -> tuple[Decimal, Decimal] | None:
    (t0, p0), (t1, p1) = segment
    if order.active_at > t1:
        return None
    begin = Decimal(max(t0, order.active_at))
    end = Decimal(t1)
    price_begin = p0 + (p1 - p0) * (begin - t0) / (t1 - t0)
    request = order.request
    if request.kind == "market":
        slip = Decimal("0.0015") if spec.profile == "legacy_costs" else spec.slippage_rate
        return begin, price_begin * (1 + (slip if request.quantity > 0 else -slip))
    level = request.price
    assert level is not None
    # A buy limit triggers below its level; a buy stop triggers above its level.
    below = (request.quantity > 0) == (request.kind == "limit")
    already = price_begin <= level if below else price_begin >= level
    reaches = p1 <= level if below else p1 >= level
    if already:
        price = price_begin
        timestamp = begin
    elif reaches and p1 != price_begin:
        fraction = (level - price_begin) / (p1 - price_begin)
        timestamp = begin + fraction * (end - begin)
        price = level
    else:
        return None
    if request.kind == "stop":
        price *= 1 + (spec.slippage_rate if request.quantity > 0 else -spec.slippage_rate)
    return timestamp, price


def _mark_at(path: tuple, timestamp: Decimal) -> Decimal:
    for (t0, p0), (t1, p1) in zip(path, path[1:], strict=False):
        if timestamp <= t1:
            return p0 + (p1 - p0) * (timestamp - t0) / (t1 - t0)
    return path[-1][1]


def simulate(
    rows: list[MarketRow], strategy: StrategyCallback, spec: SimulationSpec, cancel: Any = None
) -> dict:
    if not rows:
        raise ValueError("simulation requires candles")
    frames: dict[int, list[MarketRow]] = defaultdict(list)
    keys = set()
    instruments = {row.instrument for row in rows}
    quotes = _resolve_quote_assets(instruments, spec)
    for row in rows:
        key = row.instrument, row.open_time_ms
        if key in keys:
            raise ValueError("duplicate candle in simulation")
        keys.add(key)
        frames[row.open_time_ms].append(row)
    wallet = spec.initial_wallet
    positions = dict.fromkeys(instruments, Decimal(0))
    entry = dict.fromkeys(instruments, Decimal(0))
    revisions = dict.fromkeys(instruments, 0)
    changed_ms = dict.fromkeys(instruments, 0)
    pending: list[_Pending] = []
    history = deque(maxlen=spec.history_rows)
    fills = []
    equity = []
    events = []
    last_price = {}
    sequence = 0
    funding_by_time: dict[int, list[dict]] = defaultdict(list)
    funding_keys = set()
    for event in spec.funding:
        timestamp = event["timestamp_ms"]
        instrument = event["instrument"]
        if type(timestamp) is not int or timestamp not in frames:
            raise ValueError("funding must have an exact dataset minute timestamp")
        if instrument not in instruments or ":spot:" in instrument:
            raise ValueError("funding requires an included derivatives instrument")
        rate = Decimal(str(event["rate"]))
        if not rate.is_finite() or abs(rate) >= 1:
            raise ValueError("invalid funding rate")
        if (timestamp, instrument) in funding_keys:
            raise ValueError("duplicate funding event")
        funding_keys.add((timestamp, instrument))
        funding_by_time[timestamp].append(event)
    for timestamp in sorted(frames):
        check_cancel(cancel)
        frame = sorted(frames[timestamp], key=lambda row: row.instrument)
        current = {row.instrument: row for row in frame}
        for funding in funding_by_time[timestamp]:
            instrument = funding["instrument"]
            if instrument not in current:
                raise ValueError("funding instrument has no current candle")
            cost = positions[instrument] * current[instrument].open * Decimal(str(funding["rate"]))
            wallet -= cost
            events.append(
                {
                    "kind": "funding",
                    "timestamp_ms": timestamp,
                    "instrument": instrument,
                    "cost": str(cost),
                }
            )
        # Segment events across all instruments share chronological ordering and cash state.
        candidates = []
        paths = {row.instrument: candle_path(row, spec) for row in frame}
        for row in frame:
            path = paths[row.instrument]
            for order in pending:
                if order.request.instrument != row.instrument:
                    continue
                for segment in zip(path, path[1:], strict=False):
                    crossing = _crossing(order, segment, spec)
                    if crossing is not None:
                        candidates.append((*crossing, order.sequence, order))
                        break
        cancelled_groups = set()
        completed = set()
        possible_groups: dict[tuple[str, str], int] = defaultdict(int)
        for _, _, _, order in candidates:
            request = order.request
            if request.oco_group:
                possible_groups[request.instrument, request.oco_group] += 1
        for group, count in possible_groups.items():
            if count > 1:
                events.append(
                    {
                        "kind": "intrabar_ambiguity",
                        "timestamp_ms": timestamp,
                        "instrument": group[0],
                        "oco_group": group[1],
                        "policy": "resolved by configured synthetic path; true order unknown",
                    }
                )
        for when, price, _, order in sorted(candidates, key=lambda item: (item[0], item[2])):
            request = order.request
            group_key = (request.instrument, request.oco_group)
            if request.oco_group and group_key in cancelled_groups:
                completed.add(order.sequence)
                continue
            instrument = request.instrument
            quantity = request.quantity
            previous = positions[instrument]
            if request.reduce_only:
                if previous * quantity >= 0:
                    completed.add(order.sequence)
                    events.append({"kind": "reduce_only_rejected", "tag": request.tag})
                    continue
                quantity = min(abs(previous), abs(quantity)) * (1 if quantity > 0 else -1)
            # No implicit borrowing in this reference. Reserve new exposure against marked notional.
            proposed = previous + quantity
            if ":spot:" in instrument and proposed < 0:
                completed.add(order.sequence)
                events.append({"kind": "short_not_supported", "tag": request.tag})
                continue
            increases = abs(proposed) > abs(previous) or (previous * proposed < 0)
            if request.entry_budget is not None and increases:
                committed_after = (
                    abs(previous) * entry[instrument] + abs(quantity) * price
                    if previous * quantity >= 0
                    else abs(proposed) * price
                )
                if committed_after > request.entry_budget:
                    completed.add(order.sequence)
                    events.append(
                        {"kind": "strategy_budget_exceeded_after_gap", "tag": request.tag}
                    )
                    continue
            marks = last_price | {symbol: _mark_at(path, when) for symbol, path in paths.items()}
            mark_equity = wallet + sum(
                q * (marks[symbol] - entry[symbol])
                for symbol, q in positions.items()
                if symbol in marks
            )
            notional = sum(
                abs(proposed if symbol == instrument else q)
                * (price if symbol == instrument else marks[symbol])
                for symbol, q in positions.items()
                if symbol in marks
            )
            fee_rate = spec.fee_rate
            if spec.profile == "legacy_costs":
                if ":usdm:" not in instrument or not instrument.endswith(("USDT", "USDC")):
                    raise ValueError("legacy_costs supports Binance USDT/USDC perpetual only")
                fee_rate = (
                    (Decimal("0") if instrument.endswith("USDC") else Decimal("0.0002"))
                    if request.kind == "limit"
                    else (Decimal("0.0004") if instrument.endswith("USDC") else Decimal("0.0005"))
                )
            fee = abs(quantity * price) * fee_rate
            if increases and notional + fee > mark_equity:
                completed.add(order.sequence)
                events.append({"kind": "insufficient_unleveraged_capital", "tag": request.tag})
                continue
            closing = min(abs(previous), abs(quantity)) if previous * quantity < 0 else Decimal(0)
            pnl = closing * (price - entry[instrument]) * (1 if previous > 0 else -1)
            wallet += pnl - fee
            if proposed == 0:
                entry[instrument] = Decimal(0)
            elif previous == 0 or previous * proposed < 0:
                entry[instrument] = price
            elif previous * quantity > 0:
                entry[instrument] = (
                    abs(previous) * entry[instrument] + abs(quantity) * price
                ) / abs(proposed)
            positions[instrument] = proposed
            revisions[instrument] += 1
            changed_ms[instrument] = int(when)
            completed.add(order.sequence)
            if request.oco_group:
                cancelled_groups.add(group_key)
            fills.append(
                {
                    "timestamp_ms": str(when),
                    "instrument": instrument,
                    "quantity": str(quantity),
                    "price": str(price),
                    "fee": str(fee),
                    "realized_pnl": str(pnl),
                    "tag": request.tag,
                }
            )
        pending = [
            order
            for order in pending
            if order.sequence not in completed
            and (
                not order.request.oco_group
                or (order.request.instrument, order.request.oco_group) not in cancelled_groups
            )
        ]
        history.extend(frame)
        last_price.update({row.instrument: row.close for row in frame})
        equity.append(
            {
                "timestamp_ms": timestamp + INTERVAL_MS,
                "wallet": str(wallet),
                "equity": str(
                    wallet
                    + sum(
                        quantity * (last_price[instrument] - entry[instrument])
                        for instrument, quantity in positions.items()
                        if instrument in last_price
                    )
                ),
            }
        )
        view = SimulationView(
            timestamp + INTERVAL_MS,
            tuple(frame),
            tuple(history),
            dict(positions),
            wallet,
            dict(entry),
            dict(revisions),
            dict(changed_ms),
            frozenset(order.request.instrument for order in pending),
        )
        requests = deque(strategy(view))
        processed = 0
        while requests:
            processed += 1
            if processed > 10000:
                raise ValueError("strategy exceeded the per-frame action limit")
            request = requests.popleft()
            if isinstance(request, CancelPendingRequest):
                if request.instrument not in instruments:
                    raise ValueError("cancellation references an unknown instrument")
                count = sum(order.request.instrument == request.instrument for order in pending)
                if not count:
                    raise ValueError("no pending order to confirm cancellation")
                pending = [
                    order for order in pending if order.request.instrument != request.instrument
                ]
                events.append(
                    {
                        "kind": "orders_canceled",
                        "instrument": request.instrument,
                        "timestamp_ms": view.timestamp_ms,
                        "count": count,
                    }
                )
                acknowledged = getattr(strategy, "on_cancellation_confirmed", None)
                if callable(acknowledged):
                    # Confirmation is issued only after this executor changed its
                    # own order book. A real broker must obtain its own receipt.
                    requests.extendleft(reversed(acknowledged(request.instrument)))
                continue
            if not isinstance(request, OrderRequest) or request.instrument not in instruments:
                raise ValueError("strategy returned an invalid/unknown instrument order")
            sequence += 1
            pending.append(_Pending(request, view.timestamp_ms + spec.latency_ms, sequence))
    return {
        "schema_version": 1,
        "state": "complete",
        "quote_asset": spec.quote_asset,
        "quote_assets": quotes,
        "backend": {
            "requested": spec.backend,
            "selected": "cpu",
            "reason": "CPU reference implemented; GPU parity backend not validated",
        },
        "spec": {
            name: str(value) if isinstance(value, Decimal) else value
            for name, value in asdict(spec).items()
        },
        "model": "linear-pnl-unleveraged-reference-v1",
        "fills": fills,
        "equity": equity,
        "events": events,
        "wallet": str(wallet),
        "positions": {name: str(value) for name, value in positions.items()},
        "pending_orders": len(pending),
        "assumptions": [
            "synthetic extrema order/timing, not exchange trade history",
            "closed-1m strategy callbacks; earliest next-minute fills",
            "full fills; no depth/partial-fill or liquidation model",
            "legacy_costs restores fee/slippage defaults only, not full Eta execution parity",
            "same-time orders ordered by submission sequence",
            f"callback history is bounded to the last {spec.history_rows} market rows",
            f"wallet/PnL/fees/funding use {spec.quote_asset}; FX conversion is unsupported",
            "quote metadata is explicitly supplied or inferred only for Binance USDT symbols",
            "unqualified instrument IDs are synthetic series in the configured quote unit",
        ],
    }
