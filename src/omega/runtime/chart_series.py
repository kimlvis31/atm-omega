"""Bounded display jobs using the common causal producers, never trading state."""

import json
import math
import re
import threading
import time
from collections import OrderedDict, deque
from copy import deepcopy
from hashlib import sha256

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.catalog import create_features
from omega.analysis.order_flow import OrderFlowConfiguration
from omega.analysis.parabolic import ParabolicConfiguration
from omega.analysis.swing import SwingConfiguration
from omega.analysis.temporal_probability import TemporalProbabilityConfiguration
from omega.analysis.volume_profile import ProfileConfiguration
from omega.domain.chart_analysis import ANALYSIS_KINDS, analysis_parameters
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market

MINUTE = 60000
MAX_MINUTES = 10000
MAX_TIME = 253_402_300_799_000
_ACTIVE = {"queued", "running", "cancelling"}
_FIELDS = ("DMIxADX", "ABSMA", "ABSMAREL")
_PARAMETERS = {
    "PRICE_WINDOWS": {"display_start_ms", "definitions"},
    "DMIxADX": {"nSamples"},
    "PSAR": {"start", "acceleration", "maximum", "price_precision"},
    "SWING": {"swing_range"},
    "NES": {"nSamples", "quote_precision"},
    "TPD": {"viewLength", "nSamples", "nSamplesMA"},
    "IVP": {
        "nSamples",
        "gammaFactor",
        "deltaFactor",
        "prominence",
        "distance",
        "height",
        "price_precision",
        "max_divisions",
    },
}


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _identity(value):
    if type(value) is not str or re.fullmatch("[A-Za-z0-9_-]{1,96}", value) is None:
        raise ValidationError("invalid chart series job identity")
    return value


def _instrument(value):
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
        if provider != "binance" or result.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        return result
    except (ValueError, TypeError, AttributeError):
        raise ValidationError("chart series requires a Binance instrument") from None


def parameters(value):
    if type(value) is dict and value.get("kind") in ANALYSIS_KINDS:
        return analysis_parameters(value["kind"], value)
    common = {"instrument", "start_ms", "end_ms", "period_minutes", "kind"}
    if (
        type(value) is not dict
        or type(value.get("kind")) is not str
        or value["kind"] not in _PARAMETERS
        or set(value) != common | _PARAMETERS[value["kind"]]
    ):
        raise ValidationError("invalid chart series parameters")
    _instrument(value["instrument"])
    if (
        any(type(value[key]) is not int for key in ("start_ms", "end_ms", "period_minutes"))
        or not 0 <= value["start_ms"] < value["end_ms"] <= MAX_TIME
        or value["start_ms"] % MINUTE
        or value["end_ms"] % MINUTE
        or value["end_ms"] - value["start_ms"] > MAX_MINUTES * MINUTE
        or value["period_minutes"] not in INTERVAL_MINUTES.values()
    ):
        raise ValidationError("chart series requires a bounded closed-minute range and period")
    intervalID = next(
        key for key, minute in INTERVAL_MINUTES.items() if minute == value["period_minutes"]
    )
    if value["kind"] == "PRICE_WINDOWS":
        from omega.runtime.chart_price_windows import validate_price_window_settings

        return validate_price_window_settings(value)
    numeric = _PARAMETERS[value["kind"]] - {
        "price_precision",
        "nSamples",
        "distance",
        "max_divisions",
    }
    if any(
        type(value[key]) not in {int, float}
        or type(value[key]) is float
        and not math.isfinite(value[key])
        for key in numeric
    ):
        raise ValidationError("chart series numeric settings require finite JSON numbers")
    if value["kind"] == "DMIxADX":
        if type(value["nSamples"]) is not int or not 2 <= value["nSamples"] <= 2000:
            raise ValidationError("invalid DMI chart sample count")
    elif value["kind"] == "PSAR":
        ParabolicConfiguration(intervalID, **{key: value[key] for key in _PARAMETERS["PSAR"]})
    elif value["kind"] == "SWING":
        SwingConfiguration(intervalID, swing_range=value["swing_range"])
    elif value["kind"] == "NES":
        OrderFlowConfiguration(
            intervalID, "NES", nSamples=value["nSamples"], quote_precision=value["quote_precision"]
        )
        if value["end_ms"] - value["start_ms"] > 1440 * MINUTE:
            raise ValidationError(
                "NES display requires an explicit replay range of at most1440minutes"
            )
    elif value["kind"] == "TPD":
        TemporalProbabilityConfiguration(
            intervalID, **{key: value[key] for key in _PARAMETERS["TPD"]}
        )
    else:
        try:
            ProfileConfiguration(intervalID, **{key: value[key] for key in _PARAMETERS["IVP"]})
        except OverflowError:
            raise ValidationError("IVP settings exceed finite display arithmetic") from None
        if value["max_divisions"] > 2048 or value["end_ms"] % (value["period_minutes"] * MINUTE):
            raise ValidationError("IVP display requires at most2048bins and a closed target bar")
    return deepcopy(value)


def _producer(specification, intervalID):
    kind = specification["kind"]
    if kind == "TPD":
        fields = ("TPD", "ABSMA", "ABSMAREL")
        public = tuple(
            f"{intervalID}_TPD_0_{suffix}" for suffix in ("TPD", "TPDABSMA", "TPDABSMAREL")
        )
        configuration = {
            "temporal_probability": [
                {
                    "interval_id": intervalID,
                    "line_index": 0,
                    **{key: specification[key] for key in _PARAMETERS["TPD"]},
                }
            ]
        }
    elif kind == "DMIxADX":
        fields = _FIELDS
        prefix = f"{intervalID}_DMIxADX_0_"
        public = tuple(prefix + key for key in ("DMIxADX", "DMIxADXABSMA", "DMIxADXABSMAREL"))
        configuration = {
            "momentum": [
                {
                    "interval_id": intervalID,
                    "analysis_code": kind,
                    "nSamples": specification["nSamples"],
                    "line_index": 0,
                }
            ]
        }
    else:
        fields = (
            ("PSAR", "DCC")
            if kind == "PSAR"
            else ("LSTIMESTAMP", "LSPRICE", "LSTYPE", "CONFIRMED_AT_MS")
        )
        public = tuple(f"{intervalID}_{kind}_0_{key}" for key in fields)
        group = "parabolic" if kind == "PSAR" else "swing"
        configuration = {
            group: [
                {
                    "interval_id": intervalID,
                    "line_index": 0,
                    **{key: specification[key] for key in _PARAMETERS[kind]},
                }
            ]
        }
    return fields, public, configuration


class ChartSeriesService:
    """Lazy single worker, bounded memory, owner-thread reads and explicit expiry.

    Jobs are display snapshots, not durable CA subscriptions. A core restart
    discards them. Idempotency is retained for the latest 128 request identities.
    """

    analysis_kinds = ANALYSIS_KINDS

    def __init__(self, source_factory, *, clock_ms=None, maximum_cached_rows=20000, monotonic=None):
        if (
            not callable(source_factory)
            or clock_ms is not None
            and not callable(clock_ms)
            or type(maximum_cached_rows) is not int
            or not 10000 <= maximum_cached_rows <= 50000
            or monotonic is not None
            and not callable(monotonic)
        ):
            raise ValidationError("invalid chart series worker configuration")
        self.source_factory = source_factory
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.maximum_cached_rows = maximum_cached_rows
        self.monotonic = monotonic or time.monotonic
        self._active_identity, self._deadline, self._cancelled = None, None, set()
        self._analysis_cleanup_failure = None
        self.condition = threading.Condition(threading.RLock())
        self.jobs, self.queue = OrderedDict(), deque()
        self.thread, self.closed = None, False

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= MAX_TIME:
            raise ValidationError("invalid chart series knowledge clock")
        return value

    def submit(self, identity, specification):
        _identity(identity)
        checked = parameters(specification)
        with self.condition:
            if self.closed:
                raise StorageError("chart series owner is closed")
            if checked["kind"] in ANALYSIS_KINDS and self._analysis_cleanup_failure is not None:
                raise StorageError("chart analysis source cleanup remains incomplete")
            if identity in self.jobs:
                row = self.jobs[identity]
                if row["parameters"] != checked or (
                    checked["kind"] == "PRICE_WINDOWS"
                    and row["spec_hash"] != sha256(_encoded(checked)).hexdigest()
                ):
                    raise ValidationError("chart series ID conflicts with its original request")
                return deepcopy(row)
            if checked["end_ms"] > self._now() // MINUTE * MINUTE:
                raise ValidationError("chart series cannot include an unfinished minute")
            if len(self.queue) >= 8:
                raise ValidationError("chart series queue capacity reached")
            while len(self.jobs) >= 128:
                old = next(
                    (key for key, row in self.jobs.items() if row["state"] not in _ACTIVE), None
                )
                if old is None:
                    raise ValidationError("chart series history capacity reached")
                del self.jobs[old]
                self._cancelled.discard(old)
            row = {
                "id": identity,
                "parameters": checked,
                "spec_hash": sha256(_encoded(checked)).hexdigest(),
                "state": "queued",
                "result": None,
                "error": None,
            }
            self.jobs[identity] = row
            self.queue.append(identity)
            if self.thread is None or not self.thread.is_alive():
                self.thread = threading.Thread(
                    target=self._loop, name="Omega-ChartSeries", daemon=True
                )
                self.thread.start()
            self.condition.notify_all()
            return deepcopy(row)

    def status(self, identity):
        _identity(identity)
        with self.condition:
            if identity not in self.jobs:
                raise ValidationError("chart series job expired or was not requested")
            return deepcopy(self.jobs[identity])

    def cancel(self, identity):
        """Cancel queued work immediately; running work stays cancelling until quiescent."""
        _identity(identity)
        with self.condition:
            if identity not in self.jobs:
                raise ValidationError("chart series job expired or was not requested")
            row = self.jobs[identity]
            if row["state"] == "queued":
                self.queue.remove(identity)
                row.update(state="cancelled", result=None)
            elif row["state"] in {"running", "cancelling"}:
                self._cancelled.add(identity)
                row.update(state="cancelling", result=None)
            self.condition.notify_all()
            return deepcopy(row)

    def _check_stop(self):
        with self.condition:
            if self.closed:
                raise InterruptedError("chart series owner stopped")
            if self._active_identity in self._cancelled:
                raise InterruptedError("chart series job cancelled")
            if self._deadline is not None and self.monotonic() >= self._deadline:
                raise TimeoutError(
                    "chart analysis compute deadline reached; choose a smaller replay window"
                )

    def _record_analysis_cleanup_failure(self, failure):
        with self.condition:
            if self._analysis_cleanup_failure is None:
                self._analysis_cleanup_failure = f"{type(failure).__name__}: {failure}"[:240]

    def _calculate(self, specification):
        instrument = _instrument(specification["instrument"])
        if specification["kind"] in ANALYSIS_KINDS:
            from omega.runtime.chart_analysis_series import calculate_analysis

            return calculate_analysis(self, specification, instrument)
        if specification["kind"] == "PRICE_WINDOWS":
            from omega.runtime.chart_price_windows import calculate_price_windows

            return calculate_price_windows(self, specification, instrument)
        if specification["kind"] == "IVP":
            from omega.runtime.chart_volume_profile import calculate_profile

            return calculate_profile(self, specification, instrument)
        if specification["kind"] == "NES":
            from omega.runtime.chart_market_series import calculate_nes

            return calculate_nes(self, specification, instrument)
        period = specification["period_minutes"]
        intervalID = next(key for key, value in INTERVAL_MINUTES.items() if value == period)
        fields, public, configuration = _producer(specification, intervalID)

        def create_graph():
            return create_features(instrument, public, configuration)

        as_of_ms, graph = self._now(), create_graph()
        if specification["end_ms"] > as_of_ms // MINUTE * MINUTE:
            raise ValidationError("chart series knowledge clock moved before its target")
        previous, observed, resets, rows = None, 0, 0, []
        digest = sha256()
        source = self.source_factory()
        try:
            start, end = specification["start_ms"], specification["end_ms"]
            for cursor in range(start, end, 512 * MINUTE):
                self._check_stop()
                stop = min(end, cursor + 512 * MINUTE)
                candles = source.read((instrument.key,), cursor, stop, limit=512, as_of_ms=as_of_ms)
                if type(candles) is not tuple or len(candles) > (stop - cursor) // MINUTE:
                    raise ProtocolError("chart source returned an invalid bounded snapshot")
                for candle in candles:
                    self._check_stop()
                    if (
                        not isinstance(candle, Candle)
                        or candle.instrument != instrument
                        or not cursor <= candle.open_time_ms < stop
                        or candle.close_time_ms > as_of_ms
                        or previous is not None
                        and candle.open_time_ms <= previous.open_time_ms
                    ):
                        raise ProtocolError(
                            "chart source returned conflicting candle identity/order"
                        )
                    if previous is not None and previous.close_time_ms != candle.open_time_ms:
                        graph, resets = create_graph(), resets + 1
                    feature = graph.update(candle)
                    digest.update(
                        _encoded(
                            {
                                "instrument": instrument.key,
                                "open_time_ms": candle.open_time_ms,
                                **{
                                    key: str(getattr(candle, key))
                                    for key in ("open", "high", "low", "close", "volume")
                                },
                            }
                        )
                    )
                    observed += 1
                    # A graph may publish a preview on every input minute. Display
                    # only complete target bars, never a later preview at an old open.
                    if candle.close_time_ms % (period * MINUTE) == 0:
                        opening = candle.close_time_ms - period * MINUTE
                        if opening >= start and feature.get(f"CLOSED_{intervalID}") is True:
                            values = [feature.get(key) for key in public]
                            if any(
                                value is not None
                                and (type(value) not in {float, int} or not math.isfinite(value))
                                for value in values
                            ):
                                raise ProtocolError("chart producer returned a nonfinite value")
                            rows.append(
                                {
                                    "open_time_ms": opening,
                                    "close_time_ms": candle.close_time_ms,
                                    **dict(zip(fields, values, strict=True)),
                                    **(
                                        {"segment": resets}
                                        if specification["kind"] != "DMIxADX"
                                        else {}
                                    ),
                                }
                            )
                    previous = candle
        finally:
            source.close()
        produced_at_ms = self._now()
        if produced_at_ms < as_of_ms:
            raise ValidationError("chart series completion clock precedes its knowledge cutoff")
        for row in rows:
            row["available_at_ms"] = produced_at_ms
        return {
            "instrument": instrument.key,
            "period_minutes": period,
            **{key: specification[key] for key in _PARAMETERS[specification["kind"]]},
            **({"kind": specification["kind"]} if specification["kind"] != "DMIxADX" else {}),
            "source": "database",
            "as_of_ms": as_of_ms,
            "produced_at_ms": produced_at_ms,
            "knowledge_mode": "retrospective_display",
            **({"replay_start_ms": start} if specification["kind"] == "TPD" else {}),
            "source_checksum": digest.hexdigest(),
            "rows": rows,
            "quality": {
                "requested_minutes": (end - start) // MINUTE,
                "observed_minutes": observed,
                "missing_minutes": (end - start) // MINUTE - observed,
                "reset_count": resets,
                "complete_bars": len(rows),
            },
        }

    def _expire_results(self):
        def weight(result):
            if result.get("kind") in ANALYSIS_KINDS:
                return len(result["rows"]) * len(result.get("definitions", ({},)))
            if result.get("kind") == "PRICE_WINDOWS":
                return sum(len(row["values"]) for row in result["rows"])
            return sum(1 + len(row.get("filtered") or ()) for row in result["rows"])

        total = sum(
            weight(row["result"]) for row in self.jobs.values() if row["result"] is not None
        )
        for row in self.jobs.values():
            if total <= self.maximum_cached_rows:
                break
            if row["state"] == "complete":
                total -= weight(row["result"])
                row.update(state="expired", result=None)

    def _loop(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.closed or bool(self.queue))
                if self.closed:
                    return
                identity = self.queue.popleft()
                row = self.jobs[identity]
                row["state"] = "running"
                specification = deepcopy(row["parameters"])
                self._active_identity = identity
                self._deadline = (
                    self.monotonic() + 60 if specification["kind"] in ANALYSIS_KINDS else None
                )
            try:
                self._check_stop()
                if (
                    specification["kind"] in ANALYSIS_KINDS
                    and self._analysis_cleanup_failure is not None
                ):
                    raise StorageError("chart analysis source cleanup remains incomplete")
                result, error, state = self._calculate(specification), None, "complete"
                self._check_stop()
                if len(_encoded(result)) > 4_000_000:
                    raise StorageError(
                        "chart series response too large; "
                        "choose fewer lines or a smaller replay window"
                    )
                self._check_stop()
            except InterruptedError:
                result, error, state = None, None, "cancelled"
            except Exception as failure:
                result, error, state = (
                    None,
                    (
                        f"{type(failure).__name__}: {failure}"[:240]
                        if specification["kind"] in ANALYSIS_KINDS
                        else type(failure).__name__
                    ),
                    "failed",
                )
            with self.condition:
                if state != "failed" and (self.closed or identity in self._cancelled):
                    result, error, state = None, None, "cancelled"
                self.jobs[identity].update(state=state, result=result, error=error)
                self._cancelled.discard(identity)
                self._active_identity, self._deadline = None, None
                self._expire_results()
                self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            self.queue.clear()
            for row in self.jobs.values():
                if row["state"] in _ACTIVE:
                    row.update(
                        state="cancelling" if row["id"] == self._active_identity else "cancelled",
                        result=None,
                    )
            self.condition.notify_all()
        if self.thread is not None:
            self.thread.join(6)
            if self.thread.is_alive():
                raise StorageError("chart series worker is still stopping")
        if self._analysis_cleanup_failure is not None:
            raise StorageError(
                "chart analysis source cleanup remains incomplete: "
                + self._analysis_cleanup_failure
            )
