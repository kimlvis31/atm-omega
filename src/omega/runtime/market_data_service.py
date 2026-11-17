"""Cached database view and bounded, durable public-data operations."""

# Imports
import hashlib
import json
import re
import threading
import time
from collections import deque
from copy import deepcopy
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.execution_rules import MAX_TIME, ExecutionRulesSnapshot
from omega.infrastructure.market_data import MarketDataBackend, instrument_key, peer_parameters
from omega.runtime.market_data_worker import OwnedCollectors
from omega.runtime.settings import atomic_json

KINDS = {"catalog.refresh", "collection.set", "gaps.refetch", "peer.import"}


def empty_detail(key):
    return {
        "instrument": key,
        "metadata": {},
        "channels": {
            "ohlcv": {
                "supported": True,
                "first_open_ms": None,
                "last_open_ms": None,
                "rows": None,
                "gap_rows": None,
                "availability": None,
                "gap_rate": None,
                "dummy_rate": None,
                "available_ranges": None,
                "gap_ranges": None,
            },
            "depth": {"supported": False},
            "agg_trade": {"supported": False},
            "metrics": {"supported": False},
        },
    }


def empty_view():
    return {
        "schema_version": 1,
        "revision": 0,
        "source": "unavailable",
        "updated_at_ms": None,
        "database": {
            "state": "unavailable",
            "engine": "PostgreSQL + TimescaleDB",
            "name": None,
            "size_bytes": None,
            "free_bytes": None,
            "total_bytes": None,
            "compression_before_bytes": None,
            "compression_after_bytes": None,
            "message": "Database state has not been observed.",
        },
        "collection": {
            "last_fetched": None,
            "rows_per_second": None,
            "remaining_ranges": None,
            "eta_ms": None,
        },
        "capabilities": {
            "collection_update": True,
            "catalog_refresh": True,
            "gap_refetch": True,
            "local_network_import": True,
            "compression": False,
            "reset": False,
        },
        "instruments": [],
        "selected": None,
        "actions": [],
    }


class MarketDataService:
    def __init__(
        self,
        root: Path,
        *,
        backend=None,
        collectors=None,
        refresh_seconds=15,
        clock_ms=None,
        monotonic=None,
        capture_rest_raw=False,
        raw_directory=None,
    ):
        self.root = Path(root).resolve()
        self.path = self.root / "state" / "market-data.json"
        if type(capture_rest_raw) is not bool or not capture_rest_raw and raw_directory is not None:
            raise ValidationError("raw directory requires explicit REST capture")
        capture = (
            {"capture_rest_raw": True, "raw_directory": raw_directory} if capture_rest_raw else {}
        )
        self.backend, self.collectors = (
            backend or MarketDataBackend(self.root, **capture),
            collectors or OwnedCollectors(self.root, **capture),
        )
        self.condition = threading.Condition(threading.RLock())
        self.stop = threading.Event()
        self.queue, self.actions, self.desired, self.active_historical = deque(), {}, {}, set()
        self.historical_desired = {}
        self.history_lookback = {}
        self.views = {"spot": empty_view(), "usdm": empty_view()}
        self.catalogs = {"spot": {}, "usdm": {}}
        self.selection, self.refresh_pending, self.refresh_at = {}, {}, {}
        self.samples = {}
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.monotonic = monotonic or time.monotonic
        self.rule_cache, self.rules_pending, self.rules_inflight = {}, set(), set()
        self.rules_due, self.rules_failures = {}, {}
        self.rules_health = {
            market: {
                "state": "unavailable",
                "reason": "not_observed",
                "last_attempt_ms": None,
                "last_success_ms": None,
            }
            for market in ("spot", "usdm")
        }
        self.refresh_seconds, self.storage_failed, self.closed = refresh_seconds, False, False
        if self.path.exists():
            if self.path.stat().st_size > 4_194_304:
                raise StorageError("market operation state exceeds capacity")
            try:
                saved = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raise StorageError("market operation state is unreadable") from None
            if (
                not isinstance(saved, dict)
                or type(saved.get("schema_version")) is not int
                or saved["schema_version"] not in {1, 2}
                or set(saved)
                != (
                    {"schema_version", "actions"}
                    if saved["schema_version"] == 1
                    else {"schema_version", "actions", "desired"}
                )
                or not isinstance(saved["actions"], dict)
                or len(saved["actions"]) > 512
            ):
                raise StorageError("invalid market operation state")
            self._restore_plan(saved.get("desired", {}))
            self.actions = saved["actions"]
            for identity, record in self.actions.items():
                if (
                    not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", identity)
                    or not isinstance(record, dict)
                    or set(record) != {"digest", "status"}
                    or not isinstance(record["status"], dict)
                ):
                    raise StorageError("invalid market operation record")
                status = record["status"]
                if status.get("state") in {"queued", "running"}:
                    status.update(
                        state="unknown", error="INTERRUPTED_BEFORE_CONFIRMATION", result=None
                    )
            self._save()
        self.thread = threading.Thread(target=self._loop, name="omega-market-data", daemon=True)
        self.thread.start()

    def _restore_plan(self, plan):
        """Restore wishes only. No child spawn or old operation replay occurs here."""
        if not isinstance(plan, dict) or len(plan) > 200:
            raise StorageError("invalid saved collection plan")
        counts = {"spot": 0, "usdm": 0}
        try:
            for key, value in plan.items():
                instrument = instrument_key(key)
                if (
                    not isinstance(value, dict)
                    or set(value) != {"stream", "historical", "lookback_minutes"}
                    or value["stream"] is not True
                    or type(value["historical"]) is not bool
                    or type(value["lookback_minutes"]) is not int
                    or not 1 <= value["lookback_minutes"] <= 20_000
                ):
                    raise ValidationError("invalid saved collection preference")
                counts[instrument.market.value] += 1
                if counts[instrument.market.value] > 100:
                    raise ValidationError("saved collector capacity exceeded")
                self.desired[key] = True
                self.historical_desired[key] = value["historical"]
                self.history_lookback[key] = value["lookback_minutes"]
                self.views[instrument.market.value]["instruments"].append(
                    {
                        "instrument": key,
                        "symbol": instrument.symbol,
                        "market": instrument.market.value,
                        "status": "unknown",
                        "first_open_ms": None,
                        "last_open_ms": None,
                        "rows": None,
                        "gap_rows": None,
                        "availability": None,
                        "gap_rate": None,
                        "dummy_rate": None,
                        "metadata": {},
                    }
                )
        except ValidationError:
            raise StorageError("invalid saved collection plan") from None

    def _save(self, *, allow_closed=False):
        # Callers hold condition (or are still in __init__). Once retired, a late
        # backend callback must not replace the next owner's operation journal.
        if self.closed and not allow_closed:
            return
        try:
            plan = {
                key: {
                    "stream": True,
                    "historical": bool(self.historical_desired.get(key)),
                    "lookback_minutes": self.history_lookback.get(key, 500),
                }
                for key, enabled in sorted(self.desired.items())
                if enabled
            }
            atomic_json(self.path, {"schema_version": 2, "actions": self.actions, "desired": plan})
        except (OSError, TypeError, ValueError):
            self.storage_failed = True
            raise StorageError("market operation recording unavailable") from None

    @staticmethod
    def _market(market):
        if market not in {"spot", "usdm"}:
            raise ValidationError("market must be spot or usdm")
        return market

    def snapshot(self, market="usdm", instrument=None):
        market = self._market(market)
        if instrument is not None and instrument_key(instrument).market.value != market:
            raise ValidationError("selected instrument belongs to another market")
        with self.condition:
            changed = self.selection.get(market) != instrument
            self.selection[market] = instrument
            if not self.closed and (
                changed
                or time.monotonic() - self.refresh_at.get(market, -1e10) >= self.refresh_seconds
            ):
                self.refresh_pending[market] = instrument
                self.condition.notify()
            view = deepcopy(self.views[market])
            if instrument is not None and (
                not view["selected"] or view["selected"]["instrument"] != instrument
            ):
                view["selected"] = empty_detail(instrument)
            elif instrument is None:
                view["selected"] = None
            worker = (
                self.collectors.status(market)
                if hasattr(self.collectors, "status")
                else {
                    "state": "unobserved",
                    "running": self.collectors.running(market),
                    "ready": False,
                    "instrument_ready": {},
                    "last_close_ms": {},
                }
            )
            view["collection"]["worker"] = worker
            view["collection"]["manual_apply_required"] = bool(
                not worker["running"]
                and any(
                    enabled and instrument_key(key).market.value == market
                    for key, enabled in self.desired.items()
                )
            )
            for row in view["instruments"]:
                row["collecting_stream"] = bool(
                    self.desired.get(row["instrument"]) and self.collectors.running(market)
                )
                row["collecting_historical"] = row["instrument"] in self.active_historical or bool(
                    self.historical_desired.get(row["instrument"])
                    and self.collectors.running(market)
                )
                row["collection_requested"] = bool(self.desired.get(row["instrument"]))
                row["historical_requested"] = bool(self.historical_desired.get(row["instrument"]))
                row["history_lookback_minutes"] = self.history_lookback.get(row["instrument"])
                row["collection_state"] = (
                    worker["state"] if row["collection_requested"] else "stopped"
                )
                row["data_ready"] = bool(
                    row["collection_requested"]
                    and worker["running"]
                    and worker.get("instrument_ready", {}).get(row["instrument"], False)
                )
            view["actions"] = [
                deepcopy(record["status"]) for record in list(self.actions.values())[-20:]
            ][::-1]
            view["operation_recording_available"] = not self.storage_failed
            view["closed"] = self.closed
            view["execution_rules"] = self.execution_rules_status(market)
            return view

    def _rules_now(self):
        now = self.clock_ms()
        if type(now) is not int or not 0 <= now <= MAX_TIME:
            raise ValidationError("invalid actual execution rules clock")
        return now

    def execution_rules_status(self, market):
        market = self._market(market)
        with self.condition:
            return {
                **deepcopy(self.rules_health[market]),
                "pending": market in self.rules_pending,
                "in_flight": market in self.rules_inflight,
                "closed": self.closed,
                "recording_available": not self.storage_failed,
                "cached_symbols": sum(
                    instrument_key(key).market.value == market for key in self.rule_cache
                ),
                "retry_after_ms": max(
                    0, int((self.rules_due.get(market, 0) - self.monotonic()) * 1000)
                ),
            }

    def execution_rules(self, key, *, max_age_ms=60000):
        """Memory-only provider. Missing/stale rules schedule one worker GET per market."""
        instrument = instrument_key(key)
        if type(max_age_ms) is not int or not 1 <= max_age_ms <= 86400000:
            raise ValidationError("invalid execution rules cache age")
        with self.condition:
            if self.closed or self.storage_failed:
                return None
            snapshot = self.rule_cache.get(instrument.key)
            now = self._rules_now()
            fresh = snapshot is not None and 0 <= now - snapshot.observed_at_ms <= max_age_ms
            market = instrument.market.value
            if (
                not fresh
                and market not in self.rules_pending
                and market not in self.rules_inflight
                and self.monotonic() >= self.rules_due.get(market, 0)
            ):
                self.rules_pending.add(market)
                self.condition.notify()
            return snapshot

    def _install_catalog(self, market, catalog):
        if type(catalog) is not dict or len(catalog) > 10000:
            raise ValidationError("invalid bounded execution catalog")
        normalized, cached = {}, {}
        for key, row in catalog.items():
            instrument = instrument_key(key)
            if instrument.market.value != market or type(row) is not dict:
                raise ValidationError("execution catalog belongs to another market")
            item = deepcopy(row)
            metadata = item.get("metadata")
            if type(metadata) is not dict:
                raise ValidationError("execution catalog metadata is unavailable")
            document = metadata.get("execution_rules")
            if document is not None:
                try:
                    snapshot = ExecutionRulesSnapshot.from_document(document)
                    if snapshot.instrument != instrument:
                        raise ValidationError("execution catalog symbol identity mismatch")
                    cached[key] = snapshot
                    metadata.pop("execution_rules", None)
                    metadata["execution_rules_summary"] = {
                        "content_hash": snapshot.content_hash,
                        "observed_at_ms": snapshot.observed_at_ms,
                        "source": snapshot.source,
                        "unsupported_filter_count": len(snapshot.unsupported_filters),
                    }
                except ValidationError:
                    metadata.pop("execution_rules", None)
                    metadata["execution_rules_error"] = "invalid_normalized_execution_rules"
            normalized[key] = item
        now = self._rules_now()
        with self.condition:
            if self.closed:
                raise InterruptedError("market data owner retired")
            self.catalogs[market] = normalized
            self.rule_cache = {
                key: value
                for key, value in self.rule_cache.items()
                if instrument_key(key).market.value != market
            }
            self.rule_cache.update(cached)
            self.rules_pending.discard(market)
            self.rules_failures[market] = 0
            self.rules_due[market] = self.monotonic() + 30
            valid = any(0 <= now - value.observed_at_ms <= 60000 for value in cached.values())
            self.rules_health[market].update(
                state="ready" if valid else "unavailable",
                reason="observed" if valid else "no_fresh_symbol_rules",
                last_success_ms=now,
            )

    def _detail_metadata(self, key):
        # Thousands of symbols share the list response. Full rule documents
        # belong only to the selected detail; the typed cache remains authoritative.
        market = instrument_key(key).market.value
        metadata = deepcopy(self.catalogs[market].get(key, {}).get("metadata", {}))
        snapshot = self.rule_cache.get(key)
        if snapshot is not None:
            metadata["execution_rules"] = snapshot.document()
        return metadata

    def _fetch_catalog(self, market):
        try:
            now = self._rules_now()
            with self.condition:
                if self.closed or self.storage_failed:
                    raise StorageError("market catalog owner unavailable")
                self.rules_inflight.add(market)
                self.rules_pending.discard(market)
                self.rules_health[market].update(
                    state="refreshing", reason="worker_fetch", last_attempt_ms=now
                )
            catalog = self.backend.catalog(market)
            self._install_catalog(market, catalog)
            return catalog
        except Exception as exc:
            with self.condition:
                if not self.closed:
                    failure = self.rules_failures.get(market, 0) + 1
                    self.rules_failures[market] = failure
                    self.rules_due[market] = self.monotonic() + min(30, 2 ** min(failure - 1, 5))
                    self.rules_health[market].update(state="unavailable", reason=type(exc).__name__)
            raise
        finally:
            with self.condition:
                self.rules_inflight.discard(market)

    def _refresh_rules(self, market):
        try:
            self._fetch_catalog(market)
        except Exception:
            pass  # Health/backoff was recorded; no durable action was submitted.

    @staticmethod
    def _parameters(kind, parameters):
        if kind not in KINDS or not isinstance(parameters, dict):
            raise ValidationError("unsupported database action")
        parameters = deepcopy(parameters)
        allowed = {
            "catalog.refresh": {"market"},
            "collection.set": {"instruments", "stream", "historical", "lookback_minutes"},
            "gaps.refetch": {"instrument", "lookback_minutes"},
            "peer.import": {
                "instrument",
                "host",
                "port",
                "database",
                "user",
                "password",
                "lookback_minutes",
            },
        }
        if set(parameters) - allowed[kind]:
            raise ValidationError("unknown database action parameter")
        if kind == "catalog.refresh":
            parameters["market"] = MarketDataService._market(parameters.get("market", "usdm"))
            return parameters
        lookback = parameters.get("lookback_minutes", 500)
        if type(lookback) is not int or not 1 <= lookback <= 20_000:
            raise ValidationError("lookback_minutes must be 1..20000")
        parameters["lookback_minutes"] = lookback
        if kind == "collection.set":
            values = parameters.get("instruments")
            if not isinstance(values, list) or not 1 <= len(values) <= 100:
                raise ValidationError("select one to 100 instruments")
            parameters["instruments"] = sorted({instrument_key(key).key for key in values})
            if (
                type(parameters.get("stream")) is not bool
                or type(parameters.get("historical")) is not bool
                or (parameters["historical"] and not parameters["stream"])
            ):
                raise ValidationError(
                    "historical collection requires streaming; use gap refetch for repairs"
                )
        else:
            parameters["instrument"] = instrument_key(parameters.get("instrument")).key
            if kind == "peer.import":
                peer_parameters(parameters)
        return parameters

    def submit(self, identity, kind, parameters):
        if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", identity):
            raise ValidationError("invalid database action identity")
        parameters = self._parameters(kind, parameters)
        # No password, password hash, username or connection descriptor is
        # recorded. Reusing an ID never repeats a peer operation with new secrets.
        public = {
            key: value
            for key, value in parameters.items()
            if key not in {"host", "port", "database", "user", "password"}
        }
        digest = hashlib.sha256(
            json.dumps({"kind": kind, "parameters": public}, sort_keys=True).encode()
        ).hexdigest()
        with self.condition:
            previous = self.actions.get(identity)
            if previous:
                if previous["digest"] != digest:
                    raise ValidationError("database action ID conflicts with an earlier operation")
                return deepcopy(previous["status"])
            if self.closed or self.storage_failed:
                raise StorageError("database action service is closed or recording is unavailable")
            if len(self.queue) >= 8 or len(self.actions) >= 512:
                raise ValidationError("database action queue/history capacity reached")
            status = {
                "id": identity,
                "kind": kind,
                "state": "queued",
                "progress": 0,
                "result": None,
                "error": None,
            }
            self.actions[identity] = {"digest": digest, "status": status}
            try:
                self._save()
            except StorageError:
                status.update(state="unknown", error="RECORDING_UNAVAILABLE")
                raise
            self.queue.append((identity, parameters))
            self.condition.notify()
            return deepcopy(status)

    def status(self, identity):
        with self.condition:
            if identity not in self.actions:
                raise LookupError("database action not found")
            return deepcopy(self.actions[identity]["status"])

    def _refresh(self, market, selected):
        with self.condition:
            if self.closed:
                return
        try:
            observed = self.backend.refresh(market, selected)
            with self.condition:
                if self.closed:
                    return
                view = self.views[market]
                records = {row["instrument"]: row for row in observed["instruments"]}
                keys = (
                    set(records)
                    | set(self.catalogs[market])
                    | {key for key in self.desired if instrument_key(key).market.value == market}
                )
                combined = []
                for key in sorted(keys):
                    instrument = instrument_key(key)
                    row = {
                        "instrument": key,
                        "symbol": instrument.symbol,
                        "market": market,
                        "status": "unknown",
                        "first_open_ms": None,
                        "last_open_ms": None,
                        "rows": None,
                        "gap_rows": None,
                        "availability": None,
                        "gap_rate": None,
                        "dummy_rate": None,
                        "metadata": {},
                    }
                    row.update(records.get(key, {}))
                    row.update(deepcopy(self.catalogs[market].get(key, {})))
                    combined.append(row)
                view.update(
                    source="database",
                    database=observed["database"],
                    instruments=combined,
                    selected=observed["selected"],
                    updated_at_ms=time.time_ns() // 1_000_000,
                    revision=view["revision"] + 1,
                )
                if view["selected"]:
                    view["selected"]["metadata"] = self._detail_metadata(selected)
                total = sum(row.get("rows") or 0 for row in records.values())
                now = time.monotonic()
                previous = self.samples.get(market)
                rate = (
                    max(0, total - previous[1]) / (now - previous[0])
                    if previous and now > previous[0]
                    else None
                )
                self.samples[market] = now, total
                view["collection"].update(
                    last_fetched=observed.get("last_fetched"), rows_per_second=rate
                )
        except Exception:
            with self.condition:
                if self.closed:
                    return
                known = {row["instrument"]: row for row in self.views[market]["instruments"]}
                references = dict(self.catalogs[market])
                references.update(
                    {
                        key: references.get(key, {})
                        for key in self.desired
                        if instrument_key(key).market.value == market
                    }
                )
                for key, reference in references.items():
                    if key not in known:
                        instrument = instrument_key(key)
                        known[key] = {
                            "instrument": key,
                            "symbol": instrument.symbol,
                            "market": market,
                            "first_open_ms": None,
                            "last_open_ms": None,
                            "rows": None,
                            "gap_rows": None,
                            "availability": None,
                            "gap_rate": None,
                            "dummy_rate": None,
                        }
                    known[key].update(deepcopy(reference))
                self.views[market]["instruments"] = [known[key] for key in sorted(known)]
                self.views[market]["selected"] = empty_detail(selected) if selected else None
                if selected:
                    self.views[market]["selected"]["metadata"] = self._detail_metadata(selected)
                self.views[market]["database"].update(
                    state="unavailable",
                    message="Database or matching Omega schema could not be observed.",
                )
                self.views[market].update(
                    source="unavailable", updated_at_ms=time.time_ns() // 1_000_000
                )
        finally:
            with self.condition:
                if not self.closed:
                    self.refresh_at[market] = time.monotonic()

    def _perform(self, kind, parameters, progress):
        with self.condition:
            if self.closed:
                raise InterruptedError("market service is stopping")
        if kind == "catalog.refresh":
            market = parameters["market"]
            catalog = self._fetch_catalog(market)
            self._refresh(market, self.selection.get(market))
            return {"instruments": len(catalog), "metadata_source": "exchange"}
        if kind == "collection.set":
            keys = parameters["instruments"]
            markets = {instrument_key(key).market.value for key in keys}
            with self.condition:
                if self.closed:
                    raise InterruptedError("market service is stopping")
                proposed = dict(self.desired)
                proposed.update({key: parameters["stream"] for key in keys})
                proposed_history = dict(self.historical_desired)
                proposed_history.update(
                    {key: parameters["stream"] and parameters["historical"] for key in keys}
                )
                proposed_lookback = dict(self.history_lookback)
                proposed_lookback.update({key: parameters["lookback_minutes"] for key in keys})
                if any(
                    sum(
                        enabled and instrument_key(key).market.value == market
                        for key, enabled in proposed.items()
                    )
                    > 100
                    for market in markets
                ):
                    raise ValidationError("collector capacity is 100 instruments per market")
            for market in sorted(markets):
                if self.stop.is_set():
                    raise InterruptedError("market service is stopping")
                instruments = sorted(
                    key
                    for key, enabled in proposed.items()
                    if enabled and instrument_key(key).market.value == market
                )
                historical = any(proposed_history.get(key) for key in instruments)
                lookback = max(
                    (
                        proposed_lookback.get(key, 500)
                        for key in instruments
                        if proposed_history.get(key)
                    ),
                    default=parameters["lookback_minutes"],
                )
                starts = {
                    key: proposed_lookback.get(key, 500) if proposed_history.get(key) else 0
                    for key in instruments
                }
                self.collectors.configure(market, instruments, lookback, historical, starts=starts)
                with self.condition:
                    if self.closed:
                        raise InterruptedError("market service is stopping")
                    for key in keys:
                        if instrument_key(key).market.value == market:
                            self.desired[key] = parameters["stream"]
                            self.historical_desired[key] = (
                                parameters["stream"] and parameters["historical"]
                            )
                            self.history_lookback[key] = parameters["lookback_minutes"]
            return {
                "configured_instruments": len(keys),
                "stream": parameters["stream"],
                "historical": parameters["historical"],
                "confirmation": "worker_configured_not_data_ready",
            }
        key = parameters["instrument"]
        market = instrument_key(key).market.value
        with self.condition:
            if self.closed:
                raise InterruptedError("market service is stopping")
            self.active_historical.add(key)
        try:
            if kind == "gaps.refetch":
                result = self.backend.refetch(
                    key, parameters["lookback_minutes"], progress, self.stop
                )
            else:
                result = self.backend.import_peer(
                    key, parameters, parameters["lookback_minutes"], progress, self.stop
                )
            with self.condition:
                if self.closed:
                    raise InterruptedError("market service is stopping")
                self.views[market]["collection"]["remaining_ranges"] = result.get(
                    "remaining_ranges"
                )
            self._refresh(market, self.selection.get(market))
            return result
        finally:
            with self.condition:
                if not self.closed:
                    self.active_historical.discard(key)

    def _loop(self):
        while True:
            with self.condition:
                self.condition.wait_for(
                    lambda: self.closed or self.queue or self.refresh_pending or self.rules_pending
                )
                if self.closed:
                    return
                if self.queue:
                    identity, parameters = self.queue.popleft()
                    status = self.actions[identity]["status"]
                    status["state"] = "running"
                    try:
                        self._save()
                    except StorageError:
                        status.update(state="failed", error="RECORDING_UNAVAILABLE")
                        parameters.clear()
                        continue
                    refresh = None
                    rules_refresh = None
                elif self.rules_pending:
                    rules_refresh = next(iter(sorted(self.rules_pending)))
                    self.rules_pending.remove(rules_refresh)
                    self.rules_inflight.add(rules_refresh)
                    refresh = None
                else:
                    market = next(iter(self.refresh_pending))
                    refresh = market, self.refresh_pending.pop(market)
                    rules_refresh = None
            if rules_refresh is not None:
                self._refresh_rules(rules_refresh)
                continue
            if refresh is not None:
                self._refresh(*refresh)
                continue
            try:

                def progress(value, status=status):
                    with self.condition:
                        if not self.closed:
                            status["progress"] = min(1, max(0, value))

                result = self._perform(status["kind"], parameters, progress)
                with self.condition:
                    if not self.closed:
                        status.update(state="complete", progress=1, result=result)
            except Exception as exc:
                with self.condition:
                    if not self.closed:
                        status.update(state="failed", error=type(exc).__name__, result=None)
            finally:
                parameters.clear()
                with self.condition:
                    try:
                        self._save()
                    except StorageError:
                        status.update(state="unknown", error="RECORDING_UNAVAILABLE", result=None)

    def close(self):
        with self.condition:
            if self.closed:
                if self.thread.is_alive():
                    raise StorageError("market data worker is still stopping")
                try:
                    self.collectors.close()
                except Exception:
                    raise StorageError("owned collector shutdown was not confirmed") from None
                return
            self.closed = True
            self.stop.set()
            self.active_historical.clear()
            self.refresh_pending.clear()
            self.rules_pending.clear()
            for record in self.actions.values():
                if record["status"]["state"] == "running":
                    record["status"].update(
                        state="unknown", error="CLOSED_BEFORE_CONFIRMATION", result=None
                    )
            for identity, parameters in self.queue:
                parameters.clear()
                self.actions[identity]["status"].update(state="failed", error="SERVICE_CLOSED")
            self.queue.clear()
            self.condition.notify_all()
        collector_error = None
        try:
            self.collectors.close()
        except Exception:
            collector_error = StorageError("owned collector shutdown was not confirmed")
        # An unconfirmed child shutdown must not skip joining our own operation worker.
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise StorageError("market data worker is still stopping")
        with self.condition:
            self._save(allow_closed=True)
        if collector_error:
            raise collector_error
