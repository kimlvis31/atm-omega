"""Worker-owned opt-in TS margin session over frozen closed-minute frames.

No exchange/source access is performed here. OHLCV prices are explicitly proxy
marks, full fills only; optional funding is an assumed static rate scenario.
"""

import json
import os
import re
import stat
import threading
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import fields
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from omega.analysis.catalog import create_execution_features, create_features
from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_strategy import (
    MarginStrategyAdapter,
    StagedProtectionDecision,
    margin_bar_digest,
)
from omega.application.virtual_margin import VirtualMarginEngine
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MAX_TIME,
    FillEvent,
    MarginBracket,
    MarginBracketSnapshot,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import ExecutionConstraints, TradeDecision
from omega.research.artifacts import _status_descriptor
from omega.runtime.control import InstanceLock
from omega.runtime.margin_cleanup import MarginCleanup
from omega.runtime.margin_console import initial_account
from omega.runtime.margin_ledger import LedgerPages, LedgerQuery
from omega.runtime.margin_session_costs import LIMITATION as COST_LIMITATION
from omega.runtime.margin_session_costs import MarginSessionCosts, prepare_session_costs
from omega.runtime.margin_session_features import MarginSessionFeatures, prepare_external_bindings
from omega.runtime.margin_session_funding import (
    LIMITATION as FUNDING_LIMITATION,
)
from omega.runtime.margin_session_funding import MarginSessionFunding, prepare_session_funding
from omega.runtime.margin_session_inputs import MarginSessionInputs, destination_id
from omega.runtime.margin_store import FileMarginStateStore, account_document
from omega.runtime.margin_strategy_store import FileMarginStrategyStore
from omega.runtime.settings import atomic_json
from omega.runtime.virtual_margin_journal import FileVirtualMarginJournal
from omega.strategies.registry import StrategyRegistry

MAX_BYTES = 32_000_000
LIMITATION = "OHLCV proxy marks; full fills; zero spread/slippage; no funding/liquidation"


def _json(value):
    result = json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))
    if len(result.encode()) > MAX_BYTES:
        raise StorageError("margin session checkpoint exceeds bounded capacity")
    return result


def _digest(value):
    return sha256(_json(value).encode()).hexdigest()


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate session field")
        result[key] = value
    return result


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid margin session timestamp")


def _instrument(value):
    if type(value) is not str:
        raise ValidationError("session instrument must be a native key")
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (ValueError, TypeError):
        raise ValidationError("invalid margin session instrument") from None
    if result.key != value:
        raise ValidationError("noncanonical margin session instrument")
    return result


def _money(value):
    if type(value) not in {str, int}:
        raise ValidationError("margin session numbers require decimal strings")
    result = Decimal(value)
    if (
        not result.is_finite()
        or len(result.as_tuple().digits) > 18
        or not -18 <= result.as_tuple().exponent <= 18
    ):
        raise ValidationError("invalid bounded margin session number")
    return result


def _candle(value):
    if (
        type(value) is not dict
        or set(value) != {"instrument", "open_time_ms", "close_time_ms", "values"}
        or type(value["values"]) is not list
        or len(value["values"]) != 5
    ):
        raise ValidationError("invalid frozen margin session candle")
    _time(value["open_time_ms"])
    _time(value["close_time_ms"])
    return Candle(
        _instrument(value["instrument"]),
        value["open_time_ms"],
        value["close_time_ms"],
        *(_money(item) for item in value["values"]),
    )


def _candle_document(candle):
    return {
        "instrument": candle.instrument.key,
        "open_time_ms": candle.open_time_ms,
        "close_time_ms": candle.close_time_ms,
        "values": [str(getattr(candle, key)) for key in ("open", "high", "low", "close", "volume")],
    }


def _bracket_document(value):
    return {
        "instrument": value.instrument.key,
        "observed_at_ms": value.observed_at_ms,
        "available_at_ms": value.available_at_ms,
        "source": value.source,
        "checksum": value.checksum,
        "rows": [
            [
                str(row.notional_floor),
                str(row.notional_cap),
                str(row.maintenance_rate),
                str(row.maintenance_amount),
                row.max_leverage,
            ]
            for row in value.rows
        ],
    }


def _bracket(value):
    if (
        type(value) is not dict
        or set(value)
        != {"instrument", "observed_at_ms", "available_at_ms", "source", "checksum", "rows"}
        or type(value["rows"]) is not list
        or not 1 <= len(value["rows"]) <= 256
    ):
        raise ValidationError("invalid frozen margin bracket document")
    rows = []
    for row in value["rows"]:
        if type(row) is not list or len(row) != 5:
            raise ValidationError("invalid frozen margin bracket row")
        rows.append(MarginBracket(*(_money(item) for item in row[:4]), row[4]))
    return MarginBracketSnapshot(
        _instrument(value["instrument"]),
        tuple(rows),
        value["observed_at_ms"],
        value["available_at_ms"],
        value["source"],
        value["checksum"],
    )


def prepare_margin_configuration(
    account_document,
    strategies,
    start_ms,
    *,
    registry=None,
    fee_rate="0.001",
    funding=None,
    cost_policy=None,
    external_feature_provider=None,
    verify_external_provider=True,
):
    """Validate/prepare trusted TS data before creating any financial namespace.

    Returns (initial_account, fee_Decimal, rows, modules, graphs, constraints).
    No wallet, journal, session/TS checkpoint or filesystem owner is created.
    """
    _time(start_ms)
    if start_ms % 60000 or type(strategies) is not list or not 1 <= len(strategies) <= 100:
        raise ValidationError("invalid bounded margin session configuration")
    initial = initial_account(account_document)
    prepare_session_costs(cost_policy)
    prepare_session_funding(
        funding, tuple(position.instrument for position in initial.positions), start_ms
    )
    fee = _money(fee_rate)
    if not 0 <= fee <= 1:
        raise ValidationError("fee rate must be in [0,1]")
    registry = registry or StrategyRegistry()
    modules, graphs, constraints = {}, {}, {}
    positions = {position.instrument.key: position for position in initial.positions}
    rows, identities, seen = [], set(), set()
    for raw in strategies:
        if type(raw) is not dict or set(raw) - {"external_features", "analysis_execution"} != {
            "id",
            "instrument",
            "module",
            "configuration",
            "analysis",
            "constraints",
        }:
            raise ValidationError("margin session strategy schema mismatch")
        instrument = _instrument(raw["instrument"])
        position = positions.get(instrument.key)
        identity = raw["id"]
        if (
            type(identity) is not str
            or identity in identities
            or instrument in seen
            or position is None
            or position.owner_id != identity
        ):
            raise ValidationError(
                "each margin instrument requires exactly one matching strategy owner"
            )
        identities.add(identity)
        seen.add(instrument)
        if type(raw["constraints"]) is not dict or set(raw["constraints"]) - {
            field.name for field in fields(ExecutionConstraints)
        }:
            raise ValidationError("invalid session execution constraints")
        values = {
            key: (value if key == "can_short" or value is None else _money(value))
            for key, value in raw["constraints"].items()
        }
        values.setdefault("leverage", Decimal(position.leverage))
        values.setdefault("can_short", True)
        rules = ExecutionConstraints(**values)
        if rules.leverage != position.leverage:
            raise ValidationError("configured strategy leverage differs from account")
        module = registry.create(raw["module"], instrument, raw["configuration"])
        if any(
            not callable(getattr(module, name, None))
            for name in ("evaluate", "checkpoint", "restore", "resume")
        ):
            raise ValidationError("strategy lacks durable checkpoint/restore capability")
        checkpoint = _json(module.checkpoint())
        restored = deepcopy(module)
        restored.restore(json.loads(checkpoint))
        if (
            _json(restored.checkpoint()) != checkpoint
            or getattr(restored, "last_open_time_ms", None) is not None
        ):
            raise ValidationError("strategy does not restore a fresh checkpoint exactly")
        _, analysis_fields = prepare_external_bindings(
            raw,
            instrument,
            module,
            external_feature_provider,
            verify_provider=verify_external_provider,
        )
        execution_plan = None
        if "analysis_execution" in raw:
            graph, execution_plan = create_execution_features(
                instrument, analysis_fields, raw["analysis"], raw["analysis_execution"]
            )
            # Freeze only data. The complete caller profile remains in the original
            # strategy document; no saved CA or default graph is rewritten.
            execution_plan = json.loads(_json(execution_plan))
        else:
            graph = create_features(instrument, analysis_fields, raw["analysis"])
        channels = set(graph.descriptor.required_fields) - {"OHLCV"}
        if channels - {"trade_flow", "depth", "metric"}:
            raise ValidationError(
                "margin auxiliary analysis supports native trade_flow/depth/metric only"
            )
        if channels and not callable(getattr(module, "evaluate_protection", None)):
            raise ValidationError("auxiliary-input strategy needs an independent protection method")
        modules[identity], graphs[identity], constraints[identity] = (
            module,
            graph,
            rules,
        )
        row = {"id": identity, "instrument": instrument}
        if execution_plan is not None:
            row["analysis_execution_plan"] = execution_plan
        rows.append(row)
    if seen != {position.instrument for position in initial.positions}:
        raise ValidationError("session strategies must cover every configured account instrument")
    # Validate the actual graph's public metric policy before any financial namespace.
    MarginSessionInputs(tuple(rows), graphs)
    return (
        initial,
        fee,
        tuple(sorted(rows, key=lambda item: item["id"])),
        modules,
        graphs,
        constraints,
    )


class MarginTradingSession:
    supports_cost_policy = True
    analysis_execution_scope = "required_fields_v1"

    def __init__(
        self,
        root,
        account_document,
        strategies,
        start_ms,
        *,
        registry=None,
        create=False,
        fee_rate="0.001",
        external_feature_provider=None,
        clock_ms=None,
        funding=None,
        cost_policy=None,
    ):
        _time(start_ms)
        if (
            start_ms % 60000
            or type(create) is not bool
            or type(strategies) is not list
            or not 1 <= len(strategies) <= 100
        ):
            raise ValidationError("invalid bounded margin session configuration")
        self._thread = threading.get_ident()
        self.root = Path(root).absolute()
        self._closed, self._error, self._uncertain = False, None, None
        self._state = "starting"
        self._meta = None
        self._shutdown = None
        self._resources = []
        self._cleanup = MarginCleanup()
        self._adapters, self._graphs, self._modules, self._rules = {}, {}, {}, {}
        self.execution = self.account_owner = None
        (self.initial, self.fee_rate, self._rows, self._modules, self._graphs, self._rules) = (
            prepare_margin_configuration(
                account_document,
                strategies,
                start_ms,
                registry=registry,
                fee_rate=fee_rate,
                funding=funding,
                **({"cost_policy": cost_policy} if cost_policy is not None else {}),
                external_feature_provider=external_feature_provider,
                verify_external_provider=create,
            )
        )
        self._inputs = MarginSessionInputs(self._rows, self._graphs)
        self._external = MarginSessionFeatures(
            strategies, self._rows, self._modules, external_feature_provider, clock_ms
        )
        self._funding = MarginSessionFunding(funding, self.initial, start_ms)
        self._costs = MarginSessionCosts(cost_policy)
        self._schema_version = (
            6
            if self._inputs.metric_policies
            else 5
            if self._inputs.enabled
            else 4
            if self._costs.enabled
            else 3
            if self._funding.enabled
            else 2
            if self._external.enabled
            else 1
        )
        self.account_id = self.initial.account_id
        self.start_ms = start_ms
        self.instruments = tuple(
            sorted(self.initial.positions, key=lambda item: item.instrument.key)
        )
        configuration = {
            "account": account_document,
            "strategies": strategies,
            "start_ms": start_ms,
            "fee_rate": str(self.fee_rate),
            "model": "ohlcv_margin_cost_session_v1"
            if self._costs.enabled
            else "ohlcv_margin_funding_session_v1"
            if self._funding.enabled
            else "ohlcv_margin_external_session_v1"
            if self._external.enabled
            else "ohlcv_margin_session_v1",
        }
        if self._funding.enabled:
            configuration["funding"] = self._funding.configuration
        if self._costs.enabled:
            configuration["cost_policy"] = self._costs.configuration
        if self._inputs.enabled:
            configuration["market_inputs"] = {
                "channels": {
                    key: list(value) for key, value in self.analysis_input_channels.items()
                }
            }
            if self._inputs.metric_policies:
                configuration["market_inputs"]["metric_policies"] = self.analysis_metric_policies
        execution_plans = {}
        for row in self._rows:
            if "analysis_execution_plan" not in row:
                continue
            descriptor = self._modules[row["id"]].descriptor
            execution_plans[row["id"]] = {
                "plan": deepcopy(row["analysis_execution_plan"]),
                "module_descriptor": {
                    "module_id": descriptor.module_id,
                    "api_version": descriptor.api_version,
                    "required_fields": sorted(descriptor.required_fields),
                    "stateful": descriptor.stateful,
                },
            }
        if execution_plans:
            # This is an explicit new execution configuration, not a migration of
            # the financial/checkpoint codec or an adoption of an older session.
            configuration["analysis_execution"] = execution_plans
        self.config_hash = _digest(configuration)
        self._ledger_pages = LedgerPages(
            _digest((os.path.normcase(str(self.root.resolve())), self.config_hash))
        )
        name = sha256(self.account_id.encode()).hexdigest()
        self.strategy_root = self.root / "state/margin-strategy-accounts" / name
        self.path = self.root / "state/margin-sessions" / (name + ".json")
        self.lock_path = self.path.with_suffix(".lock")
        try:
            self._guard()
            lease = InstanceLock(self.lock_path)
            self._resources.append(lease)
            self._cleanup.add("session_lease", lease, final=True, released=lambda: lease.released)
            previous = self._read()
            if previous is None:
                if not create:
                    raise StorageError(
                        "margin session metadata is missing; automatic initialization forbidden"
                    )
                self._persist(
                    {
                        "schema_version": self._schema_version,
                        "config_hash": self.config_hash,
                        "configuration": configuration,
                        "creating": True,
                        "cursor_ms": None,
                        "graphs": {key: graph.checkpoint() for key, graph in self._graphs.items()},
                        "graph_errors": {},
                        "pending": None,
                        "last_frame": None,
                        **({"auxiliary_destination": None} if self._inputs.enabled else {}),
                    }
                )
            else:
                self._validate_meta(previous)
                self._meta = previous
                if previous["creating"] and not create:
                    raise StorageError(
                        "incomplete explicit creation requires its original creation intent"
                    )
            creating = self._meta["creating"]
            journal = FileVirtualMarginJournal(self.root, self.account_id)
            self._resources.append(journal)
            self._cleanup.add("journal", journal, released=lambda: journal.released)
            store = FileMarginStateStore(self.root, self.account_id)
            self._resources.append(store)
            self._cleanup.add("account_store", store, released=lambda: store.released)
            current = store.load(self.account_id)
            if creating and current is not None and current != self.initial:
                raise ProtocolError(
                    "incomplete session creation conflicts with existing financial facts"
                )
            self.account_owner = MarginAccountOwner(self.initial, store, create_if_missing=creating)
            self.execution = VirtualMarginEngine(
                self.account_owner, journal, create_if_missing=creating
            )
            if self.execution.snapshot().state != "ready":
                raise StorageError("margin financial state requires reconciliation")
            self._funding.validate_history(self._meta, self.execution)
            for row in self._rows:
                identity = row["id"]
                cp_store = FileMarginStrategyStore(self.strategy_root, identity)
                self._resources.append(cp_store)
                self._cleanup.add(
                    "strategy_store:" + identity,
                    cp_store,
                    released=lambda resource=cp_store: resource.released,
                )
                adapter = MarginStrategyAdapter(
                    identity,
                    row["instrument"],
                    self._modules[identity],
                    self.execution,
                    cp_store,
                    owner_id=identity,
                    create_if_missing=creating,
                    **self._costs.adapter_options(),
                    entry_authorizer=(
                        lambda action, identity=identity: self._authorize_external_action(
                            identity, action
                        )
                    )
                    if identity in self._external.bindings
                    else None,
                    entry_guard=(
                        lambda action, identity=identity: self._guard_external_action(
                            identity, action
                        )
                    )
                    if identity in self._external.bindings
                    else None,
                )
                if adapter.snapshot().state not in {"ready", "pending"}:
                    raise StorageError("margin strategy checkpoint requires reconciliation")
                self._adapters[identity] = adapter
            self._restore_graphs(self._meta["graphs"])
            self._validate_adapter_anchors()
            if creating:
                final = deepcopy(self._meta)
                final["creating"] = False
                self._persist(final)
            self._state = "ready"
            from omega.runtime.margin_session_shutdown import MarginSessionShutdown

            self._shutdown = MarginSessionShutdown(self)
        except BaseException:
            self.close()
            raise

    @property
    def analysis_input_channels(self):
        return dict(self._inputs.channels)

    @property
    def analysis_metric_policies(self):
        """Detached frozen selection policy; no settings/source/financial authority."""
        return {key: policy.document() for key, policy in self._inputs.metric_policies.items()}

    def _owner(self):
        if threading.get_ident() != self._thread or self._closed:
            raise TradingBlocked("margin session is closed or belongs to another worker")

    def _guard(self):
        for path in (self.root, self.root / "state", self.path.parent, self.path, self.lock_path):
            if path.is_symlink() or path.is_junction():
                raise StorageError("margin session cannot use linked paths")
        for path in (self.root, self.root / "state", self.path.parent):
            if path.exists() and not path.is_dir():
                raise StorageError("margin session parent is not a directory")

    def _read(self):
        self._guard()
        descriptor = None
        try:
            descriptor = _status_descriptor(self.path)
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or getattr(details, "st_file_attributes", 0) & 0x400
                or details.st_size > MAX_BYTES
            ):
                raise StorageError("margin session metadata must be a bounded regular file")
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                raw = stream.read(MAX_BYTES + 1)
        except FileNotFoundError:
            return None
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_BYTES:
            raise StorageError("margin session metadata exceeds capacity")
        try:
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if (
                type(envelope) is not dict
                or set(envelope) != {"data", "sha256"}
                or _digest(envelope["data"]) != envelope["sha256"]
            ):
                raise ValueError("session checksum mismatch")
            return envelope["data"]
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise StorageError("margin session metadata is corrupt") from None

    def _persist(self, candidate):
        self._guard()
        if candidate == self._meta:
            return
        self._uncertain = (deepcopy(self._meta), deepcopy(candidate))
        frozen_candidate = self._uncertain[1]
        try:
            envelope = {"data": frozen_candidate, "sha256": _digest(frozen_candidate)}
            _json(envelope)
            atomic_json(self.path, envelope)
        except Exception:
            self._state, self._error = "unknown", "session_commit_unknown"
            raise StorageError("margin session commit outcome is unknown") from None
        # The detached recovery snapshot becomes owned state only after fsync.
        self._meta, self._uncertain = frozen_candidate, None

    def _validate_meta(self, data):
        keys = {
            "schema_version",
            "config_hash",
            "configuration",
            "creating",
            "cursor_ms",
            "graphs",
            "graph_errors",
            "pending",
            "last_frame",
        }
        if self._inputs.enabled:
            keys.add("auxiliary_destination")
        if (
            type(data) is not dict
            or set(data) != keys
            or type(data["schema_version"]) is not int
            or data["schema_version"] != self._schema_version
            or data["config_hash"] != self.config_hash
            or _digest(data["configuration"]) != self.config_hash
            or type(data["creating"]) is not bool
            or type(data["graphs"]) is not dict
            or set(data["graphs"]) != set(self._graphs)
            or type(data["graph_errors"]) is not dict
            or set(data["graph_errors"]) - set(self._graphs)
            or any(type(value) is not str for value in data["graph_errors"].values())
        ):
            raise ProtocolError("margin session configuration/checkpoint schema changed")
        cursor = data["cursor_ms"]
        if cursor is not None:
            _time(cursor)
            if cursor < self.start_ms or cursor % 60000:
                raise ProtocolError("invalid margin session cursor")
        if (cursor is None) != (data["last_frame"] is None):
            raise ProtocolError("margin session cursor lacks its immutable candle anchor")
        if data["last_frame"] is not None:
            self._validate_frame(data["last_frame"], cursor)
            if (
                data["last_frame"]["phase"] != "finish"
                or data["graphs"] != data["last_frame"]["graphs"]
                or data["graph_errors"] != data["last_frame"]["graph_errors"]
            ):
                raise ProtocolError("consumed graph checkpoint lacks a completed frame authority")
        if data["pending"] is not None:
            self._validate_frame(
                data["pending"], self.start_ms if cursor is None else cursor + 60000
            )
        if self._inputs.enabled:
            destination = data["auxiliary_destination"]
            if destination is not None:
                destination_id(destination)
            for frame in (data["last_frame"], data["pending"]):
                if frame is not None and frame["market_inputs"]["destination"] != destination:
                    raise ProtocolError("saved margin auxiliary destination changed")
        if data["creating"] and (cursor is not None or data["pending"] is not None):
            raise ProtocolError("incomplete creation cannot contain financial progress")
        for row in self._rows:
            identity = row["id"]
            restored = self._graphs[identity].restored(data["graphs"][identity], row["instrument"])
            if (
                identity not in data["graph_errors"]
                and getattr(restored, "last_open_time_ms", cursor) != cursor
            ):
                raise ProtocolError(
                    "active analysis checkpoint differs from consumed session cursor"
                )
            if cursor is None and data["graphs"][identity] != self._graphs[identity].checkpoint():
                raise ProtocolError("uninitialized session contains unreferenced graph history")

    def _validate_frame(self, frame, opening):
        keys = {
            "candles",
            "as_of_ms",
            "brackets",
            "entries_allowed",
            "digest",
            "graphs",
            "features",
            "graph_errors",
            "graph_ready",
            "phase",
            "gates",
            "done",
            "after_done",
            "after_cycles",
        }
        if self._inputs.enabled:
            keys.add("market_inputs")
        if self._external.enabled:
            keys.add("external_features")
        if self._funding.enabled:
            keys.add("funding")
        if self._costs.enabled:
            keys.add("cost_policy")
        if (
            type(frame) is not dict
            or set(frame) != keys
            or type(frame["entries_allowed"]) is not bool
            or type(frame["candles"]) is not list
            or len(frame["candles"]) != len(self.instruments)
            or type(frame["brackets"]) is not list
            or len(frame["brackets"]) > 100
        ):
            raise ProtocolError("invalid frozen margin session frame")
        _time(frame["as_of_ms"])
        bars = self._bars(
            tuple(_candle(value) for value in frame["candles"]), frame["as_of_ms"], opening
        )
        selected = (
            self._inputs.restore(frame["market_inputs"], bars, as_of_ms=frame["as_of_ms"])
            if self._inputs.enabled
            else {}
        )
        tables = tuple(_bracket(value) for value in frame["brackets"])
        if len(tables) > 100 or len({item.instrument for item in tables}) != len(tables):
            raise ProtocolError("invalid frozen margin brackets")
        if frame["digest"] != self._frame_digest(frame):
            raise ProtocolError("frozen session input digest changed")
        if self._costs.enabled:
            self._costs.validate_frame(frame["cost_policy"])
        self._external.validate(frame, bars, account_id=self.account_id)
        if self._funding.enabled:
            self._funding.validate(frame["funding"], bars, as_of_ms=frame["as_of_ms"])
        if (
            type(frame["graphs"]) is not dict
            or set(frame["graphs"]) != set(self._graphs)
            or type(frame["features"]) is not dict
            or set(frame["features"]) != set(self._graphs)
            or type(frame["graph_ready"]) is not dict
            or set(frame["graph_ready"]) != set(self._graphs)
            or any(type(value) is not bool for value in frame["graph_ready"].values())
            or type(frame["graph_errors"]) is not dict
            or set(frame["graph_errors"]) - set(self._graphs)
            or any(
                type(value) is not str or not 1 <= len(value) <= 128
                for value in frame["graph_errors"].values()
            )
            or frame["phase"]
            not in (
                {"funding", "open", "decisions", "close", "after", "finish"}
                if self._funding.enabled
                else {"open", "decisions", "close", "after", "finish"}
            )
            or type(frame["gates"]) is not dict
            or set(frame["gates"]) - set(self._graphs)
            or any(type(value) is not bool for value in frame["gates"].values())
            or type(frame["after_cycles"]) is not dict
            or set(frame["after_cycles"]) - set(self._graphs)
            or any(
                type(value) is not int or not 1 <= value <= 31
                for value in frame["after_cycles"].values()
            )
            or any(
                type(frame[key]) is not list
                or len(frame[key]) != len(set(frame[key]))
                or set(frame[key]) - set(self._graphs)
                for key in ("done", "after_done")
            )
        ):
            raise ProtocolError("invalid frozen session computation/progress")
        identities = [row["id"] for row in self._rows]
        if (
            frame["done"] != identities[: len(frame["done"])]
            or frame["after_done"] != identities[: len(frame["after_done"])]
            or set(frame["done"]) - set(frame["gates"])
            or set(frame["after_done"]) - set(frame["after_cycles"])
            or frame["phase"] in {"funding", "open", "decisions"}
            and (frame["after_done"] or frame["after_cycles"])
            or frame["phase"] in {"funding", "open"}
            and (frame["done"] or frame["gates"])
            or frame["phase"] in {"close", "after", "finish"}
            and frame["done"] != identities
            or frame["phase"] == "close"
            and (frame["after_done"] or frame["after_cycles"])
            or frame["phase"] == "finish"
            and frame["after_done"] != identities
        ):
            raise ProtocolError("frozen session phase progress is inconsistent")
        # Restore every candidate before financial work, including codec/config validation.
        for row in self._rows:
            identity = row["id"]
            restored = self._graphs[identity].restored(frame["graphs"][identity], row["instrument"])
            if (
                identity not in frame["graph_errors"]
                and getattr(restored, "last_open_time_ms", opening) != opening
            ):
                raise ProtocolError("frozen graph cursor differs from its candle")
            if (
                identity not in frame["graph_errors"]
                and getattr(restored, "output_values", frame["features"][identity])
                != frame["features"][identity]
            ):
                raise ProtocolError("frozen analysis outputs differ from checkpoint")
            bar = next(item for item in bars if item.instrument == row["instrument"])
            if (
                identity not in frame["graph_errors"]
                and getattr(restored, "last_candle", bar) != bar
            ):
                raise ProtocolError("analysis checkpoint candle differs from frozen source")
            if identity in self._inputs.strategies and identity not in frame["graph_errors"]:
                if getattr(restored, "last_input_frame", None) != selected[row["instrument"].key]:
                    raise ProtocolError("analysis checkpoint differs from frozen auxiliary inputs")
                if frame["graph_ready"][identity] != (
                    restored.calculation_ready and restored.input_ready
                ):
                    raise ProtocolError("frozen auxiliary analysis readiness changed")
            if identity in frame["graph_errors"] and (
                frame["features"][identity] != {} or frame["graph_ready"][identity]
            ):
                raise ProtocolError("failed analysis cannot publish ready features")
        return bars

    def _validate_adapter_anchors(self):
        for row in self._rows:
            identity = row["id"]
            state = self._adapters[identity].snapshot().checkpoint
            for opening, digest in (
                (state.last_open_time_ms, state.last_bar_digest),
                (
                    None
                    if state.pending is None or type(state.pending) is StagedProtectionDecision
                    else state.pending.open_time_ms,
                    None
                    if state.pending is None or type(state.pending) is StagedProtectionDecision
                    else state.pending.bar_digest,
                ),
            ):
                if opening is None:
                    if state.last_open_time_ms is None and self._meta["cursor_ms"] is not None:
                        raise ProtocolError(
                            "strategy checkpoint is behind consumed financial history"
                        )
                    continue
                frame = next(
                    (
                        item
                        for item in (self._meta["pending"], self._meta["last_frame"])
                        if item is not None and item["candles"][0]["open_time_ms"] == opening
                    ),
                    None,
                )
                if frame is None:
                    raise ProtocolError("strategy checkpoint has no frozen frame authority")
                bar = next(
                    _candle(item)
                    for item in frame["candles"]
                    if item["instrument"] == row["instrument"].key
                )
                if digest != margin_bar_digest(
                    bar,
                    self._external.decision_features(frame, identity),
                    self._rules[identity],
                    frame["digest"],
                ):
                    raise ProtocolError(
                        "strategy checkpoint input digest differs from session authority"
                    )

    def _restore_graphs(self, states):
        self._graphs = {
            row["id"]: self._graphs[row["id"]].restored(states[row["id"]], row["instrument"])
            for row in self._rows
        }

    def _bars(self, candles, as_of_ms, opening):
        if (
            type(candles) is not tuple
            or len(candles) != len(self.instruments)
            or any(not isinstance(bar, Candle) for bar in candles)
            or {bar.instrument for bar in candles}
            != {position.instrument for position in self.instruments}
            or len({bar.instrument for bar in candles}) != len(candles)
            or any(bar.open_time_ms != opening or bar.close_time_ms > as_of_ms for bar in candles)
        ):
            raise ValidationError(
                "session requires one common immutable closed minute for every account instrument"
            )
        for bar in candles:
            if _candle(_candle_document(bar)) != bar:
                raise ValidationError("session candle does not fit the exact frozen model")
        return tuple(sorted(candles, key=lambda value: value.instrument.key))

    def _frame(
        self,
        candles,
        as_of_ms,
        brackets,
        entries_allowed,
        *,
        prepared_frames=None,
        auxiliary_destination=None,
    ):
        opening = (
            self.start_ms if self._meta["cursor_ms"] is None else self._meta["cursor_ms"] + 60000
        )
        candles = self._bars(candles, as_of_ms, opening)
        if (
            type(brackets) is not tuple
            or len(brackets) > 100
            or any(not isinstance(item, MarginBracketSnapshot) for item in brackets)
            or len({item.instrument for item in brackets}) != len(brackets)
        ):
            raise ValidationError("invalid explicit margin bracket snapshots")
        selected, input_document = {}, None
        if self._inputs.enabled:
            input_document, selected = self._inputs.freeze(
                prepared_frames,
                candles,
                as_of_ms=as_of_ms,
                destination=auxiliary_destination,
                expected=self._meta["auxiliary_destination"],
            )
        by_instrument = {bar.instrument: bar for bar in candles}
        states, outputs, ready, errors = {}, {}, {}, deepcopy(self._meta["graph_errors"])
        for row in self._rows:
            identity = row["id"]
            candidate = deepcopy(self._graphs[identity])
            try:
                if identity in errors:
                    raise ValidationError("analysis requires explicit preparation after failure")
                if identity in self._inputs.strategies:
                    outputs[identity] = candidate.update_frame(selected[row["instrument"].key])
                    input_ready = candidate.input_ready
                    if type(input_ready) is not bool:
                        raise ValidationError("invalid auxiliary input readiness metadata")
                    ready[identity] = candidate.calculation_ready and input_ready
                else:
                    outputs[identity] = candidate.update(by_instrument[row["instrument"]])
                    ready[identity] = getattr(candidate, "calculation_ready", True)
                if type(ready[identity]) is not bool:
                    raise ValidationError("invalid analysis readiness metadata")
                states[identity] = candidate.checkpoint()
            except Exception as error:
                outputs[identity], ready[identity] = {}, False
                states[identity] = deepcopy(self._meta["graphs"][identity])
                errors.setdefault(identity, type(error).__name__)
        frame = {
            "candles": [_candle_document(bar) for bar in candles],
            "as_of_ms": as_of_ms,
            "brackets": [_bracket_document(item) for item in brackets],
            "entries_allowed": entries_allowed,
            "graphs": states,
            "features": outputs,
            "graph_ready": ready,
            "graph_errors": errors,
            "phase": "funding" if self._funding.enabled else "open",
            "gates": {},
            "done": [],
            "after_done": [],
            "after_cycles": {},
        }
        if self._inputs.enabled:
            frame["market_inputs"] = input_document
        if self._funding.enabled:
            frame["funding"] = self._funding.freeze(
                candles, as_of_ms=as_of_ms, financial=self.execution.snapshot()
            )
        if self._external.enabled:
            frame["external_features"] = self._external.freeze(candles, as_of_ms)
        if self._costs.enabled:
            frame["cost_policy"] = self._costs.frame_identity()
        frame["digest"] = self._frame_digest(frame)
        return frame

    def _frame_digest(self, frame):
        body = {key: frame[key] for key in ("candles", "as_of_ms", "brackets")}
        body["config_hash"] = self.config_hash
        if self._inputs.enabled:
            body["market_inputs"] = frame["market_inputs"]
        if self._external.enabled:
            body["external_features"] = self._external.digest_inputs(frame)
        if self._funding.enabled:
            body["funding"] = frame["funding"]
        if self._costs.enabled:
            body["cost_policy"] = frame["cost_policy"]
        return _digest(body)

    def _external_action_frame(self, identity, action):
        self._owner()
        frame = next(
            (
                item
                for item in (self._meta["pending"], self._meta["last_frame"])
                if item is not None and item["digest"] == action.input_reference
            ),
            None,
        )
        if frame is None:
            return None
        instrument = next(row["instrument"] for row in self._rows if row["id"] == identity)
        bar = next(
            _candle(item) for item in frame["candles"] if item["instrument"] == instrument.key
        )
        if action.bar_digest != margin_bar_digest(
            bar,
            self._external.decision_features(frame, identity),
            self._rules[identity],
            frame["digest"],
        ):
            return None
        return frame, bar

    def _authorize_external_action(self, identity, action):
        result = self._external_action_frame(identity, action)
        return result is not None and self._external.authorize(result[0], identity, result[1])

    @contextmanager
    def _guard_external_action(self, identity, action):
        result = self._external_action_frame(identity, action)
        if result is None:
            yield False
            return
        with self._external.entry_guard(result[0], identity, result[1]) as allowed:
            yield allowed

    def _progress(self, **changes):
        candidate = deepcopy(self._meta)
        candidate["pending"].update(changes)
        self._persist(candidate)

    def _phase(self, frame, candles, brackets, phase):
        self.execution.reconcile()
        existing = self.execution.committed_batches(candles[0].open_time_ms, phase)
        if len(existing) > 1:
            raise ProtocolError("multiple durable margin batches claim one session phase")
        if existing:
            batch = existing[0]
            if (
                batch.candles != candles
                or batch.as_of_ms != frame["as_of_ms"]
                or batch.brackets != brackets
            ):
                raise ProtocolError("durable batch differs from its frozen session frame")
            return
        plan = self.execution.prepare_batch(
            candles, as_of_ms=frame["as_of_ms"], phase=phase, brackets=brackets
        )
        if plan.facts or plan.rejections:
            self.execution.commit_batch(plan)

    def _decision(self, row, frame, bars, brackets, *, gate, cycle):
        identity = row["id"]
        adapter = self._adapters[identity]
        if adapter.snapshot().state != "ready":
            adapter.reconcile()
        cp = adapter.snapshot().checkpoint
        bar = next(item for item in bars if item.instrument == row["instrument"])
        if cp.last_open_time_ms == bars[0].open_time_ms and cp.last_cycle >= cycle:
            if cp.last_bar_digest != margin_bar_digest(
                bar,
                self._external.decision_features(frame, identity),
                self._rules[identity],
                frame["digest"],
            ):
                raise ProtocolError(
                    "consumed strategy checkpoint differs from frozen session inputs"
                )
            return cp.last_decision
        marks = tuple(
            MarkSnapshot(
                item.instrument,
                item.close,
                item.close_time_ms,
                frame["as_of_ms"],
                "session_closed_ohlcv_proxy",
            )
            for item in bars
        )
        return adapter.evaluate_closed(
            bar,
            self._external.decision_features(frame, identity),
            self._rules[identity],
            as_of_ms=frame["as_of_ms"],
            marks=marks,
            brackets=brackets,
            worst_execution_price=bar.close,
            fee_rate=self.fee_rate,
            entries_allowed=gate,
            input_reference=frame["digest"],
            decision_cycle=cycle,
            protection_only=(
                identity in self._external.bindings
                and not self._external.authorize(frame, identity, bar)
            )
            or (identity in self._inputs.strategies and not frame["graph_ready"][identity]),
        )

    def _finish(self, *, actual_as_of_ms, entries_allowed):
        frame = self._meta["pending"]
        bars = self._validate_frame(frame, self.snapshot()["next_open_ms"])
        pinned = self._external.pin(frame, bars, account_id=self.account_id)
        if pinned != frame:
            candidate = deepcopy(self._meta)
            candidate["pending"] = pinned
            self._persist(candidate)
            frame = self._meta["pending"]
        brackets = tuple(_bracket(item) for item in frame["brackets"])
        while True:
            frame = self._meta["pending"]
            if frame["phase"] == "funding":
                self._funding.settle(frame, self.execution)
                self._progress(phase="open")
            elif frame["phase"] == "open":
                self._phase(frame, bars, brackets, "open")
                self._progress(phase="decisions")
            elif frame["phase"] == "decisions":
                for row in self._rows:
                    identity = row["id"]
                    if identity in self._meta["pending"]["done"]:
                        continue
                    cp = self._adapters[identity].snapshot().checkpoint
                    consumed = (
                        cp.last_open_time_ms == bars[0].open_time_ms or cp.pending is not None
                    )
                    gates = deepcopy(self._meta["pending"]["gates"])
                    if not consumed:
                        gates[identity] = (
                            frame["entries_allowed"]
                            and entries_allowed
                            and frame["graph_ready"][identity]
                            and self._external.authorize(
                                frame,
                                identity,
                                next(bar for bar in bars if bar.instrument == row["instrument"]),
                            )
                            and actual_as_of_ms - bars[0].close_time_ms <= 120000
                        )
                        self._progress(gates=gates)
                    gate = gates.get(identity, False)
                    decision = self._decision(
                        row, self._meta["pending"], bars, brackets, gate=gate, cycle=0
                    )
                    if decision.cancel_pending:
                        self._decision(
                            row, self._meta["pending"], bars, brackets, gate=gate, cycle=1
                        )
                    self._progress(done=self._meta["pending"]["done"] + [identity])
                self._progress(phase="close")
            elif frame["phase"] == "close":
                self._phase(frame, bars, brackets, "close")
                self._progress(phase="after")
            elif frame["phase"] == "after":
                # A late limit fill can change a position at this close. Only a
                # concrete adapter proof permits the extra same-candle cycle.
                for row in self._rows:
                    identity = row["id"]
                    if identity in self._meta["pending"]["after_done"]:
                        continue
                    cp = self._adapters[identity].snapshot().checkpoint
                    cycles = deepcopy(self._meta["pending"]["after_cycles"])
                    if identity not in cycles:
                        cycles[identity] = cp.last_cycle + 1
                        self._progress(after_cycles=cycles)
                    try:
                        self._decision(
                            row, frame, bars, brackets, gate=False, cycle=cycles[identity]
                        )
                    except ProtocolError as error:
                        if "requires a concrete position/pending change" not in str(error):
                            raise
                    self._progress(after_done=self._meta["pending"]["after_done"] + [identity])
                self._progress(phase="finish")
            else:
                previous_frame = self._meta["last_frame"]
                final = deepcopy(self._meta)
                final.update(
                    cursor_ms=bars[0].open_time_ms,
                    graphs=frame["graphs"],
                    graph_errors=frame["graph_errors"],
                    last_frame=frame,
                    pending=None,
                )
                self._persist(final)
                self._restore_graphs(final["graphs"])
                if previous_frame is not None:
                    self._external.release(previous_frame, receipt_sha256=_digest(previous_frame))
                self._state, self._error = "ready", None
                return self.snapshot()

    def requires_immediate_protection(self, candles, *, as_of_ms, brackets=()):
        """Read-only wait admission; uncertainty never delays financial settlement."""
        self._owner()
        try:
            _time(as_of_ms)
            shutdown = self.shutdown_status()
            if (
                shutdown["recovery_required"]
                or shutdown["active"]
                or shutdown["needs_settlement"]
                or self._state != "ready"
                or self._uncertain is not None
                or self._meta["pending"] is not None
            ):
                return True
            financial = self.execution.snapshot()
            if (
                financial.state != "ready"
                or financial.account is None
                or financial.pending
                or any(entry.fact.available_at_ms > as_of_ms for entry in financial.account.ledger)
            ):
                return True
            if any(
                state.state != "ready"
                or state.checkpoint is None
                or state.checkpoint.pending is not None
                for state in (adapter.snapshot() for adapter in self._adapters.values())
            ):
                return True
            opening = (
                self.start_ms
                if self._meta["cursor_ms"] is None
                else self._meta["cursor_ms"] + 60000
            )
            bars = self._bars(candles, as_of_ms, opening)
            if (
                type(brackets) is not tuple
                or len(brackets) > 100
                or any(not isinstance(item, MarginBracketSnapshot) for item in brackets)
                or len({item.instrument for item in brackets}) != len(brackets)
            ):
                return True
            marks = tuple(
                MarkSnapshot(
                    bar.instrument,
                    bar.close,
                    bar.close_time_ms,
                    as_of_ms,
                    "session_closed_ohlcv_proxy",
                )
                for bar in bars
            )
            positions = {item.instrument: item for item in financial.account.positions}
            by_instrument = {bar.instrument: bar for bar in bars}
            for row in self._rows:
                if positions[row["instrument"]].quantity == 0:
                    continue
                identity = row["id"]
                if not any(
                    callable(getattr(self._adapters[identity].module, name, None))
                    for name in ("preview_protection", "evaluate_protection")
                ):
                    return True
                decision = self._adapters[identity].preview_protection(
                    by_instrument[row["instrument"]],
                    self._rules[identity],
                    as_of_ms=as_of_ms,
                    marks=marks,
                    brackets=brackets,
                )
                if (
                    not isinstance(decision, TradeDecision)
                    or decision.orders
                    or decision.cancel_pending
                ):
                    return True
            return False
        except Exception:
            return True

    def observe_protection(self, observations, *, as_of_ms, brackets=()):
        """Same financial owner, no bar/signal processing or fill simulation here."""
        from omega.domain.protection import ProtectionObservation

        self._owner()
        _time(as_of_ms)
        if (
            self._state != "ready"
            or self._uncertain is not None
            or self._meta["pending"] is not None
            or self.shutdown_status()["recovery_required"]
        ):
            raise TradingBlocked("partial protection cannot interleave pending financial frames")
        if (
            type(observations) is not tuple
            or len(observations) > len(self.instruments)
            or any(type(item) is not ProtectionObservation for item in observations)
            or len({item.instrument for item in observations}) != len(observations)
            or any(item.instrument not in self._rules_by_instrument() for item in observations)
        ):
            raise ValidationError("invalid selected protection observations")
        # Empty observation input first replays durable originals, before source IO.
        for adapter in self._adapters.values():
            state = adapter.snapshot()
            if state.state != "ready":
                adapter.reconcile(reject_unsubmitted_entries=True)
        financial = self.execution.snapshot()
        if financial.state != "ready" or financial.account is None:
            raise TradingBlocked("protection needs confirmed financial ownership")
        selected = {item.instrument: item for item in observations}
        last_frame = self._meta["last_frame"]
        bars = () if last_frame is None else tuple(_candle(item) for item in last_frame["candles"])
        marks = tuple(
            MarkSnapshot(
                bar.instrument, bar.close, bar.close_time_ms, as_of_ms, "session_closed_ohlcv_proxy"
            )
            for bar in bars
        )
        for row in self._rows:
            observation = selected.get(row["instrument"])
            adapter = self._adapters[row["id"]]
            position = next(
                item for item in financial.account.positions if item.instrument == row["instrument"]
            )
            if observation is None or not callable(
                getattr(adapter.module, "evaluate_protection", None)
            ):
                continue
            if position.quantity == 0:
                continue  # A partial price is never entry authority, even for custom TS.
            observation.current(as_of_ms)
            if min(observation.event_time_ms, observation.original_received_at_ms) < max(
                (
                    entry.fact.event_time_ms
                    for entry in financial.account.ledger
                    if isinstance(entry.fact, FillEvent)
                    and entry.fact.instrument == observation.instrument
                ),
                default=0,
            ):
                continue
            current_marks = tuple(
                item for item in marks if item.instrument != observation.instrument
            ) + (
                MarkSnapshot(
                    observation.instrument,
                    observation.last,
                    observation.event_time_ms,
                    observation.received_at_ms,
                    observation.source,
                ),
            )
            adapter.evaluate_observation(
                observation,
                self._rules[row["id"]],
                as_of_ms=as_of_ms,
                marks=current_marks,
                brackets=brackets,
                fee_rate=self.fee_rate,
            )
        return self.snapshot()

    def _rules_by_instrument(self):
        return {row["instrument"] for row in self._rows}

    def run_once(
        self,
        candles,
        *,
        as_of_ms,
        brackets=(),
        entries_allowed=True,
        prepared_frames=None,
        auxiliary_destination=None,
    ):
        self._owner()
        if self.shutdown_status()["recovery_required"]:
            raise TradingBlocked(
                "shutdown advanced finance; explicit strategy preparation is required"
            )
        _time(as_of_ms)
        if type(entries_allowed) is not bool or type(candles) is not tuple:
            raise ValidationError("invalid margin session run controls")
        if self._state != "ready" or self._uncertain is not None:
            raise TradingBlocked("margin session requires explicit reconciliation")
        if not self._inputs.enabled and (
            prepared_frames is not None or auxiliary_destination is not None
        ):
            raise ValidationError("OHLCV-only session cannot acquire auxiliary inputs")
        pending = self._meta["pending"]
        if pending is not None:
            if self._inputs.enabled:
                self._inputs.check_replay(
                    prepared_frames,
                    auxiliary_destination,
                    pending["market_inputs"],
                    tuple(_candle(value) for value in pending["candles"]),
                    as_of_ms=pending["as_of_ms"],
                )
            if as_of_ms < pending["as_of_ms"]:
                raise ProtocolError("actual session clock moved backwards")
            if (
                candles
                and [
                    _candle_document(bar)
                    for bar in sorted(candles, key=lambda value: value.instrument.key)
                ]
                != pending["candles"]
            ):
                raise ProtocolError("pending frozen session frame cannot be replaced")
            if brackets and [_bracket_document(item) for item in brackets] != pending["brackets"]:
                raise ProtocolError("pending frozen session brackets cannot be replaced")
        else:
            if not candles:
                raise ValidationError("empty replay requires a pending session frame")
            if self._meta["cursor_ms"] is not None and all(
                bar.open_time_ms == self._meta["cursor_ms"] for bar in candles
            ):
                if [
                    _candle_document(bar)
                    for bar in sorted(candles, key=lambda value: value.instrument.key)
                ] != self._meta["last_frame"]["candles"]:
                    raise ProtocolError("consumed session candle changed")
                if self._inputs.enabled:
                    last = self._meta["last_frame"]
                    self._inputs.check_replay(
                        prepared_frames,
                        auxiliary_destination,
                        last["market_inputs"],
                        candles,
                        as_of_ms=last["as_of_ms"],
                    )
                return self.snapshot()
            options = (
                {"prepared_frames": prepared_frames, "auxiliary_destination": auxiliary_destination}
                if self._inputs.enabled
                else {}
            )
            frame = self._frame(candles, as_of_ms, brackets, entries_allowed, **options)
            candidate = deepcopy(self._meta)
            candidate["pending"] = frame
            if self._inputs.enabled:
                candidate["auxiliary_destination"] = frame["market_inputs"]["destination"]
            self._persist(candidate)
        try:
            return self._finish(actual_as_of_ms=as_of_ms, entries_allowed=entries_allowed)
        except Exception as error:
            self._state, self._error = "unknown", type(error).__name__
            raise

    def reconcile(self):
        self._owner()
        if self._shutdown is not None and self.shutdown_status()["recovery_required"]:
            self.execution.reconcile()
            self._shutdown.reconcile()
            return self.snapshot()
        loaded = self._read()
        self._validate_meta(loaded)
        if self._uncertain is not None:
            previous, candidate = self._uncertain
            if loaded == candidate:
                self._meta, self._uncertain = candidate, None
            elif loaded == previous:
                self._persist(candidate)
            else:
                raise ProtocolError("margin session commit conflicts with external state")
        elif loaded != self._meta:
            raise ProtocolError("margin session metadata changed outside its owner")
        self.execution.reconcile()
        self._funding.validate_history(self._meta, self.execution)
        self.execution.pause_entries(
            _digest(
                (
                    self.config_hash,
                    self._meta["cursor_ms"],
                    None if self._meta["pending"] is None else self._meta["pending"]["digest"],
                    "reconcile_pause",
                )
            )
        )
        for adapter in self._adapters.values():
            if adapter.snapshot().state != "ready":
                adapter.reconcile(reject_unsubmitted_entries=True)
        self._state, self._error = "ready", None
        return self.snapshot()

    def resume(self, request_id):
        self._owner()
        if self.shutdown_status()["recovery_required"]:
            raise TradingBlocked(
                "shutdown advanced finance; explicit strategy preparation is required"
            )
        self.execution.manual_resume(request_id)
        return self.snapshot()

    def resume_strategy(self, request_id, strategy_id):
        """Explicitly release one local TS latch while keeping account entries paused."""
        self._owner()
        if (
            type(request_id) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", request_id) is None
            or type(strategy_id) is not str
            or strategy_id not in self._adapters
        ):
            raise ValidationError("explicit request and matching strategy owner required")
        if (
            self.shutdown_status()["recovery_required"]
            or self._state != "ready"
            or self._uncertain is not None
            or self._meta["pending"] is not None
            or self.execution.snapshot().state != "ready"
        ):
            raise TradingBlocked("strategy resume requires a reconciled complete session frame")
        states = tuple(adapter.snapshot() for adapter in self._adapters.values())
        if any(
            state.state != "ready"
            or state.checkpoint is None
            or state.checkpoint.pending is not None
            for state in states
        ):
            raise TradingBlocked("strategy resume requires every strategy checkpoint to be ready")
        try:
            self.execution.pause_entries(
                _digest((self.config_hash, request_id, strategy_id, "resume_strategy_pause"))
            )
            self._adapters[strategy_id].resume_module()
        except Exception:
            self._state, self._error = "unknown", "strategy_resume_unconfirmed"
            raise
        return self.snapshot()

    def pause(self, request_id):
        self._owner()
        self.execution.pause_entries(request_id)
        return self.snapshot()

    def shutdown_status(self):
        self._owner()
        if self._shutdown is None:
            cursor = None if self._meta is None else self._meta["cursor_ms"]
            return {
                "operation_id": None,
                "state": "idle",
                "active": False,
                "needs_settlement": False,
                "next_open_ms": self.start_ms if cursor is None else cursor + 60000,
                "recovery_required": False,
            }
        return self._shutdown.status()

    def shutdown_preflight(self, operation_id, issued_at_ms, *, brackets=()):
        self._owner()
        return self._shutdown.preflight(operation_id, issued_at_ms, brackets=brackets)

    def shutdown_drain(self, operation_id, issued_at_ms, *, brackets=()):
        self._owner()
        return self._shutdown.drain(operation_id, issued_at_ms, brackets=brackets)

    def shutdown_cancel(self, operation_id):
        self._owner()
        return self._shutdown.cancel(operation_id)

    def shutdown_run_once(self, candles, *, as_of_ms, brackets=()):
        self._owner()
        return self._shutdown.run_once(candles, as_of_ms=as_of_ms, brackets=brackets)

    def cost_quote_summaries(self, *, limit=10000):
        """Forward confirmed source observations; do not evaluate models or reprice."""
        self._owner()
        return self.execution.cost_quote_summaries(limit=limit)

    def ledger_page(self, *, offset=0, limit=128, snapshot=None, start_ms=None, end_ms=None):
        """Page a confirmed immutable prefix, without model/source/file access."""
        self._owner()
        query = LedgerQuery(offset, limit, snapshot, start_ms, end_ms)
        anchor, account = self.execution.ledger_observation()
        return self._ledger_pages.page(anchor, account, query)

    def retirement_snapshot(self, registered_parameters):
        """Observe certain flat state without model/graph callbacks or new IO."""
        from omega.runtime.margin_configuration import configuration_parameters
        from omega.runtime.margin_retirement import registered_session_configuration

        self._owner()
        parameters, parameters_sha = configuration_parameters(
            registered_parameters, account_id=self.account_id
        )
        if (
            self._meta is None
            or self._meta.get("creating") is not False
            or self._meta.get("pending") is not None
            or self._uncertain is not None
        ):
            raise TradingBlocked("retirement requires a confirmed idle session frame")
        original = self._meta.get("configuration")
        if (
            type(original) is not dict
            or registered_session_configuration(parameters, original.get("model")) != original
            or _digest(original) != self.config_hash
        ):
            raise TradingBlocked(
                "registered retirement parameters differ from original session configuration"
            )
        financial = self.execution.snapshot()
        if financial.state != "ready" or financial.paused is not True or financial.pending:
            raise TradingBlocked("retirement requires confirmed paused execution and no orders")
        anchor, tip, count, chain, account = self.execution.confirmed_source_observation(
            require_paused=True
        )
        if any(item.quantity != 0 for item in account.positions):
            raise TradingBlocked("retirement requires every position to be flat")
        ids = [row["id"] for row in self._rows]
        if set(ids) != set(self._adapters) or {
            row["id"] for row in parameters["strategies"]
        } != set(ids):
            raise TradingBlocked("retirement strategy registration is incomplete")
        strategies = []
        for identity in sorted(ids):
            state = self._adapters[identity].snapshot()
            checkpoint = state.checkpoint
            if state.state != "ready" or checkpoint is None or checkpoint.pending is not None:
                raise TradingBlocked(
                    "retirement requires confirmed strategy checkpoints without staged decisions"
                )
            strategies.append(
                {
                    "id": identity,
                    "revision": checkpoint.revision,
                    "bootstrap_sha256": checkpoint.bootstrap_digest,
                    "checkpoint_sha256": _digest(checkpoint.document()),
                    "pending": False,
                }
            )
        shutdown = self.shutdown_status()
        if (
            shutdown["active"]
            or shutdown["needs_settlement"]
            or self._shutdown.uncertain is not None
            or self._shutdown.error is not None
        ):
            raise TradingBlocked("retirement cannot interrupt financial shutdown settlement")
        full = account_document(account)
        ledger = self.ledger_page(limit=1)
        return {
            "schema_version": 1,
            "scope": "virtual_margin_retirement_observation",
            "account_id": self.account_id,
            "registered_parameters_sha256": parameters_sha,
            "session_config_hash": self.config_hash,
            "ledger_namespace": self._ledger_pages.namespace,
            "account": full,
            "account_sha256": _digest(full),
            "source": {
                "anchor_sha256": anchor,
                "tip_sha256": tip,
                "count": count,
                "chain_sha256": chain,
            },
            "ledger_snapshot": ledger["snapshot"],
            "ledger_total": ledger["ledger_total"],
            "session_metadata_sha256": _digest(self._meta),
            "shutdown_sha256": _digest(shutdown),
            "shutdown_metadata_sha256": _digest(self._shutdown.data),
            "strategies": strategies,
            "paused": True,
            "orders": 0,
            "staged": 0,
            "cleanup_generation": self.cleanup_status()["generation"],
            "cleanup_resource_names": sorted(
                row["name"] for row in self.cleanup_status()["resources"]
            ),
        }

    def snapshot(self):
        self._owner()
        shutdown = self.shutdown_status()
        financial = self.execution.snapshot() if self.execution is not None else None
        adapter_states = (
            {}
            if shutdown["recovery_required"]
            else {key: value.snapshot() for key, value in self._adapters.items()}
        )
        model_errors = {}
        for identity, state in adapter_states.items():
            if state.checkpoint is None:
                continue
            document = json.loads(state.checkpoint.module_checkpoint)
            payload = document.get("payload", document) if isinstance(document, dict) else document
            failure = payload.get("model_failure") if isinstance(payload, dict) else None
            if failure is not None:
                model_errors[identity] = (
                    {"error_type": failure["error_type"], "open_time_ms": failure["open_time_ms"]}
                    if type(failure) is dict
                    and type(failure.get("error_type")) is str
                    and len(failure["error_type"]) <= 128
                    and type(failure.get("open_time_ms")) is int
                    else {"error_type": "InvalidModelFailure", "open_time_ms": None}
                )
        cursor = self._meta["cursor_ms"] if self._meta else None
        last = (self._meta or {}).get("last_frame")
        supported_references = last is not None and {
            item["instrument"]
            for item in last["brackets"]
            if item["available_at_ms"] <= last["as_of_ms"]
            and 0 <= last["as_of_ms"] - item["observed_at_ms"] <= 86400000
        } >= {item.instrument.key for item in self.instruments}
        external_status = (
            self._external.status(
                last, () if last is None else tuple(_candle(item) for item in last["candles"])
            )
            if self._external.enabled and not shutdown["recovery_required"]
            else {}
        )
        return {
            "account_id": self.account_id,
            "state": "shutdown_recovery_required" if shutdown["recovery_required"] else self._state,
            "error": self._error,
            "paused": True if financial is None else financial.paused,
            "cursor_ms": cursor,
            "next_open_ms": self.start_ms if cursor is None else cursor + 60000,
            "start_ms": self.start_ms,
            "instruments": [item.instrument.key for item in self.instruments],
            "pending_frame": self._meta is not None and self._meta["pending"] is not None,
            "new_risk_ready": financial is not None
            and not shutdown["recovery_required"]
            and financial.state == "ready"
            and not financial.paused
            and self._state == "ready"
            and not model_errors
            and all(value.state == "ready" for value in adapter_states.values())
            and not (self._meta or {}).get("graph_errors")
            and bool(supported_references)
            and last["as_of_ms"] - last["candles"][0]["close_time_ms"] <= 120000
            and all(item["new_risk_ready"] for item in external_status.values())
            and all(getattr(graph, "calculation_ready", True) for graph in self._graphs.values()),
            "account": None
            if financial is None or financial.account is None
            else account_document(financial.account, ledger_limit=128),
            "pending_orders": []
            if financial is None
            else [item.order_id for item in financial.pending],
            "graph_errors": deepcopy((self._meta or {}).get("graph_errors", {})),
            **(
                {
                    "market_inputs": self._inputs.status(
                        None if last is None else last["market_inputs"]
                    )
                }
                if self._inputs.enabled
                else {}
            ),
            **({"model_errors": model_errors} if model_errors else {}),
            **({"external_features": external_status} if external_status else {}),
            "shutdown": shutdown,
            "strategies": [
                {
                    "id": row["id"],
                    "state": "recovery_required",
                    "cursor_ms": cursor,
                    "reason": "shutdown_financial_only",
                }
                for row in self._rows
            ]
            if shutdown["recovery_required"]
            else [
                {
                    "id": key,
                    "state": value.state,
                    "cursor_ms": None
                    if value.checkpoint is None
                    else value.checkpoint.last_open_time_ms,
                    "reason": None
                    if value.checkpoint is None or value.checkpoint.last_decision is None
                    else value.checkpoint.last_decision.reason,
                }
                for key, value in adapter_states.items()
            ],
            **({"funding": self._funding.status()} if self._funding.enabled else {}),
            **({"cost_policy": self._costs.status()} if self._costs.enabled else {}),
            "limitation": COST_LIMITATION
            if self._costs.enabled
            else FUNDING_LIMITATION
            if self._funding.enabled
            else LIMITATION,
        }

    def cleanup_status(self):
        """Process-local observation only; no financial/file/model authority."""
        return self._cleanup.status()

    def close(self):
        if threading.get_ident() != self._thread:
            raise TradingBlocked("close margin session on its owning worker")
        self._closed = True
        self._cleanup.close()
