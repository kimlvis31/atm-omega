"""Opt-in closed-minute replay through the same owner used by virtual trading.

The composition root supplies a session factory. No runtime, database, exchange
or GUI is imported here. The existing unleveraged intrabar simulator is separate.
"""

# Imports
import json
import os
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, localcontext
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from omega.application.margin_funding import POLICY_SHA256, FundingSchedule
from omega.application.virtual_margin import VirtualMarginSnapshot
from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MAX_TIME,
    CollateralTransfer,
    FillEvent,
    FundingCashflow,
    MarginAccount,
    MarginBracketSnapshot,
    MarkSnapshot,
    apply_fill,
    apply_funding,
    apply_transfer,
    evaluate_margin,
)
from omega.domain.models import Candle, Instrument, Market
from omega.research.artifacts import check_cancel, read_status_json, validate_artifact_path
from omega.research.datasets import INTERVAL_MS, MAX_ROWS, MarketRow, decimal_value
from omega.research.margin_observation_inputs import NativeObservationReplay

MODEL = "shared-virtual-margin-closed-minute-v1"


@dataclass(frozen=True)
class MarginSimulationSpec:
    fee_rate: Decimal = Decimal("0.001")
    backend: str = "cpu"

    def __post_init__(self):
        if (
            type(self.fee_rate) is not Decimal
            or not self.fee_rate.is_finite()
            or not 0 <= self.fee_rate <= 1
        ):
            raise ValidationError("margin replay fee rate must be a finite Decimal within0..1")
        if (
            len(self.fee_rate.as_tuple().digits) > 18
            or not -18 <= self.fee_rate.as_tuple().exponent <= 18
        ):
            raise ValidationError("margin replay fee rate exceeds capacity")
        if type(self.backend) is not str or self.backend not in {"cpu", "auto"}:
            raise ValidationError("shared margin replay currently supports CPU only")

    @classmethod
    def parse(cls, raw):
        if type(raw) is not dict or set(raw) - {"fee_rate", "backend"}:
            raise ValidationError("unsupported shared margin replay assumptions")
        return cls(
            decimal_value(raw.get("fee_rate", "0.001"), "fee_rate"),
            raw.get("backend", "cpu"),
        )


class MarginReplaySession(Protocol):
    initial: MarginAccount
    execution: object

    def resume(self, request_id): ...
    def pause(self, request_id): ...
    def run_once(
        self,
        candles,
        *,
        as_of_ms,
        brackets,
        entries_allowed,
        prepared_frames=None,
        auxiliary_destination=None,
    ): ...
    @property
    def analysis_input_channels(self): ...
    @property
    def analysis_metric_policies(self): ...
    def snapshot(self): ...
    def cost_quote_summaries(self, *, limit=10000): ...
    def close(self): ...


class NativeAnalysisPreflight(Protocol):
    def __call__(
        self, account, strategies, start_ms, *, fee_rate, funding=None, cost_policy=None
    ) -> dict: ...


def _document(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Instrument):
        return value.key
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _document(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (tuple, list)):
        return [_document(item) for item in value]
    if type(value) is dict:
        return {key: _document(item) for key, item in value.items()}
    return value


def _json(value):
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def _instrument(key):
    if type(key) is not str:
        raise ValidationError("margin replay needs native instrument identifiers")
    try:
        provider, market, symbol = key.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (TypeError, ValueError):
        raise ValidationError("invalid margin replay instrument identifier") from None
    if result.key != key or result.provider != "binance" or result.market != Market.USDM:
        raise ValidationError("margin replay requires canonical Binance USD-M instruments")
    return result


def _inputs(rows, account, start_ms, brackets, cancel, *, cutoff_for_frame=None):
    if type(rows) not in {list, tuple} or not 1 <= len(rows) <= MAX_ROWS:
        raise ValidationError("margin replay requires a bounded one-minute dataset")
    if (
        type(start_ms) is not int
        or not 0 <= start_ms <= MAX_TIME - INTERVAL_MS
        or start_ms % INTERVAL_MS
    ):
        raise ValidationError("margin replay start must be an aligned UTC millisecond integer")
    if type(account) is not dict or type(account.get("positions")) is not list:
        raise ValidationError("margin replay needs an explicit account configuration")
    if any(
        type(item) is not dict or type(item.get("instrument")) is not str
        for item in account["positions"]
    ):
        raise ValidationError("invalid configured margin replay position")
    expected = {item["instrument"] for item in account["positions"]}
    if not 1 <= len(expected) <= 100 or len(expected) != len(account["positions"]):
        raise ValidationError("margin replay needs unique configured positions")
    instruments = {key: _instrument(key) for key in sorted(expected)}
    grouped, keys, digest = defaultdict(list), set(), sha256()
    if any(type(row) is not MarketRow for row in rows):
        raise ValidationError("margin replay needs typed one-minute rows")
    for row in sorted(rows, key=lambda item: (item.open_time_ms, item.instrument)):
        check_cancel(cancel)
        if (
            type(row) is not MarketRow
            or row.instrument not in expected
            or type(row.open_time_ms) is not int
        ):
            raise ValidationError("unexpected margin replay row/instrument")
        key = row.open_time_ms, row.instrument
        if key in keys:
            raise ValidationError("duplicate margin replay candle")
        if row.open_time_ms < start_ms or row.open_time_ms > MAX_TIME - INTERVAL_MS:
            raise ValidationError("margin replay row is outside its supported time range")
        keys.add(key)
        grouped[row.open_time_ms].append(row)
        digest.update(_json(row.as_dict()) + b"\n")
    next_open = start_ms
    for timestamp in sorted(grouped):
        if timestamp != next_open:
            raise ValidationError("margin replay has a missing common minute")
        next_open += INTERVAL_MS
    frames = []
    for timestamp, values in sorted(grouped.items()):
        if {item.instrument for item in values} != expected:
            raise ValidationError("every account instrument requires the same closed minute")
        frames.append(
            tuple(
                Candle(
                    instruments[row.instrument],
                    timestamp,
                    timestamp + INTERVAL_MS,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume,
                )
                for row in values
            )
        )
    if type(brackets) is not tuple or not 1 <= len(brackets) <= 10000:
        raise ValidationError("margin replay requires bounded typed bracket history")
    history = defaultdict(list)
    for table in brackets:
        if not isinstance(table, MarginBracketSnapshot) or table.instrument.key not in expected:
            raise ValidationError("margin replay bracket references an unknown instrument")
        history[table.instrument].append(table)
    selected, available = [], {}
    for instrument in instruments.values():
        history[instrument].sort(key=lambda item: item.available_at_ms)
        if len({item.available_at_ms for item in history[instrument]}) != len(history[instrument]):
            raise ValidationError("ambiguous margin bracket history availability")
        available[instrument] = [item.available_at_ms for item in history[instrument]]
    for frame in frames:
        as_of_ms = frame[0].close_time_ms if cutoff_for_frame is None else cutoff_for_frame(frame)
        tables = []
        for instrument in instruments.values():
            choices = history[instrument]
            index = bisect_right(available[instrument], as_of_ms) - 1
            if index < 0:
                raise ValidationError("margin bracket was not available at a replay close")
            table = choices[index]
            if table.observed_at_ms > as_of_ms or as_of_ms - table.observed_at_ms > 86400000:
                raise ValidationError("margin replay bracket is stale or comes from the future")
            tables.append(table)
        selected.append(tuple(tables))
    return frames, selected, digest.hexdigest()


def _claim(root, manifest, resume):
    path = validate_artifact_path(Path(root).absolute() / "margin-replay.json", allow_missing=True)
    if resume:
        if read_status_json(path) != manifest:
            raise ProtocolError("margin replay inputs/configuration changed since its original run")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError("margin replay lineage already exists; use explicit resume")
    if any(path.parent.iterdir()):
        raise ProtocolError(
            "new margin replay requires an empty namespace; existing financial state is preserved"
        )
    # A partial creation failure stays visible. Never replace a previous run's lineage.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(_json(manifest))
        stream.flush()
        os.fsync(stream.fileno())


def _financial(session):
    value = session.execution.snapshot()
    if (
        not isinstance(value, VirtualMarginSnapshot)
        or value.state != "ready"
        or value.pending_record_id
    ):
        raise TradingBlocked("shared margin replay financial state is not confirmed")
    if not isinstance(value.account, MarginAccount):
        raise TradingBlocked("shared margin replay has no authoritative account")
    return value


def _verify_cost_policy_view(view, policy):
    if policy is None:
        return
    value = view.get("cost_policy")
    try:
        confirmed = (
            type(value) is dict
            and value.get("scope") == "fixed_adverse_full_fill_delay_v1"
            and value.get("policy_sha256") == policy.sha256
            and value.get("venue_execution") is False
            and type(value.get("policy")) is dict
            and _json(value["policy"]) == _json(policy.document())
        )
    except (ValueError, TypeError):
        confirmed = False
    if not confirmed:
        raise ProtocolError("shared margin replay session did not confirm its original cost policy")


def _curve(initial, account, frames, tables, cancel, *, cutoff_for_frame=None):
    current, entries, index, curve = initial, account.ledger, 0, []
    for frame, references in zip(frames, tables, strict=True):
        check_cancel(cancel)
        cutoff = frame[0].close_time_ms if cutoff_for_frame is None else cutoff_for_frame(frame)
        while index < len(entries) and entries[index].fact.available_at_ms <= cutoff:
            fact = entries[index].fact
            if isinstance(fact, FillEvent):
                current = apply_fill(current, fact).account
            elif isinstance(fact, CollateralTransfer):
                current = apply_transfer(current, fact).account
            elif isinstance(fact, FundingCashflow):
                current = apply_funding(current, fact).account
            else:
                raise ProtocolError("unsupported financial fact in shared margin replay")
            index += 1
        marks = tuple(
            MarkSnapshot(
                row.instrument, row.close, row.close_time_ms, cutoff, "research_ohlcv_close_proxy"
            )
            for row in frame
        )
        evaluation = evaluate_margin(current, marks, references, as_of_ms=cutoff)
        if not evaluation.ready:
            raise TradingBlocked(
                "shared margin replay equity is unavailable: " + str(evaluation.reason)
            )
        curve.append(
            {
                "timestamp_ms": cutoff,
                **(
                    {"market_close_time_ms": frame[0].close_time_ms}
                    if cutoff_for_frame is not None
                    else {}
                ),
                "wallet": str(evaluation.wallet_balance),
                "equity": str(evaluation.equity),
            }
        )
    if index != len(entries) or current != account:
        raise ProtocolError("shared margin replay ledger does not reproduce its final account")
    return curve


def simulate_margin(
    rows,
    spec=None,
    *,
    root,
    account,
    strategies,
    start_ms,
    brackets,
    session_factory,
    funding=None,
    cost_policy=None,
    resume=False,
    cancel=None,
    progress=None,
    auxiliary_replay=None,
    analysis_preflight: NativeAnalysisPreflight | None = None,
):
    """Replay one isolated file-backed session and return its actual financial facts.

    ``resume`` is an explicit owner action, never an automatic restart. The same
    complete input/configuration lineage is mandatory. Factory and its session
    stay on this calling thread; each job must receive its own root directory.
    """
    if spec is None:
        spec = MarginSimulationSpec()
    if (
        not isinstance(spec, MarginSimulationSpec)
        or type(resume) is not bool
        or not callable(session_factory)
    ):
        raise ValidationError("invalid shared margin replay configuration")
    if type(strategies) is not list or not 1 <= len(strategies) <= 100:
        raise ValidationError("margin replay requires explicit bounded TS configurations")
    if auxiliary_replay is not None and type(auxiliary_replay) is not NativeObservationReplay:
        raise ValidationError("shared replay needs a typed native observation input")
    cutoff_for_frame = None if auxiliary_replay is None else auxiliary_replay.as_of_ms
    # Freeze all caller-owned JSON before the first filesystem/session mutation.
    configuration = {
        "account": account,
        "strategies": strategies,
        "start_ms": start_ms,
        "spec": _document(spec),
        "brackets": _document(brackets),
    }
    if funding is not None:
        configuration["funding"] = (
            funding.document() if isinstance(funding, FundingSchedule) else funding
        )
    if cost_policy is not None:
        configuration["cost_policy"] = cost_policy
    prepared_inputs = None
    if auxiliary_replay is not None and auxiliary_replay.version == 2:
        if not callable(analysis_preflight):
            raise ValidationError("native V2 replay requires actual graph preflight before claim")
        preflight_raw = _json(configuration)
        if len(preflight_raw) > 16_000_000:
            raise ValidationError("margin replay configuration exceeds capacity")
        preflight = json.loads(preflight_raw)
        prepared_inputs = _inputs(
            rows,
            preflight["account"],
            start_ms,
            brackets,
            cancel,
            cutoff_for_frame=cutoff_for_frame,
        )
        plan = analysis_preflight(
            preflight["account"],
            preflight["strategies"],
            start_ms,
            fee_rate=str(spec.fee_rate),
            **({"funding": preflight["funding"]} if funding is not None else {}),
            **({"cost_policy": preflight["cost_policy"]} if cost_policy is not None else {}),
        )
        if type(plan) is not dict or set(plan) != {"required_channels", "metric_policies"}:
            raise ValidationError("native V2 preflight must return exact public graph policies")
        native_frames = prepared_inputs[0]
        auxiliary_replay.bind_configuration(
            plan["required_channels"],
            plan["metric_policies"],
            start_ms=native_frames[0][0].open_time_ms,
            end_ms=native_frames[-1][0].close_time_ms,
        )
    if auxiliary_replay is not None:
        configuration["auxiliary_replay"] = auxiliary_replay.lineage
    raw = _json(configuration)
    if len(raw) > 16_000_000:
        raise ValidationError("margin replay configuration exceeds capacity")
    frozen = json.loads(raw)
    schedule = FundingSchedule.parse(frozen["funding"]) if "funding" in frozen else None
    costs = None
    if "cost_policy" in frozen:
        from omega.application.margin_cost import MarginCostPolicy

        costs = MarginCostPolicy.parse(frozen["cost_policy"])
    inputs_options = {} if auxiliary_replay is None else {"cutoff_for_frame": cutoff_for_frame}
    frames, references, rows_hash = (
        _inputs(rows, frozen["account"], start_ms, brackets, cancel, **inputs_options)
        if prepared_inputs is None
        else prepared_inputs
    )
    if schedule is not None:
        schedule.validate_for(tuple(bar.instrument for bar in frames[0]), start_ms)
        if any(point.event_time_ms > frames[-1][0].open_time_ms for point in schedule.points):
            raise ValidationError("funding settlement is outside the complete replay dataset")
    manifest = {
        "schema_version": 1,
        "model": MODEL,
        "request_sha256": sha256(raw).hexdigest(),
        "rows_sha256": rows_hash,
        "rows_count": len(rows),
        "frames": len(frames),
    }
    if schedule is not None:
        manifest["funding_sha256"] = schedule.sha256
    if costs is not None:
        manifest["cost_policy_sha256"] = costs.sha256
        manifest["cost_policy_request_sha256"] = sha256(_json(frozen["cost_policy"])).hexdigest()
    _claim(root, manifest, resume)
    session = session_factory(
        Path(root).absolute(),
        frozen["account"],
        frozen["strategies"],
        start_ms,
        create=not resume,
        fee_rate=str(spec.fee_rate),
        **({"funding": frozen["funding"]} if schedule is not None else {}),
        **({"cost_policy": frozen["cost_policy"]} if costs is not None else {}),
    )
    try:
        view = session.snapshot()
        if view["state"] != "ready" or view.get("graph_errors"):
            raise TradingBlocked("shared margin replay session is not ready")
        _verify_cost_policy_view(view, costs)
        _financial(session)
        if not isinstance(session.initial, MarginAccount) or session.initial.ledger:
            raise ProtocolError("margin replay needs its original unconsumed initial account")
        requirements = None
        if auxiliary_replay is not None:
            try:
                requirements = session.analysis_input_channels
            except (AttributeError, TypeError):
                raise ProtocolError(
                    "margin replay session lacks its public input requirements"
                ) from None
            auxiliary_replay.validate_requirements(requirements)
            if auxiliary_replay.version == 2:
                try:
                    metric_policies = session.analysis_metric_policies
                except (AttributeError, TypeError):
                    raise ProtocolError(
                        "margin replay session lacks its public metric policies"
                    ) from None
                auxiliary_replay.validate_configuration(requirements, metric_policies)
        next_open = view["next_open_ms"]
        if (
            type(next_open) is not int
            or next_open < start_ms
            or next_open > frames[-1][0].close_time_ms
            or next_open % INTERVAL_MS
        ):
            raise ProtocolError("margin replay session cursor exceeds its source range")
        if next_open < frames[-1][0].close_time_ms:
            # Explicit replay recovery is a new user action. Reusing the old
            # acknowledged resume ID cannot release a restarted owner's pause.
            resume_action = (manifest, "resume", next_open)
            if resume:
                resume_action = (*resume_action, uuid4().hex)
            session.resume(sha256(_json(resume_action)).hexdigest())
        for index, (frame, tables) in enumerate(zip(frames, references, strict=True)):
            check_cancel(cancel)
            if frame[0].open_time_ms < next_open:
                continue
            cutoff = frame[0].close_time_ms if cutoff_for_frame is None else cutoff_for_frame(frame)
            options = {}
            if auxiliary_replay is not None:
                if auxiliary_replay.lineage != frozen["auxiliary_replay"]:
                    raise ValidationError("native observation lineage changed during replay")
                prepared = auxiliary_replay.prepare(
                    frame, as_of_ms=cutoff, requirements=requirements
                )
                if requirements:
                    options = {
                        "prepared_frames": prepared,
                        "auxiliary_destination": auxiliary_replay.destination_id,
                    }
            view = session.run_once(
                frame, as_of_ms=cutoff, brackets=tables, entries_allowed=True, **options
            )
            _financial(session)
            if view["state"] != "ready" or view.get("pending_frame") or view.get("graph_errors"):
                raise TradingBlocked("shared margin replay frame did not finish safely")
            _verify_cost_policy_view(view, costs)
            if progress is not None:
                progress({"completed_frames": index + 1, "total_frames": len(frames)})
        session.pause(sha256(_json((manifest, "finish_pause"))).hexdigest())
        final, initial = _financial(session), session.initial
        view = session.snapshot()
        _verify_cost_policy_view(view, costs)
        if costs is not None:
            try:
                cost_quotes = session.cost_quote_summaries(limit=10000)
            except (AttributeError, TypeError):
                raise ProtocolError(
                    "shared margin replay lacks confirmed cost quote explanations"
                ) from None
    finally:
        session.close()
    equity = _curve(initial, final.account, frames, references, cancel, **inputs_options)
    result = {
        "schema_version": 1,
        "state": "complete",
        "model": MODEL,
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "quote_asset": "USDT",
        "initial_wallet": str(initial.opening_balance),
        "backend": {
            "requested": spec.backend,
            "selected": "cpu",
            "reason": "Shared virtual owner is a sequential CPU reference",
        },
        "spec": _document(spec),
        "lineage": manifest,
        "account": _document(final.account),
        "pending": _document(final.pending),
        "positions": {item.instrument.key: str(item.quantity) for item in final.account.positions},
        "fills": [
            _document(entry.fact)
            | {
                "timestamp_ms": entry.fact.event_time_ms,
                "quantity": str(entry.fact.delta_qty),
                "realized_pnl": str(entry.realized_pnl),
                "tag": entry.fact.owner_id,
            }
            for entry in final.account.ledger
            if isinstance(entry.fact, FillEvent)
        ],
        "wallet": equity[-1]["wallet"],
        "equity": equity,
        "events": [
            _document(entry.fact)
            | {"kind": "collateral_transfer", "timestamp_ms": entry.fact.event_time_ms}
            for entry in final.account.ledger
            if isinstance(entry.fact, CollateralTransfer)
        ],
        "pending_orders": len(final.pending),
        "cursor_ms": view["cursor_ms"],
        "strategies": view["strategies"],
        "event_capacity": final.account.event_capacity,
        "assumptions": [
            "same session/TS, margin owner, reservations and journal as virtual trading",
            "closed one-minute common frames; earliest eligible next-minute fills",
            "OHLCV proxy marks, deterministic session open/close phases, full fills",
            "cross and isolated wallets remain separate; USDT settlement only",
            "no intrabar path/randomization, latency, spread/slippage, funding or liquidation",
            "end-of-dataset positions/pending remain open; no invented closing candle",
            "restart is explicitly resumed only with identical complete input lineage",
            "shared owner ledger/session capacity limits apply; no unbounded event archive",
        ],
    }
    if schedule is not None:
        facts = [
            entry.fact for entry in final.account.ledger if isinstance(entry.fact, FundingCashflow)
        ]
        result["events"] = [
            _document(entry.fact)
            | {
                "kind": "funding_cashflow"
                if isinstance(entry.fact, FundingCashflow)
                else "collateral_transfer",
                "timestamp_ms": entry.fact.event_time_ms,
            }
            for entry in final.account.ledger
            if isinstance(entry.fact, (FundingCashflow, CollateralTransfer))
        ]
        with localcontext() as calculation:
            calculation.prec = 256
            net = sum((fact.amount for fact in facts), Decimal(0))
        result["funding"] = {
            "schema_version": 1,
            "model": schedule.document()["model"],
            "schedule_sha256": schedule.sha256,
            "policy_sha256": POLICY_SHA256,
            "source_kind": "scenario_rate",
            "settlements": len(facts),
            "funding_net": str(net),
        }
        result["funding_history"] = [
            event for event in result["events"] if event["kind"] == "funding_cashflow"
        ]
        result["assumptions"][4] = (
            "no intrabar path/randomization, latency, spread/slippage or liquidation"
        )
        result["assumptions"].extend(
            [
                "assumed scenario rate-derived funding cashflows, "
                "not actual venue account payments",
                "previous close, minute-boundary funding, current open; "
                "source available within settlement minute",
                "signed funding_net is credited/debited separately from "
                "trade realized PnL and fees",
            ]
        )
    if costs is not None:
        from omega.research.result_summary import _cost_quote_record

        result["cost_policy"] = {
            "schema_version": 1,
            "model": costs.document()["model"],
            "raw_document": frozen["cost_policy"],
            "canonical_document": costs.document(),
            "request_sha256": manifest["cost_policy_request_sha256"],
            "policy_sha256": costs.sha256,
        }
        try:
            result["cost_quotes"] = _cost_quote_record(
                cost_quotes, result["cost_policy"], result["fills"]
            )
        except ValueError as error:
            raise ProtocolError(str(error)) from None
        result["assumptions"][1] = (
            "closed one-minute common frames; original issue plus selected order delay "
            "rounded up to a minute boundary, not measured venue latency"
        )
        result["assumptions"][4] = (
            "no intrabar path/randomization, market impact, partial fills or liquidation"
        )
        result["assumptions"].extend(
            [
                "explicit fixed adverse full-fill scenario policy, including a zero-valued policy; "
                "source candles and observed marks remain unchanged",
                "final execution price and fee are applied once through the shared owner's "
                "confirmed FillEvent; no second slippage cash debit",
                "one millisecond of order delay may move eligibility to a later minute; "
                "reduction delay also delays protective and flatten fills, "
                "not protection decisions",
                "end-of-dataset delayed orders and exposure remain pending/open; "
                "no invented completion candle or extra funding settlement",
                "raw/final quote summaries match confirmed source admissions and applied "
                "FillEvent identities; explanatory amounts are not additional cash debits",
            ]
        )
        if schedule is None:
            result["assumptions"].append("scenario funding is not configured")
    if auxiliary_replay is not None:
        # Financial marks/fills still use the established OHLCV proxy model.
        result["input_scope"] = "native_observation_replay"
        result["auxiliary_replay"] = frozen["auxiliary_replay"]
        result["assumptions"][4] = result["assumptions"][4].replace("latency, ", "")
        result["assumptions"].extend(
            [
                (
                    "original retained-native trade-flow/depth/OI/long-short observations; "
                    "native availability/revisions/checksums and frozen graph policies retained"
                    if auxiliary_replay.version == 2
                    else "original selected-normalized trade-flow observations only; "
                    "native availability/revisions/checksums retained"
                ),
                "fixed declared decision cutoff = market close + decision latency; "
                "not measured venue latency",
                "late/missing/partial auxiliary inputs remain unready; "
                "no zero-fill or retrospective receipt times",
                "existing original order eligibility is rounded up to the next eligible minute; "
                "never fill at a past opening",
                "equity timestamp is its decision/availability cutoff; "
                "market_close_time_ms retains the original candle boundary",
            ]
        )
    return result
