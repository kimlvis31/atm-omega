"""Fail-closed validation of the persisted paper portfolio before reconstruction."""

import json
import re

from omega.analysis.warmup import MAX_WARMUP, MINUTE, SHORT_WARMUP
from omega.application.execution_rules import validate_execution_order
from omega.application.paper_inputs import input_receipt
from omega.application.paper_instruments import require_paper_settlement
from omega.domain.errors import ValidationError
from omega.domain.execution_rules import MAX_TIME, ExecutionRulesSnapshot
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side, decimal
from omega.domain.trading import OrderProposal


def _identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValidationError("invalid persisted paper identity")


def _instrument(value):
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (ValueError, TypeError, AttributeError):
        raise ValidationError("invalid persisted paper instrument") from None
    if provider != "binance" or result.market not in {Market.SPOT, Market.USDM}:
        raise ValidationError("unsupported persisted paper instrument")
    return result


def _money(value):
    if not isinstance(value, str) or len(value) > 128:
        raise ValidationError("persisted paper amounts must be bounded decimal strings")
    return decimal(value)


def _timestamp(value, *, optional=False):
    if optional and value is None:
        return
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValidationError("invalid persisted paper timestamp")


def execution_policy(value):
    if (
        type(value) is not dict
        or set(value) != {"mode", "max_age_ms", "apply_to_manual"}
        or type(value["mode"]) is not str
        or value["mode"] not in {"static", "strict"}
        or type(value["max_age_ms"]) is not int
        or not 1 <= value["max_age_ms"] <= 86400000
        or type(value["apply_to_manual"]) is not bool
    ):
        raise ValidationError("invalid paper execution policy")
    return dict(value)


def execution_snapshot(document, instrument):
    snapshot = ExecutionRulesSnapshot.from_document(document)
    if (
        snapshot.instrument != instrument
        or snapshot.source != f"{instrument.provider}:{instrument.market}:exchangeInfo"
    ):
        raise ValidationError("paper execution rules source or instrument mismatch")
    return snapshot


def _execution_metadata(value, instrument, *, compact=False):
    common = {"version", "decision_at_ms", "policy", "unchecked", "input_source", "retrospective"}
    extra = (
        {"snapshot_hash", "snapshot_observed_at_ms", "snapshot_source"} if compact else {"snapshot"}
    )
    if (
        type(value) is not dict
        or set(value) != common | extra
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["decision_at_ms"]) is not int
        or not 0 <= value["decision_at_ms"] <= MAX_TIME
        or type(value["retrospective"]) is not bool
        or value["input_source"] not in {"manual_paper", "database_committed"}
        or type(value["unchecked"]) is not list
        or len(value["unchecked"]) > 80
        or any(type(item) is not str or not 1 <= len(item) <= 160 for item in value["unchecked"])
        or sorted(set(value["unchecked"])) != value["unchecked"]
    ):
        raise ValidationError("invalid paper execution provenance")
    policy = execution_policy(value["policy"])
    if value["input_source"] == "manual_paper" and not policy["apply_to_manual"]:
        raise ValidationError("manual venue rules were not explicitly enabled")
    if compact:
        observed = value["snapshot_observed_at_ms"]
        if (
            type(value["snapshot_hash"]) is not str
            or re.fullmatch("[a-f0-9]{64}", value["snapshot_hash"]) is None
            or value["snapshot_source"] != f"{instrument.provider}:{instrument.market}:exchangeInfo"
            or type(observed) is not int
            or not 0 <= observed <= MAX_TIME
        ):
            raise ValidationError("invalid compact paper rule identity")
    else:
        observed = execution_snapshot(value["snapshot"], instrument).observed_at_ms
    if observed > value["decision_at_ms"]:
        raise ValidationError("paper execution decision used future rules")


def _execution_health(value):
    required = {
        "state",
        "mode",
        "reason",
        "input_source",
        "snapshot_hash",
        "observed_at_ms",
        "decision_at_ms",
        "unchecked",
        "protection_blocked",
    }
    if (
        type(value) is not dict
        or set(value) != required
        or value["state"]
        not in {"synthetic", "ready", "blocked", "unchecked", "protection_unavailable"}
        or value["mode"] not in {"synthetic", "static", "strict"}
        or type(value["reason"]) is not str
        or not 1 <= len(value["reason"]) <= 128
        or value["input_source"] not in {"manual_paper", "database_committed"}
        or type(value["protection_blocked"]) is not bool
        or value["snapshot_hash"] is not None
        and (
            type(value["snapshot_hash"]) is not str
            or re.fullmatch("[a-f0-9]{64}", value["snapshot_hash"]) is None
        )
        or type(value["unchecked"]) is not list
        or len(value["unchecked"]) > 80
        or any(type(item) is not str or not 1 <= len(item) <= 160 for item in value["unchecked"])
    ):
        raise ValidationError("invalid paper execution health")
    for key in ("observed_at_ms", "decision_at_ms"):
        if value[key] is not None and (
            type(value[key]) is not int or not 0 <= value[key] <= MAX_TIME
        ):
            raise ValidationError("invalid paper execution health timestamp")


def validate_state(data) -> None:
    try:
        _validate_state(data)
    except ValidationError:
        raise
    except (TypeError, ValueError, KeyError, AttributeError, ArithmeticError):
        raise ValidationError("malformed persisted paper state") from None


def _validate_state(data) -> None:
    required = {"schema_version", "paused", "accounts", "strategies", "candles"}
    if (
        not isinstance(data, dict)
        or not required <= data.keys()
        or data.keys() - (required | {"feed"})
        or type(data["schema_version"]) is not int
        or data["schema_version"] != 1
        or type(data["paused"]) is not bool
    ):
        raise ValidationError("invalid persisted paper schema")
    if "feed" in data:
        _feed(data["feed"], data["strategies"], data["candles"])
    accounts, strategies, candles = (data[key] for key in ("accounts", "strategies", "candles"))
    if (
        not all(isinstance(table, dict) for table in (accounts, strategies, candles))
        or len(accounts) > 10
        or len(strategies) > 100
        or len(candles) > 1000
    ):
        raise ValidationError("persisted paper portfolio exceeds supported limits")
    for name, account in accounts.items():
        _identity(name)
        if not isinstance(account, dict) or set(account) != {"balance", "fee_rate", "market"}:
            raise ValidationError("invalid persisted paper account")
        balance, fee = _money(account["balance"]), _money(account["fee_rate"])
        if (
            not 0 <= fee < 1
            or account["market"] not in {None, "spot", "usdm"}
            or (account["market"] != "usdm" and balance < 0)
        ):
            raise ValidationError("invalid paper fee, cash or account market")
    owners = set()
    row_keys = {
        "account_id",
        "instrument",
        "module_id",
        "configuration",
        "analysis",
        "budget",
        "quantity",
        "entry_price",
        "revision",
        "last_changed_ms",
        "pending",
        "last_candle",
        "status",
        "fills",
    }
    for name, row in strategies.items():
        _identity(name)
        if (
            not isinstance(row, dict)
            or not row_keys <= row.keys()
            or row.keys()
            - (
                row_keys
                | {
                    "last_reason",
                    "analysis_snapshot",
                    "runtime_checkpoint",
                    "history_preparation",
                    "execution_rules_snapshot",
                    "execution_health",
                    "analysis_input",
                    "analysis_input_health",
                }
            )
        ):
            raise ValidationError("invalid persisted paper strategy")
        account = row["account_id"]
        _identity(account)
        instrument = _instrument(row["instrument"])
        require_paper_settlement(instrument)
        if "execution_rules_snapshot" in row:
            execution_snapshot(row["execution_rules_snapshot"], instrument)
        if "execution_health" in row:
            _execution_health(row["execution_health"])
        if "analysis_input" in row:
            input_receipt(row["analysis_input"])
        if "analysis_input_health" in row:
            health = row["analysis_input_health"]
            if (
                type(health) is not dict
                or set(health) != {"ready", "reason", "manual_recovery_required"}
                or type(health["ready"]) is not bool
                or type(health["manual_recovery_required"]) is not bool
                or type(health["reason"]) is not str
                or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", health["reason"]) is None
                or health["ready"]
                and health["manual_recovery_required"]
            ):
                raise ValidationError("invalid paper analysis-input health")
        key = account, instrument.key
        if (
            account not in accounts
            or key in owners
            or accounts[account]["market"] != instrument.market.value
        ):
            raise ValidationError("paper account/instrument ownership is inconsistent")
        owners.add(key)
        if not isinstance(row["module_id"], str) or not re.fullmatch(
            r"[a-z][a-z0-9_]{0,47}", row["module_id"]
        ):
            raise ValidationError("invalid persisted TS module identity")
        if not isinstance(row["configuration"], dict) or not isinstance(row["analysis"], dict):
            raise ValidationError("invalid persisted paper configuration")
        for configuration in (
            row["configuration"],
            row["analysis"],
            row.get("analysis_snapshot", {}),
        ):
            if not isinstance(configuration, dict):
                raise ValidationError("persisted analysis snapshot must be an object")
            if len(json.dumps(configuration, allow_nan=False).encode("utf-8")) > 65_536:
                raise ValidationError("persisted paper configuration exceeds limit")
        if "runtime_checkpoint" in row:
            value = row["runtime_checkpoint"]
            if (
                not isinstance(value, dict)
                or len(json.dumps(value, allow_nan=False).encode()) > 8_000_000
            ):
                raise ValidationError("invalid or oversized paper runtime checkpoint")
        budget, quantity, entry = (
            _money(row[field]) for field in ("budget", "quantity", "entry_price")
        )
        if (
            budget <= 0
            or entry < 0
            or (quantity == 0) != (entry == 0)
            or (instrument.market == Market.SPOT and quantity < 0)
        ):
            raise ValidationError("invalid persisted paper position or budget")
        if (
            type(row["revision"]) is not int
            or row["revision"] < 0
            or row["status"] not in {"ready", "quarantined", "recovery_required"}
        ):
            raise ValidationError("invalid persisted paper revision or status")
        _timestamp(row["last_changed_ms"])
        if "last_reason" in row and (
            not isinstance(row["last_reason"], str) or len(row["last_reason"]) > 128
        ):
            raise ValidationError("invalid persisted strategy reason")
        if "history_preparation" in row and (
            not isinstance(row["history_preparation"], str)
            or row["history_preparation"] not in {"model", "features_only", "preserve"}
        ):
            raise ValidationError("invalid persisted strategy preparation mode")
        _timestamp(row["last_candle"], optional=True)
        if row["last_candle"] is not None and row["last_candle"] % 60_000:
            raise ValidationError("persisted strategy minute is not aligned")
        pending = row["pending"]
        if pending is not None:
            _pending(pending, quantity, instrument.market, instrument)
        fills = row["fills"]
        if not isinstance(fills, list) or len(fills) > 1000:
            raise ValidationError("persisted fill history exceeds limit")
        previous_time = -1
        for fill in fills:
            required_fill = {
                "timestamp_ms",
                "side",
                "quantity",
                "price",
                "fee",
                "reason",
            }
            if (
                not isinstance(fill, dict)
                or not required_fill <= fill.keys()
                or fill.keys() - (required_fill | {"execution"})
            ):
                raise ValidationError("invalid persisted fill record")
            if "execution" in fill:
                _execution_metadata(fill["execution"], instrument, compact=True)
            _timestamp(fill["timestamp_ms"])
            if (
                fill["timestamp_ms"] < previous_time
                or fill["side"] not in {"BUY", "SELL"}
                or _money(fill["quantity"]) <= 0
                or _money(fill["price"]) <= 0
                or _money(fill["fee"]) < 0
                or not isinstance(fill["reason"], str)
            ):
                raise ValidationError("invalid persisted fill values")
            previous_time = fill["timestamp_ms"]
    if any(
        plan["state"] != "complete"
        for plan in data.get("feed", {}).get("warmup_plans", {}).values()
    ):
        if not data["paused"] or any(
            _money(row["quantity"]) != 0 or row["pending"] is not None
            for row in strategies.values()
        ):
            raise ValidationError(
                "unfinished long warmup cannot own an unpaused or nonflat portfolio"
            )
    for key, history in candles.items():
        instrument = _instrument(key)
        if not isinstance(history, list) or not history or len(history) > 2000:
            raise ValidationError("persisted candle history exceeds limit")
        previous_time = None
        for row in history:
            if not isinstance(row, dict) or set(row) != {
                "open_time_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
            }:
                raise ValidationError("invalid persisted candle record")
            _timestamp(row["open_time_ms"])
            Candle(
                instrument,
                row["open_time_ms"],
                row["open_time_ms"] + 60_000,
                *(_money(row[field]) for field in ("open", "high", "low", "close", "volume")),
            )
            if previous_time is not None and row["open_time_ms"] != previous_time + 60_000:
                raise ValidationError("persisted candle history is not contiguous")
            previous_time = row["open_time_ms"]


def _feed(feed, strategies, candles):
    required = {
        "enabled",
        "ready",
        "reason",
        "revision",
        "start_ms",
        "warmup_minutes",
        "warmed",
        "provenance",
    }
    if (
        not isinstance(feed, dict)
        or not required <= set(feed)
        or set(feed) - (required | {"warmup_plans"})
        or type(feed["enabled"]) is not bool
        or type(feed["ready"]) is not bool
        or not isinstance(feed["reason"], str)
        or not 1 <= len(feed["reason"]) <= 128
        or type(feed["revision"]) is not int
        or feed["revision"] < 0
        or type(feed["warmup_minutes"]) is not int
        or not 1 <= feed["warmup_minutes"] <= MAX_WARMUP
        or not isinstance(feed["warmed"], dict)
        or set(feed["warmed"]) - set(strategies)
        or any(value is not True for value in feed["warmed"].values())
        or not isinstance(feed["provenance"], dict)
        or set(feed["provenance"]) - set(candles)
        or any(
            value not in {"database_committed", "manual_paper", "mixed_manual_and_database"}
            for value in feed["provenance"].values()
        )
    ):
        raise ValidationError("invalid persisted paper feed control")
    _timestamp(feed["start_ms"])
    if feed["start_ms"] % 60000:
        raise ValidationError("feed anchor must be minute aligned")
    _warmup_plans(feed, strategies)


def _warmup_plans(feed, strategies):
    plans = feed.get("warmup_plans", {})
    if (
        not isinstance(plans, dict)
        or len(plans) > 100
        or set(plans) - set(strategies)
        or (plans and feed["warmup_minutes"] <= SHORT_WARMUP)
        or (
            feed["enabled"]
            and feed["warmup_minutes"] > SHORT_WARMUP
            and set(plans) != set(strategies)
        )
    ):
        raise ValidationError("invalid staged warmup plans")
    fields = {
        "version",
        "plan_id",
        "instrument",
        "fingerprint",
        "alignment_minutes",
        "start_ms",
        "target_end_ms",
        "next_open_ms",
        "state",
        "checkpoint",
        "last_candle",
    }
    for name, plan in plans.items():
        if (
            not isinstance(plan, dict)
            or set(plan) - {"analysis_input", "analysis_input_health"}
            != (
                fields
                if plan.get("version") == 1
                else fields | {"priming_mode", "module_checkpoint"}
            )
            or type(plan["version"]) is not int
            or plan["version"] not in {1, 2}
            or not isinstance(plan["plan_id"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", plan["plan_id"])
            or not isinstance(plan["fingerprint"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", plan["fingerprint"])
            or plan["instrument"] != strategies[name]["instrument"]
            or plan["state"] not in {"warming", "complete", "not_ready"}
            or type(plan["alignment_minutes"]) is not int
            or not 1 <= plan["alignment_minutes"] <= 1440
        ):
            raise ValidationError("invalid staged warmup identity or configuration")
        if "analysis_input" in plan:
            input_receipt(plan["analysis_input"])
        if "analysis_input_health" in plan:
            health = plan["analysis_input_health"]
            if (
                type(health) is not dict
                or set(health) != {"ready", "reason", "manual_recovery_required"}
                or type(health["ready"]) is not bool
                or type(health["manual_recovery_required"]) is not bool
                or type(health["reason"]) is not str
                or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", health["reason"]) is None
                or health["ready"]
                and health["manual_recovery_required"]
            ):
                raise ValidationError("invalid staged analysis-input health")
        for key in ("start_ms", "target_end_ms", "next_open_ms"):
            _timestamp(plan[key])
            if plan[key] % MINUTE:
                raise ValidationError("staged warmup cursor is not aligned")
        low, high, cursor = plan["start_ms"], plan["target_end_ms"], plan["next_open_ms"]
        if (
            not low <= cursor <= high
            or high - low > MAX_WARMUP * MINUTE
            or (low != high and low % (plan["alignment_minutes"] * MINUTE))
            or (plan["state"] == "warming" and cursor >= high)
            or (plan["state"] != "warming" and cursor != high)
            or (plan["state"] != "complete" and name in feed["warmed"])
        ):
            raise ValidationError("inconsistent staged warmup progress")
        checkpoint = plan["checkpoint"]
        if plan["state"] == "complete":
            if checkpoint is not None:
                raise ValidationError("completed warmup retains a duplicate checkpoint")
        elif (
            not isinstance(checkpoint, dict)
            or len(json.dumps(checkpoint, allow_nan=False).encode()) > 4_000_000
        ):
            raise ValidationError("invalid staged warmup feature checkpoint")
        if plan["version"] == 2:
            mode, module_checkpoint = plan["priming_mode"], plan["module_checkpoint"]
            if not isinstance(mode, str) or mode not in {"model", "features_only", "preserve"}:
                raise ValidationError("invalid staged model preparation mode")
            if mode == "model" and plan["state"] != "complete":
                if (
                    not isinstance(module_checkpoint, dict)
                    or len(json.dumps(module_checkpoint, allow_nan=False).encode()) > 4_000_000
                ):
                    raise ValidationError("invalid staged model checkpoint")
            elif module_checkpoint is not None:
                raise ValidationError("unexpected staged model checkpoint")
        last = plan["last_candle"]
        if cursor == low:
            if last is not None:
                raise ValidationError("unstarted warmup has a cursor candle")
        else:
            if not isinstance(last, dict) or set(last) != {
                "open_time_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
            }:
                raise ValidationError("invalid staged warmup cursor candle")
            if last["open_time_ms"] != cursor - MINUTE:
                raise ValidationError("staged warmup candle does not match cursor")
            instrument = _instrument(plan["instrument"])
            Candle(
                instrument,
                last["open_time_ms"],
                cursor,
                *(_money(last[key]) for key in ("open", "high", "low", "close", "volume")),
            )


def _pending(order, position, market, instrument=None):
    required = {
        "side",
        "kind",
        "quantity",
        "price",
        "reducing",
        "reason",
        "eligible_at_ms",
        "reserved",
    }
    if (
        not isinstance(order, dict)
        or not required <= order.keys()
        or order.keys() - (required | {"execution"})
        or order["side"] not in {"BUY", "SELL"}
        or order["kind"] not in {"MARKET", "LIMIT"}
        or type(order["reducing"]) is not bool
        or not isinstance(order["reason"], str)
    ):
        raise ValidationError("invalid persisted pending order")
    if "execution" in order:
        if instrument is None:
            raise ValidationError("venue pending provenance requires instrument identity")
        _execution_metadata(order["execution"], instrument)
    quantity, reserved = _money(order["quantity"]), _money(order["reserved"])
    if (
        quantity <= 0
        or reserved < 0
        or (order["reducing"] and reserved != 0)
        or (not order["reducing"] and reserved <= 0)
    ):
        raise ValidationError("invalid persisted pending order amount")
    _timestamp(order["eligible_at_ms"])
    if order["kind"] == "MARKET":
        if order["price"] is not None:
            raise ValidationError("persisted market order has a price")
    elif _money(order["price"]) <= 0:
        raise ValidationError("invalid persisted limit price")
    if "execution" in order:
        pin = order["execution"]
        snapshot = execution_snapshot(pin["snapshot"], instrument)
        proposal = OrderProposal(
            Side(order["side"]),
            quantity,
            OrderKind(order["kind"]),
            _money(order["price"]) if order["price"] is not None else None,
            order["reducing"],
            order["reason"],
        )
        try:
            validation = validate_execution_order(
                instrument,
                proposal,
                snapshot,
                now_ms=pin["decision_at_ms"],
                max_age_ms=pin["policy"]["max_age_ms"],
                strict=pin["policy"]["mode"] == "strict",
                retrospective=pin["retrospective"],
            )
        except Exception:
            raise ValidationError("persisted pending violates its pinned execution rules") from None
        if not set(validation.unchecked) <= set(pin["unchecked"]) or pin["retrospective"] != (
            order["eligible_at_ms"] < snapshot.observed_at_ms
        ):
            raise ValidationError("persisted execution uncertainty or history provenance changed")
    signed = quantity if order["side"] == Side.BUY else -quantity
    if order["reducing"]:
        if not position or signed * position >= 0 or quantity > abs(position):
            raise ValidationError("persisted reduction exceeds its owned position")
    elif signed * position < 0 or (signed < 0 and market == Market.SPOT):
        raise ValidationError("persisted order would reverse or short spot")
