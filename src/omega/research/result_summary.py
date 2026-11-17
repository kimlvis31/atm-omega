"""Read-only summaries of completed CPU replay records; never execute model code."""

# Imports
import hashlib
import json
import math
import os
import re
import stat
from copy import deepcopy
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_RECORDS = 100_000
MAX_TIMESTAMP = 253_402_300_799_000
MAX_COST_QUOTES = 10_000


def _number(value, name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{name} must be a finite numeric value")
    if len(str(value)) > 128:
        raise ValueError(f"{name} exceeds the numeric display limit")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"{name} must be numeric") from error
    if not result.is_finite() or (result and abs(result.adjusted()) > 100):
        raise ValueError(f"{name} must be finite and within the display range")
    return result


def _text_number(value):
    return None if value is None else str(value)


def _timestamp(value):
    value = _number(value, "timestamp_ms")
    if value is None or not 0 <= value <= MAX_TIMESTAMP:
        raise ValueError("recorded timestamp_ms is missing or outside the UTC range")
    return value


def _asset(value):
    if value is None:
        return None
    if not isinstance(value, str) or re.fullmatch(r"[A-Z][A-Z0-9]{0,19}", value) is None:
        raise ValueError("currency identifiers must be uppercase asset codes")
    return value


def _records(result, name):
    values = result.get(name)
    if values is None:
        return None
    if (
        not isinstance(values, list)
        or len(values) > MAX_RECORDS
        or any(not isinstance(row, dict) for row in values)
    ):
        raise ValueError(f"{name} must be an object list with at most {MAX_RECORDS} rows")
    return values


def _validate_tree(value, depth=0):
    if depth > 32:
        raise ValueError("result nesting exceeds 32 levels")
    if isinstance(value, dict):
        for item in value.values():
            _validate_tree(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _validate_tree(item, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite JSON number is not permitted")


def _cost_policy_record(value):
    """Validate explanatory policy identity without pricing or altering financial facts."""
    from omega.application.margin_cost import MarginCostPolicy
    from omega.domain.errors import ValidationError

    required = {
        "schema_version",
        "model",
        "raw_document",
        "canonical_document",
        "request_sha256",
        "policy_sha256",
    }
    if (
        type(value) is not dict
        or not required <= value.keys()
        or value.keys() - required - {"source_snapshot"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
    ):
        raise ValueError("invalid scenario cost policy record")
    try:
        policy = MarginCostPolicy.parse(value["raw_document"])
        canonical = MarginCostPolicy.parse(value["canonical_document"])
    except ValidationError as error:
        raise ValueError("invalid scenario cost policy: " + str(error)) from None
    request = json.dumps(
        value["raw_document"], allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode()
    if (
        policy != canonical
        or value["canonical_document"] != policy.document()
        or value["model"] != policy.document()["model"]
        or value["policy_sha256"] != policy.sha256
        or value["request_sha256"] != hashlib.sha256(request).hexdigest()
        or "source_snapshot" in value
        and type(value["source_snapshot"]) is not dict
    ):
        raise ValueError("scenario cost policy identity differs from its recorded documents")
    return deepcopy(value)


def _cost_quote_record(value, policy, fills):
    """Match bounded confirmed explanations to applied fills without repricing them."""
    if (
        type(value) is not dict
        or set(value) != {"schema_version", "model", "state", "total", "truncated", "rows"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["model"] != policy["model"]
        or value["state"] != "ready"
        or type(value["total"]) is not int
        or not 0 <= value["total"] <= MAX_COST_QUOTES
        or value["truncated"] is not False
        or type(value["rows"]) is not list
        or len(value["rows"]) != value["total"]
        or type(fills) is not list
        or len(fills) != value["total"]
    ):
        raise ValueError("cost quote explanations are unavailable, incomplete or exceed capacity")
    by_id = {}
    for fill in fills:
        identity = fill.get("fill_id")
        if type(identity) is not str or identity in by_id:
            raise ValueError("cost quote explanation needs unique applied fill identities")
        by_id[identity] = fill
    fields = {
        "fill_id",
        "order_id",
        "instrument",
        "side",
        "quantity",
        "raw_price",
        "final_price",
        "fee",
        "explanatory_slippage_amount",
        "policy_sha256",
        "issued_at_ms",
        "eligible_at_ms",
        "event_time_ms",
        "available_at_ms",
    }
    seen = set()
    for row in value["rows"]:
        if type(row) is not dict or set(row) != fields:
            raise ValueError("invalid cost quote explanation fields")
        for key in ("fill_id", "order_id"):
            if (
                type(row[key]) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", row[key]) is None
            ):
                raise ValueError("invalid cost quote explanation identity")
        if (
            type(row["instrument"]) is not str
            or re.fullmatch(r"binance:usdm:[A-Z0-9_]{1,64}", row["instrument"]) is None
            or not row["instrument"].endswith("USDT")
            or type(row["side"]) is not str
            or row["side"] not in {"BUY", "SELL"}
            or row["policy_sha256"] != policy["policy_sha256"]
            or row["fill_id"] in seen
            or row["fill_id"] not in by_id
        ):
            raise ValueError("cost quote explanation has no matching policy/applied fill")
        amounts = {}
        for key in ("quantity", "raw_price", "final_price", "fee", "explanatory_slippage_amount"):
            if type(row[key]) is not str:
                raise ValueError("cost quote explanations require decimal strings")
            amounts[key] = _number(row[key], key)
            if (
                amounts[key] < 0
                or key in {"quantity", "raw_price", "final_price"}
                and amounts[key] == 0
            ):
                raise ValueError("invalid cost quote explanation amount")
        for key in ("issued_at_ms", "eligible_at_ms", "event_time_ms", "available_at_ms"):
            if type(row[key]) is not int or not 0 <= row[key] <= MAX_TIMESTAMP:
                raise ValueError("invalid cost quote explanation timestamp")
        if (
            not row["issued_at_ms"]
            <= row["eligible_at_ms"]
            <= row["event_time_ms"]
            <= row["available_at_ms"]
        ):
            raise ValueError("cost quote explanation timestamps are not causal")
        fill = by_id[row["fill_id"]]
        signed = _number(fill.get("delta_qty", fill.get("quantity")), "applied fill quantity")
        if (
            signed is None
            or signed == 0
            or (signed > 0) != (row["side"] == "BUY")
            or abs(signed) != amounts["quantity"]
            or row["order_id"] != fill.get("order_id")
            or row["instrument"] != fill.get("instrument")
            or amounts["final_price"] != _number(fill.get("price"), "applied fill price")
            or amounts["fee"] != _number(fill.get("fee"), "applied fill fee")
            or type(fill.get("event_time_ms")) is not int
            or type(fill.get("available_at_ms")) is not int
            or row["event_time_ms"] != fill.get("event_time_ms")
            or row["available_at_ms"] != fill.get("available_at_ms")
        ):
            raise ValueError("cost quote explanation differs from its applied fill")
        seen.add(row["fill_id"])
    return deepcopy(value)


def summarize_simulation(result: dict) -> dict:
    """Amounts stay Decimal strings; missing values never become measured zeroes.

    Fill counts are execution records, not round trips or a strategy win rate.
    Drawdown includes configured initial cash before the first recorded equity point.
    """
    if not isinstance(result, dict) or type(result.get("schema_version")) is not int:
        raise ValueError("simulation result must have an integer schema_version")
    if result["schema_version"] != 1 or result.get("state") != "complete":
        raise ValueError("only completed simulation schema 1 is supported")
    _validate_tree(result)
    cost_policy = _cost_policy_record(result["cost_policy"]) if "cost_policy" in result else None
    spec = result.get("spec", {})
    if not isinstance(spec, dict):
        raise ValueError("simulation spec must be an object")
    quote = _asset(result.get("quote_asset", spec.get("quote_asset")))
    quotes = result.get("quote_assets", {})
    if not isinstance(quotes, dict):
        raise ValueError("quote_assets must be an object")
    for instrument, currency in quotes.items():
        if not isinstance(instrument, str):
            raise ValueError("instrument identifiers must be strings")
        _asset(currency)
    mixed = bool(quote and any(currency != quote for currency in quotes.values()))
    wallet_currency = None if mixed else quote
    fills = _records(result, "fills")
    cost_quotes = None
    if "cost_quotes" in result:
        if cost_policy is None:
            raise ValueError("cost quote explanations need their explicit policy")
        cost_quotes = _cost_quote_record(result["cost_quotes"], cost_policy, fills)
    events = _records(result, "events")
    has_funding_cashflows = "funding" in result or any(
        event.get("kind") == "funding_cashflow" for event in events or []
    )
    if "funding" in result and type(result["funding"]) is not dict:
        raise ValueError("scenario funding metadata must be an object")
    samples = _records(result, "equity")
    pending = result.get("pending_orders")
    if pending is not None and (type(pending) is not int or pending < 0):
        raise ValueError("pending_orders must be a nonnegative integer")
    positions = result.get("positions")
    if positions is not None and (not isinstance(positions, dict) or len(positions) > 1000):
        raise ValueError("positions must be an object with at most 1000 instruments")
    for value in (positions or {}).values():
        _number(value, "position quantity")
    notes = [
        "fills are executions, not closed round trips; no round-trip win rate is inferred",
        "drawdown uses recorded equity and configured initial cash, not intrabar equity lows",
        "realized slippage cost and actual latency are not recorded; configured values only",
    ]
    if mixed:
        notes.append(
            "mixed quote units: wallet totals and curves are unavailable; no FX conversion"
        )
    if not quote:
        notes.append("wallet currency is not recorded; unlabelled monetary totals are unavailable")
    assumptions = result.get("assumptions", [])
    if not isinstance(assumptions, list) or any(not isinstance(item, str) for item in assumptions):
        raise ValueError("assumptions must be a string list")
    totals = {}
    compatible_costs = True

    def add(currency, field, value):
        key = currency or "UNAVAILABLE"
        group = totals.setdefault(
            key,
            {
                "fee": Decimal(0) if fills is not None and currency else None,
                "funding": Decimal(0) if events is not None and currency else None,
                "realized_pnl": Decimal(0) if fills is not None and currency else None,
            },
        )
        if has_funding_cashflows and "funding_net" not in group:
            group["funding_net"] = Decimal(0) if events is not None and currency else None
        if len(totals) > 128:
            raise ValueError("result exceeds 128 currency groups")
        number = _number(value, field)
        if currency is None or number is None:
            group[field] = None
        elif group[field] is not None:
            group[field] += number

    with localcontext() as context:
        context.prec = 160
        if quote:
            totals[quote] = {
                "fee": Decimal(0) if fills is not None else None,
                "realized_pnl": Decimal(0) if fills is not None else None,
                "funding": Decimal(0) if events is not None else None,
            }
            if has_funding_cashflows:
                totals[quote]["funding_net"] = Decimal(0) if events is not None else None
        for fill in fills or []:
            _timestamp(fill.get("timestamp_ms"))
            for name in ("quantity", "price"):
                _number(fill.get(name), name)
            currency = _asset(quotes.get(fill.get("instrument"), quote))
            fee_currency = _asset(fill.get("fee_asset", fill.get("fee_currency", currency)))
            pnl_currency = _asset(fill.get("pnl_asset", currency))
            compatible_costs = compatible_costs and fee_currency == quote and pnl_currency == quote
            add(fee_currency, "fee", fill.get("fee"))
            add(pnl_currency, "realized_pnl", fill.get("realized_pnl"))
        for event in events or []:
            if event.get("timestamp_ms") is not None:
                _timestamp(event["timestamp_ms"])
            if event.get("kind") == "funding":
                currency = _asset(event.get("currency", quotes.get(event.get("instrument"), quote)))
                compatible_costs = compatible_costs and currency == quote
                add(currency, "funding", event.get("cost"))
            elif event.get("kind") == "funding_cashflow":
                if event.get("source_kind") != "scenario_rate":
                    raise ValueError("funding cashflow requires explicit scenario-rate provenance")
                currency = _asset(event.get("settlement_asset"))
                compatible_costs = compatible_costs and currency == quote
                add(currency, "funding_net", event.get("amount"))
        initial = _number(
            result.get("initial_wallet")
            if result.get("scope") == "virtual_margin_v2_ohlcv_proxy"
            else spec.get("initial_wallet"),
            "initial_wallet",
        )
        final_wallet = _number(result.get("wallet"), "wallet")
        peak = initial
        maximum_drawdown = Decimal(0) if initial is not None and initial > 0 else None
        curve, previous, equity_count = [], None, 0
        for row in samples or []:
            timestamp = _timestamp(row.get("timestamp_ms"))
            if timestamp != int(timestamp) or (previous is not None and timestamp <= previous):
                raise ValueError("equity timestamps must be ordered unique integer milliseconds")
            previous = timestamp
            equity = _number(row.get("equity"), "equity")
            wallet = _number(row.get("wallet"), "wallet")
            drawdown = None
            if equity is not None:
                equity_count += 1
                peak = equity if peak is None else max(peak, equity)
                if peak > 0:
                    drawdown = (peak - equity) / peak * 100
                    maximum_drawdown = max(maximum_drawdown or Decimal(0), drawdown)
            curve.append(
                {
                    "timestamp_ms": int(timestamp),
                    "wallet": _text_number(wallet) if wallet_currency else None,
                    "equity": _text_number(equity) if wallet_currency else None,
                    "drawdown_pct": _text_number(drawdown) if wallet_currency else None,
                }
            )
        last = curve[-1] if curve else {}
        final_equity = _number(last.get("equity"), "final equity")
        last_wallet = _number(last.get("wallet"), "last wallet")
        if final_wallet is not None and last_wallet is not None and final_wallet != last_wallet:
            notes.append("reported final wallet differs from the final recorded curve wallet")
        group = totals.get(quote, {})
        expected, difference = None, None
        if (
            compatible_costs
            and wallet_currency
            and initial is not None
            and all(
                group.get(name) is not None
                for name in ("fee", "funding", "realized_pnl")
                + (("funding_net",) if has_funding_cashflows else ())
            )
        ):
            expected = initial + group["realized_pnl"] - group["fee"] - group["funding"]
            if has_funding_cashflows:
                expected += group["funding_net"]
            if final_wallet is not None:
                difference = final_wallet - expected
        metrics = {
            "initial_wallet": _text_number(initial),
            "final_wallet": _text_number(final_wallet),
            "final_equity": _text_number(final_equity),
            "unrealized_pnl": _text_number(
                final_equity - last_wallet
                if final_equity is not None and last_wallet is not None
                else None
            ),
            "net_wallet_change": _text_number(
                final_wallet - initial if final_wallet is not None and initial is not None else None
            ),
            "equity_return_pct": _text_number(
                (final_equity / initial - 1) * 100
                if final_equity is not None and initial is not None and initial > 0
                else None
            ),
            "max_drawdown_pct": _text_number(maximum_drawdown if equity_count else None),
        }
        if has_funding_cashflows:
            metrics["funding_net"] = _text_number(group.get("funding_net"))
        if not wallet_currency:
            metrics = dict.fromkeys(metrics)
        summary = {
            "schema_version": 1,
            "wallet_currency": wallet_currency,
            "quote_assets": deepcopy(quotes),
            "metrics": metrics,
            "fills_count": len(fills) if fills is not None else None,
            "events_count": len(events) if events is not None else None,
            "pending_orders": pending,
            "positions": deepcopy(positions),
            "costs": [
                {
                    "currency": currency,
                    **{key: _text_number(value) for key, value in values.items()},
                }
                for currency, values in sorted(totals.items())
            ],
            "reconciliation": {
                "expected_wallet": _text_number(expected),
                "difference": _text_number(difference),
                "matches": difference == 0 if difference is not None else None,
            },
            "curve": curve,
            "fills": deepcopy(fills),
            "events": deepcopy(events),
            "spec": deepcopy(spec),
            "input": deepcopy(result.get("input")),
            "backend": deepcopy(result.get("backend")),
            "model": result.get("model"),
            "assumptions": list(assumptions),
            "limitations": notes,
        }
        if has_funding_cashflows:
            summary["funding"] = deepcopy(result.get("funding"))
            summary["limitations"].append(
                "signed funding_net is scenario cashflow, separate from legacy positive "
                "funding cost; not venue payments"
            )
        if cost_policy is not None:
            summary["cost_policy"] = cost_policy
            summary["limitations"].extend(
                [
                    "fixed adverse full-fill scenario policy; final FillEvent price and fee "
                    "already affect the ledger, with no additional slippage wallet debit",
                    "minute-ceiling order eligibility is not measured venue latency; "
                    "reduction delay also delays protective/flatten fills",
                    "end-of-dataset pending orders/exposure remain open",
                ]
            )
            if cost_quotes is None:
                summary["limitations"].append("confirmed raw/final quote summaries are unavailable")
            else:
                summary["cost_quotes"] = cost_quotes
                summary["limitations"][2] = (
                    "source-confirmed scenario raw/final quotes explain applied fills; "
                    "not measured venue slippage/latency and not a second wallet cost"
                )
        return summary


def read_simulation_result(path: Path, allowed_root: Path | None = None) -> dict:
    """Read one bounded regular JSON file; reject path escape or changes while reading."""
    path = Path(path).resolve(strict=True)
    if path.suffix.lower() != ".json":
        raise ValueError("simulation viewer accepts JSON only")
    if allowed_root is not None and not path.is_relative_to(Path(allowed_root).resolve()):
        raise ValueError("job artifact must stay below the core artifact directory")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_RESULT_BYTES:
        raise ValueError("result must be a regular file no larger than 16 MiB")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())

        def stamp(entry):
            return entry.st_dev, entry.st_ino, entry.st_size, entry.st_mtime_ns

        if not stat.S_ISREG(opened.st_mode) or stamp(before) != stamp(opened):
            raise ValueError("result file changed before it could be read")
        raw = stream.read(MAX_RESULT_BYTES + 1)
        if len(raw) > MAX_RESULT_BYTES or stamp(opened) != stamp(os.fstat(stream.fileno())):
            raise ValueError("result file changed or exceeded 16 MiB while reading")

    def invalid_constant(_value):
        raise ValueError("non-finite JSON constants are not permitted")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON object keys are not permitted")
            value[key] = item
        return value

    try:
        result = json.loads(
            raw.decode("utf-8-sig"),
            parse_constant=invalid_constant,
            object_pairs_hook=unique_object,
        )
        summary = summarize_simulation(result)
    except RecursionError as error:
        raise ValueError("result nesting exceeds the parser limit") from error
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "summary": summary}
