"""Explicit, bounded public REST history scans; no account or order adapters."""

# Imports
import argparse
import json
import math
import re
import threading
import time
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from omega.application.history_backfill import (
    MAX_STATE_BYTES,
    MINUTE,
    BackfillPlan,
    HistoryBackfill,
)
from omega.domain.errors import OmegaError, RateLimited, TransportError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.candles import PostgresCandleStore
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Scan a fixed closed UTC 1m range into Omega PostgreSQL/TimescaleDB. "
        "Public REST only; missing minutes remain gaps and original payload is unavailable."
    )
    result.add_argument("--root", required=True, type=Path, help="Omega project root/shared budget")
    result.add_argument(
        "--db-config", required=True, type=Path, help="explicit Omega DB configuration"
    )
    result.add_argument("--market", choices=["spot", "usdm"])
    result.add_argument("--symbol")
    start = result.add_mutually_exclusive_group()
    start.add_argument(
        "--from-ms", type=int, help="inclusive UTC epoch milliseconds, minute aligned"
    )
    start.add_argument(
        "--from-utc", help="inclusive aware ISO timestamp, e.g. 2026-10-01T00:00:00Z"
    )
    end = result.add_mutually_exclusive_group()
    end.add_argument("--to-ms", type=int, help="exclusive fixed closed-minute boundary")
    end.add_argument("--to-utc", help="exclusive aware ISO timestamp")
    result.add_argument("--chunk-minutes", type=int, help="1..1000; default 512 for a new plan")
    result.add_argument(
        "--resume", help="existing 64-character hexadecimal plan ID; no range overrides"
    )
    return result


def utc_ms(value: str) -> int:
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.utcoffset() is None:
            raise ValueError("timezone missing")
        elapsed = stamp.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        micros = (elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds
        if micros < 0 or micros % (MINUTE * 1000):
            raise ValueError("not minute aligned")
        return micros // 1000
    except (ValueError, TypeError, OverflowError):
        raise ValidationError("UTC range requires aware, exact minute ISO timestamps") from None


def destination_id(settings) -> str:
    # Hash identity only: neither password nor password-derived material enters the checkpoint.
    identity = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.dbname,
        "user": settings.user,
        "schema": "market",
    }
    return sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def _safe_path(root: Path, relative: Path) -> Path:
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink() or current.is_junction():
            raise ValidationError("history state cannot use a linked path")
    resolved = current.resolve()
    if not resolved.is_relative_to(root):
        raise ValidationError("history state escaped the project root")
    return current


class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.expected = None
        self.state = None
        if path.exists():
            raw = self._read()
            try:
                self.state = json.loads(raw)
            except (ValueError, UnicodeError):
                raise ValidationError("historical checkpoint is not valid JSON") from None
            self.expected = sha256(raw).digest()

    def _read(self):
        if self.path.is_symlink() or self.path.is_junction():
            raise ValidationError("historical checkpoint cannot be a linked path")
        with self.path.open("rb") as stream:
            raw = stream.read(MAX_STATE_BYTES + 1)
        if len(raw) > MAX_STATE_BYTES:
            raise ValidationError("historical checkpoint exceeds its bound")
        return raw

    def save(self, state):
        # Cooperating writers hold one plan lock. Also refuse external edits/deletion
        # rather than overwriting an unexpected generation during this process.
        if self.path.exists():
            if self.expected is None or sha256(self._read()).digest() != self.expected:
                raise ValidationError("historical checkpoint changed during this scan")
        elif self.expected is not None:
            raise ValidationError("historical checkpoint disappeared during this scan")
        committed = sha256(
            json.dumps(state, ensure_ascii=False, allow_nan=False, sort_keys=True).encode("utf-8")
        ).digest()
        atomic_json(self.path, state)
        if sha256(self._read()).digest() != committed:
            raise ValidationError("historical checkpoint changed immediately after commit")
        self.expected = committed
        self.state = deepcopy(state)


class RetryHistory:
    """Only idempotent public reads are retried; long throttles require later resume."""

    def __init__(self, source, stop, *, attempts=3, wait=None):
        self.source, self.stop, self.attempts = source, stop, attempts
        self.wait = wait or stop.wait

    def closed_minutes(self, *args, **kwargs):
        for attempt in range(self.attempts):
            if self.stop.is_set():
                raise InterruptedError("historical scan interrupted")
            try:
                return self.source.closed_minutes(*args, **kwargs)
            except (TransportError, RateLimited) as exc:
                if attempt + 1 == self.attempts:
                    raise
                delay = 2**attempt
                if isinstance(exc, RateLimited):
                    delay = exc.retry_after if exc.retry_after is not None else 60
                    if not isinstance(delay, (int, float)) or not math.isfinite(delay) or delay < 0:
                        raise ValidationError("invalid provider retry interval") from None
                    if delay > 60:
                        raise  # Never retry before Retry-After; user can resume later.
                if self.wait(max(delay, 0.05)) or self.stop.is_set():
                    raise InterruptedError("historical scan interrupted") from None
        raise AssertionError("unreachable read retry state")


def _plan(args, settings, root):
    destination = destination_id(settings)
    if args.resume is not None:
        if not re.fullmatch(r"[a-f0-9]{64}", args.resume):
            raise ValidationError("resume requires a 64-character lowercase hexadecimal plan ID")
        if any(
            value is not None
            for value in (
                args.market,
                args.symbol,
                args.from_ms,
                args.from_utc,
                args.to_ms,
                args.to_utc,
            )
        ):
            raise ValidationError("resume does not permit instrument/range overrides")
        path = _safe_path(root, Path("state/backfill") / (args.resume + ".json"))
        checkpoint = Checkpoint(path)
        if not isinstance(checkpoint.state, dict):
            raise ValidationError("resume checkpoint does not exist")
        plan = BackfillPlan.from_specification(checkpoint.state.get("plan"))
        if plan.plan_id != args.resume or plan.destination_id != destination:
            raise ValidationError("resume plan or destination does not match")
        if args.chunk_minutes is not None and args.chunk_minutes != plan.chunk_minutes:
            raise ValidationError("resume page size does not match the fixed plan")
        HistoryBackfill.validate_state(plan, checkpoint.state)
        return plan, path
    if args.market is None or args.symbol is None:
        raise ValidationError("new scans require market and symbol")
    start = utc_ms(args.from_utc) if args.from_utc is not None else args.from_ms
    end = utc_ms(args.to_utc) if args.to_utc is not None else args.to_ms
    plan = BackfillPlan(
        Instrument("binance", Market(args.market), args.symbol),
        start,
        end,
        destination,
        args.chunk_minutes if args.chunk_minutes is not None else 512,
    )
    return plan, _safe_path(root, Path("state/backfill") / (plan.plan_id + ".json"))


def run(args) -> int:
    root = args.root.resolve(strict=True)
    if not root.is_dir():
        raise ValidationError("history root must be a directory")
    settings = load_database_settings(args.db_config)
    plan, path = _plan(args, settings, root)
    budget_path = _safe_path(root, Path("state/binance-public-budget.json"))
    lock_path = _safe_path(root, Path("state/backfill") / (plan.plan_id + ".lock"))
    owner = InstanceLock(lock_path)
    store, runner = None, None
    stop = threading.Event()
    try:
        checkpoint = Checkpoint(path)  # Read again under the cooperative plan lock.
        if checkpoint.state is not None:
            HistoryBackfill.validate_state(plan, checkpoint.state)
        now = time.time_ns() // 1_000_000
        if plan.end_ms > now // MINUTE * MINUTE:
            raise ValidationError("historical target is not yet closed")
        store = PostgresCandleStore(settings)
        budget = SharedRequestBudget(budget_path)
        client = BinanceClient(
            "https://api.binance.com"
            if plan.instrument.market == Market.SPOT
            else "https://fapi.binance.com",
            UrllibTransport(),
            budget=budget,
        )
        history = RetryHistory(BinanceMarketData(plan.instrument.market, client), stop)
        runner = HistoryBackfill(
            plan,
            history,
            store,
            persist=checkpoint.save,
            clock_ms=lambda: time.time_ns() // 1_000_000,
            state=checkpoint.state,
        )
        print(
            json.dumps(
                {
                    "state": "starting",
                    "plan_id": plan.plan_id,
                    "from_ms": plan.start_ms,
                    "target_end_ms": plan.end_ms,
                    "total_minutes": (plan.end_ms - plan.start_ms) // MINUTE,
                    "next_open_ms": runner.snapshot()["next_open_ms"],
                }
            ),
            flush=True,
        )
        while True:
            state = runner.step()
            print(
                json.dumps(
                    {
                        key: state[key]
                        for key in (
                            "plan_id",
                            "next_open_ms",
                            "processed_minutes",
                            "observed_minutes",
                            "missing_minutes",
                            "state",
                            "raw_payload",
                        )
                    }
                ),
                flush=True,
            )
            if state["state"] != "scanning":
                return 3 if state["state"] == "complete_with_gaps" else 0
    finally:
        stop.set()
        if runner is not None:
            runner.close()
        try:
            if store is not None:
                store.close()
        finally:
            owner.close()


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        return run(args)
    except (KeyboardInterrupt, InterruptedError):
        print(
            json.dumps({"state": "interrupted", "resume": "repeat the same fixed plan"}), flush=True
        )
        return 130
    except (OmegaError, OSError, RuntimeError, ValueError) as exc:
        # Exceptions may originate in a driver. Do not print DSNs, secret paths or server text.
        print(
            json.dumps(
                {
                    "state": "failed",
                    "error": type(exc).__name__,
                    "resume": "checkpoint retained; correct the cause and repeat",
                }
            ),
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
