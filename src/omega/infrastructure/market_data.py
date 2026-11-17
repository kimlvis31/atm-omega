"""Omega-only database inspection and observed-candle repair adapters."""

import ipaddress
import re
import time
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.execution_rules import rules_snapshot
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.candles import EPOCH, PostgresCandleStore, candle_from_row
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import verify_schema
from omega.ports.collection import MinuteObservation, accept_rest_observations

MINUTE = 60_000


def instrument_key(value):
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (AttributeError, TypeError, ValueError):
        raise ValidationError("expected Binance market instrument identity") from None
    if result.provider != "binance" or result.market not in {Market.SPOT, Market.USDM}:
        raise ValidationError("unsupported market data provider")
    return result


def peer_parameters(parameters):
    host = parameters.get("host")
    if not isinstance(host, str):
        raise ValidationError("peer host must be an IP literal")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise ValidationError("peer host must be a private or loopback IP literal") from None
    networks = (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "127.0.0.0/8",
        "::1/128",
        "fc00::/7",
    )
    if not any(
        address.version == network.version and address in network
        for network in map(ipaddress.ip_network, networks)
    ):
        raise ValidationError("peer host must be private or loopback")
    port = parameters.get("port", 5432)
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValidationError("invalid peer database port")
    for key in ("database", "user"):
        if not isinstance(parameters.get(key), str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_-]{0,62}", parameters[key]
        ):
            raise ValidationError("peer database and user require bounded identifiers")
    password = parameters.get("password")
    if (
        not isinstance(password, str)
        or not password
        or len(password) > 4096
        or any(character in password for character in "\x00\r\n")
    ):
        raise ValidationError("invalid peer credential")
    return {
        "host": str(address),
        "port": port,
        "database": parameters["database"],
        "user": parameters["user"],
        "password": password,
    }


def milliseconds(value):
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def ranges(open_times, start, end):
    """Return disjoint half-open observed and missing minute intervals."""
    observed, missing = [], []
    cursor = start
    for stamp in sorted(set(open_times)):
        if not start <= stamp < end:
            continue
        if stamp > cursor:
            missing.append([cursor, stamp])
        if observed and observed[-1][1] == stamp:
            observed[-1][1] = stamp + MINUTE
        else:
            observed.append([stamp, stamp + MINUTE])
        cursor = stamp + MINUTE
    if cursor < end:
        missing.append([cursor, end])
    return observed, missing


class MarketDataBackend:
    def __init__(self, root: Path, *, capture_rest_raw=False, raw_directory=None):
        self.root = Path(root)
        if type(capture_rest_raw) is not bool or not capture_rest_raw and raw_directory is not None:
            raise ValidationError("raw directory requires explicit REST capture")
        self.raw_archive = None
        if capture_rest_raw:
            from omega.infrastructure.raw_archive import FileRawArchive

            directory = (
                self.root / "data/raw/candles" if raw_directory is None else Path(raw_directory)
            )
            self.raw_archive = FileRawArchive(directory)
        self.histories = {}

    def _store(self):
        settings = load_database_settings(self.root / "database.config.toml")
        if settings.host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValidationError("local Omega database must use loopback")
        if settings.host == "localhost":
            settings = replace(settings, host="127.0.0.1")
        return PostgresCandleStore(settings)

    def history(self, market):
        key = Market(market)
        if key not in self.histories:
            self.histories[key] = (
                self._history(key, root=self.root, raw_archive=self.raw_archive)
                if self.raw_archive is not None
                else self._history(key, root=self.root)
            )
        return self.histories[key]

    @staticmethod
    def _history(market, *, root=None, raw_archive=None):
        market = Market(market)
        client = BinanceClient(
            "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
            UrllibTransport(timeout_seconds=8),
            budget=(
                SharedRequestBudget(Path(root) / "state" / "binance-public-budget.json")
                if root is not None
                else RequestBudget((Window("binance-ip", 600, 60),))
            ),
        )
        return (
            BinanceMarketData(market, client, raw_archive=raw_archive)
            if raw_archive is not None
            else BinanceMarketData(market, client)
        )

    def catalog(self, market):
        result = {}
        symbols = self.history(market).symbols()
        observed_at_ms = time.time_ns() // 1_000_000
        for rules in symbols:
            metadata = {
                "base_asset": rules.base_asset,
                "quote_asset": rules.quote_asset,
                "price_tick": None,
                "quantity_step": None,
                "min_notional": None,
            }
            try:
                snapshot = rules_snapshot(rules, observed_at_ms=observed_at_ms)
                metadata["execution_rules"] = snapshot.document()
                metadata["price_tick"] = str(snapshot.price.step)
                metadata["quantity_step"] = str(snapshot.quantity.step)
                if snapshot.notionals:
                    metadata["min_notional"] = str(max(rule.minimum for rule in snapshot.notionals))
            except (ProtocolError, ValidationError, TypeError, AttributeError):
                # One bad symbol must not discard unrelated valid references.
                metadata["execution_rules_error"] = "invalid_or_unsupported_execution_rules"
            result[rules.instrument.key] = {"status": rules.status, "metadata": metadata}
        if len(result) > 10_000:
            raise ProtocolError("exchange catalog exceeds supported capacity")
        return result

    @staticmethod
    def _times(store, instrument, start, end):
        rows = store.connection.execute(
            "SELECT open_time FROM market.candles_1m WHERE instrument=%s "
            "AND open_time>=%s AND open_time<%s ORDER BY open_time LIMIT 20001",
            (
                instrument.key,
                EPOCH + timedelta(milliseconds=start),
                EPOCH + timedelta(milliseconds=end),
            ),
        ).fetchall()
        if len(rows) > 20_000:
            raise StorageError("bounded candle inspection capacity exceeded")
        return [milliseconds(row[0]) for row in rows]

    def refresh(self, market, selected=None):
        store = self._store()
        try:
            rows = store.connection.execute(
                "SELECT instrument,min(open_time),max(open_time),count(*),max(received_ms) "
                "FROM market.candles_1m GROUP BY instrument ORDER BY instrument LIMIT 10001"
            ).fetchall()
            if len(rows) > 10_000:
                raise StorageError("database instrument capacity exceeded")
            size = store.connection.execute(
                "SELECT pg_database_size(current_database())"
            ).fetchone()[0]
            items, last_fetched, detail = [], None, None
            for key, first, last, count, received in rows:
                instrument = instrument_key(key)
                if instrument.market.value != market:
                    continue
                first_ms, last_ms = milliseconds(first), milliseconds(last)
                expected = (last_ms - first_ms) // MINUTE + 1
                gap = max(0, expected - count)
                item = {
                    "instrument": key,
                    "symbol": instrument.symbol,
                    "market": market,
                    "status": "observed",
                    "first_open_ms": first_ms,
                    "last_open_ms": last_ms,
                    "rows": int(count),
                    "gap_rows": gap,
                    "availability": count / expected,
                    "gap_rate": gap / expected,
                    "dummy_rate": None,
                    "metadata": {},
                }
                items.append(item)
                last_fetched = max(last_fetched or 0, received)
                if key == selected:
                    start = max(first_ms, last_ms - 19_999 * MINUTE)
                    available, gaps = ranges(
                        self._times(store, instrument, start, last_ms + MINUTE),
                        start,
                        last_ms + MINUTE,
                    )
                    detail = {
                        "instrument": key,
                        "metadata": {},
                        "channels": {
                            "ohlcv": {
                                "supported": True,
                                **{
                                    field: item[field]
                                    for field in (
                                        "first_open_ms",
                                        "last_open_ms",
                                        "rows",
                                        "gap_rows",
                                        "availability",
                                        "gap_rate",
                                        "dummy_rate",
                                    )
                                },
                                "available_ranges": available,
                                "gap_ranges": gaps,
                                "range_window": {"start_ms": start, "end_ms": last_ms + MINUTE},
                                "scope": "internal gaps; exact ranges bounded "
                                "to latest 20000 minutes",
                            },
                            "depth": {"supported": False},
                            "agg_trade": {"supported": False},
                            "metrics": {"supported": False},
                        },
                    }
            return {
                "database": {
                    "state": "ready",
                    "engine": "PostgreSQL + TimescaleDB",
                    "name": store.connection.info.dbname,
                    "size_bytes": int(size),
                    "free_bytes": None,
                    "total_bytes": None,
                    "compression_before_bytes": None,
                    "compression_after_bytes": None,
                    "message": "Observed OHLCV only; disk capacity and compression "
                    "are not reported.",
                },
                "instruments": items,
                "selected": detail,
                "last_fetched": last_fetched,
            }
        finally:
            store.close()

    def _repair(self, instrument, lookback, fetch, progress, stop):
        store = self._store()
        end = time.time_ns() // 1_000_000 // MINUTE * MINUTE
        start = max(0, end - lookback * MINUTE)
        inserted, skipped_raw = 0, 0
        try:
            _, gaps = ranges(self._times(store, instrument, start, end), start, end)
            planned = sum((b - a) // MINUTE for a, b in gaps)
            scanned = 0
            for low, high in gaps:
                for cursor in range(low, high, 1000 * MINUTE):
                    if stop.is_set():
                        raise InterruptedError("market data task cancelled")
                    until = min(high, cursor + 1000 * MINUTE)
                    observations = fetch(instrument, cursor, until, end)
                    for observation in observations:
                        if stop.is_set():
                            raise InterruptedError("market data task cancelled")
                        candle = observation.candle
                        if (
                            candle.instrument != instrument
                            or not cursor <= candle.open_time_ms < until
                            or observation.source not in {"rest", "websocket"}
                        ):
                            raise ProtocolError("repair returned an invalid observation")
                        existing = store.get(instrument, candle.open_time_ms)
                        if existing is not None:
                            if existing != candle:
                                raise ProtocolError(
                                    "a concurrent observed candle conflicts; retained original"
                                )
                            continue
                        canonical = MinuteObservation(
                            candle, observation.source, observation.received_at_ms
                        )
                        inserted += int(store.put(canonical))
                        if observation.raw is not None:
                            if (
                                observation.raw_source in {"rest", "websocket"}
                                and type(observation.raw_received_at_ms) is int
                            ):
                                store.put(
                                    MinuteObservation(
                                        candle,
                                        observation.source,
                                        observation.received_at_ms,
                                        observation.raw,
                                        observation.raw_source,
                                        observation.raw_received_at_ms,
                                    )
                                )
                            else:
                                skipped_raw += 1
                    scanned += (until - cursor) // MINUTE
                    progress(scanned / planned if planned else 1)
            _, unresolved = ranges(self._times(store, instrument, start, end), start, end)
            return {
                "inserted_rows": inserted,
                "remaining_ranges": len(unresolved),
                "remaining_minutes": sum((b - a) // MINUTE for a, b in unresolved),
                "window": [start, end],
                "raw_payloads_skipped_unknown_provenance": skipped_raw,
                "observed_only": True,
            }
        finally:
            store.close()

    def refetch(self, key, lookback, progress, stop):
        instrument = instrument_key(key)
        history = self.history(instrument.market)
        if self.raw_archive is not None and not callable(
            getattr(history, "closed_observations", None)
        ):
            raise ValidationError("REST capture requires original observation history")

        def fetch(instrument, low, high, now):
            if self.raw_archive is not None:
                observations = history.closed_observations(instrument, low, high, now_ms=now)
                return accept_rest_observations(
                    observations, instrument, low, high, time.time_ns() // 1_000_000
                )
            received = time.time_ns() // 1_000_000
            return [
                MinuteObservation(candle, "rest", received)
                for candle in history.closed_minutes(instrument, low, high, now_ms=now)
            ]

        return self._repair(instrument, lookback, fetch, progress, stop)

    def import_peer(self, key, parameters, lookback, progress, stop):
        peer = peer_parameters(parameters)
        try:
            import psycopg

            connection = psycopg.connect(
                host=peer["host"],
                port=peer["port"],
                dbname=peer["database"],
                user=peer["user"],
                password=peer["password"],
                connect_timeout=5,
                autocommit=True,
                application_name="omega-readonly-peer",
                options="-c default_transaction_read_only=on "
                "-c statement_timeout=5000 -c lock_timeout=2000",
            )
        except ImportError:
            raise StorageError("PostgreSQL support is not installed") from None
        except Exception:
            raise StorageError("peer database connection failed") from None
        try:
            verify_schema(connection)

            def fetch(instrument, low, high, now):
                rows = connection.execute(
                    "SELECT open_time,close_time,o,h,l,c,v,source,received_ms,raw,raw_source,"
                    "raw_received_ms FROM market.candles_1m WHERE instrument=%s "
                    "AND open_time>=%s AND open_time<%s ORDER BY open_time LIMIT 1001",
                    (
                        instrument.key,
                        EPOCH + timedelta(milliseconds=low),
                        EPOCH + timedelta(milliseconds=high),
                    ),
                ).fetchall()
                if len(rows) > 1000:
                    raise ProtocolError("peer response exceeds requested range")
                return [
                    MinuteObservation(candle_from_row(instrument, row[:7]), *row[7:])
                    for row in rows
                ]

            return self._repair(instrument_key(key), lookback, fetch, progress, stop)
        finally:
            peer.clear()
            connection.close()
