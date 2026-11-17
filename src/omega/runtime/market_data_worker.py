"""Owned public collector processes. Parent pipe EOF is a graceful stop request."""

import argparse
import asyncio
import json
import os
import secrets
import signal
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.streams import StreamPlan
from omega.infrastructure.market_data import MarketDataBackend
from omega.runtime.collection import CollectionWorker
from omega.runtime.settings import atomic_json


def collection_starts(instruments, lookback, plan, now_ms):
    """A stream-only subscription never inherits another symbol's history window."""
    if type(lookback) is not int or not 0 <= lookback <= 20_000:
        raise ValidationError("invalid collector lookback")
    symbols = {instrument.symbol for instrument in instruments}
    if len(symbols) != len(instruments):
        raise ValidationError("duplicate collector symbol")
    if plan is None:
        plan = dict.fromkeys(symbols, lookback)
    if (
        not isinstance(plan, dict)
        or set(plan) != symbols
        or any(type(value) is not int or not 0 <= value <= 20_000 for value in plan.values())
    ):
        raise ValidationError("collector history plan must match each selected symbol")
    end = now_ms // 60_000 * 60_000
    return {
        instrument: max(0, end - plan[instrument.symbol] * 60_000) for instrument in instruments
    }


class OwnedCollectors:
    def __init__(self, root: Path, *, capture_rest_raw=False, raw_directory=None):
        self.root, self.children = Path(root).resolve(), {}
        self.protection_generations, self.protection_seen = {}, {}
        self.protection_envelopes = {}
        self.protection_error = None
        if type(capture_rest_raw) is not bool or not capture_rest_raw and raw_directory is not None:
            raise ValidationError("raw directory requires explicit REST capture")
        self.capture_rest_raw = capture_rest_raw
        self.raw_directory = (
            Path(raw_directory) if raw_directory is not None else self.root / "data/raw/candles"
        ).absolute()
        self.closed = threading.Event()
        self.locks = {"spot": threading.RLock(), "usdm": threading.RLock()}

    def configure(self, market, instruments, lookback, historical, *, starts=None):
        with self.locks[market]:
            if self.closed.is_set():
                raise StorageError("collector owner is closed")
            self._configure(market, instruments, lookback, historical, starts=starts)

    def _configure(self, market, instruments, lookback, historical, *, starts=None):
        plan = {
            key.split(":")[2]: (
                starts[key] if starts is not None else lookback if historical else 0
            )
            for key in instruments
        }
        # Validate before replacing the currently running subscriptions.
        collection_starts(
            tuple(Instrument("binance", Market(market), key.split(":")[2]) for key in instruments),
            lookback if historical else 0,
            plan,
            time.time_ns() // 1_000_000,
        )
        self.stop(market)
        if not instruments or self.closed.is_set():
            return
        folder = self.root / "logs" / "collection"
        if not folder.resolve().is_relative_to(self.root):
            raise StorageError("collector log path escapes project root")
        folder.mkdir(parents=True, exist_ok=True)
        log = folder / f"{market}-{time.time_ns()}-{secrets.token_hex(4)}.log"
        generation = secrets.token_hex(16)
        command = [
            sys.executable,
            "-I",
            "-X",
            "utf8",
            "-u",
            "-m",
            "omega.runtime.market_data_worker",
            "--root",
            str(self.root),
            "--db-config",
            str(self.root / "database.config.toml"),
            "--market",
            market,
            "--lookback-minutes",
            str(lookback if historical else 0),
            "--start-plan",
            json.dumps(plan, separators=(",", ":")),
            "--health-file",
            str(log.with_suffix(".health.json")),
            "--protection-generation",
            generation,
            "--protection-file",
            str(log.with_suffix(".protection.json")),
            "--symbols",
            *(instrument.split(":")[2] for instrument in instruments),
        ]
        if self.capture_rest_raw:
            command.extend(["--capture-rest-raw", "--raw-directory", str(self.raw_directory)])
        options = {"cwd": self.root, "stdin": subprocess.PIPE, "text": True, "close_fds": True}
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NO_WINDOW
        else:
            options["start_new_session"] = True
        with log.open("xb") as stream:
            process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT, **options)
        self.children[market] = (process, tuple(instruments), log)
        self.protection_generations[market] = (process, generation)
        self.protection_seen[market] = {}
        self.protection_envelopes.pop(market, None)

    def running(self, market):
        child = self.children.get(market)
        return child is not None and child[0].poll() is None

    def status(self, market):
        """Process existence is separate from observed contiguous-data readiness."""
        if not self.locks[market].acquire(blocking=False):
            return {
                "state": "configuring",
                "running": False,
                "ready": False,
                "instrument_ready": {},
                "last_close_ms": {},
            }
        try:
            child = self.children.get(market)
            if child is None:
                return {"state": "stopped", "running": False, "ready": False}
            process, instruments, log = child
            code = process.poll()
            result = {
                "state": "starting" if code is None else "failed",
                "running": code is None,
                "ready": False,
                "exit_code": code,
                "instrument_ready": {},
                "last_close_ms": {},
            }
            if code is not None:
                return result
            path = log.with_suffix(".health.json")
            try:
                if path.stat().st_size > 65_536:
                    raise ValueError
                value = json.loads(path.read_text(encoding="utf-8"))
                now = time.time_ns() // 1_000_000
                if (
                    value.get("schema_version") != 1
                    or value.get("market") != market
                    or value.get("instruments") != list(instruments)
                    or type(value.get("updated_at_ms")) is not int
                    or not 0 <= now - value["updated_at_ms"] <= 45_000
                    or type(value.get("ready")) is not bool
                    or value.get("reason")
                    not in {"ready", "gap", "stale", "connecting", "disconnected", "rate_limited"}
                    or set(value.get("instrument_ready", {})) != set(instruments)
                    or set(value.get("last_close_ms", {})) != set(instruments)
                    or any(type(flag) is not bool for flag in value["instrument_ready"].values())
                    or any(
                        type(stamp) is not int
                        or stamp < 0
                        or stamp % 60_000
                        or stamp > now // 60_000 * 60_000
                        for stamp in value["last_close_ms"].values()
                    )
                    or value["ready"] != all(value["instrument_ready"].values())
                    or (value["reason"] == "ready") != value["ready"]
                ):
                    raise ValueError
                result.update(
                    state=value["reason"],
                    ready=value["ready"],
                    updated_at_ms=value["updated_at_ms"],
                    instrument_ready=value["instrument_ready"],
                    last_close_ms=value["last_close_ms"],
                )
                # A healthy report from just before the minute boundary isn't
                # readiness for the newly closed minute, even within its TTL.
                result["instrument_ready"] = {
                    key: flag and value["last_close_ms"][key] >= now // 60_000 * 60_000
                    for key, flag in value["instrument_ready"].items()
                }
                result["ready"] = all(result["instrument_ready"].values())
                if result["state"] == "ready" and not result["ready"]:
                    result["state"] = "stale"
            except FileNotFoundError:
                pass
            except (OSError, TypeError, ValueError, AttributeError):
                result["state"] = "health_unavailable"
            return result
        finally:
            self.locks[market].release()

    def protection_observations(self, instrument_keys, *, as_of_ms):
        if self.closed.is_set():
            return ()
        if self.protection_error is not None:
            raise StorageError("protection source held after invalid observed history")
        try:
            return self._protection_observations(instrument_keys, as_of_ms=as_of_ms)
        except (OSError, ValueError, TypeError, KeyError, ValidationError, StorageError) as error:
            self.protection_error = type(error).__name__
            raise StorageError("owned protection observation source is held") from None

    def _protection_observations(self, instrument_keys, *, as_of_ms):
        """Read only current original children, not arbitrary persisted descriptors.

        Local files are a bounded process transport, not tamper-proof authority.
        Latest snapshots can coalesce updates; no every-packet delivery is claimed.
        """
        from omega.domain.protection import ProtectionObservation

        result = []
        for market in self.locks:
            with self.locks[market]:
                child = self.children.get(market)
                bound = self.protection_generations.get(market)
                if (
                    child is None
                    or bound is None
                    or bound[0] is not child[0]
                    or child[0].poll() is not None
                ):
                    continue
                process, instruments, log = child
                selected = set(instruments).intersection(instrument_keys)
                if not selected:
                    continue
                path = log.with_suffix(".protection.json")
                try:
                    if not path.resolve().is_relative_to(self.root) or any(
                        item.is_symlink() or item.is_junction() for item in (path, *path.parents)
                    ):
                        raise StorageError("invalid owned protection transport file")
                    descriptor = os.open(
                        path,
                        os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0),
                    )
                    try:
                        info = os.fstat(descriptor)
                        if (
                            not stat.S_ISREG(info.st_mode)
                            or info.st_size > 262144
                            or getattr(info, "st_file_attributes", 0) & 0x400
                        ):
                            raise StorageError(
                                "protection transport must be a bounded regular file"
                            )
                        with os.fdopen(descriptor, "rb", closefd=False) as stream:
                            raw = stream.read(262145)
                    finally:
                        os.close(descriptor)
                    if len(raw) > 262144:
                        raise StorageError("protection transport exceeds capacity")

                    def unique(pairs):
                        result = {}
                        for key, item in pairs:
                            if key in result:
                                raise ValueError("duplicate protection field")
                            result[key] = item
                        return result

                    value = json.loads(raw, object_pairs_hook=unique)
                except FileNotFoundError:
                    continue
                if (
                    type(value) is not dict
                    or set(value)
                    != {
                        "version",
                        "generation",
                        "market",
                        "instruments",
                        "connected",
                        "connection",
                        "envelope_sequence",
                        "observations",
                    }
                    or type(value["version"]) is not int
                    or value["version"] != 1
                    or value["generation"] != bound[1]
                    or value["market"] != market
                    or value["instruments"] != list(instruments)
                    or type(value["connected"]) is not bool
                    or type(value["connection"]) is not int
                    or not 0 <= value["connection"] <= 2**53 - 1
                    or type(value["envelope_sequence"]) is not int
                    or not 1 <= value["envelope_sequence"] <= 2**53 - 1
                    or value["connected"]
                    and value["connection"] == 0
                    or type(value["observations"]) is not dict
                    or not set(value["observations"]).issubset(instruments)
                    or not value["connected"]
                    and value["observations"]
                ):
                    raise StorageError("owned protection transport identity changed")
                previous_envelope = self.protection_envelopes.get(market)
                if previous_envelope is not None:
                    if (
                        value["connection"] < previous_envelope["connection"]
                        or not previous_envelope["connected"]
                        and value["connected"]
                        and value["connection"] <= previous_envelope["connection"]
                        or value["envelope_sequence"] < previous_envelope["envelope_sequence"]
                        or value["envelope_sequence"] == previous_envelope["envelope_sequence"]
                        and value != previous_envelope
                    ):
                        raise StorageError(
                            "protection connection/publication fence moved backwards"
                        )
                seen = self.protection_seen[market]
                for key, document in value["observations"].items():
                    observation = ProtectionObservation.from_document(document)
                    if (
                        observation.instrument.key != key
                        or observation.generation != bound[1]
                        or observation.connection != value["connection"]
                        or observation.instrument.provider != "binance"
                        or observation.instrument.market != Market(market)
                        or observation.source != "binance_ws_kline_1m_partial"
                        or observation.price_kind != "kline_last"
                    ):
                        raise StorageError("foreign protection source")
                    previous = seen.get(key)
                    if previous is not None:
                        if (
                            observation.identity == previous.identity
                            and observation.document() != previous.document()
                        ):
                            raise StorageError("same protection identity changed payload")
                        if (observation.connection, observation.sequence) < (
                            previous.connection,
                            previous.sequence,
                        ):
                            raise StorageError("protection source sequence moved backwards")
                        if observation.event_time_ms < previous.event_time_ms:
                            raise StorageError("protection event clock moved backwards")
                    seen[key] = observation
                    if key in selected:
                        try:
                            observation.current(as_of_ms)
                        except ValidationError:
                            continue
                        result.append(observation)
                self.protection_envelopes[market] = value
                # A concurrent stop cannot replace this original handle under our lock.
                if process.poll() is not None:
                    result = [row for row in result if row.instrument.market.value != market]
        return tuple(result)

    def stop(self, market):
        with self.locks[market]:
            self._stop(market)

    def _stop(self, market):
        child = self.children.get(market)
        if child is None:
            return
        process = child[0]
        try:
            if process.poll() is None:
                try:
                    process.stdin.write('{"command":"stop"}\n')
                    process.stdin.flush()
                except (OSError, ValueError):
                    pass
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    # This Popen handle was created by this owner, never adopted
                    # by PID from a previous run or another project's descriptor.
                    process.terminate()
                    process.wait(timeout=5)
        finally:
            if process.stdin:
                process.stdin.close()
            if process.poll() is not None:
                self.children.pop(market, None)
                self.protection_generations.pop(market, None)
                self.protection_seen.pop(market, None)
                self.protection_envelopes.pop(market, None)

    def close(self):
        self.closed.set()
        errors = []

        def stop(market):
            try:
                self.stop(market)
            except Exception:
                errors.append(market)

        threads = [
            threading.Thread(target=stop, args=(market,), daemon=True) for market in self.locks
        ]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 21
        for thread in threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        if errors or any(thread.is_alive() for thread in threads):
            raise StorageError("owned collector shutdown was not confirmed")


async def collect(args):
    capture = getattr(args, "capture_rest_raw", False)
    directory = getattr(args, "raw_directory", None)
    if type(capture) is not bool or not capture and directory is not None:
        raise ValidationError("raw directory requires explicit REST capture")
    if directory is not None and (
        not isinstance(directory, (str, Path))
        or not str(directory)
        or any(ord(char) < 32 for char in str(directory))
    ):
        raise ValidationError("raw directory must be a local filesystem path")
    archive = None
    instruments = tuple(
        Instrument("binance", Market(args.market), symbol) for symbol in args.symbols
    )
    now = time.time_ns() // 1_000_000
    starts = collection_starts(instruments, args.lookback_minutes, args.start_plan, now)
    if args.db_config.resolve() != args.root.resolve() / "database.config.toml":
        raise StorageError("collector must use this project's explicit database configuration")
    if capture:
        from omega.infrastructure.raw_archive import FileRawArchive

        archive = FileRawArchive(args.root / "data/raw/candles" if directory is None else directory)
    store = MarketDataBackend(args.root)._store()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def watch_parent():
        try:
            while line := sys.stdin.readline():
                if json.loads(line).get("command") == "stop":
                    break
        except (OSError, ValueError, AttributeError):
            pass
        try:
            loop.call_soon_threadsafe(stop.set)
        except RuntimeError:
            pass

    threading.Thread(target=watch_parent, name="collector-parent", daemon=True).start()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        collector = MinuteCollector(starts, store, lambda _candle: None)
        last_health, last_written = None, 0.0

        def health(ready, reason):
            nonlocal last_health, last_written
            now = time.time_ns() // 1_000_000
            flags = {i.key: collector.ready(i, now) for i in instruments}
            current = ready, reason, tuple(collector.cursor.values())
            if current == last_health and time.monotonic() - last_written < 5:
                return
            if args.health_file is not None:
                atomic_json(
                    args.health_file,
                    {
                        "schema_version": 1,
                        "market": args.market,
                        "instruments": [i.key for i in instruments],
                        "updated_at_ms": now,
                        "ready": all(flags.values()),
                        "reason": "stale"
                        if reason == "ready" and not all(flags.values())
                        else reason,
                        "instrument_ready": flags,
                        "last_close_ms": {i.key: stamp for i, stamp in collector.cursor.items()},
                    },
                )
            if current != last_health:
                print(json.dumps({"ready": ready, "reason": reason}), flush=True)
            last_health, last_written = current, time.monotonic()

        history = (
            MarketDataBackend._history(args.market, root=args.root, raw_archive=archive)
            if capture
            else MarketDataBackend._history(args.market, root=args.root)
        )
        protection_rows = {}
        protection_connection, protection_connected, publication = 0, False, 0

        def publish_protection():
            nonlocal publication
            publication += 1
            atomic_json(
                args.protection_file,
                {
                    "version": 1,
                    "generation": args.protection_generation,
                    "market": args.market,
                    "instruments": [item.key for item in instruments],
                    "connected": protection_connected,
                    "connection": protection_connection,
                    "envelope_sequence": publication,
                    "observations": protection_rows,
                },
            )

        def protection_state(connection, connected):
            nonlocal protection_connection, protection_connected
            # Clear old connection rows BEFORE publishing its successor connection.
            protection_rows.clear()
            protection_connection, protection_connected = connection, connected
            publish_protection()

        def protection(observation):
            if observation is None:
                protection_rows.clear()
            else:
                if not protection_connected or observation.connection != protection_connection:
                    raise ProtocolError("protection callback differs from its original socket")
                protection_rows[observation.instrument.key] = observation.document()
            publish_protection()

        worker = CollectionWorker(
            StreamPlan(Market(args.market), instruments),
            history,
            collector,
            on_health=health,
            **(
                {
                    "on_protection": protection,
                    "protection_generation": args.protection_generation,
                    "on_protection_state": protection_state,
                }
                if getattr(args, "protection_file", None) is not None
                else {}
            ),
            **({"capture_rest_raw": True} if capture else {}),
        )
        await worker.run(stop)
    finally:
        store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Parent-owned public market collector")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--db-config", type=Path, required=True)
    parser.add_argument("--market", choices=["spot", "usdm"], required=True)
    parser.add_argument("--lookback-minutes", type=int, default=0)
    parser.add_argument("--start-plan", type=json.loads, default=None)
    parser.add_argument("--health-file", type=Path, default=None)
    parser.add_argument("--protection-file", type=Path, default=None)
    parser.add_argument("--protection-generation", default=None)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--capture-rest-raw", action="store_true")
    parser.add_argument("--raw-directory", type=Path)
    args = parser.parse_args(argv)
    if args.raw_directory is not None and not args.capture_rest_raw:
        parser.error("--raw-directory requires --capture-rest-raw")
    if not 0 <= args.lookback_minutes <= 20_000 or len(args.symbols) > 100:
        parser.error("collector instrument/history limit exceeded")
    if args.protection_file is not None:
        import re

        if (
            not args.protection_file.resolve().is_relative_to(
                (args.root / "logs/collection").resolve()
            )
            or args.protection_file.suffix != ".json"
            or type(args.protection_generation) is not str
            or re.fullmatch(r"[0-9a-f]{32}", args.protection_generation) is None
        ):
            parser.error("protection transport requires an owned local path/generation")
    elif args.protection_generation is not None:
        parser.error("protection generation requires its owned transport file")
    if args.health_file is not None and (
        not args.health_file.resolve().is_relative_to((args.root / "logs" / "collection").resolve())
        or args.health_file.suffix != ".json"
    ):
        parser.error("collector health file must stay in this project's collection logs")
    try:
        asyncio.run(collect(args))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        print(json.dumps({"event": "collector_failed", "error": type(exc).__name__}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
