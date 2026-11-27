"""Verified daily Binance Vision files with original ZIPs retained on disk."""

import csv
import hashlib
import io
import json
import math
import os
import re
import stat
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, build_opener

from omega.domain.errors import (
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.budget import Window
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import HttpRequest, UrllibTransport, _NoRedirect

BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"
DAY = 86_400_000
MAX_INDEX_BYTES = 2_097_152
MAX_ZIP_BYTES = 2_147_483_648
MAX_CSV_BYTES = 8_589_934_592
MAX_LINE_CHARS = 65_536
MAX_DAILY_ROWS = 50_000_000
MAX_FILES = 20_000
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
)
TRADE_COLUMNS = (
    "aggregate_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "timestamp",
    "is_buyer_maker",
)
DEPTH_COLUMNS = ("timestamp", "percentage", "depth", "notional")
METRIC_COLUMNS = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)


@dataclass(frozen=True, slots=True)
class VisionFile:
    object_key: str
    day_start_ms: int
    size: int


@dataclass(frozen=True, slots=True)
class VisionReceipt:
    object_key: str
    zip_sha256: str
    checksum_sha256: str
    received_at_ms: int
    archive_path: Path


@dataclass(frozen=True, slots=True)
class VisionRow:
    instrument: Instrument
    channel: str
    timestamp_ms: int
    values: tuple[str, ...]
    columns: tuple[str, ...]
    row_number: int
    receipt: VisionReceipt


def _prefix(instrument, channel):
    if (
        not isinstance(instrument, Instrument)
        or instrument.provider != "binance"
        or instrument.market not in {Market.SPOT, Market.USDM}
        or channel not in {"kline", "agg_trade", "metrics", "depth"}
        or channel == "metrics"
        and instrument.market != Market.USDM
    ):
        raise ValidationError("unsupported Binance Vision instrument or channel")
    market = "spot" if instrument.market == Market.SPOT else "futures/um"
    kind = {
        "kline": "klines",
        "agg_trade": "aggTrades",
        "metrics": "metrics",
        "depth": "bookDepth",
    }[channel]
    suffix = "/1m/" if channel == "kline" else "/"
    return f"data/{market}/daily/{kind}/{instrument.symbol}{suffix}"


def _name(instrument, channel):
    return (
        instrument.symbol
        + "-"
        + {"kline": "1m", "agg_trade": "aggTrades", "metrics": "metrics", "depth": "bookDepth"}[
            channel
        ]
    )


def _stamp(value):
    return type(value) is int and 0 <= value <= 253_402_300_799_000


def _digest_file(path, check_stop):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1_048_576):
            check_stop()
            digest.update(chunk)
    return digest.hexdigest()


class _OriginBudget(SharedRequestBudget):
    def _load(self):
        now = super()._load()
        # A rollback must not reopen admission even after an earlier deadline expired.
        if now < self._active_now:
            raise RateLimited(self._active_now - now)
        return now


class VisionArchive:
    def __init__(
        self,
        root,
        *,
        transport=None,
        clock_ms=None,
        stop_requested=lambda: False,
        cache_ttl_seconds=300,
        paced=False,
    ):
        if (
            type(paced) is not bool
            or type(cache_ttl_seconds) not in {int, float}
            or not math.isfinite(cache_ttl_seconds)
            or not 0 <= cache_ttl_seconds <= 86400
        ):
            raise ValidationError("invalid Vision metadata cache TTL")
        self.root = Path(root).absolute()
        self._guard(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.transport = transport or UrllibTransport(max_response_bytes=MAX_INDEX_BYTES)
        self._injected_transport = transport is not None
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.stop_requested = stop_requested
        self._opener = build_opener(_NoRedirect())
        origin = BASE_URL.split("/data.binance.vision/")[0]
        self._origin, self.paced = origin, paced
        budget_path = (
            self.root / "admission" / (hashlib.sha256(origin.encode()).hexdigest() + ".json")
        )
        self._guard(budget_path)
        self._budget = _OriginBudget(
            budget_path, (Window(origin, 1, 1),), clock=lambda: self.clock_ms() / 1000
        )
        self._admission_fault = False
        self.cache_ttl_seconds = float(cache_ttl_seconds)
        self._file_cache, self._first_cache, self._object_hashes = {}, {}, {}

    def _guard(self, path):
        if not path.is_relative_to(self.root):
            raise StorageError("Vision archive path escapes its root")
        current = path
        while current.is_relative_to(self.root):
            if current.is_symlink() or current.is_junction():
                raise StorageError("Vision archive cannot use links or junctions")
            if current == self.root:
                break
            current = current.parent

    def _check_stop(self):
        if self.stop_requested():
            raise InterruptedError("Vision archive operation cancelled")

    def _admit(self):
        self._check_stop()
        if self._admission_fault:
            raise StorageError("Vision admission recording failed; requests blocked")
        self._guard(self._budget.path)
        self._guard(self._budget.lock_path)
        while True:
            try:
                self._budget.reserve({self._origin: 1} if self.paced else {})
                return
            except RateLimited as error:
                if not self.paced:
                    raise
                deadline = time.monotonic() + max(0.05, error.retry_after or 1)
                while (remaining := deadline - time.monotonic()) > 0:
                    self._check_stop()
                    time.sleep(min(1, remaining))
            except StorageError:
                self._admission_fault = True
                raise

    def _status(self, status, headers):
        if status in {418, 429}:
            try:
                delay = float(
                    next((v for k, v in headers.items() if k.lower() == "retry-after"), "60")
                )
            except (ValueError, TypeError):
                delay = 60.0
            if not math.isfinite(delay) or delay < 0:
                delay = 60.0
            try:
                self._budget.cooldown(delay)
            except StorageError:
                self._admission_fault = True
                raise
            raise RateLimited(delay)
        if status != 200:
            raise TransportError("Binance Vision public request failed")

    def _small(self, url, limit):
        self._admit()
        result = self.transport.send(HttpRequest("GET", url, {"Accept": "*/*"}))
        self._status(result.status, result.headers)
        if type(result.body) is not bytes or not 0 < len(result.body) <= limit:
            raise ProtocolError("Vision public response exceeds its bound")
        return result.body

    def files(self, instrument, channel, start_ms=None, end_ms=None, first_only=False):
        prefix = _prefix(instrument, channel)
        if (
            start_ms is not None
            and not _stamp(start_ms)
            or end_ms is not None
            and not _stamp(end_ms)
            or start_ms is not None
            and end_ms is not None
            and start_ms >= end_ms
            or type(first_only) is not bool
        ):
            raise ValidationError("invalid Vision file range")
        self._check_stop()
        cache_key = (
            prefix,
            None if start_ms is None else start_ms // DAY,
            None if end_ms is None else (end_ms - 1) // DAY,
            first_only,
        )
        cached = self._file_cache.get(cache_key)
        if cached is not None and time.monotonic() < cached[0]:
            return cached[1]
        expression = re.compile(
            re.escape(prefix + _name(instrument, channel)) + r"-(\d{4}-\d{2}-\d{2})\.zip"
        )
        marker = ""
        if start_ms is not None:
            day = datetime.fromtimestamp(start_ms / 1000, UTC).strftime("%Y-%m-%d")
            marker = prefix + _name(instrument, channel) + "-" + day
        entries, checksums = {}, set()
        for _page in range(128):
            query = urlencode({"prefix": prefix, "marker": marker, "max-keys": 1000})
            raw = self._small(BASE_URL + "?" + query, MAX_INDEX_BYTES)
            try:
                tree = ET.fromstring(raw)
                contents = tree.findall("s3:Contents", NS)
                if len(contents) > 1000:
                    raise ValueError("S3 page exceeds requested key limit")
                keys = []
                reached_end = False
                for item in contents:
                    key = item.findtext("s3:Key", None, NS)
                    size = int(item.findtext("s3:Size", "", NS))
                    if not isinstance(key, str) or not key.startswith(prefix) or key <= marker:
                        raise ValueError("invalid or nonadvancing object key")
                    if keys and key <= keys[-1]:
                        raise ValueError("S3 keys are not strictly ordered")
                    keys.append(key)
                    base_key = key.removesuffix(".CHECKSUM")
                    match = expression.fullmatch(base_key)
                    if match is None:
                        continue
                    day_ms = (
                        int(datetime.strptime(match[1], "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
                        * 1000
                    )
                    if end_ms is not None and day_ms >= end_ms:
                        reached_end = True
                        break
                    if start_ms is not None and day_ms + DAY <= start_ms:
                        continue
                    if key.endswith(".CHECKSUM"):
                        if not 0 < size <= 4096:
                            raise ValueError("invalid checksum size")
                        checksums.add(base_key)
                    else:
                        if not 0 < size <= MAX_ZIP_BYTES:
                            raise ValueError("invalid ZIP size")
                        descriptor = VisionFile(key, day_ms, size)
                        if key in entries and entries[key] != descriptor:
                            raise ValueError("conflicting object descriptors")
                        entries[key] = descriptor
                    if len(entries) > MAX_FILES:
                        raise ValueError("Vision file list exceeds limit")
                    if first_only and entries.keys() & checksums:
                        reached_end = True
                        break
                truncated = tree.findtext("s3:IsTruncated", "false", NS)
                if truncated not in {"true", "false"}:
                    raise ValueError("invalid S3 truncation field")
                if reached_end or truncated == "false":
                    break
                next_marker = tree.findtext("s3:NextMarker", "", NS) or (keys[-1] if keys else "")
                if not next_marker.startswith(prefix) or next_marker <= marker:
                    raise ValueError("nonadvancing S3 marker")
                marker = next_marker
            except (ValueError, TypeError, ET.ParseError, OverflowError):
                raise ProtocolError("invalid bounded Vision file index") from None
        else:
            raise ProtocolError("Vision index page limit reached")
        paired = tuple(entries[key] for key in sorted(entries.keys() & checksums))
        result = paired[:1] if first_only else paired
        if len(self._file_cache) >= 512:
            self._file_cache.pop(next(iter(self._file_cache)))
        self._file_cache[cache_key] = (time.monotonic() + self.cache_ttl_seconds, result)
        return result

    def _publish_bytes(self, path, payload):
        self._guard(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent, prefix=".vision-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise StorageError("existing Vision archive metadata conflicts") from None
        except OSError:
            raise StorageError("Vision archive metadata publication failed") from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @staticmethod
    def _response_chunks(response):
        while True:
            try:
                chunk = response.read(1_048_576)
            except (OSError, TimeoutError, HTTPException):
                raise TransportError("Vision ZIP response failed") from None
            if not chunk:
                return
            yield chunk

    def _download(self, descriptor, destination):
        self._guard(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent, prefix=".vision-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                self._admit()
                if self._injected_transport:
                    result = self.transport.send(
                        HttpRequest("GET", BASE_URL + quote(descriptor.object_key, safe="/"))
                    )
                    self._status(result.status, result.headers)
                    chunks = (result.body,)
                    response = None
                else:
                    try:
                        response = self._opener.open(
                            Request(BASE_URL + quote(descriptor.object_key, safe="/")), timeout=30
                        )
                    except HTTPError as error:
                        self._status(error.code, dict(error.headers.items()))
                        raise TransportError("Vision ZIP request failed") from None
                    except (URLError, OSError, TimeoutError):
                        raise TransportError("Vision ZIP request failed") from None
                    self._status(response.status, dict(response.headers.items()))
                    chunks = self._response_chunks(response)
                digest, total = hashlib.sha256(), 0
                try:
                    for chunk in chunks:
                        self._check_stop()
                        total += len(chunk)
                        if total > MAX_ZIP_BYTES or total > descriptor.size:
                            raise ProtocolError("Vision ZIP exceeds declared size")
                        stream.write(chunk)
                        digest.update(chunk)
                except (URLError, TimeoutError):
                    raise TransportError("Vision ZIP response failed") from None
                finally:
                    if response is not None:
                        response.close()
                if total != descriptor.size or digest.hexdigest() != destination.stem:
                    raise ProtocolError("Vision ZIP checksum or size mismatch")
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                if _digest_file(destination, self._check_stop) != destination.stem:
                    raise StorageError("existing Vision ZIP conflicts") from None
        except InterruptedError:
            raise
        except (URLError, TimeoutError):
            raise TransportError("Vision ZIP response failed") from None
        except OSError:
            raise StorageError("Vision ZIP publication failed") from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _archive(self, instrument, channel, descriptor):
        checksum = self._small(
            BASE_URL + quote(descriptor.object_key, safe="/") + ".CHECKSUM", 4096
        )
        try:
            tokens = checksum.decode("ascii").strip().split()
            expected, filename = tokens
            if (
                not re.fullmatch("[a-fA-F0-9]{64}", expected)
                or filename.lstrip("*") != PurePosixPath(descriptor.object_key).name
            ):
                raise ValueError
        except (ValueError, UnicodeError):
            raise ProtocolError("invalid Vision CHECKSUM") from None
        expected = expected.lower()
        previous = self._object_hashes.get(descriptor.object_key)
        if previous is not None and previous != expected:
            self._file_cache.clear()
            self._first_cache.clear()
            candidates = self.files(
                instrument, channel, descriptor.day_start_ms, descriptor.day_start_ms + DAY
            )
            refreshed = next(
                (item for item in candidates if item.object_key == descriptor.object_key), None
            )
            if refreshed is None:
                raise ProtocolError("changed Vision object disappeared from its index")
            descriptor = refreshed
        checksum_hash = hashlib.sha256(checksum).hexdigest()
        path = self.root / "objects" / expected[:2] / (expected + ".zip")
        self._guard(path)
        if path.exists():
            if (
                path.stat().st_size != descriptor.size
                or _digest_file(path, self._check_stop) != expected
            ):
                raise StorageError("retained Vision ZIP failed verification")
        else:
            self._download(descriptor, path)
        self._publish_bytes(self.root / "checksums" / (checksum_hash + ".CHECKSUM"), checksum)
        identity = hashlib.sha256(
            (descriptor.object_key + "\n" + expected + "\n" + checksum_hash).encode()
        ).hexdigest()
        receipt_path = self.root / "receipts" / (identity + ".json")
        self._guard(receipt_path)
        if not receipt_path.exists():
            now = self.clock_ms()
            if not _stamp(now):
                raise ValidationError("invalid Vision reception clock")
            document = {
                "object_key": descriptor.object_key,
                "zip_sha256": expected,
                "checksum_sha256": checksum_hash,
                "received_at_ms": now,
            }
            payload = json.dumps(document, sort_keys=True).encode()
            try:
                self._publish_bytes(receipt_path, payload)
            except StorageError:
                if not receipt_path.exists():
                    raise
        try:
            if receipt_path.stat().st_size > 8192:
                raise ValueError
            document = json.loads(receipt_path.read_bytes())
            if (
                set(document) != {"object_key", "zip_sha256", "checksum_sha256", "received_at_ms"}
                or document["object_key"] != descriptor.object_key
                or document["zip_sha256"] != expected
                or document["checksum_sha256"] != checksum_hash
                or not _stamp(document["received_at_ms"])
            ):
                raise ValueError
        except (OSError, ValueError, TypeError, KeyError):
            raise StorageError("retained Vision receipt failed validation") from None
        if len(self._object_hashes) >= 512:
            self._object_hashes.pop(next(iter(self._object_hashes)))
        self._object_hashes[descriptor.object_key] = expected
        return VisionReceipt(**document, archive_path=path)

    @staticmethod
    def _timestamp(instrument, channel, values, day_start):
        try:
            if channel in {"metrics", "depth"}:
                value = datetime.strptime(values[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
                timestamp = int(value.timestamp()) * 1000
            else:
                raw = values[0 if channel == "kline" else 5]
                if not raw.isdigit():
                    raise ValueError
                timestamp = int(raw)
                if instrument.market == Market.SPOT and day_start >= 1_735_689_600_000:
                    timestamp //= 1000
            if not day_start <= timestamp < day_start + DAY:
                raise ValueError
            if channel in {"kline", "metrics"} and timestamp % (
                60_000 if channel == "kline" else 300_000
            ):
                raise ValueError
            if channel == "metrics" and values[1] != instrument.symbol:
                raise ValueError
            if channel == "agg_trade":
                if not all(values[index].isdigit() for index in (0, 3, 4)) or int(values[3]) > int(
                    values[4]
                ):
                    raise ValueError
                if values[6].lower() not in {"true", "false", "1", "0"}:
                    raise ValueError
            return timestamp
        except (ValueError, TypeError, OverflowError, IndexError):
            raise ProtocolError("invalid Vision CSV identity or timestamp") from None

    def _file_rows(self, instrument, channel, descriptor):
        receipt = self._archive(instrument, channel, descriptor)
        try:
            with zipfile.ZipFile(receipt.archive_path) as archive:
                members = archive.infolist()
                expected_name = (
                    PurePosixPath(descriptor.object_key).name.removesuffix(".zip") + ".csv"
                )
                if len(members) != 1:
                    raise ProtocolError("Vision ZIP must contain exactly one CSV")
                member = members[0]
                if (
                    member.filename != expected_name
                    or member.is_dir()
                    or stat.S_ISLNK(member.external_attr >> 16)
                    or member.flag_bits & 1
                    or not 0 < member.file_size <= MAX_CSV_BYTES
                ):
                    raise ProtocolError("invalid bounded Vision ZIP member")
                with archive.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")

                    def lines():
                        while line := text.readline(MAX_LINE_CHARS + 1):
                            self._check_stop()
                            if len(line) > MAX_LINE_CHARS:
                                raise ProtocolError("Vision CSV line exceeds limit")
                            yield line

                    reader = csv.reader(lines(), strict=True)
                    columns = None
                    previous, previous_values, previous_id = -1, None, -1
                    metrics = {}
                    for number, fields in enumerate(reader, start=1):
                        self._check_stop()
                        if number > MAX_DAILY_ROWS:
                            raise ProtocolError("Vision daily row limit exceeded")
                        values = tuple(fields)
                        if not values:
                            continue
                        if columns is None and re.sub("[^a-z]", "", values[0].lower()) in {
                            "opentime",
                            "aggregatetradeid",
                            "aggtradeid",
                            "createtime",
                        } | ({"timestamp"} if channel == "depth" else set()):
                            columns = values
                            continue
                        allowed = (
                            {12}
                            if channel == "kline"
                            else {4}
                            if channel == "depth"
                            else {8}
                            if channel == "metrics" or instrument.market == Market.SPOT
                            else {7, 8}
                        )
                        if len(values) not in allowed:
                            raise ProtocolError("unexpected Vision CSV column count")
                        if columns is None:
                            columns = (
                                KLINE_COLUMNS
                                if channel == "kline"
                                else METRIC_COLUMNS
                                if channel == "metrics"
                                else DEPTH_COLUMNS
                                if channel == "depth"
                                else TRADE_COLUMNS
                                + (
                                    ("is_best_match",)
                                    if instrument.market == Market.SPOT
                                    else ("normal_quantity",)
                                    if len(values) == 8
                                    else ()
                                )
                            )
                        if len(columns) != len(values):
                            raise ProtocolError("Vision header differs from its row")
                        stamp = self._timestamp(
                            instrument, channel, values, descriptor.day_start_ms
                        )
                        row = VisionRow(
                            instrument, channel, stamp, values, columns, number, receipt
                        )
                        if channel == "metrics":
                            old = metrics.get(stamp)
                            if old is not None and old.values != values:
                                raise ProtocolError("conflicting Vision metric sample")
                            metrics.setdefault(stamp, row)
                            if len(metrics) > 288:
                                raise ProtocolError("Vision metric day exceeds native samples")
                            continue
                        if channel == "depth":
                            if stamp < previous:
                                raise ProtocolError("Vision depth timestamp moved backward")
                            previous = stamp
                            yield row
                            continue
                        identity = int(values[0]) if channel == "agg_trade" else stamp
                        if stamp < previous or identity < previous_id:
                            raise ProtocolError("Vision CSV order moved backward")
                        if identity == previous_id:
                            if values != previous_values:
                                raise ProtocolError("conflicting duplicate Vision row")
                            continue
                        previous, previous_id, previous_values = stamp, identity, values
                        yield row
                    if channel == "metrics":
                        yield from (metrics[key] for key in sorted(metrics))
        except InterruptedError:
            raise
        except (zipfile.BadZipFile, UnicodeError, csv.Error, RuntimeError, OSError):
            raise ProtocolError("invalid or damaged Vision CSV archive") from None

    def first_timestamp(self, instrument, channel):
        prefix = _prefix(instrument, channel)
        self._check_stop()
        cached = self._first_cache.get(prefix)
        if cached is not None and time.monotonic() < cached[0]:
            return cached[1]
        result = self._find_first_timestamp(instrument, channel)
        if len(self._first_cache) >= 512:
            self._first_cache.pop(next(iter(self._first_cache)))
        self._first_cache[prefix] = (time.monotonic() + self.cache_ttl_seconds, result)
        return result

    def _find_first_timestamp(self, instrument, channel):
        row = self.first_row(instrument, channel)
        return None if row is None else row.timestamp_ms

    def first_row(self, instrument, channel, *, usable=lambda row: True):
        first = self.files(instrument, channel, first_only=True)
        if not first:
            return None
        row = next(
            (row for row in self._file_rows(instrument, channel, first[0]) if usable(row)), None
        )
        if row is not None:
            return row
        for descriptor in self.files(instrument, channel):
            if descriptor.object_key == first[0].object_key:
                continue
            row = next(
                (row for row in self._file_rows(instrument, channel, descriptor) if usable(row)),
                None,
            )
            if row is not None:
                return row
        return None

    def iter_rows(self, instrument, channel, start_ms, end_ms, *, adjacent=False):
        if not _stamp(start_ms) or not _stamp(end_ms) or start_ms >= end_ms:
            raise ValidationError("invalid Vision row range")
        if type(adjacent) is not bool:
            raise ValidationError("invalid Vision adjacency choice")
        lower = max(0, start_ms // DAY * DAY - DAY) if adjacent else start_ms
        upper = (
            min(253_402_300_799_000, (end_ms - 1) // DAY * DAY + 2 * DAY) if adjacent else end_ms
        )
        for descriptor in self.files(instrument, channel, lower, upper):
            for row in self._file_rows(instrument, channel, descriptor):
                if lower <= row.timestamp_ms < upper:
                    yield row
