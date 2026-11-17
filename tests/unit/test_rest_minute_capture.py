"""Synthetic public HTTP bodies and temporary archives only; no provider/network/DB."""

import json
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from urllib.parse import parse_qs, urlsplit

import pytest

from omega.domain.errors import ProtocolError, StorageError, TransportError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.client import BinanceClient, Credentials, DecodedResponse
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.rest_minute_capture import (
    MAX_BODY_BYTES,
    MAX_ROW_BYTES,
    capture_rest_page,
    exact_kline_fragments,
)
from omega.infrastructure.binance.transport import HttpRequest, HttpResponse, UrllibTransport
from omega.infrastructure.raw_archive import FileRawArchive
from omega.ports.collection import accept_rest_observations
from omega.ports.raw_data import RawArchiveReceipt

ROW = (
    b'[0, "1.0000", "2.00", "0.500", "1.00", "3e0", 59999, '
    b'"7", 2, "1", "1", "a\\u0062\\"[,]\\uac00"]'
)
BODY = b" \r\n[\t" + ROW + b"\r\n]\t\r\n"
NOW = 60_000
RECEIVED = NOW + 7
ACCEPTED = NOW + 20


class Budget:
    def __init__(self):
        self.costs = []

    def reserve(self, costs):
        self.costs.append(costs)

    def observe_usage(self, *_args):
        pass


class Transport:
    def __init__(self, bodies=(BODY,), *, received=RECEIVED):
        self.bodies, self.received, self.calls = list(bodies), received, []

    def send(self, request):
        self.calls.append(request)
        body = self.bodies[len(self.calls) - 1]
        return HttpResponse(
            200,
            body,
            {
                "Content-Type": "application/json",
                "X-MBX-USED-WEIGHT-1M": "2",
                "Date": "Mon, 02 Nov 2026 00:00:00 GMT",
                "Set-Cookie": "private-do-not-archive",
                "Authorization": "private-do-not-archive",
            },
            self.received,
        )


class Archive:
    def __init__(self, root):
        self.actual = FileRawArchive(root)
        self.records, self.receipts = [], []

    def put(self, record):
        self.records.append(record)
        receipt = self.actual.put(record)
        self.receipts.append(receipt)
        return receipt

    def get(self, _identity):
        raise AssertionError("capture must trust publication receipt, not reread every page")


def setup(tmp_path, market=Market.SPOT, *, transport=None, archive=True):
    transport = transport or Transport()
    client = BinanceClient(
        "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
        transport,
        budget=Budget(),
        clock_ms=lambda: ACCEPTED,
    )
    archive = Archive(tmp_path / "archive") if archive else None
    return BinanceMarketData(market, client, raw_archive=archive), transport, archive


def instrument(market=Market.SPOT):
    return Instrument("binance", market, "BTCUSDT")


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_actual_body_and_exact_row_spelling_are_preserved_separately_from_candles(tmp_path, market):
    history, transport, archive = setup(tmp_path, market)
    rows = history.closed_observations(instrument(market), 0, NOW, now_ms=NOW)
    assert len(rows) == len(archive.records) == len(transport.calls) == 1
    row = rows[0]
    assert row.source == row.raw_source == "rest"
    assert row.raw.encode("utf-8") == ROW
    assert "\\u0062" in row.raw and '\\"[,]' in row.raw
    assert row.raw_received_at_ms == RECEIVED and row.received_at_ms == ACCEPTED
    assert row.candle.open == Decimal("1.0000") and row.candle.volume == Decimal("3e0")
    record, receipt = archive.records[0], archive.receipts[0]
    assert record.body == BODY and receipt.body_sha256 == sha256(BODY).hexdigest()
    assert record.channel == "klines_1m" and record.received_at_ms == RECEIVED
    assert set(dict(record.headers)) == {"content-type", "date", "x-mbx-used-weight-1m"}
    assert "private" not in repr(record.headers)
    recovered = FileRawArchive(tmp_path / "archive").get(receipt.archive_id)
    assert recovered.record == record and recovered.record.body == BODY
    call = transport.calls[0]
    assert call.method == "GET" and call.body is None and call.headers == {}
    assert urlsplit(call.url).path == (
        "/api/v3/klines" if market == Market.SPOT else "/fapi/v1/klines"
    )
    assert history.client.budget.costs == [{"binance-ip": 2 if market == Market.SPOT else 10}]


def test_legacy_closed_minutes_keeps_old_request_path_even_with_an_archive(tmp_path):
    class LegacyClient:
        calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return json.loads(BODY)

        def request_with_response(self, *_args, **_kwargs):
            raise AssertionError("legacy adapter must keep its request call")

    legacy, archive = LegacyClient(), Archive(tmp_path / "archive")
    history = BinanceMarketData(Market.SPOT, legacy, raw_archive=archive)
    rows = history.closed_minutes(instrument(), 0, NOW, now_ms=NOW)
    assert len(rows) == 1 and archive.records == []
    assert legacy.calls == [
        (
            (
                "GET",
                "/api/v3/klines",
                {
                    "symbol": "BTCUSDT",
                    "interval": "1m",
                    "startTime": 0,
                    "endTime": 59999,
                    "limit": 1000,
                },
            ),
            {"weight": 2},
        )
    ]


def test_explicit_capture_requires_archive_before_any_network_read(tmp_path):
    history, transport, _archive = setup(tmp_path, archive=False)
    with pytest.raises(ValidationError, match="archive"):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert transport.calls == []
    assert len(history.closed_minutes(instrument(), 0, NOW, now_ms=NOW)) == 1


@pytest.mark.parametrize("fault", ["credentials", "trading", "origin", "budget", "scope", "prefix"])
def test_capture_rechecks_public_origin_credentials_and_original_rate_capability(tmp_path, fault):
    history, transport, archive = setup(tmp_path)
    if fault == "credentials":
        history.client.credentials = Credentials("synthetic", "synthetic")
    elif fault == "trading":
        history.client.enable_trading = True
    elif fault == "origin":
        history.client.base_url = "https://fapi.binance.com"
    elif fault == "budget":
        history.client.budget = None
    elif fault == "scope":
        history.client.ip_scope = "other"
    else:
        history.prefix = "/other"
    with pytest.raises(ValidationError):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert transport.calls == [] and archive.records == []


def test_missing_actual_transport_receipt_rejects_without_fabricating_time(tmp_path):
    history, transport, archive = setup(tmp_path, transport=Transport(received=None))
    with pytest.raises(ProtocolError, match="receipt"):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert len(transport.calls) == 1 and archive.records == []
    assert HttpResponse(200, b"[]").received_at_ms is None
    assert HttpResponse(200, b"[]", {}, RECEIVED) == HttpResponse(200, b"[]")


def test_backward_acceptance_clock_never_clamps_original_receipt(tmp_path):
    history, _transport, archive = setup(tmp_path, transport=Transport(received=ACCEPTED + 1))
    with pytest.raises(ProtocolError, match="backward"):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert archive.records[0].received_at_ms == ACCEPTED + 1
    assert archive.records[0].body == BODY


def test_public_page_query_boundaries_and_missing_rows_are_not_zero_filled(tmp_path):
    later = ROW.replace(b"[0,", b"[120000,", 1).replace(b"59999,", b"179999,", 1)
    transport = Transport((BODY, b"[" + later + b"]"), received=180007)
    history, _transport, archive = setup(tmp_path, transport=transport)
    history.client.clock_ms = lambda: 180020
    history.page_limit = 2
    rows = history.closed_observations(instrument(), 0, 180000, now_ms=180000)
    assert [row.candle.open_time_ms for row in rows] == [0, 120000]
    assert len(archive.records) == 2
    queries = [parse_qs(urlsplit(call.url).query) for call in transport.calls]
    assert [(row["startTime"], row["endTime"], row["limit"]) for row in queries] == [
        (["0"], ["119999"], ["2"]),
        (["120000"], ["179999"], ["2"]),
    ]
    assert history.client.budget.costs == [{"binance-ip": 2}, {"binance-ip": 2}]


def test_duplicate_rows_keep_original_first_fragment_and_conflicts_remain_errors(tmp_path):
    second = ROW.replace(b'"1.0000"', b'"1.0"', 1)
    history, _transport, archive = setup(
        tmp_path, transport=Transport((b"[" + ROW + b"," + second + b"]",))
    )
    rows = history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert len(rows) == 1 and rows[0].raw.encode() == ROW
    assert archive.records[0].body == b"[" + ROW + b"," + second + b"]"
    conflict = ROW.replace(b'"1.00", "3e0"', b'"1.50", "3e0"', 1)
    history, _transport, archive = setup(
        tmp_path / "conflict", transport=Transport((b"[" + ROW + b"," + conflict + b"]",))
    )
    with pytest.raises(ProtocolError, match="conflicting"):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert len(archive.records) == 1


@pytest.mark.parametrize("fault", ["exception", "wrong_hash", "wrong_receipt", "missing_receipt"])
def test_archive_failure_or_bad_receipt_halts_strict_optin_capture(tmp_path, fault):
    history, _transport, archive = setup(tmp_path)
    original = archive.put

    def put(record):
        if fault == "exception":
            raise OSError("synthetic archive unavailable")
        receipt = original(record)
        if fault == "wrong_hash":
            return replace(receipt, body_sha256="0" * 64)
        if fault == "wrong_receipt":
            return replace(receipt, received_at_ms=receipt.received_at_ms + 1)
        return None

    archive.put = put
    with pytest.raises(StorageError):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)


@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b"[null]",
        b"[[0]]",
        b"[" + ROW + b",]",
        b"[" + ROW + b"] trailing",
        b"\xff",
        b"[" + ROW + b"][]",
        b"[" + ROW.replace(b"59999", b"true") + b"]",
    ],
)
def test_exact_fragment_parser_rejects_ambiguous_or_unsupported_body(body):
    with pytest.raises(ProtocolError):
        exact_kline_fragments(body, [], limit=1000)


@pytest.mark.parametrize("replacement", [b"true", b"null", b"{}", b"[]"])
def test_invalid_time_or_nested_value_is_rejected_with_matching_decoded_row_count(replacement):
    # Keep decoded rows consistent so rejection cannot rely on an empty-row mismatch.
    body = b"[" + ROW.replace(b"59999", replacement, 1) + b"]"
    decoded = json.loads(body, parse_float=Decimal)
    assert len(decoded) == 1
    with pytest.raises(ProtocolError):
        exact_kline_fragments(body, decoded, limit=1000)


def test_full_page_and_per_minute_limits_do_not_truncate_or_reconstruct(tmp_path):
    with pytest.raises(ProtocolError):
        exact_kline_fragments(b" " * (MAX_BODY_BYTES + 1), [], limit=1000)
    oversized = ROW[:-1] + b',"' + b"x" * MAX_ROW_BYTES + b'"]'
    body = b"[" + oversized + b"]"
    history, _transport, archive = setup(tmp_path, transport=Transport((body,)))
    with pytest.raises(ProtocolError):
        history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    assert archive.records[0].body == body
    assert archive.receipts[0].body_sha256 == sha256(body).hexdigest()


def test_parser_checks_actual_body_against_decoded_values_and_row_count():
    changed = json.loads(BODY)
    changed[0][1] = "999"
    with pytest.raises(ProtocolError):
        exact_kline_fragments(BODY, changed, limit=1000)
    with pytest.raises(ProtocolError):
        exact_kline_fragments(b"[" + ROW + b"," + ROW + b"]", json.loads(BODY), limit=1)


def test_transport_receipt_is_sampled_immediately_after_actual_body_read(monkeypatch):
    calls = []

    class Response(BytesIO):
        code = 200
        headers = {"Content-Type": "application/json"}

        def read(self, size):
            calls.append("read")
            return super().read(size)

    class Opener:
        def open(self, _request, *, timeout):
            assert timeout == 8
            return Response(BODY)

    def clock():
        calls.append("receipt")
        return RECEIVED * 1_000_000

    monkeypatch.setattr("omega.infrastructure.binance.transport.time.time_ns", clock)
    transport = UrllibTransport(timeout_seconds=8)
    transport._opener = Opener()
    response = transport.send(HttpRequest("GET", "https://api.binance.com/api/v3/klines"))
    assert response.body == BODY and response.received_at_ms == RECEIVED
    assert calls == ["read", "receipt"]


def test_existing_transport_oversize_and_no_redirect_guards_remain(monkeypatch):
    from omega.infrastructure.binance.transport import _NoRedirect

    class Opener:
        def open(self, _request, *, timeout):
            result = BytesIO(BODY)
            result.code, result.headers = 200, {}
            return result

    transport = UrllibTransport(max_response_bytes=1)
    transport._opener = Opener()
    with pytest.raises(TransportError):
        transport.send(HttpRequest("GET", "https://api.binance.com/api/v3/klines"))
    assert _NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.invalid") is None


def test_archive_publication_receipt_is_used_without_new_readback_layer(tmp_path):
    history, _transport, archive = setup(tmp_path)
    reply = DecodedResponse(json.loads(BODY), HttpResponse(200, BODY, {}, RECEIVED))
    rows = capture_rest_page(
        reply,
        instrument(),
        {"symbol": "BTCUSDT", "interval": "1m", "startTime": 0, "endTime": 59999, "limit": 1000},
        0,
        NOW,
        now_ms=NOW,
        accepted_at_ms=ACCEPTED,
        archive=archive,
    )
    assert len(rows) == 1 and isinstance(archive.receipts[0], RawArchiveReceipt)


def test_runtime_acceptance_resamples_only_canonical_time_preserving_originals(tmp_path):
    history, _transport, _archive = setup(tmp_path)
    original = history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    accepted = accept_rest_observations(original, instrument(), 0, NOW, ACCEPTED + 100)
    assert accepted[0].received_at_ms == ACCEPTED + 100
    assert original[0].received_at_ms == ACCEPTED
    assert replace(accepted[0], received_at_ms=ACCEPTED) == original[0]
    assert accepted[0].raw_received_at_ms == RECEIVED and accepted[0].raw.encode() == ROW


@pytest.mark.parametrize(
    "fault",
    [
        "list",
        "source",
        "raw_source",
        "raw_missing",
        "raw_large",
        "raw_time",
        "accepted_time",
        "range",
        "clock",
        "surrogate",
    ],
)
def test_runtime_acceptance_rejects_invalid_capture_without_time_clamping(tmp_path, fault):
    history, _transport, _archive = setup(tmp_path)
    rows = history.closed_observations(instrument(), 0, NOW, now_ms=NOW)
    start, end, now = 0, NOW, ACCEPTED + 100
    if fault == "list":
        rows = list(rows)
    elif fault == "source":
        rows = (replace(rows[0], source="websocket"),)
    elif fault == "raw_source":
        rows = (replace(rows[0], raw_source=None),)
    elif fault == "raw_missing":
        rows = (replace(rows[0], raw=None),)
    elif fault == "raw_large":
        rows = (replace(rows[0], raw="x" * 65_537),)
    elif fault == "raw_time":
        rows = (replace(rows[0], raw_received_at_ms=True),)
    elif fault == "accepted_time":
        rows = (replace(rows[0], received_at_ms=RECEIVED - 1),)
    elif fault == "range":
        start, end = NOW, NOW * 2
    elif fault == "clock":
        now = ACCEPTED - 1
    else:
        rows = (replace(rows[0], raw="\ud800"),)
    with pytest.raises(ProtocolError):
        accept_rest_observations(rows, instrument(), start, end, now)
