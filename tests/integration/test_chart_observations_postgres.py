"""Opt-in stored auxiliary/candle fixtures through the authenticated core route.

These are synthetic observations in an isolated real PostgreSQL repository;
they do not claim exchange collection, socket HTTP or native GUI execution.
"""

import os
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.application.auxiliary_series import KINDS, UNITS
from omega.application.paper import candle_row
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_observations import ChartObservationsView
from omega.runtime.service import CoreService

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000 // 3_600_000 * 3_600_000
END = START + 15 * MINUTE
RECEIVED = END + 1000


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    config = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not config:
        pytest.fail("OMEGA_TEST_DB_CONFIG must select the isolated Omega test database")
    value = load_database_settings(Path(config).resolve())
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing chart DB tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        migrate(connection)
    return value


@pytest.fixture
def instrument(settings):
    value = Instrument("binance", Market.USDM, "TEST_AUXCHART_" + uuid4().hex[:20].upper())
    yield value
    with connect_database(settings) as connection:
        connection.execute("DELETE FROM market.auxiliary_minutes WHERE instrument=%s", (value.key,))
        connection.execute("DELETE FROM market.candles_1m WHERE instrument=%s", (value.key,))


@pytest.fixture
def store(settings):
    value = PostgresAuxiliaryStore(settings)
    try:
        yield value
    finally:
        value.close()


def sample(instrument, kind="OPENINTEREST", minute=0, *, values=None, suffix=None, **changes):
    if values is None:
        values = {
            name: 2 if unit == "count" else Decimal(".25") for name, unit in UNITS[kind].items()
        }
        if kind == "DEPTH":
            values = {
                f"DEPTH_{side}{index}": Decimal(index + 1)
                for side in ("BIDS", "ASKS")
                for index in range(6)
            }
    opening = START + minute * MINUTE
    return AuxiliaryInput(
        **{
            "channel": KINDS[kind][0],
            "values": tuple(values.items()),
            "window_start_ms": opening,
            "window_end_ms": opening + MINUTE,
            "event_time_ms": opening + (MINUTE if kind == "AGGTRADE" else 10),
            "available_at_ms": RECEIVED,
            "source": instrument.key + ":" + (suffix or KINDS[kind][1]),
            **changes,
        }
    )


def parameters(instrument, kind="OPENINTEREST", **changes):
    return {
        "instrument": instrument.key,
        "kind": kind,
        "start_ms": START,
        "end_ms": START + 5 * MINUTE,
        "period_minutes": 1,
        **changes,
    }


def request(core, instrument, kind="OPENINTEREST", **changes):
    return core.request(
        "GET",
        "/api/chart-observations?" + urlencode(parameters(instrument, kind, **changes)),
        core.token,
    )


def create_core(root, settings, *, now=RECEIVED, candle_instrument=None):
    opened = []

    def factory():
        value = PostgresAuxiliaryStore(settings)
        opened.append(value)
        return value

    clock = [now]
    view = ChartObservationsView(root, factory, lambda: clock[0])

    def candles(key, limit):
        assert key == candle_instrument.key
        reader = PostgresCandleStore(settings)
        try:
            return {
                "instrument": key,
                "source": "database",
                "candles": [candle_row(value) for value in reader.latest(candle_instrument, limit)],
                "quality": {"status": "observed"},
            }
        finally:
            reader.close()

    core = CoreService(
        root, chart_observations=view, candles_provider=candles if candle_instrument else None
    )
    assert not opened
    return core, clock, opened


@pytest.mark.parametrize("kind", tuple(KINDS))
def test_actual_saved_observation_and_candle_fixture_reach_owner_api_with_units_lineage_and_gaps(
    tmp_path, settings, instrument, store, kind
):
    value = sample(instrument, kind)
    receipt = store.put(instrument, value)
    candle = Candle(
        instrument,
        START,
        START + MINUTE,
        Decimal(100),
        Decimal(105),
        Decimal(95),
        Decimal(101),
        Decimal("3.25"),
    )
    writer = PostgresCandleStore(settings)
    try:
        writer.put(MinuteObservation(candle, "rest", RECEIVED, raw='{"fixture":"aux-chart"}'))
    finally:
        writer.close()
    core, _, opened = create_core(tmp_path, settings, candle_instrument=instrument)
    try:
        assert core.state()["core"]["capabilities"]["chart_observations"]
        result = request(core, instrument, kind)
        assert result["schema_version"] == 1 and result["scope"] == "retrospective_auxiliary_chart"
        assert result["core_epoch"] == core.control.epoch
        assert result["state"] == "partial"
        assert result["as_of_ms"] == RECEIVED
        assert result["quality"] == {
            "expected_bars": 5,
            "observed_bars": 1,
            "missing_bars": 4,
            "partial_bars": 0,
        }
        assert result["gaps"] == [[START + MINUTE, START + 5 * MINUTE]]
        assert result["units"] == UNITS[kind]
        assert result["lineage"] == [
            {
                "open_time_ms": START,
                "available_at_ms": RECEIVED,
                "revision": 1,
                "checksum": receipt.checksum,
                "quality": "observed",
                "coverage": "1",
            }
        ]
        expected = "21" if kind == "DEPTH" else "2" if kind == "AGGTRADE" else "0.25"
        field = (
            "DEPTH_BIDS"
            if kind == "DEPTH"
            else "AGGTRADE_NTRADESBUY"
            if kind == "AGGTRADE"
            else next(iter(UNITS[kind]))
        )
        assert result["series"][field] == [[START, expected]]
        saved_candles = core.request(
            "GET",
            "/api/candles?" + urlencode({"instrument": instrument.key, "limit": 5}),
            core.token,
        )
        assert saved_candles["source"] == "database"
        assert saved_candles["candles"] == [candle_row(candle)]
        assert opened and all(value.connection.closed for value in opened)
    finally:
        core.close()


def test_actual_revision_cutoff_preserves_prior_hash_and_original_availability(
    tmp_path, settings, instrument, store
):
    original = sample(instrument)
    first = store.put(instrument, original)
    repaired = replace(
        original,
        values=(("METRIC_OPENINTEREST", Decimal("999.125")),),
        available_at_ms=RECEIVED + 5000,
    )
    second = store.put(instrument, repaired, expected_previous=first.checksum)
    core, clock, _ = create_core(tmp_path, settings, now=RECEIVED - 1)
    try:
        absent = request(core, instrument)
        assert absent["state"] == "unavailable" and absent["lineage"] == []
        clock[0] = RECEIVED
        prior = request(core, instrument)
        assert prior["lineage"][0]["checksum"] == first.checksum
        assert prior["lineage"][0]["revision"] == 1
        assert prior["lineage"][0]["available_at_ms"] == RECEIVED
        assert prior["series"]["METRIC_OPENINTEREST"] == [[START, "0.25"]]
        clock[0] = repaired.available_at_ms
        latest = request(core, instrument)
        assert latest["lineage"][0]["checksum"] == second.checksum
        assert latest["lineage"][0]["revision"] == 2
        assert latest["lineage"][0]["available_at_ms"] == repaired.available_at_ms
        assert latest["series"]["METRIC_OPENINTEREST"] == [[START, "999.125"]]
        assert prior["lineage"][0]["revision"] == 1
    finally:
        core.close()


def test_actual_oi_history_preference_current_fallback_and_explicit_source_are_separate(
    tmp_path, settings, instrument, store
):
    current = sample(instrument, suffix="open_interest", values={"METRIC_OPENINTEREST": Decimal(3)})
    store.put(instrument, current)
    core, _, _ = create_core(tmp_path, settings)
    try:
        fallback = request(core, instrument)
        assert fallback["source"] == current.source
        assert fallback["series"]["METRIC_OPENINTEREST"] == [[START, "3"]]
        history = sample(
            instrument,
            minute=2,
            values={"METRIC_OPENINTEREST": Decimal(7), "METRIC_OPENINTERESTVALUE": Decimal(700)},
        )
        store.put(instrument, history)
        preferred = request(core, instrument)
        assert preferred["source"] == history.source
        assert preferred["series"]["METRIC_OPENINTEREST"] == [[START + 2 * MINUTE, "7"]]
        assert preferred["series"]["METRIC_OPENINTERESTVALUE"] == [[START + 2 * MINUTE, "700"]]
        explicit = request(core, instrument, source=current.source)
        assert explicit["source"] == current.source and explicit["series"] == fallback["series"]
    finally:
        core.close()


def test_actual_agg_partial_minute_blocks_sum_until_later_revision_is_known(
    tmp_path, settings, instrument, store
):
    receipts, values = [], []
    for minute in range(3):
        value = sample(
            instrument,
            "AGGTRADE",
            minute,
            quality="partial" if minute == 1 else "observed",
            coverage=Decimal(".5") if minute == 1 else Decimal(1),
        )
        receipts.append(store.put(instrument, value))
        values.append(value)
    repaired = replace(
        values[1], quality="observed", coverage=Decimal(1), available_at_ms=RECEIVED + 5000
    )
    repaired_receipt = store.put(instrument, repaired, expected_previous=receipts[1].checksum)
    core, clock, _ = create_core(tmp_path, settings)
    try:
        options = {"period_minutes": 3, "end_ms": START + 3 * MINUTE}
        prior = request(core, instrument, "AGGTRADE", **options)
        assert prior["series"]["AGGTRADE_NOTIONALBUY"] == []
        assert prior["quality"] == {
            "expected_bars": 1,
            "observed_bars": 0,
            "missing_bars": 1,
            "partial_bars": 1,
        }
        assert prior["gaps"] == [[START, START + 3 * MINUTE]]
        clock[0] = repaired.available_at_ms
        result = request(core, instrument, "AGGTRADE", **options)
        assert result["state"] == "ready" and result["gaps"] == []
        assert result["series"]["AGGTRADE_NOTIONALBUY"] == [[START, "0.75"]]
        assert result["series"]["AGGTRADE_NTRADESSELL"] == [[START, "6"]]
        assert result["lineage"][1]["checksum"] == repaired_receipt.checksum
    finally:
        core.close()


def test_actual_depth_uses_last_complete_book_and_partial_edges_remain_gaps(
    tmp_path, settings, instrument, store
):
    first = sample(instrument, "DEPTH")
    second = sample(
        instrument,
        "DEPTH",
        2,
        values={
            f"DEPTH_{side}{index}": Decimal(10) for side in ("BIDS", "ASKS") for index in range(6)
        },
    )
    last = sample(instrument, "DEPTH", 4, quality="partial", coverage=Decimal(".5"))
    for value in (first, second, last):
        store.put(instrument, value)
    core, _, _ = create_core(tmp_path, settings)
    try:
        result = request(core, instrument, "DEPTH", period_minutes=5)
        assert result["series"] == {"DEPTH_BIDS": [[START, "60"]], "DEPTH_ASKS": [[START, "60"]]}
        assert result["quality"]["partial_bars"] == 1
        edge = request(core, instrument, "DEPTH", start_ms=START + MINUTE, period_minutes=5)
        assert edge["series"] == {"DEPTH_BIDS": [], "DEPTH_ASKS": []}
        assert edge["gaps"] == [[START + MINUTE, START + 5 * MINUTE]]
        assert len(edge["lineage"]) == 2
    finally:
        core.close()


def test_owner_auth_query_guards_and_unsupported_capabilities_precede_database_reads(
    tmp_path, settings, instrument
):
    core, _, opened = create_core(tmp_path, settings)
    target = "/api/chart-observations?" + urlencode(parameters(instrument))
    try:
        with pytest.raises(PermissionError):
            core.request("GET", target, "invalid")
        pairing = core.control.create_pairing("aux-chart-phone")
        device = core.request("POST", "/api/pair", "", {"code": pairing["code"]})
        with pytest.raises(PermissionError, match="owner"):
            core.request("GET", target, device["token"])
        for suffix in (
            "&extra=1",
            "&kind=DEPTH",
            "&period_minutes=1.0",
            "&source=",
            "&source=a&source=b",
        ):
            with pytest.raises(ValidationError):
                core.request("GET", target + suffix, core.token)
        result = request(core, instrument, period_minutes=10080)
        assert result["state"] == "unsupported"
        assert not opened
    finally:
        core.close()


def test_actual_corrupt_stored_checksum_is_unavailable_without_publishing_points(
    tmp_path, settings, instrument, store
):
    store.put(instrument, sample(instrument))
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET checksum=%s WHERE instrument=%s",
            ("f" * 64, instrument.key),
        )
    core, _, opened = create_core(tmp_path, settings)
    try:
        result = request(core, instrument)
        assert result["state"] == "unavailable"
        assert result["lineage"] == []
        assert all(not points for points in result["series"].values())
        assert all(value.connection.closed for value in opened)
    finally:
        core.close()
