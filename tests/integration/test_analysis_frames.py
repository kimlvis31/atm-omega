"""Committed frame source contracts and opt-in isolated real revision selection."""

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure import analysis_frames as module
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import (
    DatabaseSettings,
    database_destination_id,
    load_database_settings,
)
from omega.ports.collection import MinuteObservation

START = 1_700_000_040_000
END = START + 180000
RECEIVED = END + 100000


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    settings = load_database_settings(Path(path).resolve())
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
    return settings


@pytest.fixture
def case(settings, tmp_path):
    (tmp_path / "database.config.toml").write_bytes(
        Path(os.environ["OMEGA_TEST_DB_CONFIG"]).read_bytes()
    )
    instrument = Instrument("binance", Market.USDM, "TEST_AFRAME_" + uuid4().hex[:24].upper())
    candles, auxiliary = PostgresCandleStore(settings), PostgresAuxiliaryStore(settings)
    source = module.CommittedAnalysisFrameSource(tmp_path)
    bars = tuple(
        Candle(
            instrument,
            START + i * 60000,
            START + (i + 1) * 60000,
            *(Decimal(v) for v in (100, max(102, 101 + i), 99, 101 + i, 2)),
        )
        for i in range(3)
    )
    # Final high must contain the last close as well.
    bars = tuple(replace(bar, high=max(bar.high, bar.close)) for bar in bars)
    for bar in bars:
        candles.put(MinuteObservation(bar, "rest", RECEIVED))
    value = SimpleNamespace(
        root=tmp_path,
        instrument=instrument,
        source=source,
        candles=candles,
        auxiliary=auxiliary,
        bars=bars,
        settings=settings,
    )
    yield value
    try:
        source.close()
    finally:
        auxiliary.close()
        candles.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
            connection.execute(
                "DELETE FROM market.auxiliary_minutes WHERE instrument=%s AND dataset=ANY(%s)",
                (
                    instrument.key,
                    [
                        instrument.key + suffix
                        for suffix in (
                            ":aggTrade",
                            ":open_interest_history",
                            ":long_short_ratio",
                            ":binance_depth_ws_v1",
                        )
                    ],
                ),
            )


@pytest.mark.database
def test_actual_depth_revision_pin_survives_late_db_correction(case):
    from omega.analysis.order_flow import DEPTH_FIELDS
    from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy
    from omega.runtime.analysis_replay import replay

    bar = case.bars[0]
    native = AuxiliaryInput(
        "depth",
        tuple((name, Decimal(100)) for name in DEPTH_FIELDS),
        bar.open_time_ms,
        bar.close_time_ms,
        bar.close_time_ms - 1,
        RECEIVED,
        source=case.instrument.key + ":binance_depth_ws_v1",
    )
    case.auxiliary.put(case.instrument, native)
    selected = case.source.read_frames(
        case.instrument,
        bar.open_time_ms,
        bar.close_time_ms,
        as_of_ms=RECEIVED,
        channels=("depth",),
        maximum_metric_age_ms=0,
    )
    assert len(selected) == 1 and selected[0].pins[0].revision == 1
    policy = InputPolicy(database_destination_id(case.settings), ("depth",), 0)
    manifest = AnalysisInputManifest(case.root)
    try:
        reference = manifest.stage("d" * 32, "e" * 32, 1, selected, policy)
        case.auxiliary.put(
            case.instrument,
            replace(
                native,
                values=tuple((name, Decimal(200)) for name in DEPTH_FIELDS),
                available_at_ms=RECEIVED + 100,
            ),
            expected_previous=selected[0].pins[0].checksum,
        )
        corrected = case.source.read_frames(
            case.instrument,
            bar.open_time_ms,
            bar.close_time_ms,
            as_of_ms=RECEIVED + 100,
            channels=("depth",),
            maximum_metric_age_ms=0,
        )
        assert corrected[0].pins[0].revision == 2
        restored = replay(
            manifest, reference, case.source, expected_analysis_id="d" * 32, expected_policy=policy
        )
        assert restored == selected
        assert restored[0].frame.input("depth").mapping()["DEPTH_BIDS0"] == Decimal(100)
    finally:
        manifest.close()


def observation(case, channel, *, event=None, available=RECEIVED, quality="observed", **changes):
    event = START if event is None else event
    source, values = {
        "trade_flow": (
            ":aggTrade",
            (("AGGTRADE_NOTIONALBUY", Decimal(10)), ("AGGTRADE_NOTIONALSELL", Decimal(0))),
        ),
        "open_interest": (
            ":open_interest_history",
            (("METRIC_OPENINTEREST", Decimal(1000)), ("SOURCE_PERIOD_MS", 300000)),
        ),
        "long_short_ratio": (
            ":long_short_ratio",
            (("METRIC_LONGSHORTRATIO", Decimal("1.2")), ("SOURCE_PERIOD_MS", 300000)),
        ),
    }[channel]
    opening = event - 60000 if channel == "trade_flow" else event // 60000 * 60000
    return AuxiliaryInput(
        **dict(
            channel=channel,
            values=values,
            window_start_ms=opening,
            window_end_ms=opening + 60000,
            event_time_ms=event,
            available_at_ms=available,
            quality=quality,
            coverage=Decimal(1) if quality == "observed" else Decimal(0),
            source=case.instrument.key + source,
            **changes,
        )
    )


@pytest.mark.database
def test_actual_candles_native_metrics_flow_and_retrospective_boundary_keep_original_pins(case):
    for bar in case.bars:
        case.auxiliary.put(
            case.instrument, observation(case, "trade_flow", event=bar.close_time_ms)
        )
    for channel in ("open_interest", "long_short_ratio"):
        case.auxiliary.put(case.instrument, observation(case, channel))
    result = case.source.read_frames(
        case.instrument,
        START,
        END,
        as_of_ms=RECEIVED,
        channels=("trade_flow", "metric"),
        maximum_metric_age_ms=330000,
        retrospective_until_ms=END - 60000,
    )
    assert len(result) == 3
    assert [value.retrospective for value in result] == [True, True, False]
    assert [value.frame.candle for value in result] == list(case.bars)
    assert all(len(value.pins) == 3 and value.frame.as_of_ms == RECEIVED for value in result)
    assert all(
        pin.observation.available_at_ms == RECEIVED for value in result for pin in value.pins
    )
    assert all(
        value.frame.input("metric").mapping()["METRIC_OPENINTEREST"] == Decimal(1000)
        for value in result
    )
    assert result[-1].frame.reference_time_ms is None
    assert case.source.destination_id == database_destination_id(case.settings)


@pytest.mark.database
def test_candle_reception_cutoff_is_actual_and_ohlcv_only_never_opens_auxiliary_store(case):
    calls = []

    def forbidden(settings):
        calls.append(settings)
        raise AssertionError("OHLCV-only selection opened auxiliary storage")

    source = module.CommittedAnalysisFrameSource(case.root, store_factory=forbidden)
    try:
        assert source.destination_id is None
        assert (
            source.read_frames(
                case.instrument, START, END, as_of_ms=END, channels=(), maximum_metric_age_ms=330000
            )
            == ()
        )
        assert source.read((case.instrument.key,), START, END) == case.bars
        frames = source.read_frames(
            case.instrument,
            START,
            END,
            as_of_ms=RECEIVED,
            channels=(),
            maximum_metric_age_ms=330000,
        )
        assert len(frames) == 3 and all(
            value.pins == () and value.frame.inputs == () for value in frames
        )
        assert calls == []
    finally:
        source.close()


@pytest.mark.database
def test_partial_and_real_zero_are_preserved_and_replay_pin_uses_original_cutoff(case):
    first = observation(case, "trade_flow", event=START + 60000, quality="partial")
    receipt = case.auxiliary.put(case.instrument, first)
    repaired = replace(
        first,
        quality="observed",
        coverage=Decimal(1),
        available_at_ms=RECEIVED + 1,
        values=(("AGGTRADE_NOTIONALBUY", Decimal(0)), ("AGGTRADE_NOTIONALSELL", Decimal(0))),
    )
    update = case.auxiliary.put(case.instrument, repaired, expected_previous=receipt.checksum)
    old = case.source.read_frames(
        case.instrument,
        START,
        START + 60000,
        as_of_ms=RECEIVED,
        channels=("trade_flow",),
        maximum_metric_age_ms=330000,
    )[0]
    latest = case.source.read_frames(
        case.instrument,
        START,
        START + 60000,
        as_of_ms=RECEIVED + 1,
        channels=("trade_flow",),
        maximum_metric_age_ms=330000,
    )[0]
    assert old.frame.input("trade_flow").quality == "partial" and old.pins[0].revision == 1
    assert latest.frame.input("trade_flow").quality == "observed" and latest.pins[0].revision == 2
    assert latest.frame.input("trade_flow").mapping()["AGGTRADE_NOTIONALBUY"] == 0
    assert latest.pins[0].checksum == update.checksum
    pinned = case.source.get_auxiliary(
        case.instrument,
        "trade_flow",
        first.source,
        first.window_start_ms,
        as_of_ms=first.available_at_ms,
    )
    assert pinned == old.pins[0] and pinned.checksum == receipt.checksum


@pytest.mark.database
def test_native_event_at_close_is_excluded_and_uncollected_flow_is_missing(case):
    case.auxiliary.put(case.instrument, observation(case, "open_interest", event=START + 60000))
    case.auxiliary.put(case.instrument, observation(case, "long_short_ratio"))
    case.auxiliary.put(case.instrument, observation(case, "open_interest", available=RECEIVED + 1))
    first = case.source.read_frames(
        case.instrument,
        START,
        START + 60000,
        as_of_ms=RECEIVED,
        channels=("trade_flow", "metric"),
        maximum_metric_age_ms=330000,
    )[0]
    assert (
        first.frame.input("trade_flow").quality == first.frame.input("metric").quality == "missing"
    )
    assert first.frame.input("trade_flow").values == first.frame.input("metric").values == ()
    assert len(first.pins) == 1 and first.pins[0].observation.channel == "long_short_ratio"
    later = case.source.read_frames(
        case.instrument,
        START,
        START + 60000,
        as_of_ms=RECEIVED + 1,
        channels=("metric",),
        maximum_metric_age_ms=330000,
    )[0]
    assert later.frame.input("metric").quality == "observed"
    assert len(later.pins) == 2 and later.pins[0].observation.available_at_ms == RECEIVED + 1


@pytest.mark.database
def test_owner_thread_and_changed_public_destination_cannot_retarget_existing_source(
    case, monkeypatch
):
    case.source.read((case.instrument.key,), START, END)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ValidationError, match="worker thread"):
            pool.submit(case.source.read, (case.instrument.key,), START, END).result()
        with pytest.raises(ValidationError, match="worker thread"):
            pool.submit(case.source.close).result()
    original = case.source.destination_id
    monkeypatch.setattr(
        module,
        "load_database_settings",
        lambda _: replace(case.settings, dbname="wrong_destination_never_connected"),
    )
    with pytest.raises(ValidationError, match="destination changed"):
        case.source.read((case.instrument.key,), START, END)
    assert case.source.destination_id == original


def test_constructor_has_no_io_unsupported_channel_and_bad_request_stop_before_binding(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        module,
        "load_database_settings",
        lambda _: pytest.fail("constructor/request validation performed IO"),
    )
    source = module.CommittedAnalysisFrameSource(tmp_path)
    instrument = Instrument("binance", Market.USDM, "ETHUSDT")
    for channels in (("funding",), ([],)):
        with pytest.raises(ValidationError):
            source.read_frames(
                instrument,
                START,
                END,
                as_of_ms=RECEIVED,
                channels=channels,
                maximum_metric_age_ms=330000,
            )
    assert source.destination_id is None
    source.close()


def test_failed_auxiliary_close_can_be_retried_but_never_reenables_reads(tmp_path, monkeypatch):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "not-logged")
    monkeypatch.setattr(module, "load_database_settings", lambda _: settings)

    class Store:
        destination_id = database_destination_id(settings)
        failed = True

        def get(self, *args, **kwargs):
            return None

        def close(self):
            if self.failed:
                self.failed = False
                raise StorageError("synthetic close failed")

    store = Store()
    source = module.CommittedAnalysisFrameSource(tmp_path, store_factory=lambda _: store)
    source.get_auxiliary(
        Instrument("binance", Market.USDM, "ETHUSDT"),
        "trade_flow",
        "dataset",
        START,
        as_of_ms=RECEIVED,
    )
    with pytest.raises(StorageError):
        source.close()
    with pytest.raises(StorageError, match="closed"):
        source.get_auxiliary(
            Instrument("binance", Market.USDM, "ETHUSDT"),
            "trade_flow",
            "dataset",
            START,
            as_of_ms=RECEIVED,
        )
    source.close()
    assert source._auxiliary_store is None


@pytest.mark.parametrize("fail_close", [False, True])
def test_mismatched_store_is_retired_and_failed_cleanup_remains_retryable(
    tmp_path, monkeypatch, fail_close
):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "not-logged")
    monkeypatch.setattr(module, "load_database_settings", lambda _: settings)

    class Store:
        destination_id = "f" * 64
        calls = 0

        def close(self):
            self.calls += 1
            if fail_close and self.calls == 1:
                raise StorageError("synthetic mismatch close failed")

    store = Store()
    source = module.CommittedAnalysisFrameSource(tmp_path, store_factory=lambda _: store)
    with pytest.raises(StorageError if fail_close else ValidationError):
        source.get_auxiliary(
            Instrument("binance", Market.USDM, "ETHUSDT"),
            "trade_flow",
            "dataset",
            START,
            as_of_ms=RECEIVED,
        )
    assert store.calls == 1
    source.close()
    assert source._auxiliary_store is None
    with pytest.raises(StorageError, match="closed"):
        source.get_auxiliary(
            Instrument("binance", Market.USDM, "ETHUSDT"),
            "trade_flow",
            "dataset",
            START,
            as_of_ms=RECEIVED,
        )


@pytest.mark.parametrize(
    "market,age,quality", [(Market.SPOT, 330000, "unsupported"), (Market.USDM, 0, "missing")]
)
def test_unsupported_spot_or_zero_metric_ttl_does_not_construct_a_store(
    tmp_path, monkeypatch, market, age, quality
):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "not-logged")
    monkeypatch.setattr(module, "load_database_settings", lambda _: settings)
    source = module.CommittedAnalysisFrameSource(
        tmp_path, store_factory=lambda _: pytest.fail("unusable metric constructed a store")
    )
    instrument = Instrument("binance", market, "ETHUSDT")
    bar = Candle(instrument, START, START + 60000, *(Decimal(v) for v in (100, 102, 99, 101, 2)))
    monkeypatch.setattr(source._candles, "read", lambda *args, **kwargs: (bar,))
    selected = source.read_frames(
        instrument,
        START,
        START + 60000,
        as_of_ms=RECEIVED,
        channels=("metric",),
        maximum_metric_age_ms=age,
    )[0]
    assert selected.frame.input("metric").quality == quality and selected.pins == ()
    source.close()
