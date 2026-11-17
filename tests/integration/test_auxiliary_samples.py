"""Opt-in native point-sample lookup on isolated omega_test, never candle carry views."""

import os
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate

pytestmark = pytest.mark.database
START = 1_700_000_040_000
CHANNEL = "open_interest"
SOURCE = "synthetic_native_points"


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    configured = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not configured:
        pytest.fail("OMEGA_TEST_DB_CONFIG must select the isolated Omega test database")
    value = load_database_settings(Path(configured).resolve())
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing sample tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        migrate(connection)
    return value


@pytest.fixture
def instrument(settings):
    value = Instrument("binance", Market.USDM, "TEST_SAMPLE_" + uuid4().hex[:24].upper())
    yield value
    keys = (value.key, Instrument(value.provider, Market.SPOT, value.symbol).key)
    with connect_database(settings) as connection:
        connection.execute("DELETE FROM market.auxiliary_minutes WHERE instrument IN (%s,%s)", keys)


@pytest.fixture
def store(settings):
    value = PostgresAuxiliaryStore(settings)
    yield value
    value.close()


def sample(event_ms, **changes):
    opening = event_ms // 60000 * 60000
    return AuxiliaryInput(
        **{
            "channel": CHANNEL,
            "source": SOURCE,
            "window_start_ms": opening,
            "window_end_ms": opening + 60000,
            "event_time_ms": event_ms,
            "available_at_ms": event_ms + 10,
            "values": (("OPEN_INTEREST", Decimal("123456.789012345678901234")),),
            **changes,
        }
    )


def latest(store, instrument, before, *, cutoff=None, age=330000, channel=CHANNEL, source=SOURCE):
    return store.latest_sample(
        instrument,
        channel,
        source,
        event_before_ms=before,
        as_of_ms=before if cutoff is None else cutoff,
        max_age_ms=age,
    )


def test_event_equal_to_before_is_excluded_but_one_ms_later_includes_exact_point(store, instrument):
    point = sample(START + 1000)
    saved = store.put(instrument, point)
    assert latest(store, instrument, point.event_time_ms, cutoff=point.available_at_ms) is None
    result = latest(store, instrument, point.event_time_ms + 1, cutoff=point.available_at_ms)
    assert result.observation == point and result.checksum == saved.checksum
    assert isinstance(result.observation.mapping()["OPEN_INTEREST"], Decimal)


def test_ttl_lower_boundary_is_inclusive_and_one_ms_older_is_absent(store, instrument):
    point = sample(START + 1000)
    store.put(instrument, point)
    assert latest(store, instrument, point.event_time_ms + 10000, age=10000).observation == point
    assert latest(store, instrument, point.event_time_ms + 10001, age=10000) is None


def test_future_availability_does_not_hide_an_older_eligible_native_point(store, instrument):
    older = sample(START + 1000)
    newer = sample(START + 61000, available_at_ms=START + 90000)
    store.put(instrument, older)
    store.put(instrument, newer)
    assert latest(store, instrument, START + 62000).observation == older
    assert (
        latest(store, instrument, START + 62000, cutoff=newer.available_at_ms).observation == newer
    )


def test_same_minute_event_time_precedes_revision_and_before_remains_strict(store, instrument):
    first = sample(START + 1000)
    first_receipt = store.put(instrument, first)
    second = sample(START + 2000, values=(("OPEN_INTEREST", Decimal("200")),))
    second_receipt = store.put(instrument, second, expected_previous=first_receipt.checksum)
    old_event_repair = replace(
        first, available_at_ms=START + 4000, values=(("OPEN_INTEREST", Decimal("150")),)
    )
    repair_receipt = store.put(
        instrument, old_event_repair, expected_previous=second_receipt.checksum
    )
    assert repair_receipt.revision == 3
    # A higher revision for an older event must not replace the newest native event.
    assert latest(store, instrument, START + 5000).checksum == second_receipt.checksum
    assert (
        latest(store, instrument, second.event_time_ms, cutoff=START + 5000).checksum
        == repair_receipt.checksum
    )
    assert (
        latest(store, instrument, second.event_time_ms, cutoff=first.available_at_ms).checksum
        == first_receipt.checksum
    )


def test_same_event_repair_selects_highest_eligible_revision_and_preserves_old_cutoff(
    store, instrument
):
    original = sample(START + 1000)
    first = store.put(instrument, original)
    repaired = replace(
        original, values=(("OPEN_INTEREST", Decimal("150")),), available_at_ms=START + 5000
    )
    second = store.put(instrument, repaired, expected_previous=first.checksum)
    before = START + 6000
    assert (
        latest(store, instrument, before, cutoff=repaired.available_at_ms - 1).checksum
        == first.checksum
    )
    selected = latest(store, instrument, before, cutoff=repaired.available_at_ms)
    assert selected.revision == 2 and selected.checksum == second.checksum
    assert selected.previous_checksum == first.checksum
    assert (
        latest(store, instrument, before, cutoff=original.available_at_ms).observation == original
    )


def test_sample_market_source_and_channel_are_independent(store, instrument):
    point = sample(START + 1000)
    spot = Instrument(instrument.provider, Market.SPOT, instrument.symbol)
    store.put(instrument, point)
    other_source = replace(
        point, source="independent_native_dataset", values=(("VALUE", Decimal(2)),)
    )
    other_channel = replace(point, channel="funding", values=(("VALUE", Decimal(3)),))
    spot_point = replace(point, values=(("VALUE", Decimal(4)),))
    for identity, value in (
        (instrument, other_source),
        (instrument, other_channel),
        (spot, spot_point),
    ):
        store.put(identity, value)
        selected = latest(store, identity, START + 2000, channel=value.channel, source=value.source)
        assert selected.observation == value
    assert latest(store, instrument, START + 2000).observation == point
    assert latest(store, instrument, START + 2000, source="absent_dataset") is None


def test_latest_payload_corruption_is_an_error_instead_of_silent_fallback(
    store, instrument, settings
):
    older, newer = sample(START + 1000), sample(START + 61000)
    store.put(instrument, older)
    store.put(instrument, newer)
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET payload=%s WHERE instrument=%s AND event_ms=%s",
            (b"{}", instrument.key, newer.event_time_ms),
        )
    with pytest.raises(StorageError):
        latest(store, instrument, START + 62000)


@pytest.mark.parametrize("kind", ["wrong_floor", "carried"])
def test_non_native_point_is_rejected_instead_of_masquerading_as_a_new_sample(
    store, instrument, kind
):
    point = (
        sample(START + 65000, window_start_ms=START, window_end_ms=START + 60000)
        if kind == "wrong_floor"
        else sample(START + 65000, quality="carried")
    )
    store.put(instrument, point)
    with pytest.raises(StorageError):
        latest(store, instrument, START + 70000)


def test_invalid_query_bounds_are_rejected_before_database_transaction(store, instrument):
    original = store.connection

    class NoQuery:
        def transaction(self):
            pytest.fail("invalid native-sample query reached the database")

        def execute(self, *_args, **_kwargs):
            pytest.fail("invalid native-sample query reached the database")

    store.connection = NoQuery()
    try:
        for changes in (
            {"event_before_ms": -1},
            {"event_before_ms": True},
            {"event_before_ms": 253_402_300_799_001},
            {"as_of_ms": "later"},
            {"as_of_ms": -1},
            {"as_of_ms": True},
            {"max_age_ms": 0},
            {"max_age_ms": 86400001},
            {"max_age_ms": True},
        ):
            parameters = dict(
                event_before_ms=START + 60000, as_of_ms=START + 60000, max_age_ms=330000
            )
            parameters.update(changes)
            with pytest.raises(ValidationError):
                store.latest_sample(instrument, CHANNEL, SOURCE, **parameters)
    finally:
        store.connection = original


def test_native_sample_uses_the_same_immediate_predecessor_integrity_checks(
    store, instrument, settings
):
    original = sample(START + 1000)
    first = store.put(instrument, original)
    repaired = replace(
        original, available_at_ms=START + 5000, values=(("OPEN_INTEREST", Decimal(150)),)
    )
    store.put(instrument, repaired, expected_previous=first.checksum)
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET checksum=%s WHERE instrument=%s AND revision=1",
            ("0" * 64, instrument.key),
        )
    with pytest.raises(StorageError):
        latest(store, instrument, START + 6000)
