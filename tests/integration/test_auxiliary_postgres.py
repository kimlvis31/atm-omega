"""Opt-in append-only auxiliary revisions on the isolated Omega test database."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import EPOCH, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.ports.auxiliary import AuxiliaryReadRange

pytestmark = pytest.mark.database
START = 1_700_000_040_000


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must select the isolated Omega test database")
    value = load_database_settings(Path(path).resolve())
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing auxiliary DB tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        migrate(connection)
    return value


@pytest.fixture
def instrument(settings):
    value = Instrument("binance", Market.USDM, "TEST_AUX_" + uuid4().hex[:24].upper())
    yield value
    # Only keys allocated by this test are removed; preserve preexisting ETH and Eta data.
    keys = (value.key, Instrument(value.provider, Market.SPOT, value.symbol).key)
    with connect_database(settings) as connection:
        connection.execute("DELETE FROM market.auxiliary_minutes WHERE instrument IN (%s,%s)", keys)


@pytest.fixture
def store(settings):
    value = PostgresAuxiliaryStore(settings)
    yield value
    value.close()


def observation(**changes):
    return AuxiliaryInput(
        **{
            "channel": "trade_flow",
            "values": (
                ("AGGTRADE_NOTIONALBUY", Decimal("100.123456789012345678901234567890")),
                ("AGGTRADE_NOTIONALSELL", Decimal("30.000000000000000000000000000001")),
                ("AGGTRADE_NTRADESBUY", 17),
            ),
            "window_start_ms": START,
            "window_end_ms": START + 60000,
            "event_time_ms": START + 60000,
            "available_at_ms": START + 60001,
            "source": "synthetic_exact_dataset",
            **changes,
        }
    )


def read(store, instrument, value, cutoff=None):
    return store.get(
        instrument,
        value.channel,
        value.source,
        value.window_start_ms,
        as_of_ms=value.available_at_ms if cutoff is None else cutoff,
    )


def test_exact_decimal_integer_and_availability_survive_new_connection(store, instrument, settings):
    value = observation()
    receipt = store.put(instrument, value)
    assert receipt.inserted is True and receipt.revision == 1
    assert len(receipt.checksum) == 64
    assert read(store, instrument, value, value.available_at_ms - 1) is None
    reopened = PostgresAuxiliaryStore(settings)
    try:
        stored = read(reopened, instrument, value)
        assert stored.instrument == instrument and stored.observation == value
        assert stored.revision == 1 and stored.checksum == receipt.checksum
        assert stored.previous_checksum is None
        values = stored.observation.mapping()
        assert isinstance(values["AGGTRADE_NOTIONALBUY"], Decimal)
        assert type(values["AGGTRADE_NTRADESBUY"]) is int
        assert (
            values["AGGTRADE_NOTIONALBUY"].as_tuple()
            == value.mapping()["AGGTRADE_NOTIONALBUY"].as_tuple()
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("quality", ["observed", "missing", "unsupported"])
def test_observed_zero_remains_distinct_from_missing_or_unsupported(store, instrument, quality):
    value = observation(
        quality=quality,
        coverage=Decimal(1 if quality == "observed" else 0),
        values=(("AGGTRADE_NOTIONALBUY", Decimal(0)),) if quality == "observed" else (),
    )
    receipt = store.put(instrument, value)
    stored = read(store, instrument, value)
    assert stored.observation == value and stored.checksum == receipt.checksum
    assert stored.observation.usable_at(value.available_at_ms) is (quality == "observed")


def test_decoded_equal_duplicate_preserves_original_revision_and_digest(store, instrument):
    value = observation(values=(("NET", Decimal("1.0")),))
    first = store.put(instrument, value)
    equivalent = replace(value, values=(("NET", Decimal("1.0000")),))
    assert equivalent == value
    duplicate = store.put(instrument, equivalent)
    assert duplicate.inserted is False
    assert (duplicate.revision, duplicate.checksum) == (first.revision, first.checksum)
    assert (
        read(store, instrument, value).observation.values[0][1].as_tuple()
        == Decimal("1.0").as_tuple()
    )


def test_integer_and_decimal_equal_numeric_values_are_distinct_typed_observations(
    store, instrument
):
    value = observation(values=(("COUNT", 1),))
    first = store.put(instrument, value)
    different_type = replace(value, values=(("COUNT", Decimal(1)),))
    with pytest.raises(ProtocolError):
        store.put(instrument, different_type)
    with pytest.raises(ProtocolError):
        store.put(instrument, different_type, expected_previous=first.checksum)
    different_type = replace(different_type, available_at_ms=START + 120000)
    changed = store.put(instrument, different_type, expected_previous=first.checksum)
    assert changed.revision == 2 and changed.inserted
    assert type(read(store, instrument, different_type).observation.values[0][1]) is Decimal


def test_conflict_cas_revision_and_asof_do_not_reveal_future_repair(store, instrument):
    original = observation()
    first = store.put(instrument, original)
    repaired = replace(
        original, values=(("NET", Decimal("123.25")),), available_at_ms=START + 120000
    )
    with pytest.raises(ProtocolError):
        store.put(instrument, repaired)
    with pytest.raises(ProtocolError):
        store.put(instrument, repaired, expected_previous="0" * 64)
    assert read(store, instrument, original).checksum == first.checksum
    second = store.put(instrument, repaired, expected_previous=first.checksum)
    assert second.inserted is True and second.revision == first.revision + 1
    prior = read(store, instrument, repaired, repaired.available_at_ms - 1)
    assert prior.observation == original and prior.checksum == first.checksum
    current = read(store, instrument, repaired)
    assert current.observation == repaired and current.previous_checksum == first.checksum
    assert current.checksum == second.checksum
    same_time_repair = replace(repaired, values=(("NET", Decimal("150")),))
    with pytest.raises(ProtocolError):
        store.put(instrument, same_time_repair, expected_previous=second.checksum)
    assert read(store, instrument, repaired).checksum == second.checksum
    # This fixture supplies a later normalization-completion observation time;
    # the store must never invent +1ms to make a rejected repair admissible.
    later_repair = replace(same_time_repair, available_at_ms=START + 180000)
    third = store.put(instrument, later_repair, expected_previous=second.checksum)
    assert third.revision == 3
    assert read(store, instrument, later_repair).revision == 3
    assert read(store, instrument, later_repair, later_repair.available_at_ms - 1).revision == 2
    assert (
        read(store, instrument, later_repair, repaired.available_at_ms).checksum == second.checksum
    )
    assert (
        read(store, instrument, later_repair, repaired.available_at_ms - 1).checksum
        == first.checksum
    )
    with pytest.raises(ProtocolError):
        store.put(instrument, original, expected_previous=first.checksum)


def test_first_revision_requires_no_previous_and_correction_availability_never_decreases(
    store, instrument
):
    first_value = observation(available_at_ms=START + 120000)
    with pytest.raises(ProtocolError):
        store.put(instrument, first_value, expected_previous="a" * 64)
    assert read(store, instrument, first_value) is None
    first = store.put(instrument, first_value)
    older = replace(first_value, available_at_ms=START + 119999, values=(("NET", Decimal(9)),))
    with pytest.raises(ProtocolError):
        store.put(instrument, older, expected_previous=first.checksum)
    assert read(store, instrument, first_value).observation == first_value


def test_source_channel_and_market_identity_define_independent_datasets(store, instrument):
    value = observation()
    variations = (
        (instrument, value),
        (instrument, replace(value, source="independent_dataset")),
        (instrument, replace(value, channel="depth")),
        (Instrument(instrument.provider, Market.SPOT, instrument.symbol), value),
    )
    for identity, sample in variations:
        assert store.put(identity, sample).revision == 1
        assert read(store, identity, sample).observation == sample
    assert (
        store.get(instrument, value.channel, "absent_dataset", START, as_of_ms=START + 120000)
        is None
    )


def test_receipt_is_returned_only_after_commit_is_visible_to_another_connection(
    store, instrument, settings
):
    original_connection, committed = store.connection, []
    reader = PostgresAuxiliaryStore(settings)
    value = observation()

    class InspectCommit:
        @contextmanager
        def transaction(self):
            with original_connection.transaction():
                yield
                assert read(reader, instrument, value) is None
            committed.append(True)

        def __getattr__(self, name):
            return getattr(original_connection, name)

    store.connection = InspectCommit()
    try:
        receipt = store.put(instrument, value)
        assert committed == [True] and receipt.inserted
        assert read(reader, instrument, value).checksum == receipt.checksum
    finally:
        store.connection = original_connection
        reader.close()


def test_statement_failure_rolls_back_insert_and_same_connection_can_retry(store, instrument):
    original_connection = store.connection
    value = observation()

    class FailingInsert:
        def execute(self, sql, parameters=None):
            result = original_connection.execute(sql, parameters)
            if "INSERT INTO market.auxiliary_minutes" in sql:
                original_connection.execute("SELECT 1/0")
            return result

        def __getattr__(self, name):
            return getattr(original_connection, name)

    store.connection = FailingInsert()
    try:
        with pytest.raises(StorageError):
            store.put(instrument, value)
    finally:
        store.connection = original_connection
    assert read(store, instrument, value) is None
    assert store.put(instrument, value).revision == 1


def test_archive_batch_rolls_back_and_only_repairs_older_incomplete_rows(store, instrument):
    partial = observation(quality="partial", coverage=Decimal("0.5"))
    observed = observation(channel="depth")
    new = observation(channel="open_interest")
    repair = replace(
        partial,
        quality="observed",
        coverage=Decimal(1),
        available_at_ms=partial.available_at_ms + 10,
    )
    changed = replace(
        observed, values=(("DEPTH_BIDS0", 7),), available_at_ms=observed.available_at_ms + 10
    )
    store.put(instrument, partial)
    store.put(instrument, observed)
    original_connection = store.connection

    class FailingInsert:
        def execute(self, sql, parameters=None):
            result = original_connection.execute(sql, parameters)
            if "INSERT INTO market.auxiliary_minutes" in sql:
                original_connection.execute("SELECT 1/0")
            return result

        def __getattr__(self, name):
            return getattr(original_connection, name)

    store.connection = FailingInsert()
    try:
        with pytest.raises(StorageError):
            store.put_history_batch(instrument, [new, repair, changed])
    finally:
        store.connection = original_connection

    assert read(store, instrument, new) is None
    assert read(store, instrument, repair).revision == 1
    assert store.put_history_batch(instrument, [new, repair, changed]) == 2
    assert read(store, instrument, new).revision == 1
    assert read(store, instrument, repair, partial.available_at_ms).observation == partial
    repaired = read(store, instrument, repair)
    assert repaired.revision == 2 and repaired.observation == repair
    assert read(store, instrument, changed).observation == observed
    assert store.put_history_batch(instrument, [new, repair, changed]) == 0


def test_commit_acknowledgement_loss_retry_is_a_decoded_equal_duplicate(store, instrument):
    import psycopg

    original_connection = store.connection
    value = observation()

    class LostCommitReply:
        @contextmanager
        def transaction(self):
            with original_connection.transaction():
                yield
            raise psycopg.OperationalError("synthetic commit acknowledgement loss")

        def __getattr__(self, name):
            return getattr(original_connection, name)

    store.connection = LostCommitReply()
    try:
        with pytest.raises(StorageError):
            store.put(instrument, value)
    finally:
        store.connection = original_connection
    assert read(store, instrument, value).revision == 1
    receipt = store.put(instrument, value)
    assert receipt.inserted is False and receipt.revision == 1


@pytest.mark.parametrize("repair", [False, True])
def test_independent_concurrent_writers_deduplicate_or_have_exactly_one_cas_winner(
    settings, instrument, repair
):
    base = observation()
    seed = PostgresAuxiliaryStore(settings)
    try:
        previous = seed.put(instrument, base).checksum if repair else None
    finally:
        seed.close()
    barrier = threading.Barrier(2)

    def write(index):
        current = PostgresAuxiliaryStore(settings)
        try:
            sample = (
                replace(base, values=(("NET", Decimal(index + 1)),), available_at_ms=START + 120000)
                if repair
                else base
            )
            barrier.wait(timeout=5)
            return current.put(instrument, sample, expected_previous=previous)
        except ProtocolError:
            return "conflict"
        finally:
            current.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(write, (0, 1)))
    receipts = [item for item in outcomes if item != "conflict"]
    assert sum(item.inserted for item in receipts) == 1
    if repair:
        assert outcomes.count("conflict") == 1
        assert receipts[0].revision == 2
    else:
        assert len(receipts) == 2 and {item.revision for item in receipts} == {1}
    with connect_database(settings) as connection:
        assert connection.execute(
            "SELECT count(*) FROM market.auxiliary_minutes WHERE instrument=%s", (instrument.key,)
        ).fetchone() == (2 if repair else 1,)


@pytest.mark.parametrize("field", ["payload", "checksum"])
def test_data_only_read_rejects_tampered_payload_or_checksum(store, instrument, settings, field):
    value = observation()
    store.put(instrument, value)
    # Both constant statements are restricted to this test's unique instrument.
    sql = (
        "UPDATE market.auxiliary_minutes SET payload=%s WHERE instrument=%s"
        if field == "payload"
        else "UPDATE market.auxiliary_minutes SET checksum=%s WHERE instrument=%s"
    )
    parameters = (b"{}", instrument.key) if field == "payload" else ("0" * 64, instrument.key)
    with connect_database(settings) as connection:
        connection.execute(sql, parameters)
    with pytest.raises(StorageError):
        read(store, instrument, value)


def test_repair_read_checks_its_immediate_predecessor_chain(store, instrument, settings):
    first_value = observation()
    first = store.put(instrument, first_value)
    repaired = replace(
        first_value, values=(("NET", Decimal("55")),), available_at_ms=START + 120000
    )
    store.put(instrument, repaired, expected_previous=first.checksum)
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET checksum=%s WHERE instrument=%s AND revision=1",
            ("0" * 64, instrument.key),
        )
    with pytest.raises(StorageError):
        read(store, instrument, repaired)


def test_valid_checksum_repair_with_equal_predecessor_availability_is_rejected(
    store, instrument, settings
):
    original = observation()
    first = store.put(instrument, original)
    repaired = replace(original, values=(("NET", Decimal("75")),), available_at_ms=START + 120000)
    second = store.put(instrument, repaired, expected_previous=first.checksum)
    assert second.revision == 2
    same_time = replace(repaired, available_at_ms=original.available_at_ms)
    payload = encode_observation(instrument, same_time)
    # All selected-row identity, payload, checksum and previous digest remain
    # coherent. Only the strict availability edge violates the storage contract.
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET payload=%s,checksum=%s,available_ms=%s "
            "WHERE instrument=%s AND revision=2",
            (payload, sha256(payload).hexdigest(), same_time.available_at_ms, instrument.key),
        )
    with pytest.raises(StorageError):
        read(store, instrument, repaired)


def test_bounded_range_keeps_sparse_windows_and_selects_only_known_revisions(store, instrument):
    first = observation()
    original = store.put(instrument, first)
    repaired = replace(first, available_at_ms=START + 120000, values=(("NET", Decimal("42")),))
    store.put(instrument, repaired, expected_previous=original.checksum)
    later = observation(
        window_start_ms=START + 120000,
        window_end_ms=START + 180000,
        event_time_ms=START + 180000,
        available_at_ms=START + 180001,
    )
    store.put(instrument, later)
    ranges = (AuxiliaryReadRange(instrument, first.channel, first.source, START, START + 180000),)
    early = store.read_ranges(ranges, as_of_ms=first.available_at_ms, limit=3)
    assert len(early) == 1 and early[0].revision == 1
    current = store.read_ranges(ranges, as_of_ms=later.available_at_ms, limit=3)
    assert [row.observation.window_start_ms for row in current] == [START, START + 120000]
    assert current[0].revision == 2 and current[0].observation == repaired
    assert current[1].revision == 1 and current[1].observation == later
    assert (
        store.get(instrument, first.channel, first.source, START, as_of_ms=first.available_at_ms)
        == early[0]
    )


def test_auxiliary_range_rejects_overlap_and_capacity_before_query(store, instrument):
    value = observation()
    span = AuxiliaryReadRange(instrument, value.channel, value.source, START, START + 180000)
    for ranges, options in (
        ((span,), {"limit": 2}),
        ((span, span), {"limit": 10}),
        ((span,), {"as_of_ms": True}),
        ((), {}),
    ):
        kwargs = {"as_of_ms": START + 180001} | options
        with pytest.raises(ValidationError):
            store.read_ranges(ranges, **kwargs)
    assert store.read_ranges((span,), as_of_ms=START + 180001, limit=3) == ()


@pytest.mark.parametrize("field", ["payload", "previous_edge"])
def test_auxiliary_range_rejects_corruption_and_connection_stays_usable(
    store, instrument, settings, field
):
    first = observation()
    receipt = store.put(instrument, first)
    repaired = replace(first, values=(("NET", Decimal("24")),), available_at_ms=START + 120000)
    store.put(instrument, repaired, expected_previous=receipt.checksum)
    with connect_database(settings) as connection:
        if field == "payload":
            connection.execute(
                "UPDATE market.auxiliary_minutes SET payload=%s WHERE instrument=%s AND revision=2",
                (b"{}", instrument.key),
            )
        else:
            connection.execute(
                "UPDATE market.auxiliary_minutes SET checksum=%s "
                "WHERE instrument=%s AND revision=1",
                ("b" * 64, instrument.key),
            )
    span = AuxiliaryReadRange(instrument, first.channel, first.source, START, START + 60000)
    with pytest.raises(StorageError):
        store.read_ranges((span,), as_of_ms=START + 120001, limit=1)
    assert store.connection.execute("SELECT 1").fetchone() == (1,)


def test_auxiliary_ranges_share_one_snapshot_while_another_owner_repairs(
    store, instrument, settings
):
    first = observation()
    second = replace(first, source="second_snapshot_source")
    store.put(instrument, first)
    old = store.put(instrument, second)
    entered, changed = threading.Event(), threading.Event()
    connection = store.connection

    class Cursor:
        def __init__(self, actual):
            self.actual = actual

        def execute(self, *args, **kwargs):
            return self.actual.execute(*args, **kwargs)

        def fetchmany(self, count):
            rows = self.actual.fetchmany(count)
            if rows and not entered.is_set():
                entered.set()
                assert changed.wait(5)
            return rows

    class Connection:
        def __getattr__(self, name):
            return getattr(connection, name)

        @contextmanager
        def cursor(self, **kwargs):
            with connection.cursor(**kwargs) as actual:
                yield Cursor(actual)

    def repair():
        assert entered.wait(5)
        writer = PostgresAuxiliaryStore(settings)
        try:
            value = replace(second, available_at_ms=START + 120000, values=(("NET", Decimal("9")),))
            assert writer.put(instrument, value, expected_previous=old.checksum).revision == 2
        finally:
            writer.close()
            changed.set()

    ranges = tuple(
        AuxiliaryReadRange(instrument, value.channel, value.source, START, START + 60000)
        for value in (first, second)
    )
    store.connection = Connection()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(repair)
            frozen = store.read_ranges(ranges, as_of_ms=START + 180001, limit=2)
            future.result(5)
        assert [row.revision for row in frozen] == [1, 1]
        latest = store.read_ranges(ranges, as_of_ms=START + 180001, limit=2)
        assert [row.revision for row in latest] == [1, 2]
    finally:
        changed.set()
        store.connection = connection


def test_stream_auxiliary_batch_preserves_first_receipt_and_revises_changed_metrics(
    store, instrument
):
    trade = observation()
    statistic = observation(channel="open_interest", values=(("METRIC_OPENINTEREST", 1),))
    assert store.put_live_batch([(instrument, trade), (instrument, statistic)]) == 2
    duplicate = replace(trade, available_at_ms=trade.available_at_ms + 1)
    unchanged = replace(statistic, available_at_ms=statistic.available_at_ms + 1)
    corrected = replace(unchanged, values=(("METRIC_OPENINTEREST", 2),))
    assert store.put_live_batch([(instrument, duplicate), (instrument, unchanged)]) == 0
    assert store.put_live_batch([(instrument, corrected)]) == 1
    assert read(store, instrument, duplicate).observation == trade
    assert read(store, instrument, statistic).observation == statistic
    assert read(store, instrument, corrected).observation == corrected
    assert store.put_live_batch([(instrument, corrected)]) == 0


def test_live_batch_rejects_missing_exact_predecessor_and_rolls_back_all_keys(
    store, instrument, settings
):
    first_value = observation(
        channel="open_interest", values=(("METRIC_OPENINTEREST", 1),)
    )
    first = store.put(instrument, first_value)
    second_value = replace(
        first_value,
        values=(("METRIC_OPENINTEREST", 2),),
        available_at_ms=START + 120000,
    )
    second = store.put(instrument, second_value, expected_previous=first.checksum)
    third_value = replace(
        first_value,
        values=(("METRIC_OPENINTEREST", 3),),
        available_at_ms=START + 180000,
    )
    third = store.put(instrument, third_value, expected_previous=second.checksum)

    other_key = observation(
        channel="open_interest",
        source="synthetic_second_segment",
        window_start_ms=START + 60000,
        window_end_ms=START + 120000,
        event_time_ms=START + 120000,
        available_at_ms=START + 120001,
        values=(("METRIC_OPENINTEREST", 10),),
    )
    store.put(instrument, other_key)
    other_candidate = replace(
        other_key,
        values=(("METRIC_OPENINTEREST", 11),),
        available_at_ms=other_key.available_at_ms + 1,
    )
    # A third minute/channel segment makes the query span independent identities.
    store.put(
        instrument,
        observation(
            channel="long_short_ratio",
            source="synthetic_third_segment",
            window_start_ms=START + 120000,
            window_end_ms=START + 180000,
            event_time_ms=START + 180000,
            available_at_ms=START + 180001,
            values=(("LONG_SHORT_RATIO", Decimal("1.25")),),
        ),
    )

    opening = EPOCH + timedelta(milliseconds=START)
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.auxiliary_minutes SET previous_checksum=%s "
            "WHERE instrument=%s AND channel=%s AND dataset=%s AND open_time=%s AND revision=3",
            (first.checksum, instrument.key, third_value.channel, third_value.source, opening),
        )
        connection.execute(
            "DELETE FROM market.auxiliary_minutes "
            "WHERE instrument=%s AND channel=%s AND dataset=%s AND open_time=%s AND revision=2",
            (instrument.key, second_value.channel, second_value.source, opening),
        )

    corrupt_candidate = replace(
        third_value,
        values=(("METRIC_OPENINTEREST", 4),),
        available_at_ms=START + 240000,
    )
    with pytest.raises(StorageError):
        store.put_live_batch([(instrument, corrupt_candidate), (instrument, other_candidate)])

    # The corrupt row still points to r1 while r2 is absent, and the valid
    # second key's otherwise applicable revision was rolled back with the batch.
    with connect_database(settings) as connection:
        assert connection.execute(
            "SELECT revision,checksum,previous_checksum FROM market.auxiliary_minutes "
            "WHERE instrument=%s AND channel=%s AND dataset=%s AND open_time=%s "
            "ORDER BY revision",
            (instrument.key, third_value.channel, third_value.source, opening),
        ).fetchall() == [(1, first.checksum, None), (3, third.checksum, first.checksum)]
    assert read(store, instrument, other_candidate).revision == 1
    assert store.get(
        instrument,
        "long_short_ratio",
        "synthetic_third_segment",
        START + 120000,
        as_of_ms=START + 180001,
    ).revision == 1


def test_valid_two_revision_live_key_replay_does_not_advance(store, instrument):
    original = observation(
        channel="open_interest", values=(("METRIC_OPENINTEREST", 7),)
    )
    first = store.put(instrument, original)
    revised = replace(
        original,
        values=(("METRIC_OPENINTEREST", 8),),
        available_at_ms=original.available_at_ms + 1,
    )
    second = store.put(instrument, revised, expected_previous=first.checksum)

    assert second.revision == 2 and second.inserted
    assert store.put_live_batch([(instrument, revised)]) == 0
    stored = read(store, instrument, revised)
    assert stored.revision == 2
    assert stored.checksum == second.checksum and stored.previous_checksum == first.checksum
