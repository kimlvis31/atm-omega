"""Read-only pinned replay with real shared segments and fake revision readers."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure import analysis_frames as source_module
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.postgres.config import DatabaseSettings, database_destination_id
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy
from omega.runtime.analysis_replay import PIN_KEYS, replay, validate_head

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
IDENTITY = "1" * 32
DESTINATION = "a" * 64
START = 600000
END = START + 180000
AVAILABLE = END + 100


def observation(channel, event, available, source, values):
    opening = event - 60000 if channel == "trade_flow" else event // 60000 * 60000
    return AuxiliaryInput(
        channel,
        values,
        opening,
        opening + 60000,
        event,
        available,
        source=INSTRUMENT.key + ":" + source,
    )


def stored(value, revision=1, previous=None):
    digest = sha256(encode_observation(INSTRUMENT, value)).hexdigest()
    return StoredAuxiliary(INSTRUMENT, value, revision, digest, previous)


class MemorySource:
    def __init__(self, rows, destination=DESTINATION):
        self.rows, self.destination = list(rows), destination
        self.checks, self.calls = [], []

    def check_destination(self, expected):
        self.checks.append(expected)
        if expected != self.destination:
            raise ValidationError("wrong destination")

    def get_auxiliary(self, instrument, channel, source, opening, *, as_of_ms):
        assert self.checks, "DB lookup preceded destination validation"
        self.calls.append((instrument, channel, source, opening, as_of_ms))
        return self.get(instrument, channel, source, opening, as_of_ms=as_of_ms)

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        values = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and row.observation.window_start_ms == opening
            and row.observation.available_at_ms <= as_of_ms
        ]
        return max(values, key=lambda row: row.revision, default=None)

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        values = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and event_before_ms - max_age_ms <= row.observation.event_time_ms < event_before_ms
            and row.observation.available_at_ms <= as_of_ms
        ]
        return max(
            values, key=lambda row: (row.observation.event_time_ms, row.revision), default=None
        )


@pytest.fixture
def case(tmp_path):
    rows = [
        stored(
            observation(
                "open_interest",
                START - 60000,
                AVAILABLE,
                "open_interest_history",
                (("METRIC_OPENINTEREST", Decimal("1000.00")), ("SOURCE_PERIOD_MS", 300000)),
            )
        ),
        stored(
            observation(
                "long_short_ratio",
                START,
                AVAILABLE + 1,
                "long_short_ratio",
                (("METRIC_LONGSHORTRATIO", Decimal("1.20")), ("SOURCE_PERIOD_MS", 300000)),
            )
        ),
    ]
    for index in range(3):
        rows.append(
            stored(
                observation(
                    "trade_flow",
                    START + (index + 1) * 60000,
                    AVAILABLE + 10,
                    "aggTrade",
                    (
                        ("AGGTRADE_NOTIONALBUY", Decimal("3.0003")),
                        ("AGGTRADE_NOTIONALSELL", Decimal(0)),
                    ),
                )
            )
        )
    source = MemorySource(rows)
    policy = InputPolicy(DESTINATION, ("metric", "trade_flow"))
    builder = MarketFrameBuilder(source)
    frames = tuple(
        builder.prepare(
            Candle(
                INSTRUMENT,
                START + index * 60000,
                START + (index + 1) * 60000,
                *(Decimal(v) for v in (100, 102, 99, 101, 2)),
            ),
            as_of_ms=AVAILABLE + 20,
            channels=policy.channels,
            retrospective=index < 2,
        )
        for index in range(3)
    )
    writer = AnalysisInputManifest(tmp_path)
    reference = writer.stage(IDENTITY, "2" * 32, 1, frames, policy)
    reader = AnalysisInputManifest(tmp_path)
    yield writer, reader, reference, source, policy, frames
    writer.close()
    reader.close()


def run(case, reference=None, source=None, policy=None, identity=IDENTITY):
    _, reader, head, selected, expected, _ = case
    return replay(
        reader,
        reference or head,
        source or selected,
        expected_analysis_id=identity,
        expected_policy=policy or expected,
    )


def rewrite(case, *, chunk_change=None, head_change=None):
    writer, reader, reference, _, _, _ = case
    head = reader.read(reference)
    if chunk_change:
        chunk = reader.read(head["source"])
        chunk_change(chunk)
        head["source"] = writer._append(chunk).document()
    if head_change:
        head_change(head)
    return writer._append(head)


def test_exact_replay_queries_each_deduplicated_pin_at_its_original_availability(case):
    _, reader, reference, source, policy, frames = case
    before = {path: path.read_bytes() for path in reader.root.rglob("*.jsonl")}
    assert run(case) == frames
    assert source.checks == [policy.destination_id]
    assert (
        len(source.calls) == 5
    )  # Three minute flow pins and two native points shared by three frames.
    assert {call[-1] for call in source.calls} == {AVAILABLE, AVAILABLE + 1, AVAILABLE + 10}
    assert all(call[-1] < frames[0].frame.as_of_ms for call in source.calls)
    assert reader.owner is None and reader.stream is None
    assert before == {path: path.read_bytes() for path in reader.root.rglob("*.jsonl")}
    head = validate_head(reader, reference, expected_analysis_id=IDENTITY, expected_policy=policy)
    chunk = reader.read(head["source"])
    assert all(set(pin) == PIN_KEYS and "payload" not in pin for pin in chunk["pins"])


def test_newer_revision_is_not_substituted_for_original_pin(case):
    source = case[3]
    first = source.rows[0]
    newer = replace(
        first.observation,
        available_at_ms=AVAILABLE + 100,
        values=(("METRIC_OPENINTEREST", Decimal(2000)), ("SOURCE_PERIOD_MS", 300000)),
    )
    source.rows.append(stored(newer, revision=2, previous=first.checksum))
    assert run(case) == case[-1]
    assert len(source.calls) == 5


@pytest.mark.parametrize("damage", ["missing", "wrong_revision", "same_metadata_changed_payload"])
def test_missing_or_changed_real_pin_is_rejected_without_manifest_changes(case, damage):
    source = case[3]
    if damage == "missing":
        source.rows.pop(0)
    elif damage == "wrong_revision":
        source.rows[0] = replace(source.rows[0], revision=2, previous_checksum="b" * 64)
    else:
        first = source.rows[0]
        source.rows[0] = replace(
            first,
            observation=replace(first.observation, values=(("METRIC_OPENINTEREST", Decimal(999)),)),
        )
    before = case[1].read(case[2])
    with pytest.raises(StorageError):
        run(case)
    assert case[1].read(case[2]) == before


def test_wrong_destination_precedes_every_native_lookup(case):
    source = MemorySource(case[3].rows, destination="b" * 64)
    with pytest.raises(ValidationError, match="destination"):
        run(case, source=source)
    assert source.calls == []


def test_actual_source_destination_check_binds_metadata_without_connecting(tmp_path, monkeypatch):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "not-logged")
    monkeypatch.setattr(source_module, "load_database_settings", lambda _: settings)
    source = source_module.CommittedAnalysisFrameSource(
        tmp_path,
        connect=lambda _: pytest.fail("wrong destination opened DB"),
        store_factory=lambda _: pytest.fail("wrong destination opened store"),
    )
    with pytest.raises(ValidationError):
        source.check_destination("b" * 64)
    assert source.destination_id == database_destination_id(settings)
    source.close()


@pytest.mark.parametrize(
    "damage", ["extra", "wrong_ca", "policy", "bool_generation", "range", "source_kind"]
)
def test_valid_checksum_does_not_allow_wrong_head_identity_schema_or_range(case, damage):
    def mutate(head):
        if damage == "extra":
            head["unknown"] = 1
        elif damage == "wrong_ca":
            head["analysis_id"] = "f" * 32
        elif damage == "policy":
            head["policy_hash"] = "f" * 64
        elif damage == "bool_generation":
            head["generation"] = True
        elif damage == "range":
            head["end_ms"] += 60000
        else:
            head["kind"] = "source_chunk"

    reference = rewrite(case, head_change=mutate)
    with pytest.raises(ValidationError):
        run(case, reference)
    assert case[3].calls == []


@pytest.mark.parametrize(
    "damage",
    [
        "extra",
        "policy",
        "bool_time",
        "nonfinite",
        "numeric_decimal",
        "pin_index_bool",
        "pin_index_missing",
        "pin_duplicate",
        "pin_revision_bool",
        "pin_unavailable",
        "digest",
        "frame_gap",
        "unused_pin",
    ],
)
def test_valid_checksum_chunk_semantic_damage_is_rejected(case, damage):
    def mutate(chunk):
        frame = chunk["frames"][0]
        if damage == "extra":
            chunk["unknown"] = 1
        elif damage == "policy":
            chunk["policy"]["maximum_metric_age_ms"] += 1
        elif damage == "bool_time":
            frame["candle"]["open_ms"] = True
        elif damage == "nonfinite":
            frame["candle"]["volume"] = "NaN"
        elif damage == "numeric_decimal":
            frame["candle"]["close"] = 101
        elif damage == "pin_index_bool":
            frame["pins"][0] = True
        elif damage == "pin_index_missing":
            frame["pins"][0] = 9999
        elif damage == "pin_duplicate":
            frame["pins"].append(frame["pins"][0])
        elif damage == "pin_revision_bool":
            chunk["pins"][0]["revision"] = True
        elif damage == "pin_unavailable":
            frame["as_of_ms"] = AVAILABLE
        elif damage == "digest":
            frame["frame_digest"] = "f" * 64
        elif damage == "frame_gap":
            chunk["frames"][1]["candle"]["open_ms"] += 60000
        else:
            chunk["pins"].append(deepcopy(chunk["pins"][0]))

    reference = rewrite(case, chunk_change=mutate)
    with pytest.raises((ValidationError, StorageError)):
        run(case, reference)


def test_source_bytes_are_verified_by_manifest_reader_before_replay(case):
    _, reader, reference, source, _, _ = case
    head = reader.read(reference)
    ref = head["source"]
    path = reader.root / "segments" / (ref["segment"] + ".jsonl")
    raw = bytearray(path.read_bytes())
    raw[ref["offset"] + 1] ^= 1
    path.write_bytes(raw)
    with pytest.raises(StorageError):
        run(case)
    assert source.calls == []


def test_immediate_chain_edge_checks_contiguity_and_generation(case):
    writer, reader, _, source, policy, frames = case
    prior = writer.stage(IDENTITY, "3" * 32, 1, frames[:1], policy)
    current = writer.stage(IDENTITY, "4" * 32, 2, frames[1:], policy, previous=prior)
    assert (
        replay(reader, current, source, expected_analysis_id=IDENTITY, expected_policy=policy)
        == frames[1:]
    )
    head = reader.read(current)
    head["generation"] = 0
    invalid = writer._append(head)
    with pytest.raises(ValidationError, match="generation"):
        run(case, invalid)
    head = reader.read(current)
    head["start_ms"] -= 60000
    invalid = writer._append(head)
    with pytest.raises(ValidationError, match="discontinuous"):
        run(case, invalid)


def test_ohlcv_only_replay_checks_destination_but_queries_no_auxiliary_rows(tmp_path):
    source = MemorySource([])
    policy = InputPolicy(DESTINATION, ())
    frame = MarketFrameBuilder(None).prepare(
        Candle(INSTRUMENT, START, START + 60000, *(Decimal(v) for v in (100, 102, 99, 101, 2))),
        as_of_ms=AVAILABLE,
        retrospective=True,
    )
    manifest = AnalysisInputManifest(tmp_path)
    try:
        reference = manifest.stage(IDENTITY, "2" * 32, 0, (frame,), policy)
        assert replay(
            manifest, reference, source, expected_analysis_id=IDENTITY, expected_policy=policy
        ) == (frame,)
        assert source.checks == [DESTINATION] and source.calls == []
    finally:
        manifest.close()


def test_late_downloaded_retrospective_facts_keep_actual_availability_on_replay(case):
    writer, reader, _, original, policy, frames = case
    delay = 86400000
    source = MemorySource(
        [
            stored(
                replace(row.observation, available_at_ms=row.observation.available_at_ms + delay)
            )
            for row in original.rows
        ]
    )
    builder = MarketFrameBuilder(source)
    selected = tuple(
        builder.prepare(
            row.frame.candle,
            as_of_ms=row.frame.as_of_ms + delay,
            channels=policy.channels,
            retrospective=True,
        )
        for row in frames
    )
    reference = writer.stage(IDENTITY, "5" * 32, 2, selected, policy)
    restored = replay(
        reader, reference, source, expected_analysis_id=IDENTITY, expected_policy=policy
    )
    assert restored == selected and all(row.retrospective for row in restored)
    assert all(
        pin.observation.available_at_ms >= AVAILABLE + delay for row in restored for pin in row.pins
    )
    assert len(source.calls) == 5 and all(call[-1] >= AVAILABLE + delay for call in source.calls)


def test_original_missing_selection_is_not_silently_upgraded_when_new_data_arrives(case):
    writer, reader, _, source, policy, frames = case
    missing = MarketFrameBuilder(MemorySource([])).prepare(
        frames[0].frame.candle, as_of_ms=frames[0].frame.as_of_ms, channels=policy.channels
    )
    reference = writer.stage(IDENTITY, "6" * 32, 2, (missing,), policy)
    restored = replay(
        reader, reference, source, expected_analysis_id=IDENTITY, expected_policy=policy
    )
    assert restored == (missing,)
    assert all(
        value.quality == "missing" and value.values == () for value in restored[0].frame.inputs
    )
    assert restored[0].pins == () and source.calls == []
