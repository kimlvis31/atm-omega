"""Stored auxiliary display semantics, with no database or trading resources."""

from dataclasses import replace
from decimal import Decimal, localcontext

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.application.auxiliary_series import (
    KINDS,
    MINUTE,
    UNITS,
    project_auxiliary_series,
    validate_parameters,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.chart_observations import ChartObservationsView

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
NOW = 20 * MINUTE


def spec(kind="OPENINTEREST", **changes):
    return {
        "instrument": INSTRUMENT.key,
        "kind": kind,
        "start_ms": 0,
        "end_ms": 5 * MINUTE,
        "period_minutes": 1,
        **changes,
    }


def row(
    kind="OPENINTEREST",
    minute=0,
    values=None,
    *,
    quality="observed",
    coverage=Decimal(1),
    available=NOW,
    source=None,
    revision=1,
):
    if values is None:
        values = {"METRIC_OPENINTEREST": Decimal("123.40")}
    observation = AuxiliaryInput(
        KINDS[kind][0],
        tuple(values.items()),
        minute * MINUTE,
        (minute + 1) * MINUTE,
        minute * MINUTE + 10,
        available,
        quality,
        coverage,
        source or INSTRUMENT.key + ":" + KINDS[kind][1],
    )
    return StoredAuxiliary(
        INSTRUMENT,
        observation,
        revision,
        observation_checksum(encode_observation(INSTRUMENT, observation)),
        None if revision == 1 else "a" * 64,
    )


def project(rows, kind="OPENINTEREST", **changes):
    return project_auxiliary_series(
        spec(kind, **changes),
        tuple(rows),
        source=INSTRUMENT.key + ":" + KINDS[kind][1],
        as_of_ms=NOW,
    )


def trades(minute=0, **changes):
    values = {
        name: 1 if unit == "count" else Decimal("0.12345678901234567890123456789")
        for name, unit in UNITS["AGGTRADE"].items()
    }
    return row("AGGTRADE", minute, values, **changes)


def depth(minute=0, *, missing=None, **changes):
    values = {
        f"DEPTH_{side}{index}": Decimal(index + 1)
        for side in ("BIDS", "ASKS")
        for index in range(6)
    }
    if missing:
        del values[missing]
    return row("DEPTH", minute, values, **changes)


def test_native_points_have_gaps_without_carry_or_manufactured_zero():
    result = project([row(minute=3), row(minute=0)])
    assert result["state"] == "partial"
    assert result["series"]["METRIC_OPENINTEREST"] == [[0, "123.40"], [3 * MINUTE, "123.40"]]
    assert result["series"]["METRIC_OPENINTERESTVALUE"] == []
    assert result["gaps"] == [[MINUTE, 3 * MINUTE], [4 * MINUTE, 5 * MINUTE]]
    assert result["quality"] == {
        "expected_bars": 5,
        "observed_bars": 2,
        "missing_bars": 3,
        "partial_bars": 0,
    }
    assert result["scope"] == "retrospective_auxiliary_chart"
    assert [item["open_time_ms"] for item in result["lineage"]] == [0, 3 * MINUTE]


def test_native_group_uses_last_actual_sample_and_keeps_original_lineage():
    first, last = (
        row(minute=0),
        row(
            minute=3, values={"METRIC_OPENINTEREST": Decimal(7)}, revision=2, available=10 * MINUTE
        ),
    )
    result = project([first, last], period_minutes=5)
    assert result["series"]["METRIC_OPENINTEREST"] == [[0, "7"]]
    assert result["quality"]["observed_bars"] == 1
    assert result["lineage"][1] == {
        "open_time_ms": 3 * MINUTE,
        "available_at_ms": 10 * MINUTE,
        "revision": 2,
        "checksum": last.checksum,
        "quality": "observed",
        "coverage": "1",
    }


def test_agg_sum_is_exact_in_low_precision_caller_context():
    with localcontext() as context:
        context.prec = 3
        result = project([trades(index) for index in range(5)], "AGGTRADE", period_minutes=5)
    assert result["state"] == "ready"
    assert result["series"]["AGGTRADE_QUANTITYBUY"] == [[0, "0.61728394506172839450617283945"]]
    assert result["series"]["AGGTRADE_NTRADESSELL"] == [[0, "5"]]


@pytest.mark.parametrize(
    "rows",
    [
        lambda: [trades(0), trades(1)],
        lambda: [
            trades(index, quality="partial", coverage=Decimal(".5"))
            if index == 2
            else trades(index)
            for index in range(5)
        ],
    ],
)
def test_agg_incomplete_group_has_no_sum_or_zero(rows):
    result = project(rows(), "AGGTRADE", period_minutes=5)
    assert all(not points for points in result["series"].values())
    assert result["quality"]["partial_bars"] == 1
    assert result["gaps"] == [[0, 5 * MINUTE]]


def test_agg_unaligned_edge_is_partial_instead_of_summing_only_requested_minutes():
    result = project(
        [trades(index) for index in range(1, 5)], "AGGTRADE", start_ms=MINUTE, period_minutes=5
    )
    assert result["series"]["AGGTRADE_NOTIONALBUY"] == []
    assert result["gaps"] == [[MINUTE, 5 * MINUTE]]


@pytest.mark.parametrize(
    "kind,stored",
    [
        ("OPENINTEREST", lambda: row(minute=1)),
        (
            "LONGSHORTRATIO",
            lambda: row(
                "LONGSHORTRATIO",
                minute=1,
                values={
                    "METRIC_LONGSHORTRATIO": Decimal(2),
                    "METRIC_LONGACCOUNT": Decimal(".6"),
                    "METRIC_SHORTACCOUNT": Decimal(".4"),
                },
            ),
        ),
        ("DEPTH", lambda: depth(1)),
        ("FUNDING", lambda: row("FUNDING", minute=1, values={"FUNDING_RATE": Decimal(".0001")})),
    ],
)
@pytest.mark.parametrize("start,end", [(MINUTE, 5 * MINUTE), (0, 4 * MINUTE)])
def test_all_kinds_omit_partial_edge_bucket_points_but_retain_raw_lineage(kind, stored, start, end):
    result = project([stored()], kind, start_ms=start, end_ms=end, period_minutes=5)
    assert all(not values for values in result["series"].values())
    assert result["gaps"] == [[start, end]]
    assert result["quality"] == {
        "expected_bars": 1,
        "observed_bars": 0,
        "missing_bars": 1,
        "partial_bars": 1,
    }
    assert result["lineage"][0]["open_time_ms"] == MINUTE


def test_observed_zero_trades_are_preserved():
    values = {
        name: 0 if unit == "count" else Decimal(0) for name, unit in UNITS["AGGTRADE"].items()
    }
    result = project([row("AGGTRADE", values=values)], "AGGTRADE", end_ms=MINUTE)
    assert result["state"] == "ready"
    assert result["series"]["AGGTRADE_NOTIONALBUY"] == [[0, "0"]]


def test_depth_uses_last_complete_snapshot_quote_notional_bins():
    result = project(
        [depth(), depth(2), depth(4, missing="DEPTH_BIDS5")], "DEPTH", period_minutes=5
    )
    assert result["series"] == {"DEPTH_BIDS": [[0, "21"]], "DEPTH_ASKS": [[0, "21"]]}
    assert set(result["units"].values()) == {"quote_asset"}
    assert result["quality"]["partial_bars"] == 1
    assert "heatmap" in " ".join(result["warnings"])


def test_incomplete_depth_is_absent_and_never_a_partial_sum():
    result = project([depth(missing="DEPTH_ASKS3")], "DEPTH", end_ms=MINUTE)
    assert result["series"] == {"DEPTH_BIDS": [], "DEPTH_ASKS": []}


def test_signed_funding_and_ratio_units_are_preserved():
    result = project([row("FUNDING", values={"FUNDING_RATE": Decimal("-.0001")})], "FUNDING")
    assert result["series"]["FUNDING_RATE"] == [[0, "-0.0001"]]
    result = project(
        [
            row(
                "LONGSHORTRATIO",
                values={
                    "METRIC_LONGSHORTRATIO": Decimal(2),
                    "METRIC_LONGACCOUNT": Decimal(".6"),
                    "METRIC_SHORTACCOUNT": Decimal(".4"),
                },
            )
        ],
        "LONGSHORTRATIO",
    )
    assert result["units"]["METRIC_LONGSHORTRATIO"] == "ratio"
    assert result["units"]["METRIC_LONGACCOUNT"] == "fraction"


@pytest.mark.parametrize(
    "change",
    [
        {"start_ms": True},
        {"end_ms": NOW + MINUTE},
        {"period_minutes": 0},
        {"source": "bad\nsource"},
        {"source": "x" * 97},
        {"instrument": "binance:usdm:ethusdt"},
        {"unexpected": 1},
    ],
)
def test_invalid_parameters_are_rejected_before_store_creation(tmp_path, change):
    view = ChartObservationsView(tmp_path, lambda: pytest.fail("no I/O expected"), lambda: NOW)
    with pytest.raises(ValidationError):
        view.read(spec(**change))


@pytest.mark.parametrize(
    "change",
    [{"period_minutes": 10080}, {"end_ms": 10001 * MINUTE}, {"instrument": "binance:spot:ETHUSDT"}],
)
def test_valid_unsupported_capabilities_do_not_open_store(tmp_path, change):
    view = ChartObservationsView(
        tmp_path, lambda: pytest.fail("no I/O expected"), lambda: 20000 * MINUTE
    )
    result = view.read(spec(**change))
    assert result["state"] == "unsupported"
    assert result["warnings"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: replace(value, checksum="f" * 64),
        lambda value: replace(value, instrument=Instrument("binance", Market.USDM, "BTCUSDT")),
        lambda value: row(available=NOW + 1),
        lambda value: row(source="foreign"),
        lambda value: row(minute=5),
    ],
)
def test_projection_rejects_corrupt_foreign_out_of_range_or_future_rows(mutate):
    with pytest.raises(ProtocolError):
        project([mutate(row())])


def test_projection_rejects_duplicate_minute_revisions():
    with pytest.raises(ProtocolError):
        project([row(), row(revision=2)])


@pytest.mark.parametrize(
    "values",
    [
        {"METRIC_OPENINTEREST": -1},
        {"METRIC_OPENINTEREST": Decimal(-1)},
        {"METRIC_OPENINTEREST": Decimal("1e999")},
        {"METRIC_OPENINTEREST": 2},
    ],
)
def test_wrong_field_dtypes_or_unbounded_values_are_rejected(values):
    with pytest.raises(ProtocolError):
        project([row(values=values)])


class Store:
    def __init__(self, rows=()):
        self.rows, self.calls, self.closed = rows, [], 0

    def read_ranges(self, ranges, *, as_of_ms, limit):
        self.calls.append((ranges, as_of_ms, limit))
        return tuple(value for value in self.rows if value.observation.source == ranges[0].source)

    def close(self):
        self.closed += 1


def test_view_constructor_idle_and_read_owns_store_with_cutoff_and_fallback(tmp_path):
    current = row(source=INSTRUMENT.key + ":open_interest")
    store, creates = Store((current,)), []
    view = ChartObservationsView(tmp_path, lambda: creates.append(1) or store, lambda: NOW)
    assert not creates
    result = view.read(spec())
    assert creates == [1] and store.closed == 1
    assert len(store.calls) == 2
    assert all(call[1:] == (NOW, 10000) for call in store.calls)
    assert result["source"] == current.observation.source
    assert result["series"]["METRIC_OPENINTEREST"] == [[0, "123.40"]]


def test_explicit_source_is_not_replaced_with_default(tmp_path):
    store = Store()
    result = ChartObservationsView(tmp_path, lambda: store, lambda: NOW).read(
        spec(source="explicit")
    )
    assert result["state"] == "unavailable"
    assert len(store.calls) == 1 and result["source"] == "explicit"


def test_history_with_partial_rows_is_not_silently_replaced(tmp_path):
    store = Store((row(quality="partial", coverage=Decimal(".5")),))
    result = ChartObservationsView(tmp_path, lambda: store, lambda: NOW).read(spec())
    assert result["state"] == "partial" and len(store.calls) == 1


def test_read_error_redacts_details_and_closes_store(tmp_path):
    class Failing(Store):
        def read_ranges(self, *args, **kwargs):
            raise StorageError("secret credential should not escape")

    store = Failing()
    result = ChartObservationsView(tmp_path, lambda: store, lambda: NOW).read(spec())
    assert store.closed == 1 and result["state"] == "unavailable"
    assert "secret" not in str(result)


def test_missing_database_configuration_is_unavailable(tmp_path):
    result = ChartObservationsView(tmp_path, clock_ms=lambda: NOW).read(spec())
    assert result["state"] == "unavailable"


def test_close_failure_cannot_publish_a_ready_snapshot(tmp_path):
    class FailingClose(Store):
        def close(self):
            raise StorageError("private cleanup details")

    store = FailingClose((row(),))
    result = ChartObservationsView(tmp_path, lambda: store, lambda: NOW).read(spec(end_ms=MINUTE))
    assert result["state"] == "unavailable"
    assert "private" not in str(result)


def test_injected_revision_contract_is_revalidated():
    value = row()
    object.__setattr__(value, "revision", True)
    with pytest.raises(ValidationError):
        project([value])


@pytest.mark.parametrize(
    "quality,coverage", [("carried", "1"), ("missing", "0"), ("partial", ".5")]
)
def test_unusable_native_rows_do_not_create_points(quality, coverage):
    values = {} if quality == "missing" else {"METRIC_OPENINTEREST": Decimal(3)}
    result = project(
        [row(values=values, quality=quality, coverage=Decimal(coverage))], end_ms=MINUTE
    )
    assert result["series"]["METRIC_OPENINTEREST"] == []
    assert result["gaps"] == [[0, MINUTE]]
    assert result["lineage"][0]["quality"] == quality


def test_validate_copies_parameters():
    value = spec()
    result = validate_parameters(value, as_of_ms=NOW)
    assert result == value and result is not value


def test_eta_chart_reads_mapped_minutes_inside_a_bucket_with_both_native_checksums(tmp_path):
    class MappedStore(Store):
        def read_eta_metric_minutes(self, instrument, start_ms, end_ms, *, as_of_ms):
            assert (instrument, start_ms, end_ms, as_of_ms) == (
                INSTRUMENT.key,
                MINUTE,
                5 * MINUTE,
                NOW,
            )
            return tuple(
                {
                    "open_ms": opening,
                    "close_ms": opening + MINUTE,
                    "metric_time_ms": 0,
                    "available_ms": 2 * MINUTE,
                    "metrics": {
                        "METRIC_OPENINTEREST": "123.40",
                        "METRIC_OPENINTERESTVALUE": "200",
                        "METRIC_LONGSHORTRATIO": "2",
                    },
                    "open_interest_revision": 1,
                    "long_short_revision": 2,
                    "open_interest_checksum": "a" * 64,
                    "long_short_checksum": "b" * 64,
                }
                for opening in range(start_ms, end_ms, MINUTE)
            )

    store = MappedStore()
    view = ChartObservationsView(tmp_path, lambda: store, lambda: NOW)
    result = view.read(spec(start_ms=MINUTE))
    assert result["state"] == "ready"
    assert result["series"]["METRIC_OPENINTEREST"] == [
        [opening, "123.40"] for opening in range(MINUTE, 5 * MINUTE, MINUTE)
    ]
    (original,) = result["lineage"]
    assert original["open_time_ms"] == 0
    assert original["available_at_ms"] == 2 * MINUTE
    assert original["open_interest_checksum"] == "a" * 64
    assert original["long_short_checksum"] == "b" * 64
    assert store.calls == [] and store.closed == 1


def test_eta_chart_does_not_display_unpaired_native_statistics(tmp_path):
    class NoPairStore(Store):
        def read_eta_metric_minutes(self, *args, **kwargs):
            return ()

    store = NoPairStore(
        (row(values={"METRIC_OPENINTEREST": Decimal(2), "SOURCE_PERIOD_MS": 5 * MINUTE}),)
    )
    view = ChartObservationsView(tmp_path, lambda: store, lambda: NOW)
    result = view.read(spec())
    assert result["gaps"] == [[0, 5 * MINUTE]]
    assert result["series"]["METRIC_OPENINTEREST"] == []
