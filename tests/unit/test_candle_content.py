"""Wire decimals and worker Candle values must identify the same scoped content."""

from decimal import Decimal, localcontext

import pytest

from omega.domain.candle_content import candle_content_pin
from omega.domain.errors import ValidationError

BTC = "binance:usdm:BTCUSDT"


def rows():
    return [(0, "100", "103", "99", "101", "0"), (60_000, 101, 104, 100, 102, 3)]


def test_published_canonical_wire_vector_matches_worker_decimals():
    wire = rows()
    worker = [tuple([row[0]] + [Decimal(value) for value in row[1:]]) for row in wire]
    expected = "21bcd3696832f4abbeb230fd2d4d6687ab556f51c40b38cb362568bf8e63156d"
    assert candle_content_pin(BTC, 0, 120_000, wire) == expected
    assert candle_content_pin(BTC, 0, 120_000, worker) == expected


def test_economic_decimal_spellings_and_signed_zero_do_not_change_the_pin():
    changed = [(0, "+1E+2", "103.000", ".99e2", "101.0", "-0.000")]
    assert candle_content_pin(BTC, 0, 60_000, rows()[:1]) == candle_content_pin(
        BTC, 0, 60_000, changed
    )


def test_unequal_exact_prices_remain_distinct_when_display_floats_collapse():
    first, second = "1000000000000000.01", "1000000000000000.02"
    assert float(first) == float(second)
    a = [(0, first, first, first, first, "1")]
    b = [(0, second, second, second, second, "1")]
    assert candle_content_pin(BTC, 0, 60_000, a) != candle_content_pin(BTC, 0, 60_000, b)


def test_decimal_context_precision_and_rounding_do_not_change_content():
    values = [(0, "1.234567890123456789", "2", "1", "1.5", "3")]
    expected = candle_content_pin(BTC, 0, 60_000, values)
    with localcontext() as context:
        context.prec = 2
        assert candle_content_pin(BTC, 0, 60_000, values) == expected


def test_missing_minutes_instrument_and_range_are_part_of_identity():
    original = candle_content_pin(BTC, 0, 120_000, rows())
    assert candle_content_pin(BTC, 0, 120_000, rows()[:1]) != original
    assert candle_content_pin("binance:spot:BTCUSDT", 0, 120_000, rows()) != original
    assert candle_content_pin(BTC, 0, 180_000, rows()) != original
    assert candle_content_pin(BTC, 0, 60_000, ()) != candle_content_pin(BTC, 0, 120_000, ())


@pytest.mark.parametrize(
    "value",
    [
        True,
        1.0,
        float("nan"),
        float("inf"),
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        "1e129",
        "1e-129",
        "9" * 129,
        "1" * 513,
        "1e1000000000",
        10**1000,
        "not-a-number",
        "",
        object(),
    ],
)
def test_invalid_or_unbounded_exact_numbers_are_rejected_before_hashing(value):
    with pytest.raises(ValidationError):
        candle_content_pin(BTC, 0, 60_000, [(0, value, 103, 99, 101, 0)])


@pytest.mark.parametrize(
    "values",
    [
        [(0, 100, 103, 99, 101, 0), (0, 100, 103, 99, 101, 0)],
        list(reversed(rows())),
        [(True, 100, 103, 99, 101, 0)],
        [(1, 100, 103, 99, 101, 0)],
        [(120_000, 100, 103, 99, 101, 0)],
        [(0, 100, 103, 99, 101)],
    ],
)
def test_order_identity_range_and_row_shape_are_strict(values):
    with pytest.raises(ValidationError):
        candle_content_pin(BTC, 0, 120_000, values)


@pytest.mark.parametrize(
    "instrument,start,end",
    [
        ("", 0, 60_000),
        ("x" * 257, 0, 60_000),
        (BTC + "\n", 0, 60_000),
        (BTC, False, 60_000),
        (BTC, 0, True),
        (BTC, 0, 1),
        (BTC, -60_000, 60_000),
        (BTC, 60_000, 60_000),
        (BTC, 0, 10_001 * 60_000),
    ],
)
def test_content_scope_rejects_invalid_or_unbounded_ranges(instrument, start, end):
    with pytest.raises(ValidationError):
        candle_content_pin(instrument, start, end, ())


def test_row_iterator_is_bounded_and_never_eagerly_consumed():
    consumed = []

    def untrusted():
        for index in range(20_000):
            consumed.append(index)
            yield (index * 60_000, 100, 103, 99, 101, 0)

    with pytest.raises(ValidationError):
        candle_content_pin(BTC, 0, 10_000 * 60_000, untrusted())
    assert len(consumed) == 10_001
