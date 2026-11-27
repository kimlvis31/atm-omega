from dataclasses import replace
from decimal import Decimal

import pytest

from omega.domain.bar_schedule import BarSchedule
from omega.domain.bar_series import BarSeries, SeriesCandle, SeriesObservation
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market

STOCK = Instrument("binance", Market.CASH_EQUITY, "AAPL")
OPEN = 360_000
FIVE = 300_000


def test_native_one_minute_takes_precedence_and_metadata_versions_bind_identity():
    schedule = BarSchedule(FIVE, ((OPEN, OPEN + 2 * FIVE),))
    with pytest.raises(ValidationError, match="shortest available"):
        BarSeries(STOCK, "equity", (60_000, FIVE), schedule, OPEN, "calendar-1")
    series = BarSeries(STOCK, "equity", (FIVE,), schedule, OPEN, "calendar-1")
    assert BarSeries.from_document(series.document()) == series
    assert replace(series, calendar_version="calendar-2").series_id != series.series_id
    assert replace(series, effective_from_ms=OPEN + FIVE).series_id != series.series_id


def test_non_epoch_bar_and_receipts_only_accept_series_geometry():
    schedule = BarSchedule(FIVE, ((OPEN, OPEN + FIVE),))
    series = BarSeries(STOCK, "equity", (FIVE,), schedule, OPEN, "calendar-1")
    price = Decimal("100.1234567890123456789")
    candle = SeriesCandle(series, OPEN, OPEN + FIVE, price, price, price, price, Decimal("0"))
    observation = SeriesObservation(
        candle, "websocket", OPEN + FIVE, "original", "websocket", OPEN + FIVE - 1
    )
    assert observation.candle.close == price
    with pytest.raises(ValidationError, match="series interval"):
        replace(candle, open_time_ms=OPEN + FIVE, close_time_ms=OPEN + 2 * FIVE)
    with pytest.raises(ValidationError, match="receipt"):
        replace(observation, received_at_ms=OPEN + FIVE - 1)
    with pytest.raises(ValidationError, match="provenance"):
        replace(observation, raw_source=None)
