"""Adapt the existing Binance 1m websocket/REST worker to durable base-bar series."""

from omega.domain.bar_schedule import BarSchedule
from omega.domain.bar_series import MAX_BAR_TIME_MS, BarSeries, SeriesCandle, SeriesObservation
from omega.domain.models import Candle
from omega.infrastructure.bar_series_store import PostgresBarSeriesStore

MINUTE = 60_000


def binance_minute_series(instruments, effective_from_ms):
    return {
        instrument: BarSeries(
            instrument, "binance-public", (MINUTE,), BarSchedule(), effective_from_ms
        )
        for instrument in instruments
    }


class BinanceMinuteSeriesStore:
    def __init__(self, settings, series, now_ms):
        self.series = series
        self.store = PostgresBarSeriesStore(settings)
        try:
            self.starts = {
                instrument: self.store.claim_live_series(series, now_ms)
                for instrument, series in self.series.items()
            }
        except BaseException:
            self.store.close()
            raise

    def put(self, observation):
        candle = observation.candle
        series = self.series[candle.instrument]
        if candle.open_time_ms < series.effective_from_ms:
            return False
        raw_source = observation.raw_source
        raw_received = observation.raw_received_at_ms
        if observation.raw is not None and raw_source is None and raw_received is None:
            raw_source = observation.source
            raw_received = observation.received_at_ms
        return self.store.put_live_observation(
            SeriesObservation(
                SeriesCandle(
                    series,
                    candle.open_time_ms,
                    candle.close_time_ms,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                ),
                observation.source,
                observation.received_at_ms,
                observation.raw,
                raw_source,
                raw_received,
            )
        )

    def get(self, instrument, open_time_ms):
        series = self.series[instrument]
        rows = self.store.read_observations(
            series.series_id,
            open_time_ms,
            open_time_ms + MINUTE,
            as_of_ms=MAX_BAR_TIME_MS,
            limit=1,
            include_raw=False,
        )
        if not rows:
            return None
        candle = rows[0].candle
        return Candle(
            instrument,
            candle.open_time_ms,
            candle.close_time_ms,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
        )

    def close(self):
        self.store.close()
