"""Adapt the existing Binance 1m websocket/REST worker to durable base-bar series."""

import json
import time

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
    def __init__(self, settings, series, now_ms, *, on_commit=None):
        self.series = series
        self.store = PostgresBarSeriesStore(settings, on_commit=on_commit)
        try:
            self.starts = {
                instrument: self.store.claim_live_series(series, now_ms)
                for instrument, series in self.series.items()
            }
        except BaseException:
            self.store.close()
            raise

    def _series_observation(self, observation):
        candle = observation.candle
        series = self.series[candle.instrument]
        raw_source = observation.raw_source
        raw_received = observation.raw_received_at_ms
        if observation.raw is not None and raw_source is None and raw_received is None:
            raw_source = observation.source
            raw_received = observation.received_at_ms
        return SeriesObservation(
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

    @staticmethod
    def _minute(candle):
        return Candle(
            candle.series.instrument,
            candle.open_time_ms,
            candle.close_time_ms,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
        )

    def put(self, observation):
        candle = observation.candle
        if candle.open_time_ms < self.series[candle.instrument].effective_from_ms:
            return False
        return self.store.put_live_observation(self._series_observation(observation))

    def put_batch(self, observations, *, missing_only=False):
        candles = [row.candle for row in observations]
        in_scope = [
            index
            for index, candle in enumerate(candles)
            if candle.open_time_ms >= self.series[candle.instrument].effective_from_ms
        ]
        if in_scope:
            rows = tuple(self._series_observation(observations[index]) for index in in_scope)
            started = time.monotonic()
            stored = self.store.put_live_batch(rows, missing_only=missing_only)
            print(
                json.dumps(
                    {
                        "collector_write": "kline_repair" if missing_only else "kline_stream",
                        "rows": len(rows),
                        "inserted": sum(inserted for inserted, _ in stored),
                        "db_ms": round((time.monotonic() - started) * 1000, 2),
                        "commit_lag_ms": time.time_ns() // 1_000_000
                        - min(row.received_at_ms for row in rows),
                    }
                ),
                flush=True,
            )
            for index, (_, candle) in zip(in_scope, stored, strict=True):
                candles[index] = self._minute(candle)
        return candles

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
        return self._minute(rows[0].candle)

    def close(self):
        self.store.close()
