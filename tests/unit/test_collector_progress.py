from math import isclose

from omega.runtime.collector_progress import BackfillProgress, TransferMeter


def test_byte_rates_use_one_elapsed_period_despite_other_writes_and_idle_time():
    now = [100.0]
    meter = TransferMeter(clock=lambda: now[0])
    now[0] = 105.5
    meter.record("backfill", "kline", 5, 1000)
    now[0] = 110.0
    meter.record("live", "trade_flow", 1, 300)
    now[0] = 125.0
    meter.record("backfill", "open_interest", 1, 700)
    meter.record("backfill", "long_short_ratio", 1, 500)
    meter.record("repair", "trade_flow", 1, 200)
    now[0] = 160.5
    meter.record("backfill", "depth", 4, 4000)
    windows, samples, _ = meter.snapshot()
    minute = windows["60"]
    assert minute["elapsed_seconds"] == 60
    assert minute["backfill"]["total"] == {"bytes": 2200, "bytes_per_second": 36.67}
    assert minute["backfill"]["channels"]["depth"]["bytes"] == 0
    assert minute["live"]["channels"]["agg_trade"]["bytes_per_second"] == 5
    assert minute["combined"]["total"]["bytes"] == 2700
    assert samples["metrics"] == (2, 1200)

    now[0] = 220.9
    minute = meter.snapshot()[0]["60"]
    assert minute["live"]["total"]["bytes"] == 0
    assert minute["backfill"]["total"]["bytes"] == 4000
    assert isclose(minute["backfill"]["total"]["bytes_per_second"], 66.67)


def test_backfill_remaining_bytes_use_measured_sizes_and_native_slot_capacity(
    tmp_path, monkeypatch
):
    now = [0.0]
    meter = TransferMeter(clock=lambda: now[0])
    meter.record("backfill", "kline", 10, 10000)
    meter.record("backfill", "open_interest", 1, 700)
    meter.record("backfill", "long_short_ratio", 1, 300)
    progress = BackfillProgress(tmp_path, None, (), meter)
    ranges = {
        "kline": (100, 40, 3, 0),
        "agg_trade": (200, 200, 0, 0),
        "depth": (0, 0, 0, 0),
        "metrics": (20, 5, 1, 0),
    }
    monkeypatch.setattr(progress, "_read_ranges", lambda: (ranges, 1))
    now[0] = 3.0
    status = progress.snapshot()
    assert status["channels"]["kline"]["remaining_bytes_estimate"] == 60_000
    assert status["channels"]["metrics"]["remaining_bytes_estimate"] == 15_000
    assert status["total"]["remaining_bytes_estimate"] == 75_000
    assert status["total"]["verified_percent"] == 76.5625
    assert status["total"]["unresolved_slots"] == 4
    ranges["metrics"] = (20, 5, 1, 1)
    assert progress.snapshot()["total"]["remaining_bytes_estimate"] is None
