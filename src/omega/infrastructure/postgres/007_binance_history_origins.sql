-- Public data availability is tracked per instrument and native channel.
CREATE TABLE market.binance_history_origins (
    instrument TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('kline','agg_trade','depth','metrics')),
    discovery_state TEXT NOT NULL CHECK (discovery_state IN ('verified','unavailable','retry')),
    first_ms BIGINT CHECK (first_ms BETWEEN 0 AND 253402300799000),
    rest_first_ms BIGINT CHECK (rest_first_ms BETWEEN 0 AND 253402300799000),
    vision_first_ms BIGINT CHECK (vision_first_ms BETWEEN 0 AND 253402300799000),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    checked_ms BIGINT NOT NULL CHECK (checked_ms BETWEEN 0 AND 253402300799000),
    next_check_ms BIGINT CHECK (next_check_ms >= checked_ms),
    PRIMARY KEY (instrument,channel),
    CHECK ((discovery_state = 'verified' AND first_ms IS NOT NULL AND next_check_ms IS NULL)
        OR (discovery_state <> 'verified' AND next_check_ms IS NOT NULL))
);

-- Older bars live under a separate immutable series boundary during backfill.
CREATE TABLE market.binance_kline_history (
    instrument TEXT PRIMARY KEY,
    live_series_id TEXT NOT NULL REFERENCES market.bar_series(series_id),
    historical_series_id TEXT NOT NULL REFERENCES market.bar_series(series_id),
    next_ms BIGINT NOT NULL CHECK (next_ms >= 0 AND next_ms % 60000 = 0),
    end_ms BIGINT NOT NULL CHECK (end_ms >= next_ms AND end_ms % 60000 = 0),
    mirror_ms BIGINT NOT NULL CHECK (mirror_ms >= end_ms AND mirror_ms % 60000 = 0),
    observed BIGINT NOT NULL DEFAULT 0 CHECK (observed >= 0),
    missing BIGINT NOT NULL DEFAULT 0 CHECK (missing >= 0),
    updated_ms BIGINT
);
CREATE TABLE market.binance_kline_history_gaps (
    instrument TEXT NOT NULL REFERENCES market.binance_kline_history(instrument),
    start_ms BIGINT NOT NULL CHECK (start_ms >= 0 AND start_ms % 60000 = 0),
    end_ms BIGINT NOT NULL CHECK (end_ms > start_ms AND end_ms % 60000 = 0),
    PRIMARY KEY (instrument,start_ms,end_ms)
);
