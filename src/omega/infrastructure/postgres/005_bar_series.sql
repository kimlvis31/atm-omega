-- Each immutable definition has its own candle identity, including its base interval.
CREATE TABLE market.bar_series (
    series_id TEXT PRIMARY KEY CHECK (series_id ~ '^[a-f0-9]{64}$'),
    definition JSONB NOT NULL CHECK (jsonb_typeof(definition) = 'object')
);

CREATE TABLE market.series_candles (
    series_id TEXT NOT NULL REFERENCES market.bar_series(series_id),
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    o NUMERIC NOT NULL CHECK (o > 0 AND o < 'Infinity'::numeric),
    h NUMERIC NOT NULL CHECK (h > 0 AND h < 'Infinity'::numeric),
    l NUMERIC NOT NULL CHECK (l > 0 AND l < 'Infinity'::numeric),
    c NUMERIC NOT NULL CHECK (c > 0 AND c < 'Infinity'::numeric),
    v NUMERIC NOT NULL CHECK (v >= 0 AND v < 'Infinity'::numeric),
    source TEXT NOT NULL CHECK (source IN ('rest', 'websocket', 'vision')),
    received_ms BIGINT NOT NULL CHECK (received_ms BETWEEN 0 AND 253402300799000),
    raw TEXT CHECK (octet_length(raw) <= 65536),
    raw_source TEXT CHECK (raw_source IN ('rest', 'websocket', 'vision')),
    raw_received_ms BIGINT CHECK (raw_received_ms BETWEEN 0 AND 253402300799000),
    PRIMARY KEY (series_id, open_time),
    CHECK (open_time >= '1970-01-01 00:00:00+00'::timestamptz AND close_time > open_time),
    CHECK (mod(extract(epoch FROM open_time), 60) = 0),
    CHECK (mod(extract(epoch FROM close_time), 60) = 0),
    CHECK (received_ms >= extract(epoch FROM close_time) * 1000),
    CHECK (h >= GREATEST(o, c) AND l <= LEAST(o, c) AND h >= l),
    CHECK ((raw_source IS NULL AND raw_received_ms IS NULL)
           OR (raw IS NOT NULL AND raw_source IS NOT NULL AND raw_received_ms IS NOT NULL))
);
SELECT create_hypertable('market.series_candles', 'open_time',
                        chunk_time_interval => INTERVAL '7 days');


-- Durable first missing minute for an explicitly enabled live writer.
CREATE TABLE market.series_live_cursor (
    series_id TEXT PRIMARY KEY REFERENCES market.bar_series(series_id),
    next_ms BIGINT NOT NULL CHECK (next_ms BETWEEN 0 AND 253402300799000 AND next_ms % 60000=0)
);
