CREATE TABLE market.candles_1m (
    instrument TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    o NUMERIC NOT NULL CHECK (o > 0),
    h NUMERIC NOT NULL CHECK (h > 0),
    l NUMERIC NOT NULL CHECK (l > 0),
    c NUMERIC NOT NULL CHECK (c > 0),
    v NUMERIC NOT NULL CHECK (v >= 0),
    source TEXT NOT NULL CHECK (source IN ('rest', 'websocket')),
    received_ms BIGINT NOT NULL CHECK (received_ms >= 0),
    raw TEXT CHECK (octet_length(raw) <= 65536),
    PRIMARY KEY (instrument, open_time),
    CHECK (h >= GREATEST(o, c) AND l <= LEAST(o, c) AND h >= l),
    CHECK (close_time = open_time + INTERVAL '1 minute'),
    CHECK (mod((extract(epoch FROM open_time) * 1000)::bigint, 60000) = 0)
);
SELECT create_hypertable('market.candles_1m', 'open_time',
                        chunk_time_interval => INTERVAL '7 days');
-- Compression/retention maintenance is deliberately separate from ingestion commits.
