-- Derived, immutable minute observations. Provider wire payload archives are separate.
CREATE TABLE market.auxiliary_minutes (
    instrument TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel ~ '^[a-z][a-z0-9_]{0,31}$'),
    dataset TEXT NOT NULL CHECK (length(dataset) BETWEEN 1 AND 96),
    open_time TIMESTAMPTZ NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    available_ms BIGINT NOT NULL CHECK (available_ms BETWEEN 0 AND 253402300799000),
    event_ms BIGINT NOT NULL CHECK (event_ms BETWEEN 0 AND available_ms),
    payload BYTEA NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 262144),
    checksum TEXT NOT NULL CHECK (checksum ~ '^[a-f0-9]{64}$'),
    previous_checksum TEXT CHECK (previous_checksum ~ '^[a-f0-9]{64}$'),
    PRIMARY KEY (instrument, channel, dataset, open_time, revision),
    CHECK ((revision = 1 AND previous_checksum IS NULL)
           OR (revision > 1 AND previous_checksum IS NOT NULL)),
    CHECK (mod((extract(epoch FROM open_time) * 1000)::bigint, 60000) = 0)
);
SELECT create_hypertable('market.auxiliary_minutes', 'open_time',
                        chunk_time_interval => INTERVAL '7 days');
CREATE INDEX auxiliary_minutes_asof
    ON market.auxiliary_minutes (instrument, channel, dataset, open_time, available_ms DESC, revision DESC);
-- No retention or compression maintenance inside observation commits.
