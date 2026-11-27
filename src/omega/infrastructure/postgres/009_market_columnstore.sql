-- Eta compresses old market data by symbol and orders each segment by descending time.
-- Use the corresponding query keys on Omega's shared market hypertables.
SET LOCAL lock_timeout = '5s';

ALTER TABLE market.candles_1m SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'instrument',
    timescaledb.orderby = 'open_time DESC'
);
ALTER TABLE market.series_candles SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'series_id',
    timescaledb.orderby = 'open_time DESC'
);
ALTER TABLE market.auxiliary_minutes SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'instrument,channel,dataset',
    timescaledb.orderby = 'open_time DESC,revision DESC'
);

CALL add_columnstore_policy('market.candles_1m', after => INTERVAL '7 days',
    schedule_interval => INTERVAL '30 minutes', initial_start => now() + INTERVAL '40 minutes');
CALL add_columnstore_policy('market.series_candles', after => INTERVAL '7 days',
    schedule_interval => INTERVAL '30 minutes', initial_start => now() + INTERVAL '30 minutes');
CALL add_columnstore_policy('market.auxiliary_minutes', after => INTERVAL '7 days',
    schedule_interval => INTERVAL '30 minutes', initial_start => now() + INTERVAL '45 minutes');

-- Stagger small background runs so conversion does not monopolize disk and table locks.
SELECT alter_job(job_id, max_runtime => INTERVAL '2 minutes',
    config_merge => '{"maxchunks_to_compress":4}'::jsonb)
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'market' AND proc_name = 'policy_compression'
  AND hypertable_name IN ('candles_1m','series_candles','auxiliary_minutes');

SET LOCAL lock_timeout = '2s';
