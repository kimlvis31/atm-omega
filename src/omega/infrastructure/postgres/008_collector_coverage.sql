-- Durable coverage for immutable collector data. Half-open ranges use UTC milliseconds.
CREATE TABLE market.collector_coverage (
    scope_key text NOT NULL,
    channel text NOT NULL,
    dataset text NOT NULL,
    day_ms bigint NOT NULL CHECK (day_ms >= 0 AND day_ms % 86400000 = 0),
    observed int8multirange NOT NULL,
    incomplete int8multirange NOT NULL,
    confirmed_empty int8multirange NOT NULL,
    PRIMARY KEY (scope_key,channel,dataset,day_ms),
    CHECK (NOT (observed && incomplete)),
    CHECK (confirmed_empty <@ observed)
);

CREATE TABLE market.collector_history_checks (
    instrument text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('kline','agg_trade','depth','metrics')),
    source text NOT NULL CHECK (source IN ('vision','rest')),
    start_ms bigint NOT NULL CHECK (start_ms >= 0),
    end_ms bigint NOT NULL CHECK (end_ms > start_ms),
    result text NOT NULL CHECK (result IN ('unlisted','no_rows','incomplete')),
    checked_ms bigint NOT NULL CHECK (checked_ms >= end_ms),
    PRIMARY KEY (instrument,channel,source,start_ms,end_ms),
    CHECK (result <> 'unlisted' OR source = 'vision')
);

CREATE FUNCTION market.merge_collector_coverage(
    coverage_key text, coverage_channel text, coverage_dataset text, bucket_ms bigint,
    new_observed int8multirange, new_incomplete int8multirange, new_empty int8multirange
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO market.collector_coverage AS saved
        (scope_key,channel,dataset,day_ms,observed,incomplete,confirmed_empty)
    VALUES (
        coverage_key,coverage_channel,coverage_dataset,bucket_ms,
        new_observed,new_incomplete,new_empty
    )
    ON CONFLICT (scope_key,channel,dataset,day_ms) DO UPDATE SET
        observed=(saved.observed-EXCLUDED.incomplete)+EXCLUDED.observed,
        incomplete=(saved.incomplete-EXCLUDED.observed)+EXCLUDED.incomplete,
        confirmed_empty=(saved.confirmed_empty-(EXCLUDED.observed+EXCLUDED.incomplete))
                        +EXCLUDED.confirmed_empty;
$$;

CREATE FUNCTION market.collector_bar_coverage_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM market.merge_collector_coverage(
        series_id,'kline','',day_ms,observed,'{}'::int8multirange,'{}'::int8multirange
    )
    FROM (
        SELECT series_id,from_ms/86400000*86400000 AS day_ms,
               range_agg(int8range(from_ms,to_ms,'[)')) AS observed
        FROM (
            SELECT series_id,(extract(epoch FROM open_time)*1000)::bigint AS from_ms,
                   (extract(epoch FROM close_time)*1000)::bigint AS to_ms
            FROM coverage_bars
        ) AS bar
        GROUP BY series_id,day_ms ORDER BY series_id,day_ms
    ) AS groups;
    RETURN NULL;
END;
$$;

CREATE FUNCTION market.collector_aux_coverage_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM market.merge_collector_coverage(
        instrument,channel,dataset,day_ms,
        COALESCE(range_agg(slot) FILTER (WHERE quality='observed'),'{}'::int8multirange),
        COALESCE(range_agg(slot) FILTER (WHERE quality<>'observed'),'{}'::int8multirange),
        COALESCE(range_agg(slot) FILTER (WHERE quality='observed' AND empty),'{}'::int8multirange)
    )
    FROM (
        SELECT instrument,channel,dataset,opening/86400000*86400000 AS day_ms,
               int8range(opening,opening+60000,'[)') AS slot,
               observation->>'quality' AS quality,
               channel='trade_flow' AND dataset=instrument||':aggTrade' AND
               observation->'values' @> '[
                   {"name":"AGGTRADE_KLINE_ZERO","type":"int","value":1},
                   {"name":"AGGTRADE_EVENTCOUNT","type":"int","value":0}
               ]'
                   ::jsonb AS empty
        FROM (
            SELECT DISTINCT ON (instrument,channel,dataset,open_time)
                   instrument,channel,dataset,(extract(epoch FROM open_time)*1000)::bigint AS opening,
                   convert_from(payload,'UTF8')::jsonb->'observation' AS observation
            FROM coverage_aux
            ORDER BY instrument,channel,dataset,open_time,revision DESC
        ) AS latest
    ) AS windows
    GROUP BY instrument,channel,dataset,day_ms
    ORDER BY instrument,channel,dataset,day_ms;
    RETURN NULL;
END;
$$;

-- Seed once from the existing database before enabling incremental maintenance.
-- Wait for existing writers before starting the snapshot and hold them off until the seed commits.
SET LOCAL statement_timeout = '5min';
LOCK TABLE market.series_candles,market.auxiliary_minutes IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM market.auxiliary_minutes WHERE checksum <> encode(sha256(payload),'hex')
    ) THEN
        RAISE EXCEPTION 'cannot seed coverage for corrupt auxiliary payloads';
    END IF;
END;
$$;

INSERT INTO market.collector_coverage
    (scope_key,channel,dataset,day_ms,observed,incomplete,confirmed_empty)
SELECT series_id,'kline','',from_ms/86400000*86400000,
       range_agg(int8range(from_ms,to_ms,'[)')),'{}','{}'
FROM (
    SELECT series_id,(extract(epoch FROM open_time)*1000)::bigint AS from_ms,
           (extract(epoch FROM close_time)*1000)::bigint AS to_ms
    FROM market.series_candles
) AS bar
GROUP BY series_id,from_ms/86400000*86400000;

INSERT INTO market.collector_coverage
    (scope_key,channel,dataset,day_ms,observed,incomplete,confirmed_empty)
SELECT instrument,channel,dataset,day_ms,
       COALESCE(range_agg(slot) FILTER (WHERE quality='observed'),'{}'::int8multirange),
       COALESCE(range_agg(slot) FILTER (WHERE quality<>'observed'),'{}'::int8multirange),
       COALESCE(range_agg(slot) FILTER (WHERE quality='observed' AND empty),'{}'::int8multirange)
FROM (
    SELECT instrument,channel,dataset,opening/86400000*86400000 AS day_ms,
           int8range(opening,opening+60000,'[)') AS slot,
           observation->>'quality' AS quality,
           channel='trade_flow' AND dataset=instrument||':aggTrade' AND
           observation->'values' @> '[
                   {"name":"AGGTRADE_KLINE_ZERO","type":"int","value":1},
                   {"name":"AGGTRADE_EVENTCOUNT","type":"int","value":0}
               ]'
               ::jsonb AS empty
    FROM (
        SELECT DISTINCT ON (instrument,channel,dataset,open_time)
               instrument,channel,dataset,(extract(epoch FROM open_time)*1000)::bigint AS opening,
               convert_from(payload,'UTF8')::jsonb->'observation' AS observation
        FROM market.auxiliary_minutes
        ORDER BY instrument,channel,dataset,open_time,revision DESC
    ) AS latest
) AS windows
GROUP BY instrument,channel,dataset,day_ms;

CREATE TRIGGER collector_bar_coverage_after_insert
AFTER INSERT ON market.series_candles REFERENCING NEW TABLE AS coverage_bars
FOR EACH STATEMENT EXECUTE FUNCTION market.collector_bar_coverage_insert();

CREATE TRIGGER collector_aux_coverage_after_insert
AFTER INSERT ON market.auxiliary_minutes REFERENCING NEW TABLE AS coverage_aux
FOR EACH STATEMENT EXECUTE FUNCTION market.collector_aux_coverage_insert();

SET LOCAL statement_timeout = '5s';
