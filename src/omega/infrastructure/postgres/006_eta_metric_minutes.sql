-- Eta's native five-minute statistic at T applies to closed minutes T through T+4m.
-- Pair by the provider timestamp and select each source revision at the caller's cutoff.
CREATE FUNCTION market.eta_metric_minutes(
    instrument_key TEXT,
    start_ms BIGINT,
    end_ms BIGINT,
    as_of_ms BIGINT
) RETURNS TABLE (
    open_ms BIGINT,
    close_ms BIGINT,
    metric_time_ms BIGINT,
    available_ms BIGINT,
    metrics JSONB,
    open_interest_revision BIGINT,
    long_short_revision BIGINT,
    open_interest_checksum TEXT,
    long_short_checksum TEXT
)
LANGUAGE plpgsql STABLE STRICT SET search_path = pg_catalog
AS $function$
DECLARE
    closed_end_ms BIGINT;
BEGIN
    IF instrument_key !~ '^binance:usdm:[A-Z0-9]{1,24}$'
       OR start_ms < 0 OR start_ms >= end_ms
       OR end_ms > 253402300799000 OR as_of_ms NOT BETWEEN 0 AND 253402300799000
       OR start_ms % 60000 <> 0 OR end_ms % 60000 <> 0
       OR end_ms - start_ms > 600000000 THEN
        RAISE EXCEPTION 'Expected a canonical USD-M instrument and at most 10000 aligned minutes'
            USING ERRCODE = '22023';
    END IF;
    IF as_of_ms > floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint THEN
        RAISE EXCEPTION 'Metric cutoff cannot be in the future' USING ERRCODE = '22023';
    END IF;
    closed_end_ms := least(end_ms, as_of_ms / 60000 * 60000);
    IF closed_end_ms <= start_ms THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH native_pairs AS (
        SELECT bucket.opening, oi.available_ms AS oi_available, ls.available_ms AS ls_available,
               oi.revision AS oi_revision, ls.revision AS ls_revision,
               oi.checksum AS oi_checksum, ls.checksum AS ls_checksum,
               convert_from(oi.payload, 'UTF8')::jsonb -> 'observation' AS oi_doc,
               convert_from(ls.payload, 'UTF8')::jsonb -> 'observation' AS ls_doc
        FROM generate_series(start_ms / 300000 * 300000, closed_end_ms - 60000, 300000)
            AS bucket(opening)
        CROSS JOIN LATERAL (
            SELECT a.available_ms, a.revision, a.checksum, a.payload, a.event_ms
            FROM market.auxiliary_minutes AS a
            WHERE a.instrument = instrument_key AND a.channel = 'open_interest'
              AND a.dataset = instrument_key || ':open_interest_history'
              AND a.open_time = to_timestamp(bucket.opening / 1000)
              AND a.available_ms <= as_of_ms
            ORDER BY a.revision DESC LIMIT 1
        ) AS oi
        CROSS JOIN LATERAL (
            SELECT a.available_ms, a.revision, a.checksum, a.payload, a.event_ms
            FROM market.auxiliary_minutes AS a
            WHERE a.instrument = instrument_key AND a.channel = 'long_short_ratio'
              AND a.dataset = instrument_key || ':long_short_ratio'
              AND a.open_time = to_timestamp(bucket.opening / 1000)
              AND a.available_ms <= as_of_ms
            ORDER BY a.revision DESC LIMIT 1
        ) AS ls
        WHERE oi.event_ms = bucket.opening AND ls.event_ms = bucket.opening
          AND encode(sha256(oi.payload), 'hex') = oi.checksum
          AND encode(sha256(ls.payload), 'hex') = ls.checksum
    ), metric_buckets AS (
        SELECT pair.*, fields.values AS mapped_values
        FROM native_pairs AS pair
        CROSS JOIN LATERAL (
            SELECT jsonb_object_agg(value ->> 'name', value -> 'value') AS values
            FROM (
                SELECT value FROM jsonb_array_elements(pair.oi_doc -> 'values')
                WHERE value ->> 'name' IN ('METRIC_OPENINTEREST', 'METRIC_OPENINTERESTVALUE')
                UNION ALL
                SELECT value FROM jsonb_array_elements(pair.ls_doc -> 'values')
                WHERE value ->> 'name' IN (
                    'METRIC_LONGSHORTRATIO', 'METRIC_LONGACCOUNT', 'METRIC_SHORTACCOUNT'
                )
            ) AS metric_fields
            WHERE value ->> 'type' = 'decimal' AND jsonb_typeof(value -> 'value') = 'string'
        ) AS fields
        WHERE pair.oi_doc ->> 'quality' = 'observed' AND pair.ls_doc ->> 'quality' = 'observed'
          AND (pair.oi_doc ->> 'coverage')::numeric = 1
          AND (pair.ls_doc ->> 'coverage')::numeric = 1
          AND pair.oi_doc -> 'values' @> '[{"name":"SOURCE_PERIOD_MS","type":"int","value":300000}]'
          AND pair.ls_doc -> 'values' @> '[{"name":"SOURCE_PERIOD_MS","type":"int","value":300000}]'
          AND fields.values ?& ARRAY[
              'METRIC_OPENINTEREST', 'METRIC_OPENINTERESTVALUE', 'METRIC_LONGSHORTRATIO'
          ]
    )
    SELECT minute.opening, minute.opening + 60000, bucket.opening,
           greatest(bucket.oi_available, bucket.ls_available), bucket.mapped_values,
           bucket.oi_revision, bucket.ls_revision, bucket.oi_checksum, bucket.ls_checksum
    FROM metric_buckets AS bucket
    CROSS JOIN LATERAL generate_series(
        greatest(start_ms, bucket.opening),
        least(closed_end_ms - 60000, bucket.opening + 240000),
        60000
    ) AS minute(opening)
    ORDER BY minute.opening;
END;
$function$;
