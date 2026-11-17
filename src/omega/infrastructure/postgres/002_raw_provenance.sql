-- Keep the reception metadata of a late raw capture separate from the original
-- canonical candle. Existing payloads have unknown provenance; never invent it.
ALTER TABLE market.candles_1m
    ADD COLUMN raw_source TEXT CHECK (raw_source IN ('rest', 'websocket')),
    ADD COLUMN raw_received_ms BIGINT CHECK (raw_received_ms >= 0),
    ADD CONSTRAINT raw_metadata_pair CHECK (
        (raw_source IS NULL AND raw_received_ms IS NULL)
        OR (raw IS NOT NULL AND raw_source IS NOT NULL AND raw_received_ms IS NOT NULL)
    );
