-- Verified Binance Vision files have independent observation provenance.
ALTER TABLE market.candles_1m
    DROP CONSTRAINT candles_1m_source_check,
    ADD CONSTRAINT candles_1m_source_check CHECK (source IN ('rest', 'websocket', 'vision')),
    DROP CONSTRAINT candles_1m_raw_source_check,
    ADD CONSTRAINT candles_1m_raw_source_check CHECK (raw_source IN ('rest', 'websocket', 'vision'));
