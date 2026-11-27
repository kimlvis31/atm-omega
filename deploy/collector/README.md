# kbeeshome collector

This Linux Docker Compose deployment collects the existing 20 public Binance
one-minute series into a dedicated TimescaleDB volume. The database and
collector read API publish only to loopback.

The same subscriptions receive aggregate trades and diff depth. Trade-flow
rows contain exact buy/sell one-minute sums and fill counts. The collector
repairs gaps of up to 1,000 aggregate IDs with one public REST request and
marks unproven minutes partial. Depth starts with a 1,000-level REST snapshot,
checks diff continuity, and stores minute-end price-distance bands from
the synchronized book. Limited book coverage or a stale sample is partial.
The 10 USD-M symbols also store five-minute open interest and account
long/short ratio at the provider's timestamps. The database function
`market.eta_metric_minutes(instrument_key, start_ms, end_ms, as_of_ms)`
maps each matched pair to the five closed minutes in its Eta block. It
retains both original revisions and returns the later source availability
time. An incomplete pair is a gap. Existing and newly collected statistics
use the same mapping. Auxiliary REST calls
use the same 600-weight-per-minute ledger as candles, with a separate limit of
100 metric requests per five minutes and staggered socket starts.

The history service shares the live REST weight ledger and separately paces
Vision reads. It uses the existing handoff inventory to attach to the 20 live
series. `market.binance_history_origins` stores each verified first timestamp
with Vision object/ZIP checksum evidence and the REST first candle. Verified
origins are reused on restart. Unavailable and failed searches carry their
next retry time.

The historical candle series starts at each verified first candle. Its ID,
backfill cursor, live-tail mirror cursor and unresolved spans are recorded in
`market.binance_kline_history` and `market.binance_kline_history_gaps`. The
existing live series ID remains available while the older range fills.
Auxiliary history advances by day from the checked origin, records unavailable
ranges in `auxiliary-history-unresolved/` and can resume from
`auxiliary-history-progress.json`. USD-M Vision `bookDepth` uses the Eta
cumulative-band redistribution and is stored as the separate
`binance_depth_vision_v1` source. Availability depends on archive coverage.
Live snapshots start when the stream is observed.

The history service checks recently closed candle volumes against available
trade-flow minutes. It repairs within current REST retention under the shared
budget and marks repairs verified when they match the closed candle volume.
When Binance returns different totals, the measured trade-flow row and its
source receipts are retained as partial. Zero-volume minutes are verified
from committed candles.

Coverage is stored in market.collector_coverage as half-open UTC millisecond ranges.
Kline uses the series ID as scope_key and an empty dataset. Auxiliary data uses
the instrument key and exact dataset, so archived and streamed depth stay distinct.
The latest persisted revision determines the observed and incomplete ranges.
confirmed_empty marks verified zero-trade slots and is a subset of observed.
A source check lives separately in market.collector_history_checks and records
the source, range, result and check time. An unlisted Vision file or an empty
REST response describes that source at check time. Unchecked gaps remain unknown.
The migration seeds coverage from the existing rows without classifying old gaps.
Coverage is updated once per insert statement in the same transaction as the rows.
The recent trade repair selects candidates from the range difference, then reads
only the relevant candle volumes.

The collector owns Binance REST, Vision, WebSocket subscriptions and observation
aggregation. The data manager owns range planning, PostgreSQL writes, checkpoint
updates and the committed read API. They exchange bounded frames over private
Unix sockets in the shared runtime directory. A live ingest reply is sent after
its DB transaction commits. The data manager submits up to OMEGA_VISION_WORKERS
candle fetches at a time. Vision workers share the provider cooldown state.

When replacing an installation with the former history service, stop the old
collector and history before starting collector and data_manager. The existing
volume, original series boundaries, inventory, and checkpoints are reused.

The checkout lives on the deployment host's Desktop. Keep the original
series boundary in `state/kbeeshome-collector/deploy.env` as
`OMEGA_EFFECTIVE_FROM_MS`. The runtime database configuration belongs at
`state/kbeeshome-collector/database.config.toml` and names the local DB port,
`omega_collector_live`, user `omega_app`, and password file
`/run/secrets/db_app_password`. Keep the administrator password, application
password, and API token in the ignored `secrets/kbeeshome-collector/`
directory as `db_admin_password`, `db_app_password` and `api_token`.

From the checkout, use these commands after reviewing and publishing an update.

```sh
git pull --ff-only
docker compose --env-file state/kbeeshome-collector/deploy.env -f deploy/collector/compose.yaml up -d --wait postgres
docker compose --env-file state/kbeeshome-collector/deploy.env -f deploy/collector/compose.yaml build collector
docker compose --env-file state/kbeeshome-collector/deploy.env -f deploy/collector/compose.yaml run --rm --no-deps --entrypoint python collector -I -B -m omega.runtime.database_cli --root /run/omega status
docker compose --env-file state/kbeeshome-collector/deploy.env -f deploy/collector/compose.yaml up -d collector data_manager
```

For a fresh database, use `migrate` instead of `status`. For an existing
database with a new schema version, verify a backup before migrating. Verify
that the published commit matches the checkout and that each of the 20 series
stores new closed candles. Check the four auxiliary channels in
`market.auxiliary_minutes` and check response weights, throttling and
reconnects in the collector log.
