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
use the same mapping. Historical depth is unavailable. Auxiliary REST calls
use the same 600-weight-per-minute ledger as candles, with a separate limit of
100 metric requests per five minutes and staggered socket starts.

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
docker compose --env-file state/kbeeshome-collector/deploy.env -f deploy/collector/compose.yaml up -d collector
```

For a fresh database, use `migrate` instead of `status`. For an existing
database with a new schema version, verify a backup before migrating. Verify
that the published commit matches the checkout and that each of the 20 series
stores new closed candles. Check the four auxiliary channels in
`market.auxiliary_minutes` and check response weights, throttling and
reconnects in the collector log.
