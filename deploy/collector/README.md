# kbeeshome collector

This Linux Docker Compose deployment collects the existing 20 public Binance
one-minute series into a dedicated TimescaleDB volume. The database and
collector read API publish only to loopback.

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
stores new closed candles before recording the deployment as healthy.
