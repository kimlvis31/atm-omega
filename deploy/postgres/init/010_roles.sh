#!/usr/bin/env sh
set -eu

app_password="$(tr -d '\r\n' < "$OMEGA_DB_PASSWORD_FILE")"
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 --set app_password="$app_password" --set db_name="$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE ROLE omega_app LOGIN PASSWORD :'app_password';
CREATE SCHEMA market AUTHORIZATION omega_app;
CREATE SCHEMA omega_meta AUTHORIZATION omega_app;
GRANT CONNECT ON DATABASE :"db_name" TO omega_app;
SQL
