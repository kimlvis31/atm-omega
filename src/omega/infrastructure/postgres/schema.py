"""Transactional, checksum-checked migrations. Compression never runs in ingestion."""

from hashlib import sha256
from importlib.resources import files

from omega.domain.errors import StorageError

MIGRATIONS = {
    1: "001_market.sql",
    2: "002_raw_provenance.sql",
    3: "003_auxiliary_minutes.sql",
    4: "004_vision_provenance.sql",
    5: "005_bar_series.sql",
    6: "006_eta_metric_minutes.sql",
}
SCHEMA_VERSION = max(MIGRATIONS)


def migration_sql(version: int = 1) -> str:
    return (
        files("omega.infrastructure.postgres")
        .joinpath(MIGRATIONS[version])
        .read_text(encoding="utf-8")
    )


def migrate(connection) -> None:
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(830922011)")
        extension = connection.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        ).fetchone()
        if extension is None:
            raise StorageError("TimescaleDB extension is required before migration")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS omega_meta.schema_migrations "
            "(version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        rows = connection.execute(
            "SELECT version,checksum FROM omega_meta.schema_migrations ORDER BY version"
        ).fetchall()
        expected = [(v, sha256(migration_sql(v).encode()).hexdigest()) for v in MIGRATIONS]
        if rows != expected[: len(rows)]:
            raise StorageError("database migration version or checksum mismatch")
        for version, digest in expected[len(rows) :]:
            connection.execute(migration_sql(version), prepare=False)
            connection.execute(
                "INSERT INTO omega_meta.schema_migrations(version,checksum) VALUES (%s,%s)",
                (version, digest),
            )


def verify_schema(connection) -> None:
    rows = connection.execute(
        "SELECT version,checksum FROM omega_meta.schema_migrations ORDER BY version"
    ).fetchall()
    expected = [(v, sha256(migration_sql(v).encode()).hexdigest()) for v in MIGRATIONS]
    if rows != expected:
        raise StorageError("run omega-db migrate for the matching schema before collection")
    present = connection.execute(
        "SELECT count(*) FROM timescaledb_information.hypertables "
        "WHERE hypertable_schema='market' AND hypertable_name='candles_1m'"
    ).fetchone()
    if present != (1,):
        raise StorageError("the expected TimescaleDB candle hypertable is missing")
    auxiliary = connection.execute(
        "SELECT count(*) FROM timescaledb_information.hypertables "
        "WHERE hypertable_schema='market' AND hypertable_name='auxiliary_minutes'"
    ).fetchone()
    if auxiliary != (1,):
        raise StorageError("the expected TimescaleDB auxiliary hypertable is missing")
