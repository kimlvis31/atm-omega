"""Project-local Docker database lifecycle and explicit schema migration."""

# Imports
import argparse
import os
import subprocess
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import connect_database
from omega.infrastructure.postgres.config import configure_local_database, load_database_settings
from omega.infrastructure.postgres.schema import migrate, verify_schema


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the Omega PostgreSQL/TimescaleDB database")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="project directory")
    parser.add_argument("--port", type=int, help="new local database port (configure/init only)")
    parser.add_argument("command", choices=("configure", "init", "migrate", "status", "stop"))
    args = parser.parse_args()
    if args.port is not None:
        if args.command not in {"configure", "init"}:
            parser.error("--port is only supported by configure and init")
        if not 1 <= args.port <= 65535:
            parser.error("--port must be from 1 to 65535")
    root = args.root.resolve()
    try:
        import psycopg
    except ImportError:
        parser.exit(1, "Install the postgres extra before managing the database.\n")
    try:
        if args.command in {"configure", "init"}:
            if args.port is None:
                configure_local_database(root)
            else:
                configure_local_database(root, port=args.port)
        settings = None
        compose_environment = None
        if args.command == "init":
            settings = load_database_settings(root / "database.config.toml")
            if settings.host != "127.0.0.1" or settings.user != "omega_app":
                raise ValidationError("init requires the local Omega application database")
            compose_environment = dict(
                os.environ, OMEGA_DB_PORT=str(settings.port), OMEGA_DB_NAME=settings.dbname
            )
        if args.command in {"init", "stop"}:
            command = [
                "docker",
                "compose",
                "--project-directory",
                str(root),
                "-f",
                str(root / "compose.yaml"),
            ]
            command += (
                ["up", "--detach", "--wait", "--wait-timeout", "120", "postgres"]
                if args.command == "init"
                else ["stop", "postgres"]
            )
            if args.command == "init":
                subprocess.run(command, cwd=root, check=True, timeout=150, env=compose_environment)
            else:
                subprocess.run(command, cwd=root, check=True, timeout=150)
        if args.command in {"init", "migrate", "status"}:
            if args.command != "init":
                settings = load_database_settings(root / "database.config.toml")
            with connect_database(settings) as connection:
                if args.command != "status":
                    migrate(connection)
                verify_schema(connection)
                extension = connection.execute(
                    "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
                ).fetchone()
            print(f"Database ready: PostgreSQL + TimescaleDB {extension[0]}")
        elif args.command == "configure":
            print(
                "Local database configuration and credentials are ready; existing files preserved."
            )
        else:
            print("Database stopped. Its persistent volume is preserved.")
    except (OSError, StorageError, ValidationError, psycopg.Error, subprocess.SubprocessError):
        parser.exit(
            1,
            "Database operation failed. Check Docker, local configuration and schema; "
            "credentials and server messages are not printed.\n",
        )


if __name__ == "__main__":
    main()
