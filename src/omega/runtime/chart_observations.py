"""On-demand stored auxiliary chart reads; no account, HTTP or trading clients."""

import time
from pathlib import Path

from omega.application.auxiliary_series import (
    KINDS,
    MAX_TIME,
    auxiliary_ranges,
    empty_result,
    project_auxiliary_series,
    project_eta_metric_series,
    unsupported_reason,
    validate_parameters,
)
from omega.domain.errors import ValidationError


class ChartObservationsView:
    """Each read owns and closes its store; construction performs no I/O."""

    def __init__(self, root, store_factory=None, clock_ms=None):
        if any(value is not None and not callable(value) for value in (store_factory, clock_ms)):
            raise ValidationError("invalid chart observation factories")
        self.root = Path(root)
        self.store_factory = store_factory or self._store
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    def _store(self):
        from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
        from omega.infrastructure.postgres.config import load_database_settings

        return PostgresAuxiliaryStore(load_database_settings(self.root / "database.config.toml"))

    def read(self, parameters: dict) -> dict:
        as_of = self.clock_ms()
        if type(as_of) is not int or not 0 <= as_of <= MAX_TIME:
            raise ValidationError("invalid chart observation knowledge clock")
        spec = validate_parameters(parameters, as_of_ms=as_of)
        source = spec.get("source", spec["instrument"] + ":" + KINDS[spec["kind"]][1])
        reason = unsupported_reason(spec)
        if reason:
            return empty_result(
                spec, source=source, as_of_ms=as_of, state="unsupported", warning=reason
            )
        store = None
        try:
            store = self.store_factory()
            result = self._read_series(store, spec, source, as_of)
        except Exception:
            result = empty_result(
                spec,
                source=source,
                as_of_ms=as_of,
                state="unavailable",
                warning="Stored auxiliary observations could not be read or verified.",
            )
        finally:
            if store is not None:
                try:
                    store.close()
                except Exception:
                    result = empty_result(
                        spec,
                        source=source,
                        as_of_ms=as_of,
                        state="unavailable",
                        warning="Stored auxiliary chart connection cleanup failed.",
                    )
        return result

    @staticmethod
    def _read_series(store, spec, source, as_of):
        kind, instrument = spec["kind"], spec["instrument"]
        mapped_read = getattr(store, "read_eta_metric_minutes", None)
        eta_metrics = (
            kind in {"OPENINTEREST", "LONGSHORTRATIO"}
            and source == instrument + ":" + KINDS[kind][1]
            and callable(mapped_read)
        )
        if eta_metrics:
            mapped = mapped_read(instrument, spec["start_ms"], spec["end_ms"], as_of_ms=as_of)
            if mapped:
                return project_eta_metric_series(spec, mapped, source=source, as_of_ms=as_of)
        rows = store.read_ranges(auxiliary_ranges(spec, source), as_of_ms=as_of, limit=10000)
        if eta_metrics and any(
            row.observation.mapping().get("SOURCE_PERIOD_MS") == 300000 for row in rows
        ):
            # An unpaired native snapshot must stay a gap, including in the individual charts.
            return project_eta_metric_series(spec, (), source=source, as_of_ms=as_of)
        if not rows and kind == "OPENINTEREST" and "source" not in spec:
            source = instrument + ":open_interest"
            rows = store.read_ranges(auxiliary_ranges(spec, source), as_of_ms=as_of, limit=10000)
        return project_auxiliary_series(spec, rows, source=source, as_of_ms=as_of)

    def close(self):
        """No persistent connection or worker is retained by this view."""
