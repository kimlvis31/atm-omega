"""Bounded process-local cleanup observations; never durable financial authority."""

import re
from threading import RLock
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError

SCOPE = "process_local_margin_cleanup"
_FIELDS = {"schema_version", "scope", "generation", "logical_closed", "state", "resources"}
_RESOURCE_FIELDS = {"name", "state", "error_type", "attempts"}


def confirmed_cleanup(document, *, generation=None):
    """Strict observation of the same instance, not a reopened owner/receipt."""
    if (
        type(document) is not dict
        or set(document) != _FIELDS
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["scope"] != SCOPE
        or type(document["generation"]) is not str
        or re.fullmatch(r"[a-f0-9]{32}", document["generation"]) is None
        or generation is not None
        and document["generation"] != generation
        or document["logical_closed"] is not True
        or document["state"] != "complete"
        or type(document["resources"]) is not list
        or len(document["resources"]) > 128
    ):
        return False
    names = set()
    for row in document["resources"]:
        if (
            type(row) is not dict
            or set(row) != _RESOURCE_FIELDS
            or type(row["name"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", row["name"]) is None
            or row["name"] in names
            or row["state"] != "complete"
            or type(row["attempts"]) is not int
            or not 0 <= row["attempts"] <= 2**63 - 1
            or row["error_type"] is not None
            and (
                type(row["error_type"]) is not str
                or re.fullmatch(r"[A-Za-z0-9_]{1,96}", row["error_type"]) is None
            )
        ):
            return False
        names.add(row["name"])
    return True


class MarginCleanup:
    """Keep unresolved handles; no failed close is upgraded by a later None.

    Calls are serialized by the owning Session/worker/store. The short status
    lock is never held while invoking close or a release observer.
    """

    def __init__(self):
        self.generation = uuid4().hex
        self.logical_closed = False
        self._lock, self._resources = RLock(), []

    def add(self, name, resource, *, final=False, released=None, initial_ack=False):
        with self._lock:
            if (
                self.logical_closed
                or type(name) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", name) is None
                or any(row["name"] == name for row in self._resources)
                or len(self._resources) >= 128
            ):
                raise ValidationError("invalid bounded margin cleanup resource")
            self._resources.append(
                {
                    "name": name,
                    "resource": resource,
                    "final": final,
                    "released": released,
                    "initial_ack": initial_ack,
                    "state": "not_started",
                    "error_type": None,
                    "attempts": 0,
                }
            )

    def status(self):
        with self._lock:
            rows = [{key: row[key] for key in _RESOURCE_FIELDS} for row in self._resources]
            return {
                "schema_version": 1,
                "scope": SCOPE,
                "generation": self.generation,
                "logical_closed": self.logical_closed,
                "state": "not_started"
                if not self.logical_closed
                else "complete"
                if all(row["state"] == "complete" for row in rows)
                else "pending",
                "resources": rows,
            }

    def close(self):
        with self._lock:
            self.logical_closed = True
        failures = []
        # Session coordinator leases are released only after every child lease.
        rows = [row for row in reversed(self._resources) if not row["final"]] + [
            row for row in reversed(self._resources) if row["final"]
        ]
        for row in rows:
            if row["state"] == "complete":
                continue
            if row["final"] and any(
                other["state"] != "complete" for other in self._resources if not other["final"]
            ):
                continue
            error = None
            with self._lock:
                row["state"] = "pending"
                row["attempts"] = min(row["attempts"] + 1, 2**63 - 1)
            try:
                row["resource"].close()
            except Exception as failure:
                error = type(failure).__name__[:96]
                failures.append(error)
                with self._lock:
                    row["error_type"] = row["error_type"] or error
            try:
                released = (
                    row["released"]() is True
                    if row["released"] is not None
                    else (row["initial_ack"] and row["error_type"] is None and error is None)
                )
            except Exception as failure:
                released = False
                failures.append(type(failure).__name__[:96])
                with self._lock:
                    row["error_type"] = row["error_type"] or type(failure).__name__[:96]
            with self._lock:
                row["state"] = "complete" if released else "pending"
        if failures or self.status()["state"] != "complete":
            raise StorageError("margin resource cleanup is not confirmed")
