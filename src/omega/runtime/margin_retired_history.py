"""Bounded explicit retired-archive loading; cached GETs perform no filesystem IO."""

import re
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import MAX_EVENTS, MarginAccount
from omega.runtime.margin_configuration import configuration_parameters, configuration_response
from omega.runtime.margin_ledger import LedgerPages, LedgerQuery, unavailable_ledger
from omega.runtime.margin_retirement import MarginRetirement, retirement_sha256
from omega.runtime.margin_store import account_from_document

SCOPE = "virtual_margin_retired_history"
MAX_JOBS = 8
CLOSE_TIMEOUT = 5
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid retired history identity")
    return value


def _receipt(value):
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise ValidationError("invalid retired history receipt")
    return value


@dataclass(frozen=True, slots=True)
class _ArchiveCache:
    account_id: str
    receipt_sha256: str
    account: MarginAccount
    parameters: dict
    source_anchor: str
    pages: LedgerPages


def _read_archive(root, account_id, expected_receipt):
    # Construct this read-only helper on the reader thread, never a Session,
    # source owner or financial store. Its no-follow reader bounds each file.
    helper = MarginRetirement(root, account_id)
    receipt = helper.read()
    if receipt is None or receipt["state"] != "retired" or receipt["sha256"] != expected_receipt:
        raise ProtocolError("retired receipt differs")
    archive = helper.archive()
    observation = archive["observation"]
    parameters, digest = configuration_parameters(
        archive["registered_parameters"], account_id=account_id
    )
    if (
        archive["account_id"] != account_id
        or archive["sha256"] != receipt["archive_sha256"]
        or digest != receipt["registered_parameters_sha256"]
        or archive["session_config_hash"] != receipt["session_config_hash"]
        or retirement_sha256(observation) != receipt["observation_sha256"]
        or _SHA.fullmatch(archive["session_config_hash"]) is None
    ):
        raise ProtocolError("retired archive identity differs")
    account = account_from_document(archive["account"])
    if (
        account.account_id != account_id
        or len(account.ledger) > MAX_EVENTS
        or type(observation.get("ledger_total")) is not int
        or observation["ledger_total"] != len(account.ledger)
    ):
        raise ProtocolError("retired ledger count differs")
    anchor = observation["source"]["anchor_sha256"]
    pages = LedgerPages(archive["ledger_namespace"])
    # Reuse the exact old namespace/anchor and verify its complete prefix and
    # tail through the existing opaque-token contract, including an empty ledger.
    page = pages.page(
        anchor, account, LedgerQuery(limit=1, snapshot=observation["ledger_snapshot"])
    )
    if page["ledger_total"] != len(account.ledger) or helper.read() != receipt:
        raise ProtocolError("retired ledger snapshot or receipt changed")
    return _ArchiveCache(account_id, expected_receipt, account, parameters, anchor, pages)


class MarginRetiredHistory:
    """One decoder, eight ephemeral jobs and one immutable-account cache.

    The16MB encoded-file cap is supplied by MarginRetirement, not a claim
    about Python heap use. A decode may coexist with the previous single cache.
    Page serialization holds the generation lock so replacement cannot evict a
    cache being served. No financial operation receipts are evicted here.
    """

    def __init__(self, root):
        self.root = Path(root).absolute()
        self.condition = threading.Condition()
        self._jobs, self._queue = OrderedDict(), deque()
        self._cache = None
        self._thread = None
        self._closed = self._close_complete = False
        self._generation = uuid4().hex
        self._close_attempts = 0
        self._close_error = None

    def _dto(self, job_id, row=None, *, reason="job_unknown"):
        cached = (
            row is not None
            and self._cache is not None
            and (self._cache.account_id, self._cache.receipt_sha256) == row["identity"]
        )
        return {
            "schema_version": 1,
            "scope": SCOPE,
            "job_id": job_id,
            "account_id": row["identity"][0] if row else None,
            "receipt_sha256": row["identity"][1] if row else None,
            "state": row["state"] if row else "unavailable",
            "reason_code": row["reason_code"] if row else reason,
            "cached": cached,
            "ledger_total": row["ledger_total"] if row else None,
        }

    def load(self, job_id, account_id, expected_receipt_sha256):
        job_id, account_id = _identity(job_id), _identity(account_id)
        identity = (account_id, _receipt(expected_receipt_sha256))
        with self.condition:
            old = self._jobs.get(job_id)
            if old is not None:
                if old["identity"] != identity:
                    raise ProtocolError("retired history job identity conflicts")
                return self._dto(job_id, old)
            if self._closed:
                return self._dto(job_id, reason="reader_closed")
            if len(self._jobs) >= MAX_JOBS:
                completed = next(
                    (
                        key
                        for key, row in self._jobs.items()
                        if row["state"] in {"completed", "failed"}
                    ),
                    None,
                )
                if completed is None:
                    return self._dto(job_id, reason="job_capacity")
                del self._jobs[completed]
            row = {
                "identity": identity,
                "state": "queued",
                "reason_code": None,
                "ledger_total": None,
            }
            self._jobs[job_id] = row
            self._queue.append(job_id)
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="omega-retired-history", daemon=False
                )
                try:
                    self._thread.start()
                except Exception:
                    self._closed = True
                    self._queue.clear()
                    row.update(state="failed", reason_code="reader_unavailable")
            self.condition.notify_all()
            return self._dto(job_id, row)

    def status(self, job_id):
        job_id = _identity(job_id)
        with self.condition:
            return self._dto(job_id, self._jobs.get(job_id))

    def _run(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self._closed or self._queue)
                if self._closed:
                    return
                job_id = self._queue.popleft()
                row = self._jobs[job_id]
                row["state"] = "running"
                account_id, receipt = row["identity"]
            cache, reason = None, None
            try:
                cache = _read_archive(self.root, account_id, receipt)
            except (ProtocolError, ValidationError, KeyError, TypeError, ValueError):
                reason = "archive_conflict"
            except Exception:
                reason = "archive_unavailable"
            with self.condition:
                if not self._closed:
                    if cache is not None:
                        self._cache = cache
                        row.update(state="completed", ledger_total=len(cache.account.ledger))
                    else:
                        row.update(state="failed", reason_code=reason)
                # Do not retain a second full account while waiting for work.
                cache = None
                self.condition.notify_all()

    def ledger_page(self, account_id, receipt_sha256, **arguments):
        account_id, receipt_sha256 = _identity(account_id), _receipt(receipt_sha256)
        query = LedgerQuery(**arguments)
        with self.condition:
            cache = self._cache
            if (
                self._closed
                or cache is None
                or (cache.account_id, cache.receipt_sha256) != (account_id, receipt_sha256)
            ):
                return unavailable_ledger(account_id, query, "archive_not_loaded")
            return cache.pages.page(cache.source_anchor, cache.account, query)

    def configuration(self, account_id, receipt_sha256):
        account_id, receipt_sha256 = _identity(account_id), _receipt(receipt_sha256)
        with self.condition:
            cache = self._cache
            if (
                self._closed
                or cache is None
                or (cache.account_id, cache.receipt_sha256) != (account_id, receipt_sha256)
            ):
                return configuration_response(account_id, reason_code="archive_not_loaded")
            return configuration_response(account_id, parameters=cache.parameters)

    def cleanup_status(self):
        with self.condition:
            return {
                "schema_version": 1,
                "scope": "process_local_margin_cleanup",
                "generation": self._generation,
                "logical_closed": self._closed,
                "state": "complete"
                if self._close_complete
                else "pending"
                if self._closed
                else "not_started",
                "resources": [
                    {
                        "name": "archive_reader",
                        "state": "complete"
                        if self._close_complete
                        else "pending"
                        if self._closed
                        else "not_started",
                        "error_type": self._close_error,
                        "attempts": self._close_attempts,
                    }
                ],
            }

    def close(self):
        with self.condition:
            if self._close_complete:
                return
            self._closed = True
            self._close_attempts = min(self._close_attempts + 1, 2**63 - 1)
            self._queue.clear()
            for row in self._jobs.values():
                if row["state"] in {"queued", "running"}:
                    row.update(state="failed", reason_code="reader_closed")
            thread = self._thread
            self.condition.notify_all()
        try:
            if thread is not None:
                if thread.ident is not None:
                    thread.join(CLOSE_TIMEOUT)
                if thread.is_alive():
                    raise StorageError("retired history reader cleanup remains unconfirmed")
        except Exception:
            with self.condition:
                self._close_error = self._close_error or "StorageError"
            raise StorageError("retired history reader cleanup remains unconfirmed") from None
        with self.condition:
            self._close_complete = True
            self._cache = None
