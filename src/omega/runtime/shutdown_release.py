"""One process-local correction of an unconfirmed root-lease release.

The trusted Core must still own its original OS lease. This is not a public
operation, a new exit request, or evidence that the process has physically died.
"""

from copy import deepcopy
from hashlib import sha256

from omega.domain.errors import StorageError, ValidationError


def _commit_pair(original, target, *, core_epoch, read, write, before_io, label):
    """Finish only this fixed row-first pair; an unknown ACK requires a re-read."""

    def current():
        before_io()
        row = read("record")
        before_io()
        index = read("index")
        if row.get("core_epoch") != core_epoch or index.get("core_epoch") != core_epoch:
            raise StorageError(label + " generation changed")
        return row, index

    original_row, original_index = original
    target_row, target_index = target
    row, index = current()
    if (row, index) not in (original, (target_row, original_index), target):
        raise StorageError(label + " documents changed")
    if row == original_row:
        before_io()
        write("record", deepcopy(target_row))
    row, index = current()
    if row != target_row or index not in (original_index, target_index):
        raise StorageError(label + " partial documents changed")
    if index == original_index:
        before_io()
        write("index", deepcopy(target_index))
    if current() != target:
        raise StorageError(label + " commit is unconfirmed")
    return deepcopy(target)


class ExitCompletionIntent:
    """One fixed completion intent captured before the first OP write."""

    def __init__(self, row, index, *, core_epoch, successful):
        self.core_epoch, self.successful = core_epoch, successful
        self.original = deepcopy(row), deepcopy(index)
        target_row, target_index = deepcopy(row), deepcopy(index)
        target_row.update(
            state="completed" if successful else "failed",
            result={"core_exit_confirmed": successful},
        )
        target_index.update(active=None, last_exit=row["id"], state="closed")
        target_index["revision"] += 1
        self.target = target_row, target_index

    def complete(self, *, core_epoch, successful, read, write):
        if core_epoch != self.core_epoch:
            raise ValidationError("shutdown completion belongs to another generation")
        if self.successful is True and successful is not True:
            raise StorageError("shutdown completion outcome changed; unconfirmed")
        # A failed first intent stays failed even if a later caller reports True.
        return _commit_pair(
            self.original,
            self.target,
            core_epoch=self.core_epoch,
            read=read,
            write=write,
            before_io=lambda: None,
            label="shutdown completion",
        )


class RootReleaseCorrection:
    """Bounded original/failed document pairs; never persisted as another intent."""

    def __init__(self, core_epoch, encode):
        self.core_epoch, self.encode = core_epoch, encode
        self.guard = self.original = self.failed = None

    def _hash(self, value):
        return sha256(self.encode(value)).hexdigest()

    def arm(self, row, index):
        guard = {
            "operation_id": row["id"],
            "core_epoch": self.core_epoch,
            "record_sha256": self._hash(row),
            "index_sha256": self._hash(index),
        }
        if self.guard is not None and guard != self.guard:
            raise StorageError("root release correction already belongs to another exit")
        self.guard = guard
        self.original = deepcopy(row), deepcopy(index)
        failed_row, failed_index = deepcopy(row), deepcopy(index)
        failed_row.update(
            state="failed",
            result={"core_exit_confirmed": False},
            error="root_lease_release_unconfirmed",
        )
        failed_index.update(state="failed", revision=index["revision"] + 1)
        self.failed = failed_row, failed_index
        return deepcopy(guard)

    def correct(self, guard, *, core_epoch, root_lease_held, read, write):
        """CAS original/original -> failed/original -> failed/failed only.

        ``read``/``write`` are owner-local OP/index callbacks, not financial
        callbacks. A raised write acknowledgement is retried by re-reading the
        same pair; it never authorizes overwriting another generation/change.
        """
        if (
            type(guard) is not dict
            or self.guard is None
            or guard != self.guard
            or core_epoch != self.core_epoch
            or not callable(root_lease_held)
        ):
            raise ValidationError("root release correction guard does not match this manager")

        def held():
            try:
                confirmed = root_lease_held()
            except Exception:
                raise StorageError("root lease ownership is unconfirmed") from None
            if confirmed is not True:
                raise StorageError("original root lease is no longer confirmed held")

        return _commit_pair(
            self.original,
            self.failed,
            core_epoch=self.core_epoch,
            read=read,
            write=write,
            before_io=held,
            label="root release correction",
        )
