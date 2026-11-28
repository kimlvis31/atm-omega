"""Bounded Polars CSV batches with exact ``csv.reader`` fallback semantics."""

from __future__ import annotations

import csv
import io
import itertools

_BATCH_BYTES = 1_048_576
_DEFAULT_BATCH_ROWS = 4096
_MAX_BATCH_ROWS = 8192
_PROBE_ROWS = 16
_MAX_COLUMNS = 12


def _fallback(unread, pending, source):
    tail = () if pending is _NO_PENDING else (pending,)
    yield from csv.reader(itertools.chain(unread, tail, source), strict=True)


_NO_PENDING = object()


def _physical_body(line):
    if line.endswith("\r\n"):
        return line[:-2]
    if line.endswith("\n"):
        return line[:-1]
    return line


def _checked_lines(lines, check_stop):
    for line in lines:
        if check_stop is not None:
            check_stop()
        yield line


def _polars_rows(pl, batch):
    payload = "".join(batch).encode("utf-8")
    frame = pl.read_csv(
        io.BytesIO(payload),
        has_header=False,
        infer_schema=False,
        ignore_errors=False,
        truncate_ragged_lines=False,
        raise_if_empty=False,
        quote_char=None,
    )
    if frame.height != len(batch) or not 1 <= frame.width <= _MAX_COLUMNS:
        return None
    if any(frame.null_count().row(0)):
        return None
    return frame.width, frame.iter_rows()


def iter_csv_fields(lines, *, check_stop, batch_rows=_DEFAULT_BATCH_ROWS):
    """Yield CSV rows, using Polars only for bounded canonical physical records.

    Quoted, multiline, blank, ragged, wide, or otherwise ambiguous batches are
    replayed through strict ``csv.reader`` from the beginning of that unread
    batch. Previously yielded batches are never replayed.
    """
    if type(batch_rows) is not int or not 1 <= batch_rows <= _MAX_BATCH_ROWS:
        raise ValueError("batch_rows must be between 1 and 8192")
    source = iter(_checked_lines(lines, check_stop))
    try:
        import polars as pl
    except ImportError:
        yield from csv.reader(source, strict=True)
        return

    pending = _NO_PENDING
    first = True
    canonical_width = None
    while True:
        limit = min(_PROBE_ROWS, batch_rows) if first else batch_rows
        batch = []
        buffered_bytes = 0
        unsupported = False

        while len(batch) < limit:
            try:
                line = next(source) if pending is _NO_PENDING else pending
            except StopIteration:
                pending = _NO_PENDING
                break
            pending = _NO_PENDING
            if not isinstance(line, str):
                pending = line
                unsupported = True
                break
            body = _physical_body(line)
            if (
                not body
                or '"' in line
                or "\r" in body
                or "\n" in body
                or "\0" in body
                or body.startswith("\ufeff")
                or line.count(",") + 1 > _MAX_COLUMNS
            ):
                pending = line
                unsupported = True
                break
            try:
                encoded_size = len(line.encode("utf-8"))
            except UnicodeEncodeError:
                pending = line
                unsupported = True
                break
            if batch and buffered_bytes + encoded_size > _BATCH_BYTES:
                pending = line
                break
            batch.append(line)
            buffered_bytes += encoded_size

        if unsupported:
            yield from _fallback(batch, pending, source)
            return
        if not batch:
            if pending is not _NO_PENDING:
                yield from _fallback(batch, pending, source)
            return

        try:
            parsed = _polars_rows(pl, batch)
        except pl.exceptions.PolarsError:
            parsed = None
        if parsed is None:
            yield from _fallback(batch, pending, source)
            return
        width, rows = parsed
        if canonical_width is not None and width != canonical_width:
            yield from _fallback(batch, pending, source)
            return
        canonical_width = width

        if check_stop is not None:
            check_stop()
        for index, row in enumerate(rows):
            if index % 256 == 0 and check_stop is not None:
                check_stop()
            yield list(row)
        first = False
