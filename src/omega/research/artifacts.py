"""Portable, atomic JSON artifacts shared by offline workers and viewers."""

# Imports
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from omega.infrastructure.atomic_files import replace_atomic

# Constants
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024


def validate_artifact_path(path: Path, *, allow_missing: bool = False) -> Path:
    """Check existing ancestors before any resolution can hide a reparse point.

    Parent directories remain trusted. This is not protection against every
    adversarial ancestor replacement race; actual opened files are checked too.
    """
    if type(allow_missing) is not bool:
        raise ValueError("allow_missing must be boolean")
    if ".." in Path(path).parts:
        raise ValueError("artifact paths cannot contain parent traversal")
    path = Path(os.path.abspath(path))
    for current in (path, *path.parents):
        try:
            details = current.lstat()
        except FileNotFoundError:
            if allow_missing:
                continue
            raise
        if stat.S_ISLNK(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("artifact paths cannot contain symlinks/reparse points")
    return path


def _artifact_limit(max_bytes):
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_ARTIFACT_BYTES:
        raise ValueError("artifact byte limit must be within [1,512 MiB]")


def _artifact_identity(details):
    # Windows fstat/lstat expose different ctime semantics for replaced files.
    # Compare ctime only between two reads of the same opened descriptor.
    return details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns


@contextmanager
def _bounded_artifact_stream(path, max_bytes):
    _artifact_limit(max_bytes)
    path = validate_artifact_path(path)
    descriptor = _status_descriptor(path)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("artifact must be a regular non-reparse file")
        if details.st_size > max_bytes:
            raise ValueError("artifact exceeds its byte capacity")
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        with stream:
            yield stream
            current = os.fstat(stream.fileno())
            path_current = validate_artifact_path(path).lstat()
            if (
                current.st_ctime_ns != details.st_ctime_ns
                or _artifact_identity(current) != _artifact_identity(details)
                or _artifact_identity(path_current) != _artifact_identity(details)
            ):
                raise ValueError("artifact changed while reading")
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _bounded_artifact_chunks(stream, max_bytes):
    count = 0
    while chunk := stream.read(min(1024 * 1024, max_bytes - count + 1)):
        count += len(chunk)
        if count > max_bytes:
            raise ValueError("artifact grew beyond its byte capacity")
        yield chunk


def bounded_artifact_hash(path: Path, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> str:
    """Hash one actual regular file, including an explicit streaming growth cap."""
    digest = hashlib.sha256()
    with _bounded_artifact_stream(path, max_bytes) as stream:
        for chunk in _bounded_artifact_chunks(stream, max_bytes):
            digest.update(chunk)
    return digest.hexdigest()


def copy_bounded_artifact(
    source: Path, destination: Path, *, max_bytes: int = MAX_ARTIFACT_BYTES
) -> str:
    """Exclusively copy a bounded snapshot; never replace existing destination bytes."""
    destination = validate_artifact_path(destination, allow_missing=True)
    digest = hashlib.sha256()
    with _bounded_artifact_stream(source, max_bytes) as stream, destination.open("xb") as target:
        details = os.fstat(target.fileno())
        if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("snapshot destination must be a regular non-reparse file")
        for chunk in _bounded_artifact_chunks(stream, max_bytes):
            target.write(chunk)
            digest.update(chunk)
        target.flush()
        os.fsync(target.fileno())
        validate_artifact_path(destination)
    return digest.hexdigest()


def content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def child_path(root: Path, name: str) -> Path:
    """Reject absolute paths, parent traversal and symlink escape."""
    root = root.resolve()
    path = (root / name).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError("artifact path must stay below its run directory")
    return path


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(data, stream, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        replace_atomic(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _status_descriptor(path: Path) -> int:
    if os.name != "nt":
        return os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    # Import the Windows API only when needed. Permit DELETE sharing, and keep
    # the handle short-lived: Windows replacement may still need the existing
    # bounded writer retry until this read finishes.
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    handle = create_file(str(path), 0x80000000, 7, None, 3, 0x00200000, None)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close_handle(handle)
        raise


def read_status_json(path: Path) -> Any:
    """Read one actual regular status file without following its final symlink.

    Inspect the opened descriptor, not a separate path predicate. This does not
    invent metadata when a file is missing or protect untrusted parent folders.
    ResearchService owns and validates the jobs directory separately.
    """
    descriptor = _status_descriptor(path)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("job metadata must be a regular non-reparse file")
        if details.st_size > 2_097_152:
            raise ValueError("job metadata exceeds capacity")
        # fdopen owns the descriptor after construction, including on read errors.
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        with stream:
            raw = stream.read(2_097_153)
        if len(raw) > 2_097_152:
            raise ValueError("job metadata exceeds capacity")
        return json.loads(raw.decode("utf-8"))
    finally:
        if descriptor is not None:
            os.close(descriptor)


class Cancelled(Exception):
    """Cooperative cancellation, distinct from a failed computation."""


def check_cancel(cancel: Any = None) -> None:
    if cancel is not None and cancel.is_set():
        raise Cancelled("job cancelled")
