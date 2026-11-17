"""Exclusive Windows PIPE input without exposing its pending read to children.

The standard Python streams stay untouched. Acquire before starting any console
reader or child, and close after the original ConsoleInput worker is stopped.
"""

import codecs
import os
import sys
import threading
import time

MAX_LINE_CHARACTERS = 262_144
_CHUNK = 4096
_POLL_SECONDS = 0.05
_CLOSE_SECONDS = 2.0
_guard = threading.RLock()
_owner = None


class _Kernel:
    def __init__(self):
        import ctypes
        import msvcrt
        from ctypes import wintypes as W

        self.ctypes, self.W, self.crt = ctypes, W, msvcrt
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        self.identity_api = ctypes.WinDLL("kernelbase", use_last_error=True)
        self._unconfirmed_handle = None
        signatures = {
            "GetStdHandle": ([W.DWORD], W.HANDLE),
            "SetStdHandle": ([W.DWORD, W.HANDLE], W.BOOL),
            "GetFileType": ([W.HANDLE], W.DWORD),
            "GetCurrentProcess": ([], W.HANDLE),
            "DuplicateHandle": (
                [W.HANDLE, W.HANDLE, W.HANDLE, ctypes.POINTER(W.HANDLE), W.DWORD, W.BOOL, W.DWORD],
                W.BOOL,
            ),
            "GetHandleInformation": ([W.HANDLE, ctypes.POINTER(W.DWORD)], W.BOOL),
            "CreateFileW": (
                [W.LPCWSTR, W.DWORD, W.DWORD, ctypes.c_void_p, W.DWORD, W.DWORD, W.HANDLE],
                W.HANDLE,
            ),
            "PeekNamedPipe": (
                [
                    W.HANDLE,
                    ctypes.c_void_p,
                    W.DWORD,
                    ctypes.c_void_p,
                    ctypes.c_void_p,
                    ctypes.c_void_p,
                ],
                W.BOOL,
            ),
            "ReadFile": (
                [W.HANDLE, ctypes.c_void_p, W.DWORD, ctypes.c_void_p, ctypes.c_void_p],
                W.BOOL,
            ),
            "CloseHandle": ([W.HANDLE], W.BOOL),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.api, name)
            function.argtypes, function.restype = arguments, result
        self.identity_api.CompareObjectHandles.argtypes = [W.HANDLE, W.HANDLE]
        self.identity_api.CompareObjectHandles.restype = W.BOOL

    def _check(self, result):
        if not result:
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        return result

    def stdin(self):
        return self.api.GetStdHandle(0xFFFFFFF6)

    def native_handle(self, stream):
        return self.crt.get_osfhandle(stream.fileno())

    def file_type(self, handle):
        return self.api.GetFileType(handle)

    def _noninheritable(self, handle):
        flags = self.W.DWORD()
        try:
            self._check(self.api.GetHandleInformation(handle, self.ctypes.byref(flags)))
            if flags.value & 1:
                raise OSError("console input handle must not be inheritable")
        except BaseException:
            try:
                self.close(handle)
            except BaseException as error:
                self._unconfirmed_handle = handle
                raise OSError("console handle acquisition cleanup is unconfirmed") from error
            raise
        return handle

    def duplicate(self, handle):
        result = self.W.HANDLE()
        process = self.api.GetCurrentProcess()
        self._check(
            self.api.DuplicateHandle(
                process, handle, process, self.ctypes.byref(result), 0, False, 2
            )
        )
        return self._noninheritable(result.value)

    def nul(self):
        handle = self.api.CreateFileW("NUL", 0x80000000, 3, None, 3, 0x80, None)
        if handle in (None, 0, self.ctypes.c_void_p(-1).value):
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        return self._noninheritable(handle)

    def set_stdin(self, handle):
        self._check(self.api.SetStdHandle(0xFFFFFFF6, handle))

    def same(self, first, second):
        return bool(self.identity_api.CompareObjectHandles(first, second))

    def peek(self, handle):
        available = self.W.DWORD()
        if not self.api.PeekNamedPipe(handle, None, 0, None, self.ctypes.byref(available), None):
            if self.ctypes.get_last_error() in (109, 232, 233):
                return None
            raise self.ctypes.WinError(self.ctypes.get_last_error())
        return available.value

    def read(self, handle, size):
        buffer, count = self.ctypes.create_string_buffer(size), self.W.DWORD()
        if not self.api.ReadFile(handle, buffer, size, self.ctypes.byref(count), None):
            error = self.ctypes.get_last_error()
            if error == 234 and count.value:  # Message PIPE: retain the available partial bytes.
                return buffer.raw[: count.value]
            if error in (109, 232, 233):
                return b""
            raise self.ctypes.WinError(error)
        return buffer.raw[: count.value]

    def close(self, handle):
        self._check(self.api.CloseHandle(handle))


class _PipeInput:
    def __init__(self, kernel, original, private, nul, *, encoding, errors, stopped):
        self._kernel, self._original = kernel, original
        self._private, self._nul = private, nul
        self._decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
        self._stopped, self._stop = stopped, threading.Event()
        self._condition, self._close_lock = threading.Condition(), threading.Lock()
        self._reader_thread = None
        self._reading = self._closing = self._closed = False
        self._attempted = self._installed = self._restored = False
        self._input_error = self._cleanup_error = None
        self._text, self._skip_lf, self._eof, self._discarding = "", False, False, False

    def _activate(self):
        global _owner
        with _guard:
            if (_owner is not None and _owner is not self) or self._attempted:
                raise OSError("console PIPE already has an input owner")
            if self._private is None or self._nul is None or self._closed:
                raise OSError("console PIPE handles are not prepared")
            if self._kernel.stdin() != self._original or not self._kernel.same(
                self._original, self._private
            ):
                raise OSError("console standard input changed before activation")
            _owner, self._attempted = self, True
            self._kernel.set_stdin(self._nul)
            if self._kernel.stdin() != self._nul or not self._kernel.same(
                self._original, self._private
            ):
                raise OSError("console PIPE activation identity is unconfirmed")
            self._installed = True
        return self

    def _take(self):
        if self._skip_lf:
            if not self._text and not self._eof:
                return "wait", None
            if self._text.startswith("\n"):
                self._text = self._text[1:]
            self._skip_lf = False
        positions = [
            index for index in (self._text.find("\r"), self._text.find("\n")) if index >= 0
        ]
        end = min(positions) if positions else -1
        if self._discarding or (end if end >= 0 else len(self._text)) > MAX_LINE_CHARACTERS:
            self._discarding = True
            if end >= 0:
                self._skip_lf = self._text[end] == "\r"
                self._text, self._discarding = self._text[end + 1 :], False
                return "discarded", None
            self._text = ""
            if self._eof:
                self._discarding = False
                return "discarded", None
            return "wait", None
        if end >= 0:
            line, self._skip_lf = self._text[:end], self._text[end] == "\r"
            self._text = self._text[end + 1 :]
            return "line", line
        if self._eof:
            if self._text:
                line, self._text = self._text, ""
                return "line", line
            return "eof", None
        return "wait", None

    def __call__(self, prompt=""):
        current = threading.current_thread()
        with self._condition:
            if self._closed or self._closing or self._stop.is_set():
                raise EOFError
            if not self._installed:
                raise OSError("console PIPE input has not been activated")
            if self._input_error:
                raise OSError("console PIPE input previously failed")
            if self._reading or (
                self._reader_thread is not None and self._reader_thread is not current
            ):
                raise OSError("console PIPE accepts one original input reader")
            if current is not threading.main_thread():
                self._reader_thread = current
            self._reading = True
        try:
            print(prompt, end="", flush=True)
            while not self._stop.is_set() and not self._stopped.is_set():
                kind, line = self._take()
                if kind == "line":
                    return line
                if kind == "eof":
                    raise EOFError
                if kind == "discarded":
                    print(
                        "Console input limit exceeded; entire line discarded.",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(prompt, end="", flush=True)
                    continue
                available = self._kernel.peek(self._private)
                if available is None:
                    self._text += self._decoder.decode(b"", final=True)
                    self._eof = True
                elif available:
                    raw = self._kernel.read(self._private, min(available, _CHUNK))
                    self._text += self._decoder.decode(raw, final=not raw)
                    self._eof = not raw
                else:
                    self._stop.wait(_POLL_SECONDS)
            raise EOFError
        except EOFError:
            raise
        except Exception as error:
            self._input_error, self._text = type(error).__name__, ""
            raise
        finally:
            with self._condition:
                self._reading = False
                self._condition.notify_all()

    def status(self):
        with self._condition:
            return {
                "state": "unconfirmed"
                if self._cleanup_error
                else "closed"
                if self._closed
                else "closing"
                if self._closing
                else "active"
                if self._installed
                else "prepared",
                "closed": self._closed,
                "reader_active": self._reading,
                "reader_thread_alive": bool(self._reader_thread and self._reader_thread.is_alive()),
                "standard_input_restored": self._restored,
                "private_handle_closed": self._private is None,
                "nul_handle_closed": self._nul is None,
                "acquisition_unconfirmed": getattr(self._kernel, "_unconfirmed_handle", None)
                is not None,
                "error_type": self._cleanup_error or self._input_error,
                "owner_active": _owner is self,
            }

    def close(self):
        global _owner
        with self._close_lock:
            if self._cleanup_error:
                raise OSError("console PIPE cleanup previously failed; ownership retained")
            if self._closed:
                return
            self._closing = True
            self._stop.set()
            try:
                until = time.monotonic() + _CLOSE_SECONDS
                with self._condition:
                    while self._reading and time.monotonic() < until:
                        self._condition.wait(max(0, until - time.monotonic()))
                    if self._reading:
                        raise OSError("original console input call has not stopped")
                reader = self._reader_thread
                if reader is threading.current_thread():
                    raise OSError("console input worker cannot confirm its own retirement")
                if reader is not None:
                    reader.join(max(0, until - time.monotonic()))
                    if reader.is_alive():
                        raise OSError("original console input worker has not stopped")
                if getattr(self._kernel, "_unconfirmed_handle", None) is not None:
                    raise OSError("console handle acquisition cleanup is unconfirmed")
                if self._attempted:
                    actual = self._kernel.stdin()
                    if actual == self._nul and self._kernel.same(self._original, self._private):
                        self._kernel.set_stdin(self._original)
                        if self._kernel.stdin() != self._original:
                            raise OSError("console standard input restoration is unconfirmed")
                    elif (
                        not self._installed
                        and actual == self._original
                        and self._kernel.same(self._original, self._private)
                    ):
                        pass  # Failed activation never replaced the original slot.
                    else:
                        raise OSError("console standard input identity changed; not overwritten")
                self._restored = True
                errors = []
                for name in ("_private", "_nul"):
                    handle = getattr(self, name)
                    if handle is None:
                        continue
                    try:
                        self._kernel.close(handle)
                        setattr(self, name, None)
                    except OSError as error:
                        errors.append(error)
                if errors:
                    raise OSError("console PIPE handle close is unconfirmed") from errors[0]
                self._closed = True
                with _guard:
                    if _owner is self:
                        _owner = None
            except BaseException as error:
                self._cleanup_error = type(error).__name__
                raise


def prepare_console_input(*, stopped):
    """Detach only the genuine native Windows PIPE; never fall back after mutation."""
    global _owner
    if os.name != "nt":
        return None
    with _guard:
        if _owner is not None:
            raise OSError("console PIPE already has an input owner")
        stream = sys.stdin
        if stream is None or stream is not sys.__stdin__:
            return None
        try:
            if stream.isatty() or stream.fileno() != 0:
                return None
        except (AttributeError, ImportError, OSError, ValueError):
            return None  # A custom/closed stream has no native input contract.
        kernel = _Kernel()  # Native binding/qualification faults must not revive builtin input.
        original = kernel.stdin()
        if not original or original == -1 or kernel.native_handle(stream) != original:
            return None
        if kernel.file_type(original) != 3:
            return None
        if not callable(getattr(stopped, "is_set", None)):
            raise TypeError("console input requires the original stop event")
        encoding, errors = stream.encoding, stream.errors
        codecs.getincrementaldecoder(encoding)(errors=errors)
        codecs.lookup_error(errors)
        reader = _PipeInput(
            kernel, original, None, None, encoding=encoding, errors=errors, stopped=stopped
        )
        _owner = reader
        try:
            reader._private = kernel.duplicate(original)
            reader._nul = kernel.nul()
            return reader._activate()
        except BaseException:
            reader.close()
            raise
