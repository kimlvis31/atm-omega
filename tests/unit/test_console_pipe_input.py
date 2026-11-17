"""Exclusive pipe ownership, complete commands and confirmed input retirement."""

import os
import sys
import threading
from collections import deque
from io import StringIO
from types import SimpleNamespace

import pytest

from omega.runtime import console_pipe
from omega.runtime.console import ControlConsole


class Kernel:
    """One exclusive byte pipe; no Windows APIs/real process stdio are touched."""

    def __init__(self, chunks=(), *, eof=True):
        self.chunks = deque(chunks)
        self.eof = eof
        self.standard = 10
        self.pipe = object()
        self.objects = {10: self.pipe, 99: object()}
        self.closed, self.close_attempts, self.set_calls, self.read_sizes = [], [], [], []
        self.fail_duplicate = self.fail_nul = False
        self.fail_close, self.fail_set = set(), set()
        self.peek_entered = threading.Event()
        self.read_hook = None

    def stdin(self):
        return self.standard

    def native_handle(self, stream):
        return stream.native

    def file_type(self, handle):
        return 3 if self.objects.get(handle) is self.pipe else 2

    def duplicate(self, handle):
        if self.fail_duplicate:
            raise OSError("duplicate failed")
        assert handle == 10
        self.objects[20] = self.objects[handle]
        return 20

    def nul(self):
        if self.fail_nul:
            raise OSError("NUL allocation failed")
        self.objects[30] = object()
        return 30

    def set_stdin(self, handle):
        self.set_calls.append(handle)
        if handle in self.fail_set:
            raise OSError("standard handle update failed")
        self.standard = handle

    def same(self, first, second):
        return (
            first in self.objects
            and second in self.objects
            and self.objects[first] is self.objects[second]
        )

    def peek(self, handle):
        assert self.objects[handle] is self.pipe and handle not in self.closed
        self.peek_entered.set()
        return len(self.chunks[0]) if self.chunks else None if self.eof else 0

    def read(self, handle, size):
        assert self.objects[handle] is self.pipe
        assert self.chunks and 0 < size <= len(self.chunks[0])
        self.read_sizes.append(size)
        if self.read_hook:
            self.read_hook()
        first = self.chunks.popleft()
        if len(first) > size:
            self.chunks.appendleft(first[size:])
        return first[:size]

    def close(self, handle):
        assert handle is not None and handle != 10, "the borrowed original must never be closed"
        assert handle != self.standard, "do not close an installed standard handle"
        self.close_attempts.append(handle)
        if handle in self.fail_close:
            raise OSError("owned handle close unconfirmed")
        assert handle not in self.closed
        self.closed.append(handle)
        del self.objects[handle]


@pytest.fixture
def fake(monkeypatch):
    # Each fixture uses fake handles only. Expected unconfirmed owners stay held
    # through assertions; monkeypatch restores the isolated fake module state.
    monkeypatch.setattr(console_pipe, "_owner", None)
    stream = SimpleNamespace(
        isatty=lambda: False, fileno=lambda: 0, native=10, encoding="utf-8", errors="strict"
    )
    streams = SimpleNamespace(stdin=stream, __stdin__=stream, stdout=sys.stdout, stderr=StringIO())
    monkeypatch.setattr(console_pipe, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(console_pipe, "sys", streams)
    kernel = Kernel()
    monkeypatch.setattr(console_pipe, "_Kernel", lambda: kernel)
    return kernel, streams


def prepare(fake, chunks=(), *, eof=True):
    kernel, _streams = fake
    kernel.chunks, kernel.eof = deque(chunks), eof
    return console_pipe.prepare_console_input(stopped=threading.Event())


def test_split_multibyte_crlf_cr_blank_and_final_unterminated_line_preserve_commands(fake):
    encoded = "한글".encode()
    reader = prepare(
        fake,
        [b"alpha\r", b"\n", b"beta\n", encoded[:1], encoded[1:3], encoded[3:] + b"\r", b"\r\nlast"],
    )
    try:
        assert [reader() for _ in range(5)] == ["alpha", "beta", "한글", "", "last"]
        with pytest.raises(EOFError):
            reader()
    finally:
        reader.close()
    state = reader.status()
    assert state["closed"] and state["standard_input_restored"]
    assert state["private_handle_closed"] and state["nul_handle_closed"]
    assert not state["owner_active"] and not state["reader_active"]
    assert fake[0].standard == 10 and sorted(fake[0].closed) == [20, 30]
    reader.close()
    assert fake[0].close_attempts == [20, 30]


def test_oversized_line_is_entirely_discarded_then_same_prompt_accepts_next_valid_command(
    fake, capsys
):
    kernel, streams = fake
    bound = console_pipe.MAX_LINE_CHARACTERS
    assert bound >= 262144
    oversized = b"x" * (2 * bound + 17) + b" command-dangerous-tail\r\nstatus\r\n"
    reader = prepare(fake, [oversized])
    observed = []
    kernel.read_hook = lambda: observed.append(len(reader._text))
    try:
        assert reader("choose> ") == "status"
        output = capsys.readouterr()
        assert output.out.count("choose> ") == 2
        assert "entire line discarded" in streams.stderr.getvalue()
        assert max(observed) <= bound + 4096
        assert max(kernel.read_sizes) <= 4096
    finally:
        reader.close()


def test_original_decoder_error_policy_is_preserved_and_strict_fault_has_no_executable_tail(
    fake, monkeypatch
):
    kernel, streams = fake
    reader = prepare(fake, [b"\xff\nstatus\n"])
    try:
        with pytest.raises(UnicodeDecodeError):
            reader()
        before = list(kernel.read_sizes)
        assert reader.status()["error_type"] == "UnicodeDecodeError"
        with pytest.raises(OSError, match="previously failed"):
            reader()
        assert kernel.read_sizes == before
    finally:
        reader.close()
    streams.stdin.errors = "replace"
    replacement = Kernel()
    monkeypatch.setattr(console_pipe, "_Kernel", lambda: replacement)
    reader = prepare((replacement, streams), [b"\xffok\n"])
    try:
        assert reader() == "\ufffdok"
    finally:
        reader.close()


def test_close_wakes_and_joins_original_waiting_reader_before_handles_close(fake):
    kernel, _streams = fake
    reader = prepare(fake, eof=False)
    outcomes = []

    def consume():
        try:
            reader()
        except BaseException as error:
            outcomes.append(type(error))

    thread = threading.Thread(target=consume)
    thread.start()
    assert kernel.peek_entered.wait(1)
    try:
        with pytest.raises(OSError, match="one original input reader"):
            reader()
        assert reader.status()["reader_active"] and not kernel.closed
    finally:
        reader.close()
        thread.join(1)
    assert outcomes == [EOFError] and not thread.is_alive()
    assert not reader.status()["reader_thread_alive"] and reader.status()["closed"]
    assert sorted(kernel.closed) == [20, 30]


@pytest.mark.parametrize("change", ["standard", "borrowed_original"])
def test_foreign_handle_identity_is_not_overwritten_or_unsafe_closed(fake, change):
    kernel, _streams = fake
    reader = prepare(fake)
    if change == "standard":
        kernel.standard = 99
    else:
        kernel.objects[10] = object()
    expected_standard = kernel.standard
    with pytest.raises(OSError, match="identity changed"):
        reader.close()
    assert kernel.standard == expected_standard and not kernel.closed
    assert reader.status()["state"] == "unconfirmed" and reader.status()["owner_active"]
    before = list(kernel.set_calls)
    with pytest.raises(OSError, match="previously failed"):
        reader.close()
    assert kernel.set_calls == before


@pytest.mark.parametrize("failed_handle", [20, 30])
def test_handle_close_failure_stays_owned_and_is_not_retried_as_success(fake, failed_handle):
    kernel, _streams = fake
    reader = prepare(fake)
    kernel.fail_close.add(failed_handle)
    with pytest.raises(OSError, match="close is unconfirmed"):
        reader.close()
    state = reader.status()
    assert state["standard_input_restored"] and not state["closed"] and state["owner_active"]
    before = list(kernel.close_attempts)
    kernel.fail_close.clear()
    with pytest.raises(OSError, match="previously failed"):
        reader.close()
    assert kernel.close_attempts == before and reader.status()["state"] == "unconfirmed"


def test_restore_failure_preserves_installed_nul_and_all_owned_handles(fake):
    kernel, _streams = fake
    reader = prepare(fake)
    kernel.fail_set.add(10)
    with pytest.raises(OSError):
        reader.close()
    assert kernel.standard == 30 and not kernel.closed
    assert reader.status()["owner_active"] and not reader.status()["standard_input_restored"]


def test_failed_nul_allocation_with_failed_private_close_retains_exact_acquisition_owner(fake):
    kernel, _streams = fake
    kernel.fail_nul, kernel.fail_close = True, {20}
    with pytest.raises(OSError):
        prepare(fake)
    retained = console_pipe._owner
    assert retained is not None and retained.status()["state"] == "unconfirmed"
    assert retained.status()["owner_active"] and not retained.status()["private_handle_closed"]
    assert kernel.standard == 10 and kernel.close_attempts == [20]
    kernel.fail_close.clear()
    with pytest.raises(OSError):
        retained.close()
    assert kernel.close_attempts == [20] and console_pipe._owner is retained


@pytest.mark.parametrize("failure", ["duplicate", "nul", "activate"])
def test_acquisition_failure_does_not_change_borrowed_stdio_or_leak_confirmed_handles(
    fake, failure
):
    kernel, streams = fake
    before = streams.stdin, streams.stdout, streams.stderr
    if failure == "duplicate":
        kernel.fail_duplicate = True
    elif failure == "nul":
        kernel.fail_nul = True
    else:
        kernel.fail_set.add(30)
    with pytest.raises(OSError):
        prepare(fake)
    assert (streams.stdin, streams.stdout, streams.stderr) == before
    assert kernel.standard == 10 and 10 not in kernel.close_attempts
    assert sorted(kernel.closed) == {"duplicate": [], "nul": [20], "activate": [20, 30]}[failure]
    assert console_pipe._owner is None


def test_duplicate_owner_is_rejected_then_normal_close_releases_the_slot(fake):
    kernel, streams = fake
    original = streams.stdin, streams.stdout, streams.stderr
    reader = prepare(fake)
    try:
        assert kernel.standard == 30
        assert (streams.stdin, streams.stdout, streams.stderr) == original
        with pytest.raises(OSError, match="already has an input owner"):
            prepare(fake)
        assert console_pipe._owner is reader
    finally:
        reader.close()
    assert console_pipe._owner is None and kernel.standard == 10


@pytest.mark.parametrize(
    "case", ["non_windows", "tty", "injected", "non_pipe", "wrong_native", "missing"]
)
def test_unqualified_input_is_unchanged_and_acquires_no_handles(fake, monkeypatch, case):
    kernel, streams = fake
    if case == "non_windows":
        monkeypatch.setattr(console_pipe, "os", SimpleNamespace(name="posix"))
    elif case == "tty":
        streams.stdin.isatty = lambda: True
    elif case == "injected":
        streams.stdin = SimpleNamespace()
    elif case == "non_pipe":
        kernel.objects[10] = object()
    elif case == "wrong_native":
        streams.stdin.native = 99
    else:
        streams.stdin = streams.__stdin__ = None
    original = streams.stdin, streams.stdout, streams.stderr
    assert console_pipe.prepare_console_input(stopped=threading.Event()) is None
    assert (streams.stdin, streams.stdout, streams.stderr) == original
    assert not kernel.set_calls and not kernel.close_attempts and set(kernel.objects) == {10, 99}


def test_injected_console_input_function_keeps_existing_plain_contract(tmp_path, monkeypatch):
    def forbidden():
        raise AssertionError("injected console input must not acquire native stdin")

    monkeypatch.setattr(console_pipe, "_Kernel", forbidden)
    commands = iter(("help", "exit"))
    prompts, output = [], []

    def supplied(prompt):
        prompts.append(prompt)
        return next(commands)

    assert ControlConsole(tmp_path, output=output.append).loop(input_fn=supplied) == 0
    assert prompts == ["omega> ", "omega> "]
    assert any("paper only" in value for value in output)


def test_qualified_native_kernel_binding_failure_is_explicit_not_default_input_fallback(
    fake, monkeypatch
):
    kernel, streams = fake
    original = streams.stdin, streams.stdout, streams.stderr

    def unavailable_binding():
        raise AttributeError("required Windows API binding is unavailable")

    monkeypatch.setattr(console_pipe, "_Kernel", unavailable_binding)
    with pytest.raises(AttributeError, match="required Windows API binding"):
        console_pipe.prepare_console_input(stopped=threading.Event())
    assert (streams.stdin, streams.stdout, streams.stderr) == original
    assert not kernel.set_calls and not kernel.close_attempts


@pytest.mark.skipif(os.name != "nt", reason="actual Windows API DLL binding and own handles")
def test_real_windows_kernel_binds_and_compares_only_its_own_nul_handles():
    # No SetStdHandle, original-stdin duplication/read or process/global stream
    # mutation: validate the actual DLL and functions before an expensive spawn.
    streams = sys.stdin, sys.stdout, sys.stderr
    kernel = console_pipe._Kernel()
    standard = kernel.stdin()
    owned = []
    try:
        nul = kernel.nul()
        owned.append(nul)
        duplicate = kernel.duplicate(nul)
        owned.append(duplicate)
        assert nul != duplicate and kernel.same(nul, duplicate)
        assert kernel.stdin() == standard
        assert (sys.stdin, sys.stdout, sys.stderr) == streams
    finally:
        failures = []
        for handle in reversed(owned):
            try:
                kernel.close(handle)
            except OSError as error:
                failures.append(error)
        if failures:
            raise ExceptionGroup("Only test-owned NUL handles failed to close", failures)
    assert kernel.stdin() == standard
    assert (sys.stdin, sys.stdout, sys.stderr) == streams
