"""Explicit spawn lifecycle for a headless control owner; network-free by default."""

import multiprocessing as mp
import uuid
from multiprocessing.connection import Connection

from omega.domain.safety import BlockReason, EntryGate
from omega.runtime.ipc import receive, send


def _control_owner(connection: Connection):
    gate = EntryGate()
    send(connection, {"id": "ready", "status": "DEGRADED", "gui_started": False})
    try:
        while True:
            message = receive(connection)  # OS wait; no timer-driven empty-queue loop.
            command = message.get("command")
            if command == "PauseNewRisk":
                gate.block(BlockReason.PAUSE)
                send(connection, {"id": message["id"], "applied": True, "durability": "volatile"})
            elif command == "GetSnapshot":
                send(
                    connection,
                    {
                        "id": message["id"],
                        "entry_reasons": sorted(gate.reasons),
                        "allows_new_risk": gate.allows_new_risk,
                    },
                )
            elif command == "ShutdownIdle":
                send(connection, {"id": message["id"], "stopped": True})
                return
            else:
                # Resume/flatten are not implemented by this idle control-only worker.
                send(connection, {"id": message["id"], "error": "UNSUPPORTED_COMMAND"})
    except (EOFError, BrokenPipeError):
        return
    finally:
        connection.close()


class ControlProcess:
    """A network-free control prototype, not the production trading supervisor."""

    def __init__(self):
        self._process = None
        self._connection = None

    def start(self, timeout: float = 5) -> dict:
        if self._process is not None:
            raise RuntimeError("process already started")
        context = mp.get_context("spawn")
        parent, child = context.Pipe(duplex=True)
        self._process = context.Process(target=_control_owner, args=(child,))
        self._process.start()
        child.close()
        self._connection = parent
        try:
            return receive(parent, timeout)
        except Exception:
            self.close()
            raise

    def command(self, command: str, timeout: float = 5) -> dict:
        if self._connection is None:
            raise RuntimeError("process not started")
        request_id = uuid.uuid4().hex
        send(self._connection, {"id": request_id, "command": command})
        # Late replies belong to their original request and are not a new command's ACK.
        import time

        deadline = time.monotonic() + timeout
        while True:
            remaining = max(0, deadline - time.monotonic())
            response = receive(self._connection, remaining)
            if response["id"] == request_id:
                return response

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
        if self._process:
            self._process.join(2)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(2)
            self._process.close()
            self._process = None
