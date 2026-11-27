"""Bounded threaded HTTP server for loopback listeners."""

import socket
import threading
import time
from http.server import HTTPServer

from omega.domain.errors import StorageError


class BoundedHTTPServer(HTTPServer):
    allow_reuse_address = False
    request_queue_size = 8

    def __init__(self, *args, **kwargs):
        self.slots = threading.BoundedSemaphore(8)
        self.workers = set()
        self.workers_lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def server_bind(self):
        # HTTPServer's reverse hostname lookup is unnecessary for a loopback service.
        socket.socket.bind(self.socket, self.server_address)
        self.server_name = "127.0.0.1"
        self.server_port = self.socket.getsockname()[1]

    def process_request(self, request, client_address):
        self._reap_workers()
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        worker = None
        try:
            request.settimeout(5)
            worker = threading.Thread(
                target=self._worker, args=(request, client_address), daemon=True
            )
            with self.workers_lock:
                self.workers.add(worker)
            worker.start()
        except Exception:
            if worker is None or worker.ident is None:
                with self.workers_lock:
                    self.workers.discard(worker)
                try:
                    self.shutdown_request(request)
                finally:
                    self.slots.release()
            raise

    def _worker(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except (OSError, TimeoutError):
            pass
        finally:
            self.shutdown_request(request)

    def _reap_workers(self):
        # A worker cannot confirm its own native thread has exited. Keep its
        # reference and admission permit until another thread observes that fact.
        with self.workers_lock:
            finished = [worker for worker in self.workers if worker.is_alive() is False]
            for worker in finished:
                self.workers.remove(worker)
                self.slots.release()

    def server_close(self):
        super().server_close()
        with self.workers_lock:
            workers = tuple(self.workers)
        deadline = time.monotonic() + 6
        for worker in workers:
            worker.join(timeout=max(0, deadline - time.monotonic()))
        self._reap_workers()
        if any(worker.is_alive() is not False for worker in workers):
            raise StorageError("core HTTP request worker cleanup is not confirmed")
