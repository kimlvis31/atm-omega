"""Bounded fair market-event admission, separate from arbitrary-ID controls."""

from omega.domain.errors import StorageError
from omega.domain.inference import digest


class CausalFeatureEvents:
    def __init__(self, provider):
        self.provider = provider
        self.pending, self.by_request, self.stopped = {}, {}, {}

    @staticmethod
    def _stream_key(binding, epochs):
        return binding.binding_sha256, epochs

    def sync_locked(self, current):
        valid = {self._stream_key(row["binding"], row["epochs"]) for row in current.values()}
        self.stopped = {key: reason for key, reason in self.stopped.items() if key in valid}
        self.pending = {
            key: event
            for key, event in self.pending.items()
            if self._stream_key(event["binding"], event["epochs"]) in valid
            and not event.get("cancelled")
        }
        self.by_request = {
            key: event for key, event in self.by_request.items() if event["key"] in self.pending
        }

    def allowed_locked(self, binding, epochs):
        return self._stream_key(binding, epochs) not in self.stopped

    def offer(self, binding, candle):
        provider, service = self.provider, self.provider.service
        enqueue = None
        now = service.clock_ms()
        if not provider.lock.acquire(blocking=False):
            return provider._unavailable_now(binding, candle, now, "authority_cache_busy")
        try:
            self.pending = {
                key: event for key, event in self.pending.items() if not event.get("cancelled")
            }
            delivery = provider._lookup(binding, candle, now)
            if delivery.disposition in {"ready", "no_signal", "expired"}:
                return delivery
            current = provider._live(binding)
            if current is None:
                return delivery
            key = digest(
                {
                    "binding": binding.binding_sha256,
                    "epochs": current["epochs"],
                    "instrument": binding.instrument.key,
                    "open_time_ms": candle.open_time_ms,
                }
            )
            event = self.pending.get(key)
            if event is not None:
                reason = (
                    "event_already_pending"
                    if event["candle"] == candle
                    else "event_candle_conflict"
                )
                return provider._empty_delivery(
                    binding,
                    candle,
                    now,
                    reason,
                    disposition="pending" if event["candle"] == candle else "unavailable",
                )
            if any(
                self._stream_key(row["binding"], row["epochs"])
                == self._stream_key(binding, current["epochs"])
                for row in self.pending.values()
            ):
                # A queued event already owns this recurrent stream even before
                # the actor has created its durable gate pending request. Do not
                # dispatch a second minute against the same previous state.
                return provider._empty_delivery(
                    binding, candle, now, "stream_event_busy", disposition="unavailable"
                )
            if current["pending_open_ms"] is not None:
                return provider._empty_delivery(
                    binding,
                    candle,
                    now,
                    "existing_pending_request"
                    if current["pending_open_ms"] == candle.open_time_ms
                    else "stream_busy_gap_requires_manual_reset",
                    disposition="pending"
                    if current["pending_open_ms"] == candle.open_time_ms
                    else "unavailable",
                )
            last = current["last_open_ms"]
            if last is not None and candle.open_time_ms <= last:
                return provider._empty_delivery(binding, candle, now, "event_already_consumed")
            kind = "event"
            if last is not None and candle.open_time_ms != last + 60000:
                self.stopped[self._stream_key(binding, current["epochs"])] = (
                    "stream_gap_manual_reset"
                )
                kind = "gap"
            if len(self.pending) >= 128:
                return provider._empty_delivery(binding, candle, now, "event_mailbox_capacity")
            event = {
                "kind": kind,
                "key": key,
                "binding": binding,
                "candle": candle,
                "epochs": current["epochs"],
                "request_id": None,
            }
            self.pending[key] = event
            enqueue = event
            result = provider._empty_delivery(
                binding,
                candle,
                now,
                "stream_gap_manual_reset" if kind == "gap" else "event_queued",
                disposition="unavailable" if kind == "gap" else "pending",
            )
        finally:
            provider.lock.release()
        if not service._enqueue_feature_event(enqueue):
            enqueue["cancelled"] = True
            return provider._unavailable_now(binding, candle, now, "event_mailbox_unavailable")
        return result

    def _finish(self, event, *, reason=None):
        with self.provider.lock:
            self.pending.pop(event["key"], None)
            if event["request_id"]:
                self.by_request.pop(event["request_id"], None)
            if reason is not None:
                self.stopped[self._stream_key(event["binding"], event["epochs"])] = reason

    def _block_actor(self, event, reason):
        service = self.provider.service
        row = service._data["deployments"].get(event["binding"].instance_id)
        if row is not None and service.active == row["id"]:
            row["uncertain_streams"] = sorted(
                set(row["uncertain_streams"]) | {event["binding"].stream}
            )
            service._refresh_streams(row)
            service._save()
        self._finish(event, reason=reason)

    def run_actor(self, event):
        provider, service = self.provider, self.provider.service
        provider._actor()
        with provider.lock:
            current = provider._current.get(event["binding"].binding_sha256)
            if (
                event["key"] not in self.pending
                or current is None
                or current["epochs"] != event["epochs"]
            ):
                self._finish(event)
                return
        if event["kind"] == "gap":
            self._block_actor(event, "stream_gap_manual_reset")
            return
        if service._now() - event["candle"].close_time_ms > 120000:
            self._block_actor(event, "queued_event_stale_manual_reset")
            return
        service._dispatch_feature_event = event
        try:
            binding, candle = event["binding"], event["candle"]
            service._predict(
                {
                    "deployment_id": binding.instance_id,
                    "stream": binding.stream,
                    "instrument": binding.instrument.key,
                    "open_time_ms": candle.open_time_ms,
                    "timeout_ms": 30000,
                },
                None,
            )
            request_id = event["request_id"]
            if request_id is None:
                raise StorageError("internal event lacks its durable inference request")
            with provider.lock:
                self.by_request[request_id] = event
        except StorageError:
            self._finish(event, reason="internal_recording_unconfirmed")
            service._fault("EXTERNAL_FEATURE_RECORDING_UNCONFIRMED")
        except Exception:
            self._block_actor(event, "internal_inference_unavailable_manual_reset")
        finally:
            service._dispatch_feature_event = None

    def poll_actor(self, request_id):
        provider, service = self.provider, self.provider.service
        event = self.by_request.get(request_id)
        if event is None:
            return
        request = service._requests[request_id]
        try:
            feature = service.owner.poll(request_id)
            state = service.owner.snapshot()["state"]
            if feature is None and request_id in state["pending"]:
                return
            if feature is not None or service._is_committed_no_signal(request, state):
                provider._accepted_actor(request_id, feature)
                self._finish(event)
                row = service._data["deployments"][service.active]
                service._refresh_streams(row)
                service._save()
            else:
                self._block_actor(event, "internal_deadline_or_result_unknown")
        except StorageError:
            self._finish(event, reason="internal_recording_unconfirmed")
            service._fault("EXTERNAL_FEATURE_RECORDING_UNCONFIRMED")
        except Exception:
            self._block_actor(event, "internal_result_unavailable_manual_reset")
        finally:
            if event["key"] not in self.pending:
                service._pending.pop(request_id, None)
                service._requests.pop(request_id, None)
                provider._forgot_actor(request_id)

    def cancel_actor(self, request_id):
        event = self.by_request.get(request_id)
        if event is not None:
            self._finish(event, reason="internal_event_cancelled_manual_reset")
