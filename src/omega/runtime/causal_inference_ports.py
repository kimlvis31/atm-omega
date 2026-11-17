"""One causal actor's dispatch correlation; no financial or model execution."""


class ObservedInferenceWorker:
    def __init__(self, worker, before_submit, after_receipt=None):
        self.worker, self.before_submit = worker, before_submit
        self.after_receipt = after_receipt
        self.closed = False

    def submit(self, request, previous_state_json):
        self.before_submit(request)
        return self.worker.submit(request, previous_state_json)

    def receipt(self, identity):
        value = self.worker.receipt(identity)
        if value is not None and self.after_receipt is not None:
            self.after_receipt(value)
        return value

    def cancel(self, identity):
        return self.worker.cancel(identity)

    def close(self):
        result = self.worker.close()
        self.closed = True  # Only a confirmed successful port ACK reaches here.
        return result

    def retirement_status(self):
        probe = getattr(self.worker, "retirement_status", None)
        if callable(probe):
            return probe()
        return {"closed": self.closed, "worker_alive": not self.closed, "confirmed": self.closed}

    def transport_status(self):
        probe = getattr(self.worker, "transport_status", None)
        return probe() if callable(probe) else None
