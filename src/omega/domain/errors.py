"""Errors crossing application ports carry codes, never signed requests or secrets."""


class OmegaError(Exception):
    pass


class AuthorizationDenied(PermissionError):
    """An explicit access-policy denial, distinct from filesystem permission failure."""


class ValidationError(OmegaError):
    pass


class CommandRejected(ValidationError):
    """A provider guarantees that validation rejected the command before mutation."""


class UnsupportedCapability(OmegaError):
    pass


class TradingBlocked(OmegaError):
    pass


class ProtocolError(OmegaError):
    pass


class TransportError(OmegaError):
    pass


class OrderOutcomeUnknown(OmegaError):
    """Query the original client order ID; never blindly resubmit."""


class BrokerRejected(OmegaError):
    def __init__(self, code: int, status: int):
        self.code = code
        self.status = status
        super().__init__(f"broker rejected request: code={code}, http={status}")


class RateLimited(OmegaError):
    def __init__(self, retry_after: float | None = None):
        self.retry_after = retry_after
        super().__init__("provider request budget exhausted")


class JournalError(OmegaError):
    pass


class StorageError(OmegaError):
    """Persistence failure without DSNs, server messages, or credential contents."""
