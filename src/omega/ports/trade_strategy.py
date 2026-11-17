"""A TS family proposes orders; the common executor never assumes TEF or exposure."""

from typing import Protocol

from omega.domain.trading import TradeContext, TradeDecision
from omega.ports.modules import ModuleDescriptor


class TradeStrategy(Protocol):
    descriptor: ModuleDescriptor

    def evaluate(self, context: TradeContext) -> TradeDecision: ...

    def snapshot(self) -> dict:
        """Return a detached JSON-compatible state for diagnostics/checkpointing."""
        ...

    def resume(self) -> None:
        """Apply explicit user resume to module-local latches; common risk gates remain separate."""
        ...


class CheckpointableTradeStrategy(TradeStrategy, Protocol):
    """Durable execution capability required by the virtual margin host.

    Checkpoints are detached finite JSON data. Restoring the same checkpoint
    must reproduce the exact decision cursor and model state; it performs no
    exchange, filesystem or account mutation. A plugin may still implement the
    base protocol for non-durable probes, but cannot silently opt into this host.
    """

    def checkpoint(self) -> dict: ...

    def restore(self, checkpoint: dict) -> None: ...


class ProtectionTradeStrategy(CheckpointableTradeStrategy, Protocol):
    """Optional capability required when a host can withhold external inputs.

    The host supplies current market/account state with entries disabled. The
    module must not read an absent external signal, and may only hold, cancel
    an owned pending order, or reduce the observed position. Normal execution
    validation still applies; this capability does not grant extra authority.
    """

    def evaluate_protection(self, context: TradeContext) -> TradeDecision: ...


class ProtectionPreviewStrategy(TradeStrategy, Protocol):
    """Optional detached preview; this does not claim durable cursor advancement.

    Hosts call it only on a clone to decide whether immediate financial work must
    bypass an external-signal wait. Committed decisions still use the module's
    ordinary evaluate/evaluate_protection path and their durable checkpoint.
    """

    def preview_protection(self, context: TradeContext) -> TradeDecision: ...
