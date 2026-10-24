"""Reason-scoped entry gates. Incident acknowledgement never changes this state."""

from enum import StrEnum

from omega.domain.errors import TradingBlocked


class BlockReason(StrEnum):
    DATA = "DATA_NOT_READY"
    PAUSE = "MANUAL_PAUSE"
    RESTART = "RESTART_MANUAL"
    STORAGE = "STORAGE_RECOVERY_MANUAL"
    MODEL = "MODEL_RECOVERY_MANUAL"
    EXECUTION = "EXECUTION_RECOVERY_MANUAL"
    EXTERNAL = "EXTERNAL_INTERVENTION"


class EntryGate:
    def __init__(self):
        self._reasons = {BlockReason.RESTART}
        self._recovered: set[BlockReason] = set()

    @property
    def reasons(self) -> frozenset[BlockReason]:
        return frozenset(self._reasons)

    @property
    def allows_new_risk(self) -> bool:
        return not self._reasons

    def block(self, reason: BlockReason) -> None:
        self._reasons.add(reason)
        self._recovered.discard(reason)

    def recovered(self, reason: BlockReason) -> None:
        if reason == BlockReason.EXTERNAL:
            raise TradingBlocked("external position needs explicit control reclaim")
        if reason == BlockReason.DATA:
            self._reasons.discard(reason)
        elif reason in self._reasons:
            self._recovered.add(reason)

    def resume(self, *, reconciled: bool, durable: bool) -> None:
        unresolved = self._reasons - self._recovered - {BlockReason.PAUSE}
        if unresolved or not reconciled or not durable:
            raise TradingBlocked("readiness, reconciliation and durable control are required")
        self._reasons.clear()
        self._recovered.clear()

    def reclaim(self, *, reconciled: bool, durable: bool, strategy_ready: bool) -> None:
        if not (reconciled and durable and strategy_ready):
            raise TradingBlocked("position control reclaim is not ready")
        self._reasons.discard(BlockReason.EXTERNAL)
        self._reasons.add(BlockReason.PAUSE)  # Reclaim does not silently resume entries.
