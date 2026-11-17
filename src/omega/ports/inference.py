"""Framework/process/storage boundaries for causal feature inference."""

from typing import Protocol

from omega.domain.inference import (
    InferenceCommitAck,
    InferenceEvidenceReceipt,
    InferenceReceipt,
    InferenceRequest,
    InferenceTransition,
)


class CausalInferenceWorker(Protocol):
    def submit(self, request: InferenceRequest, previous_state_json: str) -> None:
        """Dispatch only after the owner durably saved this exact request intent.

        Host verifies real input/model evidence, then runs its external module in
        a child. An unconfirmed result never permits automatic recurrent replay.
        """
        ...

    def receipt(self, request_id: str) -> InferenceReceipt | None: ...
    def cancel(self, request_id: str) -> None: ...
    def close(self) -> None: ...


class CausalInferenceEvidence(Protocol):
    def verify(self, request: InferenceRequest) -> InferenceEvidenceReceipt:
        """Read/check actual source/model evidence, never just relabel offline metadata.

        Verify every raw minute and actual availability at the decision cutoff,
        original/unrepaired/non-MC provenance, the frozen module/checkpoint/input
        schema, and training feature/label cutoff and model availability. The
        receipt binds the actual frozen input hash to this exact request. Source
        leases/pins and the evidence record remain the composition owner's duty.
        """
        ...


class InferenceStateStore(Protocol):
    def save_snapshot(
        self, snapshot_json: str, *, expected_revision: int | None
    ) -> InferenceCommitAck:
        """Durably CAS open/reset/request/failure metadata before dispatch or publication."""
        ...

    def commit(self, transition: InferenceTransition) -> InferenceCommitAck:
        """CAS base_revision and durably save exactly snapshot_json before returning an ACK.

        Unknown writes must never return durable=True. Recovery reads and compares
        the actual stored hash; an ACK is not a guess based on an intended write.
        """
        ...

    def read(self) -> str | None:
        """Return the actual bounded, checked snapshot JSON, or fail explicitly."""
        ...

    def close(self) -> None: ...
