"""Internal immutable transport evidence; never a recovery or model-cleanup port."""

# Imports
from dataclasses import dataclass, field

from omega.runtime.causal_process_resources import HostRetirement, QueueRetirement
from omega.runtime.causal_receipt_channel import ReceiptFailure, ReceiptRetirementStatus


def _text(value, maximum, *, values=None):
    if (
        type(value) is not str
        or len(value) > maximum
        or (values is not None and value not in values)
    ):
        raise ValueError("invalid bounded transport evidence string")


def _boolean(value):
    if type(value) is not bool:
        raise ValueError("native transport evidence boolean required")


def _number(value, *, optional=False, signed=False):
    if optional and value is None:
        return
    if type(value) is not int or not (-(2**63) if signed else 0) <= value < 2**63:
        raise ValueError("native bounded transport evidence integer required")


def _codes(value, maximum=128):
    if type(value) is not tuple or len(value) > maximum:
        raise ValueError("bounded immutable transport diagnostics required")
    for code in value:
        _text(code, 256)


def validate_host_retirement(value):
    """Validate exact existing receipt leaves without rewriting their evidence."""
    if type(value) is not HostRetirement:
        raise ValueError("exact original host receipt required")
    _text(value.resource_token, 128)
    _number(value.attempt)
    _text(value.status, 32, values={"unknown", "physical_retired"})
    _number(value.pid, optional=True)
    _number(value.exitcode, optional=True, signed=True)
    _number(value.death_observed_at_ns, optional=True)
    for flag in (
        value.process_handle_closed,
        value.ever_uncertain,
        value.process_destructive_unknown,
        value.normal_model_cleanup_confirmed,
        value.all_kernel_resources_confirmed,
    ):
        _boolean(flag)
    _text(value.scope, 128, values={"original_cpython_host_process_feeder_endpoints_v1"})
    if value.normal_model_cleanup_confirmed or value.all_kernel_resources_confirmed:
        raise ValueError("host receipt cannot grant model or all-kernel authority")
    if type(value.queues) is not tuple or len(value.queues) > 2:
        raise ValueError("bounded exact queue receipts required")
    for queue in value.queues:
        if type(queue) is not QueueRetirement:
            raise ValueError("exact original queue receipt required")
        _text(queue.name, 16, values={"incoming", "outgoing"})
        _text(queue.feeder, 16, values={"not_created", "joined", "unknown"})
        for flag in (
            queue.reader_closed,
            queue.writer_closed,
            queue.reader_close_ack,
            queue.writer_close_ack,
        ):
            _boolean(flag)
        _codes(queue.destructive_errors)
    _codes(value.errors)
    if type(value.history) is not tuple or len(value.history) > 16:
        raise ValueError("bounded immutable retirement history required")
    for item in value.history:
        if type(item) is not tuple or len(item) != 2:
            raise ValueError("exact retirement history tuple required")
        _number(item[0])
        _codes(item[1])
    return value


def validate_receipt_retirement(value):
    if type(value) is not ReceiptRetirementStatus:
        raise ValueError("exact original reader receipt required")
    for flag in (
        value.closed,
        value.started,
        value.reader_alive,
        value.reader_done,
        value.join_confirmed,
    ):
        _boolean(flag)
    if value.error is not None:
        if type(value.error) is not ReceiptFailure:
            raise ValueError("exact bounded reader failure required")
        _text(value.error.kind, 16, values={"eof", "transport", "overflow"})
        _text(value.error.error_type, 96)
        # Existing raw bounded diagnostics are internal-only, possibly with paths.
        _text(value.error.message, 384)
    return value


def host_physical_confirmed(value):
    validate_host_retirement(value)
    return (
        value.status == "physical_retired"
        and value.process_handle_closed
        and value.pid is not None
        and value.exitcode is not None
        and value.death_observed_at_ns is not None
        and not value.process_destructive_unknown
        and not value.errors
        and tuple(queue.name for queue in value.queues) == ("incoming", "outgoing")
        and all(
            queue.reader_closed
            and queue.writer_closed
            and queue.reader_close_ack
            and queue.writer_close_ack
            and queue.feeder in {"joined", "not_created"}
            and not queue.destructive_errors
            for queue in value.queues
        )
    )


@dataclass(frozen=True, slots=True)
class WorkerTransportRetirement:
    worker_token: str
    channel_binding_token: str | None
    identity_intact: bool
    admission_closed: bool
    start_attempted: bool
    never_started_confirmed: bool
    ownership_transfer_confirmed: bool
    host: HostRetirement | None
    receipt: ReceiptRetirementStatus | None
    physical_confirmed: bool
    normal_model_cleanup: str
    errors: tuple[str, ...]
    all_kernel_resources_confirmed: bool = field(default=False, init=False)
    actor_recovery_authorized: bool = field(default=False, init=False)

    def __post_init__(self):
        validate_worker_transport_retirement(self)


def validate_worker_transport_retirement(value):
    if type(value) is not WorkerTransportRetirement:
        raise ValueError("exact worker transport evidence required")
    _text(value.worker_token, 128)
    if not value.worker_token:
        raise ValueError("original worker token required")
    if value.channel_binding_token is not None:
        _text(value.channel_binding_token, 128)
        if not value.channel_binding_token:
            raise ValueError("original channel binding token required")
    for flag in (
        value.identity_intact,
        value.admission_closed,
        value.start_attempted,
        value.never_started_confirmed,
        value.ownership_transfer_confirmed,
        value.physical_confirmed,
        value.all_kernel_resources_confirmed,
        value.actor_recovery_authorized,
    ):
        _boolean(flag)
    if value.all_kernel_resources_confirmed or value.actor_recovery_authorized:
        raise ValueError("worker evidence cannot authorize recovery or all-kernel cleanup")
    _text(value.normal_model_cleanup, 16, values={"not_started", "unconfirmed"})
    _codes(value.errors, 16)
    if value.host is not None:
        validate_host_retirement(value.host)
    if value.receipt is not None:
        validate_receipt_retirement(value.receipt)
    no_start = (
        value.identity_intact
        and value.admission_closed
        and not value.start_attempted
        and not value.ownership_transfer_confirmed
        and value.host is value.receipt is None
        and value.channel_binding_token is None
        and value.normal_model_cleanup == "not_started"
        and not value.errors
    )
    if value.never_started_confirmed and not no_start:
        raise ValueError("never-started evidence has inconsistent acquisition facts")
    if value.physical_confirmed:
        started = (
            value.identity_intact
            and value.admission_closed
            and value.start_attempted
            and not value.never_started_confirmed
            and value.ownership_transfer_confirmed
            and value.channel_binding_token is not None
            and value.host is not None
            and host_physical_confirmed(value.host)
            and value.receipt is not None
            and value.receipt.closed
            and value.receipt.started
            and value.receipt.reader_done
            and value.receipt.join_confirmed
            and not value.receipt.reader_alive
            and value.normal_model_cleanup == "unconfirmed"
            and not value.errors
        )
        if not ((value.never_started_confirmed and no_start) or started):
            raise ValueError("physical worker evidence has incomplete original acknowledgements")
    return value
