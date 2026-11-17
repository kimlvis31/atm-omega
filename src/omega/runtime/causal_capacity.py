"""Startup-only host evidence limits; no filesystem/model work or retention policy."""

from dataclasses import dataclass

from omega.domain.errors import ValidationError


@dataclass(frozen=True, slots=True)
class EvidenceBudgetLimits:
    max_bytes: int = 536870912
    max_records: int = 100000
    max_pins: int = 2048
    warning_percent: int = 80

    def __post_init__(self):
        for name, lower, upper in (
            ("max_bytes", 4 * 1024 * 1024, 8 * 1024 * 1024 * 1024),
            ("max_records", 16, 1000000),
            ("max_pins", 1, 65536),
            ("warning_percent", 1, 99),
        ):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise ValidationError(
                    f"neural evidence {name} must be a native integer {lower}..{upper}"
                )

    def document(self):
        return {
            "max_bytes": self.max_bytes,
            "max_records": self.max_records,
            "max_pins": self.max_pins,
            "warning_percent": self.warning_percent,
        }


def evidence_storage_unavailable(limits):
    """Known configuration with unknown accounting, never an invented empty store."""
    return {
        "schema_version": 1,
        "scope": "host_causal_evidence_accounting",
        "state": "unavailable",
        "ready": False,
        "accepts_new_reservation": False,
        "accepts_new_pin": False,
        "limits": limits.document(),
        "charged_bytes": None,
        "charged_records": None,
        "active_pins": None,
        "uncertain_reservations": None,
        "uncertain_reserved_bytes": None,
        "remaining_bytes": None,
        "remaining_records": None,
        "remaining_pins": None,
        "reservation_headroom_bytes": 2 * 1024 * 1024,
        "consumed_receipt_allowance_bytes": limits.max_pins * 2048,
        "consumed_receipt_allowance_records": limits.max_pins,
    }
