"""Owner configuration provenance reads; never copies financial or model state."""

# Imports
import re
from urllib.parse import quote

from omega.domain.errors import ValidationError
from omega.runtime.margin_configuration import configuration_parameters

SCOPE = "virtual_margin_configuration"
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_FIELDS = {
    "schema_version",
    "scope",
    "state",
    "account_id",
    "configuration_sha256",
    "parameters",
    "reason_code",
    "core_epoch",
}


def configuration_url(account_id: str) -> str:
    if type(account_id) is not str or _ID.fullmatch(account_id) is None:
        raise ValidationError("invalid configuration account identity")
    return "/api/margin-trading/accounts/" + quote(account_id, safe="") + "/configuration"


def validate_configuration(document, *, account_id: str, core_epoch=None) -> dict:
    """Validate transport identity, provenance and bounded plain registration data.

    This is not an activation/financial safety check. A new draft still needs all
    current form, module, binding and ordinary configure checks and fresh consent.
    """
    configuration_url(account_id)
    if type(document) is not dict or set(document) != _FIELDS:
        raise ValidationError("invalid registered configuration response")
    if (
        type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["scope"] != SCOPE
        or document["account_id"] != account_id
    ):
        raise ValidationError("registered configuration identity changed")
    epoch = document["core_epoch"]
    if type(epoch) is not str or re.fullmatch(r"[a-f0-9]{32}", epoch) is None:
        raise ValidationError("invalid configuration core epoch")
    if core_epoch is not None and epoch != core_epoch:
        raise ValidationError("registered configuration belongs to another core epoch")
    state = document["state"]
    if type(state) is not str or state not in {"ready", "unavailable"}:
        raise ValidationError("invalid configuration state")
    reason = document["reason_code"]
    if state == "unavailable":
        if (
            document["parameters"] is not None
            or document["configuration_sha256"] is not None
            or type(reason) is not str
            or re.fullmatch(r"[a-z0-9_]{1,64}", reason) is None
        ):
            raise ValidationError("unavailable configuration must not expose draft data")
        return dict(document)
    if reason is not None:
        raise ValidationError("ready configuration has an error")
    parameters, checksum = configuration_parameters(document["parameters"], account_id=account_id)
    if document["configuration_sha256"] != checksum:
        raise ValidationError("registered configuration checksum changed")
    return {**document, "parameters": parameters}
