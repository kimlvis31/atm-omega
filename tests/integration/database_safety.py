"""Explicit test-only ports; destination host, database and role stay guarded."""

import os
import re


def _configured_port(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    if not re.fullmatch(r"[1-9][0-9]{3,4}", value):
        raise ValueError(name + " must be a canonical integer port in 1024..65535")
    port = int(value)
    if not 1024 <= port <= 65535:
        raise ValueError(name + " must be a canonical integer port in 1024..65535")
    return port


def isolated_test_port(*, peer=False):
    """Use legacy defaults unless explicitly overridden; never merge destinations."""
    if type(peer) is not bool:
        raise ValueError("peer must be an explicit boolean")
    main = _configured_port("OMEGA_TEST_DB_PORT", 55433)
    other = _configured_port("OMEGA_TEST_PEER_DB_PORT", 55434)
    if main == other:
        raise ValueError("isolated main and peer database ports must differ")
    return other if peer else main
