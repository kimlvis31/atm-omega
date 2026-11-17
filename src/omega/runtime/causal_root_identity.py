"""Observe the same managed local Root lease, without publishing machine IDs.

This is not a child/model cleanup receipt. Missing identity fails closed for
cold adoption. It does not interfere with normal startup or live recovery.
"""

import os
import re
import stat
from pathlib import Path
from uuid import UUID

from omega.domain.inference import digest
from omega.runtime.control import InstanceLock


def _machine_scope():
    if os.name == "nt":
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
                0,
                winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
            ) as key:
                value, kind = winreg.QueryValueEx(key, "MachineGuid")
            if kind != winreg.REG_SZ or type(value) is not str or len(value) > 64:
                return None
            parsed = UUID(value)
            return None if parsed.int == 0 else ("os_instance", parsed.hex)
        except (OSError, ValueError, ImportError):
            return None
    if os.name == "posix":
        for name in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
            try:
                with Path(name).open("r", encoding="ascii") as source:
                    value = source.read(257).strip().lower()
                if re.fullmatch(r"[a-f0-9]{32}", value) and int(value, 16):
                    return "os_instance", value
            except (OSError, UnicodeError):
                pass
        # Minimal Linux containers may have no OS-instance ID. This fallback
        # permits only same-boot comparison; it cannot assert cross-boot identity.
        try:
            with Path("/proc/sys/kernel/random/boot_id").open("r", encoding="ascii") as source:
                value = source.read(65).strip()
            parsed = UUID(value)
            return None if parsed.int == 0 else ("same_boot_only", parsed.hex)
        except (OSError, ValueError, UnicodeError):
            return None
    return None


def _named_identity(path):
    observed = path.stat()
    return observed.st_dev, observed.st_ino


def capture_managed_root_identity(root, lease, *, original_stream=None):
    """Only the actual still-open original lock and same current path qualify."""
    if type(lease) is not InstanceLock:
        return None
    stream = lease.stream if original_stream is None else original_stream
    try:
        root = Path(root).absolute()
        path = root / "state/core.lock"
        if (
            lease.stream is not stream
            or stream.closed is not False
            or Path(stream.name).absolute() != path
        ):
            return None
        opened, named = os.fstat(stream.fileno()), _named_identity(path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_ino <= 0
            or (opened.st_dev, opened.st_ino) != named
        ):
            return None
        machine = _machine_scope()
        if machine is None:
            return None
        return digest(
            {
                "schema_version": 1,
                "scope": "managed_local_root_identity",
                "platform": os.name,
                "machine_scope_kind": machine[0],
                "machine_sha256": digest(machine[1]),
                "root_path_sha256": digest(os.path.normcase(str(root))),
                "lock_device": hex(opened.st_dev),
                "lock_inode": hex(opened.st_ino),
            }
        )
    except (OSError, ValueError, TypeError, AttributeError):
        return None
