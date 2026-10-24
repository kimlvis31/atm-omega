from enum import StrEnum
from typing import Protocol


class Durability(StrEnum):
    PRIMARY = "PRIMARY_COMMITTED"
    FALLBACK = "LOCAL_JOURNALED"
    VOLATILE = "VOLATILE_ONLY"


class Journal(Protocol):
    def append(self, record: dict) -> None: ...

    def records(self) -> tuple[dict, ...]: ...


class RecordSink(Protocol):
    def record(self, event: dict) -> Durability: ...

    def records(self) -> tuple[dict, ...]: ...
