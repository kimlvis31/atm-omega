"""Explicit extension points: registering a provider never opens a connection."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum

from omega.domain.errors import UnsupportedCapability, ValidationError
from omega.ports.brokers import Execution, MarketData


class Support(StrEnum):
    SUPPORTED = "SUPPORTED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_SUPPORTED = "NOT_SUPPORTED"
    PERMISSION_REQUIRED = "PERMISSION_REQUIRED"
    UNVERIFIED = "UNVERIFIED"


@dataclass(frozen=True)
class ProviderCapabilities:
    provider_id: str
    fields: Mapping[str, Support]
    execution: Support

    def require(self, field: str) -> None:
        if self.fields.get(field, Support.UNVERIFIED) != Support.SUPPORTED:
            raise UnsupportedCapability(f"required field is unavailable: {field}")


@dataclass(frozen=True)
class ProviderServices:
    capabilities: ProviderCapabilities
    market_data: MarketData | None = None
    execution: Execution | None = None


class ProviderRegistry:
    def __init__(self):
        self._factories: dict[str, Callable[[], ProviderServices]] = {}

    def register(self, provider_id: str, factory: Callable[[], ProviderServices]) -> None:
        if not provider_id or provider_id in self._factories:
            raise ValidationError("provider registration identity is empty or duplicated")
        self._factories[provider_id] = factory

    def create(self, provider_id: str) -> ProviderServices:
        if provider_id not in self._factories:
            raise UnsupportedCapability("provider is not implemented or registered")
        services = self._factories[provider_id]()
        if services.capabilities.provider_id != provider_id:
            raise ValidationError("provider factory returned another provider's services")
        return services
