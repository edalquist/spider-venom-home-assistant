"""Base entities for Spider Grills Venom."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import CONNECTION_NETWORK_MAC, DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_THING_NAME, DOMAIN
from .coordinator import SpiderVenomDataUpdateCoordinator, value_at_path


class SpiderVenomEntity(CoordinatorEntity[SpiderVenomDataUpdateCoordinator]):
    """Base Spider Grills Venom entity."""

    _attr_has_entity_name = True
    _available_when_offline: bool = False

    def __init__(
        self,
        coordinator: SpiderVenomDataUpdateCoordinator,
        entry: ConfigEntry,
        key: str,
    ) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        self._thing_name = entry.data[CONF_THING_NAME]
        self._device_name = entry.title or "Venom"
        self._attr_unique_id = f"{self._thing_name}_{key}"

    @property
    def available(self) -> bool:
        """Return True if the coordinator is available and device is online when required."""
        if not super().available:
            return False
        if self._available_when_offline:
            return True
        return self.coordinator.is_device_online

    @property
    def device_info(self) -> DeviceInfo:
        """Return device registry information."""
        reported = self.coordinator.reported
        connections: set[tuple[str, str]] = set()
        mac = reported.get("mac")
        if isinstance(mac, str) and mac:
            connections.add((CONNECTION_NETWORK_MAC, mac))

        return DeviceInfo(
            identifiers={(DOMAIN, self._thing_name)},
            connections=connections,
            manufacturer="Spider Grills",
            model=str(reported.get("model") or "Venom"),
            name=self._device_name,
            sw_version=str(reported.get("vers")) if reported.get("vers") else None,
        )


def path_value_fn(path: tuple[str, ...]) -> Callable[[dict[str, Any]], Any]:
    """Return a function that extracts a nested value at path."""
    return lambda reported: value_at_path(reported, path)
