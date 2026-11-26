"""Binary sensor entities for Spider Grills Venom."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import SpiderVenomConfigEntry, SpiderVenomDataUpdateCoordinator
from .entity import SpiderVenomEntity, path_value_fn


@dataclass(frozen=True, kw_only=True)
class SpiderVenomBinarySensorDescription(BinarySensorEntityDescription):
    """Description of a Spider Grills Venom binary sensor."""

    value_fn: Callable[[dict[str, Any]], Any]


BINARY_SENSORS: tuple[SpiderVenomBinarySensorDescription, ...] = (
    SpiderVenomBinarySensorDescription(
        key="door",
        translation_key="door",
        device_class=BinarySensorDeviceClass.OPENING,
        value_fn=path_value_fn(("doorOpn",)),
    ),
    SpiderVenomBinarySensorDescription(
        key="heating",
        translation_key="heating",
        device_class=BinarySensorDeviceClass.HEAT,
        value_fn=path_value_fn(("heat", "t2", "heating")),
    ),
    SpiderVenomBinarySensorDescription(
        key="fahrenheit_mode",
        translation_key="fahrenheit_mode",
        value_fn=path_value_fn(("fah",)),
    ),
    SpiderVenomBinarySensorDescription(
        key="high_temp_notification",
        translation_key="high_temp_notification",
        device_class=BinarySensorDeviceClass.PROBLEM,
        value_fn=path_value_fn(("notifications", "high_temp")),
    ),
    SpiderVenomBinarySensorDescription(
        key="low_temp_notification",
        translation_key="low_temp_notification",
        device_class=BinarySensorDeviceClass.PROBLEM,
        value_fn=path_value_fn(("notifications", "low_temp")),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: SpiderVenomConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Spider Grills Venom binary sensors."""
    coordinator = entry.runtime_data
    async_add_entities(
        SpiderVenomBinarySensor(coordinator, entry, description)
        for description in BINARY_SENSORS
    )


class SpiderVenomBinarySensor(SpiderVenomEntity, BinarySensorEntity):
    """Spider Grills Venom binary sensor."""

    entity_description: SpiderVenomBinarySensorDescription

    def __init__(
        self,
        coordinator: SpiderVenomDataUpdateCoordinator,
        entry: SpiderVenomConfigEntry,
        description: SpiderVenomBinarySensorDescription,
    ) -> None:
        """Initialize the binary sensor."""
        super().__init__(coordinator, entry, description.key)
        self.entity_description = description

    @property
    def is_on(self) -> bool | None:
        """Return the binary sensor state."""
        value = self.entity_description.value_fn(self.coordinator.reported)
        return bool(value) if value is not None else None
