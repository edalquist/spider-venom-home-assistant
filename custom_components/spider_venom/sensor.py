"""Sensor entities for Spider Grills Venom."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import PERCENTAGE, EntityCategory, UnitOfTemperature
from homeassistant.const import SIGNAL_STRENGTH_DECIBELS_MILLIWATT as DBM
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import SpiderVenomConfigEntry, SpiderVenomDataUpdateCoordinator
from .entity import SpiderVenomEntity, path_value_fn, value_at_path


@dataclass(frozen=True, kw_only=True)
class SpiderVenomSensorDescription(SensorEntityDescription):
    """Description of a Spider Grills Venom sensor."""

    value_fn: Callable[[dict[str, Any]], Any]
    use_root: bool = False


def _numeric_path_value_fn(path: tuple[str, ...]) -> Callable[[dict[str, Any]], int | float | None]:
    """Return a value extractor returning only non-boolean numeric values."""

    def numeric_value(reported: dict[str, Any]) -> int | float | None:
        value = value_at_path(reported, path)
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        return value

    return numeric_value


def _timestamp(path: tuple[str, ...]) -> Callable[[dict[str, Any]], Any]:
    def timestamp_value(reported: dict[str, Any]) -> datetime | None:
        value = value_at_path(reported, path)
        if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
            return None
        try:
            return datetime.fromtimestamp(value, UTC)
        except (OverflowError, OSError, ValueError):
            return None

    return timestamp_value


def _errors(reported: dict[str, Any]) -> str | None:
    errors = reported.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    return ",".join(str(error) for error in errors)[:255]


def _latest_reported_timestamp(shadow: dict[str, Any]) -> datetime | None:
    """Return the newest timestamp from shadow metadata.reported."""
    metadata = shadow.get("metadata")
    reported_meta = metadata.get("reported") if isinstance(metadata, dict) else None
    if not isinstance(reported_meta, dict):
        return None

    latest: float | None = None
    stack: list[Any] = [reported_meta]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            for item in node:
                if isinstance(item, dict | list):
                    stack.append(item)
            continue
        if not isinstance(node, dict):
            continue
        ts = node.get("timestamp")
        if not isinstance(ts, bool) and isinstance(ts, int | float) and ts > 0 and (
            latest is None or ts > latest
        ):
            latest = float(ts)
        for key, child in node.items():
            if key != "timestamp" and isinstance(child, dict | list):
                stack.append(child)

    if latest is None:
        return None
    try:
        return datetime.fromtimestamp(latest, UTC)
    except (OverflowError, OSError, ValueError):
        return None


SENSORS: tuple[SpiderVenomSensorDescription, ...] = (
    SpiderVenomSensorDescription(
        key="model",
        translation_key="model",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=path_value_fn(("model",)),
    ),
    SpiderVenomSensorDescription(
        key="firmware_version",
        translation_key="firmware_version",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=path_value_fn(("vers",)),
    ),
    SpiderVenomSensorDescription(
        key="mac_address",
        translation_key="mac_address",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=path_value_fn(("mac",)),
    ),
    SpiderVenomSensorDescription(
        key="main_temp",
        translation_key="main_temp",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=_numeric_path_value_fn(("mainTemp",)),
    ),
    SpiderVenomSensorDescription(
        key="target_min",
        translation_key="target_min",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        value_fn=_numeric_path_value_fn(("heat", "t2", "min")),
    ),
    SpiderVenomSensorDescription(
        key="target_max",
        translation_key="target_max",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        value_fn=_numeric_path_value_fn(("heat", "t2", "max")),
    ),
    SpiderVenomSensorDescription(
        key="heat_intensity",
        translation_key="heat_intensity",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=_numeric_path_value_fn(("heat", "t2", "intensity")),
    ),
    SpiderVenomSensorDescription(
        key="heat_start_time",
        translation_key="heat_start_time",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=_timestamp(("heat", "t2", "startTime")),
    ),
    SpiderVenomSensorDescription(
        key="high_temp_limit",
        translation_key="high_temp_limit",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        value_fn=_numeric_path_value_fn(("heat", "t2", "high_limit")),
    ),
    SpiderVenomSensorDescription(
        key="low_temp_limit",
        translation_key="low_temp_limit",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        value_fn=_numeric_path_value_fn(("heat", "t2", "low_limit")),
    ),
    SpiderVenomSensorDescription(
        key="rssi",
        translation_key="rssi",
        native_unit_of_measurement=DBM,
        device_class=SensorDeviceClass.SIGNAL_STRENGTH,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_numeric_path_value_fn(("RSSI",)),
    ),
    SpiderVenomSensorDescription(
        key="ssid",
        translation_key="ssid",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=path_value_fn(("ssid",)),
    ),
    SpiderVenomSensorDescription(
        key="bssid",
        translation_key="bssid",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=path_value_fn(("bssid",)),
    ),
    SpiderVenomSensorDescription(
        key="update_flag",
        translation_key="update_flag",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=path_value_fn(("update",)),
    ),
    SpiderVenomSensorDescription(
        key="trigger_flag",
        translation_key="trigger_flag",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=path_value_fn(("trigger",)),
    ),
    SpiderVenomSensorDescription(
        key="errors",
        translation_key="errors",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_errors,
    ),
    SpiderVenomSensorDescription(
        key="shadow_last_updated",
        translation_key="shadow_last_updated",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        use_root=True,
        value_fn=_timestamp(("timestamp",)),
    ),
    SpiderVenomSensorDescription(
        key="last_reported",
        translation_key="last_reported",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        use_root=True,
        value_fn=_latest_reported_timestamp,
    ),
    SpiderVenomSensorDescription(
        key="probe_1_temp",
        translation_key="probe_1_temp",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=_numeric_path_value_fn(("probes", "p1", "temp")),
    ),
    SpiderVenomSensorDescription(
        key="probe_2_temp",
        translation_key="probe_2_temp",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=_numeric_path_value_fn(("probes", "p2", "temp")),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: SpiderVenomConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Spider Grills Venom sensors."""
    coordinator = entry.runtime_data
    async_add_entities(SpiderVenomSensor(coordinator, entry, description) for description in SENSORS)


class SpiderVenomSensor(SpiderVenomEntity, SensorEntity):
    """Spider Grills Venom sensor."""

    entity_description: SpiderVenomSensorDescription

    def __init__(
        self,
        coordinator: SpiderVenomDataUpdateCoordinator,
        entry: SpiderVenomConfigEntry,
        description: SpiderVenomSensorDescription,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, entry, description.key)
        self.entity_description = description

    @property
    def native_value(self) -> Any:
        """Return the sensor value."""
        source = (
            (self.coordinator.data or {})
            if self.entity_description.use_root
            else self.coordinator.reported
        )
        return self.entity_description.value_fn(source)
