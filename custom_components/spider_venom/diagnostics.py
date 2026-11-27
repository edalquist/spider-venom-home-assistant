"""Diagnostics support for Spider Grills Venom."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .coordinator import SpiderVenomConfigEntry

TO_REDACT = {
    "thing_name",
    "mac",
    "ssid",
    "bssid",
    "identity_id",
    "email",
    "password",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: SpiderVenomConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a Spider Grills Venom config entry."""
    coordinator = entry.runtime_data
    return {
        "entry_data": async_redact_data(dict(entry.data), TO_REDACT),
        "entry_options": async_redact_data(dict(entry.options), TO_REDACT),
        "coordinator_data": async_redact_data(coordinator.data or {}, TO_REDACT),
        "last_update_success": coordinator.last_update_success,
        "is_device_online": coordinator.is_device_online,
        "last_reported_at": (
            coordinator.last_reported_at.isoformat() if coordinator.last_reported_at else None
        ),
    }
