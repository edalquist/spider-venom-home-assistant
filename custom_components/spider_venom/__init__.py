"""Spider Grills Venom integration."""

from __future__ import annotations

import voluptuous as vol

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import (
    ATTR_AREA_ID,
    ATTR_DEVICE_ID,
    ATTR_ENTITY_ID,
    ATTR_FLOOR_ID,
    ATTR_LABEL_ID,
    EVENT_HOMEASSISTANT_STOP,
)
from homeassistant.core import Event, HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.service import async_extract_config_entry_ids
from homeassistant.helpers.typing import ConfigType

from .api import SpiderVenomApiError, SpiderVenomClient
from .const import (
    ATTR_TEMPERATURE,
    CONF_ENDPOINT,
    CONF_IDENTITY_POOL_ID,
    CONF_REGION,
    CONF_THING_NAME,
    DEFAULT_ENDPOINT,
    DEFAULT_IDENTITY_POOL_ID,
    DEFAULT_REGION,
    DOMAIN,
    MAX_TARGET_TEMPERATURE,
    MIN_TARGET_TEMPERATURE,
    PLATFORMS,
    SERVICE_SET_TARGET_TEMPERATURE,
)
from .coordinator import SpiderVenomConfigEntry, SpiderVenomDataUpdateCoordinator

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

TARGET_ATTRS = (
    ATTR_DEVICE_ID,
    ATTR_ENTITY_ID,
    ATTR_AREA_ID,
    ATTR_FLOOR_ID,
    ATTR_LABEL_ID,
)

SET_TARGET_TEMPERATURE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_TEMPERATURE): vol.All(
            vol.Coerce(float),
            round,
            int,
            vol.Range(min=MIN_TARGET_TEMPERATURE, max=MAX_TARGET_TEMPERATURE),
        ),
        vol.Optional(CONF_THING_NAME): str,
        vol.Optional(ATTR_DEVICE_ID): vol.All(cv.ensure_list, [str]),
        vol.Optional(ATTR_ENTITY_ID): vol.All(cv.ensure_list, [str]),
        vol.Optional(ATTR_AREA_ID): vol.All(cv.ensure_list, [str]),
        vol.Optional(ATTR_FLOOR_ID): vol.All(cv.ensure_list, [str]),
        vol.Optional(ATTR_LABEL_ID): vol.All(cv.ensure_list, [str]),
    }
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Spider Grills Venom domain services."""
    _async_register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: SpiderVenomConfigEntry) -> bool:
    """Set up Spider Grills Venom from a config entry."""
    client = SpiderVenomClient(
        session=async_get_clientsession(hass),
        endpoint=entry.data.get(CONF_ENDPOINT, DEFAULT_ENDPOINT),
        identity_pool_id=entry.data.get(CONF_IDENTITY_POOL_ID, DEFAULT_IDENTITY_POOL_ID),
        region=entry.data.get(CONF_REGION, DEFAULT_REGION),
        thing_name=entry.data[CONF_THING_NAME],
    )
    coordinator = SpiderVenomDataUpdateCoordinator(hass, client, entry)
    try:
        await coordinator.async_start_push()
    except SpiderVenomApiError as err:
        await coordinator.async_stop_push()
        raise ConfigEntryNotReady(f"Unable to connect to Spider Venom MQTT: {err}") from err

    try:
        await coordinator.async_config_entry_first_refresh()
    except Exception:
        await coordinator.async_stop_push()
        raise

    async def _async_stop_on_shutdown(_event: Event) -> None:
        await coordinator.async_stop_push()

    entry.runtime_data = coordinator
    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_stop_on_shutdown))
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))
    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except Exception:
        await coordinator.async_stop_push()
        raise
    _async_register_services(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: SpiderVenomConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.async_stop_push()
        entry.runtime_data.async_clear_optimistic_timers()
    return unloaded


async def _async_reload_entry(hass: HomeAssistant, entry: SpiderVenomConfigEntry) -> None:
    """Reload config entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


def _async_register_services(hass: HomeAssistant) -> None:
    """Register Spider Grills Venom services."""
    if hass.services.has_service(DOMAIN, SERVICE_SET_TARGET_TEMPERATURE):
        return

    async def async_set_target_temperature(call: ServiceCall) -> None:
        entries = [
            entry
            for entry in hass.config_entries.async_entries(DOMAIN)
            if entry.state is ConfigEntryState.LOADED
        ]
        thing_name = call.data.get(CONF_THING_NAME)
        has_target = any(key in call.data for key in TARGET_ATTRS)
        temperature = call.data[ATTR_TEMPERATURE]

        if not entries:
            raise HomeAssistantError("No Spider Grills Venom devices are configured")

        if has_target:
            matched_entry_ids = await async_extract_config_entry_ids(call)
            target_entries = [entry for entry in entries if entry.entry_id in matched_entry_ids]
            if not target_entries:
                raise HomeAssistantError("No matching Spider Grills Venom devices found for target")
        elif thing_name is not None:
            target_entry = next(
                (
                    candidate
                    for candidate in entries
                    if candidate.data[CONF_THING_NAME] == thing_name.strip()
                ),
                None,
            )
            if target_entry is None:
                raise HomeAssistantError(f"No configured Venom thing named {thing_name}")
            target_entries = [target_entry]
        elif len(entries) == 1:
            target_entries = [entries[0]]
        else:
            raise HomeAssistantError(
                "Specify device_id, entity_id, or thing_name when multiple Venoms are configured"
            )

        for target_entry in target_entries:
            coordinator: SpiderVenomDataUpdateCoordinator = target_entry.runtime_data
            try:
                await coordinator.client.async_set_target_temperature(temperature)
            except SpiderVenomApiError as err:
                raise HomeAssistantError(f"Unable to set Venom target temperature: {err}") from err

            coordinator.set_optimistic_value(("heat", "t2", "trgt"), temperature)
            await coordinator.async_request_refresh()

    hass.services.async_register(
        DOMAIN,
        SERVICE_SET_TARGET_TEMPERATURE,
        async_set_target_temperature,
        schema=SET_TARGET_TEMPERATURE_SCHEMA,
    )
