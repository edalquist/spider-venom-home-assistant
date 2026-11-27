"""Config flow for Spider Grills Venom."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD, CONF_SCAN_INTERVAL
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
from homeassistant.util.ssl import get_default_context

from .api import (
    SpiderGrillsAccountClient,
    SpiderVenomApiError,
    SpiderVenomAuthError,
    SpiderVenomClient,
)
from .const import (
    CONF_OFFLINE_TIMEOUT,
    CONF_THING_NAME,
    DEFAULT_ENDPOINT,
    DEFAULT_IDENTITY_POOL_ID,
    DEFAULT_OFFLINE_TIMEOUT,
    DEFAULT_REGION,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=10,
        max=3600,
        step=1,
        unit_of_measurement="seconds",
        mode=NumberSelectorMode.BOX,
    )
)

OFFLINE_TIMEOUT_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=0,
        max=3600,
        step=1,
        unit_of_measurement="seconds",
        mode=NumberSelectorMode.BOX,
    )
)

ACCOUNT_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.EMAIL)
        ),
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
        vol.Optional(
            CONF_SCAN_INTERVAL,
            default=int(DEFAULT_SCAN_INTERVAL.total_seconds()),
        ): SCAN_INTERVAL_SELECTOR,
    }
)


class SpiderVenomConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a Spider Grills Venom config flow."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._discovered_devices: dict[str, dict[str, Any]] = {}
        self._scan_interval: int = int(DEFAULT_SCAN_INTERVAL.total_seconds())

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> SpiderVenomOptionsFlow:
        """Return the options flow."""
        return SpiderVenomOptionsFlow()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial step."""
        return await self.async_step_account(user_input)

    async def async_step_account(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Discover devices with a Spider Grills account."""
        errors: dict[str, str] = {}

        if user_input is not None:
            account_client = SpiderGrillsAccountClient(session=async_get_clientsession(self.hass))
            try:
                access_token = await account_client.async_login(
                    email=user_input[CONF_EMAIL].strip(),
                    password=user_input[CONF_PASSWORD],
                )
                devices = await account_client.async_get_devices(access_token)
            except SpiderVenomAuthError:
                errors["base"] = "invalid_auth"
            except SpiderVenomApiError as err:
                if _is_auth_error(err):
                    errors["base"] = "invalid_auth"
                else:
                    _LOGGER.warning("Unable to discover Spider Venom devices: %s", err)
                    errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error discovering Spider Venom devices")
                errors["base"] = "unknown"
            else:
                self._scan_interval = int(user_input[CONF_SCAN_INTERVAL])
                discovered = {
                    thing_name: device
                    for device in devices
                    if (thing_name := _device_thing_name(device)) is not None
                }
                if not discovered:
                    errors["base"] = "no_devices"
                else:
                    configured_ids = self._async_current_ids()
                    self._discovered_devices = {
                        thing_name: device
                        for thing_name, device in discovered.items()
                        if thing_name not in configured_ids
                    }
                    if not self._discovered_devices:
                        return self.async_abort(reason="already_configured")
                    return await self.async_step_select_device()

        schema = ACCOUNT_SCHEMA
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(
                ACCOUNT_SCHEMA,
                {
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_SCAN_INTERVAL: user_input[CONF_SCAN_INTERVAL],
                },
            )

        return self.async_show_form(
            step_id="account",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_select_device(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Select a discovered Spider Grills device."""
        errors: dict[str, str] = {}
        device_options = {
            thing_name: _device_label(device, thing_name)
            for thing_name, device in self._discovered_devices.items()
        }

        if user_input is not None:
            thing_name = user_input[CONF_THING_NAME]
            device = self._discovered_devices[thing_name]
            await self.async_set_unique_id(thing_name)
            self._abort_if_unique_id_configured()
            try:
                return await self._async_create_validated_entry(
                    thing_name=thing_name,
                    endpoint=DEFAULT_ENDPOINT,
                    identity_pool_id=DEFAULT_IDENTITY_POOL_ID,
                    region=DEFAULT_REGION,
                    scan_interval=self._scan_interval,
                    title=_device_label(device, thing_name),
                )
            except SpiderVenomApiError as err:
                _LOGGER.warning("Unable to validate Spider Venom config: %s", err)
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error validating Spider Venom config")
                errors["base"] = "unknown"

        return self.async_show_form(
            step_id="select_device",
            data_schema=vol.Schema({vol.Required(CONF_THING_NAME): vol.In(device_options)}),
            errors=errors,
        )

    async def _async_create_validated_entry(
        self,
        *,
        thing_name: str,
        endpoint: str,
        identity_pool_id: str,
        region: str,
        scan_interval: int,
        title: str,
    ) -> config_entries.ConfigFlowResult:
        """Validate shadow access and create a config entry."""
        client = SpiderVenomClient(
            session=async_get_clientsession(self.hass),
            endpoint=endpoint,
            identity_pool_id=identity_pool_id,
            region=region,
            thing_name=thing_name,
            ssl_context=get_default_context(),
        )
        await client.async_test_connection()
        return self.async_create_entry(
            title=title,
            data={
                CONF_THING_NAME: thing_name,
                CONF_SCAN_INTERVAL: scan_interval,
            },
        )


def _is_auth_error(err: SpiderVenomApiError) -> bool:
    """Return true if the API error represents an authentication failure."""
    return getattr(err, "status", None) in (401, 403) or "HTTP 401" in str(err) or "HTTP 403" in str(err)


def _device_thing_name(device: dict[str, Any]) -> str | None:
    """Extract an AWS IoT thing name from a Spider Grills device object."""
    for key in ("thingName", "thing_name", "externalId", "deviceId"):
        value = device.get(key)
        if isinstance(value, str) and (stripped := value.strip()):
            return stripped
    return None


def _device_label(device: dict[str, Any], thing_name: str) -> str:
    """Return a friendly device label."""
    for key in ("deviceName", "name", "model"):
        value = device.get(key)
        if isinstance(value, str) and (stripped := value.strip()):
            return f"{stripped} ({thing_name})"
    return thing_name


class SpiderVenomOptionsFlow(config_entries.OptionsFlow):
    """Handle Spider Grills Venom options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage integration options."""
        if user_input is not None:
            return self.async_create_entry(
                data={
                    CONF_SCAN_INTERVAL: int(user_input[CONF_SCAN_INTERVAL]),
                    CONF_OFFLINE_TIMEOUT: int(user_input[CONF_OFFLINE_TIMEOUT]),
                }
            )

        current_interval = self.config_entry.options.get(
            CONF_SCAN_INTERVAL,
            self.config_entry.data.get(
                CONF_SCAN_INTERVAL,
                int(DEFAULT_SCAN_INTERVAL.total_seconds()),
            ),
        )
        current_offline_timeout = self.config_entry.options.get(
            CONF_OFFLINE_TIMEOUT,
            self.config_entry.data.get(
                CONF_OFFLINE_TIMEOUT,
                DEFAULT_OFFLINE_TIMEOUT,
            ),
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_SCAN_INTERVAL,
                        default=current_interval,
                    ): SCAN_INTERVAL_SELECTOR,
                    vol.Optional(
                        CONF_OFFLINE_TIMEOUT,
                        default=current_offline_timeout,
                    ): OFFLINE_TIMEOUT_SELECTOR,
                }
            ),
        )
