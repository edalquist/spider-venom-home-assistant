"""Tests for Spider Grills Venom config and options flows."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.spider_venom.api import SpiderVenomApiError
from custom_components.spider_venom.config_flow import _device_label, _device_thing_name
from custom_components.spider_venom.const import (
    CONF_ENDPOINT,
    CONF_IDENTITY_POOL_ID,
    CONF_REGION,
    CONF_THING_NAME,
    DEFAULT_ENDPOINT,
    DEFAULT_IDENTITY_POOL_ID,
    DEFAULT_REGION,
    DOMAIN,
)
from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD, CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from .conftest import MOCK_ENTRY_DATA, MOCK_THING_NAME, SAMPLE_SHADOW


async def test_user_flow_creates_entry_without_storing_credentials(hass: HomeAssistant) -> None:
    """Verify account login -> device selection creates an entry without password or token."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "account"

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="ephemeral-token",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            return_value=[{"thingName": "venom-abc", "deviceName": "Patio Grill"}],
        ),
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_EMAIL: " user@example.com ",
                CONF_PASSWORD: "super-secret-password",
                CONF_SCAN_INTERVAL: 45,
            },
        )

    assert result2["type"] is FlowResultType.FORM
    assert result2["step_id"] == "select_device"

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderVenomClient.async_test_connection",
            return_value={"state": {"reported": {}}},
        ),
        patch(
            "custom_components.spider_venom.async_setup_entry",
            return_value=True,
        ),
    ):
        result3 = await hass.config_entries.flow.async_configure(
            result2["flow_id"],
            {CONF_THING_NAME: "venom-abc"},
        )

    assert result3["type"] is FlowResultType.CREATE_ENTRY
    assert result3["title"] == "Patio Grill (venom-abc)"
    assert result3["data"][CONF_THING_NAME] == "venom-abc"
    assert result3["data"][CONF_SCAN_INTERVAL] == 45
    assert CONF_ENDPOINT not in result3["data"]
    assert CONF_IDENTITY_POOL_ID not in result3["data"]
    assert CONF_REGION not in result3["data"]
    assert CONF_EMAIL not in result3["data"]
    assert CONF_PASSWORD not in result3["data"]


async def test_account_step_errors_and_suggested_values(hass: HomeAssistant) -> None:
    """Verify auth error, connection error, no devices, and suggested value preservation."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
        side_effect=SpiderVenomApiError("Spider Grills API returned HTTP 401", status=401),
    ):
        auth_err = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "wrong", CONF_SCAN_INTERVAL: 60},
        )
    assert auth_err["type"] is FlowResultType.FORM
    assert auth_err["errors"] == {"base": "invalid_auth"}

    assert auth_err["data_schema"] is not None
    schema_dict = auth_err["data_schema"].schema
    suggested = {
        key.schema: key.description.get("suggested_value")
        for key in schema_dict
        if hasattr(key, "description") and key.description
    }
    assert suggested.get(CONF_EMAIL) == "user@example.com"
    assert suggested.get(CONF_SCAN_INTERVAL) == 60
    assert CONF_PASSWORD not in suggested

    with patch(
        "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
        side_effect=SpiderVenomApiError("network unreachable"),
    ):
        err_result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )
    assert err_result["type"] is FlowResultType.FORM
    assert err_result["errors"] == {"base": "cannot_connect"}

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="tok",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            return_value=[{"name": "Missing Thing Name"}],
        ),
    ):
        no_dev_result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )
    assert no_dev_result["type"] is FlowResultType.FORM
    assert no_dev_result["errors"] == {"base": "no_devices"}


async def test_duplicate_device_aborts_before_test_connection(hass: HomeAssistant) -> None:
    """Duplicate thing_name aborts with already_configured without testing shadow."""
    existing = MockConfigEntry(
        domain=DOMAIN,
        unique_id="venom-dup",
        data={
            CONF_ENDPOINT: DEFAULT_ENDPOINT,
            CONF_IDENTITY_POOL_ID: DEFAULT_IDENTITY_POOL_ID,
            CONF_REGION: DEFAULT_REGION,
            CONF_THING_NAME: "venom-dup",
            CONF_SCAN_INTERVAL: 30,
        },
    )
    existing.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="tok",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            return_value=[{"thingName": "venom-dup", "deviceName": "Grill"}],
        ),
    ):
        step2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "u@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )

    with patch(
        "custom_components.spider_venom.config_flow.SpiderVenomClient.async_test_connection",
    ) as mock_test:
        aborted = await hass.config_entries.flow.async_configure(
            step2["flow_id"],
            {CONF_THING_NAME: "venom-dup"},
        )

    assert aborted["type"] is FlowResultType.ABORT
    assert aborted["reason"] == "already_configured"
    mock_test.assert_not_called()


async def test_select_device_shadow_validation_error(hass: HomeAssistant) -> None:
    """Verify shadow test failure on device selection shows cannot_connect."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="tok",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            return_value=[{"thingName": "venom-abc"}],
        ),
    ):
        step2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )

    with patch(
        "custom_components.spider_venom.config_flow.SpiderVenomClient.async_test_connection",
        side_effect=SpiderVenomApiError("IoT MQTT rejected"),
    ):
        err_step = await hass.config_entries.flow.async_configure(
            step2["flow_id"],
            {CONF_THING_NAME: "venom-abc"},
        )

    assert err_step["type"] is FlowResultType.FORM
    assert err_step["errors"] == {"base": "cannot_connect"}


async def test_options_flow_updates_scan_interval_and_reloads(hass: HomeAssistant) -> None:
    """Verify OptionsFlow updates scan_interval and reloads coordinator."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=SAMPLE_SHADOW,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert entry.runtime_data.update_interval == timedelta(seconds=30)

        opt_result = await hass.config_entries.options.async_init(entry.entry_id)
        assert opt_result["type"] is FlowResultType.FORM
        assert opt_result["step_id"] == "init"

        saved = await hass.config_entries.options.async_configure(
            opt_result["flow_id"],
            {CONF_SCAN_INTERVAL: 90},
        )
        assert saved["type"] is FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done()

        assert entry.options[CONF_SCAN_INTERVAL] == 90
        assert entry.runtime_data.update_interval == timedelta(seconds=90)


def test_device_helpers() -> None:
    """Verify fallback keys in _device_thing_name and _device_label."""
    assert _device_thing_name({"thingName": "t1"}) == "t1"
    assert _device_thing_name({"thing_name": "t2"}) == "t2"
    assert _device_thing_name({"externalId": "t3"}) == "t3"
    assert _device_thing_name({"deviceId": "t4"}) == "t4"
    assert _device_thing_name({}) is None

    assert _device_label({"deviceName": "Grill"}, "t1") == "Grill (t1)"
    assert _device_label({"name": "Kettle"}, "t1") == "Kettle (t1)"
    assert _device_label({"model": "Venom"}, "t1") == "Venom (t1)"
    assert _device_label({}, "t1") == "t1"


async def test_config_flow_unexpected_errors_and_get_devices_failure(hass: HomeAssistant) -> None:
    """Verify unexpected exceptions produce unknown error and device fetch failure produces cannot_connect."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
        side_effect=RuntimeError("boom"),
    ):
        unknown_account = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )
    assert unknown_account["type"] is FlowResultType.FORM
    assert unknown_account["errors"] == {"base": "unknown"}

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="tok",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            side_effect=SpiderVenomApiError("device list down"),
        ),
    ):
        dev_err = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )
    assert dev_err["type"] is FlowResultType.FORM
    assert dev_err["errors"] == {"base": "cannot_connect"}

    with (
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_login",
            return_value="tok",
        ),
        patch(
            "custom_components.spider_venom.config_flow.SpiderGrillsAccountClient.async_get_devices",
            return_value=[{"thingName": "venom-abc"}],
        ),
    ):
        step2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_EMAIL: "user@example.com", CONF_PASSWORD: "pw", CONF_SCAN_INTERVAL: 30},
        )

    with patch(
        "custom_components.spider_venom.config_flow.SpiderVenomClient.async_test_connection",
        side_effect=RuntimeError("unexpected validation error"),
    ):
        unknown_select = await hass.config_entries.flow.async_configure(
            step2["flow_id"],
            {CONF_THING_NAME: "venom-abc"},
        )
    assert unknown_select["type"] is FlowResultType.FORM
    assert unknown_select["errors"] == {"base": "unknown"}
