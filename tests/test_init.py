"""Tests for Spider Grills Venom setup, unload, manifest, and service calls."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry
import voluptuous as vol
import yaml

from custom_components.spider_venom import async_setup
from custom_components.spider_venom.api import SpiderVenomApiError
from custom_components.spider_venom.const import (
    ATTR_TEMPERATURE,
    CONF_THING_NAME,
    DEFAULT_ENDPOINT,
    DEFAULT_IDENTITY_POOL_ID,
    DEFAULT_REGION,
    DOMAIN,
    SERVICE_SET_TARGET_TEMPERATURE,
)
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    service,
)

from .conftest import MOCK_ENTRY_DATA, MOCK_THING_NAME, SAMPLE_SHADOW, make_shadow


def test_manifest_metadata() -> None:
    """Verify manifest.json has integration_type=device, cloud_push, and paho-mqtt."""
    manifest = json.loads(
        Path("custom_components/spider_venom/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["integration_type"] == "device"
    assert manifest["iot_class"] == "cloud_push"
    assert "paho-mqtt>=2.1.0" in manifest["requirements"]


def test_services_yaml_schema() -> None:
    """Verify services.yaml conforms to Home Assistant schema without target.device."""
    raw = yaml.safe_load(
        Path("custom_components/spider_venom/services.yaml").read_text(encoding="utf-8")
    )
    validated = service._SERVICES_SCHEMA(raw)
    assert SERVICE_SET_TARGET_TEMPERATURE in validated
    for service_def in raw.values():
        target = service_def.get("target")
        if isinstance(target, dict):
            assert "device" not in target


async def test_setup_raises_config_entry_not_ready_when_push_connect_fails(hass: HomeAssistant) -> None:
    """Verify failed initial MQTT push connect puts entry into SETUP_RETRY and cleans up."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_start",
            side_effect=SpiderVenomApiError("MQTT refused"),
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_stop",
            new_callable=AsyncMock,
        ) as mock_stop,
    ):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state.name == "SETUP_RETRY"
    mock_stop.assert_called_once()


async def test_setup_falls_back_to_default_aws_settings(hass: HomeAssistant) -> None:
    """Verify setup succeeds using constant AWS defaults when omitted from entry.data."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_THING_NAME: MOCK_THING_NAME},
        unique_id=MOCK_THING_NAME,
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=SAMPLE_SHADOW,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    client = entry.runtime_data.client
    assert client._endpoint == DEFAULT_ENDPOINT
    assert client._identity_pool_id == DEFAULT_IDENTITY_POOL_ID
    assert client._region == DEFAULT_REGION


async def test_setup_unload_and_service_lifecycle(hass: HomeAssistant) -> None:
    """Verify service is registered in async_setup and persists across entry unload/reload."""
    assert await async_setup(hass, {})
    assert hass.services.has_service(DOMAIN, SERVICE_SET_TARGET_TEMPERATURE)

    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=SAMPLE_SHADOW,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert hasattr(entry, "runtime_data")
    assert hass.services.has_service(DOMAIN, SERVICE_SET_TARGET_TEMPERATURE)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert hass.services.has_service(DOMAIN, SERVICE_SET_TARGET_TEMPERATURE)

    entry2 = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id="venom-empty-check")
    entry2.add_to_hass(hass)
    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=SAMPLE_SHADOW,
    ):
        assert await hass.config_entries.async_setup(entry2.entry_id)
        await hass.async_block_till_done()

    with (
        patch.object(hass.config_entries, "async_entries", return_value=[]),
        pytest.raises(HomeAssistantError, match="No Spider Grills Venom devices"),
    ):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 250},
            blocking=True,
        )


async def test_set_target_temperature_service_single_and_multi(hass: HomeAssistant) -> None:
    """Verify set_target_temperature routing for single and multiple Venoms."""
    entry1 = MockConfigEntry(
        domain=DOMAIN,
        data=MOCK_ENTRY_DATA,
        unique_id=MOCK_THING_NAME,
    )
    entry1.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=SAMPLE_SHADOW,
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_set_target_temperature",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_set_temp,
    ):
        assert await hass.config_entries.async_setup(entry1.entry_id)
        await hass.async_block_till_done()

        # Single Venom works without thing_name
        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 275},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(275)

        # Unknown thing_name fails
        with pytest.raises(HomeAssistantError, match="No configured Venom thing named"):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_SET_TARGET_TEMPERATURE,
                {ATTR_TEMPERATURE: 275, CONF_THING_NAME: "does-not-exist"},
                blocking=True,
            )

        # Add a second Venom
        entry2_data = dict(MOCK_ENTRY_DATA, **{CONF_THING_NAME: "venom-test-002"})
        entry2 = MockConfigEntry(
            domain=DOMAIN,
            data=entry2_data,
            unique_id="venom-test-002",
        )
        entry2.add_to_hass(hass)
        assert await hass.config_entries.async_setup(entry2.entry_id)
        await hass.async_block_till_done()

        # Multiple Venoms require thing_name
        with pytest.raises(HomeAssistantError, match="multiple Venoms"):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_SET_TARGET_TEMPERATURE,
                {ATTR_TEMPERATURE: 300},
                blocking=True,
            )

        # Specifying thing_name succeeds
        mock_set_temp.reset_mock()
        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 300, CONF_THING_NAME: "venom-test-002"},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(300)

        # API error converts to HomeAssistantError
        mock_set_temp.side_effect = SpiderVenomApiError("boom")
        with pytest.raises(HomeAssistantError, match="Unable to set Venom target temperature"):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_SET_TARGET_TEMPERATURE,
                {ATTR_TEMPERATURE: 310, CONF_THING_NAME: "venom-test-002"},
                blocking=True,
            )


async def test_set_target_temperature_service_device_and_entity_targets(hass: HomeAssistant) -> None:
    """Verify set_target_temperature supports standard HA device_id and entity_id targets."""
    entry1 = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry2 = MockConfigEntry(
        domain=DOMAIN,
        data={**MOCK_ENTRY_DATA, CONF_THING_NAME: "venom-test-002"},
        unique_id="venom-test-002",
    )
    entry1.add_to_hass(hass)
    entry2.add_to_hass(hass)

    async def fake_get_shadow(client_self: object) -> dict[str, object]:
        if getattr(client_self, "_thing_name", "") == "venom-test-002":
            return make_shadow(mac="11:22:33:44:55:66")
        return make_shadow()

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            autospec=True,
            side_effect=fake_get_shadow,
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_set_target_temperature",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_set_temp,
    ):
        assert await hass.config_entries.async_setup(entry1.entry_id)
        await hass.async_block_till_done()
        assert entry2.state.name == "LOADED"

        dev_reg = dr.async_get(hass)
        device2 = dev_reg.async_get_device(identifiers={(DOMAIN, "venom-test-002")})
        assert device2 is not None

        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 315},
            target={"device_id": device2.id},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(315)
        assert entry2.runtime_data.get_optimistic_value(("heat", "t2", "trgt")) == (True, 315)

        mock_set_temp.reset_mock()
        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 320, "device_id": device2.id},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(320)
        assert entry2.runtime_data.get_optimistic_value(("heat", "t2", "trgt")) == (True, 320)

        mock_set_temp.reset_mock()
        ent_reg = er.async_get(hass)
        eid1 = ent_reg.async_get_entity_id("number", DOMAIN, f"{MOCK_THING_NAME}_target_temperature_control")
        assert eid1 is not None

        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 265},
            target={"entity_id": eid1},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(265)
        assert entry1.runtime_data.get_optimistic_value(("heat", "t2", "trgt")) == (True, 265)

        with pytest.raises(HomeAssistantError, match="No matching Spider Grills Venom"):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_SET_TARGET_TEMPERATURE,
                {ATTR_TEMPERATURE: 265},
                target={"device_id": "nonexistent-device-id"},
                blocking=True,
            )

        # Multi-device list targets update all specified devices
        device1 = dev_reg.async_get_device(identifiers={(DOMAIN, MOCK_THING_NAME)})
        assert device1 is not None
        mock_set_temp.reset_mock()
        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 290},
            target={"device_id": [device1.id, device2.id]},
            blocking=True,
        )
        assert mock_set_temp.call_count == 2

        # Out-of-range temperature fails schema validation
        with pytest.raises(vol.Invalid):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_SET_TARGET_TEMPERATURE,
                {ATTR_TEMPERATURE: 100},
                target={"device_id": device1.id},
                blocking=True,
            )


async def test_setup_stops_push_when_first_refresh_fails_and_on_ha_stop(hass: HomeAssistant) -> None:
    """Verify async_stop_push runs when first refresh fails and when EVENT_HOMEASSISTANT_STOP fires."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            side_effect=SpiderVenomApiError("initial poll timeout"),
        ),
        patch(
            "custom_components.spider_venom.coordinator.SpiderVenomDataUpdateCoordinator.async_stop_push",
            new_callable=AsyncMock,
        ) as mock_stop_push,
    ):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state.name == "SETUP_RETRY"
    mock_stop_push.assert_awaited_once()

    entry2 = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id="venom-ha-stop")
    entry2.add_to_hass(hass)
    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=SAMPLE_SHADOW,
        ),
        patch(
            "custom_components.spider_venom.coordinator.SpiderVenomDataUpdateCoordinator.async_stop_push",
            new_callable=AsyncMock,
        ) as mock_stop_push2,
    ):
        assert await hass.config_entries.async_setup(entry2.entry_id)
        await hass.async_block_till_done()
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        await hass.async_block_till_done()

    mock_stop_push2.assert_awaited_once()


async def test_setup_stops_push_when_platform_forward_fails(hass: HomeAssistant) -> None:
    """Verify async_stop_push runs when async_forward_entry_setups raises."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id="venom-forward-fail")
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=SAMPLE_SHADOW,
        ),
        patch.object(
            hass.config_entries,
            "async_forward_entry_setups",
            side_effect=RuntimeError("platform load failed"),
        ),
        patch(
            "custom_components.spider_venom.coordinator.SpiderVenomDataUpdateCoordinator.async_stop_push",
            new_callable=AsyncMock,
        ) as mock_stop_push,
    ):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state.name == "SETUP_ERROR"
    mock_stop_push.assert_awaited_once()


async def test_service_rounds_float_supports_area_target_and_has_translations(
    hass: HomeAssistant,
) -> None:
    """Verify float rounding, area_id targeting, and service translations."""
    strings = json.loads(Path("custom_components/spider_venom/strings.json").read_text(encoding="utf-8"))
    en = json.loads(Path("custom_components/spider_venom/translations/en.json").read_text(encoding="utf-8"))
    assert strings["services"] == en["services"]
    assert SERVICE_SET_TARGET_TEMPERATURE in en["services"]

    entry1 = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry2 = MockConfigEntry(
        domain=DOMAIN,
        data={**MOCK_ENTRY_DATA, CONF_THING_NAME: "venom-test-002"},
        unique_id="venom-test-002",
    )
    entry1.add_to_hass(hass)
    entry2.add_to_hass(hass)

    async def fake_get_shadow(client_self: object) -> dict[str, object]:
        if getattr(client_self, "_thing_name", "") == "venom-test-002":
            return make_shadow(mac="11:22:33:44:55:66")
        return make_shadow()

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            autospec=True,
            side_effect=fake_get_shadow,
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_set_target_temperature",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_set_temp,
    ):
        assert await hass.config_entries.async_setup(entry1.entry_id)
        await hass.async_block_till_done()

        area_reg = ar.async_get(hass)
        patio = area_reg.async_get_or_create("Patio")
        dev_reg = dr.async_get(hass)
        device2 = dev_reg.async_get_device(identifiers={(DOMAIN, "venom-test-002")})
        assert device2 is not None
        dev_reg.async_update_device(device2.id, area_id=patio.id)

        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 300.6},
            target={"area_id": patio.id},
            blocking=True,
        )
        mock_set_temp.assert_called_once_with(301)
        assert entry2.runtime_data.get_optimistic_value(("heat", "t2", "trgt")) == (True, 301)
