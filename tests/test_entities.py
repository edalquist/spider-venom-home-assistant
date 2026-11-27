"""Tests for Spider Grills Venom entities."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.spider_venom.api import SpiderVenomApiError
from custom_components.spider_venom.binary_sensor import BINARY_SENSORS
from custom_components.spider_venom.const import (
    ATTR_TEMPERATURE,
    CONF_OFFLINE_TIMEOUT,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    SERVICE_SET_TARGET_TEMPERATURE,
)
from custom_components.spider_venom.coordinator import SpiderVenomDataUpdateCoordinator
from custom_components.spider_venom.number import NUMBERS
from custom_components.spider_venom.sensor import SENSORS, _errors, _latest_reported_timestamp, _timestamp
from custom_components.spider_venom.switch import SWITCHES
from homeassistant.components.number import NumberDeviceClass
from homeassistant.const import CONF_SCAN_INTERVAL, EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_system import METRIC_SYSTEM, US_CUSTOMARY_SYSTEM

from .conftest import MOCK_ENTRY_DATA, MOCK_THING_NAME, SAMPLE_SHADOW


def _entity_id_by_key(hass: HomeAssistant, platform: str, key: str) -> str:
    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id(platform, DOMAIN, f"{MOCK_THING_NAME}_{key}")
    assert entity_id is not None, f"Missing entity for {platform} {key}"
    return entity_id


def _state(hass: HomeAssistant, entity_id: str) -> str:
    state = hass.states.get(entity_id)
    assert state is not None, f"Missing state for {entity_id}"
    return state.state


def test_number_descriptions_have_temperature_device_class() -> None:
    """All temperature number entities declare NumberDeviceClass.TEMPERATURE."""
    assert all(desc.device_class is NumberDeviceClass.TEMPERATURE for desc in NUMBERS)


async def test_entities_initial_states_diagnostics_and_device_registry(hass: HomeAssistant) -> None:
    """Verify sensors, binary sensors, numbers, switches, diagnostics, and device info."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME, title="Patio Venom")
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=deepcopy(SAMPLE_SHADOW),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    # Sensors
    assert _state(hass, _entity_id_by_key(hass, "sensor", "model")) == "Venom"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "firmware_version")) == "1.2.3"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "main_temp")) == "225"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "heat_intensity")) == "65"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "rssi")) == "-58"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "errors")) == "E01,E02"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "probe_1_temp")) == "145"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "probe_2_temp")) == "180"
    assert (
        _state(hass, _entity_id_by_key(hass, "sensor", "shadow_last_updated"))
        == "2023-11-14T22:15:23+00:00"
    )
    assert (
        _state(hass, _entity_id_by_key(hass, "sensor", "last_reported"))
        == "2023-11-14T22:15:10+00:00"
    )

    # Diagnostic entity categories
    registry = er.async_get(hass)
    for diag_key in (
        "model",
        "firmware_version",
        "mac_address",
        "rssi",
        "ssid",
        "bssid",
        "update_flag",
        "trigger_flag",
        "errors",
        "shadow_last_updated",
        "last_reported",
    ):
        eid = _entity_id_by_key(hass, "sensor", diag_key)
        reg_entry = registry.async_get(eid)
        assert reg_entry is not None
        assert reg_entry.entity_category is EntityCategory.DIAGNOSTIC

    for disabled_key in ("mac_address", "bssid", "update_flag", "trigger_flag"):
        eid = _entity_id_by_key(hass, "sensor", disabled_key)
        reg_entry = registry.async_get(eid)
        assert reg_entry is not None
        assert reg_entry.disabled_by is er.RegistryEntryDisabler.INTEGRATION
        assert hass.states.get(eid) is None

    # Static limits should not have state_class
    for limit_key in ("target_min", "target_max", "high_temp_limit", "low_temp_limit"):
        state = hass.states.get(_entity_id_by_key(hass, "sensor", limit_key))
        assert state is not None
        assert "state_class" not in state.attributes

    # Binary sensors
    assert _state(hass, _entity_id_by_key(hass, "binary_sensor", "connected")) == "on"
    assert _state(hass, _entity_id_by_key(hass, "binary_sensor", "door")) == "off"
    assert _state(hass, _entity_id_by_key(hass, "binary_sensor", "heating")) == "on"
    assert _state(hass, _entity_id_by_key(hass, "binary_sensor", "low_temp_notification")) == "on"

    # Diagnostic binary sensors
    for diag_bin_key in ("connected", "fahrenheit_mode"):
        bin_reg = registry.async_get(_entity_id_by_key(hass, "binary_sensor", diag_bin_key))
        assert bin_reg is not None
        assert bin_reg.entity_category is EntityCategory.DIAGNOSTIC

    # Duplicate read-only entities removed
    for platform, dup_key in (
        ("sensor", "target_temp"),
        ("sensor", "probe_1_target"),
        ("sensor", "probe_2_target"),
        ("binary_sensor", "power"),
        ("binary_sensor", "engaged"),
        ("binary_sensor", "paused"),
        ("binary_sensor", "high_temp_enabled"),
    ):
        assert registry.async_get_entity_id(platform, DOMAIN, f"{MOCK_THING_NAME}_{dup_key}") is None

    # Numbers & switches
    assert _state(hass, _entity_id_by_key(hass, "number", "target_temperature_control")) == "250"
    assert _state(hass, _entity_id_by_key(hass, "number", "probe_1_target_temperature_control")) == "165"
    assert _state(hass, _entity_id_by_key(hass, "number", "probe_2_target_temperature_control")) == "203"
    assert _state(hass, _entity_id_by_key(hass, "switch", "power_control")) == "on"
    assert _state(hass, _entity_id_by_key(hass, "switch", "paused_control")) == "off"

    # Device registry + MAC connection
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get_device(identifiers={(DOMAIN, MOCK_THING_NAME)})
    assert device is not None
    assert device.name == "Patio Venom"
    assert device.manufacturer == "Spider Grills"
    assert device.model == "Venom"
    assert device.sw_version == "1.2.3"
    assert (dr.CONNECTION_NETWORK_MAC, "aa:bb:cc:dd:ee:ff") in device.connections


async def test_metric_unit_conversion_for_number_entities(hass: HomeAssistant) -> None:
    """Metric users get temperature conversion on number controls."""
    hass.config.units = METRIC_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=deepcopy(SAMPLE_SHADOW),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    state = hass.states.get(_entity_id_by_key(hass, "number", "target_temperature_control"))
    assert state is not None
    assert state.attributes["unit_of_measurement"] == "°C"
    assert float(state.state) == pytest.approx(121.1, abs=0.5)


async def test_number_and_switch_controls(hass: HomeAssistant) -> None:
    """Verify number and switch service calls update desired shadow and optimistic state."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=deepcopy(SAMPLE_SHADOW),
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_update_desired",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_update,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
        await hass.services.async_call(
            "number",
            "set_value",
            {"entity_id": target_entity, "value": 325},
            blocking=True,
        )
        mock_update.assert_any_call({"heat": {"t2": {"trgt": 325}}})
        assert _state(hass, target_entity) == "325"

        probe1_entity = _entity_id_by_key(hass, "number", "probe_1_target_temperature_control")
        await hass.services.async_call(
            "number",
            "set_value",
            {"entity_id": probe1_entity, "value": 195},
            blocking=True,
        )
        mock_update.assert_any_call({"probes": {"p1": {"trgt": 195}}})
        assert _state(hass, probe1_entity) == "195"

        power_entity = _entity_id_by_key(hass, "switch", "power_control")
        await hass.services.async_call("switch", "turn_off", {"entity_id": power_entity}, blocking=True)
        mock_update.assert_any_call({"pwrOn": False})
        assert _state(hass, power_entity) == "off"

        paused_entity = _entity_id_by_key(hass, "switch", "paused_control")
        await hass.services.async_call("switch", "turn_on", {"entity_id": paused_entity}, blocking=True)
        mock_update.assert_any_call({"paused": True})
        assert _state(hass, paused_entity) == "on"


async def test_optimistic_reconciliation_service_external_and_expiry(hass: HomeAssistant) -> None:
    """Verify optimistic state syncs with service, clears on external change, and expires via timer."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    shadow = deepcopy(SAMPLE_SHADOW)
    shadow["state"]["reported"]["pwrOn"] = 1
    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            side_effect=lambda: deepcopy(shadow),
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_set_target_temperature",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_update_desired",
            new_callable=AsyncMock,
            return_value={},
        ),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
        power_entity = _entity_id_by_key(hass, "switch", "power_control")

        await hass.services.async_call(
            DOMAIN,
            SERVICE_SET_TARGET_TEMPERATURE,
            {ATTR_TEMPERATURE: 300},
            blocking=True,
        )
        assert _state(hass, target_entity) == "300"

        shadow["state"]["reported"]["heat"]["t2"]["trgt"] = 275
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert _state(hass, target_entity) == "275"

        await hass.services.async_call("switch", "turn_off", {"entity_id": power_entity}, blocking=True)
        assert _state(hass, power_entity) == "off"

        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=61))
        await hass.async_block_till_done()
        assert _state(hass, power_entity) == "on"

        coordinator = entry.runtime_data
        await hass.services.async_call("switch", "turn_off", {"entity_id": power_entity}, blocking=True)
        assert coordinator._optimistic
        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert not coordinator._optimistic


async def test_push_update_updates_entities_and_reconciles_optimistic(hass: HomeAssistant) -> None:
    """Verify incoming MQTT push updates entities immediately and reconciles optimistic state."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=deepcopy(SAMPLE_SHADOW),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    main_temp_eid = _entity_id_by_key(hass, "sensor", "main_temp")
    target_eid = _entity_id_by_key(hass, "number", "target_temperature_control")
    assert _state(hass, main_temp_eid) == "225"

    entry.runtime_data.set_optimistic_value(("heat", "t2", "trgt"), 310)
    assert _state(hass, target_eid) == "310"

    pushed = deepcopy(SAMPLE_SHADOW)
    pushed["state"]["reported"]["mainTemp"] = 242
    pushed["state"]["reported"]["heat"]["t2"]["trgt"] = 310
    entry.runtime_data._handle_push_update(pushed)
    await hass.async_block_till_done()

    assert _state(hass, main_temp_eid) == "242"
    assert _state(hass, target_eid) == "310"
    assert not entry.runtime_data._optimistic


async def test_number_entity_rejects_out_of_range_directly(hass: HomeAssistant) -> None:
    """Verify SpiderVenomTargetTemperatureNumber validates min/max bounds."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=deepcopy(SAMPLE_SHADOW),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
    entity = hass.data["entity_components"]["number"].get_entity(target_entity)
    assert entity is not None

    with pytest.raises(ServiceValidationError):
        await entity.async_set_native_value(100)


async def test_number_entity_uses_dynamic_pit_bounds_from_shadow(hass: HomeAssistant) -> None:
    """Verify pit target temperature honors dynamic heat.t2.min/max from reported state."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    high_temp_shadow = deepcopy(SAMPLE_SHADOW)
    high_temp_shadow["state"]["reported"]["heat"]["t2"]["max"] = 650

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=high_temp_shadow,
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_update_desired",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_update,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
        await hass.services.async_call(
            "number",
            "set_value",
            {"entity_id": target_entity, "value": 600},
            blocking=True,
        )
        mock_update.assert_called_once_with({"heat": {"t2": {"trgt": 600}}})

        entity = hass.data["entity_components"]["number"].get_entity(target_entity)
        assert entity is not None
        with pytest.raises(ServiceValidationError):
            await entity.async_set_native_value(700)


async def test_coordinator_null_state_and_interval_guard(hass: HomeAssistant) -> None:
    """Coordinator handles null state and invalid/bool scan_interval safely."""
    for bad_interval in (True, False, 0, -10, 5):
        entry = MockConfigEntry(
            domain=DOMAIN,
            data={**MOCK_ENTRY_DATA, CONF_SCAN_INTERVAL: bad_interval},
            unique_id=f"{MOCK_THING_NAME}-{bad_interval}",
        )
        coord = SpiderVenomDataUpdateCoordinator(hass, MagicMock(), entry)
        assert coord.update_interval == DEFAULT_SCAN_INTERVAL
        coord.data = {"state": None}
        assert coord.reported == {}
        coord.data = {"state": {"reported": None}}
        assert coord.reported == {}


def test_sensor_helper_functions() -> None:
    """Verify _timestamp and _errors helpers including edge cases."""
    ts_fn = _timestamp(("heat", "t2", "startTime"))
    assert ts_fn({"heat": {"t2": {"startTime": 1700000000}}}) == datetime.fromtimestamp(1700000000, UTC)
    assert ts_fn({"heat": {"t2": {"startTime": 0}}}) is None
    assert ts_fn({"heat": {"t2": {"startTime": -5}}}) is None
    assert ts_fn({"heat": {"t2": {"startTime": True}}}) is None
    assert ts_fn({"heat": {"t2": {"startTime": 1e20}}}) is None
    assert ts_fn({}) is None

    assert _errors({"errors": ["E1", "E2"]}) == "E1,E2"
    assert _errors({"errors": []}) is None
    assert _errors({"errors": "not-a-list"}) is None
    assert _errors({}) is None
    long_errors = _errors({"errors": [f"ERR{i:03d}" for i in range(100)]})
    assert long_errors is not None and len(long_errors) == 255

    assert _latest_reported_timestamp({}) is None
    assert _latest_reported_timestamp({"metadata": None}) is None
    assert _latest_reported_timestamp({"metadata": {"reported": {}}}) is None
    assert _latest_reported_timestamp({"metadata": {"reported": {"a": {"timestamp": True}}}}) is None
    assert _latest_reported_timestamp({"metadata": {"reported": {"a": {"timestamp": 1e20}}}}) is None
    assert _latest_reported_timestamp(
        {"metadata": {"reported": {"a": {"timestamp": 1700000100}, "b": {"c": {"timestamp": 1700000200}}}}}
    ) == datetime.fromtimestamp(1700000200, UTC)
    assert _latest_reported_timestamp(
        {
            "metadata": {
                "reported": {
                    "a": {"timestamp": 1700000100},
                    "errors": [{"timestamp": 1700000300}],
                }
            }
        }
    ) == datetime.fromtimestamp(1700000300, UTC)


def test_entity_translation_keys_present_in_translations() -> None:
    """Verify every entity description defines translation_key present in strings and en.json."""
    strings = json.loads(Path("custom_components/spider_venom/strings.json").read_text(encoding="utf-8"))
    en = json.loads(Path("custom_components/spider_venom/translations/en.json").read_text(encoding="utf-8"))

    for platform, descriptions in (
        ("sensor", SENSORS),
        ("binary_sensor", BINARY_SENSORS),
        ("number", NUMBERS),
        ("switch", SWITCHES),
    ):
        for desc in descriptions:
            assert desc.translation_key == desc.key
            assert desc.key in strings["entity"][platform]
            assert desc.key in en["entity"][platform]
            assert strings["entity"][platform][desc.key]["name"] == en["entity"][platform][desc.key]["name"]

    assert en["entity"]["sensor"]["main_temp"]["name"] == "Pit temperature"
    assert en["entity"]["sensor"]["heat_intensity"]["name"] == "Fan speed"
    assert en["entity"]["binary_sensor"]["door"]["name"] == "Lid"
    assert en["entity"]["number"]["target_temperature_control"]["name"] == "Pit target temperature"
    assert en["entity"]["switch"]["power_control"]["name"] == "Power"
    assert en["entity"]["switch"]["paused_control"]["name"] == "Pause"
    assert en["entity"]["switch"]["engaged_control"]["name"] == "Temperature control"


async def test_switch_and_number_api_error_skips_optimistic(hass: HomeAssistant) -> None:
    """Verify switch and number API errors raise HomeAssistantError without setting optimistic state."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=deepcopy(SAMPLE_SHADOW),
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_update_desired",
            new_callable=AsyncMock,
            side_effect=SpiderVenomApiError("write failed"),
        ),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        power_entity = _entity_id_by_key(hass, "switch", "power_control")
        with pytest.raises(HomeAssistantError, match="Unable to update Venom control"):
            await hass.services.async_call("switch", "turn_off", {"entity_id": power_entity}, blocking=True)

        target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
        with pytest.raises(HomeAssistantError, match="Unable to set"):
            await hass.services.async_call(
                "number",
                "set_value",
                {"entity_id": target_entity, "value": 300},
                blocking=True,
            )

    assert not entry.runtime_data._optimistic


async def test_metric_number_set_value_converts_celsius_to_fahrenheit(hass: HomeAssistant) -> None:
    """Verify metric user setting number in °C writes rounded °F to desired shadow."""
    hass.config.units = METRIC_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
            return_value=deepcopy(SAMPLE_SHADOW),
        ),
        patch(
            "custom_components.spider_venom.api.SpiderVenomClient.async_update_desired",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_update,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        target_entity = _entity_id_by_key(hass, "number", "target_temperature_control")
        await hass.services.async_call(
            "number",
            "set_value",
            {"entity_id": target_entity, "value": 121.1},
            blocking=True,
        )

    mock_update.assert_called_once_with({"heat": {"t2": {"trgt": 250}}})


async def test_entities_handle_empty_shadow_safely(hass: HomeAssistant) -> None:
    """Verify entities set up cleanly with unknown states when shadow is empty."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value={},
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert _state(hass, _entity_id_by_key(hass, "sensor", "main_temp")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "binary_sensor", "door")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "number", "target_temperature_control")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "switch", "power_control")) == "unknown"


async def test_coordinator_reconnect_callback_requests_refresh(hass: HomeAssistant) -> None:
    """Verify coordinator's on_reconnect callback requests a shadow refresh."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=deepcopy(SAMPLE_SHADOW),
    ) as mock_get:
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        initial_calls = mock_get.call_count

        entry.runtime_data._handle_reconnect()
        await hass.async_block_till_done()
        assert mock_get.call_count == initial_calls + 1


async def test_numeric_entities_ignore_booleans_and_non_numeric_values(hass: HomeAssistant) -> None:
    """Verify numeric sensors and numbers return unknown on booleans or strings instead of raising."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    bad_shadow = deepcopy(SAMPLE_SHADOW)
    bad_shadow["state"]["reported"]["mainTemp"] = "err"
    bad_shadow["state"]["reported"]["RSSI"] = True
    bad_shadow["state"]["reported"]["heat"]["t2"]["trgt"] = False
    bad_shadow["state"]["reported"]["probes"]["p1"]["temp"] = None

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=bad_shadow,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert _state(hass, _entity_id_by_key(hass, "sensor", "main_temp")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "rssi")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "number", "target_temperature_control")) == "unknown"
    assert _state(hass, _entity_id_by_key(hass, "sensor", "probe_1_temp")) == "unknown"


async def test_coordinator_ignores_stale_shadow_versions(hass: HomeAssistant) -> None:
    """Verify older shadow versions from push or poll do not overwrite newer versions."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_ENTRY_DATA, unique_id=MOCK_THING_NAME)
    entry.add_to_hass(hass)

    initial = deepcopy(SAMPLE_SHADOW)
    initial["version"] = 10
    stale = deepcopy(SAMPLE_SHADOW)
    stale["version"] = 9
    stale["state"]["reported"]["mainTemp"] = 100

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        side_effect=[initial, stale],
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        main_temp_eid = _entity_id_by_key(hass, "sensor", "main_temp")
        assert _state(hass, main_temp_eid) == "225"

        # Newer push updates to version 12
        newer = deepcopy(SAMPLE_SHADOW)
        newer["version"] = 12
        newer["state"]["reported"]["mainTemp"] = 240
        entry.runtime_data._handle_push_update(newer)
        await hass.async_block_till_done()
        assert _state(hass, main_temp_eid) == "240"

        # Stale push (version 11 < 12) is ignored
        older_push = deepcopy(SAMPLE_SHADOW)
        older_push["version"] = 11
        older_push["state"]["reported"]["mainTemp"] = 180
        entry.runtime_data._handle_push_update(older_push)
        await hass.async_block_till_done()
        assert _state(hass, main_temp_eid) == "240"

        # Stale poll (version 9 < 12) is ignored
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert _state(hass, main_temp_eid) == "240"


async def test_offline_detection_connectivity_availability_and_recovery(hass: HomeAssistant) -> None:
    """Verify stale device reports mark controller offline while keeping diagnostics available."""
    hass.config.units = US_CUSTOMARY_SYSTEM
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={**MOCK_ENTRY_DATA, CONF_SCAN_INTERVAL: 300},
        unique_id=MOCK_THING_NAME,
    )
    entry.add_to_hass(hass)

    # Initial poll shows shadow updated 300s after last device report -> offline
    stale_shadow = deepcopy(SAMPLE_SHADOW)
    stale_shadow["timestamp"] = 1700000410

    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=stale_shadow,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    connected_eid = _entity_id_by_key(hass, "binary_sensor", "connected")
    last_reported_eid = _entity_id_by_key(hass, "sensor", "last_reported")
    shadow_updated_eid = _entity_id_by_key(hass, "sensor", "shadow_last_updated")
    power_eid = _entity_id_by_key(hass, "switch", "power_control")
    main_temp_eid = _entity_id_by_key(hass, "sensor", "main_temp")
    target_eid = _entity_id_by_key(hass, "number", "target_temperature_control")
    heating_eid = _entity_id_by_key(hass, "binary_sensor", "heating")

    assert _state(hass, connected_eid) == "off"
    assert _state(hass, last_reported_eid) != "unavailable"
    assert _state(hass, shadow_updated_eid) != "unavailable"
    for eid in (power_eid, main_temp_eid, target_eid, heating_eid):
        assert _state(hass, eid) == "unavailable"

    # A fresh push from the device brings entities back online immediately
    fresh_push = deepcopy(SAMPLE_SHADOW)
    fresh_push["timestamp"] = 1700000500
    fresh_push["metadata"]["reported"]["mainTemp"]["timestamp"] = 1700000498
    entry.runtime_data._handle_push_update(fresh_push)
    await hass.async_block_till_done()

    assert _state(hass, connected_eid) == "on"
    assert _state(hass, power_eid) == "on"
    assert _state(hass, main_temp_eid) == "225"

    # Advancing past 120s without new reports flips Connectivity off and entities unavailable
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=125))
    await hass.async_block_till_done()

    assert _state(hass, connected_eid) == "off"
    assert _state(hass, power_eid) == "unavailable"
    assert _state(hass, main_temp_eid) == "unavailable"

    # Setting offline_timeout=0 disables offline gating
    entry_disabled = MockConfigEntry(
        domain=DOMAIN,
        data=MOCK_ENTRY_DATA,
        options={CONF_OFFLINE_TIMEOUT: 0},
        unique_id="venom-offline-disabled",
    )
    entry_disabled.add_to_hass(hass)
    with patch(
        "custom_components.spider_venom.api.SpiderVenomClient.async_get_shadow",
        return_value=stale_shadow,
    ):
        assert await hass.config_entries.async_setup(entry_disabled.entry_id)
        await hass.async_block_till_done()

    assert entry_disabled.runtime_data.is_device_online is True
