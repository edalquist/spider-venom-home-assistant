# Changelog

## v0.3.0 - 2026-11-25

### Changed

- Switch AWS IoT shadow transport from HTTP REST to persistent MQTT-over-WebSocket push (`cloud_push`) with automatic reconnect and credential rotation.
- Align entity names with Spider Grills Venom mobile app terminology and add entity `translation_key`s.
- Remove duplicate read-only sensor and binary sensor entities already represented by controls.
- Use config entry title for device name and fall back to default AWS settings when omitted.
- Improve config flow duplicate-device abort, auth error handling, masked selectors, and add options flow for `scan_interval`.
- Support standard Home Assistant `device_id` and `entity_id` targets in `spider_venom.set_target_temperature`.

### Added

- Add `NumberDeviceClass.TEMPERATURE` to target temperature controls for automatic unit conversion.
- Add centralized optimistic state reconciliation and diagnostic `shadow_last_updated` and `last_reported` sensors.
- Add `pytest` suite, `ruff`, and `mypy` validation.

## v0.2.0 - 2026-07-25

### Added

- Add target temperature controls for Probe 1 and Probe 2.
- Add debug response-body logging for API discovery with token redaction.

### Notes

- Debug response logging is only emitted when `custom_components.spider_venom` is set to `debug`.
- Debug logs may include device and network metadata such as model, firmware version, Wi-Fi SSID, BSSID, and MAC address.
