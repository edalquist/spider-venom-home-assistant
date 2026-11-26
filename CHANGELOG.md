# Changelog

## v0.3.1 - 2026-11-26

### Fixed

- Replace deprecated `target.device` filter in `services.yaml` with a `device` selector so Hassfest validation passes.
- Treat `paho-mqtt` `WebsocketConnectionError` and MQTT v3.1.1 failure disconnects (`is_failure`) as credential-refresh triggers instead of waiting until token expiry.
- Disable `paho-mqtt` internal auto-reconnect so reconnects always use fresh SigV4 presigned WebSocket URLs without racing the persistent loop.
- Guard `client.loop_start()` against concurrent shutdown, wake pending handshake waits on `async_stop()`, run inner attempt `finally` teardown before reconnect backoff sleeps, handle unexpected runner exceptions, and stop push tasks if `async_forward_entry_setups` fails.
- Clamp Cognito credential rotation delays (`>= 60s`) to prevent tight reconnect loops under clock skew, and avoid opening parallel short-lived MQTT connections while persistent mode is active.
- Refresh shadow state immediately on persistent MQTT reconnect and ignore stale out-of-order shadow `version` payloads.
- Guard cross-thread event-loop callbacks against `RuntimeError`, check `subscribe()`/`publish()` return codes, validate Cognito credential string fields, ignore booleans/non-numeric shadow values, and traverse list entries in `metadata.reported` timestamps.
- Switch optimistic expiration tracking to Home Assistant's `dt_util.utcnow()` clock and normalize AWS IoT endpoint hostnames consistently before SigV4 signing.

### Changed

- Register `spider_venom.set_target_temperature` in `async_setup`, support `area_id`/`floor_id`/`label_id` targets via `async_extract_config_entry_ids`, round float temperatures, and add service translations.
- Read dynamic `heat.t2.min`/`max` bounds on pit target temperature, raise `ServiceValidationError` on out-of-range inputs, and mark `fahrenheit_mode` as a diagnostic entity.
- Redact `ssid`, `bssid`, `mac`, additional account PII keys, and fallback Cognito error bodies in logs; stop persisting hardcoded AWS defaults in newly created config entries.
- Filter already-configured devices during setup discovery, add `NumberSelector` (`seconds`) with helper text for `scan_interval`, reuse Home Assistant's `get_default_context()`, and pin `paho-mqtt>=2.1.0,<3.0.0`.

### Added

- Add redacted config entry `diagnostics.py` support (`async_get_config_entry_diagnostics`).
- Add `icons.json` definitions for all entities and `spider_venom.set_target_temperature`.
- Expand the `pytest` suite (32 -> 58 tests), enable `mypy` checking on `tests/`, and ignore the HACS `license` check in CI pending upstream licensing.

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
