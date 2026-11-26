# Spider Grills Venom

Custom Home Assistant integration for Spider Grills Venom controllers.

This integration connects Home Assistant to a Spider Grills Venom through the cloud-backed AWS IoT Device Shadow used by the mobile app, maintaining a persistent MQTT-over-WebSocket connection for live shadow push updates with automatic reconnect. It can discover devices from a Spider Grills email/password account, read grill state, expose temperature/status entities, and control target temperature plus selected Venom operating toggles.

The integration does not store your Spider Grills password or access token after setup. It stores the selected AWS IoT Thing Name and uses temporary Cognito credentials for ongoing shadow reads/writes.

## Features

- Account-based setup with device discovery.
- Temperature, probe, Wi-Fi, firmware, and status entities.
- Power, engaged, paused, and high-temperature mode controls.
- Target temperature number entity and service.

## Installation

### HACS

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=edalquist&repository=spider-venom-home-assistant&category=integration)

Or manually in HACS, add this repository as a custom repository with category `Integration`, then install it from HACS:

```text
https://github.com/edalquist/spider-venom-home-assistant
```

Restart Home Assistant after installation, then add `Spider Grills Venom` from Settings -> Devices & services.

### Manual

Copy `custom_components/spider_venom` from this repository into your Home Assistant config directory:

```text
/config/custom_components/spider_venom
```

Restart Home Assistant, then add `Spider Grills Venom` from Settings -> Devices & services.

## Setup

Use the account setup flow with a Spider Grills email/password login. The integration uses it once to discover your devices.

Apple sign-in is not supported yet. Manual Thing Name setup was removed because there is no reliable, user-facing way to obtain the Thing Name and AWS IoT endpoint.

## Entities

Sensors include pit temperature, Probe 1 and Probe 2 temperatures, fan speed, cook start time, firmware, Wi-Fi details, signal strength, pit limits/alerts, flags, errors, and last reported/updated timestamps.

Binary sensors include lid, heating, Fahrenheit mode, and high/low temp alerts.

Controls include pit target temperature, Probe 1 and Probe 2 target temperatures, power, temperature control, pause, and high temp mode.

## Service

The integration also exposes:

```yaml
action: spider_venom.set_target_temperature
data:
  temperature: 300
```

If multiple Venoms are configured, pass a `target` (`device_id` or `entity_id`) or `thing_name`:

```yaml
action: spider_venom.set_target_temperature
target:
  device_id: your_venom_device_id
data:
  temperature: 300
```

## Notes

This version does not include timer controls. It also does not expose a vent-position entity because the observed shadow state does not include top or bottom vent opening fields.

This integration was developed with assistance from AI tools.
