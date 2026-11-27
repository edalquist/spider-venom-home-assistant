"""Data update coordinator for Spider Grills Venom."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import SpiderVenomApiError, SpiderVenomClient
from .const import (
    CONF_OFFLINE_TIMEOUT,
    DEFAULT_OFFLINE_TIMEOUT,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)
_DEFAULT_OPTIMISTIC_TTL = timedelta(seconds=60)

type SpiderVenomConfigEntry = ConfigEntry[SpiderVenomDataUpdateCoordinator]


@dataclass(slots=True)
class _OptimisticEntry:
    """Pending optimistic value awaiting confirmation from shadow polling."""

    value: Any
    baseline: Any
    expires_at: datetime
    unsub: CALLBACK_TYPE | None = None


def _extract_reported(data: dict[str, Any] | None) -> dict[str, Any]:
    """Extract the reported state dict safely."""
    if not isinstance(data, dict):
        return {}
    state = data.get("state")
    if not isinstance(state, dict):
        return {}
    reported = state.get("reported")
    return reported if isinstance(reported, dict) else {}


def value_at_path(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Return a nested value from a dict."""
    value: Any = data
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def latest_reported_timestamp(shadow: dict[str, Any] | None) -> datetime | None:
    """Return the newest timestamp from shadow metadata.reported."""
    metadata = shadow.get("metadata") if isinstance(shadow, dict) else None
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


class SpiderVenomDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinate Spider Grills Venom shadow polling."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: SpiderVenomClient,
        entry: SpiderVenomConfigEntry,
    ) -> None:
        """Initialize the coordinator."""
        interval_seconds = entry.options.get(
            CONF_SCAN_INTERVAL,
            entry.data.get(CONF_SCAN_INTERVAL),
        )
        interval = (
            timedelta(seconds=interval_seconds)
            if isinstance(interval_seconds, int)
            and not isinstance(interval_seconds, bool)
            and interval_seconds >= 10
            else DEFAULT_SCAN_INTERVAL
        )
        offline_timeout = entry.options.get(
            CONF_OFFLINE_TIMEOUT,
            entry.data.get(CONF_OFFLINE_TIMEOUT, DEFAULT_OFFLINE_TIMEOUT),
        )
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=interval,
        )
        self.client = client
        self._optimistic: dict[tuple[str, ...], _OptimisticEntry] = {}
        self._last_shadow_version: int | None = None
        self._offline_timeout: int = (
            offline_timeout
            if isinstance(offline_timeout, int)
            and not isinstance(offline_timeout, bool)
            and offline_timeout >= 0
            else DEFAULT_OFFLINE_TIMEOUT
        )
        self._shadow_received_at: datetime | None = None
        self._last_reported_at: datetime | None = None
        self._age_at_receipt: float | None = None
        self._timed_out = False
        self._offline_unsub: CALLBACK_TYPE | None = None

    def _is_stale_version(self, data: dict[str, Any]) -> bool:
        """Return True if data carries a strictly older shadow version than already seen."""
        version = data.get("version")
        if isinstance(version, int) and not isinstance(version, bool):
            if self._last_shadow_version is not None and version < self._last_shadow_version:
                return True
            self._last_shadow_version = version
        return False

    def _update_liveness(self, data: dict[str, Any]) -> None:
        """Record latest device report timestamp and schedule offline timeout."""
        last_reported = latest_reported_timestamp(data)
        self._last_reported_at = last_reported
        now = dt_util.utcnow()
        self._shadow_received_at = now
        if self._offline_unsub is not None:
            self._offline_unsub()
            self._offline_unsub = None

        if last_reported is None:
            self._age_at_receipt = None
            self._timed_out = False
            return

        reference_dt = now
        shadow_ts = data.get("timestamp")
        if not isinstance(shadow_ts, bool) and isinstance(shadow_ts, int | float) and shadow_ts > 0:
            try:
                reference_dt = datetime.fromtimestamp(shadow_ts, UTC)
            except (OverflowError, OSError, ValueError):
                reference_dt = now

        age_at_receipt = max(0.0, (reference_dt - last_reported).total_seconds())
        self._age_at_receipt = age_at_receipt
        self._timed_out = self._offline_timeout > 0 and age_at_receipt >= self._offline_timeout

        if self._offline_timeout > 0 and not self._timed_out:
            @callback
            def _on_offline_timeout(_now: datetime) -> None:
                self._offline_unsub = None
                self._timed_out = True
                self.async_update_listeners()

            self._offline_unsub = async_call_later(
                self.hass,
                self._offline_timeout - age_at_receipt,
                _on_offline_timeout,
            )

    @property
    def last_reported_at(self) -> datetime | None:
        """Return the latest device report timestamp from shadow metadata."""
        return self._last_reported_at

    @property
    def is_device_online(self) -> bool:
        """Return True if the controller has reported within offline_timeout."""
        if self._offline_timeout <= 0:
            return True
        if self._timed_out:
            return False
        if self._age_at_receipt is None or self._shadow_received_at is None:
            return True
        elapsed = max(0.0, (dt_util.utcnow() - self._shadow_received_at).total_seconds())
        return (self._age_at_receipt + elapsed) < self._offline_timeout

    async def async_start_push(self) -> None:
        """Start the persistent MQTT push connection."""
        await self.client.async_start(self._handle_push_update, on_reconnect=self._handle_reconnect)

    async def async_stop_push(self) -> None:
        """Stop the persistent MQTT push connection."""
        await self.client.async_stop()

    @callback
    def _handle_reconnect(self) -> None:
        """Request a shadow refresh after persistent MQTT reconnects."""
        self.hass.async_create_task(self.async_request_refresh())

    @callback
    def _handle_push_update(self, data: dict[str, Any]) -> None:
        """Apply a pushed shadow update from MQTT."""
        if self._is_stale_version(data):
            return
        self._update_liveness(data)
        self._reconcile_optimistic(data)
        self.async_set_updated_data(data)

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from AWS IoT."""
        try:
            data = await self.client.async_get_shadow()
        except SpiderVenomApiError as err:
            raise UpdateFailed(str(err)) from err

        if self._is_stale_version(data) and self.data is not None:
            return self.data
        self._update_liveness(data)
        self._reconcile_optimistic(data)
        return data

    @property
    def reported(self) -> dict[str, Any]:
        """Return reported shadow state."""
        return _extract_reported(self.data)

    def set_optimistic_value(
        self,
        path: tuple[str, ...],
        value: Any,
        ttl: timedelta = _DEFAULT_OPTIMISTIC_TTL,
    ) -> None:
        """Record an optimistic desired state value and notify listeners."""
        prior = self._optimistic.pop(path, None)
        if prior is not None and prior.unsub is not None:
            prior.unsub()

        baseline = value_at_path(self.reported, path)
        expires_at = dt_util.utcnow() + ttl
        entry = _OptimisticEntry(value=value, baseline=baseline, expires_at=expires_at)

        @callback
        def _on_expire(_now: datetime) -> None:
            if self._optimistic.get(path) is entry:
                self._optimistic.pop(path, None)
                self.async_update_listeners()

        entry.unsub = async_call_later(self.hass, ttl.total_seconds(), _on_expire)
        self._optimistic[path] = entry
        self.async_update_listeners()

    def get_optimistic_value(self, path: tuple[str, ...]) -> tuple[bool, Any]:
        """Return (is_active, value) for an optimistic override without mutating state."""
        entry = self._optimistic.get(path)
        if entry is None or dt_util.utcnow() >= entry.expires_at:
            return False, None
        return True, entry.value

    @callback
    def async_clear_timers(self) -> None:
        """Cancel all coordinator timers (offline timeout and optimistic)."""
        if self._offline_unsub is not None:
            self._offline_unsub()
            self._offline_unsub = None
        self.async_clear_optimistic_timers()

    @callback
    def async_clear_optimistic_timers(self) -> None:
        """Cancel all pending optimistic expiration timers."""
        for entry in self._optimistic.values():
            if entry.unsub is not None:
                entry.unsub()
        self._optimistic.clear()

    def _reconcile_optimistic(self, data: dict[str, Any]) -> None:
        """Clear optimistic entries once confirmed, expired, or superseded."""
        if not self._optimistic:
            return

        reported = _extract_reported(data)
        now = dt_util.utcnow()
        for path, entry in list(self._optimistic.items()):
            fresh_value = value_at_path(reported, path)
            baseline = entry.baseline
            if isinstance(entry.value, bool):
                fresh_cmp: Any = bool(fresh_value) if fresh_value is not None else None
                baseline_cmp: Any = bool(baseline) if baseline is not None else None
            else:
                fresh_cmp = fresh_value
                baseline_cmp = baseline

            if (
                now >= entry.expires_at
                or fresh_cmp == entry.value
                or fresh_cmp != baseline_cmp
            ):
                if entry.unsub is not None:
                    entry.unsub()
                self._optimistic.pop(path, None)
