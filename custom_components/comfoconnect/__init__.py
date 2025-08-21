"""Support to control a Zehnder ComfoAir Q350/450/600 ventilation unit."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from aiocomfoconnect import ComfoConnect, discover_bridges
from aiocomfoconnect.exceptions import (
    AioComfoConnectNotConnected,
    AioComfoConnectTimeout,
    ComfoConnectError,
    ComfoConnectNotAllowed,
)
from aiocomfoconnect.properties import (
    PROPERTY_FIRMWARE_VERSION,
    PROPERTY_MODEL,
    PROPERTY_NAME,
)
from aiocomfoconnect.sensors import Sensor
from aiocomfoconnect.util import version_decode
from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import CONF_HOST, EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryError,
    ConfigEntryNotReady,
)
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.dispatcher import dispatcher_send
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType

from .const import CONF_LOCAL_UUID, CONF_UUID, DOMAIN

PLATFORMS: list[Platform] = [
    Platform.FAN,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SELECT,
    Platform.BUTTON,
]

_LOGGER = logging.getLogger(__name__)

SIGNAL_COMFOCONNECT_UPDATE_RECEIVED = "comfoconnect_update_{}_{}"

KEEP_ALIVE_INTERVAL = timedelta(seconds=30)
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_BACKOFF_BASE = 2  # seconds


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Zehnder ComfoConnect integration from yaml."""
    if DOMAIN in config:
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN,
                context={"source": SOURCE_IMPORT},
                data=config[DOMAIN],
            )
        )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Zehnder ComfoConnect from a config entry."""

    hass.data.setdefault(DOMAIN, {})

    try:
        bridge = ComfoConnectBridge(hass, entry.data[CONF_HOST], entry.data[CONF_UUID])
        await bridge.connect(entry.data[CONF_LOCAL_UUID])

    except ComfoConnectNotAllowed:
        raise ConfigEntryAuthFailed("Access denied")

    except ComfoConnectError as err:
        raise ConfigEntryError from err

    except AioComfoConnectTimeout as err:
        # We got a timeout, this can happen when the IP address of the bridge has changed.
        _LOGGER.warning(
            'Timeout connecting to bridge "%s", trying discovery again.',
            entry.data[CONF_HOST],
        )

        bridges = await discover_bridges()
        discovered_bridge = next((b for b in bridges if b.uuid == entry.data[CONF_UUID]), None)
        if not discovered_bridge:
            _LOGGER.warning('Unable to discover bridge "%s". Retrying later.', entry.data[CONF_UUID])
            raise ConfigEntryNotReady from err

        # Try again, with the updated host this time
        bridge = ComfoConnectBridge(hass, discovered_bridge.host, entry.data[CONF_UUID])
        try:
            await bridge.connect(entry.data[CONF_LOCAL_UUID])

            # Update the host in the config entry
            hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_HOST: discovered_bridge.host})

        except ComfoConnectNotAllowed:
            raise ConfigEntryAuthFailed("Access denied")

        except ComfoConnectError as err:
            raise ConfigEntryNotReady from err

    hass.data[DOMAIN][entry.entry_id] = bridge

    # Get device information
    bridge_info = await bridge.cmd_version_request()
    unit_model = await bridge.get_property(PROPERTY_MODEL)
    unit_firmware = await bridge.get_property(PROPERTY_FIRMWARE_VERSION)
    unit_name = await bridge.get_property(PROPERTY_NAME)

    device_registry = dr.async_get(hass)

    # Add Bridge to device registry
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, bridge_info.serialNumber)},
        manufacturer="Zehnder",
        name=bridge_info.serialNumber,
        model="ComfoConnect LAN C",
        sw_version=version_decode(bridge_info.gatewayVersion),
    )

    # Add Ventilation Unit to device registry
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, bridge.uuid)},
        manufacturer="Zehnder",
        name=unit_name,
        model=unit_model,
        sw_version=version_decode(unit_firmware),
        via_device=(DOMAIN, bridge_info.serialNumber),
    )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Add reconnection tracking
    reconnect_attempts = 0
    reconnect_lock = asyncio.Lock()
    last_reconnect_time = 0

    @callback
    async def send_keepalive(now) -> None:
        """Send keepalive to the bridge."""
        nonlocal reconnect_attempts, last_reconnect_time

        # Check if we're already trying to reconnect
        if reconnect_lock.locked():
            _LOGGER.debug("Reconnection already in progress, skipping keepalive")
            return

        _LOGGER.debug("Sending keepalive...")
        try:
            # Use cmd_time_request as a keepalive since cmd_keepalive doesn't send back a reply we can wait for
            await bridge.cmd_time_request()

            # Reset reconnection attempts on successful communication
            reconnect_attempts = 0

            # TODO: Mark sensors as available

        except (AioComfoConnectNotConnected, AioComfoConnectTimeout) as exc:
            _LOGGER.debug("Keepalive failed: %s", exc)

            # Use lock to prevent concurrent reconnection attempts
            async with reconnect_lock:
                # Check if we've exceeded max attempts
                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    _LOGGER.error("Max reconnection attempts reached (%d), giving up", MAX_RECONNECT_ATTEMPTS)
                    # TODO: Mark all sensors as unavailable
                    return

                # Implement exponential backoff
                backoff_time = RECONNECT_BACKOFF_BASE ** reconnect_attempts
                current_time = now.timestamp()
                if current_time - last_reconnect_time < backoff_time:
                    _LOGGER.debug("Backoff period not elapsed, skipping reconnection attempt")
                    return

                reconnect_attempts += 1
                last_reconnect_time = current_time

                _LOGGER.info("Reconnection attempt %d/%d", reconnect_attempts, MAX_RECONNECT_ATTEMPTS)

                try:
                    await bridge.connect(entry.data[CONF_LOCAL_UUID])
                    _LOGGER.info("Reconnection successful")
                    reconnect_attempts = 0  # Reset on success
                except AioComfoConnectTimeout:
                    _LOGGER.warning("Reconnection attempt %d failed with timeout", reconnect_attempts)
                    # TODO: Mark all sensors as unavailable
                except Exception as reconnect_exc:
                    _LOGGER.error("Reconnection attempt %d failed: %s", reconnect_attempts, reconnect_exc)

    entry.async_on_unload(async_track_time_interval(hass, send_keepalive, KEEP_ALIVE_INTERVAL))

    # Disconnect when shutting down
    async def disconnect_bridge(event):
        """Close connection to the bridge."""
        await bridge.disconnect()

    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, disconnect_bridge))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        bridge = hass.data[DOMAIN][entry.entry_id]
        await bridge.disconnect()
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


class ComfoConnectBridge(ComfoConnect):
    """Representation of a ComfoConnect bridge."""

    def __init__(self, hass: HomeAssistant, host: str, uuid: str):
        """Initialize the ComfoConnect bridge."""
        super().__init__(
            host,
            uuid,
            hass.loop,
            self.sensor_callback,
            self.alarm_callback,
        )
        self.hass = hass
        self._connection_lock = asyncio.Lock()

    async def connect(self, local_uuid: str):
        """Connect to the bridge with concurrency protection."""
        async with self._connection_lock:
            return await super().connect(local_uuid)

    async def disconnect(self):
        """Disconnect from the bridge with concurrency protection."""
        async with self._connection_lock:
            return await super().disconnect()

    @callback
    def sensor_callback(self, sensor: Sensor, value):
        """Notify listeners that we have received an update."""
        dispatcher_send(
            self.hass,
            SIGNAL_COMFOCONNECT_UPDATE_RECEIVED.format(self.uuid, sensor.id),
            value,
        )

    @callback
    def alarm_callback(self, node_id, errors):
        """Print alarm updates."""
        message = f"Alarm received for Node {node_id}:\n"
        for error_id, error in errors.items():
            message += f"* {error_id}: {error}\n"
        _LOGGER.warning(message)
