from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional, TypedDict

import zigpy.types as t
from bellows.zigbee.application import ControllerApplication
from zigpy.zcl import AttributeReportedEvent
from zigpy.zcl.clusters.lighting import Color

from config_loader import SerialConfig

logger = logging.getLogger(__name__)

DeviceEventCallback = Callable[[str], Awaitable[None]]


def rgb_to_xy(r: int, g: int, b: int) -> tuple[float, float, float]:
    def gamma(c: int) -> float:
        v = c / 255.0
        return ((v + 0.055) / 1.055) ** 2.4 if v > 0.04045 else v / 12.92

    rl, gl, bl = gamma(r), gamma(g), gamma(b)
    x_ = rl * 0.649926 + gl * 0.103455 + bl * 0.197109
    y_ = rl * 0.234327 + gl * 0.743075 + bl * 0.022598
    z_ = gl * 0.053077 + bl * 1.035763
    total = x_ + y_ + z_
    if total <= 0:
        # r=g=b=0: any R=G=B converges to this same (x,y) as k->0 (the D65
        # white point), verified numerically -- not an arbitrary fallback.
        return 0.312730, 0.329020, 0.0
    return x_ / total, y_ / total, max(0.0, min(1.0, y_))


def xy_to_rgb(x: float, y: float, brightness: float = 1.0) -> tuple[int, int, int]:
    if y <= 0:
        return 0, 0, 0
    cap_y = brightness
    cap_x = (cap_y / y) * x
    cap_z = (cap_y / y) * (1 - x - y)

    r = cap_x * 3.2406 + cap_y * -1.5372 + cap_z * -0.4986
    g = cap_x * -0.9689 + cap_y * 1.8758 + cap_z * 0.0415
    b = cap_x * 0.0557 + cap_y * -0.2040 + cap_z * 1.0570

    def degamma(c: float) -> float:
        c = max(0.0, min(1.0, c))
        return 12.92 * c if c <= 0.0031308 else 1.055 * (c ** (1 / 2.4)) - 0.055

    r, g, b = degamma(r), degamma(g), degamma(b)
    return (
        max(0, min(255, round(r * 255))),
        max(0, min(255, round(g * 255))),
        max(0, min(255, round(b * 255))),
    )


ZCL_LEVEL_MIN = 1    # CurrentLevel practical min (0x01)
ZCL_LEVEL_MAX = 254  # CurrentLevel practical max (0xFE)


def level_to_percent(level: int) -> int:
    if level <= 0:
        return 0
    clamped = min(ZCL_LEVEL_MAX, level)
    return round((clamped - ZCL_LEVEL_MIN) * (100 - 1) / (ZCL_LEVEL_MAX - ZCL_LEVEL_MIN) + 1)


def percent_to_level(percent: int) -> int:
    """Inverse of level_to_percent()."""
    if percent <= 0:
        return 0
    clamped = min(100, percent)
    return round((clamped - 1) * (ZCL_LEVEL_MAX - ZCL_LEVEL_MIN) / (100 - 1) + ZCL_LEVEL_MIN)


class UplinkFrame(TypedDict, total=False):
    ieee_addr: str
    endpoint: int
    cluster: int
    attribute: str    # zigpy's own attribute name, e.g. "on_off"
    value: object      # already type-decoded (bool/int/str/...) by zigpy


class _ZigpyEventListener:

    def __init__(
        self,
        uplink_queue: "asyncio.Queue[UplinkFrame]",
        queue_maxsize: int,
        on_device_ready: DeviceEventCallback,
        on_device_leave: DeviceEventCallback,
    ) -> None:
        self._uplink_queue = uplink_queue
        self._queue_maxsize = queue_maxsize
        self._on_device_ready = on_device_ready
        self._on_device_leave = on_device_leave

    def _put(self, frame: UplinkFrame) -> None:
        try:
            self._uplink_queue.put_nowait(frame)
        except asyncio.QueueFull:
            logger.error(
                "uplink_queue full (maxsize=%d), dropping frame from %s",
                self._queue_maxsize, frame.get("ieee_addr"),
            )

    def device_initialized(self, device) -> None:
        asyncio.create_task(self._on_device_ready(str(device.ieee)), name=f"register_{device.ieee}")

    def device_init_failure(self, device) -> None:
        asyncio.create_task(self._on_device_ready(str(device.ieee)), name=f"register_{device.ieee}")

    def device_left(self, device) -> None:
        asyncio.create_task(self._on_device_leave(str(device.ieee)), name=f"leave_{device.ieee}")

    def on_attribute_reported(self, event: AttributeReportedEvent) -> None:
        self._put({
            "ieee_addr": event.device_ieee,
            "endpoint": event.endpoint_id,
            "cluster": event.cluster_id,
            "attribute": event.attribute_name,
            "value": event.value,
        })


class ZigbeeHandler:
    """Owns the zigpy ControllerApplication. Pure I/O -- pushes/pulls opaque buffers only."""

    def __init__(self, serial_cfg: SerialConfig, uplink_queue: "asyncio.Queue[UplinkFrame]"):
        self._cfg = serial_cfg
        self._uplink_queue = uplink_queue
        self._app: Optional[ControllerApplication] = None
        self._listener: Optional[_ZigpyEventListener] = None

    async def connect(self) -> None:
        if self._cfg.use_tcp:
            # bellows' native tcp:// scheme -- untested against real TCP hardware.
            device_path = f"tcp://{self._cfg.tcp_host}:{self._cfg.tcp_port}"
        else:
            device_path = self._cfg.port

        zigpy_config = {
            "device": {
                "path": device_path,
                "baudrate": self._cfg.baudrate,
                "flow_control": self._cfg.flow_control,
            },
            "database_path": self._cfg.database_path,
        }

        try:
            self._app = await ControllerApplication.new(zigpy_config, auto_form=True, start_radio=True)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"Failed to start zigpy/EZSP radio: {exc}") from exc

        logger.info(
            "Zigbee radio up on %s, coordinator IEEE=%s PAN=0x%04X channel=%d",
            device_path, self._app.state.node_info.ieee,
            self._app.state.network_info.pan_id, self._app.state.network_info.channel,
        )

    def start_listening(self, on_device_ready: DeviceEventCallback, on_device_leave: DeviceEventCallback) -> None:
        assert self._app is not None
        self._listener = _ZigpyEventListener(
            self._uplink_queue, self._cfg.uplink_queue_maxsize, on_device_ready, on_device_leave
        )
        self._app.add_listener(self._listener)

    async def discover_paired_devices(self) -> list[dict]:
        assert self._app is not None
        coordinator_ieee = self._app.state.node_info.ieee
        return [
            {
                "ieee_addr": str(dev.ieee),
                "short_addr": str(dev.nwk),
                "endpoint": 1,
                "last_seen": dev.last_seen,
            }
            for dev in self._app.devices.values()
            if dev.ieee != coordinator_ieee
        ]

    def is_paired(self, ieee_addr: str) -> bool:
        assert self._app is not None
        try:
            device = self._app.get_device(ieee=t.EUI64.convert(ieee_addr))
        except (KeyError, ValueError):
            return False
        return device.ieee != self._app.state.node_info.ieee

    def get_device_clusters(self, ieee_addr: str, endpoint: int) -> set[int]:
        assert self._app is not None
        try:
            device = self._app.get_device(ieee=t.EUI64.convert(ieee_addr))
        except (KeyError, ValueError):
            return set()
        ep = device.endpoints.get(endpoint)
        if ep is None:
            return set()
        return set(ep.in_clusters.keys())

    def get_device_type(self, ieee_addr: str, endpoint: int) -> tuple[Optional[int], Optional[int]]:
        assert self._app is not None
        try:
            device = self._app.get_device(ieee=t.EUI64.convert(ieee_addr))
        except (KeyError, ValueError):
            return None, None
        ep = device.endpoints.get(endpoint)
        if ep is None:
            return None, None
        device_type = ep.device_type
        return ep.profile_id, int(device_type) if device_type is not None else None

    def _get_cluster(self, ieee_addr: str, endpoint: int, cluster_id: int):
        assert self._app is not None
        try:
            device = self._app.get_device(ieee=t.EUI64.convert(ieee_addr))
        except (KeyError, ValueError) as exc:
            raise ConnectionError(f"Unknown device {ieee_addr}") from exc
        ep = device.endpoints.get(endpoint)
        if ep is None:
            raise ConnectionError(f"Device {ieee_addr} has no endpoint {endpoint}")
        cluster = ep.in_clusters.get(cluster_id)
        if cluster is None:
            raise ConnectionError(f"Device {ieee_addr} endpoint {endpoint} has no cluster 0x{cluster_id:04x}")
        return cluster

    def watch_device(self, ieee_addr: str, endpoint: int, cluster_ids: list[int]) -> None:
        assert self._listener is not None
        for cluster_id in cluster_ids:
            try:
                cluster = self._get_cluster(ieee_addr, endpoint, cluster_id)
            except ConnectionError:
                continue
            cluster.on_event(AttributeReportedEvent.event_type, self._listener.on_attribute_reported)

    async def read_attributes(
        self, ieee_addr: str, endpoint: int, cluster_id: int, attr_names: list[str]
    ) -> dict[str, object]:
        try:
            cluster = self._get_cluster(ieee_addr, endpoint, cluster_id)
        except ConnectionError:
            return {}
        try:
            success, _failed = await cluster.read_attributes(attr_names)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"Read Attributes failed for {ieee_addr}: {exc}") from exc
        return dict(success)

    async def send_command(
        self, ieee_addr: str, endpoint: int, cluster_id: int, attr_name: str, value: object
    ) -> None:
        cluster = self._get_cluster(ieee_addr, endpoint, cluster_id)
        try:
            if attr_name == "on_off":
                await (cluster.on() if value else cluster.off())
            elif attr_name == "current_level":
                await cluster.move_to_level_with_on_off(int(value), 0)  # type: ignore[arg-type]
            else:
                raise ValueError(f"No downlink command known for attribute {attr_name!r}")
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"send_command({attr_name}) failed for {ieee_addr}: {exc}") from exc

    async def set_color_rgb(
        self, ieee_addr: str, endpoint: int, r: int, g: int, b: int, transition_time: int = 0
    ) -> None:
        cluster = self._get_cluster(ieee_addr, endpoint, Color.cluster_id)
        x, y, _brightness = rgb_to_xy(int(r), int(g), int(b))
        color_x = max(0, min(65535, round(x * 65535)))
        color_y = max(0, min(65535, round(y * 65535)))
        try:
            await cluster.move_to_color(color_x, color_y, transition_time)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"Color command failed for {ieee_addr}: {exc}") from exc

    async def send_permit_join(self, duration_sec: int) -> None:
        assert self._app is not None
        try:
            await self._app.permit(time_s=duration_sec)
            logger.info("Permit-join opened for %ds", duration_sec)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"Zigbee permit-join failed: {exc}") from exc

    async def remove_device(self, ieee_addr: str) -> None:
        assert self._app is not None
        try:
            await self._app.remove(t.EUI64.convert(ieee_addr), remove_children=True, rejoin=False)
            logger.info("Removed %s from the Zigbee network", ieee_addr)
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"Zigbee device removal failed for {ieee_addr}: {exc}") from exc

    async def stop(self) -> None:
        if self._app is not None:
            await self._app.shutdown()
