from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Callable, Optional

import sdt_loader
from config_loader import AppConfig
from onem2m_client import DownlinkItem, OneM2MClient, req_topic, resp_topic
from zigbee_handler import UplinkFrame, ZigbeeHandler, level_to_percent, percent_to_level, rgb_to_xy, xy_to_rgb

logger = logging.getLogger(__name__)


class ResourceType(IntEnum):
    ACP = 1
    AE = 2
    CONTAINER = 3
    CONTENT_INSTANCE = 4
    CSE_BASE = 5
    GROUP = 9
    SUBSCRIPTION = 23
    FLEX_CONTAINER = 28


class Operation(IntEnum):
    CREATE = 1
    RETRIEVE = 2
    UPDATE = 3
    DELETE = 4
    NOTIFY = 5

NET_UPDATE = 1


class OneM2MError(Exception):
    """A oneM2M request came back with a non-2xxx response status code."""

    def __init__(self, rsc: Optional[int], pc: object = None):
        self.rsc = rsc
        self.pc = pc
        super().__init__(f"oneM2M response status code {rsc}")

ON_OFF_CLUSTER = 0x0006
LEVEL_CONTROL_CLUSTER = 0x0008
COLOR_CLUSTER = 0x0300

ZCL_CLUSTER_TO_MODULE: dict[int, str] = {
    ON_OFF_CLUSTER: "binarySwitch",
    LEVEL_CONTROL_CLUSTER: "brightness",
    COLOR_CLUSTER: "colour",
}

_KNOWN_MODULES = frozenset(ZCL_CLUSTER_TO_MODULE.values())

_COLOR_CAPABILITIES_XY_BIT = 0x0008

_HA_PROFILE_ID = 0x0104

_ZCL_DEVICE_TYPE_TO_SDT_CLASS: dict[tuple[int, int], str] = {
    (_HA_PROFILE_ID, 0x0100): "deviceLight",  # ON_OFF_LIGHT
    (_HA_PROFILE_ID, 0x0101): "deviceLight",  # DIMMABLE_LIGHT
    (_HA_PROFILE_ID, 0x0102): "deviceLight",  # COLOR_DIMMABLE_LIGHT
    (_HA_PROFILE_ID, 0x010C): "deviceLight",  # COLOR_TEMPERATURE_LIGHT
    (_HA_PROFILE_ID, 0x010D): "deviceLight",  # EXTENDED_COLOR_LIGHT
}

_MODULE_TO_CLUSTER: dict[str, int] = {module: cluster for cluster, module in ZCL_CLUSTER_TO_MODULE.items()}

_MODULE_READ_ATTRS: dict[str, list[str]] = {
    "binarySwitch": ["on_off"],
    "brightness": ["current_level"],
    "colour": ["current_x", "current_y"],
}

_MODULE_DEFAULTS: dict[str, dict[str, object]] = {
    "binarySwitch": {"state": False},
    "brightness": {"brightness": 0},
    "colour": {"red": 0, "green": 0, "blue": 0},
    "faultDetection": {"status": False},
}

FAULT_DETECTION_MODULE = "faultDetection"

ATTR_NAME_MAP: dict[tuple[int, str], tuple[str, Callable[[object], object], Callable[[object], object]]] = {
    (ON_OFF_CLUSTER, "on_off"): ("state", lambda v: bool(v), lambda v: bool(v)),
    (LEVEL_CONTROL_CLUSTER, "current_level"): ("brightness", level_to_percent, percent_to_level),
}

_COLOR_XY_ATTRS = ("current_x", "current_y")

_RGB_FIELDS = ("red", "green", "blue")
_KNOWN_DATAPOINT_FIELDS = ("state", "brightness", "red", "green", "blue")
_SHORTNAME_TO_FIELD: dict[str, str] = {
    sdt_loader.SHORT_NAMES[field]: field for field in _KNOWN_DATAPOINT_FIELDS
}


def _tracked_attrs_for_modules(modules: tuple[str, ...]) -> dict[int, list[str]]:
    """Which attribute names to Read per cluster for a device's actual modules."""
    grouped: dict[int, list[str]] = {}
    for module in modules:
        attrs = _MODULE_READ_ATTRS.get(module)
        cluster = _MODULE_TO_CLUSTER.get(module)
        if attrs is not None and cluster is not None:
            grouped[cluster] = attrs
    return grouped

DOWNLINK_ATTR_MAP: dict[tuple[str, str], tuple[int, str, Callable[[object], object]]] = {
    (ZCL_CLUSTER_TO_MODULE[cluster], name): (cluster, attr, downlink_convert)
    for (cluster, attr), (name, _uplink_convert, downlink_convert) in ATTR_NAME_MAP.items()
}


@dataclass(frozen=True)
class DeviceProfile:
    device_class: str          # sdt_loader.DEVICE_CLASSES key, e.g. "deviceLight"
    modules: tuple[str, ...]   # only the modules this device actually supports


def _classify_device(
    cluster_ids: set[int],
    profile_id: Optional[int] = None,
    device_type: Optional[int] = None,
) -> Optional[DeviceProfile]:
    supported = {ZCL_CLUSTER_TO_MODULE[c] for c in cluster_ids if c in ZCL_CLUSTER_TO_MODULE}
    if not supported:
        return None

    def _required_ok(device_class: str) -> bool:
        module_specs = sdt_loader.DEVICE_CLASSES.get(device_class, [])
        required = {name for name, is_required in module_specs if is_required}
        return required.issubset(supported)

    best_class: Optional[str] = None
    if profile_id is not None and device_type is not None:
        declared_class = _ZCL_DEVICE_TYPE_TO_SDT_CLASS.get((profile_id, device_type))
        if declared_class is not None and declared_class in sdt_loader.DEVICE_CLASSES and _required_ok(declared_class):
            best_class = declared_class

    if best_class is None:
        best_score = -1
        for device_class in sdt_loader.DEVICE_CLASSES:
            if not _required_ok(device_class):
                continue
            all_names = {name for name, _ in sdt_loader.DEVICE_CLASSES[device_class]}
            score = len(all_names & supported)
            if score > best_score:
                best_class, best_score = device_class, score

    if best_class is None:
        return None
    all_names = {name for name, _ in sdt_loader.DEVICE_CLASSES[best_class]}
    modules = supported & all_names
    # faultDetection has no ZCL backing, added declaratively if the class lists it.
    if FAULT_DETECTION_MODULE in all_names:
        modules = modules | {FAULT_DETECTION_MODULE}
    return DeviceProfile(device_class=best_class, modules=tuple(sorted(modules)))

_DEVICE_RN_PREFIX = "bulb"


def _sanitize_ieee(ieee_addr: str) -> str:
    return ieee_addr.replace(":", "").upper()


def _device_rn(ieee_addr: str, endpoint: int) -> str:
    return f"{_DEVICE_RN_PREFIX}_{_sanitize_ieee(ieee_addr)}_{endpoint}"


def _device_target_uri(ae_path: str, ieee_addr: str, endpoint: int) -> str:
    return f"{ae_path}/{_device_rn(ieee_addr, endpoint)}"

def _device_shortname(device_class: str) -> str:
    return f"{sdt_loader.DOMAIN_PREFIX}:{sdt_loader.SHORT_NAMES[device_class]}"


def _module_shortname(module: str) -> str:
    return f"{sdt_loader.DOMAIN_PREFIX}:{sdt_loader.SHORT_NAMES[module]}"


def _datapoint_shortnames(fields: dict[str, object]) -> dict[str, object]:
    """Each DataPoint inside a module flexContainer needs its own shortName
    as the JSON key too, not just the wrapper -- a strict CSE (confirmed
    against real TinyIoT) rejects a long field name outright."""
    return {sdt_loader.SHORT_NAMES[field]: value for field, value in fields.items()}


def _device_cnd(device_class: str) -> str:
    return f"org.onem2m.common.device.{device_class}"


def _module_cnd(module: str) -> str:
    return f"org.onem2m.common.moduleclass.{module}"


# CSE field name -> which module resource it lives under.
_MODULE_BY_FIELD: dict[str, str] = {
    "state": "binarySwitch",
    "brightness": "brightness",
    "red": "colour",
    "green": "colour",
    "blue": "colour",
}


def _module_uri(ae_path: str, ieee_addr: str, endpoint: int, module: str) -> str:
    return f"{_device_target_uri(ae_path, ieee_addr, endpoint)}/{module}"


def _parse_device_rn(rn: str) -> Optional[tuple[str, int]]:
    """Inverse of _device_rn(): recovers (ieee_addr, endpoint), or None if
    `rn` doesn't match the bulb_<IEEE>_<EP> pattern."""
    prefix = f"{_DEVICE_RN_PREFIX}_"
    if not rn.startswith(prefix):
        return None
    ieee_hex, _, ep_str = rn[len(prefix):].rpartition("_")
    if len(ieee_hex) != 16 or not ep_str.isdigit():
        return None
    ieee_addr = ":".join(ieee_hex[i:i + 2] for i in range(0, 16, 2)).lower()
    return ieee_addr, int(ep_str)


def _normalize_ieee(raw: str) -> Optional[str]:
    """Canonicalize an admin-supplied ieee (colon or bare hex) to the
    lowercase colon-separated form every internal cache is keyed by."""
    hex_only = raw.replace(":", "").replace("-", "")
    if len(hex_only) != 16 or not all(c in "0123456789abcdefABCDEF" for c in hex_only):
        return None
    return ":".join(hex_only[i:i + 2] for i in range(0, 16, 2)).lower()


def _extract_aei(resp: dict) -> Optional[str]:
    """Pull the CSE-assigned AE-ID out of a RETRIEVE/CREATE response for the AE resource."""
    body = (resp.get("pc") or {}).get("m2m:ae")
    return body.get("aei") if isinstance(body, dict) else None


def _extract_cin_con(rep: dict) -> object:
    """Pull a contentInstance's `con` out of a Notify `rep`, JSON-decoding it if it's a string."""
    body = rep.get("m2m:cin") if isinstance(rep, dict) else None
    if not isinstance(body, dict):
        return None
    con = body.get("con")
    if isinstance(con, str):
        try:
            return json.loads(con)
        except json.JSONDecodeError:
            return con
    return con


class Translator:
    def __init__(
        self,
        cfg: AppConfig,
        zigbee: ZigbeeHandler,
        onem2m: OneM2MClient,
        uplink_queue: "asyncio.Queue[UplinkFrame]",
        downlink_queue: "asyncio.Queue[DownlinkItem]",
    ):
        self.cfg = cfg
        self.zigbee = zigbee
        self.onem2m = onem2m
        self._uplink_queue = uplink_queue
        self._downlink_queue = downlink_queue

        self._pending: dict[str, asyncio.Future[dict]] = {}
        self._last_online_status: dict[str, bool] = {}
        self._permit_join_active = False
        self._pending_color_xy: dict[str, dict[str, object]] = {}
        self._pending_rgb: dict[str, dict[str, object]] = {}
        self._pending_colour_report: dict[str, "asyncio.Future[None]"] = {}
        self._device_brightness: dict[str, int] = {}
        self._device_on: dict[str, bool] = {}
        self._pending_self_update: dict[str, int] = {}
        self._last_known_fields: dict[str, dict[str, object]] = {}
        self._device_profiles: dict[str, DeviceProfile] = {}

        self._ae_path = f"{cfg.onem2m.cse_base}/{cfg.onem2m.ae_name}"
        self._control_uri = f"{self._ae_path}/{cfg.onem2m.control_rn}"
        self._poa = f"mqtt://{cfg.mqtt.broker_host}:{cfg.mqtt.broker_port}"
        self._ae_id: Optional[str] = None

    def _notify_target(self) -> str:
        return f"{self._poa}/{self._ae_id}/json"

    async def request(
        self,
        op: int,
        to: str,
        fr: str,
        ty: Optional[int] = None,
        pc: Optional[dict] = None,
        timeout: Optional[float] = None,
        publish_topic: Optional[str] = None,
    ) -> dict:
        rqi = uuid.uuid4().hex
        body: dict = {"op": op, "to": to, "fr": fr, "rqi": rqi, "rvi": "3"}
        if ty is not None:
            body["ty"] = ty
        if pc is not None:
            body["pc"] = pc

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict] = loop.create_future()
        self._pending[rqi] = future
        topic = publish_topic or req_topic(fr, self.cfg.onem2m.cse_id)
        try:
            self.onem2m.publish(topic, json.dumps(body))
        except ConnectionError:
            self._pending.pop(rqi, None)
            raise

        try:
            return await asyncio.wait_for(future, timeout=timeout or self.cfg.onem2m.request_timeout_sec)
        except asyncio.TimeoutError as exc:
            self._pending.pop(rqi, None)
            raise TimeoutError(f"oneM2M request timed out (op={op}, to={to})") from exc

    async def request_ok(
        self,
        op: int,
        to: str,
        fr: str,
        ty: Optional[int] = None,
        pc: Optional[dict] = None,
        ok_codes: tuple[int, ...] = (2000, 2001, 2002, 2004),
        timeout: Optional[float] = None,
        publish_topic: Optional[str] = None,
    ) -> dict:
        resp = await self.request(op, to, fr, ty=ty, pc=pc, timeout=timeout, publish_topic=publish_topic)
        if resp.get("rsc") not in ok_codes:
            raise OneM2MError(resp.get("rsc"), resp.get("pc"))
        return resp

    async def bootstrap(self) -> None:
        acp_path = await self._ensure_acp()
        await self._ensure_ae(acp_path)
        await self._ensure_control_resource()
        await self._sync_devices_from_nvm()

    async def _ensure_acp(self) -> str:
        acp_path = f"{self.cfg.onem2m.cse_base}/{self.cfg.onem2m.acp_name}"
        resp = await self.request(int(Operation.RETRIEVE), to=acp_path, fr=self.cfg.onem2m.origin)
        if resp.get("rsc") == 2000:
            logger.info("ACP %s already exists", self.cfg.onem2m.acp_name)
            return acp_path

        pc = {
            "m2m:acp": {
                "rn": self.cfg.onem2m.acp_name,
                "pv": {"acr": [{"acor": self.cfg.onem2m.allowed_originators, "acop": 63}]},
                "pvs": {"acr": [{"acor": self.cfg.onem2m.allowed_originators, "acop": 63}]},
            }
        }
        await self.request_ok(
            int(Operation.CREATE), to=self.cfg.onem2m.cse_base, fr=self.cfg.onem2m.origin,
            ty=int(ResourceType.ACP), pc=pc,
        )
        logger.info("Created ACP %s", self.cfg.onem2m.acp_name)
        return acp_path

    async def _ensure_ae(self, acp_path: str) -> None:
        """Retrieve/register the AE under the fixed admin originator
        (self.cfg.onem2m.origin) -- TS-0001's "assign me an AE-ID"
        self-registration convention was rejected by real TinyIoT, so this
        uses the simpler pre-known-identity model instead.
        """
        resp = await self.request(int(Operation.RETRIEVE), to=self._ae_path, fr=self.cfg.onem2m.origin)
        if resp.get("rsc") == 2000:
            self._ae_id = _extract_aei(resp) or self.cfg.onem2m.origin
            logger.info("AE %s already registered, originator=%s", self.cfg.onem2m.ae_name, self._ae_id)
        else:
            pc = {
                "m2m:ae": {
                    "rn": self.cfg.onem2m.ae_name,
                    "api": self.cfg.onem2m.app_id,
                    "rr": True,
                    "acpi": [acp_path],
                    "lbl": ["IPE"],
                    "poa": [self._poa],
                }
            }
            resp = await self.request_ok(
                int(Operation.CREATE), to=self.cfg.onem2m.cse_base, fr=self.cfg.onem2m.origin,
                ty=int(ResourceType.AE), pc=pc,
            )
            self._ae_id = _extract_aei(resp) or self.cfg.onem2m.origin
            logger.info("Registered AE %s, assigned originator=%s", self.cfg.onem2m.ae_name, self._ae_id)

        # Only now do we know our real AE-ID -- wire up topics that depend on it.
        self.onem2m.subscribe(resp_topic(self._ae_id, self.cfg.onem2m.cse_id))
        self.onem2m.subscribe(req_topic(self.cfg.onem2m.cse_id, self._ae_id))

    async def _ensure_subscription(
        self, target_uri: str, sub_rn: str, net: list[int], nct: int = 1
    ) -> None:
        sub_uri = f"{target_uri}/{sub_rn}"
        nu = [self._notify_target()]
        resp = await self.request(int(Operation.RETRIEVE), to=sub_uri, fr=self._ae_id)
        if resp.get("rsc") == 2000:
            existing_nu = ((resp.get("pc") or {}).get("m2m:sub") or {}).get("nu")
            if existing_nu == nu:
                return
            await self.request_ok(
                int(Operation.UPDATE), to=sub_uri, fr=self._ae_id,
                pc={"m2m:sub": {"nu": nu}},
            )
            logger.info("Updated stale nu on subscription %s", sub_uri)
            return

        pc = {"m2m:sub": {"rn": sub_rn, "nu": nu, "nct": nct, "enc": {"net": net}}}
        await self.request_ok(
            int(Operation.CREATE), to=target_uri, fr=self._ae_id,
            ty=int(ResourceType.SUBSCRIPTION), pc=pc,
        )
        logger.info("Created subscription %s", sub_uri)

    async def _ensure_control_resource(self) -> None:
        resp = await self.request(int(Operation.RETRIEVE), to=self._control_uri, fr=self._ae_id)
        if resp.get("rsc") != 2000:
            pc = {"m2m:cnt": {"rn": self.cfg.onem2m.control_rn}}
            await self.request_ok(
                int(Operation.CREATE), to=self._ae_path, fr=self._ae_id,
                ty=int(ResourceType.CONTAINER), pc=pc,
            )
            logger.info("Created control resource %s", self.cfg.onem2m.control_rn)

        await self._ensure_subscription(self._control_uri, f"sub_{self.cfg.onem2m.control_rn}", net=[3])

    async def _sync_devices_from_nvm(self) -> None:
        try:
            nvm_devices = await self.zigbee.discover_paired_devices()
        except ConnectionError:
            logger.exception("Failed to read paired-device list from coordinator NVM")
            return

        for device in nvm_devices:
            ieee_addr = device["ieee_addr"]
            endpoint = device.get("endpoint", 1)
            target_uri = _device_target_uri(self._ae_path, ieee_addr, endpoint)
            try:
                resp = await self.request(int(Operation.RETRIEVE), to=target_uri, fr=self._ae_id)
            except (OneM2MError, TimeoutError):
                logger.exception("Failed to check CSE resource for NVM device %s", ieee_addr)
                continue

            if resp.get("rsc") != 2000:
                await self._register_new_device(ieee_addr)  # orphaned in NVM but missing from the CSE
                continue

            profile = await self._classify_device_confirmed(ieee_addr, endpoint)
            if profile is None:
                continue
            self._device_profiles[ieee_addr] = profile
            await self._create_device_subscription(ieee_addr, endpoint, profile)  # heals a stale nu, if any
            attrs_by_cluster = _tracked_attrs_for_modules(profile.modules)
            if not attrs_by_cluster:
                continue
            # Listener state is in-memory only -- must be re-attached every restart.
            self.zigbee.watch_device(ieee_addr, endpoint, list(attrs_by_cluster.keys()))
            try:
                await self._refresh_single_device(ieee_addr, endpoint, attrs_by_cluster)
            except (asyncio.TimeoutError, ConnectionError):
                logger.warning("No response refreshing state for %s, leaving as-is", ieee_addr)
                await self._report_online_status(ieee_addr, endpoint, online=False)

    async def _read_device_attrs(
        self, ieee_addr: str, endpoint: int, attrs_by_cluster: dict[int, list[str]]
    ) -> dict[str, object]:
        """Read Attributes round trip only -- no CSE traffic. Raises on
        send/response failure; callers decide what "no answer" means."""
        fields: dict[str, object] = {}
        for cluster, attr_names in attrs_by_cluster.items():
            values = await asyncio.wait_for(
                self.zigbee.read_attributes(ieee_addr, endpoint, cluster, attr_names),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
            if cluster == COLOR_CLUSTER:
                color_fields = self._accumulate_color_xy(ieee_addr, values)
                if color_fields:
                    fields.update(color_fields)
                continue
            for attr_name, value in values.items():
                target = ATTR_NAME_MAP.get((cluster, attr_name))
                if target is not None:
                    name, convert, _downlink_convert = target
                    converted = convert(value)
                    fields[name] = converted
                    if name == "state":
                        self._device_on[ieee_addr] = bool(converted)
                    elif name == "brightness":
                        self._device_brightness[ieee_addr] = converted  # real level, kept regardless of on/off
                        fields[name] = self._effective_brightness(ieee_addr)
        return fields

    def _effective_brightness(self, ieee_addr: str) -> int:
        """What the CSE should show right now -- 0 while off, the real
        remembered level once on (self._device_brightness keeps tracking
        the real level even while off, so nothing needs restoring)."""
        if not self._device_on.get(ieee_addr, True):
            return 0
        return self._device_brightness.get(ieee_addr, 100)

    def _accumulate_color_xy(self, ieee_addr: str, values: dict[str, object]) -> Optional[dict[str, object]]:
        buf = self._pending_color_xy.setdefault(ieee_addr, {})
        buf.update({k: v for k, v in values.items() if k in _COLOR_XY_ATTRS})
        if "current_x" not in buf or "current_y" not in buf:
            return None
        brightness_ratio = self._effective_brightness(ieee_addr) / 100
        r, g, b = xy_to_rgb(buf["current_x"] / 65535, buf["current_y"] / 65535, brightness_ratio)  # type: ignore[operator]
        return {"red": r, "green": g, "blue": b}

    def _accumulate_rgb(self, ieee_addr: str, field: str, value: object) -> Optional[tuple[int, int, int]]:
        """Downlink mirror of _accumulate_color_xy(): buffer red/green/blue
        until all three are known before issuing the color command."""
        buf = self._pending_rgb.setdefault(ieee_addr, {})
        buf[field] = value
        if not all(k in buf for k in _RGB_FIELDS):
            return None
        return int(buf["red"]), int(buf["green"]), int(buf["blue"])  # type: ignore[arg-type]

    async def _refresh_single_device(
        self, ieee_addr: str, endpoint: int, attrs_by_cluster: dict[int, list[str]]
    ) -> None:
        fields = await self._read_device_attrs(ieee_addr, endpoint, attrs_by_cluster)

        await self._report_online_status(ieee_addr, endpoint, online=True)
        if not fields:
            return
        self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Refreshed")

    async def _resync_device(self, ieee_addr: str, endpoint: int) -> None:
        profile = self._device_profiles.get(ieee_addr)
        if profile is None:
            return

        device_uri = _device_target_uri(self._ae_path, ieee_addr, endpoint)
        try:
            resp = await self.request(int(Operation.RETRIEVE), to=device_uri, fr=self._ae_id)
        except TimeoutError:
            logger.warning("Resync existence check timed out for %s, will retry on next recovery", ieee_addr)
            return
        if resp.get("rsc") != 2000:
            logger.warning("CSE resource for %s missing on resync, re-registering", ieee_addr)
            await self._register_new_device(ieee_addr)
            return

        attrs_by_cluster = _tracked_attrs_for_modules(profile.modules)
        if not attrs_by_cluster:
            return
        try:
            await self._refresh_single_device(ieee_addr, endpoint, attrs_by_cluster)
        except (asyncio.TimeoutError, ConnectionError):
            logger.warning("Resync read failed for %s, will retry on next report/healthcheck", ieee_addr)

    def _publish_fields_by_module(
        self, ieee_addr: str, endpoint: int, fields: dict[str, object], *, log_verb: str
    ) -> None:
        by_module: dict[str, dict[str, object]] = {}
        for field, value in fields.items():
            module = _MODULE_BY_FIELD.get(field)
            if module is not None:
                by_module.setdefault(module, {})[field] = value

        for module, module_fields in by_module.items():
            # Sole place _last_known_fields gets updated -- always a
            # confirmed-real value, so _revert_fields() has a true fallback.
            self._last_known_fields.setdefault(ieee_addr, {}).update(module_fields)

            target_uri = _module_uri(self._ae_path, ieee_addr, endpoint, module)
            body = {
                "op": int(Operation.UPDATE),
                "to": target_uri,
                "fr": self._ae_id,
                "rqi": uuid.uuid4().hex,
                "rvi": "3",
                "pc": {_module_shortname(module): _datapoint_shortnames(module_fields)},
            }
            # Incremented before publishing -- see _handle_notification()'s self-echo check.
            self._pending_self_update[target_uri] = self._pending_self_update.get(target_uri, 0) + 1
            try:
                self.onem2m.publish(req_topic(self._ae_id, self.cfg.onem2m.cse_id), json.dumps(body))
                logger.info("%s %s -> %s", log_verb, target_uri, module_fields)
            except ConnectionError:
                self._pending_self_update[target_uri] -= 1
                if self._pending_self_update[target_uri] <= 0:
                    del self._pending_self_update[target_uri]
                logger.exception("Failed to publish %s update for %s", log_verb.lower(), target_uri)

    async def _report_online_status(self, ieee_addr: str, endpoint: int, online: bool) -> None:
        if self._last_online_status.get(ieee_addr) == online:
            return
        profile = self._device_profiles.get(ieee_addr)
        if profile is None or FAULT_DETECTION_MODULE not in profile.modules:
            logger.warning("No faultDetection module for %s, skipping online-status report", ieee_addr)
            return
        self._last_online_status[ieee_addr] = online

        target_uri = _module_uri(self._ae_path, ieee_addr, endpoint, FAULT_DETECTION_MODULE)
        body = {
            "op": int(Operation.UPDATE),
            "to": target_uri,
            "fr": self._ae_id,
            "rqi": uuid.uuid4().hex,
            "rvi": "3",
            "pc": {_module_shortname(FAULT_DETECTION_MODULE): _datapoint_shortnames({"status": not online})},
        }
        try:
            self.onem2m.publish(req_topic(self._ae_id, self.cfg.onem2m.cse_id), json.dumps(body))
            logger.info("Device %s online=%s", ieee_addr, online)
        except ConnectionError:
            logger.exception("Failed to report online=%s for %s", online, target_uri)

    # ------------------------------------------------------------------
    # Downlink: MQTT raw text -> rqi correlation OR ZCL command
    # ------------------------------------------------------------------
    async def run_downlink_consumer(self) -> None:
        while True:
            item = await self._downlink_queue.get()
            try:
                await self._process_downlink_item(item)
            except Exception:
                logger.exception("Error processing downlink item on %s", item.get("topic"))
            finally:
                self._downlink_queue.task_done()

    async def _process_downlink_item(self, item: DownlinkItem) -> None:
        try:
            payload = json.loads(item["payload"])
        except json.JSONDecodeError:
            logger.warning("Discarding non-JSON MQTT message on %s", item["topic"])
            return

        rqi = payload.get("rqi")
        future = self._pending.pop(rqi, None) if rqi else None
        if future is not None:
            logger.debug("Resolved pending request rqi=%s rsc=%s", rqi, payload.get("rsc"))
            if not future.cancelled():
                future.set_result(payload)
            return

        if "m2m:sgn" in payload or payload.get("op") == int(Operation.NOTIFY):
            logger.debug("Notify on %s: %s", item["topic"], payload)
            await self._handle_notification(payload)
        else:
            logger.debug("Unhandled downlink message on %s: %s", item["topic"], payload)

    async def _handle_notification(self, payload: dict) -> None:
        req_fr = payload.get("fr", self.cfg.onem2m.cse_id)  # CSE (originator of the Notify)
        req_to = payload.get("to", self._ae_id)     # this AE (receiver)
        rqi = payload.get("rqi", uuid.uuid4().hex)

        sgn = payload.get("m2m:sgn") or (payload.get("pc") or {}).get("m2m:sgn") or {}
        nev = sgn.get("nev", {}) if isinstance(sgn, dict) else {}
        rep = nev.get("rep", {}) if isinstance(nev, dict) else {}
        net = nev.get("net") if isinstance(nev, dict) else None
        sur = sgn.get("sur", "") if isinstance(sgn, dict) else ""

        target_uri = sur.rstrip("/").rsplit("/", 1)[0] if sur else ""
        if not target_uri:
            logger.warning("Downlink notification missing subscription reference, ignoring")
            return

        ack = {"rqi": rqi, "fr": req_to, "to": req_fr, "rsc": 2000}
        try:
            self.onem2m.publish(resp_topic(req_fr, req_to), json.dumps(ack))
        except ConnectionError:
            logger.exception("Failed to ack Notify rqi=%s", rqi)

        if target_uri == self._control_uri:
            await self._handle_control_notification(rep)
            return

        if net != NET_UPDATE:
            return
        
        pending = self._pending_self_update.get(target_uri, 0)
        if pending > 0:
            if pending > 1:
                self._pending_self_update[target_uri] = pending - 1
            else:
                del self._pending_self_update[target_uri]
            return

        module_name = target_uri.rsplit("/", 1)[-1]
        device_uri = target_uri.rsplit("/", 1)[0]
        device_rn = device_uri.rsplit("/", 1)[-1]
        parsed = _parse_device_rn(device_rn)
        if module_name not in _KNOWN_MODULES or parsed is None:
            logger.warning("Update notify for unrecognized target %s, ignoring", target_uri)
            return
        ieee_addr, endpoint = parsed
        if not self.zigbee.is_paired(ieee_addr):
            logger.warning("Update notify for unpaired device %s, ignoring", ieee_addr)
            return
        await self._dispatch_downlink_commands(ieee_addr, endpoint, module_name, rep)

    async def _handle_control_notification(self, rep: dict) -> None:
        con = _extract_cin_con(rep)
        cmd = con.get("cmd") if isinstance(con, dict) else con

        if cmd == "removeDevice":
            ieee_raw = con.get("ieee") if isinstance(con, dict) else None
            await self._handle_remove_device_command(ieee_raw)
            return

        if cmd != "permitJoin":
            logger.debug("Ignoring control CIN with unrecognized content: %r", con)
            return

        if self._permit_join_active:
            logger.debug("permitJoin already in progress, ignoring repeated request")
            return
        self._permit_join_active = True

        duration = self.cfg.pairing.permit_join_duration_sec
        try:
            await asyncio.wait_for(
                self.zigbee.send_permit_join(duration),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
        except (ConnectionError, asyncio.TimeoutError):
            logger.exception("Failed to open permit-join window")
            self._permit_join_active = False
            return

        asyncio.create_task(self._reset_permit_join_after(duration), name="permit_join_reset")

    async def _reset_permit_join_after(self, duration_sec: int) -> None:
        """Clears the in-progress flag once the join window actually closes."""
        await asyncio.sleep(duration_sec)
        self._permit_join_active = False

    async def _handle_remove_device_command(self, ieee_raw: Optional[str]) -> None:
        ieee_addr = _normalize_ieee(ieee_raw) if ieee_raw else None
        if ieee_addr is None:
            logger.warning("removeDevice command missing/malformed ieee: %r", ieee_raw)
            return
        if not self.zigbee.is_paired(ieee_addr):
            logger.warning("removeDevice command for unknown/already-removed device %s, ignoring", ieee_addr)
            return

        try:
            await self.zigbee.remove_device(ieee_addr)
        except ConnectionError:
            logger.exception("Failed to remove %s from the Zigbee network", ieee_addr)
            return

        await self._deregister_device(ieee_addr, endpoint=1, reason="removed via control command")

    async def _dispatch_downlink_commands(
        self, ieee_addr: str, endpoint: int, module_name: str, rep: dict
    ) -> bool:
        ok = True
        for body in rep.values() if isinstance(rep, dict) else []:
            if not isinstance(body, dict):
                continue
            for raw_field, value in body.items():
                field = _SHORTNAME_TO_FIELD.get(raw_field, raw_field)
                if field in _RGB_FIELDS:
                    success = await self._apply_rgb_downlink(ieee_addr, endpoint, field, value)
                elif field == "brightness":
                    success = await self._apply_brightness_downlink(ieee_addr, endpoint, value)
                elif field == "state":
                    success = await self._apply_state_downlink(ieee_addr, endpoint, value)
                else:
                    target = DOWNLINK_ATTR_MAP.get((module_name, field))
                    if target is None:
                        continue
                    cluster_id, attr_name, convert = target
                    success = await self._apply_simple_downlink(
                        ieee_addr, endpoint, field, value, cluster_id, attr_name, convert
                    )
                ok = ok and success
        return ok

    async def _apply_simple_downlink(
        self,
        ieee_addr: str,
        endpoint: int,
        field: str,
        value: object,
        cluster_id: int,
        attr_name: str,
        convert: Callable[[object], object],
    ) -> bool:
        try:
            # Bounded explicitly -- an unbounded hang here would stall every
            # other item on downlink_queue, not just this device's command.
            await asyncio.wait_for(
                self.zigbee.send_command(ieee_addr, endpoint, cluster_id, attr_name, convert(value)),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
            logger.info("Downlink %s.%s=%s -> %s/ep%d", ieee_addr, field, value, ieee_addr, endpoint)
            return True
        except (ConnectionError, asyncio.TimeoutError):
            logger.exception("Failed to send downlink command to %s, marking offline", ieee_addr)
            await self._report_online_status(ieee_addr, endpoint, online=False)
            await self._revert_fields(ieee_addr, endpoint, (field,))
            return False

    async def _revert_fields(self, ieee_addr: str, endpoint: int, fields: tuple[str, ...]) -> None:
        known = self._last_known_fields.get(ieee_addr, {})
        revert = {field: known[field] for field in fields if field in known}
        if revert:
            self._publish_fields_by_module(ieee_addr, endpoint, revert, log_verb="Reverted")

    async def _apply_state_downlink(self, ieee_addr: str, endpoint: int, value: object) -> bool:
        cluster_id, attr_name, convert = DOWNLINK_ATTR_MAP[("binarySwitch", "state")]
        success = await self._apply_simple_downlink(ieee_addr, endpoint, "state", value, cluster_id, attr_name, convert)
        if not success:
            return False
        self._device_on[ieee_addr] = bool(value)
        fields: dict[str, object] = {"brightness": self._effective_brightness(ieee_addr)}
        rgb_fields = self._accumulate_color_xy(ieee_addr, {})
        if rgb_fields:
            fields.update(rgb_fields)
        self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Derived")
        return True

    async def _apply_brightness_downlink(self, ieee_addr: str, endpoint: int, value: object) -> bool:
        cluster_id, attr_name, convert = DOWNLINK_ATTR_MAP[("brightness", "brightness")]
        success = await self._apply_simple_downlink(
            ieee_addr, endpoint, "brightness", value, cluster_id, attr_name, convert
        )
        if not success:
            return False
        percent = max(0, min(100, int(value)))  # type: ignore[arg-type]
        on = percent > 0
        self._device_on[ieee_addr] = on
        if on:
            self._device_brightness[ieee_addr] = percent  # only overwrite the real level on a nonzero write
        fields: dict[str, object] = {"brightness": self._effective_brightness(ieee_addr), "state": on}
        rgb_fields = self._accumulate_color_xy(ieee_addr, {})
        if rgb_fields:
            fields.update(rgb_fields)
        self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Derived")
        return True

    async def _apply_rgb_downlink(self, ieee_addr: str, endpoint: int, field: str, value: object) -> bool:
        rgb = self._accumulate_rgb(ieee_addr, field, value)
        if rgb is None:
            return True
        r, g, b = rgb
        x, y, brightness_ratio = rgb_to_xy(r, g, b)
        percent = max(0, min(100, round(brightness_ratio * 100)))
        on = percent > 0
        try:
            await asyncio.wait_for(
                self.zigbee.set_color_rgb(ieee_addr, endpoint, r, g, b),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
            await asyncio.wait_for(
                self.zigbee.send_command(
                    ieee_addr, endpoint, LEVEL_CONTROL_CLUSTER, "current_level", percent_to_level(percent)
                ),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
        except (ConnectionError, asyncio.TimeoutError):
            logger.exception("Failed to send downlink color command to %s, marking offline", ieee_addr)
            await self._report_online_status(ieee_addr, endpoint, online=False)
            await self._revert_fields(ieee_addr, endpoint, _RGB_FIELDS)
            return False
        logger.info(
            "Downlink %s.rgb=(%d,%d,%d) -> xy=(%.4f,%.4f) level=%d%%", ieee_addr, r, g, b, x, y, percent,
        )
        self._device_on[ieee_addr] = on
        if on:
            self._device_brightness[ieee_addr] = percent
        self._pending_color_xy[ieee_addr] = {
            "current_x": max(0, min(65535, round(x * 65535))),
            "current_y": max(0, min(65535, round(y * 65535))),
        }
        fields = {"brightness": self._effective_brightness(ieee_addr), "state": on}
        self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Derived")
        if on:
            asyncio.create_task(self._verify_colour_applied(ieee_addr, endpoint), name=f"colour_verify_{ieee_addr}")
        return True

    async def _verify_colour_applied(self, ieee_addr: str, endpoint: int) -> None:
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[None]" = loop.create_future()
        self._pending_colour_report[ieee_addr] = future
        try:
            await asyncio.wait_for(future, timeout=self.cfg.pairing.zcl_response_timeout_sec)
            return  # a report arrived -- _handle_zcl_report() already handled it
        except asyncio.TimeoutError:
            pass
        finally:
            self._pending_colour_report.pop(ieee_addr, None)

        try:
            values = await asyncio.wait_for(
                self.zigbee.read_attributes(ieee_addr, endpoint, COLOR_CLUSTER, list(_COLOR_XY_ATTRS)),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
        except (asyncio.TimeoutError, ConnectionError):
            logger.warning("Could not actively verify colour for %s, no report arrived either", ieee_addr)
            return
        fields = self._accumulate_color_xy(ieee_addr, values)
        if fields:
            self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Verified")

    # ------------------------------------------------------------------
    # Uplink: zigpy-decoded attribute reports -> oneM2M UPDATE
    # ------------------------------------------------------------------
    async def run_uplink_consumer(self) -> None:
        while True:
            frame = await self._uplink_queue.get()
            try:
                await self._handle_zcl_report(frame)
            except Exception:
                logger.exception("Error processing uplink frame from %s", frame.get("ieee_addr"))
            finally:
                self._uplink_queue.task_done()

    async def _handle_zcl_report(self, frame: UplinkFrame) -> None:
        ieee_addr = frame.get("ieee_addr", "")
        cluster = frame.get("cluster")
        endpoint = frame.get("endpoint") or 1
        attribute = frame.get("attribute")
        if cluster is None or attribute is None:
            return

        if not self.zigbee.is_paired(ieee_addr):
            logger.warning("Uplink from unpaired device %s, ignoring (join first)", ieee_addr)
            return

        was_offline = self._last_online_status.get(ieee_addr) is False
        await self._report_online_status(ieee_addr, endpoint, online=True)
        if was_offline:
            asyncio.create_task(self._resync_device(ieee_addr, endpoint), name=f"resync_{ieee_addr}")

        if cluster == COLOR_CLUSTER and attribute in _COLOR_XY_ATTRS:
            pending_colour = self._pending_colour_report.get(ieee_addr)
            if pending_colour is not None and not pending_colour.done():
                pending_colour.set_result(None)
            fields = self._accumulate_color_xy(ieee_addr, {attribute: frame.get("value")})
        else:
            target = ATTR_NAME_MAP.get((cluster, attribute))
            fields = {target[0]: target[1](frame.get("value"))} if target is not None else None
            if fields and "state" in fields:
                self._device_on[ieee_addr] = fields["state"]
                fields["brightness"] = self._effective_brightness(ieee_addr)
                rgb_fields = self._accumulate_color_xy(ieee_addr, {})
                if rgb_fields:
                    fields.update(rgb_fields)
            elif fields and "brightness" in fields:
                self._device_brightness[ieee_addr] = fields["brightness"]
                fields["brightness"] = self._effective_brightness(ieee_addr)
                rgb_fields = self._accumulate_color_xy(ieee_addr, {})
                if rgb_fields:
                    fields.update(rgb_fields)
        if not fields:
            return
        self._publish_fields_by_module(ieee_addr, endpoint, fields, log_verb="Uplink")

    async def _handle_leave_indication(self, ieee_addr: str) -> None:
        if not self.zigbee.is_paired(ieee_addr):
            return
        await self._deregister_device(ieee_addr, endpoint=1, reason="left the network")

    async def _deregister_device(self, ieee_addr: str, endpoint: int, reason: str) -> None:
        target_uri = _device_target_uri(self._ae_path, ieee_addr, endpoint)
        try:
            await self.request_ok(
                int(Operation.DELETE), to=target_uri, fr=self._ae_id,
                ok_codes=(2002, 4004),  # 4004 NOT_FOUND is fine -- already gone is the goal state
            )
        except (OneM2MError, TimeoutError) as exc:
            debug = f", debug={exc.pc}" if isinstance(exc, OneM2MError) else ""
            logger.exception("Failed to delete CSE resource %s for %s%s", target_uri, ieee_addr, debug)

        self._last_online_status.pop(ieee_addr, None)
        self._pending_color_xy.pop(ieee_addr, None)
        self._pending_rgb.pop(ieee_addr, None)
        self._device_brightness.pop(ieee_addr, None)
        self._device_on.pop(ieee_addr, None)
        self._last_known_fields.pop(ieee_addr, None)
        pending_colour = self._pending_colour_report.pop(ieee_addr, None)
        if pending_colour is not None and not pending_colour.done():
            pending_colour.cancel()
        profile = self._device_profiles.pop(ieee_addr, None)
        for module in (profile.modules if profile is not None else _KNOWN_MODULES):
            self._pending_self_update.pop(_module_uri(self._ae_path, ieee_addr, endpoint, module), None)
        logger.info("Device %s %s, unpaired and deleted %s", ieee_addr, reason, target_uri)

    # ------------------------------------------------------------------
    # Health check: actively push online/offline for every paired device
    # ------------------------------------------------------------------
    async def run_healthcheck(self) -> None:
        interval = self.cfg.healthcheck.interval_sec
        threshold_sec = self.cfg.healthcheck.offline_threshold_sec
        while True:
            await asyncio.sleep(interval)
            try:
                nvm_devices = await self.zigbee.discover_paired_devices()
            except ConnectionError:
                logger.exception("Healthcheck: failed to read paired-device list from coordinator NVM")
                continue

            now = time.time()
            for device in nvm_devices:
                ieee_addr = device["ieee_addr"]
                endpoint = device.get("endpoint", 1)
                last_seen = device.get("last_seen")
                online = last_seen is not None and (now - last_seen) < threshold_sec
                was_offline = self._last_online_status.get(ieee_addr) is False
                await self._report_online_status(ieee_addr, endpoint, online)
                if online and was_offline:
                    # last_seen went fresh again without us ever having caught a report for it.
                    asyncio.create_task(self._resync_device(ieee_addr, endpoint), name=f"resync_{ieee_addr}")

    # ------------------------------------------------------------------
    # Pairing Manager
    # ------------------------------------------------------------------
    async def _classify_device_confirmed(self, ieee_addr: str, endpoint: int) -> Optional[DeviceProfile]:
        """_classify_device() from cluster list + device_type, then -- if
        "colour" is included -- confirms XY support with one extra read
        (_confirm_colour_support()), since Color Control's cluster presence
        alone doesn't guarantee XY specifically. Used everywhere a profile
        is computed so classification can't drift across restarts."""
        profile_id, device_type = self.zigbee.get_device_type(ieee_addr, endpoint)
        profile = _classify_device(
            self.zigbee.get_device_clusters(ieee_addr, endpoint), profile_id, device_type
        )
        if profile is None or "colour" not in profile.modules:
            return profile
        if await self._confirm_colour_support(ieee_addr, endpoint):
            return profile
        return DeviceProfile(
            device_class=profile.device_class,
            modules=tuple(m for m in profile.modules if m != "colour"),
        )

    async def _confirm_colour_support(self, ieee_addr: str, endpoint: int) -> bool:
        try:
            values = await asyncio.wait_for(
                self.zigbee.read_attributes(ieee_addr, endpoint, COLOR_CLUSTER, ["color_capabilities"]),
                timeout=self.cfg.pairing.zcl_response_timeout_sec,
            )
        except (asyncio.TimeoutError, ConnectionError):
            logger.warning("Could not read color_capabilities for %s, assuming no XY colour support", ieee_addr)
            return False
        capabilities = values.get("color_capabilities")
        return capabilities is not None and bool(int(capabilities) & _COLOR_CAPABILITIES_XY_BIT)

    async def _register_new_device(self, ieee_addr: str) -> None:
        endpoint = 1
        try:
            profile = await self._classify_device_confirmed(ieee_addr, endpoint)
            if profile is None:
                logger.warning(
                    "Device %s exposes no ZCL clusters we can map to an SDT ModuleClass, skipping registration",
                    ieee_addr,
                )
                return
            self._device_profiles[ieee_addr] = profile

            fields: dict[str, object] = {}
            online = True
            attrs_by_cluster = _tracked_attrs_for_modules(profile.modules)
            if attrs_by_cluster:
                # Attach report listeners before reading, so nothing sent in between is missed.
                self.zigbee.watch_device(ieee_addr, endpoint, list(attrs_by_cluster.keys()))
                try:
                    fields = await self._read_device_attrs(ieee_addr, endpoint, attrs_by_cluster)
                except (asyncio.TimeoutError, ConnectionError):
                    logger.warning("No response reading initial state for new device %s", ieee_addr)
                    online = False

            await self._create_device_flex_container(ieee_addr, endpoint, profile, online=online, fields=fields)
            await self._create_device_subscription(ieee_addr, endpoint, profile)
            self._last_online_status[ieee_addr] = online  # CREATE above already told the CSE this value

            logger.info(
                "Paired new device %s as %s (%s: %s)",
                ieee_addr, _device_rn(ieee_addr, endpoint), profile.device_class, ", ".join(profile.modules),
            )
        except OneM2MError as exc:
            logger.exception("CSE resource creation failed for %s, debug=%s", ieee_addr, exc.pc)
        except Exception:
            logger.exception("Pairing failed for %s", ieee_addr)

    async def _create_device_flex_container(
        self,
        ieee_addr: str,
        endpoint: int,
        profile: DeviceProfile,
        *,
        online: bool = False,
        fields: Optional[dict[str, object]] = None,
    ) -> None:
        fields = dict(fields or {})
        if FAULT_DETECTION_MODULE in profile.modules:
            fields["status"] = not online
        device_uri = _device_target_uri(self._ae_path, ieee_addr, endpoint)

        resp = await self.request(int(Operation.RETRIEVE), to=device_uri, fr=self._ae_id)
        if resp.get("rsc") != 2000:
            parent_pc = {
                _device_shortname(profile.device_class): {
                    "rn": _device_rn(ieee_addr, endpoint),
                    "cnd": _device_cnd(profile.device_class),
                }
            }
            await self.request_ok(
                int(Operation.CREATE), to=self._ae_path, fr=self._ae_id,
                ty=int(ResourceType.FLEX_CONTAINER), pc=parent_pc,
            )

        for module in profile.modules:
            module_uri = f"{device_uri}/{module}"
            resp = await self.request(int(Operation.RETRIEVE), to=module_uri, fr=self._ae_id)
            if resp.get("rsc") == 2000:
                continue
            defaults = _MODULE_DEFAULTS.get(module, {})
            module_fields = {
                field: fields.get(field, default) for field, default in defaults.items()
            }
            pc = {
                _module_shortname(module): {
                    "rn": module, "cnd": _module_cnd(module), **_datapoint_shortnames(module_fields),
                }
            }
            await self.request_ok(
                int(Operation.CREATE), to=device_uri, fr=self._ae_id,
                ty=int(ResourceType.FLEX_CONTAINER), pc=pc,
            )

    async def _create_device_subscription(self, ieee_addr: str, endpoint: int, profile: DeviceProfile) -> None:
        device_uri = _device_target_uri(self._ae_path, ieee_addr, endpoint)
        for module in profile.modules:
            if module == FAULT_DETECTION_MODULE:
                continue
            module_uri = f"{device_uri}/{module}"
            await self._ensure_subscription(module_uri, f"sub_{module}", net=[NET_UPDATE], nct=1)
