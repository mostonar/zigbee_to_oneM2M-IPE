from __future__ import annotations

import asyncio
import logging
from typing import Optional, TypedDict

import paho.mqtt.client as mqtt

from config_loader import MqttConfig, OneM2MConfig

logger = logging.getLogger(__name__)


class DownlinkItem(TypedDict):
    topic: str
    payload: str  # raw JSON text, unparsed -- Translator's job to decode


def req_topic(originator: str, receiver: str) -> str:
    return f"/oneM2M/req/{originator}/{receiver}/json"


def resp_topic(originator: str, receiver: str) -> str:
    return f"/oneM2M/resp/{originator}/{receiver}/json"


def reg_req_topic(credential_id: str, receiver: str) -> str:
    return f"/oneM2M/reg_req/{credential_id}/{receiver}/json"


def reg_resp_topic(credential_id: str, receiver: str) -> str:
    return f"/oneM2M/reg_resp/{credential_id}/{receiver}/json"


class OneM2MClient:

    def __init__(
        self,
        mqtt_cfg: MqttConfig,
        onem2m_cfg: OneM2MConfig,
        downlink_queue: "asyncio.Queue[DownlinkItem]",
    ):
        self._mqtt_cfg = mqtt_cfg
        self._onem2m_cfg = onem2m_cfg
        self._downlink_queue = downlink_queue
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=mqtt_cfg.client_id,
            clean_session=mqtt_cfg.clean_session,
        )
        if mqtt_cfg.username:
            self._client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        self._connected_event = asyncio.Event()
        self._connect_error: Optional[int] = None
        self._subscribed_topics: set[str] = set()  # replayed on every (re)connect

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> None:
        """Connect to the broker. Fail-fast: raises ConnectionError on failure/timeout."""
        self._loop = asyncio.get_running_loop()
        try:
            self._client.connect(
                self._mqtt_cfg.broker_host,
                self._mqtt_cfg.broker_port,
                keepalive=self._mqtt_cfg.keepalive,
            )
        except OSError as exc:
            raise ConnectionError(f"Failed to connect to MQTT broker: {exc}") from exc

        self._client.loop_start()
        try:
            await asyncio.wait_for(
                self._connected_event.wait(), timeout=self._mqtt_cfg.connect_timeout_sec
            )
        except asyncio.TimeoutError as exc:
            self._client.loop_stop()
            raise ConnectionError("Timed out waiting for MQTT CONNACK") from exc

        if self._connect_error is not None:
            self._client.loop_stop()
            raise ConnectionError(f"MQTT broker rejected connection, rc={self._connect_error}")

        logger.info(
            "Connected to MQTT broker %s:%d as %s",
            self._mqtt_cfg.broker_host,
            self._mqtt_cfg.broker_port,
            self._mqtt_cfg.client_id,
        )

    def disconnect(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        rc = reason_code.value if reason_code is not None else 0  # paho 2.x hands a ReasonCode, not an int
        self._connect_error = rc if rc != 0 else None
        if rc == 0 and self._subscribed_topics:
            for topic in self._subscribed_topics:
                self._client.subscribe(topic, qos=1)
            logger.info("Re-subscribed to %d topic(s) after (re)connect", len(self._subscribed_topics))
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._connected_event.set)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        logger.warning("MQTT disconnected, rc=%s", reason_code)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected_event.clear)

    # ------------------------------------------------------------------
    # Subscribe / publish -- no payload interpretation
    # ------------------------------------------------------------------
    def subscribe(self, topic: str, qos: int = 1) -> None:
        result, _ = self._client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError(f"Failed to subscribe to {topic}, rc={result}")
        self._subscribed_topics.add(topic)
        logger.debug("Subscribed to %s", topic)

    def publish(self, topic: str, payload_text: str, qos: int = 1) -> None:
        info = self._client.publish(topic, payload_text, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError(f"Failed to publish to {topic}, rc={info.rc}")
        logger.debug("-> %s (%d bytes)", topic, len(payload_text))

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        assert self._loop is not None
        try:
            text = msg.payload.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("Discarding non-UTF8 MQTT message on %s", msg.topic)
            return

        logger.debug("<- %s (%d bytes)", msg.topic, len(text))

        item: DownlinkItem = {"topic": msg.topic, "payload": text}
        self._loop.call_soon_threadsafe(self._enqueue_downlink, item)

    def _enqueue_downlink(self, item: DownlinkItem) -> None:
        try:
            self._downlink_queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.error(
                "downlink_queue full (maxsize=%d), dropping message on %s",
                self._mqtt_cfg.downlink_queue_maxsize,
                item["topic"],
            )
