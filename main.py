"""Wiring/orchestration only: instantiate queues, the two transport layers,
the Translator, run the bootstrap sequence, then run everything concurrently.

No ZCL/JSON parsing happens here -- see translator.py for that.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import colorama

from config_loader import AppConfig, load_config
from onem2m_client import DownlinkItem, OneM2MClient, reg_resp_topic, resp_topic
from translator import OneM2MError, Translator
from zigbee_handler import UplinkFrame, ZigbeeHandler

logger = logging.getLogger("ipe")


class _ColorFormatter(logging.Formatter):
    """Colors just the [LEVELNAME] token for at-a-glance readability."""

    _COLORS = {
        "DEBUG": "\033[36m",              # cyan
        "INFO": "\033[32m",               # green
        "WARNING": "\033[33m",            # yellow
        "ERROR": "\033[31m",              # red
        "CRITICAL": "\033[97m\033[41m",   # white on red
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS.get(record.levelname)
        original = record.levelname
        if color:
            record.levelname = f"{color}{original}{self._RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original


# Only these loggers get elevated to config.yaml's log_level -- zigpy/bellows/
# paho stay at INFO (their DEBUG output is otherwise a firehose).
_OWN_LOGGERS = ("ipe", "translator", "onem2m_client", "zigbee_handler")


def setup_logging(level: str) -> None:
    colorama.just_fix_windows_console()  # no-op except on old Windows consoles
    handler = logging.StreamHandler()
    handler.setFormatter(_ColorFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

    logging.basicConfig(level=logging.INFO, handlers=[handler])

    requested = getattr(logging, level.upper(), logging.INFO)
    for name in _OWN_LOGGERS:
        logging.getLogger(name).setLevel(requested)


async def _connect_onem2m(cfg: AppConfig, downlink_queue: "asyncio.Queue[DownlinkItem]") -> OneM2MClient:
    onem2m = OneM2MClient(cfg.mqtt, cfg.onem2m, downlink_queue)
    try:
        await onem2m.connect()
    except ConnectionError:
        logger.critical("Fatal: could not connect to MQTT broker, exiting", exc_info=True)
        sys.exit(1)

    # Pre-AE-ID bootstrap topics only -- Translator._ensure_ae() subscribes
    # our real AE-ID's topic once that's known.
    onem2m.subscribe(resp_topic(cfg.onem2m.origin, cfg.onem2m.cse_id))
    onem2m.subscribe(reg_resp_topic(cfg.onem2m.ae_name, cfg.onem2m.cse_id))
    return onem2m


async def _connect_zigbee(cfg: AppConfig, uplink_queue: "asyncio.Queue[UplinkFrame]") -> ZigbeeHandler:
    zigbee = ZigbeeHandler(cfg.serial, uplink_queue)
    try:
        await zigbee.connect()
    except ConnectionError:
        logger.critical("Fatal: could not open Zigbee serial/TCP link, exiting", exc_info=True)
        sys.exit(1)
    return zigbee


async def _shutdown(zigbee: ZigbeeHandler, onem2m: OneM2MClient, tasks: list["asyncio.Task"]) -> None:
    """Cancels every task and tears down both transports -- run from
    async_main()'s `finally` so cleanup happens no matter why it's exiting.
    """
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await zigbee.stop()
    except Exception:
        logger.exception("Error shutting down Zigbee radio")
    try:
        onem2m.disconnect()
    except Exception:
        logger.exception("Error disconnecting MQTT client")


async def async_main() -> None:
    cfg = load_config("config.yaml")
    setup_logging(cfg.log_level)

    uplink_queue: "asyncio.Queue[UplinkFrame]" = asyncio.Queue(maxsize=cfg.serial.uplink_queue_maxsize)
    downlink_queue: "asyncio.Queue[DownlinkItem]" = asyncio.Queue(maxsize=cfg.mqtt.downlink_queue_maxsize)

    onem2m = await _connect_onem2m(cfg, downlink_queue)
    zigbee = await _connect_zigbee(cfg, uplink_queue)

    # No local device mapping/cache: pairing state lives in zigpy's own NVM,
    # and CSE target URIs are computed deterministically -- see translator.py.
    translator = Translator(cfg, zigbee, onem2m, uplink_queue, downlink_queue)

    # Must be wired before bootstrap() opens permit-join, and the downlink
    # consumer must already be draining before bootstrap() issues requests.
    zigbee.start_listening(translator._register_new_device, translator._handle_leave_indication)
    downlink_task = asyncio.create_task(translator.run_downlink_consumer(), name="downlink_consumer")
    tasks = [downlink_task]

    try:
        try:
            await translator.bootstrap()
        except (OneM2MError, TimeoutError):
            logger.critical("Fatal error during oneM2M bootstrap", exc_info=True)
            raise SystemExit(1)

        # zigpy runs its own internal tasks once started (no zigbee_task
        # needed); device (de)registration is spawned per lifecycle
        # callback (start_listening() above), not a queued consumer.
        uplink_task = asyncio.create_task(translator.run_uplink_consumer(), name="uplink_consumer")
        healthcheck_task = asyncio.create_task(translator.run_healthcheck(), name="healthcheck")
        tasks += [uplink_task, healthcheck_task]

        logger.info("Bootstrap complete, entering runtime event loop")
        await asyncio.gather(*tasks)
    finally:
        await _shutdown(zigbee, onem2m, tasks)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
