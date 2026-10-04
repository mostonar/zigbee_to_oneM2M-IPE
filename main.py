from __future__ import annotations

import asyncio
import logging
import os

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


_OWN_LOGGERS = ("ipe", "translator", "onem2m_client", "zigbee_handler")

def setup_logging(level: str) -> None:
    colorama.just_fix_windows_console()
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
        os._exit(1)  # sys.exit() would hang on a lingering non-daemon thread

    onem2m.subscribe(resp_topic(cfg.onem2m.origin, cfg.onem2m.cse_id))
    onem2m.subscribe(reg_resp_topic(cfg.onem2m.ae_name, cfg.onem2m.cse_id))
    return onem2m


async def _connect_zigbee(cfg: AppConfig, uplink_queue: "asyncio.Queue[UplinkFrame]") -> ZigbeeHandler:
    zigbee = ZigbeeHandler(cfg.serial, uplink_queue)
    # asyncio.wait(), not wait_for(): a stuck NCP handshake can block inside
    # an uncancellable background thread, so wait_for()'s own cancel-and-wait
    # would hang too -- this just stops watching and force-exits instead.
    connect_task = asyncio.ensure_future(zigbee.connect())
    done, _pending = await asyncio.wait({connect_task}, timeout=cfg.serial.radio_connect_timeout_sec)
    if connect_task not in done:
        logger.critical("Fatal: Zigbee radio connect timed out, exiting")
        os._exit(1)
    exc = connect_task.exception()
    if exc is not None:
        logger.critical("Fatal: could not open Zigbee serial/TCP link, exiting", exc_info=exc)
        os._exit(1)  # sys.exit() would hang on a lingering non-daemon thread
    return zigbee


async def _shutdown(zigbee: ZigbeeHandler, onem2m: OneM2MClient, tasks: list["asyncio.Task"]) -> None:
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

    translator = Translator(cfg, zigbee, onem2m, uplink_queue, downlink_queue)

    zigbee.start_listening(translator._register_new_device, translator._handle_leave_indication)
    downlink_task = asyncio.create_task(translator.run_downlink_consumer(), name="downlink_consumer")
    tasks = [downlink_task]

    try:
        try:
            await translator.bootstrap()
        except (OneM2MError, TimeoutError):
            logger.critical("Fatal error during oneM2M bootstrap", exc_info=True)
            raise SystemExit(1)

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
