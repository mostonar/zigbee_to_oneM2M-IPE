from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class OneM2MConfig(BaseModel):
    cse_base: str                      # CSEBase resource name, used in structured to=/fr= paths
    cse_id: str                        # CSE-ID, used for MQTT topic addressing -- can differ from cse_base
    ae_name: str
    app_id: str
    origin: str
    acp_name: str
    control_rn: str = "cnt_control"
    allowed_originators: list[str] = Field(default_factory=list)
    request_timeout_sec: float = 10.0

    @field_validator("allowed_originators")
    @classmethod
    def _must_include_origin(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("allowed_originators must not be empty")
        return v


class MqttConfig(BaseModel):
    broker_host: str
    broker_port: int = 1883
    client_id: str
    keepalive: int = 60
    clean_session: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    connect_timeout_sec: float = 10.0
    connect_retry: int = 0
    downlink_queue_maxsize: int = 500


class SerialConfig(BaseModel):
    use_tcp: bool = False
    port: str = "COM3"
    baudrate: int = 115200
    timeout: float = 1.0
    tcp_host: str = "127.0.0.1"
    tcp_port: int = 6638
    reconnect_delay_sec: float = 5.0
    radio_connect_timeout_sec: float = 40.0  # NCP reset + EZSP negotiation can take 10-30s
    uplink_queue_maxsize: int = 500
    database_path: str = "zigpy.db"  # zigpy's own network/device state persistence (NVM backup)
    flow_control: Optional[str] = None  # None confirmed correct for a real Sonoff ZBDongle-E


class PairingConfig(BaseModel):
    permit_join_duration_sec: int = 60
    zcl_response_timeout_sec: float = 5.0  # per ZCL send+response round trip


class HealthCheckConfig(BaseModel):
    interval_sec: float = 60.0            # sweep frequency
    offline_threshold_sec: float = 600.0  # no uplink report for this long -> mark offline


class AppConfig(BaseModel):
    onem2m: OneM2MConfig
    mqtt: MqttConfig
    serial: SerialConfig
    pairing: PairingConfig = Field(default_factory=PairingConfig)
    healthcheck: HealthCheckConfig = Field(default_factory=HealthCheckConfig)
    log_level: str = "INFO"


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return AppConfig.model_validate(raw)


@lru_cache(maxsize=1)
def get_config(path: str = "config.yaml") -> AppConfig:
    return load_config(path)
