from __future__ import annotations

import csv
import xml.etree.ElementTree as ET
from pathlib import Path

_SDT_DIR = Path(__file__).resolve().parent / "SDT"
_NS = {"sdt": "http://www.onem2m.org/xml/sdt/4.0"}


def _load_short_names() -> dict[str, str]:
    path = _SDT_DIR / "shortname.csv"
    names: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 2:
                continue
            name, short = row[0].strip(), row[1].strip()
            if name:
                names[name] = short
    return names


def _load_module_class_datapoints() -> dict[str, list[str]]:
    """ModuleClass name -> list of its DataPoint names."""
    tree = ET.parse(_SDT_DIR / "SDT-TS0023-ModuleClasses-Common.XML")
    module_classes = tree.getroot().find("sdt:ModuleClasses", _NS)
    result: dict[str, list[str]] = {}
    if module_classes is None:
        return result
    for mc in module_classes.findall("sdt:ModuleClass", _NS):
        name = mc.get("name")
        if not name:
            continue
        data = mc.find("sdt:Data", _NS)
        datapoints = [dp.get("name") for dp in data.findall("sdt:DataPoint", _NS)] if data is not None else []
        result[name] = [dp for dp in datapoints if dp]
    return result


def _load_device_classes() -> dict[str, list[tuple[str, bool]]]:
    tree = ET.parse(_SDT_DIR / "SDT-TS0023-Devices-Common.XML")
    device_classes = tree.getroot().find("sdt:DeviceClasses", _NS)
    result: dict[str, list[tuple[str, bool]]] = {}
    if device_classes is None:
        return result
    for dc in device_classes.findall("sdt:DeviceClass", _NS):
        name = dc.get("id")
        if not name:
            continue
        modules = dc.find("sdt:ModuleClasses", _NS)
        entries: list[tuple[str, bool]] = []
        if modules is not None:
            for mc in modules.findall("sdt:ModuleClass", _NS):
                mod_name = mc.get("name")
                if not mod_name:
                    continue
                required = mc.get("minOccurs", "0") != "0"
                entries.append((mod_name, required))
        result[name] = entries
    return result


SHORT_NAMES: dict[str, str] = _load_short_names()
MODULE_CLASS_DATAPOINTS: dict[str, list[str]] = _load_module_class_datapoints()
DEVICE_CLASSES: dict[str, list[tuple[str, bool]]] = _load_device_classes()

DOMAIN_PREFIX = "cod"
