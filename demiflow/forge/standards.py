"""Versioned engineering requirements, independent of any pipeline topology."""
from pathlib import Path

import json

from .platform_environment import read_bounded
from .platform import content_digest
from .schema import ForgeError


def engineering_standard(version=5):
    if type(version) is not int or version not in (1,2,3,4,5):
        raise ForgeError('Unsupported engineering standard version')
    value = json.loads(read_bounded(Path(__file__).with_name("standards") / f"engineering-v{version}.json", 256 * 1024))
    if set(value) != {"format", "id", "version", "rules"} or value["format"] != "demiflow_forge_standard_v1":
        raise ForgeError("Invalid engineering standard")
    if not isinstance(value["rules"], list) or not 1 <= len(value["rules"]) <= 64:
        raise ForgeError("Invalid engineering rules")
    ids = set()
    for rule in value["rules"]:
        if not isinstance(rule, dict) or set(rule) != {"id", "requirement", "scope", "verification", "coverage"}:
            raise ForgeError("Invalid engineering rule")
        if any(not isinstance(rule[key], str) or not rule[key] for key in ("id", "requirement", "scope", "coverage")) or rule["id"] in ids:
            raise ForgeError("Invalid engineering rule")
        if not isinstance(rule["verification"], list) or not rule["verification"] or any(method not in {"static", "behavior", "review"} for method in rule["verification"]):
            raise ForgeError("Invalid engineering rule")
        ids.add(rule["id"])
    return {**value, "digest": content_digest(value)}
