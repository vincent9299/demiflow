"""Read immutable Candidate-local resources through a bounded safe API."""
from __future__ import annotations

import copy
import json
import math
from pathlib import Path
from typing import Any

import yaml

_MAX_RESOURCE_BYTES = 2 * 1024 * 1024
_MAX_RESOURCE_DEPTH = 32
_MAX_RESOURCE_NODES = 100_000
_TEXT_SUFFIXES = frozenset({".txt", ".md", ".json", ".yaml", ".yml"})
_JSON_SUFFIXES = frozenset({".json"})
_YAML_SUFFIXES = frozenset({".yaml", ".yml"})
CANDIDATE_TEXT_FORMAT = "candidate_text_v1"
CANDIDATE_JSON_FORMAT = "candidate_json_v1"
CANDIDATE_YAML_FORMAT = "candidate_yaml_v1"


class ResourceAPI:
    """Read frozen top-level resources from the current Candidate package.

    Paths are literal top-level basenames inside ``pipeline/``. Absolute paths,
    traversal, nested paths, symlinks, and files larger than 2 MiB are rejected.
    ``read_json`` and ``read_yaml`` return strict JSON-compatible values only;
    YAML custom tags, dates, sets, binary values, and non-string mapping keys are
    unavailable. The API never expands environment variables or includes other
    files.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        if not self._root.is_dir() or self._root.is_symlink():
            raise ValueError("Candidate resource root must be a regular directory")
        self._cache: dict[tuple[str, str], Any] = {}

    def read_text(self, path: str) -> str:
        """Read one UTF-8 text resource by a static top-level basename."""
        key = ("text", str(path))
        if key not in self._cache:
            target = self._resolve(path, _TEXT_SUFFIXES)
            self._cache[key] = self._read_bytes(target).decode("utf-8")
        return str(self._cache[key])

    def read_json(self, path: str) -> Any:
        """Read strict JSON from one static top-level ``.json`` resource."""
        key = ("json", str(path))
        if key not in self._cache:
            target = self._resolve(path, _JSON_SUFFIXES)
            text = self._read_bytes(target).decode("utf-8")
            value = json.loads(
                text, parse_constant=_reject_json_constant,
                object_pairs_hook=_json_object,
            )
            self._cache[key] = _strict_json_value(value, "$", 0, [0], set())
        return copy.deepcopy(self._cache[key])

    def read_yaml(self, path: str) -> Any:
        """Read safe YAML normalized to strict JSON-compatible values."""
        key = ("yaml", str(path))
        if key not in self._cache:
            target = self._resolve(path, _YAML_SUFFIXES)
            value = yaml.load(
                self._read_bytes(target).decode("utf-8"), Loader=_StrictSafeLoader,
            )
            self._cache[key] = _strict_json_value(value, "$", 0, [0], set())
        return copy.deepcopy(self._cache[key])

    def _resolve(self, raw: str, suffixes: frozenset[str]) -> Path:
        value = str(raw)
        relative = Path(value)
        if (
            not value or relative.is_absolute() or len(relative.parts) != 1
            or relative.name in {"", ".", ".."}
            or "/" in value or "\\" in value or relative.suffix not in suffixes
        ):
            raise ValueError("Candidate resource path must be an allowed top-level basename")
        candidate = self._root / relative
        if candidate.is_symlink():
            raise ValueError("Candidate resource symlinks are forbidden")
        target = candidate.resolve()
        try:
            target.relative_to(self._root)
        except ValueError as exc:
            raise ValueError("Candidate resource escapes the package") from exc
        if not target.is_file():
            raise ValueError(f"Candidate resource is unavailable: {value}")
        return target

    @staticmethod
    def _read_bytes(target: Path) -> bytes:
        raw = target.read_bytes()
        if len(raw) > _MAX_RESOURCE_BYTES:
            raise ValueError(f"Candidate resource exceeds {_MAX_RESOURCE_BYTES} bytes")
        return raw


class _StrictSafeLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("Candidate YAML aliases are forbidden")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        pairs = self.construct_pairs(node, deep=deep)
        output = {}
        for key, value in pairs:
            if key in output:
                raise ValueError(f"Candidate YAML contains duplicate key: {key!r}")
            output[key] = value
        return output


def _reject_json_constant(value: str):
    raise ValueError(f"JSON constant is not finite: {value}")


def _json_object(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"Candidate JSON contains duplicate key: {key!r}")
        output[key] = value
    return output


def _strict_json_value(value: Any, path: str, depth: int, nodes: list[int], active: set[int]) -> Any:
    nodes[0] += 1
    if depth > _MAX_RESOURCE_DEPTH or nodes[0] > _MAX_RESOURCE_NODES:
        raise ValueError("Candidate resource complexity limit exceeded")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Candidate resource {path} contains a non-finite number")
        return value
    if isinstance(value, (list, dict)):
        identity = id(value)
        if identity in active:
            raise ValueError(f"Candidate resource {path} contains a reference cycle")
        active.add(identity)
        try:
            if isinstance(value, list):
                return [_strict_json_value(item, f"{path}[{index}]", depth + 1, nodes, active) for index, item in enumerate(value)]
            output = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError(f"Candidate resource {path} mapping keys must be strings")
                output[key] = _strict_json_value(item, f"{path}.{key}", depth + 1, nodes, active)
            return output
        finally:
            active.remove(identity)
    raise ValueError(
        f"Candidate resource {path} contains unsupported value type {type(value).__name__}"
    )


__all__ = [
    "CANDIDATE_JSON_FORMAT", "CANDIDATE_TEXT_FORMAT", "CANDIDATE_YAML_FORMAT",
    "ResourceAPI",
]
