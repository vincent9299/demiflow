"""Workbench HTTP route table and query-parameter decoding over Forge run operations."""
from __future__ import annotations

from dataclasses import dataclass, field
import re
from types import MappingProxyType

from demiflow.forge.bridge.run_tools import OPERATIONS, OperationSpec
from demiflow.forge.schema import ForgeError


@dataclass(frozen=True)
class HttpRoute:
    spec: OperationSpec
    method: str
    endpoint: str
    wire_names: MappingProxyType = field(default_factory=lambda: MappingProxyType({}))
    http_required: frozenset[str] = frozenset()

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def permission(self) -> str:
        return self.spec.permission

    def wire_fields(self) -> tuple[set[str], set[str]]:
        required, optional = set(), set()
        for field_name, field in self.spec.fields:
            key = self.wire_names.get(field_name, field_name)
            (required if field.required or field_name in self.http_required else optional).add(key)
        return required, optional


_ROUTES = (
    HttpRoute(OPERATIONS['prepare'], 'POST', 'run-prepare'),
    HttpRoute(OPERATIONS['start'], 'POST', 'run-start', http_required=frozenset({'expected_attempt'})),
    HttpRoute(OPERATIONS['show'], 'GET', 'run', wire_names=MappingProxyType({'include_report': 'report'})),
    HttpRoute(OPERATIONS['list'], 'GET', 'runs'),
    HttpRoute(OPERATIONS['logs'], 'GET', 'run-logs'),
    HttpRoute(OPERATIONS['cancel'], 'POST', 'run-cancel', http_required=frozenset({'expected_attempt'})),
    HttpRoute(OPERATIONS['resume'], 'POST', 'run-resume'),
)

HTTP_GET = MappingProxyType({r.endpoint: r for r in _ROUTES if r.method == 'GET'})
HTTP_POST = MappingProxyType({r.endpoint: r for r in _ROUTES if r.method == 'POST'})


def http_arguments(route: HttpRoute, values) -> dict:
    """Decode Workbench HTTP query/JSON wire shape, then validate against Forge's OperationSpec."""
    required, optional = route.wire_fields()
    if type(values) is not dict or not required <= values.keys() or values.keys() - required - optional:
        raise ForgeError('Missing or unknown request fields', code='invalid_request')
    result = {}
    for field_name, field in route.spec.fields:
        wire_name = route.wire_names.get(field_name, field_name)
        if wire_name not in values:
            continue
        value = values[wire_name]
        if route.method == 'GET':
            if field.kind == 'int':
                if type(value) is not str or not re.fullmatch(r'[0-9]{1,20}', value):
                    raise ForgeError('Invalid integer query field: ' + wire_name, code='invalid_request')
                value = int(value)
            elif field.kind == 'bool':
                if value not in {'0', '1'}:
                    raise ForgeError(wire_name + ' must be 0 or 1', code='invalid_request')
                value = value == '1'
        elif (value is None and field.kind != 'int') or (field.kind == 'parameters' and type(value) is not str):
            raise ForgeError('Invalid HTTP run argument: ' + wire_name, code='invalid_request')
        result[field_name] = value
    return route.spec.validate(result)
