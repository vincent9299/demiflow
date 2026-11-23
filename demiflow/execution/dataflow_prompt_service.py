"""Passive Prompt services reconstructed inside the original worker unit.

The HTTP request/cache identity is unchanged. Service construction belongs to
the sealed execution program, with the same guardian and device authority used
by Embedding; a live owner or an undeclared mutation never becomes a factory.
"""
from dataclasses import replace

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_managed_http import freeze_managed_http, managed_http_digest


def prompt_service_declaration(actor):
    service = actor._runtime._service
    expected = getattr(actor, '_sealed_service_declaration', None)
    if service is None:
        if expected is not None:
            raise DataflowError('INVALID_CONTRACT', 'Managed Prompt service was removed after declaration')
        return None
    value = freeze_managed_http(service)
    if expected is None or managed_http_digest(service) != expected:
        raise DataflowError('INVALID_CONTRACT', 'Managed Prompt service differs from its sealed declaration')
    return value





def bind_prompt_service(actor, spec, *, services, node_id):
    digest = spec.configuration.get('managed_http_service_digest')
    service = actor._runtime._service
    if (digest is None) != (service is None):
        raise DataflowError('INVALID_CONTRACT', 'Prompt worker service differs from the sealed plan')
    if digest is None:
        return
    if getattr(actor, '_sealed_service_declaration', None) != digest:
        raise DataflowError('INVALID_CONTRACT', 'Prompt worker service declaration does not match its program')
    from demiflow.execution.dataflow_managed_devices import bind_worker_service
    bind_worker_service(actor, spec, services=services, node_id=node_id, service=service)
