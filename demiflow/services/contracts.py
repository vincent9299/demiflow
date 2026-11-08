"""Small deployment contracts shared by all native operators.

Declarations are inert. A lease owns resources acquired during execution only.
Model libraries are never imported by this module.
"""
from typing import Protocol, runtime_checkable
from contextlib import asynccontextmanager


class ModelServiceError(RuntimeError):
    """A deployment failure, which must stop its operator rather than each row."""


class ModelServiceConfigurationError(ModelServiceError, ValueError):
    """Retain ValueError compatibility while stopping the failed deployment."""


def served_model_names(value):
    """Read OpenAI and native SystemOne discovery without model-family branches."""
    if not isinstance(value,dict): raise ValueError('Model discovery must be an object')
    rows,key=(value['data'],'id') if 'data' in value else (value.get('models'),'name')
    if (not isinstance(rows,list) or len(rows)>1024
            or any(not isinstance(row,dict) or not isinstance(row.get(key),str) for row in rows)):
        raise ValueError('Invalid model discovery')
    return [row[key] for row in rows]


async def bounded_metadata_get(client, url):
    """Discovery/health documents are small; enforce the bound while reading."""
    import httpx
    async with client.stream('GET',url) as response:
        response.raise_for_status(); raw=bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(raw)+len(chunk)>65536: raise ValueError('Service metadata exceeds 64 KiB')
            raw.extend(chunk)
        return httpx.Response(response.status_code,content=bytes(raw),request=response.request)


@runtime_checkable
class ServiceLease(Protocol):
    async def ensure_ready(self) -> None: ...
    async def aclose(self) -> None: ...


@runtime_checkable
class Service(Protocol):
    def bind(self, model=None) -> ServiceLease: ...


def validate_service(value):
    if value is not None and not callable(getattr(value, 'bind', None)):
        raise TypeError('service must provide bind(model) returning a service lease')
    return value


@asynccontextmanager
async def service_request_slot(owner):
    """Optionally return a per-call URL; preserve legacy context-manager values."""
    slot = getattr(owner, 'request_slot', None)
    if slot is None:
        yield
    else:
        async with slot() as endpoint:
            yield endpoint if isinstance(endpoint,str) and endpoint else None
