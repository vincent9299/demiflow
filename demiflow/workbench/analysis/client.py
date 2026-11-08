"""Python Notebook helpers for the restricted broker, never direct host IO."""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

from contextlib import closing
from dataclasses import dataclass
import hashlib
import struct
import tempfile
import uuid

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import CHUNK_BYTES
from demiflow.workbench.analysis.arrow_bridge import MAX_ENCODED_BYTES
from demiflow.workbench.analysis.arrow_bridge import decoded_batches
from demiflow.workbench.analysis.arrow_bridge import encode_batches
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.framing import connect_broker
from demiflow.forge.schema import ForgeError

_SPOOL_DIRECTORY = job_path("/scratch")


def _response(value, request_id):
    if type(value) is not dict:
        c.reject("Invalid analysis broker response", code="operation_failed")
    if "error" in value:
        c.fields(value, {"error", "code"}, label="analysis error")
        c.text(value["error"], "analysis error", 1024)
        code = value["code"]
        c.reject(value["error"] + "; original request_id=" + request_id,
                 code=code if type(code) is str and code in {"conflict", "resource_limit", "operation_rejected", "operation_failed", "not_found", "busy"} else "operation_failed")
    return value


class BrokerClient:
    def __init__(self):
        self._connect = connect_broker

    def _call(self, operation, payload, *, request_id=None):
        request_id = c.identity(request_id or "a" + uuid.uuid4().hex, "request ID")
        try:
            with closing(self._connect()) as connection:
                frames = Frames(connection)
                frames.send("json", dict(version=1, operation=operation, request_id=request_id, payload=payload))
                return _response(frames.json(), request_id)
        except OSError:
            c.reject("Analysis reply is unknown; query original request_id=" + request_id + "; do not automatically execute again", code="operation_failed")
        except ForgeError as exc:
            if exc.code == "operation_failed" and "request_id=" not in str(exc):
                c.reject(str(exc) + "; original request_id=" + request_id, code=exc.code)
            raise

    def sql(self, source, *, sources=None, request_id=None):
        c.text(source, "SQL cell", 16384)
        sources = {} if sources is None else sources
        if type(sources) is not dict or len(sources) > 16:
            c.reject("SQL accepts at most 16 named table handles")
        bindings = {}
        for alias, handle in sources.items():
            c.sql_alias(alias)
            if not isinstance(handle, TableHandle) or handle._client is not self:
                c.reject("SQL source must be a handle returned by this broker client")
            bindings[alias] = handle.reference()
        value = self._call("query", dict(sql=source, sources=bindings), request_id=request_id)
        return TableHandle(self, value)

    def table(self, alias):
        c.text(alias, "registered table alias", 128)
        return TableHandle(self, self._call("table", dict(alias=alias)))

    def source(self, alias):
        c.text(alias, "registered source alias", 128)
        return SourceHandle(self, alias, self._call("source-info", dict(alias=alias)))

    def receipt(self, request_id):
        c.identity(request_id, "original request ID")
        return self._call("receipt", dict(request_id=request_id))

    def publish(self, table, *, request_id=None):
        import pyarrow as pa
        if not isinstance(table, (pa.Table, pa.RecordBatchReader)):
            c.reject("publish accepts an Arrow Table or RecordBatchReader, never pickle or callbacks")
        reader = table.to_reader(max_chunksize=1024) if isinstance(table, pa.Table) else table
        request_id = c.identity(request_id or "a" + uuid.uuid4().hex, "request ID")
        schema_sha = schema_digest(reader.schema)
        try:
            # Bind idempotency to the complete content before admission. A
            # schema hash alone cannot distinguish two Tables with different
            # rows. A bounded scratch spool consumes a Reader only once.
            with tempfile.TemporaryFile(dir=_SPOOL_DIRECTORY) as spool:
                stream_hash = hashlib.sha256()
                encoded_bytes = chunks = 0
                for chunk in encode_batches(reader):
                    prefix = struct.pack("!I", len(chunk))
                    if encoded_bytes + len(prefix) + len(chunk) > MAX_ENCODED_BYTES:
                        c.reject("Arrow upload spool exceeds scratch reservation", code="resource_limit")
                    stream_hash.update(prefix); stream_hash.update(chunk)
                    spool.write(prefix); spool.write(chunk)
                    encoded_bytes += len(prefix) + len(chunk); chunks += 1
                spool.seek(0)
                payload = dict(schema_sha256=schema_sha, stream_sha256=stream_hash.hexdigest(), encoded_bytes=encoded_bytes, chunks=chunks)
                with closing(self._connect()) as connection:
                    frames = Frames(connection)
                    frames.send("json", dict(version=1, operation="publish", request_id=request_id, payload=payload))
                    admission = _response(frames.json(), request_id)
                    if admission != {"accepted": True}:
                        return TableHandle(self, admission)
                    for _ in range(chunks):
                        prefix = spool.read(4)
                        if len(prefix) != 4:
                            c.reject("Arrow spool is incomplete", code="operation_failed")
                        size = struct.unpack("!I", prefix)[0]
                        if not 1 <= size <= CHUNK_BYTES:
                            c.reject("Arrow spool frame is invalid", code="operation_failed")
                        raw = spool.read(size)
                        if len(raw) != size:
                            c.reject("Arrow spool frame is incomplete", code="operation_failed")
                        frames.send("arrow", raw)
                    frames.send("end")
                    return TableHandle(self, _response(frames.json(), request_id))
        except OSError:
            c.reject("Arrow publish result is unknown; query original request_id=" + request_id, code="operation_failed")
        except ForgeError as exc:
            if exc.code == "operation_failed" and "request_id=" not in str(exc):
                c.reject(str(exc) + "; original request_id=" + request_id, code=exc.code)
            raise


@dataclass(frozen=True)
class SourceHandle:
    """Fixed rich source; sample is JSON, table handles remain typed Arrow."""
    _client: BrokerClient
    alias: str
    snapshot: dict

    def __post_init__(self):
        c.text(self.alias, "source alias", 128)
        checked=c.source_snapshot({key:value for key,value in self.snapshot.items() if key!="digest"})
        if checked!=self.snapshot:c.reject("Source descriptor digest differs",code="conflict")

    def sample(self, columns=None, *, offset=0, limit=20):
        from demiflow.workbench.analysis.source_query import request
        columns=[field["name"] for field in self.snapshot["schema"]][:16] if columns is None else columns
        query=request(dict(kind="sample",columns=columns,offset=offset,limit=limit),self.snapshot)
        return self._client._call("source-sample",dict(alias=self.alias,digest=self.snapshot["digest"],query=query))

    def _repr_mimebundle_(self, include=None, exclude=None):
        return {"text/plain":f"DemiFlow fixed source: {self.alias}, {self.snapshot['rows']} rows; sample() reads a bounded JSON page"}


@dataclass(frozen=True)
class TableHandle:
    _client: BrokerClient
    _value: dict

    def __post_init__(self):
        c.fields(self._value, {"id", "generation", "digest", "rows", "schema", "retention"}, label="table handle")
        c.identity(self._value["id"], "table object ID")
        c.integer(self._value["generation"], "object generation", 2**31-1, 1)
        c.sha(self._value["digest"], "table digest")
        c.integer(self._value["rows"], "table row count", 2**63-1)
        c.sequence(self._value["schema"], "table schema", 256)
        c.one_of(self._value["retention"], {"ephemeral", "pinned", "external"}, "table retention")
        c.encoded(self._value)

    def reference(self):
        return {key: self._value[key] for key in ("id", "generation", "digest")}

    def bind(self, alias, *, expected=None):
        """Expose this table to SQL cells in the current kernel generation.

        Original Notebook sources keep their names. Replacing a generated
        name requires its previous handle; restart clears generated names.
        """
        c.sql_alias(alias)
        payload=dict(alias=alias,handle=self.reference())
        if expected is not None:
            if not isinstance(expected,TableHandle) or expected._client is not self._client:
                c.reject('Expected alias value must be this client\'s previous table handle')
            payload['expected']=expected.reference()
        return TableHandle(self._client,self._client._call('bind-table',payload))

    def iter_batches(self):
        request_id = "a" + uuid.uuid4().hex
        with closing(self._client._connect()) as connection:
            frames = Frames(connection)
            frames.send("json", dict(version=1, operation="read", request_id=request_id, payload={"handle": self.reference()}))
            value = _response(frames.json(), request_id)
            if value != {"accepted": True}:
                c.reject("Arrow read was not admitted", code="operation_failed")
            yield from decoded_batches(frames.arrow())

    def to_arrow(self):
        import pyarrow as pa
        batches = list(self.iter_batches())
        if not batches:
            c.reject("Arrow read omitted its schema", code="operation_failed")
        return pa.Table.from_batches(batches)

    def to_pandas(self):
        import pandas as pd
        # Arrow-backed columns preserve null/types and avoid per-value Python
        # object expansion. Manual native pandas conversion remains user code.
        return self.to_arrow().to_pandas(types_mapper=pd.ArrowDtype, use_threads=False)

    def pin(self, *, request_id=None):
        return TableHandle(self._client, self._client._call("pin", {"handle": self.reference()}, request_id=request_id))

    def _repr_mimebundle_(self, include=None, exclude=None):
        # Register this exact live-cell use even for a variable returned by a
        # prior cell. A producer-supplied MIME handle alone is not provenance.
        actual=TableHandle(self._client,self._client._call("handle-info",{"handle":self.reference()}))
        return {"text/plain": f"DemiFlow table: {actual._value['rows']} rows ({actual._value['retention']}); call to_arrow()/to_pandas() to read",
                "application/vnd.demiflow.table+json": actual._value}


@dataclass(frozen=True)
class CaseView:
    descriptor: dict

    def _repr_mimebundle_(self, include=None, exclude=None):
        return {"text/plain": "DemiFlow Case view: fields, grouping/filters, evaluation questions",
                "application/vnd.demiflow.case-view+json": self.descriptor}


def case_view(handle, *, fields, grouping=None, filters=None, sort=None, layout="card", questions=None, variant_key=None, output_groups=None, name=None):
    if not isinstance(handle, (TableHandle, SourceHandle)):
        c.reject("Case view requires a broker table handle")
    source = (handle._client._call("source-info",dict(alias=handle.alias)) if isinstance(handle, SourceHandle)
        else handle._client._call("case-source", {"handle": handle.reference()}))
    if isinstance(handle, SourceHandle) and source!=handle.snapshot:
        c.reject("Source alias changed after its handle was created",code="conflict")
    if type(source) is not dict or "digest" not in source:
        c.reject("Invalid fixed Case source", code="operation_failed")
    checked = c.source_snapshot({key: value for key, value in source.items() if key != "digest"})
    if checked["digest"] != source["digest"]:
        c.reject("Case source digest changed", code="conflict")
    descriptor = dict(version=1, source_snapshot=source["digest"], case_key=source["case_key"], fields=fields,
                      grouping=[] if grouping is None else grouping, filters=filters, sort=[] if sort is None else sort,
                      layout=layout, questions=[] if questions is None else questions)
    if variant_key is not None:
        descriptor["variant_key"] = variant_key
    if output_groups is not None:
        descriptor["version"] = 2
        descriptor["output_groups"] = output_groups
    if name is not None:descriptor['name']=name
    return CaseView(c.case_view(descriptor, source))


_default = BrokerClient()
sql = _default.sql
table = _default.table
source = _default.source
publish = _default.publish
receipt = _default.receipt
