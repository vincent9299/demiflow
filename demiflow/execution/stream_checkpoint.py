"""Atomic commit vector for a single-writer local streaming graph.

Every participating sink publishes before forwarding rows. One bounded write
intent covers the gap between a Lance commit and publication of the vector.
Recovery verifies that intent; it never picks unrelated latest table heads.
Business code supplies identity and pending-row relations, not storage logic.
"""
import hashlib
import json
import os
from pathlib import Path
import threading
import uuid
import stat

import lance

from .lance_predicate import scalar_equal


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def rows_digest(rows):
    """Hash the canonical JSON row list without retaining recovered payloads."""
    digest = hashlib.sha256(b'[')
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    for index, row in enumerate(rows):
        if index:
            digest.update(b',')
        for chunk in encoder.iterencode(row):
            digest.update(chunk.encode())
    digest.update(b']')
    return digest.hexdigest()


class StreamCheckpoint:
    """Retired facade; construction cannot create or recover legacy sidecars."""
    def __init__(self, path, *, identity, initial=None, max_bytes=1024**2,
                 accepted_identities=(), allow_identity_migration=False, declared_outputs=None):
        from demiflow.execution.compiler_support.capabilities import require_action
        require_action('stream_checkpoint')
        supplied = Path(path).absolute()
        for component in (supplied, *supplied.parents):
            if component.is_symlink():
                raise ValueError('Checkpoint path cannot contain a symlink')
        self.path = supplied.resolve()
        self.declared_outputs = declared_outputs
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 16*1024**2:
            raise ValueError('Invalid checkpoint max_bytes')
        self.max_bytes = max_bytes
        self.identity = hashlib.sha256(canonical(identity).encode()).hexdigest()
        self.lock = threading.RLock()
        self.failed = False
        if self.path.exists():
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
                    raise ValueError('Checkpoint exceeds max_bytes or is not a regular file')
                raw = source.read(max_bytes + 1)
                if len(raw) > max_bytes:
                    raise ValueError('Checkpoint exceeds max_bytes')
                self.state = json.loads(raw)
            self.validate_ownership()
            accepted = set()
            for value in accepted_identities or ():
                if isinstance(value, str):
                    accepted.add(value)
                else:
                    accepted.add(hashlib.sha256(canonical(value).encode()).hexdigest())
            identity_known = self.state.get('identity') in ({self.identity} | accepted)
            if self.state.get('format') != (2 if declared_outputs is not None else 1) or (not identity_known and not allow_identity_migration):
                raise ValueError('Checkpoint pipeline identity changed')
            if self.state.get('identity') != self.identity:
                self.state['identity'] = self.identity
                self.publish()
            self.recover()
        else:
            self.state = {'format': 2 if declared_outputs is not None else 1, 'identity': self.identity, 'revision': 0,
                          'outputs': initial or {}, 'pending': None}
            self.validate_ownership()
            self.publish()

    def validate_ownership(self):
        """Validate untrusted checkpoint locations before any Lance access."""
        if type(self.state) is not dict or set(self.state) != {'format', 'identity', 'revision', 'outputs', 'pending'}:
            raise ValueError('Invalid checkpoint state')
        if (type(self.state['revision']) is not int or not 0 <= self.state['revision'] < 2**63
                or type(self.state['identity']) is not str or len(self.state['identity']) != 64):
            raise ValueError('Invalid checkpoint identity or revision')
        outputs = self.state['outputs']
        if type(outputs) is not dict or len(outputs) > 32:
            raise ValueError('Checkpoint outputs exceed declared stage budget')
        for stage, ref in outputs.items():
            if (type(stage) is not str or type(ref) is not dict or set(ref) != {'uri', 'version'}
                    or type(ref['uri']) is not str or type(ref['version']) is not int or ref['version'] < 1):
                raise ValueError('Invalid checkpoint output reference')
            if self.declared_outputs is not None and (stage not in self.declared_outputs or
                    ref['uri'] != self.declared_outputs[stage]['uri']):
                raise ValueError('Checkpoint output violates declared ownership')
        pending = self.state['pending']
        if pending is not None:
            if type(pending) is dict and pending.get('kind') == 'create':
                if (self.declared_outputs is None or set(pending) != {'kind', 'stage', 'uri', 'operation_id'}
                        or pending['stage'] not in self.declared_outputs or pending['stage'] in outputs
                        or pending['uri'] != self.declared_outputs[pending['stage']]['uri']
                        or type(pending['operation_id']) is not str or len(pending['operation_id']) != 32
                        or any(c not in '0123456789abcdef' for c in pending['operation_id'])):
                    raise ValueError('Invalid declared checkpoint initialization intent')
                return
            if type(pending) is not dict or set(pending) != {'stage', 'uri', 'base', 'fields', 'keys', 'max_row_bytes', 'digest'}:
                raise ValueError('Invalid checkpoint intent')
            if outputs.get(pending['stage']) != {'uri': pending['uri'], 'version': pending['base']}:
                raise ValueError('Checkpoint intent violates declared output ownership')
            if (type(pending['fields']) is not list or not 1 <= len(pending['fields']) <= 32
                    or any(type(f) is not str or not f for f in pending['fields'])
                    or type(pending['keys']) is not list or len(pending['keys']) > 4096
                    or any(type(k) is not list or len(k) != len(pending['fields']) for k in pending['keys'])
                    or type(pending['max_row_bytes']) is not int or not 1 <= pending['max_row_bytes'] <= 64 * 1024**2):
                raise ValueError('Invalid bounded checkpoint intent')

    def validate_sinks(self, sinks):
        if self.declared_outputs is None:
            return
        from ..lance.schema import schema_identity
        if {sink.stage for sink in sinks} != set(self.declared_outputs):
            raise ValueError('Stream sinks differ from declared checkpoint stages')
        for sink in sinks:
            declared = self.declared_outputs[sink.stage]
            if (sink.uri != declared['uri'] or sink.mode != 'append' or
                    schema_identity(sink.schema) != schema_identity(declared['schema'])):
                raise ValueError('Stream sink violates declared checkpoint ownership/schema')
            if sink.stage not in self.state['outputs'] and Path(sink.uri).exists():
                raise ValueError('Unowned existing table cannot become a checkpoint output')

    def publish(self):
        self.validate_ownership()
        payload = canonical(self.state).encode()
        if len(payload) > self.max_bytes:
            raise ValueError('Checkpoint exceeds max_bytes')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(self.path.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            with temp.open('xb') as out:
                out.write(payload); out.flush(); os.fsync(out.fileno())
            os.replace(temp, self.path)
            descriptor = os.open(self.path.parent, os.O_RDONLY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
        finally:
            temp.unlink(missing_ok=True)

    def recover(self):
        pending = self.state.get('pending')
        if pending is not None and pending.get('kind') == 'create':
            if Path(pending['uri']).exists():
                from ..lance.schema import schema_identity
                current = lance.dataset(pending['uri'])
                marker = (current.read_transaction(current.version).transaction_properties or {}).get('__lance_commit_message')
                if (current.version != 1 or current.count_rows() != 0 or
                        marker != 'demiflow-lance:' + pending['operation_id'] or
                        schema_identity(current.schema) != schema_identity(self.declared_outputs[pending['stage']]['schema'])):
                    raise ValueError('Checkpoint initialization has unrecognized commit evidence')
                self.state['outputs'][pending['stage']] = {'uri': pending['uri'], 'version': 1}
            self.state['pending'] = None
            self.state['revision'] += 1
            self.publish()
            pending = None
        if pending:
            current = lance.dataset(pending['uri'])
            if current.version == pending['base'] + 1:
                def recovered_rows():
                    for key in pending['keys']:
                        predicate = ' AND '.join(scalar_equal(k, v) for k, v in zip(pending['fields'], key))
                        found = []
                        for batch in current.scanner(filter=predicate, limit=2, batch_size=1,
                                batch_readahead=1, fragment_readahead=1).to_batches():
                            if batch.nbytes > pending['max_row_bytes']:
                                raise ValueError('Checkpoint receipt exceeds row budget')
                            found.extend(batch.to_pylist())
                        if len(found) != 1:
                            raise ValueError('Checkpoint write intent does not match committed rows')
                        yield found[0]
                if rows_digest(recovered_rows()) != pending['digest']:
                    raise ValueError('Checkpoint write intent content mismatch')
                self.state['outputs'][pending['stage']] = {'uri': pending['uri'], 'version': current.version}
            elif current.version != pending['base']:
                raise ValueError('Checkpoint has an unrecognized table head')
            self.state['pending'] = None
            self.state['revision'] += 1
            self.publish()
        for stage, ref in self.state['outputs'].items():
            if lance.dataset(ref['uri']).version != ref['version']:
                raise ValueError('Table head changed outside the checkpoint writer')
            if self.declared_outputs is not None:
                from ..lance.schema import schema_identity
                if schema_identity(lance.dataset(**ref).schema) != schema_identity(self.declared_outputs[stage]['schema']):
                    raise ValueError('Committed checkpoint schema differs from declaration')

    def register(self, stage, ref):
        with self.lock:
            if self.declared_outputs is not None and (stage not in self.declared_outputs or ref['uri'] != self.declared_outputs[stage]['uri']):
                raise ValueError('Sink violates declared checkpoint ownership')
            old = self.state['outputs'].get(stage)
            if old is not None and old != ref:
                raise ValueError('Sink does not match the global checkpoint')
            if old is None and (ref['version'] != 1 or lance.dataset(**ref).count_rows()):
                raise ValueError('Existing stage requires an explicit initial checkpoint reference')
            self.state['outputs'][stage] = ref
            self.state['revision'] += 1
            self.publish()

    def begin_initialize(self, stage):
        if self.failed or self.state['pending'] is not None or self.declared_outputs is None or stage in self.state['outputs']:
            raise ValueError('Checkpoint initialization requires an uncommitted owned stage')
        uri = self.declared_outputs[stage]['uri']
        if Path(uri).exists():
            raise ValueError('Checkpoint initialization target already exists')
        operation = uuid.uuid4().hex
        self.state['pending'] = {'kind': 'create', 'stage': stage, 'uri': uri, 'operation_id': operation}
        self.publish()
        return operation

    def begin(self, stage, uri, base, fields, rows, max_row_bytes):
        if self.failed:
            raise ValueError('Checkpoint writer failed; recovery is required')
        if self.state['pending'] is not None:
            raise ValueError('Unresolved checkpoint write intent')
        if self.state['outputs'].get(stage) != {'uri': uri, 'version': base}:
            raise ValueError('Sink write is not based on the global checkpoint')
        self.state['pending'] = {'stage': stage, 'uri': uri, 'base': base, 'fields': list(fields),
            'keys': [[r[k] for k in fields] for r in rows], 'max_row_bytes': max_row_bytes,
            'digest': rows_digest(rows)}
        self.publish()

    def committed(self, stage, ref):
        self.state['outputs'][stage] = ref
        self.state['pending'] = None
        self.state['revision'] += 1
        self.publish()


def normalize_checkpoint(arguments):
    from ..lance.schema import build_schema, schema_identity
    from ..data.flow_contract import arrow_fields
    import pyarrow as pa
    value = dict(arguments)
    path, outputs = value['path'], value['outputs']
    if type(path) is not str or not path or len(path.encode()) > 4096:
        raise ValueError('Checkpoint path must be bounded explicit text')
    if type(outputs) is not dict or not 1 <= len(outputs) <= 32:
        raise ValueError('Checkpoint requires 1..32 declared outputs')
    declared = {}
    for stage, output in outputs.items():
        if type(stage) is not str or not stage.isidentifier() or len(stage) > 64 or type(output) is not dict or set(output) != {'uri', 'schema'}:
            raise ValueError('Invalid checkpoint stage declaration')
        uri = output['uri']
        if type(uri) is not str or not Path(uri).is_absolute() or str(Path(uri).resolve()) != uri:
            raise ValueError('Checkpoint target requires an absolute physical path without symlinks')
        schema = output['schema']
        if type(schema) is dict and set(schema) == {'schema_declaration'}:
            schema = build_schema(schema['schema_declaration'])
        if not isinstance(schema, pa.Schema):
            raise ValueError('Checkpoint output requires an explicit native schema')
        declared[stage] = {'uri': uri, 'schema_identity': schema_identity(schema), 'fields': arrow_fields(schema)}
    if len({output['uri'] for output in declared.values()}) != len(declared):
        raise ValueError('Checkpoint targets must be unique')
    if type(value['max_bytes']) is not int or not 1024 <= value['max_bytes'] <= 16 * 1024**2:
        raise ValueError('Invalid checkpoint max_bytes')
    value['outputs'] = declared
    if len(canonical(value).encode()) > 256 * 1024:
        raise ValueError('Checkpoint declaration exceeds 256 KiB')
    return value


def analyze_checkpoint(arguments, *, inputs, callables, resources):
    from ..data.flow_contract import scalar
    return {'fields': {}, 'driver': 'pull', 'value_type': {**scalar('checkpoint'), 'outputs': arguments['outputs']},
            'cardinality': 'none', 'order': 'preserved'}


def normalize_checkpoint_read(arguments):
    values = dict(arguments)
    values['columns'] = list(values['columns'] or ())
    return values


def analyze_checkpoint_read(arguments, *, inputs, callables, resources):
    from ..data.flow_contract import require_fields
    outputs = resources['checkpoint']['outputs']
    if arguments['stage'] not in outputs:
        raise ValueError('Read stage is absent from declared checkpoint')
    fields = outputs[arguments['stage']]['fields']
    if arguments['columns']:
        require_fields(fields, arguments['columns'])
        fields = {name: fields[name] for name in arguments['columns']}
    return {'fields': fields, 'driver': 'pull', 'cardinality': 'unknown', 'order': 'unspecified'}


from ..data.flow_contract import flow_api


@flow_api('open_checkpoint', role='query', execution='action', effects=('checkpoint',),
          normalize=normalize_checkpoint, analyze=analyze_checkpoint)
def open_checkpoint(path, *, identity, outputs, max_bytes=1024**2):
    """Open one run-owned checkpoint with exact stage targets and Arrow schemas.

    In checked execution ``path`` is relative to its persistent run directory.
    No table is read until loaded output references pass the declaration check.
    """
    from demiflow.execution.pipeline.checked import action, current
    arguments = normalize_checkpoint(dict(path=path, identity=identity, outputs=outputs, max_bytes=max_bytes))
    owner = current()
    actual_path = Path(path)
    if owner is not None:
        if owner.resource_root is None or actual_path.is_absolute() or '..' in actual_path.parts or actual_path.parts[:1] != ('checkpoints',):
            raise ValueError('Managed checkpoint requires checkpoints/ path and a persistent resource root')
        actual_path = owner.resource_root / actual_path
    with action('open_checkpoint', None, None, arguments=arguments) as checked:
        checkpoint = StreamCheckpoint(actual_path, identity={'identity': identity, 'outputs': arguments['outputs']},
                                      max_bytes=max_bytes, declared_outputs=outputs)
        checkpoint.contract_arguments = arguments
        checked.checkpoint(checkpoint)
        return checkpoint


@flow_api('read_checkpoint', role='bridge', execution='bridge', effects=('read_table',),
          normalize=normalize_checkpoint_read, analyze=analyze_checkpoint_read)
def read_checkpoint(checkpoint, stage, *, columns=None):
    """Read the current committed reference once, or a typed empty Dataset.

    The returned Dataset remains bound to that version if the checkpoint later
    advances. This never reads a table head or accepts a user-supplied receipt.
    """
    from .checkpoint_read import _read_checkpoint_binding
    from ..data.api import _select_executor
    from ..data.dataset import Dataset, MaterializedDataset
    from ..data.execution_contract import _current_executor
    from ..data.sources import MaterializedSource

    binding, executor = _read_checkpoint_binding(
        checkpoint, stage, columns=columns,
        select_executor=lambda: _select_executor(_current_executor.get()),
    )
    dataset_type = MaterializedDataset if isinstance(binding.source, MaterializedSource) else Dataset
    return dataset_type(binding.source, binding.plan, executor)
