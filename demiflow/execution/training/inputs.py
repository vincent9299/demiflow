"""Fix a training input to an ordered, versioned Lance dataset before claiming a job."""
import hashlib
import json
from pathlib import Path

from ..contracts.dataflow import thaw_record_value
from ..contracts.dataflow_errors import DataflowError
from ..contracts.training import FixedDatasetRef
from demiflow.execution.outputs.sinks import publication_blocks
from ..recovery_contract import identity
from ...lance.storage import open_lance_dataset


def _source_proof(graph, physical, occurrence):
    sources = {s.source_id: s for s in physical.sources}
    parents = {}
    for edge in graph.edges:
        parents.setdefault(edge.consumer, []).append((edge.input_port, edge.producer))
    memo = {}
    def proof(key):
        if key not in memo:
            if key in sources:
                source = sources[key]
                if not source.finite or source.replay == 'attempt_only':
                    raise ValueError("Training requires an immutable finite source")
                memo[key] = identity(thaw_record_value(source.immutable_identity))
            else:
                memo[key] = identity([[port, proof(parent)] for port, parent in sorted(parents[key])])
        return memo[key]
    return proof(occurrence)


def _verify_media(value, *, file_limit, roots, check_stop):
    if isinstance(value, list):
        for child in value:
            _verify_media(child, file_limit=file_limit, roots=roots, check_stop=check_stop)
    elif isinstance(value, dict):
        if 'uri' in value and 'sha256' in value:
            from urllib.parse import urlsplit, unquote
            locator = urlsplit(value['uri'])
            if locator.scheme not in ('', 'file') or locator.netloc not in ('', 'localhost'):
                raise ValueError("Training media needs a locally verifiable content reference")
            path = Path(unquote(locator.path) if locator.scheme else value['uri'])
            if not path.is_absolute():
                raise ValueError("Training media requires an absolute file reference")
            if path.resolve(strict=True) != path or not any(path.is_relative_to(root) for root in roots):
                raise ValueError("Training media is outside its authorized content directory")
            digest = hashlib.sha256()
            size = 0
            from .store import open_regular_file
            with open_regular_file(path) as stream:
                while chunk := stream.read(min(file_limit, 1024 * 1024)):
                    check_stop()
                    size += len(chunk)
                    if size > file_limit:
                        raise ValueError("Training media exceeds the admitted per-file budget")
                    digest.update(chunk)
            if digest.hexdigest() != value['sha256']:
                raise ValueError("Training media content differs from its fixed digest")
        else:
            raise ValueError("Training media requires URI and SHA-256")
    elif value is not None:
        raise ValueError("Training media requires a content reference")


def prove_lance(uri, version, columns, *, source_digest, bounds, check_stop, media, media_roots, storage_options=()):
    import pyarrow as pa
    table = open_lance_dataset(uri, version, storage_options)
    if version != table.version:
        raise ValueError("Lance training input changed version")
    from ...data.training_schema import validate_columns
    validate_columns(table.schema, columns)
    names = table.schema.names
    roots = tuple(Path(root) for root in media_roots)
    if media and (not roots or any(not r.is_absolute() or not r.is_dir() or r.resolve() != r for r in roots)):
        raise ValueError("Training media requires authorized absolute content directories")
    if not names or len(set(names)) != len(names):
        raise ValueError("Training columns must have unambiguous names")
    schema = table.schema
    digest = hashlib.sha256(schema.serialize().to_pybytes())
    order = hashlib.sha256()
    count = 0
    for batch in table.to_batches(columns=names, batch_size=1, batch_readahead=1,
                                  fragment_readahead=1, scan_in_order=True):
        check_stop()
        if batch.nbytes > bounds.max_row_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Training row exceeds its admitted size')
        for row in batch.to_pylist():
            for name in media:
                _verify_media(row[name], file_limit=bounds.max_state_bytes, roots=roots, check_stop=check_stop)
            # Rebuild the one-row buffers. IPC of a sliced array may otherwise
            # carry padding or an unrelated offset into the preceding fragment.
            canonical = pa.RecordBatch.from_pylist([row], schema=schema).serialize()
            token = hashlib.sha256(canonical).digest()
            digest.update(token)
            order.update(count.to_bytes(8, 'big') + token)
            count += 1
    schema_digest = identity([hashlib.sha256(schema.serialize()).hexdigest(), dict(columns), sorted(media)])
    return FixedDatasetRef(uri, version, digest.hexdigest(), schema_digest, order.hexdigest(),
                           count, source_digest)


def fix_input(occurrence, columns, *, graph, physical, action, image, services, check_stop, media, media_roots):
    source = next((s for s in physical.sources if s.source_id == occurrence), None)
    proof = _source_proof(graph, physical, occurrence)
    if source is not None:
        fixed = source.immutable_identity
        query = fixed['query']
        if (fixed['kind'] != 'LanceSource' or query['kind'] != 'scan' or query['columns']
                or query['filter'] or query['limit'] is not None or query.get('projection')):
            raise ValueError("Only a complete versioned Lance scan can supply a direct training input")
        return prove_lance(query['uri'], fixed['version'], columns, source_digest=proof,
                           bounds=services.bounds, check_stop=check_stop, media=media, media_roots=media_roots,
                           storage_options=tuple(query['storage_options'].items()))

    if image is None or occurrence not in image.confirmed_sinks:
        raise ValueError("Training input requires a confirmed row publication")
    ref = image.confirmed_sinks[occurrence]
    raw = b''.join(services.store.read(ref, max_bytes=services.bounds.max_control_bytes))
    publication = json.loads(raw)
    if publication['sink_id'] != occurrence:
        raise ValueError("Training input publication belongs to a different occurrence")
    root = action.context.directory / 'materializations' / 'fixed-datasets'
    root.mkdir(parents=True, exist_ok=True)
    destination = root / occurrence
    from ..commit import CommitCoordinator
    from ..recovery_contract import SourcePosition
    from ..run_context import RunAction
    from demiflow.execution.outputs.lance_sink import LanceSink
    from ...lance.model import LanceWriteSpec
    scope = identity(['training_input', action.action_id, occurrence])
    # The row receipt can be carried into later cuts. Its ordered partition
    # contents and source versions identify the original publication.
    position = SourcePosition(identity([proof, publication['manifest']['partitions']]), 1)
    declaration = {'kind': 'fixed_dataset', 'input': occurrence, 'target': str(destination),
                   'source': position.source_id}
    run = action.context
    run.journal.bind_commit_scope(run.fence, scope, action.action_id, declaration)
    child = RunAction(run, scope, declaration)
    sink = LanceSink(child, LanceWriteSpec(str(destination), mode='create'))
    coordinator = CommitCoordinator(child)
    fact = run.journal.latest_execution_commit(scope)
    if fact and fact['status'] != 'pending':
        coordinator.recover({'lance': sink})
    else:
        def blocks():
            for block in publication_blocks(publication['manifest'], store=services.store,
                        bounds=services.bounds, batch_rows=1, publication_workspace=True,
                        publication_metadata=True):
                check_stop()
                yield block
        coordinator.commit(1, outputs={'lance': (sink, blocks())}, position=position)
    fact = run.journal.latest_execution_commit(scope)
    if fact['status'] != 'acked' or fact['intent']['position'] != position.to_dict():
        raise ValueError("Fixed training input lacks its original commit receipt")
    receipt = fact['receipts']['lance']['data']
    if receipt['uri'] != str(destination):
        raise ValueError("Fixed training input receipt belongs to another table")
    return prove_lance(str(destination), receipt['committed_version'], columns, source_digest=proof,
                       bounds=services.bounds, check_stop=check_stop, media=media, media_roots=media_roots)
