"""Registration locators project the original native action, never its authority."""
from contextlib import nullcontext
import fcntl
import json
from pathlib import Path

from .control import control_directory
from .model import LanceWriteReceipt
from .storage import open_lance_dataset, schema_hash
from ..execution.recovery_contract import RecoveryContractError, RunRef, document
from demiflow.execution.pipeline.run_context import RunContext, current_run
from ..execution.run_journal import RunJournal
from demiflow.execution.storage.state_artifacts import atomic_bytes

_SCHEMA = 'demiflow.registered-native-write.v1'
_LIMIT = 16384


def _read_locator(path):
    if not path.exists():
        return None
    if path.is_symlink():
        raise RecoveryContractError('Registration locator contains a symlink')
    with path.open('rb') as stream:
        raw = stream.read(_LIMIT + 1)
    if len(raw) > _LIMIT:
        raise RecoveryContractError('Registration locator exceeds byte budget')
    value = json.loads(raw)
    if (type(value) is not dict or set(value) != {'schema', 'run_id', 'directory', 'relative', 'action_id'}
            or value['schema'] != _SCHEMA or type(value['relative']) is not bool
            or any(type(value[k]) is not str or not value[k] for k in ('run_id', 'directory', 'action_id'))
            or len(value['run_id']) > 64 or len(value['action_id']) > 64):
        raise RecoveryContractError('Invalid native registration locator')
    directory = Path(value['directory'])
    if value['relative']:
        if directory.is_absolute() or '..' in directory.parts:
            raise RecoveryContractError('Registration locator escapes its control directory')
        directory = path.parent / directory
    elif not directory.is_absolute():
        raise RecoveryContractError('External registration locator needs its original RunRef')
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise RecoveryContractError('Registration run contains a symlink')
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise RecoveryContractError('Original registration RunJournal is unavailable')
    identity = journal.recovery_identity()
    if identity is None or identity['run_id'] != value['run_id']:
        raise RecoveryContractError('Registration locator differs from original RunRef')
    return value, journal


def _confirmed(journal, action_id, *, request):
    binding = journal.source_binding(action_id + '-registered-write')
    if binding != request:
        raise ValueError('Registered fingerprint/schema request differs; use a new location')
    fact = journal.latest_execution_commit(action_id)
    if fact is None or fact['status'] != 'acked' or fact['prepared'] is None:
        return None
    if fact['intent']['sinks'] != ['write_lance']:
        raise RecoveryContractError('Registration locator does not name a native Lance action')
    prepared = fact['prepared']['sinks']['write_lance']
    published = fact['receipts'].get('write_lance')
    if published is None or published['commit_id'] != fact['commit_id']:
        raise RecoveryContractError('Original ACK has no matching native publication receipt')
    data = published['data']
    if type(prepared) is dict and {'objects', 'rows'} <= set(prepared):
        prepared, data = prepared['rows'], data['rows']
    if not prepared['manifest']['final']:
        return None
    native = data.get('native_receipt')
    if (native is None or native['status'] != 'committed'
            or native['commit_id'] != fact['commit_id'] or native['sink_id'] != 'lance'):
        raise RecoveryContractError('Final registration ACK lacks the original native receipt')
    receipt = LanceWriteReceipt.from_dict(native['data'])
    if (receipt.status != 'committed' or receipt.schema_hash != request['schema_hash']
            or receipt.input_rows != receipt.written_rows):
        raise RecoveryContractError('Original registration receipt differs from declared output')
    from demiflow.execution.outputs.lance_sink import spec_from_identity
    action = journal.execution_action(action_id)
    if action is None or action['declaration'].get('kind') != 'write_table':
        raise RecoveryContractError('Registration action declaration is unavailable')
    spec = spec_from_identity(action['declaration']['sink'])
    if spec.mode != 'create' or spec.content_hash != receipt.request_hash:
        raise RecoveryContractError('Registration receipt differs from the original create request')
    return receipt, fact['commit_id']


def _verify_table(uri, receipt, options, expected_schema, commit_id):
    table = open_lance_dataset(uri, receipt.committed_version, options)
    transaction = table.read_transaction(receipt.committed_version)
    if (transaction.transaction_properties or {}).get('__lance_commit_message') != 'demiflow-lance:' + commit_id:
        raise RecoveryContractError('Registered fixed-version publication identity differs from original ACK')
    if table.count_rows() != receipt.written_rows:
        raise RecoveryContractError('Registered fixed-version row count differs from native receipt')
    if schema_hash(table.schema) != schema_hash(expected_schema):
        raise ValueError('requested schema differs from committed native table schema')


def write_registered_native(dataset, *, uri, relative_uri, schema, fingerprint,
                            storage_options, max_rows_per_batch):
    """Return the original receipt and whether this call only read confirmed facts.

    The locator has no status, row count, version, fingerprint or replacement
    receipt. Those facts are read exclusively from the original RunJournal.
    """
    from .storage import normalize_storage_options
    from ..errors import LanceWriteConflict
    from demiflow.execution.executors.dataset_runtime import bind_lance_action
    options = normalize_storage_options(storage_options)
    request = {'schema': _SCHEMA, 'relative_uri': relative_uri,
               'fingerprint': fingerprint, 'schema_hash': schema_hash(schema)}
    document(request, _LIMIT)
    directory = control_directory(uri)
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise RecoveryContractError('Registration control directory contains a symlink')
    directory.mkdir(parents=True, exist_ok=True)
    locator_path = directory / 'registered-native.json'
    with (directory / 'registered-write.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise LanceWriteConflict('Another registered writer owns this target') from error
        original = _read_locator(locator_path)
        active = current_run()
        if original is not None:
            locator, journal = original
            confirmed = _confirmed(journal, locator['action_id'], request=request)
            if confirmed is not None:
                receipt, commit_id = confirmed
                _verify_table(uri, receipt, options, schema, commit_id)
                # A caller resuming the owning run still consumes its original
                # action position. A confirmed read never loads the new factory.
                if active is not None and active.resuming and active.ref.run_id == locator['run_id']:
                    prior = journal.execution_action(locator['action_id'])
                    with active.action(prior['declaration'], action_id=locator['action_id']):
                        pass
                return receipt, True
            reference = RunRef(str(journal.directory), locator['run_id'])
            if active is not None:
                if not active.resuming or active.ref != reference:
                    raise RecoveryContractError('Resume the original registration RunRef before executing unfinished input')
                scope = nullcontext(active)
            elif locator['relative']:
                scope = RunContext.resume(reference)
            else:
                raise RecoveryContractError('Resume the owning RunContext explicitly for unfinished registered output')
        elif active is not None:
            scope = nullcontext(active)
        else:
            owned = directory / 'registered-run'
            if (owned / 'run.sqlite').exists():
                raise RecoveryContractError('An original registration run exists without its locator; inspect its RunRef')
            scope = RunContext.start(owned, entry_identity=request)

        with scope as run:
            def bind(action, spec):
                if spec.uri != uri or spec.mode != 'create':
                    raise RecoveryContractError('Registration must bind its original native create action')
                if original is not None and action.action_id != original[0]['action_id']:
                    raise RecoveryContractError('Registration action position changed on resume')
                action.context.journal.bind_source(action.context.fence,
                    action.action_id + '-registered-write', request)
                local = action.context.directory.is_relative_to(directory)
                pointer = {'schema': _SCHEMA, 'run_id': action.context.ref.run_id,
                    'directory': str(action.context.directory.relative_to(directory) if local else action.context.directory),
                    'relative': local, 'action_id': action.action_id}
                if original is None:
                    atomic_bytes(locator_path, document(pointer, _LIMIT))
                elif pointer != original[0]:
                    raise RecoveryContractError('Original registration locator changed')
            with bind_lance_action(bind, max_rows_per_batch=max_rows_per_batch):
                receipt = dataset.write_lance(uri, mode='create', schema=schema,
                    storage_options=storage_options, return_receipt=True)
            located = _read_locator(locator_path)
            verified = _confirmed(located[1], located[0]['action_id'], request=request)
            if verified is None or verified[0] != receipt:
                raise RecoveryContractError('Registered write lacks its original final ACK/receipt')
            _verify_table(uri, verified[0], options, schema, verified[1])
            return verified[0], False
