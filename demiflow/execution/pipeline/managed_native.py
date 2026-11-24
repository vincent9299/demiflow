"""Host admission for a native Dataset flow, using the original run and driver.

The contract covers one compiled terminal and its entire upstream graph. It is
neither a Python analyser nor a second executor. Extra actions are rejected;
no receipt is fabricated to discover branches after an execution boundary.
"""

import inspect
from demiflow.execution.executors.plan_capture import CapturedAction
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path

from demiflow.execution.pipeline.checked import CheckedExecution, CheckedAction, _equal
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity

from demiflow.execution.contracts.pipeline import NATIVE_SUBMISSION_FORMAT as FORMAT


def capture_managed_pipeline(entry, arguments, *, limits, explicit_limits=None, inherited_native_resources=None, on_compiled=None):
    from demiflow.execution.pipeline.local_kernel import local_execution
    def construct():
        with local_execution(worker_mode='thread', partitions=1, batch_rows=limits['batch_rows'],
                block_size=limits['batch_rows'], memory_bytes=limits['memory_bytes'],
                inherited_native_resources=inherited_native_resources):
            return entry(**arguments)
    try:
        return capture_pipeline(construct, {}, on_compiled=on_compiled,
                                _submission=True, limits=limits, explicit_limits=explicit_limits)
    except DataflowError as exc:
        # Host inspection returns a bounded admission rejection. It has not
        # started data/model execution; do not lose it as a worker crash.
        raise ValueError(str(exc)) from exc





def submission_contract(request, compiled, *, bounds):
    """Derive write grants from actual native declarations before any execution."""
    from demiflow.execution.contracts.dataflow_codec import encode_wire
    from demiflow.lance.schema import schema_identity
    from demiflow.lance.model import LanceWriteSpec
    from demiflow.execution.native_queue.dataflow_stream_lance import StreamLanceFactory
    import pyarrow as pa
    if request.provider != 'local' or request.kind != 'write_lance':
        raise DataflowError('UNSUPPORTED_CAPABILITY',
                            'Managed native submission currently requires a Local write_lance terminal')
    spec = next((p.value for p in request.programs if p.name == 'write_lance'), None)
    if not isinstance(spec, LanceWriteSpec) or spec.mode != 'create' or spec.schema is None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed native terminal requires create and an explicit schema')
    if spec.storage_options:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed native output requires a local target')
    graph = compiled.graph
    terminal = graph.terminals[0]
    outputs = [dict(node_id=request.action.action_id, occurrence_id=terminal,
                    uri=spec.uri, mode='create', expected_version=None,
                    schema_identity=schema_identity(spec.schema), write_spec=spec.to_dict())]
    for declaration in compiled.declarations:
        factory = declaration.callable_spec
        if factory is None or factory.target is not StreamLanceFactory:
            continue
        config = factory.constructor_args[0]
        if config['mode'] not in {'append', 'overwrite'}:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Unsupported managed stage write mode')
        schema = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(config['schema'])))
        outputs.append(dict(node_id=declaration.occurrence_id,
            occurrence_id=declaration.occurrence_id, uri=config['uri'], mode='create',
            expected_version=None, writer_mode=config['mode'], stage=config['stage'],
            schema_identity=schema_identity(schema), max_index_bytes=config['max_index_bytes']))
    if len(outputs) > 32 or len({o['uri'] for o in outputs}) != len(outputs):
        raise DataflowError('INVALID_CONTRACT', 'Managed native outputs must be unique and bounded to 32')
    for output in outputs:
        path = Path(output['uri'])
        if not path.is_absolute() or str(path.resolve()) != output['uri']:
            raise DataflowError('INVALID_CONTRACT', 'Managed native output must be a canonical local path')
    object_roots, credentials = set(), set()
    for node in graph.nodes:
        if node.kernel.kind not in {'prompt', 'agent', 'embedding'}:
            continue
        config = node.kernel.configuration
        options = config.get('options', {})
        model = config.get('model', {})
        if not any(name in options for name in ('codex_exec', 'codex_agent', 'cli')):
            for name in ('api_key_env', 'base_url_env'):
                if model.get(name):
                    credentials.add(model[name])
        from collections.abc import Mapping
        if isinstance(options.get('cli'), Mapping):
            for env_name in options['cli'].get('env_passthrough') or ():
                if isinstance(env_name, str) and env_name:
                    credentials.add(env_name)
        if options.get('object_store'):
            object_roots.add(options['object_store'])
        for name in ('codex_exec', 'codex_agent', 'cli'):
            root = options.get(name, {}).get('artifact_store', {}).get('directory')
            if root:
                object_roots.add(root)
    if len(object_roots) > 32 or len(credentials) > 16:
        raise DataflowError('INVALID_CONTRACT', 'Managed native resources exceed the host contract')
    for root in object_roots:
        if not isinstance(root, str) or not Path(root).is_absolute() or str(Path(root).resolve()) != root:
            raise DataflowError('INVALID_CONTRACT', 'Native object store must have a canonical local root')
    # Full wire equality includes configuration, bounds, resources, ports and
    # code identities. dag_id alone is never the admission comparison.
    wire = encode_wire(graph, type_name='DataflowGraph', bounds=bounds).decode()
    return dict(format=FORMAT, action_id=request.action.action_id, kind=request.kind,
                graph=json.loads(wire), outputs=outputs, object_roots=sorted(object_roots),
                credential_names=sorted(credentials))


class NativeExecution(CheckedExecution):
    """Attach native admission to the existing checked observation/receipt sink."""
    def __init__(self, *, contract, **kwargs):
        if contract.get('format') != FORMAT:
            raise ValueError('Unknown managed native contract')
        self.contract = contract
        self.guard_count = 0
        super().__init__(**kwargs)

    @contextmanager
    def action(self, kind, source, plan, spec, arguments=None):
        self.check_stop()
        if kind == 'from_items':
            # Literal construction is not an action, reservation or execution.
            yield CheckedAction(None, None, source, plan)
            return
        if kind == 'require' and source is None and plan is None:
            # Business prerequisites/result assertions use the original native
            # action journal, without consuming the compiled Dataset action ID.
            self.guard_count += 1
            if self.guard_count > 32:
                raise DataflowError('INVALID_CONTRACT', 'Native prerequisite count exceeds 32')
            yield CheckedAction(None, 'guard-' + str(self.guard_count), None, None)
            return
        if self.active is not None or self.position != 0 or kind != 'write_table':
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Flow reached an action outside its sealed native submission')
        action_id = self.contract['action_id']
        output = self.contract['outputs'][0]
        _equal(spec.to_dict() if spec else None, output['write_spec'], 'native terminal')
        from demiflow.execution.pipeline.run_context import current_run
        self.active = self.matching_action = action_id
        self.operation_id = identity([current_run().ref.run_id, action_id, 1])
        self.observations.clear()
        for node in self.nodes:
            self._match(node)
        self.record(action_id, 'intent', dict(kind=kind, spec=spec.to_dict(),
            request_hash=spec.content_hash, operation_id=self.operation_id))
        value = CheckedAction(self, action_id, source, plan)
        try:
            yield value
            if not value.completed:
                raise DataflowError('INVALID_CONTRACT', 'Native action returned without a receipt')
            self.position += 1
            self.flush_observations(final=True, succeeded=True)
        except BaseException as exc:
            self.record(action_id, 'failed', dict(type=type(exc).__name__, message=str(exc)[:2048]))
            self.flush_observations(final=True)
            raise
        finally:
            self.active = self.operation_id = None

    def admit_compiled(self, request, compiled, *, bounds):
        actual = submission_contract(request, compiled, bounds=bounds)
        _equal(actual, self.contract, 'full native compiled submission')
        terminal = compiled.graph.terminals[0]
        bindings = tuple(dict(occurrence_id=node.occurrence_id,
            report_node_id=request.action.action_id if node.occurrence_id == terminal else node.occurrence_id,
            report_occurrence=0, logical_node_id=node.semantic_id, mode='execute')
            for node in (*compiled.graph.nodes, *compiled.graph.jobs))
        return replace(compiled, report_bindings=bindings)

    def native_save_policy(self, execution, node, writer):
        from demiflow.execution.journal.dataflow_stage_facts import stage_binding
        _, binding = stage_binding(execution._action.context.journal,
            execution._action.context.directory, execution._action.action_id, node, self.report_digest)
        declared = next((o for o in self.contract['outputs'] if o['node_id'] == binding['report_node_id']), None)
        if (declared is None or declared['uri'] != writer.uri or declared.get('stage') != writer.stage
                or declared.get('writer_mode') != writer.mode):
            raise DataflowError('INVALID_CONTRACT', 'Native stage differs from admitted output')
        return dict(mode='create', expected_version=None)


def capture_pipeline(entry, arguments, *, on_compiled=None, limits=None, explicit_limits=None, _submission=False) -> "CapturedAction":
    """Compile the exact first action request from a trusted callable and kwargs.

    The same make_action_request/resolve_action_runtime/lower_action functions
    are used by execution. No placeholder receipt, source snapshot, physical
    placement, worker or completion fact is produced. The returned graph covers
    one action, never a claim about the rest of an arbitrary Python program.
    Optional host callback ``on_compiled(CompilationProvenance)`` runs once after
    final graph validation, before unwinding the active SDK call. It receives
    only bounded, process-local origin links; it never changes sealed identity.
    Budget exhaustion is reported as unavailable provenance. Callback errors
    propagate (capture context still resets); no success is fabricated.
    """
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    if not callable(entry) or type(arguments) is not dict:
        raise TypeError('capture_pipeline requires a callable and keyword arguments')
    if on_compiled is not None and not callable(on_compiled):
        raise TypeError('on_compiled must be a callable or None')
    from demiflow.execution.executors.plan_capture import capture_scope, _Captured
    with capture_scope(on_compiled, limits=limits, explicit_limits=explicit_limits, submission=_submission) as capture:
        try:
            returned = entry(**arguments)
        except _Captured:
            if capture.result is not None:
                return capture.result
            raise
        if inspect.iscoroutine(returned) or inspect.isgenerator(returned):
            returned.close()
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'entry did not reach a Dataset action, or swallowed the planning boundary')
