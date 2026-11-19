"""The kind table: one row per kernel kind, and every older table is a view of it."""
import subprocess
import sys
from pathlib import Path

import pytest

from demiflow.execution.contracts import dataflow_kinds as kinds
from demiflow.execution.contracts.dataflow_kinds import FAMILIES, NATIVE, ROLES, TYPED, kind_records, kinds_with

import compile_contract_matrix

ROOT = Path(__file__).resolve().parents[1]

# The registered names, in order. Sealing admits a node without a user program by
# membership in these registries, so a change here changes what a sealed plan may omit.
OPERATORS = (
    'MapOp', 'FlatMapOp', 'MapBatchesOp', 'BoundMapOp', 'OperatorLLMMapOp', 'FilterOp', 'AsyncMapOp', 'DeduplicateOp',
    'AgentMapOp', 'BatchMapOp', 'EmbeddingMapOp', 'StreamGroupBatchesOp', 'StreamPrependOp', 'LimitOp',
    'SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp', 'AddColumnOp', 'RandomSampleOp', 'SortOp', 'RepartitionOp',
    'RandomShuffleOp', 'RandomizeBlockOrderOp', 'VectorSearchOp', 'SearchWebOp', 'RegisterDocumentsOp',
    'RegisterDocumentsBatchOp', 'ReadDocumentsOp', 'SaveLanceOp',
    'prompt', 'embedding', 'agent', 'raw_fetch', 'queue_produce', 'queue_ack', 'lance_lookup', 'search_vectors',
    'search_web', 'deduplicate', 'group_batches', 'stream_prepend', 'save_lance', 'register_documents',
    'register_documents_batch', 'read_documents', 'pack_prompt_inputs', 'read_local_object', 'decode_document',
    'encode_image', 'parse_wikitext')
SOURCES = ('items', 'range', 'file', 'lance', 'sql', 'materialized', 'iterable', 'datasource', 'queue', 'producer',
           'queue_history', 'join', 'exclude_keys', 'reduce', 'groups', 'union')
TERMINALS = ('take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'count', 'aggregate', 'schema', 'columns',
             'size_bytes', 'materialize', 'write_datasink', 'write_lance', 'write_file', 'run_stream', 'collect_summary')


def test_registry_keys_and_their_order_are_unchanged():
    from demiflow.planning.dataflow_registry import operator_registry, source_registry, terminal_registry
    assert tuple(operator_registry()) == OPERATORS and len(OPERATORS) == 50
    assert tuple(source_registry()) == SOURCES and tuple(terminal_registry()) == TERMINALS
    assert tuple(name for name, record in kind_records().items() if record.operator is not None) == OPERATORS


def test_rows_are_well_formed():
    records = kind_records()
    assert len(records) == 58
    for name, record in records.items():
        assert record.name == name and record.roles <= set(ROLES)
        assert (record.entry is None) == (record.operator is None) == (record.traits is None), name
        if record.operator is not None:
            compiler, kernel, ports = record.operator
            assert ':' in compiler and ':' in kernel and ports and ports[0] == 'main', name
            state, effect, role, mergeable = record.traits
            assert state in {'none', 'auto_json', 'adapter'} and effect in {'pure', 'journaled'}, name
            assert role in {'cpu', 'io', 'remote_api'} and type(mergeable) is bool, name
    typed = [name for name, record in records.items() if record.entry == TYPED]
    native = [name for name, record in records.items() if record.entry == NATIVE]
    assert tuple(typed + native) == OPERATORS and len(typed) == 29 and len(native) == 21
    assert all(name[:1].isupper() for name in typed) and all(name[:1].islower() for name in native)


def test_roles_agree_with_each_other():
    family = {name: kinds_with(name) for name in FAMILIES}
    admitted = frozenset().union(*family.values())
    # A kind called through an instance, by batch, by task, or journaled by batch is an admitted kernel kind.
    for role in ('instance', 'batch_instance', 'task', 'batch_membership'):
        assert kinds_with(role) <= admitted, role
    assert kinds_with('batch_instance') <= kinds_with('instance')
    assert kinds_with('task') <= family['unary'] and kinds_with('batch_membership') <= kinds_with('batch_instance')
    assert family['unary'] | family['batch'] <= kinds_with('keeps_layout')
    assert kinds_with('global_ordering') <= kinds_with('finite_complete') and kinds_with('global_ordering') <= family['ordering']
    # Families overlap only where a batch kind is also a model kind.
    overlap = {name for name in admitted if sum(name in members for members in family.values()) > 1}
    assert overlap == {'EmbeddingMapOp', 'RegisterDocumentsBatchOp', 'SaveLanceOp'} == family['batch'] & family['model']
    # The role expressions the kernel modules used to spell out.
    assert kinds_with('instance') == kinds_with('task') | family['batch'] | family['model']
    assert kinds_with('batch_instance') == family['batch'] | {'embedding'}
    assert kinds_with('keeps_layout') == family['unary'] | family['batch'] | {
        'RepartitionOp', 'RandomShuffleOp', 'RandomizeBlockOrderOp'}


def test_what_the_table_makes_visible():
    records = kind_records()
    from demiflow.planning.dataflow_registry import source_registry, terminal_registry
    without_kernel = [name for name, record in records.items() if record.operator is not None and not record.roles]
    assert without_kernel == list(OPERATORS[32:]) and len(without_kernel) == 18    # registered, never a kernel kind
    not_registered = [name for name, record in records.items() if record.operator is None]
    assert not_registered == ['fetch', 'document', 'join', 'exclude_keys', 'reduce', 'groups', 'union', 'aggregate']
    assert kinds_with('relational') <= set(source_registry()) and kinds_with('aggregate') <= set(terminal_registry())
    assert not {'fetch', 'document'} & (set(OPERATORS) | set(source_registry()) | set(terminal_registry()))


def test_a_native_declaration_is_refused_by_name_not_by_accident(tmp_path):
    """All 21 native names are declaration names. Placed in a plan directly, each is refused, and told why."""
    import compile_contract_corpus as corpus
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    native = tuple(name for name, record in kind_records().items() if record.entry == NATIVE)
    assert native == corpus.NATIVE_NAMES and len(native) == 21
    context = corpus.Context(tmp_path)
    for name in native:
        with pytest.raises(DataflowError) as refused:
            corpus.lower_case('native/' + name, context)
        assert refused.value.code == 'UNSUPPORTED_CAPABILITY', name
        assert 'native declaration cannot be lowered directly: ' + name + ';' in str(refused.value), name
    # A name that is not registered at all keeps its own refusal.
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import NativeOperation
    from demiflow.planning.dataflow_compile import compile_kernel_spec
    with pytest.raises(DataflowError, match='operator kind is not registered: not_registered_kind'):
        compile_kernel_spec(NativeOperation(kind='not_registered_kind', configuration={}, bindings={},
                                            factory=CallableSpec.create(len)), program_ref=None, bounds=corpus.bounds())


def test_every_older_table_is_a_view_of_the_rows():
    from demiflow.execution import dataflow_batch_membership
    from demiflow.execution.kernels import dataflow as kernels, operator, task_scope
    from demiflow.planning import dataflow_compile as compiler, dataflow_configuration
    views = {
        'unary': kernels._UNARY_KINDS, 'batch': kernels._BATCH_KINDS, 'model': kernels._MODEL_KINDS,
        'relational': kernels._RELATIONAL_KINDS, 'ordering': kernels._ORDERING_KINDS,
        'aggregate': kernels._AGGREGATE_KINDS, 'stream_group': kernels._STREAM_GROUP_KINDS,
        'instance': operator._INSTANCE_KINDS, 'batch_instance': operator._BATCH_INSTANCE_KINDS,
        'task': task_scope.TASK_KINDS, 'batch_membership': dataflow_batch_membership.KINDS,
        'global_ordering': compiler._ORDERING_KINDS, 'finite_complete': compiler._FINITE_COMPLETE_KINDS,
    }
    for role, view in views.items():
        assert type(view) is frozenset and view == kinds_with(role), role
    assert dataflow_configuration.TASK_KINDS == kinds_with('task')
    assert kernels.KERNEL_KINDS == frozenset().union(*(kinds_with(name) for name in FAMILIES)) and len(kernels.KERNEL_KINDS) == 40
    assert dict(compiler._TRAITS) == dict(kinds.traits_of(TYPED)) and dict(compiler._NATIVE_TRAITS) == dict(kinds.traits_of(NATIVE))
    assert not set(compiler._TRAITS) & set(compiler._NATIVE_TRAITS)


def test_the_literal_tables_are_gone():
    """One table: the names survive as views, the second copies of the data do not."""
    for relative, gone in {
        'planning/dataflow_registry.py': '("MapOp", _COMPILE',
        'planning/dataflow_compile.py': '"MapOp": ("none"',
        'execution/kernels/dataflow.py': '"MapOp", "FlatMapOp"',
        'execution/kernels/operator.py': 'frozenset({"MapOp"',
        'execution/kernels/task_scope.py': "frozenset({'MapOp'",
        'execution/dataflow_batch_membership.py': "frozenset({'embedding'",
        'execution/worker/terminal_blocks.py': '_UNARY_KINDS | _BATCH_KINDS',
        'planning/dataflow_configuration.py': "frozenset({'MapOp'",
    }.items():
        assert gone not in (ROOT / 'demiflow' / relative).read_text(encoding='utf-8'), relative


def test_the_table_loads_without_the_engine():
    script = ("import sys\nimport demiflow.execution.contracts.dataflow_kinds as kinds\n"
              "assert len(kinds.kind_records()) == 58\n"
              "heavy = [name for name in sys.modules if name.startswith('demiflow.planning') or name in ("
              "'pyarrow', 'lance', 'demiflow.execution.kernels.operator', 'demiflow.data.plan')]\n"
              "assert not heavy, heavy\n")
    done = subprocess.run([sys.executable, '-P', '-B', '-c', script], cwd=ROOT, capture_output=True, text=True, timeout=60,
                          env={'PYTHONPATH': str(ROOT), 'PATH': '/usr/bin:/bin'})
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_loaded_matrix_is_self_consistent():
    loaded = compile_contract_matrix.matrix()
    assert compile_contract_matrix.compare(loaded, compile_contract_matrix.matrix()) == []
    cells = loaded['cells']
    assert all(not any(value for name, value in cells[kind].items()
                       if name not in ('port_main', 'port_prefix', 'port_left', 'sealable_as'))
               for kind in compile_contract_matrix.UNKNOWN)
    assert cells['EmbeddingMapOp']['batch_call'] and not cells['EmbeddingMapOp']['row_call']
    assert cells['OperatorLLMMapOp']['row_call'] and cells['MapOp']['task_scoped'] and not cells['LimitOp']['task_scoped']
