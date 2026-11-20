"""The configuration schema: every key of every compiled kernel has a known writer and shape.

The check is report-only. These tests hold the evidence that it is ready to be
trusted: it reports nothing on any graph the corpus compiles, and it reports
each kind of defect when one is introduced.
"""
import logging
from dataclasses import replace
from pathlib import Path

import pytest

from demiflow.planning import dataflow_configuration as schema
from demiflow.planning.dataflow_compile import _plain
from demiflow.planning.dataflow_configuration import configuration_findings, report_configuration

import compile_contract_corpus as corpus
from configuration_schema_gate import reports_on_purpose


@pytest.fixture(scope='module')
def context(tmp_path_factory):
    return corpus.Context(tmp_path_factory.mktemp('configuration'))


@pytest.fixture(scope='module')
def graphs(context):
    compiled = {}
    for name in sorted(corpus.CASES):
        try:
            compiled['case ' + name] = corpus.lower_case(name, context)[0].graph
        except Exception:
            continue          # a refused declaration has no graph; refusals are pinned by the corpus test
    for name in sorted(corpus.CAPTURED):
        compiled['captured ' + name] = corpus.capture(name, context).compiled.graph
    return compiled


def test_every_compiled_graph_of_the_corpus_fits_the_schema(graphs):
    assert len(graphs) >= 320
    found = {name: configuration_findings(graph) for name, graph in graphs.items()}
    assert {name: value for name, value in found.items() if value} == {}


def test_every_key_the_corpus_produces_has_one_writer(graphs):
    """Each observed key is a declared operation field, a source fact, a model key or a compiler key."""
    unexplained = set()
    for graph in graphs.values():
        for node in graph.nodes:
            kind = node.kernel.kind
            declared = schema.declared_fields(kind) or frozenset()
            owned = (schema.SOURCE_KEYS.get(kind, frozenset()) | schema.MODEL_KEYS.get(kind, frozenset())
                     | (schema.TERMINAL_KEYS if node.kind == 'terminal' else frozenset()) | declared
                     | schema.STREAM_KEYS | {schema.SCHEMA_KEY})
            unexplained |= {(node.kind, kind, key) for key in node.kernel.configuration
                            if key not in owned and key not in schema.COMPILER_KEYS}
    assert unexplained == set()
    stages = {key.stage for key in schema.COMPILER_KEYS.values()}
    assert stages == {'capture', 'lowering', 'budget', 'graph', 'shared'}


def test_names_owned_by_other_modules_are_mirrored_exactly():
    from demiflow.execution import dataflow_managed_devices, dataflow_publication_budget, dataflow_schema
    from demiflow.execution.kernels import reducer_memory, shuffle_order, task_scope
    from demiflow.planning import dataflow_compile
    assert dataflow_publication_budget.FIELD == 'publication_workspace' in schema.COMPILER_KEYS
    assert {dataflow_publication_budget.SCHEMA, dataflow_publication_budget.PHASE_SCHEMA} == schema.PUBLICATION_SCHEMAS
    assert reducer_memory.FIELD == 'reducer_memory_v1' in schema.COMPILER_KEYS
    assert shuffle_order.FIELD == 'shuffle_order_version' in schema.COMPILER_KEYS
    assert dataflow_schema.KEY == schema.SCHEMA_KEY
    assert dataflow_managed_devices._KEY == 'managed_http_devices' in schema.MODEL_KEYS['prompt']
    assert task_scope.TASK_KINDS == schema.TASK_KINDS
    assert dataflow_compile._NON_SEMANTIC_FIELDS | {'callable', 'native_options', 'stream_options'} == schema.NOT_COPIED


def test_every_key_a_reader_asks_for_is_explained():
    """Source scan: a key read from a configuration is written by a known stage, or listed as a gap."""
    import dataclasses
    import compile_contract_inventory as inventory
    from demiflow.data import plan
    access = inventory.configuration_access()
    read = set(access['literal']) | set(access['accessor'])
    declared = set()
    for name in dir(plan):
        operation = getattr(plan, name)
        if isinstance(operation, type) and dataclasses.is_dataclass(operation) and name.endswith('Op'):
            declared |= {field.name for field in dataclasses.fields(operation)}
    written = (set(schema.COMPILER_KEYS) | set().union(*schema.SOURCE_KEYS.values())
               | set().union(*schema.MODEL_KEYS.values()) | schema.TERMINAL_KEYS | schema.STREAM_KEYS
               | {schema.SCHEMA_KEY} | declared | schema.SEAL_KEYS)
    unexplained = read - written - set(schema.READ_WITHOUT_WRITER)
    # What remains is not a kernel configuration: the scan also sees other objects' '.configuration'
    # (the run record, a sink declaration) and the inner mapping of graph_result.
    assert unexplained == {'limits', 'limits_explicit', 'run_id', 'started_at',     # run record
                           'stream_protocol',                                       # run recovery format
                           'partition_id', 'sink_id',                                # sink declaration
                           'ancestor', 'targets'}                                    # inside graph_result
    assert set(schema.READ_WITHOUT_WRITER) <= read and not set(schema.READ_WITHOUT_WRITER) & written
    assert schema.SEAL_KEYS <= read


def test_keys_without_a_writer_are_one_of_two_things(graphs):
    assert set(schema.HISTORICAL_KEYS) == {'initial', 'initial_program', 'reducer_kwargs'}
    assert set(schema.UNPRODUCED_KEYS) == {'aggregates', 'cache', 'cache_namespace'}
    assert not set(schema.HISTORICAL_KEYS) & set(schema.UNPRODUCED_KEYS)
    root = Path(__file__).resolve().parents[1]
    relational = (root / 'demiflow/execution/kernels/relational.py').read_text(encoding='utf-8')
    # The earlier reduce form is read on purpose, and a test holds it.
    assert 'Legacy sealed plans keep their original' in relational
    assert all(repr(name)[1:-1] in relational for name in schema.HISTORICAL_KEYS)
    assert 'def test_legacy_unfinished_reduce_validates_original_grant_without_rewriting_seal' in (
        root / 'tests/test_runtime_v3_reducer_memory.py').read_text(encoding='utf-8')
    # No compiled plan reaches the kernels the other keys declare.
    nodes = [node for graph in graphs.values() for node in graph.nodes]
    assert not any(set(node.kernel.configuration) & set(schema.READ_WITHOUT_WRITER) for node in nodes)
    assert {node.kind for node in nodes if node.kernel.kind == 'aggregate'} == {'terminal'}
    assert not any(node.kernel.kind in ('fetch', 'document') for node in nodes)
    engine = root / 'demiflow'
    writers = [path.relative_to(engine).as_posix() for path in sorted(engine.rglob('*.py'))
               if 'cache_namespace' in path.read_text(encoding='utf-8')]
    assert writers == ['execution/kernels/dataflow.py', 'planning/dataflow_configuration.py']   # one reader, this schema


def _mutate(graph, kernel_kind, change):
    nodes = []
    done = False
    for node in graph.nodes:
        if not done and node.kernel.kind == kernel_kind:
            configuration = change(_plain(node.kernel.configuration))     # thawed: KernelSpec freezes plain Json
            node = replace(node, kernel=replace(node.kernel, configuration=configuration))
            done = True
        nodes.append(node)
    assert done, kernel_kind
    return replace(graph, nodes=tuple(nodes))


def _drop(name):
    return lambda configuration: {key: value for key, value in configuration.items() if key != name}


def _set(name, value):
    return lambda configuration: {**configuration, name: value}


@pytest.mark.parametrize('case, kind, change, expected', [
    ('case map', 'MapOp', _set('not_a_key', 1), 'is not admitted on this node'),
    ('case map', 'MapOp', _set('occurrence_id', 'f' * 64), 'occurrence_id does not name this node'),
    ('case map', 'MapOp', _set('program', 'f' * 64), 'program does not name this node'),
    ('case map', 'MapOp', _drop('task_origin'), 'needs task_batch_rows, task_group and task_origin together'),
    ('case map', 'MapOp', _set('task_batch_rows', 0), 'key task_batch_rows does not have the shape'),
    ('case select_drop_rename', 'SelectColumnsOp', _set('task_origin', True), 'key task_origin is not admitted'),
    ('case limit', 'LimitOp', _set('task_group', 'g'), 'key task_group is not admitted'),
    ('case map_batches', 'MapBatchesOp', _set('default_batch_size', 0), 'key default_batch_size does not have the shape'),
    ('case map_batches_sized', 'MapBatchesOp', _set('default_batch_size', 8), 'default_batch_size beside a declared batch_size'),
    ('case fetch', 'AsyncMapOp', _set('native_operation', 'download'), 'key native_operation does not have the shape'),
    ('case ack_queue', 'AsyncMapOp', _set('native_key', 'k'), 'native_key without an acquire or search declaration'),
    ('case map', 'take', _set('kind', 'count'), 'terminal kind differs from its kernel kind'),
    ('case map', 'take', _set('terminal_memory', {'payload_bytes': 1}), 'key terminal_memory does not have the shape'),
    ('case terminal_write_lance', 'write_lance', _set('publication_workspace', {'schema': 'v0', 'resident_bytes': 1}),
     'key publication_workspace does not have the shape'),
    ('case exclude_keys', 'exclude_keys', _set('lance_key_source', 'f' * 64), 'lance_key_source does not name a source'),
    ('case join', 'join', _set('output_carrier', 'stored_typed_row.v1'), 'output_carrier has no matching consumer'),
    ('case source_range', 'range', _set('rows', 5), 'key rows is not admitted'),
    ('case map_prompt', 'prompt', _set('max_batch', 4), 'key max_batch is not admitted'),
])
def test_a_defect_is_reported(graphs, case, kind, change, expected):
    assert configuration_findings(graphs[case]) == ()
    found = configuration_findings(_mutate(graphs[case], kind, change))
    assert any(expected in finding for finding in found), found


@reports_on_purpose
def test_the_opt_in_gate_collects_reports_and_counts_checked_graphs():
    """What the gate plugin acts on: one line per checked graph, one line per finding."""
    import configuration_schema_gate as gate
    handler = gate._Reports()
    logger = logging.getLogger(gate._LOGGER)
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        logger.debug('not parsed', extra={'configuration_schema': {
            'stage': 'final', 'findings': 0, 'check_completed': True}})
        assert handler.graphs == 1 and handler.findings == []
        logger.debug('not parsed', extra={'configuration_schema': {
            'stage': 'budget', 'findings': 1, 'check_completed': False}})
        assert len(handler.findings) == 1
        assert handler.stages == {'final': 1, 'budget': 1} and handler.graphs == 1
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def test_a_kind_the_schema_does_not_describe_is_reported_not_guessed(graphs):
    graph = graphs['case map']
    node = next(node for node in graph.nodes if node.kernel.kind == 'MapOp')
    strange = replace(graph, nodes=tuple(
        replace(item, kernel=replace(item.kernel, kind='raw_fetch')) if item is node else item for item in graph.nodes))
    assert any('not described by the configuration schema' in finding for finding in configuration_findings(strange))


@reports_on_purpose
def test_production_warns_and_never_refuses(graphs, caplog):
    broken = _mutate(graphs['case map'], 'MapOp', _set('not_a_key', 1))
    logger = logging.getLogger('demiflow.planning.configuration')
    level, propagate = logger.level, logger.propagate
    logger.setLevel(logging.NOTSET)                            # the default, whatever a harness configured
    logger.propagate = True
    try:
        with caplog.at_level(logging.WARNING):
            assert report_configuration(graphs['case map']) == ()
            assert not [record for record in caplog.records if record.name == logger.name]   # a clean plan is silent
            found = report_configuration(broken)                # returned, logged, not raised
            assert len(found) == 1 and 'unknown#' in found[0] and 'not_a_key' not in found[0]
            warnings = [record for record in caplog.records if record.name == logger.name]
            assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING and 'unknown#' in warnings[0].getMessage()
            caplog.clear()
            assert 'inspection is incomplete' in report_configuration(object())[0]  # not a false clean result
            assert [record.levelno for record in caplog.records if record.name == logger.name] == [logging.WARNING]
            caplog.clear()
            many = _mutate(graphs['case map'], 'MapOp', lambda configuration: {
                **configuration, **{'unknown_%02d' % index: index for index in range(30)}})
            assert len(report_configuration(many)) == 30
            messages = [record.getMessage() for record in caplog.records if record.name == logger.name]
            assert len(messages) == schema._MAX_REPORTED + 1 and messages[-1].endswith('and 10 more findings')
        with caplog.at_level(logging.DEBUG, logger=logger.name):
            caplog.clear()
            report_configuration(graphs['case map'])
            assert any('3 nodes checked, 0 findings' in record.getMessage() for record in caplog.records)
    finally:
        logger.setLevel(level)
        logger.propagate = propagate


@pytest.mark.parametrize('value', [{}, [], [{}], None, 0, True])
def test_invalid_source_reference_keeps_other_findings(graphs, value):
    graph = _mutate(graphs['case exclude_keys'], 'exclude_keys', lambda c: {
        **c, 'lance_key_source': value, 'occurrence_id': 'f' * 64})
    terminal = next(node for node in graph.nodes if node.kind == 'terminal')
    graph = _mutate(graph, terminal.kernel.kind, _set('kind', 'wrong'))
    findings = tuple(schema._graph_findings(graph))
    codes = {finding.code for finding in findings}
    assert {'compiler_key_shape', 'occurrence_binding', 'terminal_kind'} <= codes
    assert not {'source_reference', 'check_error'} & codes
    assert all(finding.category == 'invariant' for finding in findings)


def test_full_scan_is_total_for_compiler_fields_and_nested_json(graphs):
    # Exercise the real collector, not only individual predicates. Each field
    # uses a node on which the compiler owns it, including the two optional
    # shared-actor fields not present in the main corpus.
    values = (None, False, True, 0, -1, 1, 1.5, '', 'text', [], {}, [{}],
              {'schema': {}, 'resident_bytes': 1},
              {'owner': {}, 'members': [{}], 'state_owner': {}},
              {'owner': 'x', 'members': [{'nested': [[]]}], 'state_owner': 'x'})
    checked = set()
    for key in schema.COMPILER_KEYS:
        for graph in graphs.values():
            candidates = [node for node in graph.nodes if key in (schema._admitted(node) or ())
                          and not (node.kind == 'transform' and key in (schema.declared_fields(node.kernel.kind) or ()))]
            if candidates:
                node = candidates[0]
                break
        else:
            raise AssertionError('no admitted node for ' + key)
        for value in values:
            changed = _mutate(graph, node.kernel.kind, _set(key, value))
            findings = tuple(schema._graph_findings(changed))
            assert all(f.code != 'check_error' for f in findings), (key, value, findings)
            assert tuple(f.text() for f in findings) == configuration_findings(changed)
        checked.add(key)
    assert checked == set(schema.COMPILER_KEYS)


@reports_on_purpose
def test_internal_checker_failure_keeps_partial_and_later_findings(graphs, monkeypatch, caplog):
    graph = _mutate(graphs['case map'], 'MapOp', _set('occurrence_id', 'f' * 64))
    graph = _mutate(graph, 'take', _set('kind', 'wrong'))
    original = schema._node_findings
    def failing(node, by_occurrence, *, stage):
        for finding in original(node, by_occurrence, stage=stage):
            yield finding
            if node.kernel.kind == 'MapOp':
                raise RuntimeError('api_key=never-log-this-value')
    monkeypatch.setattr(schema, '_node_findings', failing)
    with caplog.at_level(logging.DEBUG, logger=schema._LOG.name):
        result = report_configuration(graph)
    assert any('occurrence_id does not name' in f for f in result)
    assert any('terminal kind differs' in f for f in result)
    assert any('inspection is incomplete' in f for f in result)
    summaries = [r.configuration_schema for r in caplog.records if hasattr(r, 'configuration_schema')]
    assert len(summaries) == 1 and summaries[0]['check_completed'] is False
    assert summaries[0]['codes']['check_error'] == 1
    assert 'never-log-this-value' not in caplog.text


def test_unknown_keys_are_observations_with_safe_stable_locations(graphs):
    key = ('private-key-material-' * 100)[:256]  # maximum admitted DTO key length
    graph = _mutate(graphs['case map'], 'MapOp', _set(key, {'secret': 'model body'}))
    findings = tuple(schema._graph_findings(graph))
    assert len(findings) == 1
    finding = findings[0]
    assert (finding.code, finding.category, finding.stage) == ('unknown_key', 'observation', 'final')
    assert finding.key.startswith('unknown#') and len(finding.text()) < 160
    assert key not in finding.text() and 'model body' not in finding.text()
    assert finding == tuple(schema._graph_findings(graph))[0]


def test_carrier_check_inspects_all_edges_independent_of_order(graphs):
    from types import SimpleNamespace
    graph = _mutate(graphs['case join'], 'join', _set('output_carrier', 'stored_typed_row.v1'))
    producer = next(n for n in graph.nodes if n.kernel.kind == 'join')
    terminal = next(n for n in graph.nodes if n.kind == 'terminal')
    graph = _mutate(graph, terminal.kernel.kind, _set('input_carrier', 'stored_typed_row.v1'))
    assert not any(f.code == 'carrier_pair' for f in schema._graph_findings(graph))
    other = next(n for n in graph.nodes if n.kind == 'source')
    bad = SimpleNamespace(producer=producer.occurrence_id, consumer=other.occurrence_id)
    for edges in ((*graph.edges, bad), (bad, *graph.edges)):
        view = SimpleNamespace(nodes=graph.nodes, edges=edges)
        found = [f for f in schema._graph_findings(view) if f.code == 'carrier_pair']
        assert len(found) == 1 and found[0].category == 'observation'
