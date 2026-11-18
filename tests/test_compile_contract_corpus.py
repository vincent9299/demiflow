"""The compile corpus: real declaration paths, lowered by the real compiler.

Byte-for-byte equality between a baseline engine tree and a changed one is not
a unit test; it is the comparison described in ``compile_contract_corpus`` (run
``dump`` under each tree against the same root, then ``compare``). What is
checked here holds inside one tree: every case has a recorded outcome, shapes
match the committed baseline, lowering is deterministic, and coverage is stated.
"""
import json
from pathlib import Path

import pytest

import compile_contract_corpus as corpus
import compile_contract_inventory as inventory

STRUCTURE = Path(__file__).with_name('fixtures') / 'compile-contract-structure.json'


@pytest.fixture(scope='module')
def result(tmp_path_factory):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
    return corpus.dump(tmp_path_factory.mktemp('compile-corpus'))


def test_structure_and_refusals_match_the_committed_baseline(result):
    assert corpus.structure(result) == json.loads(STRUCTURE.read_text(encoding='utf-8')), (
        'The shape of compiled graphs changed (kinds, traits, ports, configuration keys, labels, report bindings) or '
        'a declaration is now refused or admitted. If intended, regenerate '
        'tests/fixtures/compile-contract-structure.json with compile_contract_corpus and review the diff.')


def test_lowering_the_same_declarations_twice_gives_the_same_outputs(result):
    # Graph bytes, program declarations, report bindings and observation labels, in both families.
    assert corpus.compare(result, corpus.dump(result['root'])) == []


def test_real_terminal_methods_reach_the_compiler_without_running(result):
    refused = {name: item['error'] for name, item in result['captured'].items() if not item['ok']}
    assert not refused, refused
    kinds = {name: item['kind'] for name, item in result['captured'].items()}
    assert {kinds[name] for name in ('take', 'take_zero')} == {'take'}
    assert {kinds[name] for name in ('write_parquet', 'write_json', 'write_csv')} == {'write_file'}
    assert {kinds[name] for name in ('sum', 'aggregate')} == {'aggregate'}
    assert kinds['write_lance'] == 'write_lance' and kinds['write_datasink'] == 'write_datasink'
    assert all(item['action_id'] == 'preview' and item['provider'] == 'local' for item in result['captured'].values())


def test_coverage_is_stated_not_assumed(result):
    tables = inventory.tables()
    compiled = [item for item in result['cases'].values() if item['ok']]
    nodes = [node for item in compiled for node in item['nodes']]
    exercised = {operation for item in compiled for operation in item['operations']}
    typed = {kind for kind in tables['registry.operators'] if kind[:1].isupper()}
    assert exercised == typed                      # every typed operation is lowered by some case
    kinds = lambda role: {node['kernel_kind'] for node in nodes if node['node_kind'] == role}
    # 'producer' is registered as a source but source classification never yields it.
    assert set(tables['registry.sources']) - kinds('source') - kinds('relational') == {'producer'}
    assert kinds('terminal') == set(tables['registry.terminals'])
    # Transform kernels carry either a typed operation name or one of three model kinds; the other
    # eighteen registered native names are never a kernel kind (see the inventory's differences).
    assert kinds('transform') - typed == {'prompt', 'embedding', 'agent'}
    assert {node.get('native_operation') for node in nodes} - {None} == {'acquire', 'finalize', 'producer', 'search'}
    keys = {key for node in nodes for key in node['configuration']}
    for branch in ('default_batch_size', 'task_group', 'task_origin', 'task_batch_rows', 'streaming_map',
                   'shuffle_order_version', 'native_key', 'lance_key_source', 'reducer_program', 'reducer_memory_v1',
                   'publication_workspace', 'input_carrier', 'output_carrier', 'terminal_memory',
                   'graph_route', 'graph_reader', 'graph_observe', 'graph_private_tag', 'graph_result',
                   'graph_literal_prefix', 'managed_http_service_digest'):
        assert branch in keys, branch
    states = {node['state'] for node in nodes}
    assert states == {'none', 'auto_json', 'adapter', 'nonrecoverable'}, states
    assert any(node['resource_role'] == 'gpu' for node in nodes)
    reported = [name for name, item in result['cases'].items() if item['ok'] and item['report_bindings']]
    assert reported and all(name.startswith('report/') for name in reported)
    assert {binding['mode'] for name in reported for binding in result['cases'][name]['report_bindings']} == {
        'execute', 'reuse'}
    refused = {item['error'].split(': ', 1)[0] for item in result['cases'].values() if not item['ok']}
    assert {'DataflowError:BUDGET_EXCEEDED', 'DataflowError:INVALID_CONTRACT', 'DataflowError:RESOURCE_INFEASIBLE',
            'DataflowError:UNSUPPORTED_CAPABILITY'} <= refused


def test_executor_defaults_are_the_only_difference_between_executor_variants(result):
    """What the stand-in executors change is exactly the three defaults the compiler reads from them."""
    def transform(name):
        return result['cases'][name]['nodes'][1]['configuration']    # key -> type name of its value
    assert 'default_batch_size' in transform('executor/map_batches@local')
    assert 'default_batch_size' in transform('executor/map_batches@block_only')
    assert 'default_batch_size' in transform('executor/map_batches@local_without_rows')   # falls back to the block size
    assert 'default_batch_size' not in transform('map_batches')
    assert 'default_batch_size' not in transform('executor/map_batches_sized@local')      # an explicit size wins
    assert transform('random_shuffle')['num_blocks'] == 'NoneType'
    assert transform('executor/random_shuffle@local')['num_blocks'] == 'int'
    assert transform('executor/random_shuffle@block_only')['num_blocks'] == 'NoneType'    # no partitions without a kernel
    assert transform('executor/random_shuffle_blocks@local') == transform('random_shuffle_blocks')
    without = {name.split('@')[0] for name in result['cases'] if name.startswith('executor/')}
    assert without == {'executor/map_batches', 'executor/map_batches_sized', 'executor/random_shuffle',
                       'executor/random_shuffle_blocks', 'executor/task_chain', 'executor/streaming_map'}
