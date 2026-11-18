"""The compile corpus: real declaration paths, lowered by the real compiler.

Three properties are checked here. Byte-for-byte equality between a baseline
engine tree and a changed one is not a unit test; it is the comparison
described in ``compile_contract_corpus`` (run ``dump`` under each tree against
the same root and compare the digests).
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


def test_every_case_lowers_without_running_anything(result):
    refused = {name: case['error'] for name, case in result['cases'].items() if not case['ok']}
    assert not refused, refused
    assert len(result['cases']) == len(corpus.CASES) >= 38


def test_structure_matches_the_committed_baseline(result):
    assert corpus.structure(result) == json.loads(STRUCTURE.read_text(encoding='utf-8')), (
        'The shape of compiled graphs changed (kinds, traits, ports, configuration keys). If intended, '
        'regenerate tests/fixtures/compile-contract-structure.json with compile_contract_corpus and review the diff.')


def test_lowering_the_same_declarations_twice_gives_the_same_bytes(result):
    again = corpus.dump(result['root'])
    assert {name: case['sha256'] for name, case in again['cases'].items()} == {
        name: case['sha256'] for name, case in result['cases'].items()}


def test_coverage_is_stated_not_assumed(result):
    tables = inventory.tables()
    exercised = {operation for case in result['cases'].values() for operation in case['operations']}
    typed = {kind for kind in tables['registry.operators'] if kind[:1].isupper()}
    assert exercised <= typed
    # Typed operations no case builds yet. Extending the corpus removes names here.
    assert sorted(typed - exercised) == [
        'AgentMapOp', 'BatchMapOp', 'BoundMapOp', 'DeduplicateOp', 'ReadDocumentsOp', 'RegisterDocumentsBatchOp',
        'RegisterDocumentsOp', 'SaveLanceOp', 'SearchWebOp', 'StreamGroupBatchesOp', 'VectorSearchOp']
    kernel_kinds = {node['kernel_kind'] for case in result['cases'].values() for node in case['nodes']}
    # Every kernel kind a case produces is one the runtime admits, or a source/terminal kind.
    admitted = set(tables['runtime.kernel_kinds']) | set(tables['registry.sources']) | set(tables['registry.terminals'])
    assert kernel_kinds <= admitted, sorted(kernel_kinds - admitted)
