"""S015 · VectorSearch.restore_state — validate identity, restore the fixed pin."""
import asyncio
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import demiflow
from demiflow.lance.search import VectorSearch
from demiflow.execution.contracts.recovery_contract import RecoveryContractError

ROOT = Path(__file__).resolve().parents[1]


def _module_source_ok():
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT), demiflow.__file__


@pytest.fixture
def table(tmp_path):
    uri = str(tmp_path / 'vectors.lance')
    schema = pa.schema([('id', pa.string()), ('embedding', pa.list_(pa.float32(), 2))])
    lance.write_dataset(pa.Table.from_pylist(
        [dict(id='a', embedding=[1., 0.])], schema=schema), uri)
    return uri, schema


def make(uri, version=None, **kw):
    settings = dict(query='q', output='hits', uri=uri, vector_column='embedding',
                    columns=['id'], top_k=1, version=version, metric='l2', filter=None,
                    filter_column=None, storage_options=None, options=None,
                    label='search_vectors')
    settings.update(kw)
    return VectorSearch(**settings)


def test_resume_reopens_pinned_version_not_new_head(table):
    _module_source_ok()
    uri, schema = table
    first = make(uri)
    asyncio.run(first.astart())
    first({'q': [1., 0.]})
    saved = first.snapshot_state()
    assert saved['version'] == 1
    asyncio.run(first.aclose())
    # External table advances to head version 2 with a nearer row 'b'.
    lance.write_dataset(pa.Table.from_pylist(
        [dict(id='b', embedding=[0., 1.])], schema=schema), uri, mode='append')
    assert lance.dataset(uri).version == 2

    resumed = make(uri)
    asyncio.run(resumed.astart())
    assert resumed._version is None  # astart reset the pin
    try:
        resumed.restore_state(saved)
        assert resumed._version == 1 and resumed._queries == 1 and resumed._hits == 1
        # Query nearest to 'b': only the pinned v1 ('a') may answer, not head v2.
        row = resumed({'q': [0., 1.]})
        assert row['hits'][0]['id'] == 'a'
        assert resumed._version == 1  # did NOT re-fetch HEAD
        assert resumed.snapshot_state() == {'queries': 2, 'hits': 2, 'version': 1}
    finally:
        asyncio.run(resumed.aclose())


def test_restore_leaves_resources_closed_for_astart(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        actor.restore_state({'queries': 3, 'hits': 5, 'version': 1})
        # restore must not open a handle or rebuild the Arrow type itself.
        assert actor._dataset is None and actor._vector_type is None
        assert actor._queries == 3 and actor._hits == 5 and actor._version == 1
    finally:
        asyncio.run(actor.aclose())


def test_restore_lazy_without_version_is_allowed(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        # Snapshot taken before any row: no pin existed, so none is restored.
        actor.restore_state({'queries': 0, 'hits': 0})
        assert actor._version is None
        actor({'q': [1., 0.]})
        assert actor._version == 1  # pins fresh head on first real use
    finally:
        asyncio.run(actor.aclose())


def test_restore_explicit_version_match_restores(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri, version=1)
    asyncio.run(actor.astart())
    try:
        actor.restore_state({'queries': 0, 'hits': 0, 'version': 1})
        assert actor._version == 1
    finally:
        asyncio.run(actor.aclose())


@pytest.mark.parametrize('state', [
    {'queries': 0, 'hits': 0},               # missing required pin
    {'queries': 0, 'hits': 0, 'version': 2}  # drifted pin
])
def test_restore_explicit_version_missing_or_drifted_refuses(table, state):
    _module_source_ok()
    uri, _ = table
    actor = make(uri, version=1)
    with pytest.raises(RecoveryContractError, match='fixed search declaration'):
        actor.restore_state(state)


@pytest.mark.parametrize('state', [
    None, [], 'x', 5,
    {'queries': 0, 'hits': 0, 'extra': 1},
    {'queries': 0, 'hits': 0, '__proto__': 1},
    {'queries': -1, 'hits': 0},
    {'queries': 0, 'hits': -2},
    {'queries': True, 'hits': 0},
    {'queries': 0, 'hits': 1.5},
    {'queries': 0, 'hits': 0, 'version': 0},
    {'queries': 0, 'hits': 0, 'version': -1},
    {'queries': 0, 'hits': 0, 'version': 1.0},
    {'queries': 0, 'hits': 0, 'version': True},
    {'hits': 0},
    {'queries': 0},
])
def test_restore_invalid_state_refuses_before_mutation(table, state):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        actor({'q': [1., 0.]})
        before = actor.snapshot_state()
        with pytest.raises(RecoveryContractError):
            actor.restore_state(state)
        # Validation precedes mutation: the live actor is unchanged on refusal.
        assert actor.snapshot_state() == before
    finally:
        asyncio.run(actor.aclose())


def test_restore_then_snapshot_round_trips(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        state = {'queries': 7, 'hits': 9, 'version': 1}
        actor.restore_state(state)
        assert actor.snapshot_state() == state
    finally:
        asyncio.run(actor.aclose())