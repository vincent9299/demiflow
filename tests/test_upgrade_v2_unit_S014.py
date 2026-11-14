"""S014 · VectorSearch.snapshot_state — bounded semantic progress, no resources."""
import asyncio
import json
import os
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import demiflow
from demiflow.lance.search import VectorSearch
from demiflow.execution.recovery_contract import document

ROOT = Path(__file__).resolve().parents[1]


def _module_source_ok():
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT), demiflow.__file__


@pytest.fixture
def table(tmp_path):
    uri = str(tmp_path / 'vectors.lance')
    schema = pa.schema([('id', pa.string()), ('embedding', pa.list_(pa.float32(), 2))])
    lance.write_dataset(pa.Table.from_pylist(
        [dict(id='a', embedding=[1., 0.]), dict(id='b', embedding=[0., 1.])],
        schema=schema), uri)
    return uri, schema


def make(uri, version=None, **kw):
    settings = dict(query='q', output='hits', uri=uri, vector_column='embedding',
                    columns=['id'], top_k=1, version=version, metric='l2', filter=None,
                    filter_column=None, storage_options=None, options=None,
                    label='search_vectors')
    settings.update(kw)
    return VectorSearch(**settings)


def test_snapshot_before_open_has_no_version(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    # No table I/O yet: the head is not pinned, so version is absent.
    assert actor.snapshot_state() == {'queries': 0, 'hits': 0}
    assert actor._dataset is None


def test_snapshot_after_queries_pins_version_and_counters(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        actor({'q': [1., 0.]})
        actor({'q': [0., 1.]})
        state = actor.snapshot_state()
        assert state == {'queries': 2, 'hits': 2, 'version': 1}
        assert state['version'] == actor._version
    finally:
        asyncio.run(actor.aclose())


def test_snapshot_explicit_version_is_recorded(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri, version=1)
    asyncio.run(actor.astart())
    try:
        actor({'q': [1., 0.]})
        assert actor.snapshot_state()['version'] == 1
    finally:
        asyncio.run(actor.aclose())


def test_snapshot_is_bounded_strict_json_without_resources(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri)
    asyncio.run(actor.astart())
    try:
        actor({'q': [1., 0.]})
        state = actor.snapshot_state()
        # Only the fixed semantic fields; never the lock, client or Arrow type.
        assert set(state) <= {'version', 'queries', 'hits'}
        assert all(type(v) is int for v in state.values())
        raw = document(state, 4096)
        assert json.loads(raw) == state
        assert '_dataset' not in state and '_lock' not in state
        assert '_vector_type' not in state and 'resources' not in state
    finally:
        asyncio.run(actor.aclose())


def test_snapshot_empty_hits_still_counts_queries(table):
    _module_source_ok()
    uri, _ = table
    actor = make(uri, filter="id = 'missing'")
    asyncio.run(actor.astart())
    try:
        row = actor({'q': [1., 0.]})
        assert row['hits'] == []
        assert actor.snapshot_state() == {'queries': 1, 'hits': 0, 'version': 1}
    finally:
        asyncio.run(actor.aclose())