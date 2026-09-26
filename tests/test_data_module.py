"""Public data readers compose with Dataset and inherit driver execution only."""
import inspect
from pathlib import Path
from typing import get_type_hints

import pyarrow as pa
import pytest

from demiflow import data, Dataset
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.pipeline.core import Pipeline, PipelineExecution
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server


def test_public_readers_match_the_existing_reader_contract():
    names = [name for name in vars(DataAPI) if name.startswith(('read_', 'from_'))]
    names += ['range', 'vector_search_lance']
    for name in names:
        public = getattr(data, name)
        original = list(inspect.signature(getattr(DataAPI, name)).parameters.values())[1:]
        assert list(inspect.signature(public).parameters.values()) == original
        get_type_hints(public)
    assert not hasattr(data, 'DataAPI')
    assert not hasattr(__import__('demiflow'), 'DataAPI')


def test_module_readers_join_and_write_configured_lance_versions(tmp_path):
    article_uri, visual_uri, target_uri = [str(tmp_path / name) for name in ('articles', 'visuals', 'output')]
    articles = [{'concept': 'A', 'text': 'old'}, {'concept': 'B', 'text': 'other'}]
    data.from_items(articles).write_lance(article_uri, mode='overwrite')
    data.from_items([{'concept': 'A', 'text': 'new'}]).write_lance(article_uri, mode='overwrite')
    data.from_items([{'concept': 'A', 'image': 'figure'}]).write_lance(visual_uri)
    result = (data.read_lance(article_uri, version=1, filter="concept = 'A'")
              .join(data.read_lance(visual_uri, version=1), on='concept')
              .map(lambda row: {**row, 'checked': True}).materialize())
    assert isinstance(result, Dataset)
    result.write_lance(target_uri, mode='overwrite')
    result.write_lance(target_uri, mode='append')
    assert data.read_lance(target_uri).take_all() == [
        {'concept': 'A', 'text': 'old', 'image': 'figure', 'checked': True},
    ] * 2
    schema = pa.schema([('concept', pa.string())])
    data.from_arrow(pa.Table.from_pylist([], schema=schema)).write_lance(target_uri, mode='overwrite', schema=schema)
    assert data.read_lance(target_uri).take_all() == []


def test_driver_module_readers_use_selected_executor_and_reset_after_failure(tmp_path):
    executor = LocalDatasetExecutor()
    saved = []
    class Program:
        def run(self, ctx):
            ds = data.from_items([{'x': 1}])
            assert ds._executor is executor
            assert ctx.data.read_lance(str(tmp_path / 'unused.lance'))._executor is executor
            saved.append(ds)
            raise RuntimeError('intentional test failure')
    pipeline = Pipeline('test', 'test', Program(), PipelineExecution('portable'), resource_root=tmp_path)
    try:
        with pytest.raises(RuntimeError, match='intentional'):
            pipeline.run(dataset_executor=executor)
        assert saved[0].take_all() == [{'x': 1}]
        assert data.from_items([])._executor is not executor
        assert executor.resource_root == tmp_path
    finally:
        executor.close()


def test_module_prompt_chain_binds_independent_node_configuration(server):
    rows = (data.from_items([{'item': 1}])
        .map_prompt_async('enrich', config=parse_prompt_pack(PACK),
            options={'request_options': {'max_tokens': 11}}, max_requests=1,
            inputs={'payload': 'item'}, output='first')
        .map_prompt_async('enrich', config=parse_prompt_pack(PACK),
            options={'request_options': {'max_tokens': 22}}, max_requests=1,
            inputs={'payload': 'first'}, output='second')
        .materialize().take_all())
    assert rows == [{'item': 1, 'first': 'ok', 'second': 'ok'}]
    assert [request['body']['max_tokens'] for request in server['requests']] == [11, 22]
