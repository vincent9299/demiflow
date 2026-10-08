"""验证通用写入模式、空表及并发提交；全部数据位于 pytest 临时目录。"""
import pytest

pa = pytest.importorskip('pyarrow')
lance = pytest.importorskip('lance')

from demiflow.errors import InvalidLanceRequest, LanceWriteConflict, LanceWriteError
from demiflow.lance.model import LanceWriteSpec
from demiflow.data.api import DataAPI


def test_overwrite_is_one_new_snapshot_across_batches(tmp_path):
    """多批输入只提交一次覆盖，并保留可读取的历史版本。"""
    uri = str(tmp_path / 'data.lance')
    data = DataAPI(block_size=1)
    data.from_items([{'old': 1}]).write_lance(uri)
    old_version = lance.dataset(uri).version
    replacement = [{'new': str(i)} for i in range(5)]
    assert data.from_items(replacement).write_lance(uri, mode='overwrite') is None
    current = lance.dataset(uri)
    assert current.version == old_version + 1
    assert current.to_table().to_pylist() == replacement
    assert data.read_lance(uri, version=old_version).take_all() == [{'old': 1}]
    data.from_items([{'new': 'appended'}]).write_lance(uri)
    assert lance.dataset(uri).count_rows() == 6


@pytest.mark.parametrize('mode', ['append', 'overwrite'])
@pytest.mark.parametrize('exists', [False, True])
def test_empty_write_requires_explicit_schema(tmp_path, mode, exists):
    """显式 schema 允许空输入；追加保留旧行，覆盖清空当前版本。"""
    uri = str(tmp_path / 'empty.lance')
    schema = pa.schema([('value', pa.string())])
    data = DataAPI()
    if exists:
        data.from_items([{'value': 'old'}]).write_lance(uri)
    data.from_items([]).write_lance(uri, mode=mode, schema=schema)
    current = lance.dataset(uri)
    assert current.schema == schema
    assert current.count_rows() == int(exists and mode == 'append')


def test_explicit_schema_preserves_nullable_nested_fields(tmp_path):
    """空列表、全 null 列与后续非空批次使用同一个明确类型。"""
    uri = str(tmp_path / 'typed.lance')
    schema = pa.schema([('values', pa.list_(pa.string())), ('note', pa.large_string())])
    rows = [{'values': [], 'note': None}, {'values': ['a'], 'note': 'text'}]
    DataAPI(block_size=1).from_items(rows).write_lance(uri, mode='overwrite', schema=schema)
    assert lance.dataset(uri).schema == schema
    assert lance.dataset(uri).to_table().to_pylist() == rows


@pytest.mark.parametrize('expected', [False, True])
def test_overwrite_failure_preserves_current_version(tmp_path, expected):
    """输入中途失败时，不能先清空旧表或提交部分新数据。"""
    uri = str(tmp_path / 'failed.lance')
    data = DataAPI(block_size=1)
    data.from_items([{'value': 1}]).write_lance(uri)
    version = lance.dataset(uri).version

    def broken():
        yield {'value': 2}
        raise RuntimeError('injected input failure')

    with pytest.raises(LanceWriteError):
        data.from_iter(broken).write_lance(
            uri, mode='overwrite', expected_version=version if expected else None)
    assert lance.dataset(uri).version == version
    assert lance.dataset(uri).to_table().to_pylist() == [{'value': 1}]


@pytest.mark.parametrize('empty', [False, True])
def test_overwrite_expected_version_supports_new_schema_and_empty(tmp_path, empty):
    """带版本约束的覆盖可以替换 schema，也可以提交空表。"""
    uri = str(tmp_path / 'cas.lance')
    data = DataAPI()
    data.from_items([{'old': 1}]).write_lance(uri)
    version = lance.dataset(uri).version
    schema = pa.schema([('new', pa.string())])
    rows = [] if empty else [{'new': 'replacement'}]
    data.from_items(rows).write_lance(uri, mode='overwrite', schema=schema, expected_version=version)
    assert lance.dataset(uri).version == version + 1
    assert lance.dataset(uri).to_table().to_pylist() == rows
    assert lance.dataset(uri).schema == schema


def test_stale_version_and_invalid_mode_do_not_consume_input(tmp_path):
    uri = str(tmp_path / 'stale.lance')
    data = DataAPI()
    data.from_items([{'value': 1}]).write_lance(uri)
    stale = lance.dataset(uri).version
    data.from_items([{'value': 2}]).write_lance(uri)

    def unused():
        pytest.fail('write validation must precede input execution')
        yield

    with pytest.raises(LanceWriteConflict):
        data.from_iter(unused).write_lance(uri, mode='overwrite', expected_version=stale)
    with pytest.raises(InvalidLanceRequest, match='mode'):
        data.from_iter(unused).write_lance(uri, mode='upsert')
    with pytest.raises(InvalidLanceRequest, match='schema'):
        data.from_items([]).write_lance(uri, mode='overwrite')
    assert lance.dataset(uri).count_rows() == 2


def test_concurrent_commit_is_not_lost_during_overwrite(tmp_path, monkeypatch):
    """在版本预检之后插入一次竞争提交，覆盖仍必须原子拒绝。"""
    uri = str(tmp_path / 'race.lance')
    data = DataAPI()
    data.from_items([{'value': 1}]).write_lance(uri)
    expected = lance.dataset(uri).version
    original = lance.fragment.write_fragments

    def write_then_append(*args, **kwargs):
        fragments = original(*args, **kwargs)
        lance.write_dataset(pa.table({'value': [2]}), uri, mode='append')
        return fragments

    monkeypatch.setattr(lance.fragment, 'write_fragments', write_then_append)
    with pytest.raises(LanceWriteConflict):
        data.from_items([{'value': 3}]).write_lance(uri, mode='overwrite', expected_version=expected)
    assert lance.dataset(uri).version == expected + 1
    assert lance.dataset(uri).to_table().to_pylist() == [{'value': 1}, {'value': 2}]


def test_mode_and_schema_are_part_of_write_identity(tmp_path):
    uri = str(tmp_path / 'identity.lance')
    default = LanceWriteSpec(uri)
    assert default == LanceWriteSpec(uri, mode='append')
    assert default.to_dict()['kind'] == 'append'
    assert default.content_hash != LanceWriteSpec(uri, mode='overwrite').content_hash
    assert default.content_hash != LanceWriteSpec(uri, schema=pa.schema([('x', pa.int64())])).content_hash


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('expected', [False, True])
@pytest.mark.parametrize('mode', ['overwrite', 'merge'])
def test_ray_direct_sink_submits_one_overwrite(tmp_path, monkeypatch, empty, expected, mode):
    """只模拟 Ray 的调度边界，真实验证 sink 对多批/空输入的一次 Lance 提交。"""
    import importlib.util
    import sys
    from pathlib import Path
    from types import ModuleType, SimpleNamespace
    import demiflow

    for name in ('ray', 'ray.data', 'ray.data.datasource'):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    sys.modules['ray.data.datasource'].Datasink = object
    path = Path(demiflow.__file__).parent / 'execution/executors/ray.py'
    module_spec = importlib.util.spec_from_file_location('demiflow.execution.executors._ray_fixture', path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    uri = str(tmp_path / 'ray_sink.lance')
    DataAPI().from_items([{'value': -1}]).write_lance(uri)
    version = lance.dataset(uri).version
    spec = LanceWriteSpec(uri, mode=mode, schema=pa.schema([('value', pa.int64())]),
                          expected_version=version if expected else None,
                          **({'on': 'value', 'when_not_matched': 'insert'} if mode == 'merge' else {}))
    owner = module._RayLanceDirectWriteSink(spec)
    adapter = owner.ray_datasink()
    assert adapter.supports_distributed_writes is False
    returns = [] if empty else [adapter.write(iter([pa.table({'value': [1]}), pa.table({'value': [2]})]), None)]
    adapter.on_write_complete(SimpleNamespace(write_returns=returns))
    assert owner.receipt.status == 'committed'
    assert owner.receipt.committed_version == version + int(mode == 'overwrite' or not empty)
    expected_rows = ([] if empty else [{'value': 1}, {'value': 2}])
    if mode == 'merge':
        expected_rows.insert(0, {'value': -1})
    assert lance.dataset(uri).to_table().sort_by('value').to_pylist() == expected_rows

    # 即便给定 expected_version，覆盖仍走单 writer，而不是分片追加提交。
    executor = object.__new__(module.RayDatasetExecutor)
    selected = []
    monkeypatch.setattr(executor, '_write_lance_direct', lambda *args, **kwargs: selected.append(args[2]))
    executor.write_lance(None, None, spec)
    assert selected == [spec]


def test_create_mode_never_overwrites_an_existing_or_racing_table(tmp_path,monkeypatch):
    from demiflow import data
    from demiflow.errors import LanceWriteError,InvalidLanceRequest
    uri=str(tmp_path/'create.lance')
    original=lance.write_dataset
    raced=[False]
    def race(reader,path,**kwargs):
        if str(path)==uri and not raced[0]:
            raced[0]=True
            original(pa.table({'id':[99]}),uri)
        return original(reader,path,**kwargs)
    monkeypatch.setattr(lance,'write_dataset',race)
    with pytest.raises(LanceWriteError): data.from_items([{'id':1}]).write_lance(uri,mode='create')
    assert lance.dataset(uri).version==1
    assert lance.dataset(uri).to_table()['id'].to_pylist()==[99]
    with pytest.raises(InvalidLanceRequest): data.from_items([{'id':1}]).write_lance(uri,mode='create',expected_version=1)
