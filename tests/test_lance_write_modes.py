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

    from demiflow.execution import RunContext
    # Source failure precedes native prepare in the unified engine; preserve
    # the original business exception, not a fabricated publication receipt.
    from demiflow.execution.journal.run_journal import RunJournal
    with pytest.raises(RuntimeError) as raised:
        with RunContext.start(tmp_path / 'failed-run'):
            data.from_iter(broken).write_lance(
                uri, mode='overwrite', expected_version=version if expected else None)
    assert type(raised.value) is RuntimeError
    assert str(raised.value) == 'injected input failure'
    facts = RunJournal(tmp_path / 'failed-run').execution_commits('action-1')
    for fact in facts:
        assert fact['status'] == 'acked'
        prepared = fact['prepared']['sinks']['write_lance']
        assert not prepared['manifest']['final']
        assert fact['receipts']['write_lance']['data'].get('native_receipt') is None
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


@pytest.fixture(scope='module')
def native_ray_executor():
    import tempfile
    ray = pytest.importorskip('ray', reason='requires real Ray Core')
    from runtime_v3_regression_inventory import assert_private
    from demiflow.execution.executors.ray import RayDatasetExecutor
    assert_private()
    assert not ray.is_initialized(), 'test must own its cluster'
    ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
        object_store_memory=100 * 1024**2,
        _temp_dir=tempfile.mkdtemp(prefix='demiflow-lance-ray-', dir='/tmp'))
    executor = RayDatasetExecutor(workers=1)
    try:
        yield executor
    finally:
        try:
            executor.close()
        finally:
            ray.shutdown()
    assert not ray.is_initialized()


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('expected', [False, True])
@pytest.mark.parametrize('mode', ['overwrite', 'merge'])
def test_ray_direct_sink_submits_one_overwrite(tmp_path, native_ray_executor, empty, expected, mode):
    """Real Ray Core consumes the whole Dataset into one original native commit."""
    from demiflow.execution import RunContext
    uri = str(tmp_path / 'ray_sink.lance')
    lance.write_dataset(pa.table({'value': [-1]}), uri)
    version = lance.dataset(uri).version
    replacement = [] if empty else [{'value': 1}, {'value': 2}]
    with RunContext.start(tmp_path / 'run', limits={'batch_rows': 1}) as run:
        receipt = DataAPI(executor=native_ray_executor).from_items(replacement).write_lance(
            uri, mode=mode, schema=pa.schema([('value', pa.int64())]),
            expected_version=version if expected else None, return_receipt=True,
            **({'on': 'value', 'when_not_matched': 'insert'} if mode == 'merge' else {}))
        facts = run.journal.execution_commits('action-1')
        assert facts and all(fact['status'] == 'acked' for fact in facts)
        final = [fact for fact in facts if fact['prepared']['sinks']['write_lance']['manifest']['final']]
        assert len(final) == 1
        for fact in facts:
            if fact is not final[0]:
                assert fact['prepared']['sinks']['write_lance']['native'] is None
                assert fact['receipts']['write_lance']['data'].get('native_receipt') is None
        native = final[0]['receipts']['write_lance']['data']['native_receipt']
        assert native['data'] == receipt.to_dict()
        assert run.position == 1 and run.journal.execution_action('action-2') is None
    assert receipt.status == 'committed'
    assert receipt.committed_version == version + int(mode == 'overwrite' or not empty)
    expected_rows = list(replacement)
    if mode == 'merge':
        expected_rows.insert(0, {'value': -1})
    assert lance.dataset(uri).to_table().sort_by('value').to_pylist() == expected_rows
    assert lance.dataset(uri, version=version).to_table().to_pylist() == [{'value': -1}]


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
