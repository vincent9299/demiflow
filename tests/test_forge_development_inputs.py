"""Input exploration is independent of project writes, control files and registration."""
import hashlib
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.inputs import InputAccess


@pytest.fixture
def access(tmp_path):
    project = tmp_path / 'code'
    project.mkdir()
    private = tmp_path / 'control'
    private.mkdir(mode=0o700)
    (private / 'state.json').write_text('controller-secret')
    scratch = tmp_path / 'scratch'
    scratch.mkdir(mode=0o700)
    turn = dict(epoch='original', created=time.time(), stop_at=None, cleanup='pending',
                development_control=None)
    store = SimpleNamespace(limits=SimpleNamespace(turn_seconds=120,
        rss_bytes=1024**3, processes=32), turn=lambda tid: dict(turn))
    from demiflow.workbench.conversations.execution import runtime_paths
    api = InputAccess(store, 'turn', dict(root=str(project)), scratch,
        denied_paths=[private], runtime_paths=runtime_paths(dict(executable=sys.executable)))
    return api, tmp_path, project, private, turn


def test_discovers_inputs_without_path_or_registration_and_reads_other_output(access):
    api, root, project, _, _ = access
    external = root / 'upstream' / '.forge' / 'runs' / 'previous' / 'outputs'
    external.mkdir(parents=True)
    data = external / 'records.jsonl'
    data.write_text('{"text":"中文"}\n', encoding='utf-8')
    initial = api.invoke({})
    assert initial['source']['path'] == str(root)
    assert str(root / 'upstream') in {e['path'] for e in initial['entries']}
    found = api.invoke(dict(action='find', path=str(root / 'upstream'), pattern='*.jsonl', depth=4))
    assert found['entries'][0]['path'] == str(data)
    read = api.invoke(dict(action='read', path=str(data)))
    assert read['text'] == data.read_text()
    assert read['readonly'] and not read['truncated']
    assert read['source']['path'] == str(data)
    assert list(project.iterdir()) == []


def test_control_roots_devices_credentials_and_aliases_never_become_inputs(access):
    api, root, _, private, _ = access
    ordinary = root / 'ordinary.txt'
    ordinary.write_text('safe input')
    linked = root / 'alias'
    linked.symlink_to(private, target_is_directory=True)
    hardlink = root / 'disguised.txt'
    os.link(private / 'state.json', hardlink)
    fifo = root / 'stream'
    os.mkfifo(fifo)
    creds = root / '.ssh'
    creds.mkdir()
    (creds / 'id_rsa').write_text('SSH secret')
    web = root / 'demiflow-combined-preview-fixture'
    web.mkdir(mode=0o700)
    (web / 'ready.json').write_text('url with session token')
    (root / 'access.token').write_text('another session token')
    for path in (private / 'state.json', linked / 'state.json', hardlink, fifo,
                 creds / 'id_rsa', web / 'ready.json', root / 'access.token', Path('/proc/self/environ')):
        with pytest.raises(ForgeError):
            api.invoke(dict(action='read', path=str(path)))
    visible = api.invoke(dict(action='find', path=str(root), pattern='*', depth=4))
    assert str(ordinary) in {e['path'] for e in visible['entries']}
    assert all('secret' not in str(e) and e['path'] not in {str(private), str(linked),
        str(hardlink), str(fifo), str(creds), str(web), str(root / 'access.token')} for e in visible['entries'])


def test_text_paging_is_bounded_and_preserves_utf8(access):
    api, root, _, _, _ = access
    path = root / 'text.txt'
    content = ('中abc' * 3000) + '\n'
    path.write_text(content)
    offset, pages = 0, []
    while True:
        page = api.invoke(dict(action='read', path=str(path), offset=offset, bytes=1024))
        assert page['bytes'] <= 1024
        pages.append(page['text'])
        if not page['truncated']:
            break
        assert page['next_offset'] > offset
        offset = page['next_offset']
    assert ''.join(pages) == content
    with pytest.raises(ForgeError, match='UTF-8 character'):
        api.invoke(dict(action='read', path=str(path), bytes=1))
    with pytest.raises(ForgeError):
        api.invoke(dict(action='read', path=str(path), bytes=1000000))
    path.write_bytes(b'\x00binary')
    with pytest.raises(ForgeError):
        api.invoke(dict(action='read', path=str(path)))


def test_discovery_has_truthful_page_and_scan_bounds(access):
    api, root, _, _, _ = access
    directory = root / 'data'
    directory.mkdir()
    for i in range(150):
        (directory / f'{i}.csv').write_text('a,b\n1,2\n')
    page1 = api.invoke(dict(action='list', path=str(directory), limit=80))
    page2 = api.invoke(dict(action='list', path=str(directory), limit=80, offset=page1['next_offset']))
    assert page1['truncated'] and page1['next_offset'] == 80
    assert not page2['truncated']
    assert len({entry['path'] for entry in page1['entries'] + page2['entries']}) == 150
    result = api.invoke(dict(action='find', path=str(directory), pattern='*.missing', depth=0))
    assert result['entries'] == [] and result['scanned'] == 150
    with pytest.raises(ForgeError):
        api.invoke(dict(action='find', path=str(directory), depth=200))


@pytest.mark.parametrize('change', [dict(stop_at=1), dict(cleanup='complete'), dict(epoch='replacement')])
def test_old_turns_cannot_read_even_ordinary_inputs(access, change):
    api, root, _, _, turn = access
    path = root / 'text.txt'
    path.write_text('business input')
    turn.update(change)
    with pytest.raises(ForgeError, match='inactive'):
        api.invoke(dict(action='read', path=str(path)))


def test_no_command_expression_or_runtime_authority_in_arguments(access):
    api, root, _, _, _ = access
    for request in (dict(action='shell', command='id'), dict(action='read', path=str(root), command='id'),
                    dict(action='inspect', path=str(root), runtime_paths=['/']),
                    dict(action='find', path=str(root), pattern='../*')):
        with pytest.raises(ForgeError):
            api.invoke(request)


def _snapshot(root):
    return {str(p.relative_to(root)): (p.stat().st_mode, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in root.rglob('*') if p.is_file()}


@pytest.mark.parametrize('format_', ['lance', 'parquet'])
def test_real_confined_native_external_schema_sample_without_writes(access, format_):
    import pyarrow as pa
    api, root, project, private, _ = access
    external = root / 'other-pipeline' / '.forge' / 'runs' / 'upstream' / 'outputs'
    external.mkdir(parents=True)
    path = external / ('records.lance' if format_ == 'lance' else 'records.parquet')
    table = pa.table({'id': list(range(12)), 'text': ['中文 input'] * 12, 'hidden': ['not projected'] * 12})
    if format_ == 'lance':
        import lance
        lance.write_dataset(table, str(path))
    else:
        import pyarrow.parquet as pq
        pq.write_table(table, path)
    before = _snapshot(root)
    result = api.invoke(dict(action='inspect', path=str(path), columns=['id', 'text'], limit=3))
    assert result['source']['path'] == str(path)
    assert result['readonly'] and result['row_count'] == 12
    assert {field['name'] for field in result['schema']} == {'id', 'text', 'hidden'}
    assert len(result['sample']) == 3 and result['sample'][0]['text'] == '中文 input'
    assert 'hidden' not in result['sample'][0]
    assert result['sample_truncated']
    assert _snapshot(root) == before
    assert list(project.iterdir()) == []
    assert (private / 'state.json').read_text() == 'controller-secret'


def test_wide_native_samples_fail_bounded_but_schema_remains_available(access):
    import pyarrow as pa
    import pyarrow.parquet as pq
    api, root, _, _, _ = access
    path = root / 'wide.parquet'
    pq.write_table(pa.table({'text': ['x' * 100000]}), path)
    with pytest.raises(ForgeError, match='format/resource'):
        api.invoke(dict(action='inspect', path=str(path), limit=1))
    result = api.invoke(dict(action='inspect', path=str(path), limit=0))
    assert result['row_count'] == 1 and result['sample'] == []
    assert result['schema'][0]['name'] == 'text'


def test_many_schema_fields_are_reported_as_partial(access):
    import pyarrow as pa
    import pyarrow.parquet as pq
    api, root, _, _, _ = access
    path = root / 'many-columns.parquet'
    pq.write_table(pa.table({f'column{i}': [i] for i in range(100)}), path)
    result = api.invoke(dict(action='inspect', path=str(path), limit=0))
    assert len(result['schema']) == 64 and result['schema_truncated']


@pytest.mark.parametrize('alias_kind', ['manifest_hardlink', 'fragment_hardlink', 'fragment_symlink'])
def test_native_lance_rejects_aliases_before_returning_data(access, alias_kind):
    import lance
    import pyarrow as pa
    api, root, _, _, _ = access
    path = root / 'linked.lance'
    lance.write_dataset(pa.table({'text': ['input']}), str(path))
    target = next((path / ('_versions' if alias_kind == 'manifest_hardlink' else 'data')).iterdir())
    copy = root / 'hidden-copy'
    copy.write_bytes(target.read_bytes())
    target.unlink()
    if alias_kind == 'fragment_symlink':
        target.symlink_to(copy)
    else:
        os.link(copy, target)
    with pytest.raises(ForgeError, match='format/resource'):
        api.invoke(dict(action='inspect', path=str(path), limit=1))


@pytest.mark.parametrize('stop', [False, True])
def test_native_timeout_or_stop_kills_its_owned_process_and_removes_private_home(access, monkeypatch, stop):
    from demiflow.workbench.conversations import inputs, sandbox
    api, root, _, _, turn = access
    path = root / 'waiting.parquet'
    path.write_bytes(b'not read by the fixed test sleeper')
    launch = sandbox.launch_sandbox
    observed = []

    def slow(command, **kwargs):
        process = launch([sys.executable, '-I', '-B', '-c', 'import time; time.sleep(30)'], **kwargs)
        observed.append((process, Path(kwargs['spec'].private_home)))
        if stop:
            turn['stop_at'] = time.time()
        return process

    monkeypatch.setattr(sandbox, 'launch_sandbox', slow)
    monkeypatch.setattr(inputs, '_INSPECTION_SECONDS', .5)
    started = time.monotonic()
    with pytest.raises(ForgeError, match='inactive|time budget|format/resource'):
        api.invoke(dict(action='inspect', path=str(path), limit=1))
    assert time.monotonic() - started < 3
    assert len(observed) == 1
    process, private = observed[0]
    assert process.poll() is not None and not private.exists()


def test_missing_path_reports_not_found_and_mid_utf8_offset_aligns(access):
    api, root, _, _, _ = access
    with pytest.raises(ForgeError) as exc_info:
        api.invoke(dict(action='list', path=str(root / 'does_not_exist')))
    assert exc_info.value.code == 'not_found'

    zh = root / 'zh.md'
    zh.write_text('中文测试内容' * 20, encoding='utf-8')
    # Offset 1 lands in the middle of the first 3-byte UTF-8 character '中'.
    page = api.invoke(dict(action='read', path=str(zh), offset=1, bytes=64))
    assert page['offset'] == 3
    assert page['text'].startswith('文测试')
