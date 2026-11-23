import io
import json
import os

import pytest

from demiflow.execution.storage import location_index as locations
from demiflow.execution.storage import artifacts


@pytest.fixture
def index_config(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_LOCATION_CACHE',str(tmp_path/'index'))
    monkeypatch.setattr(locations,'MAX_INDEX',2*1024**2)
    monkeypatch.setattr(locations,'MAX_CACHE',4*1024**2)
    monkeypatch.setattr(locations,'CHUNK',7)
    monkeypatch.setattr(artifacts,'_LOCATION_INDEX_THRESHOLD',0)
    return tmp_path/'index'


def write(tmp_path,document):
    path=tmp_path/'_demiflow/artifact_locations.json';path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(document,ensure_ascii=False))
    return path


def test_streaming_map_preserves_unicode_escaping_and_last_key_wins():
    text='{"files":{"a":"first","a":"last","汉\\\"字":"目标","none":null},"version":1}'
    rows={}
    for item in locations._records(io.StringIO(text),'files'):
        if item is None:rows.clear()
        else:rows[item[0]]=json.loads(item[1])
    assert rows=={'a':'last','汉"字':'目标','none':None}
    duplicate='{"files":null,"files":{"old":"ignored"},"files":{"x":"y"},"version":1}'
    rows={}
    for item in locations._records(io.StringIO(duplicate),'files'):
        if item is None:rows.clear()
        else:rows[item[0]]=json.loads(item[1])
    assert rows=={'x':'y'}


def test_index_preserves_alias_priority_and_invalidates_same_mtime_size(tmp_path,index_config):
    (tmp_path/'old').write_text('old')
    (tmp_path/'one').write_text('one');(tmp_path/'two').write_text('two')
    path=write(tmp_path,dict(version=1,files={'old':'one','escape':'../outside'}))
    old=path.stat()
    assert artifacts.resolve_local_artifact(tmp_path/'old')==tmp_path/'one'
    assert artifacts.resolve_local_artifact(tmp_path/'absent')==tmp_path/'absent'
    with pytest.raises(ValueError,match='Invalid relocated'):artifacts.resolve_local_artifact(tmp_path/'escape')
    write(tmp_path,dict(version=1,files={'old':'two','escape':'../outside'}))
    os.utime(path,ns=(old.st_atime_ns,old.st_mtime_ns))
    assert path.stat().st_size==old.st_size
    assert artifacts.resolve_local_artifact(tmp_path/'old')==tmp_path/'two'


def test_fresh_index_rejects_changed_bytes_with_identical_stat_ticks(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'old':'one'}))
    original=locations.identity(path.stat())
    identify=locations.identity
    def coarse(info):
        value=identify(info)
        return original if value[:3]==original[:3] else value
    monkeypatch.setattr(locations,'identity',coarse)
    first=locations.IndexedLocations(str(path),original)
    assert first.get('old')=='one'
    write(tmp_path,dict(version=1,files={'old':'two'}))
    assert locations.identity(path.stat())==original
    with pytest.raises(ValueError,match='content changed'):
        first.get('old')
    second=locations.IndexedLocations(str(path),original)
    assert second.path!=first.path and second.get('old')=='two'


def test_second_process_object_uses_index_without_reparsing(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'a':'b'}));identity=locations.identity(path.stat())
    first=locations.IndexedLocations(str(path),identity);assert first.get('a')=='b'
    def forbidden(*a): raise AssertionError('Manifest parsed again')
    monkeypatch.setattr(locations,'_records',forbidden)
    second=locations.IndexedLocations(str(path),identity)
    assert second.get('a')=='b' and second.get('missing') is None


@pytest.mark.parametrize('text',[
    '{"files":{"a":"b",},"version":1}', '{"files":{},"version":1,}',
    '{"files":{},"version":1} garbage', '{"files":{},"version":10}',
    '{"files":{},"version":1,"files":null}', '{"files":{"a":"unterminated}',
])
def test_bad_manifest_does_not_publish_a_partial_index(tmp_path,index_config,text):
    path=write(tmp_path,{});path.write_text(text)
    mapping=locations.IndexedLocations(str(path),locations.identity(path.stat()))
    with pytest.raises(ValueError):mapping.get('a')
    assert not list(index_config.glob('*.sqlite')) and not list(index_config.glob('*.partial'))


def test_record_and_source_limits_fail_without_publishing(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'a':'x'*80}))
    monkeypatch.setattr(locations,'MAX_TOKEN',32)
    mapping=locations.IndexedLocations(str(path),locations.identity(path.stat()))
    with pytest.raises(ValueError,match='token exceeds'):mapping.get('a')
    assert not mapping.path.exists()
    monkeypatch.setattr(locations,'MAX_SOURCE',10)
    with pytest.raises(ValueError,match='exceeds 1 GiB'):
        locations.IndexedLocations(str(path),locations.identity(path.stat()))


def test_row_budget_and_changed_source_leave_no_index(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'a':'a','b':'b','c':'c'}))
    monkeypatch.setattr(locations,'MAX_RECORDS',2)
    mapping=locations.IndexedLocations(str(path),locations.identity(path.stat()))
    with pytest.raises(ValueError,match='row/time budget'):mapping.get('a')
    assert not mapping.path.exists()
    monkeypatch.setattr(locations,'MAX_RECORDS',100)
    stale=locations.IndexedLocations(str(path),locations.identity(path.stat()))
    path.write_text('{"files":{},"version":1}')
    with pytest.raises(ValueError,match='changed before indexing'):stale.get('a')


def test_cache_pruning_and_rebuild_never_delete_unowned_files(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'a':'b'}))
    first=locations.IndexedLocations(str(path),locations.identity(path.stat()));assert first.get('a')=='b'
    first.path.write_bytes(b'bad derivative')
    unrelated=index_config/'user.partial';unrelated.write_text('keep')
    second=locations.IndexedLocations(str(path),locations.identity(path.stat()));assert second.get('a')=='b'
    assert unrelated.read_text()=='keep'


def test_lookup_closes_every_connection_including_missing_keys(tmp_path,index_config,monkeypatch):
    path=write(tmp_path,dict(version=1,files={'a':'b'}))
    opened=[];connect=locations.sqlite3.connect
    class Tracked(locations.sqlite3.Connection):
        def __init__(self,*a,**kw):
            super().__init__(*a,**kw);self.closed=False;opened.append(self)
        def close(self): self.closed=True;super().close()
    monkeypatch.setattr(locations.sqlite3,'connect',lambda *a,**kw:connect(*a,**kw,factory=Tracked))
    mapping=locations.IndexedLocations(str(path),locations.identity(path.stat()))
    assert mapping.get('a')=='b'
    for i in range(20):assert mapping.get('absent'+str(i)) is None
    assert opened and all(connection.closed for connection in opened)
