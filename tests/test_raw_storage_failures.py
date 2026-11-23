"""A short file write cannot become an apparently complete prepared object."""
import errno
from pathlib import Path
import sqlite3

import pytest
from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.raw_objects import RawWriter
from demiflow.execution.pipeline.run_context import RunContext
from test_raw_fetch import origin_server, BODY


class ShortFile:
    def __init__(self, file, failure):
        self.file,self.failure,self.calls=file,failure,0

    def __getattr__(self, name):
        return getattr(self.file,name)

    def write(self, value):
        self.calls+=1
        if self.calls==2 and self.failure=='zero':return 0
        if self.calls==2 and self.failure=='full':raise OSError(errno.ENOSPC,'fixture full')
        return self.file.write(value[:min(4096,len(value))])


@pytest.mark.parametrize('failure',[None,'zero','full'])
def test_partial_writes_complete_or_fail_before_prepare(tmp_path,monkeypatch,failure):
    original=RawWriter.__init__
    def initialize(self,*args,**kwargs):
        original(self,*args,**kwargs)
        self.file=ShortFile(self.file,failure)
    monkeypatch.setattr(RawWriter,'__init__',initialize)
    with origin_server() as (url,calls):
        try:
            with RunContext.start(tmp_path/'run'):
                result=data.from_items([{'request':{'uri':url+'/file'}}]).fetch(
                    store=LocalObjectStore(tmp_path/'objects'),concurrency=1,
                    policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True))).materialize()
        except OSError as exc:
            assert failure is not None and exc.errno==(errno.EIO if failure=='zero' else errno.ENOSPC)
            assert not list((tmp_path/'objects').glob('**/.pending/*'))
            with sqlite3.connect(tmp_path/'run/run.sqlite') as db:
                assert db.execute("SELECT count(*) FROM runtime_commits WHERE status='prepared'").fetchone()[0]==0
        else:
            assert failure is None
            row,=result.take_all()
            from urllib.parse import urlsplit
            assert Path(urlsplit(row['result']['raw_ref']['uri']).path).read_bytes()==BODY
        assert calls['/file']==1
