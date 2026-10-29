import asyncio
import fcntl
import multiprocessing
from pathlib import Path
import time
import pytest
from demiflow import data
from demiflow.data.result_cache import CachedMap


def _competing_process(directory, ready, start, output, concept):
    from demiflow.data.result_cache import CachedSyncMap
    def compute(url):
        with (Path(directory)/'calls').open('a') as stream:
            stream.write(url+'\n')
        time.sleep(.1)
        return {'url':url}
    actor = CachedSyncMap(compute,directory,'v1',inputs=['url'],output='answer')
    ready.put(True)
    if not start.wait(15):
        raise TimeoutError('test start barrier')
    output.put(actor({'url':'shared','concept':concept}))


def test_local_processes_share_one_completed_result_and_keep_current_rows(tmp_path):
    context = multiprocessing.get_context('spawn')
    ready, output, start = context.Queue(), context.Queue(), context.Event()
    processes = [context.Process(target=_competing_process,args=(str(tmp_path),ready,start,output,name))
                 for name in ('first','second')]
    try:
        for process in processes: process.start()
        for _ in processes: assert ready.get(timeout=20)
        start.set()
        rows = [output.get(timeout=20) for _ in processes]
        for process in processes:
            process.join(timeout=20)
            assert process.exitcode == 0
        assert (tmp_path/'calls').read_text() == 'shared\n'
        assert {row['concept'] for row in rows} == {'first','second'}
        assert all(row['answer'] == {'url':'shared'} for row in rows)
    finally:
        for process in processes:
            if process.is_alive(): process.terminate()
            process.join(timeout=5)
        ready.close(); output.close()


def test_unbound_async_none_and_list_keep_map_semantics(tmp_path):
    calls = []
    async def compute(row):
        calls.append(row['n'])
        return None if row['n']==0 else [{'answer':1},{'answer':2}]
    def run():
        return data.from_items([{'n':0},{'n':1}]).map_cached(compute,cache_dir=tmp_path,version='v1').materialize().take(4)
    assert run() == run() == [{'answer':1},{'answer':2}]
    assert calls == [0,1]


def test_legacy_cache_requires_explicit_cutover_without_eager_io(tmp_path):
    (tmp_path/'ab').mkdir()
    actor = CachedMap(lambda row:row,tmp_path,'v1')
    with pytest.raises(ValueError,match='Legacy map_cached'):
        asyncio.run(actor({'a':1}))


def test_relative_cache_and_lock_paths_are_pinned_at_declaration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    actor = CachedMap(lambda row:row,'cache','v1')
    other = tmp_path/'other'; other.mkdir()
    monkeypatch.chdir(other)
    assert asyncio.run(actor({'a':1})) == {'a':1}
    assert actor.directory == tmp_path/'cache'
    assert (tmp_path/'cache/results.sqlite').is_file() and not (other/'cache').exists()


@pytest.mark.parametrize('synchronous', [False, True])
@pytest.mark.parametrize('answer', [None, [1, 2], {'n': 3}])
def test_only_computation_result_is_shared(tmp_path, synchronous, answer):
    calls = []
    def compute(url):
        calls.append(url)
        return answer
    rows = data.from_items([{'url':'same', 'concept':'first'}, {'url':'same','concept':'second'}]).map_cached(
        compute, cache_dir=tmp_path, version='v1', inputs=['url'], output='answer', synchronous=synchronous)
    result = (rows if synchronous else rows.materialize()).take(4)
    assert calls == ['same']
    assert [r['concept'] for r in result] == ['first', 'second']
    assert all(r['answer'] == answer for r in result)


def test_shared_wait_cancellation_and_actor_hooks(tmp_path):
    class Compute:
        concurrency = 2
        def __init__(self):
            self.calls = 0; self.events = []
        async def astart(self): self.events.append('start')
        async def astop(self): self.events.append('stop')
        async def aclose(self): self.events.append('close')
        async def __call__(self, url):
            self.calls += 1
            await asyncio.sleep(.03)
            return url
    compute = Compute()
    rows = data.from_items([{'url':'same'}]*4).map_cached(compute, cache_dir=tmp_path,
        version='v1', inputs=['url'], output='answer', queue_depth=1).materialize().take(5)
    assert len(rows) == 4 and compute.calls == 1
    assert compute.events == ['start', 'stop', 'close']
    actor = CachedMap(compute, tmp_path, 'v2', inputs=['url'], output='answer', wait_timeout_s=.03)
    _, key = actor.prepare({'url':'locked'})
    async def run():
        with actor.lock(key) as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            with pytest.raises(TimeoutError, match='lock wait'):
                await actor({'url':'locked'})
            task = asyncio.create_task(actor({'url':'locked'}))
            await asyncio.sleep(.005); task.cancel()
            with pytest.raises(asyncio.CancelledError): await task
        assert (await actor({'url':'locked'}))['answer'] == 'locked'
    asyncio.run(run())


def test_configuration_and_limits_are_cache_identity(tmp_path):
    class Compute:
        def __init__(self, value): self.cache_identity = {'value':value}
        def __call__(self, url): return self.cache_identity['value']
    def run(value):
        return data.from_items([{'url':'same'}]).map_cached(Compute(value), cache_dir=tmp_path,
            version='v1', inputs=['url'], output='answer', synchronous=True).take(1)[0]['answer']
    assert run(1) == 1 and run(2) == 2
    with pytest.raises(ValueError, match='max_value_bytes'):
        data.from_items([{'text':'x'*100}]).map_cached(lambda text:text, cache_dir=tmp_path,
            version='wide', inputs=['text'], output='answer', synchronous=True, max_input_bytes=16).take(1)
