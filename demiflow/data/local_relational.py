"""本地、有界落盘的关系算子；缓存/checkpoint 沿用独立的持久化协议。

小右表使用有界哈希索引，大表使用稳定外部归并；重复右键只在组超过
预算时落盘。排序载荷只编码一次，按块读写。join 的键序可供紧邻的分组
复用，任意 Python 变换之后不假设键或顺序不变。reducer 自行控制状态大小。
"""
import asyncio
import fcntl
import hashlib
import heapq
import inspect
import itertools
import json
import os
import pickle
import struct
import tempfile
import uuid
import multiprocessing
import logging
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import ExitStack
from pathlib import Path


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def keys(value):return [value] if isinstance(value,str) else list(value)
def key_of(row,fields):return canonical([row[k] for k in fields])


# 有界块避免每行重新建立 Unpickler、执行文件 read/readline；不保留跨块 memo。
_IO_BYTES = 1024 * 1024
_BLOCK_BYTES = 256 * 1024
_MERGE_FAN_IN = 32


def _write_records(path, records):
    """临时排序文件：批量写 (canonical key, opaque row bytes)，不解码载荷。"""
    with Path(path).open('wb', buffering=_IO_BYTES) as stream:
        batch = []
        size = 0
        for record in records:
            batch.append(record)
            size += len(record[0]) * 4 + len(record[1]) + 128
            if size >= _BLOCK_BYTES:
                pickle.dump(batch, stream, protocol=pickle.HIGHEST_PROTOCOL)
                batch = []
                size = 0
        if batch:
            pickle.dump(batch, stream, protocol=pickle.HIGHEST_PROTOCOL)


def _read_records(path):
    with Path(path).open('rb', buffering=_IO_BYTES) as stream:
        while True:
            try:
                batch = pickle.load(stream)
            except EOFError:
                return
            yield from batch


def _merge_records(paths):
    # 下游提前终止或抛错时也关闭每个文件，不能依赖 GC 释放描述符。
    with ExitStack() as stack:
        readers = [_read_records(p) for p in paths]
        for reader in readers:
            stack.callback(reader.close)
        yield from heapq.merge(*readers, key=lambda item: item[0])


def _sort_run(path, chunk):
    """工作进程只接收已编码的排序块，不执行或序列化用户回调。"""
    chunk.sort(key=lambda item: item[0])
    _write_records(path, chunk)


def _merge_run(path, sources):
    _write_records(path, _merge_records(sources))


def sorted_rows(rows, fields, directory, chunk_bytes, *, workers=1):
    """稳定排序；小输入留内存，大输入按预算分块且最多打开固定数量文件。"""
    if chunk_bytes < 1:
        raise ValueError('chunk_bytes must be positive')
    # 小块不值得启动进程。每个算子遵守已有 local workers 上限，待处理
    # 块也受此数约束；spawn 避免在 Lance/Arrow 多线程进程中使用 fork。
    parallel = workers > 1 and chunk_bytes >= 8 * 1024 * 1024
    with ExitStack() as stack:
        yield from _sort_rows(rows, fields, directory, chunk_bytes, workers if parallel else 1, stack)


def _sort_rows(rows, fields, directory, chunk_bytes, workers, stack):
    paths = []
    chunk = []
    size = 0
    pool = None
    pending = deque()
    payload_file = None

    def finish_job(job):
        nonlocal pool, workers
        future, fn, args = job
        try:
            future.result()
        except BrokenProcessPool:
            # Only pure sorting/merging is replayed, never the input iterator or
            # user callbacks. Keep the bounded encoded chunks until completion.
            if pool is not None:
                pool.shutdown(wait=True, cancel_futures=True)
                pool = None
                workers = 1
                logging.getLogger(__name__).warning('Sort process pool failed; continuing in this process')
            fn(*args)

    def submit(fn, *args):
        nonlocal pool, workers
        if pool is None:
            fn(*args)
            return
        try:
            pending.append((pool.submit(fn, *args), fn, args))
        except BrokenProcessPool:
            pool.shutdown(wait=True, cancel_futures=True)
            pool = None
            workers = 1
            fn(*args)

    def encode(row):
        nonlocal payload_file
        payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        if len(payload) <= max(1, min(chunk_bytes, _BLOCK_BYTES)):
            return payload
        # Wide rows are written once; merge passes carry only a file offset.
        # Protocol-HIGHEST pickles begin with 0x80, so 0x00 is unambiguous.
        if payload_file is None:
            payload_file = stack.enter_context(open(Path(directory) / 'payloads.pickle', 'w+b'))
        offset = payload_file.tell()
        payload_file.write(payload)
        return b'\x00' + struct.pack('!Q', offset)

    def decode(payload):
        if payload[:1] == b'\x00':
            payload_file.seek(struct.unpack('!Q', payload[1:])[0])
            return pickle.load(payload_file)
        return pickle.loads(payload)

    def flush():
        nonlocal chunk, pool
        path = Path(directory) / f'sort-{len(paths)}.pickle'
        if workers > 1:
            if pool is None:
                pool = stack.enter_context(ProcessPoolExecutor(
                    max_workers=workers, mp_context=multiprocessing.get_context('spawn')))
            # 提交前等待，避免生产者把整个输入排进进程队列。
            if len(pending) >= workers:
                finish_job(pending.popleft())
            submit(_sort_run, path, chunk)
        else:
            _sort_run(path, chunk)
        paths.append(path)
        chunk = []

    for row in rows:
        key = key_of(row, fields)
        payload = encode(row)
        chunk.append((key, payload))
        size += len(key) * 4 + len(payload) + 192
        if size >= chunk_bytes:
            flush()
            size = 0
    if not paths:
        chunk.sort(key=lambda item: item[0])
        for key, payload in chunk:
            yield key, decode(payload)
        return
    if chunk:
        flush()
    while pending:
        finish_job(pending.popleft())
    while len(paths) > _MERGE_FAN_IN:
        merged = []
        for i in range(0, len(paths), _MERGE_FAN_IN):
            group = paths[i:i + _MERGE_FAN_IN]
            out = Path(directory) / ('merge-' + uuid.uuid4().hex)
            if pool is None:
                _merge_run(out, group)
            else:
                if len(pending) >= workers:
                    finish_job(pending.popleft())
                submit(_merge_run, out, group)
            merged.append(out)
        while pending:
            finish_job(pending.popleft())
        for path in paths:
            path.unlink()
        paths = merged
    # 最后一轮归并/任意 Python reducer 仍由父进程按稳定顺序执行。
    if pool is not None:
        pool.shutdown()
    for key, payload in _merge_records(paths):
        yield key, decode(payload)


def _ordered_rows(dataset, fields, directory, chunk_bytes):
    """只复用框架自己证明的键序；map/filter 等用户回调可能修改键。"""
    factory = getattr(dataset._source, 'factory', None)
    ordered = getattr(factory, '_demiflow_ordered_on', None)
    if not dataset._plan.operations and ordered == tuple(fields):
        return ((key_of(row, fields), row) for row in dataset.iter_rows())
    return sorted_rows(dataset.iter_rows(), fields, directory, chunk_bytes,
                       workers=getattr(dataset._executor, '_sort_workers', 1))


def _merge_row(left, right, lk, rk, suffix):
    out = dict(left)
    for key, value in right.items():
        if key in lk and key in rk and key in out and out[key] == value:
            continue
        dest = key if key not in out else key + suffix
        if dest in out:
            raise ValueError(f'join column collision: {dest}')
        out[dest] = value
    return out


def _right_index(rows, fields, chunk_bytes):
    """试建有界右表；超限时把已读取的原记录接回流中，不重读上游。"""
    index = {}
    buffered = []
    size = 0
    for row in rows:
        key = key_of(row, fields)
        payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        buffered.append(payload)
        # 使用编码载荷而非全表 Python 字典，预算包括索引/列表的保守开销。
        size += len(payload) + 4 * len(key) + 256
        if size > chunk_bytes:
            return None, itertools.chain((pickle.loads(p) for p in buffered), rows)
        if all(row[field] is not None for field in fields):
            index.setdefault(key, []).append(payload)
    return index, ()


def _matched_rows(rows, directory, chunk_bytes):
    """普通右键组留内存；只有热键超预算时才生成可重复读取的磁盘组。"""
    memory = []
    size = 0
    for _, row in rows:
        payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        memory.append(payload)
        size += len(payload) + 64
        if size > chunk_bytes:
            path = Path(directory) / 'right-group.pickle'
            records = itertools.chain((('', p) for p in memory),
                                      (('', pickle.dumps(r, protocol=pickle.HIGHEST_PROTOCOL)) for _, r in rows))
            _write_records(path, records)
            return lambda: (pickle.loads(p) for _, p in _read_records(path))
    # 每个匹配对获得独立嵌套对象，保持旧版 pickle 重读的隔离语义。
    return lambda: (pickle.loads(p) for p in memory)


def join(left, right, on, right_on=None, how='inner', suffix='_right', chunk_bytes=32*1024*1024):
    """原生 local equijoin，保持 null、不匹配字段缺省、重复键乘积和列冲突规则。"""
    if getattr(left._executor, 'NAME', None) != 'local' or getattr(right._executor, 'NAME', None) != 'local':
        raise NotImplementedError('join currently supports the local backend only')
    if how not in {'inner', 'left', 'semi', 'anti'}:
        raise ValueError('unsupported join type')
    lk = keys(on)
    rk = keys(right_on or on)
    if not lk or len(lk) != len(rk):
        raise ValueError('join keys must have equal nonzero length')
    if chunk_bytes < 1:
        raise ValueError('chunk_bytes must be positive')
    from .api import DataAPI

    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-join-') as root, ExitStack() as stack:
            a = Path(root) / 'left'
            b = Path(root) / 'right'
            a.mkdir()
            b.mkdir()
            right_input = iter(right.iter_rows())
            stack.callback(getattr(right_input, 'close', lambda: None))
            index, remaining = _right_index(right_input, rk, chunk_bytes)
            if index is not None:
                # 小右表先过滤再排序：保持原键序/组内次序，却不把不匹配的
                # 大表载荷送进外部排序。left/anti 仍须保留不匹配行。
                if how in {'inner', 'semi'}:
                    left_input = iter(left.iter_rows())
                    stack.callback(getattr(left_input, 'close', lambda: None))
                    matching = (row for row in left_input if key_of(row, lk) in index)
                    factory = getattr(left._source, 'factory', None)
                    if not left._plan.operations and getattr(factory, '_demiflow_ordered_on', None) == tuple(lk):
                        left_rows = ((key_of(row, lk), row) for row in matching)
                    else:
                        left_rows = sorted_rows(matching, lk, a, chunk_bytes,
                                                workers=getattr(left._executor, '_sort_workers', 1))
                else:
                    left_rows = _ordered_rows(left, lk, a, chunk_bytes)
                stack.callback(left_rows.close)
                for key, row in left_rows:
                    matches = index.get(key, ())
                    if how == 'semi':
                        if matches:
                            yield dict(row)
                    elif how == 'anti':
                        if not matches:
                            yield dict(row)
                    elif matches:
                        for payload in matches:
                            yield _merge_row(row, pickle.loads(payload), lk, rk, suffix)
                    elif how == 'left':
                        yield dict(row)
                return
            left_rows = _ordered_rows(left, lk, a, chunk_bytes)
            stack.callback(left_rows.close)
            right_sorted = sorted_rows(remaining, rk, b, chunk_bytes,
                                       workers=getattr(right._executor, '_sort_workers', 1))
            stack.callback(right_sorted.close)
            right_groups = iter(itertools.groupby(right_sorted, key=lambda item: item[0]))
            current = next(right_groups, None)
            for key, group in itertools.groupby(left_rows, key=lambda item: item[0]):
                while current is not None and current[0] < key:
                    current = next(right_groups, None)
                matched = current is not None and current[0] == key and all(x is not None for x in json.loads(key))
                # semi/anti 只需要存在性，绝不写重复键组文件。
                matches = _matched_rows(current[1], root, chunk_bytes) if matched and how in {'inner', 'left'} else None
                for _, row in group:
                    if how == 'semi':
                        if matched:
                            yield dict(row)
                    elif how == 'anti':
                        if not matched:
                            yield dict(row)
                    elif matched:
                        for other in matches():
                            yield _merge_row(row, other, lk, rk, suffix)
                    elif how == 'left':
                        yield dict(row)
                if matched:
                    current = next(right_groups, None)

    generate._demiflow_ordered_on = tuple(lk)
    return DataAPI(left._executor).from_iter(generate)


def reduce_by_key(dataset, on, reducer, initial=None, chunk_bytes=32*1024*1024):
    """按键稳定执行 Python reducer；复用直接上游 join 已建立的同键顺序。"""
    if getattr(dataset._executor, 'NAME', None) != 'local':
        raise NotImplementedError('reduce_by_key supports local only')
    import copy
    from .api import DataAPI
    fields = keys(on)
    if not fields:
        raise ValueError('empty group keys')
    if chunk_bytes < 1:
        raise ValueError('chunk_bytes must be positive')

    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-group-') as root, ExitStack() as stack:
            rows = _ordered_rows(dataset, fields, root, chunk_bytes)
            stack.callback(rows.close)
            for _, items in itertools.groupby(rows, key=lambda item: item[0]):
                state = copy.deepcopy(initial)
                for _, row in items:
                    state = reducer(state, row)
                if state is not None:
                    yield state
    # reducer 可以返回任意字段，不能擅自给结果加上有序性保证。
    return DataAPI(dataset._executor).from_iter(generate)


def group_batches(dataset, on, max_rows=32, output='items', chunk_bytes=32*1024*1024):
    """同键分组分批，复用可证明的输入键序，保留组内原始次序。"""
    if max_rows < 1:
        raise ValueError('max_rows must be positive')
    if getattr(dataset._executor, 'NAME', None) != 'local':
        raise NotImplementedError('group_batches supports local only')
    from .api import DataAPI
    fields = keys(on)
    if not fields or output in fields or output in {'group_index', 'group_last'}:
        raise ValueError('invalid grouping fields')
    if chunk_bytes < 1:
        raise ValueError('chunk_bytes must be positive')

    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-group-') as root, ExitStack() as stack:
            rows = _ordered_rows(dataset, fields, root, chunk_bytes)
            stack.callback(rows.close)
            for key, items in itertools.groupby(rows, key=lambda item: item[0]):
                batch = []
                index = 0
                base = dict(zip(fields, json.loads(key)))
                for _, row in items:
                    if len(batch) == max_rows:
                        yield {**base, output: batch, 'group_index': index, 'group_last': False}
                        batch = []
                        index += 1
                    batch.append(row)
                if batch:
                    yield {**base, output: batch, 'group_index': index, 'group_last': True}
    generate._demiflow_ordered_on = tuple(fields)
    return DataAPI(dataset._executor).from_iter(generate)


def _cache_selected(predicate, output):
    if predicate is None:
        return True
    selected = predicate(output)
    if type(selected) is not bool:
        if inspect.iscoroutine(selected):
            selected.close()
        raise TypeError('cache_when must return a boolean synchronously')
    return selected


class CachedSyncMap:
    """Atomic per-input cache for synchronous local process map operators."""
    def __init__(self, fn, directory, version, *, cache_when=None):
        if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, '__call__', None)):
            raise TypeError('Synchronous cache does not accept async callables')
        self.fn, self.directory, self.version = fn, Path(directory), version
        self.cache_when = cache_when

    def __call__(self, row):
        key = digest({'version': self.version, 'input': row})
        folder = self.directory / key[:2]
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / (key + '.json')
        with (folder / (key + '.lock')).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if path.exists():
                cached = json.loads(path.read_text())['output']
                if _cache_selected(self.cache_when, cached):
                    return cached
            try:
                result = self.fn(row)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result): result.close()
                    raise TypeError('Synchronous cache callable returned an awaitable')
                if not _cache_selected(self.cache_when, result):
                    return result
                temporary = folder / (key + '.' + uuid.uuid4().hex + '.tmp')
                temporary.write_text(canonical({'output': result}))
                os.replace(temporary, path)
                return result
            except BaseException as error:
                (folder / (key + '.' + uuid.uuid4().hex + '.error.json')).write_text(
                    canonical({'type': type(error).__name__, 'error': str(error)}))
                raise


class CachedMap:
    def __init__(self,fn,directory,version,*,cache_when=None):
        self.fn=fn;self.directory=Path(directory);self.version=version
        self.cache_when=cache_when
        self.concurrency=getattr(fn,'concurrency',1);self.queue_depth=getattr(fn,'queue_depth',8);self.catch=()
    async def __call__(self,row):
        key=digest({'version':self.version,'input':row});folder=self.directory/key[:2];folder.mkdir(parents=True,exist_ok=True)
        path=folder/(key+'.json')
        with (folder/(key+'.lock')).open('a') as lock:
            # Nonblocking avoids blocking an async event loop while another worker holds the key.
            while True:
                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:await asyncio.sleep(.05)
            if path.exists():
                cached=json.loads(path.read_text())['output']
                if _cache_selected(self.cache_when, cached):return cached
            try:
                out=self.fn(row)
                if inspect.isawaitable(out):out=await out
                if not _cache_selected(self.cache_when, out):return out
                tmp=folder/(key+'.'+uuid.uuid4().hex+'.tmp')
                tmp.write_text(canonical({'output':out}));os.replace(tmp,path)
                return out
            except BaseException as error:
                (folder/(key+'.'+uuid.uuid4().hex+'.error.json')).write_text(canonical({'type':type(error).__name__,'error':str(error)}))
                raise
    async def aclose(self):
        close=getattr(self.fn,'aclose',None)
        if close:
            result=close()
            if inspect.isawaitable(result):await result


def checkpoint(dataset,path,version):
    from .api import DataAPI
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    metadata=path.with_suffix(path.suffix+'.meta.json')
    with path.with_suffix(path.suffix+'.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if metadata.exists() and json.loads(metadata.read_text())['version']!=version:raise ValueError('checkpoint version changed; use a new path')
        if path.exists() and not metadata.exists():raise ValueError('checkpoint metadata missing')
        if not metadata.exists():metadata.write_text(canonical({'version':version}))
        if not path.exists():
            partial=path.with_suffix(path.suffix+'.'+uuid.uuid4().hex+'.partial')
            with partial.open('w') as f:
                class Write:
                    concurrency=1;queue_depth=8;catch=()
                    async def __call__(self,row):f.write(canonical(row)+'\n');return row
                from .plan import is_stream_operation, LogicalPlan
                from .dataset import Dataset
                ops=dataset._plan.operations
                first=next((i for i,op in enumerate(ops) if is_stream_operation(op)),None)
                if first is None:
                    for row in dataset.iter_rows():f.write(canonical(row)+'\n')
                else:
                    prefix=Dataset(dataset._source,LogicalPlan(ops[:first]),dataset._executor)
                    source=DataAPI(dataset._executor).from_iter(prefix.iter_rows)
                    stream=Dataset(source._source,LogicalPlan(ops[first:]),dataset._executor,dataset._stages)
                    stream.map_async(Write()).run_stream(log_every=0)
                f.flush();os.fsync(f.fileno())
            os.replace(partial,path)
    def rows():
        with path.open() as f:
            for line in f:yield json.loads(line)
    return DataAPI(dataset._executor).from_iter(rows)
