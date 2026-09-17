"""Local, spillable relational operators and durable JSON checkpoints.

No database dependency. Join/sort memory is bounded by sort chunk size and one
row; duplicate right groups spill to a temporary file. Caller reducers own their
accumulator size. These operators compose at synchronous Dataset boundaries;
checkpoint an async plan before joining it.
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
import tempfile
import uuid
from pathlib import Path


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def keys(value):return [value] if isinstance(value,str) else list(value)
def key_of(row,fields):return canonical([row[k] for k in fields])


def sorted_rows(rows,fields,directory,chunk_bytes):
    if chunk_bytes<1:raise ValueError('chunk_bytes must be positive')
    paths=[];chunk=[];size=0
    def flush():
        chunk.sort(key=lambda x:x[0]);path=Path(directory)/f'sort-{len(paths)}.pickle'
        with path.open('wb') as f:
            for item in chunk:pickle.dump(item,f)
        paths.append(path);chunk.clear()
    for row in rows:
        item=(key_of(row,fields),row);chunk.append(item);size+=len(pickle.dumps(item))
        if size>=chunk_bytes:flush();size=0
    if chunk:flush()
    def read(path):
        with path.open('rb') as f:
            while True:
                try:yield pickle.load(f)
                except EOFError:return
    # Bound file descriptors even for very large inputs.
    while len(paths)>64:
        merged=[]
        for i in range(0,len(paths),64):
            group=paths[i:i+64];out=Path(directory)/('merge-'+uuid.uuid4().hex)
            with out.open('wb') as f:
                for item in heapq.merge(*(read(p) for p in group),key=lambda x:x[0]):pickle.dump(item,f)
            for p in group:p.unlink()
            merged.append(out)
        paths=merged
    yield from heapq.merge(*(read(p) for p in paths),key=lambda x:x[0])


def join(left,right,on,right_on=None,how='inner',suffix='_right',chunk_bytes=32*1024*1024):
    if getattr(left._executor,'NAME',None)!='local' or getattr(right._executor,'NAME',None)!='local':
        raise NotImplementedError('join currently supports the local backend only')
    if how not in {'inner','left','semi','anti'}:raise ValueError('unsupported join type')
    lk=keys(on);rk=keys(right_on or on)
    if not lk or len(lk)!=len(rk):raise ValueError('join keys must have equal nonzero length')
    from .api import DataAPI
    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-join-') as root:
            a=Path(root)/'left';b=Path(root)/'right';a.mkdir();b.mkdir()
            left_rows=sorted_rows(left.iter_rows(),lk,a,chunk_bytes)
            right_rows=iter(itertools.groupby(sorted_rows(right.iter_rows(),rk,b,chunk_bytes),key=lambda x:x[0]))
            current=next(right_rows,None)
            for key,group in itertools.groupby(left_rows,key=lambda x:x[0]):
                while current is not None and current[0]<key:current=next(right_rows,None)
                matched=current is not None and current[0]==key and all(x is not None for x in json.loads(key))
                spool=Path(root)/'right-group.pickle'
                if matched:
                    with spool.open('wb') as f:
                        for _,row in current[1]:pickle.dump(row,f)
                for _,row in group:
                    if how=='semi':
                        if matched:yield dict(row)
                        continue
                    if how=='anti':
                        if not matched:yield dict(row)
                        continue
                    if not matched:
                        if how=='left':yield dict(row) # unmatched fields are absent, not fabricated
                        continue
                    with spool.open('rb') as f:
                        while True:
                            try:r=pickle.load(f)
                            except EOFError:break
                            out=dict(row)
                            for k,v in r.items():
                                if k in lk and k in rk and k in out and out[k]==v:continue
                                dest=k if k not in out else k+suffix
                                if dest in out:raise ValueError(f'join column collision: {dest}')
                                out[dest]=v
                            yield out
                if matched:current=next(right_rows,None)
    return DataAPI(left._executor).from_iter(generate)


def reduce_by_key(dataset,on,reducer,initial=None,chunk_bytes=32*1024*1024):
    if getattr(dataset._executor,'NAME',None)!='local':raise NotImplementedError('reduce_by_key supports local only')
    import copy
    from .api import DataAPI
    fields=keys(on)
    if not fields:raise ValueError('empty group keys')
    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-group-') as root:
            for _,items in itertools.groupby(sorted_rows(dataset.iter_rows(),fields,root,chunk_bytes),key=lambda x:x[0]):
                state=copy.deepcopy(initial)
                for _,row in items:state=reducer(state,row)
                if state is not None:yield state
    return DataAPI(dataset._executor).from_iter(generate)


def group_batches(dataset,on,max_rows=32,output='items',chunk_bytes=32*1024*1024):
    if max_rows<1:raise ValueError('max_rows must be positive')
    if getattr(dataset._executor,'NAME',None)!='local':raise NotImplementedError('group_batches supports local only')
    from .api import DataAPI
    fields=keys(on)
    if not fields or output in fields or output in {'group_index','group_last'}:raise ValueError('invalid grouping fields')
    def generate():
        with tempfile.TemporaryDirectory(prefix='demiflow-group-') as root:
            for key,items in itertools.groupby(sorted_rows(dataset.iter_rows(),fields,root,chunk_bytes),key=lambda x:x[0]):
                batch=[];index=0;base=dict(zip(fields,json.loads(key)))
                for _,row in items:
                    if len(batch)==max_rows:
                        yield {**base,output:batch,'group_index':index,'group_last':False};batch=[];index+=1
                    batch.append(row)
                if batch:yield {**base,output:batch,'group_index':index,'group_last':True}
    return DataAPI(dataset._executor).from_iter(generate)


class CachedMap:
    def __init__(self,fn,directory,version):
        self.fn=fn;self.directory=Path(directory);self.version=version
        self.concurrency=getattr(fn,'concurrency',1);self.queue_depth=getattr(fn,'queue_depth',8);self.catch=()
    async def __call__(self,row):
        key=digest({'version':self.version,'input':row});folder=self.directory/key[:2];folder.mkdir(parents=True,exist_ok=True)
        path=folder/(key+'.json')
        with (folder/(key+'.lock')).open('a') as lock:
            # Nonblocking avoids blocking an async event loop while another worker holds the key.
            while True:
                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:await asyncio.sleep(.05)
            if path.exists():return json.loads(path.read_text())['output']
            try:
                out=self.fn(row)
                if inspect.isawaitable(out):out=await out
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
                from .plan import AsyncMapOp, LogicalPlan
                from .dataset import Dataset
                ops=dataset._plan.operations
                first=next((i for i,op in enumerate(ops) if isinstance(op,AsyncMapOp)),None)
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
