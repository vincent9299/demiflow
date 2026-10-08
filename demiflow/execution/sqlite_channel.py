"""Bounded, embedded SQLiteQueue delivery for Dataset streams.

Messages and completed results are immutable. A reader can freeze their rowid
high-water mark for archival; archiving never claims consumer work. No broker
process, image bytes, or implicit retry of an external request lives here.
"""
import asyncio
from contextlib import contextmanager
import json
import sqlite3
import threading
import time
from pathlib import Path

from ..collect.sqlite_queue import SQLiteQueue, TaskHandle
from .blocking_io import BlockingIOPool


def bounded_json(value, limit):
    """Reject oversized/deep values before creating a serialization copy."""
    remaining = limit
    def walk(item, depth=0):
        nonlocal remaining
        if depth > 24:
            raise ValueError('Queue JSON nesting limit exceeded')
        if isinstance(item, str):
            remaining -= 6 * len(item) + 2
        elif isinstance(item, (dict, list, tuple)):
            if len(item) > 16384:
                raise ValueError('Queue JSON container limit exceeded')
            remaining -= 2 + 2 * len(item)
            for entry in (item.items() if isinstance(item, dict) else item):
                if isinstance(item, dict):
                    walk(entry[0], depth+1); walk(entry[1], depth+1)
                else:
                    walk(entry, depth+1)
        elif item is None or type(item) in (bool, int, float):
            remaining -= 32
        else:
            raise ValueError('Queue payload must be JSON data')
        if remaining < 0:
            raise ValueError('Queue JSON byte budget exceeded')
    walk(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def channel_config(path, *, max_tasks=100000, max_payload_bytes=2*1024**2,
                   max_result_bytes=2*1024**2, max_file_bytes=2*1024**3):
    values = dict(path=str(path), max_tasks=max_tasks, max_payload_bytes=max_payload_bytes,
                  max_result_bytes=max_result_bytes, max_file_bytes=max_file_bytes)
    bounds = {'max_tasks':(1,1000000), 'max_payload_bytes':(1024,16*1024**2),
              'max_result_bytes':(1024,16*1024**2), 'max_file_bytes':(1024**2,16*1024**3)}
    for key, (lo, hi) in bounds.items():
        if type(values[key]) is not int or not lo <= values[key] <= hi:
            raise ValueError('Invalid queue limit: '+key)
    if not values['path'] or len(values['path']) > 4096:
        raise ValueError('Invalid queue path')
    return values


class SQLiteChannel(SQLiteQueue):
    """SQLiteQueue with finite Dataset payloads, closure and immutable scans.

    FULL sync is deliberate. max_file_bytes limits main-database pages; WAL and
    SQLite bookkeeping need additional disk headroom. Transactions hold at most
    32 messages / 16 MiB; scans fetch one value and release the read snapshot before
    yielding, so they never pin a long-running WAL snapshot.
    """
    def __init__(self, path, **limits):
        self.config = channel_config(path, **limits)
        self._readonly = False
        super().__init__(path, timeout_s=10)
        with self._connection(write=True) as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS dataset_channels (
                pool TEXT PRIMARY KEY, closed INTEGER NOT NULL DEFAULT 0);
              CREATE TABLE IF NOT EXISTS dataset_message_identity (
                task_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS dataset_queue_limits (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS dataset_queue_count (id INTEGER PRIMARY KEY, tasks INTEGER NOT NULL);
              CREATE INDEX IF NOT EXISTS dataset_tasks_pool_sequence ON tasks(pool);
            ''')
            db.execute('INSERT OR IGNORE INTO dataset_queue_count SELECT 1,count(*) FROM tasks')
            encoded = json.dumps({k:v for k,v in self.config.items() if k!='path'}, sort_keys=True)
            old = db.execute('SELECT value FROM dataset_queue_limits WHERE id=1').fetchone()
            if old and old[0] != encoded:
                raise ValueError('Queue resource limits changed; use an explicit new channel')
            db.execute('INSERT OR IGNORE INTO dataset_queue_limits VALUES (1,?)',(encoded,))

    @classmethod
    def read_only(cls, path, **limits):
        """Existing immutable receipts only; no file/schema creation or claims."""
        reader=object.__new__(cls)
        reader.config=channel_config(path,**limits)
        reader.path=Path(path).resolve();reader.timeout_s=10;reader._readonly=True
        return reader

    @contextmanager
    def _connection(self, *, write=False, allow_thread_transfer=False):
        if self._readonly and write:raise ValueError('Queue archive is read only')
        db = (sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=self.timeout_s,
                             check_same_thread=not allow_thread_transfer) if self._readonly else
              sqlite3.connect(self.path, timeout=self.timeout_s, check_same_thread=not allow_thread_transfer))
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA cache_size=-4096')
            if not self._readonly:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('PRAGMA wal_autocheckpoint=256')
                page_size = db.execute('PRAGMA page_size').fetchone()[0]
                db.execute('PRAGMA max_page_count='+str(self.config['max_file_bytes']//page_size))
            with db:
                if write: db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            db.close()

    def open(self, pool):
        self._pool(pool)
        with self._connection(write=True) as db:
            if (db.execute('SELECT count(*) FROM dataset_channels').fetchone()[0]>=128 and
                db.execute('SELECT 1 FROM dataset_channels WHERE pool=?',(pool,)).fetchone() is None):
                raise ValueError('Queue channel count exceeds 128')
            db.execute('INSERT INTO dataset_channels VALUES (?,0) ON CONFLICT(pool) DO UPDATE SET closed=0',(pool,))

    def add(self, tasks, **kwargs):
        raise TypeError('Dataset channels require bounded put(messages) with semantic fingerprints')

    def seal(self, pool):
        self._pool(pool)
        with self._connection(write=True) as db:
            if db.execute('UPDATE dataset_channels SET closed=1 WHERE pool=?',(pool,)).rowcount != 1:
                raise ValueError('Unknown queue channel')

    @staticmethod
    def _pool(pool):
        if not isinstance(pool,str) or not 1 <= len(pool) <= 128:
            raise ValueError('Queue pool must be 1..128 characters')

    def put(self, messages):
        """Atomically enqueue <=32 bounded messages, retaining first provenance.

        fingerprint is the producer's semantic identity. Replayed input may carry
        another equivalent fixed archive reference; same ID/fingerprint retains
        its first immutable payload. Pool, cost and budget class must also match.
        """
        if not isinstance(messages,list) or len(messages)>32:
            raise ValueError('Queue transaction requires at most 32 messages')
        encoded=[]; total=0
        for item in messages:
            key=item['task_id']; fingerprint=item['fingerprint']; pool=item.get('pool','default')
            self._pool(pool)
            if any(not isinstance(v,str) or not 1<=len(v)<=256 for v in (key,fingerprint)):
                raise ValueError('Invalid queue message identity')
            payload=bounded_json(item['payload'],self.config['max_payload_bytes'])
            total+=len(payload.encode())
            if total>16*1024**2:raise ValueError('Queue transaction exceeds 16 MiB')
            cost=item.get('cost',0)
            if type(cost) is not int or not 0<=cost<=1000000:raise ValueError('Invalid queue cost')
            encoded.append((item,payload,pool,cost))
        with self._connection(write=True) as db:
            count=db.execute('SELECT tasks FROM dataset_queue_count WHERE id=1').fetchone()[0]
            added=0
            for item,payload,pool,cost in encoded:
                old=db.execute('SELECT t.pool,t.budget_class,t.cost,i.fingerprint FROM tasks t '
                    'JOIN dataset_message_identity i USING(task_id) WHERE task_id=?',(item['task_id'],)).fetchone()
                budget=item.get('budget_class','default')
                if old:
                    if tuple(old)!=(pool,budget,cost,item['fingerprint']):
                        raise ValueError('Conflicting queue message identity')
                    continue
                state=db.execute('SELECT closed FROM dataset_channels WHERE pool=?',(pool,)).fetchone()
                if state is None or state[0]:raise ValueError('Queue producer channel is not open')
                if count+added>=self.config['max_tasks']:raise ValueError('Queue task budget exhausted')
                db.execute('INSERT INTO tasks(task_id,payload_json,pool,budget_class,cost,max_attempts) '
                           'VALUES (?,?,?,?,?,3)',(item['task_id'],payload,pool,budget,cost))
                db.execute('INSERT INTO dataset_message_identity VALUES (?,?)',(item['task_id'],item['fingerprint']))
                added+=1
            db.execute('UPDATE dataset_queue_count SET tasks=tasks+? WHERE id=1',(added,))
            return added

    def complete(self, handle, result, *, actual_cost=None):
        bounded_json(result,self.config['max_result_bytes'])
        if actual_cost is not None and actual_cost>handle.cost:
            raise ValueError('Completed cost exceeds pre-dispatch reservation')
        return super().complete(handle,result,actual_cost=actual_cost)

    def state(self, pool):
        self._pool(pool)
        with self._connection() as db:
            row=db.execute('SELECT closed FROM dataset_channels WHERE pool=?',(pool,)).fetchone()
            counts={r[0]:r[1] for r in db.execute('SELECT state,count(*) FROM tasks WHERE pool=? GROUP BY state',(pool,))}
        return {'closed':bool(row and row[0]),'exists':row is not None,**counts}

    def high_water(self, pool, *, results=False):
        with self._connection() as db:
            query=('SELECT COALESCE(max(c.rowid),0) FROM completions c JOIN tasks t USING(task_id) WHERE t.pool=?'
                   if results else 'SELECT COALESCE(max(rowid),0) FROM tasks WHERE pool=?')
            return db.execute(query,(pool,)).fetchone()[0]

    def scan(self, pool, *, through, results=False):
        """Immutable records <= an explicit high-water mark, without claiming."""
        if type(through) is not int or through<0:raise ValueError('Invalid queue high-water mark')
        last=0
        with self._connection(allow_thread_transfer=True) as db:
            # Dataset may pull successive next() calls on different workers.
            # Generator execution is serial; the connection is never shared by
            # concurrent statements, including close from the action owner.
            for _ in range(self.config['max_tasks']):
                # CROSS JOIN keeps the bounded completion rowid range outermost.
                # Starting from all tasks in the pool sorts the full pool again
                # for every returned record, making archival quadratic.
                query=('SELECT c.rowid,c.task_id,c.result_json FROM completions c CROSS JOIN tasks t USING(task_id) '
                       'WHERE t.pool=? AND c.rowid>? AND c.rowid<=? ORDER BY c.rowid LIMIT 1' if results else
                       'SELECT rowid,task_id,payload_json FROM tasks WHERE pool=? AND rowid>? AND rowid<=? ORDER BY rowid LIMIT 1')
                cursor=db.execute(query,(pool,last,through))
                try:row=cursor.fetchone()
                finally:cursor.close()  # Release the implicit read snapshot before yielding.
                if row is None:return
                last=row[0]
                limit=self.config['max_result_bytes'] if results else self.config['max_payload_bytes']
                if len(row[2].encode())>limit:raise ValueError('Stored queue record exceeds byte budget')
                yield {'sequence':last,'task_id':row[1],'value':json.loads(row[2])}


class QueuePublisher:
    concurrency=1
    def __init__(self, config, column):
        self.config,self.column=config,column
        self.io=BlockingIOPool(1,name='queue-publish');self.queue=None
    async def astart(self):
        self.queue=await self.io.run(lambda:SQLiteChannel(**self.config))
    async def __call__(self,row):
        messages=row[self.column]
        if not isinstance(messages,list) or len(messages)>257:
            raise ValueError('Queue publication row exceeds 257 messages')
        for start in range(0,len(messages),32):
            await self.io.run(self.queue.put,messages[start:start+32])
        return {'queued_messages':len(row[self.column])}
    async def aclose(self):await self.io.aclose()


class QueueReader:
    """One action owns claims; idle pull returns on seal/drain or stop.

    Replay emits stored results before claiming fresh tasks. Metadata survives
    business maps explicitly and acknowledgement is a separate Dataset node.
    """
    def __init__(self,config,pool,stop=None,idle_timeout_s=1800):
        self.config,self.pool=config,pool
        self.stop=stop or threading.Event();self.queue=None;self.worker=None
        self.handles={};self.lock=threading.Lock();self.heartbeat=None
        self.idle_timeout_s=idle_timeout_s;self.heartbeat_error=None
    async def astart(self):
        self.queue=await asyncio.to_thread(lambda:SQLiteChannel(**self.config))
        await asyncio.to_thread(self.queue.reclaim,stale_s=1)
        self.worker=await asyncio.to_thread(self.queue.register_worker,self.pool)
        async def pulse():
            try:
                while not self.stop.is_set():
                    await asyncio.sleep(2)
                    await asyncio.to_thread(self.queue.heartbeat,self.worker)
            except Exception as exc:
                self.heartbeat_error=exc
                self.stop.set()
        self.heartbeat=asyncio.create_task(pulse())
    def rows(self):
        if self.worker is None:raise RuntimeError('Queue source requires run_stream')
        upper=self.queue.high_water(self.pool,results=True)
        for record in self.queue.scan(self.pool,through=upper,results=True):
            if self.stop.is_set():return
            yield {'_queue_delivery':None,'_queue_result':record['value']}
        idle_since=time.monotonic();last_reclaim=idle_since;last_state=None
        while not self.stop.is_set():
            if time.monotonic()-last_reclaim>=5:
                self.queue.reclaim(stale_s=1);last_reclaim=time.monotonic()
            with self.lock:owned=len(self.handles)
            if owned>=256:
                if time.monotonic()-idle_since>self.idle_timeout_s:
                    raise TimeoutError('Queue consumer has made no progress before idle deadline')
                self.stop.wait(.2)
                continue
            handles=self.queue.claim(self.worker,limit=1)
            if handles:
                idle_since=time.monotonic()
                handle=handles[0]
                with self.lock:self.handles[handle.task_id]=handle
                delivery={name:getattr(handle,name) for name in
                          ('task_id','pool','budget_class','cost','attempt','worker','token')}
                yield {**handle.payload,'_queue_delivery':delivery,'_queue_result':None}
            else:
                state=self.queue.state(self.pool)
                if state!=last_state:
                    idle_since=time.monotonic();last_state=state
                if state['closed'] and not state.get('running',0):
                    if state.get('pending',0) or state.get('failed',0):
                        raise RuntimeError('Sealed queue has uncompleted or budget-blocked tasks')
                    return
                if time.monotonic()-idle_since>self.idle_timeout_s:
                    raise TimeoutError('Queue producer has made no progress before idle deadline')
                self.stop.wait(.2)
        if self.heartbeat_error is not None:raise self.heartbeat_error
    async def aclose(self):
        self.stop.set()
        if self.heartbeat:
            self.heartbeat.cancel()
            await asyncio.gather(self.heartbeat,return_exceptions=True)
        if self.worker:
            # No automatic HTTP retry: request journals retain unanswered calls.
            for handle in list(self.handles.values()):
                try:await asyncio.to_thread(self.queue.fail,handle,'consumer stopped',backoff_s=0)
                except ValueError:pass  # already completed by the ACK node
            await asyncio.to_thread(self.queue.unregister_worker,self.worker)


class QueueAcknowledger:
    concurrency=1
    def __init__(self,config,column,reader,*,keep_row=False):
        self.config,self.column=config,column
        self.keep_row=keep_row
        self.reader=reader
        self.io=BlockingIOPool(1,name='queue-ack');self.queue=None
    async def astart(self):self.queue=await self.io.run(lambda:SQLiteChannel(**self.config))
    async def __call__(self,row):
        handle=row.get('_queue_delivery')
        if handle:
            await self.io.run(self.queue.complete,TaskHandle(**handle,payload=None),row[self.column])
            with self.reader.lock:self.reader.handles.pop(handle['task_id'],None)
        return {**(row if self.keep_row else {}),'acknowledged':bool(handle)}
    async def aclose(self):await self.io.aclose()
