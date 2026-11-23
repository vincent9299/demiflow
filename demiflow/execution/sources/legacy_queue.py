"""Read-only v1 SQLite archive facts, without claims or v2 certification."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from demiflow.data.datasource import Datasource, ReadTask, BlockMetadata
from demiflow.execution.queue_contract import HistoryReadLimits, HistoryReadLimit
from demiflow.execution.recovery_contract import RecoveryContractError, identity, identifier


class LegacyQueueHistoryDatasource(Datasource):
    def __init__(self,path,*,sha256,pool,through,results=False,limits=None,max_journal_bytes=1024**3,after=None):
        if type(path) is not str or not Path(path).is_absolute() or len(path)>4096:
            raise ValueError('Legacy history requires an absolute closed SQLite backup path')
        if type(sha256) is not str or len(sha256)!=64 or any(c not in '0123456789abcdef' for c in sha256):
            raise ValueError('Legacy history requires the backup SHA256')
        if type(through) is not int or not 0<=through<2**63 or type(results) is not bool:
            raise ValueError('Invalid legacy rowid bound or result mode')
        if type(max_journal_bytes) is not int or not 1<=max_journal_bytes<=8*1024**3:
            raise ValueError('Legacy journal requires a finite byte ceiling')
        limits=HistoryReadLimits() if limits is None else limits
        if type(limits) is not HistoryReadLimits:raise TypeError('Use HistoryReadLimits')
        self.path,self.sha256,self.pool,self.through,self.results=path,sha256,identifier(pool),through,results
        self.limits,self.max_journal_bytes=limits,max_journal_bytes
        expected=dict(archive_sha256=sha256,pool=pool,through=through,results=results)
        if after is not None and (type(after) is not dict or set(after)!=set(expected)|{'after_rowid'} or
                any(after[key]!=value for key,value in expected.items()) or type(after['after_rowid']) is not int or
                not 0<=after['after_rowid']<=through):
            raise ValueError('Legacy cursor differs from the fixed archive')
        self.after=after['after_rowid'] if after is not None else 0

    def snapshot_identity(self):
        from dataclasses import asdict
        return dict(contract='demiflow.queue.legacy-archive.v1',path=self.path,sha256=self.sha256,
            pool=self.pool,through=self.through,results=self.results,limits=asdict(self.limits),
            max_journal_bytes=self.max_journal_bytes,after=self.after)

    def recovery_identity(self):return self.snapshot_identity()

    def get_read_tasks(self,parallelism,per_task_row_limit=None,data_context=None):
        return [ReadTask(self.rows,BlockMetadata(),source_id=identity(self.snapshot_identity()),partition_id='legacy')]

    def rows(self):
        path=Path(self.path);limits=self.limits;started=time.monotonic();position=self.after;count=total=resident=0
        from demiflow.execution.pipeline.run_context import current_run
        from demiflow.data.runtime_contract import check_size
        run=current_run()
        resident_limit=min(limits.max_total_record_bytes*8,run.limits['memory_bytes']//2 if run else 128*1024**2)
        def cursor():return dict(archive_sha256=self.sha256,pool=self.pool,through=self.through,after_rowid=position,results=self.results)
        def check():
            if run is not None:run.check_stop()
            if time.monotonic()-started>limits.timeout_s:raise HistoryReadLimit('Legacy archive time budget exhausted',cursor())
        if any(p.is_symlink() for p in (path,*path.parents)):
            raise RecoveryContractError('Legacy archive path contains a symlink')
        if Path(self.path+'-wal').exists():
            raise RecoveryContractError('Legacy history requires a closed backup without a WAL sidecar')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            before=os.fstat(fd)
            signature=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
            if not stat.S_ISREG(before.st_mode) or before.st_size>self.max_journal_bytes:
                raise RecoveryContractError('Legacy archive exceeds its regular-file byte budget')
            if before.st_size>limits.max_hash_bytes:
                raise HistoryReadLimit('Legacy archive checksum exceeds its hash budget',cursor())
            digest=hashlib.sha256()
            while chunk:=os.read(fd,65536):
                check();digest.update(chunk)
            if digest.hexdigest()!=self.sha256 or signature(os.fstat(fd))!=signature(before):
                raise RecoveryContractError('Legacy archive SHA256 or file identity changed')
            with sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True) as db:
                db.execute('PRAGMA query_only=ON');db.execute('PRAGMA cache_size=-4096')
                while True:
                    check()
                    query=('SELECT c.rowid,c.task_id,length(CAST(c.result_json AS BLOB)) FROM completions c CROSS JOIN tasks t USING(task_id) '
                           'WHERE t.pool=? AND c.rowid>? AND c.rowid<=? ORDER BY c.rowid LIMIT 1' if self.results else
                           'SELECT rowid,task_id,length(CAST(payload_json AS BLOB)) FROM tasks WHERE pool=? AND rowid>? AND rowid<=? ORDER BY rowid LIMIT 1')
                    record=db.execute(query,(self.pool,position,self.through)).fetchone()
                    if record is None:return
                    if count>=limits.max_rows:raise HistoryReadLimit('Legacy archive row budget exhausted',cursor())
                    if record[2]>limits.max_record_bytes or total+record[2]>limits.max_total_record_bytes:
                        raise HistoryReadLimit('Legacy record byte budget exhausted',cursor())
                    table,column=('completions','result_json') if self.results else ('tasks','payload_json')
                    value=json.loads(db.execute('SELECT '+column+' FROM '+table+' WHERE rowid=?',(record[0],)).fetchone()[0])
                    if signature(path.stat())!=signature(before):raise RecoveryContractError('Legacy archive changed during reading')
                    output=dict(sequence=record[0],task_id=record[1],value=value,verification={'status':'legacy_not_certified'})
                    try:size=check_size(output,resident_limit)
                    except TypeError:
                        raise HistoryReadLimit('Legacy resident record budget exhausted',cursor()) from None
                    if resident+size>resident_limit:raise HistoryReadLimit('Legacy resident window budget exhausted',cursor())
                    position=record[0];count+=1;total+=record[2]
                    resident+=size
                    yield (output,)
        finally:os.close(fd)
