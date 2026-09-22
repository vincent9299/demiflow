"""Read SHA-addressed Blob rows from one Lance table, with optional version pinning.

There is no file fallback or application-defined shard routing. Local and object
store tables use the same Lance URI; materialized files are disposable caches.
"""
from __future__ import annotations
from dataclasses import dataclass,field
from pathlib import Path
from urllib.parse import urlsplit
import hashlib
import os
import re
import uuid

class AssetError(ValueError):
    """资产读取契约破坏（缺失、损坏、参数非法）。

    继承 ValueError：消费端既有 (ValueError, OSError) 捕获语义保持不变
    ——损坏显式抛错、缺失可捕获降级，两者不互相吞并。"""


class AssetMissing(AssetError):
    pass


class AssetCorrupted(AssetError):
    pass


class AssetReadError(AssetError):
    pass


@dataclass(frozen=True)
class AssetResolution:
    """一次资产解析的完整结果：状态 + 字节 + 来源身份（不含缓存路径）。"""

    sha256: str
    ext: str | None
    status: str  # ok | missing | corrupt | read_error
    data: bytes | None = None
    byte_size: int | None = None
    actual_sha256: str | None = None
    error: str | None = None
    source: dict = field(default_factory=dict)  # mode/table/lance_version/path



class BlobAssetReader:
    def __init__(self,uri,*,column='data',id_column='sha256',version=None,
                 storage_options=None,verify=True,cache_dir=None):
        if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',s) for s in (column,id_column)):
            raise ValueError('Invalid Blob column name')
        self.uri=str(uri);self.column=column;self.id_column=id_column
        self.version=version;self.storage_options=dict(storage_options or {})
        self.verify=verify;self._cache_dir=Path(cache_dir) if cache_dir is not None else None

    def resolve(self,sha256,ext=None,*,version=None):
        if not isinstance(sha256,str) or not re.fullmatch(r'[0-9a-f]{64}',sha256):
            raise AssetError('sha256 must be 64 lowercase hex characters')
        pinned=self.version if version is None else version
        source={'mode':'lake','table':self.uri}
        if urlsplit(self.uri).scheme=='' and not Path(self.uri).exists():
            return AssetResolution(sha256,ext,'missing',source=source)
        try:
            import lance
            ds=lance.dataset(self.uri,version=pinned,storage_options=self.storage_options or None)
            source['lance_version']=ds.version;source[self.id_column]=sha256
            rows=ds.scanner(columns=[self.id_column],filter=f"{self.id_column} = '{sha256}'",with_row_id=True).to_table()
            if rows.num_rows==0:return AssetResolution(sha256,ext,'missing',source=source)
            if rows.num_rows!=1:raise AssetReadError('Duplicate content IDs in Blob table')
            blob=ds.take_blobs(self.column,ids=[rows['_rowid'][0].as_py()])[0]
            if blob is None:return AssetResolution(sha256,ext,'missing',source=source)
            value=blob if isinstance(blob,bytes) else blob.read()
        except Exception as exc:
            return AssetResolution(sha256,ext,'read_error',error=str(exc),source=source)
        actual=hashlib.sha256(value).hexdigest()
        if self.verify and actual!=sha256:
            return AssetResolution(sha256,ext,'corrupt',byte_size=len(value),actual_sha256=actual,source=source)
        return AssetResolution(sha256,ext,'ok',data=value,byte_size=len(value),actual_sha256=actual,source=source)

    def read(self,sha256,ext=None,*,version=None):
        result=self.resolve(sha256,ext,version=version)
        if result.status=='missing':raise AssetMissing('Blob bytes not found: '+sha256)
        if result.status=='corrupt':raise AssetCorrupted('Blob content SHA mismatch: '+sha256)
        if result.status=='read_error':raise AssetReadError(result.error)
        return result.data,result.source

    def read_bytes(self,sha256,ext=None):return self.read(sha256,ext)[0]

    def materialize(self,sha256,ext='bin',cache_dir=None):
        cache=Path(cache_dir) if cache_dir is not None else self._cache_dir
        if cache is None:raise AssetError('Explicit cache directory required')
        if not re.fullmatch(r'[a-z0-9]{2,8}',ext):raise AssetError('Invalid cache extension')
        if not re.fullmatch(r'[0-9a-f]{64}',sha256):raise AssetError('Invalid content SHA')
        target=cache/sha256[:2]/(sha256+'.'+ext)
        data,_=self.read(sha256,ext)
        if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest()==sha256:return target
        target.parent.mkdir(parents=True,exist_ok=True)
        temporary=target.with_name('.'+target.name+'.'+uuid.uuid4().hex)
        try:
            with temporary.open('wb') as stream:
                stream.write(data);stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,target)
        finally:temporary.unlink(missing_ok=True)
        return target
