"""Pure, finite declarations of the v2 native task channel."""
from dataclasses import asdict, dataclass, field
import math
from pathlib import Path

from ..collect.raw_contract import integer
from ..secrets import Secret, public
from .recovery_contract import RecoveryContractError, document, identifier


class QueueIncomplete(RecoveryContractError):
    pass


class ProducerWindowLimit(QueueIncomplete):
    pass


class QueueIdleTimeout(QueueIncomplete):
    pass


class StaleQueueOwner(RecoveryContractError):
    pass


class ArtifactDomainUnavailable(RecoveryContractError):
    pass


class HistoryReadLimit(RecoveryContractError):
    def __init__(self, message, next_cursor):
        super().__init__(message)
        self.next_cursor = next_cursor


@dataclass(frozen=True)
class QueueLimits:
    max_pools: int = 128
    max_tasks: int = 100000
    max_payload_bytes: int = 256*1024
    max_result_bytes: int = 256*1024
    max_control_bytes: int = 512*1024**2
    max_object_bytes: int = 1024**3
    max_output_bytes: int = 64*1024**3
    max_scratch_bytes: int = 64*1024**3
    max_wire_bytes: int = 192*1024**3
    max_requests: int = 1000000
    max_commits: int = 216384
    max_producer_commits: int = 16384
    max_reservations: int = 100000
    max_control_grants: int = 262144
    max_workers: int = 32
    max_active_tasks: int = 256
    max_claims_per_task: int = 16
    max_elapsed_s: int = 86400
    max_result_total_bytes: int = 2*1024**3
    db_timeout_s: float = 5

    def __post_init__(self):
        for name,low,high in [('max_pools',1,128),('max_tasks',1,1000000),
            ('max_payload_bytes',1024,256*1024),('max_result_bytes',1024,256*1024),
            ('max_control_bytes',1024**2,64*1024**3),('max_object_bytes',1,1024**4),
            ('max_output_bytes',1,1024**5),('max_scratch_bytes',1,1024**5),('max_wire_bytes',1,1024**5),
            ('max_requests',1,10000000),('max_commits',1,4000000),('max_producer_commits',1,1000000),('max_reservations',1,100000),
            ('max_control_grants',1,262144),('max_workers',1,1024),('max_active_tasks',1,4096),('max_claims_per_task',1,64),
            ('max_elapsed_s',1,7*86400),('max_result_total_bytes',1,1024**5)]:
            integer(getattr(self,name),name,low,high)
        if type(self.db_timeout_s) not in (int,float) or not math.isfinite(self.db_timeout_s) or not 0<self.db_timeout_s<=5:
            raise ValueError('Database timeout must be finite and at most 5 seconds')


@dataclass(frozen=True)
class QueueConfig:
    backend: str
    queue_id: str
    path: str | None = None
    dsn: Secret | None = None
    namespace: str = 'demiflow_queue_v2'
    limits: QueueLimits = field(default_factory=QueueLimits)

    def __post_init__(self):
        if self.backend not in {'sqlite','postgresql'}:
            raise ValueError('Queue backend must be sqlite or postgresql')
        identifier(self.queue_id)
        identifier(self.namespace)
        if not self.namespace.replace('_','').isalnum() or not self.namespace[0].isalpha() or len(self.namespace)>63:
            raise ValueError('Invalid SQL queue namespace')
        if self.backend == 'sqlite':
            if type(self.path) is not str or not Path(self.path).is_absolute() or len(self.path)>4096 or self.dsn is not None:
                raise ValueError('SQLite queue requires an absolute local path and no DSN')
        elif type(self.dsn) is not Secret or self.path is not None:
            raise ValueError('PostgreSQL queue requires a Secret DSN and no path')
        if type(self.limits) is dict:
            object.__setattr__(self,'limits',QueueLimits(**self.limits))
        if type(self.limits) is not QueueLimits:
            raise ValueError('limits requires QueueLimits')

    def declaration(self):
        return dict(backend=self.backend,queue_id=self.queue_id,path=self.path,
                    dsn=public(self.dsn),namespace=self.namespace,limits=asdict(self.limits))


def queue_config(value):
    if type(value) is QueueConfig:
        return value
    if type(value) is dict:
        return QueueConfig(**value)
    raise TypeError('Use a v2 QueueConfig; legacy channels require an explicit new store')


@dataclass(frozen=True)
class ProducerSourceSpec:
    kind: str
    source_id: str
    uri: str
    sha256: str | None = None
    version: int | str | None = None
    max_rows: int = 100000
    max_bytes: int = 256*1024**2

    def __post_init__(self):
        if self.kind not in {'manifest','records','keywords','lance'}:
            raise ValueError('Unsupported fixed producer source kind')
        identifier(self.source_id)
        if type(self.uri) is not str or not 1<=len(self.uri.encode('utf8'))<=4096:
            raise ValueError('Invalid producer source URI')
        if self.sha256 is None and self.version is None:
            raise ValueError('Producer source requires a fixed SHA256 or version')
        if self.sha256 is not None and (type(self.sha256) is not str or len(self.sha256)!=64 or any(c not in '0123456789abcdef' for c in self.sha256)):
            raise ValueError('Invalid producer source SHA256')
        if self.version is not None and (type(self.version) not in (int,str) or type(self.version) is str and len(self.version)>1024):
            raise ValueError('Invalid producer source version')
        integer(self.max_rows,'max_rows',0,1000000)
        integer(self.max_bytes,'max_bytes',1,1024**4)


@dataclass(frozen=True)
class ProducerRef:
    queue_uuid: str
    pool: str
    generation: str
    producer_id: str
    source: ProducerSourceSpec
    epoch: int
    token: str = field(repr=False)


@dataclass(frozen=True)
class QueueSnapshot:
    queue_uuid: str
    pool: str
    generation: str
    kind: str
    cursor: int

    def __post_init__(self):
        for v in (self.queue_uuid,self.pool,self.generation):
            identifier(v)
        if self.kind not in {'tasks','results','observations'}:
            raise ValueError('Invalid queue snapshot kind')
        integer(self.cursor,'cursor',0,2**63-1)


@dataclass(frozen=True)
class HistoryReadLimits:
    max_rows: int = 128
    max_record_bytes: int = 256*1024
    max_total_record_bytes: int = 32*1024**2
    max_hash_bytes: int = 1024**3
    timeout_s: float = 600
    page_rows: int = 32
    page_bytes: int = 1024**2

    def __post_init__(self):
        for name,low,high in [('max_rows',1,1000000),('max_record_bytes',1024,256*1024),
            ('max_total_record_bytes',1024,1024**4),('max_hash_bytes',0,1024**5),
            ('page_rows',1,128),('page_bytes',1024,32*1024**2)]:
            integer(getattr(self,name),name,low,high)
        if type(self.timeout_s) not in (int,float) or not math.isfinite(self.timeout_s) or not 0<self.timeout_s<=86400:
            raise ValueError('History timeout must be finite')


def task_message(task_id, payload, *, pool='default'):
    identifier(task_id)
    identifier(pool)
    if type(payload) is not dict or '_queue_delivery' in payload:
        raise ValueError('Task payload requires a record without reserved delivery fields')
    document(payload,256*1024)
    return dict(task_id=task_id,pool=pool,payload=payload)


from .queue_flow import declare_value as _declare_queue_value
for _value, _kind in ((QueueConfig,'config'), (QueueLimits,'limits'), (ProducerSourceSpec,'source'),
                       (QueueSnapshot,'snapshot'), (HistoryReadLimits,'history_limits')):
    _declare_queue_value(_value, _kind)
