"""Public native SQLite/PostgreSQL task channels and bounded history declarations."""
from demiflow.execution.native_queue.queue import QueueChannel
from demiflow.execution.contracts.queue_contract import QueueConfig, QueueLimits, ProducerSourceSpec, ProducerRef, QueueSnapshot, HistoryReadLimits, QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable, HistoryReadLimit, task_message
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.contracts.queue_contract import ProducerWindowLimit
# Public discovery installs the existing value handlers at the API assembly boundary.
from demiflow.execution.executors import queue_flow as _queue_flow


def queue_status(config,*,pool='default'):
    """Read existing native counts and stable publication prefixes without writes."""
    channel=QueueChannel(config)
    state=channel.state(pool)
    return {**state,'tasks_snapshot':channel.snapshot(pool,kind='tasks'),
            'results_snapshot':channel.snapshot(pool,kind='results'),
            'observations_snapshot':channel.snapshot(pool,kind='observations')}


__all__=['QueueChannel','QueueConfig','QueueLimits','ProducerSourceSpec','ProducerRef',
    'QueueSnapshot','HistoryReadLimits','QueueResultSink','QueueIncomplete','QueueIdleTimeout',
    'ProducerWindowLimit','StaleQueueOwner','ArtifactDomainUnavailable','HistoryReadLimit','task_message','queue_status']
