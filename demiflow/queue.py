"""Public native SQLite/PostgreSQL task channels and bounded history declarations."""
from .execution.queue import (QueueChannel,QueueConfig,QueueLimits,ProducerSourceSpec,ProducerRef,
    QueueSnapshot,HistoryReadLimits,QueueIncomplete,QueueIdleTimeout,StaleQueueOwner,
    ArtifactDomainUnavailable,HistoryReadLimit,task_message)
from .execution.queue_sinks import QueueResultSink
from .execution.queue_contract import ProducerWindowLimit


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
