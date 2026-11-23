"""Pure queue declarations for discovery and explicit typed-check boundaries.

Runtime queue payloads have no declared static schema. Discovery must describe
the capability without guessing business fields or manufacturing an owner ref.
"""
from dataclasses import asdict
from functools import partial

from ..data.flow_contract import flow_api, literal_type


def decode(value):
    from ..collect.fetch_flow import _decode
    if type(value) is dict and set(value) == {'native_queue_declaration', 'value'}:
        from .queue_contract import QueueConfig, QueueLimits, ProducerSourceSpec, QueueSnapshot, HistoryReadLimits
        target = {'config': QueueConfig, 'limits': QueueLimits, 'source': ProducerSourceSpec,
                  'snapshot': QueueSnapshot, 'history_limits': HistoryReadLimits}[value['native_queue_declaration']]
        return target(**{key: decode(item) for key, item in value['value'].items()})
    if type(value) is dict:
        return _decode({key: decode(item) for key, item in value.items()})
    if type(value) is list:
        return [decode(item) for item in value]
    return value


def normalize_declaration(kind, arguments):
    from .queue_contract import QueueConfig, QueueLimits, ProducerSourceSpec, QueueSnapshot, HistoryReadLimits
    target = {'config': QueueConfig, 'limits': QueueLimits, 'source': ProducerSourceSpec,
              'snapshot': QueueSnapshot, 'history_limits': HistoryReadLimits}[kind]
    value = target(**{key: decode(item) for key, item in arguments.items()})
    return {'native_queue_declaration': kind, 'value': value.declaration() if kind == 'config' else asdict(value)}


def analyze_declaration(arguments, *, inputs, callables, resources):
    return dict(fields={}, driver='pull', cardinality='none', order='preserved',
                value=arguments, value_type=literal_type(arguments))


def declare_value(target, kind):
    return flow_api('declare_queue_'+kind, role='value', normalize=partial(normalize_declaration, kind),
                    analyze=analyze_declaration)(target)


def normalize_source(arguments):
    from .queue_source import QueueDatasource
    return QueueDatasource(**{key: decode(value) for key, value in arguments.items()}).declaration()


def normalize_history(arguments):
    from demiflow.execution.sources.queue_history import QueueHistoryDatasource
    return QueueHistoryDatasource(**{key: decode(value) for key, value in arguments.items()}).declaration()


def normalize_enqueue(arguments):
    from demiflow.execution.native_queue.queue_producer import QueueProducer
    from .queue_contract import ProducerRef
    values = {key: decode(value) for key, value in arguments.items()}
    if type(values.get('producer')) is not ProducerRef:
        raise ValueError('enqueue needs its real administrative ProducerRef; static inspection cannot manufacture an owner')
    return QueueProducer(**values).recovery_identity()


def normalize_ack(arguments):
    from .queue_ack import QueueAcknowledge
    return QueueAcknowledge(**{key: decode(value) for key, value in arguments.items()}).recovery_identity()


def analyze_untyped(arguments, *, inputs, callables, resources):
    raise ValueError('Queue payloads have no fixed static schema in this revision; typed Forge admission is unavailable')


def resource(config, access):
    return dict(kind='queue', access=access, backend=config.backend, queue_id=config.queue_id,
                path=config.path, namespace=config.namespace, dsn=config.declaration()['dsn'])
