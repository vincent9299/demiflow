"""Passive group/prefix declarations for the existing Dataset operations."""
import copy
import math
from .flow_contract import field, scalar, require_fields, transfer_fields


def normalize_grouping(arguments):
    values=dict(arguments)
    keys=values['on']; keys=[keys] if isinstance(keys,str) else list(keys)
    if not keys or len(keys)>32 or len(set(keys))!=len(keys) or any(type(k) is not str or not k for k in keys):
        raise ValueError('Grouping requires bounded distinct key names')
    values['on']=keys
    if type(values['output']) is not str or not values['output'] or values['output'] in {*keys,'group_index','group_last'}:
        raise ValueError('Grouping output must be distinct from keys and group metadata')
    for name in ('max_rows','chunk_bytes','max_groups','buffer_bytes'):
        if type(values[name]) is not int or values[name]<1:
            raise ValueError('Grouping bounds must be positive integers')
    interval=values['flush_interval']
    if interval is not None:
        if type(interval) not in (int,float) or not math.isfinite(interval) or not 0<interval<=3600:
            raise ValueError('Streaming grouping requires a positive finite flush_interval')
        if values['max_rows']>10000 or values['max_groups']>1024 or max(values['chunk_bytes'],values['buffer_bytes'])>1024**3:
            raise ValueError('Streaming grouping bounds exceed the native limits')
        if values['buffer_bytes']<values['chunk_bytes']:
            raise ValueError('buffer_bytes must cover one chunk_bytes batch')
    return values


def analyze_grouping(arguments, *, inputs, callables, resources):
    a=arguments; schema=inputs[0];require_fields(schema,a['on'])
    for key in a['on']:
        if schema[key]['type']['kind'] not in {'str','int','float','bool'}:
            raise ValueError('Grouping keys require declared scalar fields')
    grouped={**{k:copy.deepcopy(schema[k]) for k in a['on']},
             a['output']:field({**scalar('list'),'item':{**scalar('record'),'fields':copy.deepcopy(schema)}})}
    timed=a['flush_interval'] is not None
    result={'fields':grouped,'driver':'stream' if timed else 'pull','input_driver':'either',
            'cardinality':'zero_or_more','order':'unspecified'}
    if not timed:
        grouped.update(group_index=field(scalar('int')),group_last=field(scalar('bool')))
    else:
        result.update(kind='batch_map',native={'contract':'demiflow.stream_group_batches',
            'arguments':{name:a[name] for name in ('on','output','chunk_bytes','max_groups','buffer_bytes')}},
            stream_arguments={'max_batch':a['max_rows'],'flush_interval':a['flush_interval'],
                'concurrency':1,'queue_depth':1,'catch':[],'label':'group_batches','hard_timeout':None})
    return result


def normalize_prepend(arguments):
    values=dict(arguments)
    for name,lower,upper in [('max_rows',0,10000000),('max_row_bytes',1,64*1024**2)]:
        if type(values[name]) is not int or not lower<=values[name]<=upper:
            raise ValueError('Invalid prepend bound: '+name)
    return values


def analyze_prepend(arguments, *, inputs, callables, resources):
    schema=transfer_fields('union',inputs[0],{},other=inputs[1])
    return {'fields':schema,'driver':'stream','input_driver':'either','kind':'async_map',
        'cardinality':'zero_or_more','order':'prefix_then_main',
        'native':{'contract':'demiflow.stream_prepend',
            'arguments':{name:arguments[name] for name in ('max_rows','max_row_bytes')}},
        'stream_arguments':{'concurrency':1,'queue_depth':1,'execution':'inline',
            'catch':[],'label':'prepend','hard_timeout':None}}
