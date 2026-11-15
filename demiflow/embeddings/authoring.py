"""Pure declarations for the existing EmbeddingModel and native embedding actor.

This layer constructs only the platform's immutable value model. It never
constructs the actor, opens a client, reserves a request or reads business data.
"""
import copy
from dataclasses import asdict
import json
import re
from ..data.flow_contract import field, scalar, require_fields


def normalize_model(arguments):
    from .model import EmbeddingModel
    model = EmbeddingModel(**arguments)
    values = asdict(model)
    if model.dimensions > 65536 or any(len(values[n]) > 4096 for n in ('name','revision','base_url')):
        raise ValueError('Embedding model declaration exceeds its bounded value contract')
    if model.api_key_env and not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,255}', model.api_key_env):
        raise ValueError('Embedding credential must be an environment variable name')
    if len(json.dumps(values, allow_nan=False).encode()) > 65536:
        raise ValueError('Embedding model declaration exceeds 64 KiB')
    return values


def analyze_model(arguments, *, inputs, callables, resources):
    return {'fields': {}, 'driver': 'pull', 'value_type': scalar('embedding_model'),
            'value': {'embedding_model': arguments}}


def normalize_embedding(arguments):
    from .config import embedding_execution_config
    values = dict(arguments)
    declaration = values['model']
    if type(declaration) is not dict or set(declaration) != {'embedding_model'}:
        raise ValueError('Embedding model must use the original native EmbeddingModel value')
    values['model'] = normalize_model(declaration['embedding_model'])
    if values['service'] is not None or values['request_gate'] is not None:
        raise ValueError('Managed embedding requires an explicit HTTP endpoint and declarative request_policy')
    values.update(embedding_execution_config(**{k: values[k] for k in (
        'batch_size','concurrency','queue_depth','flush_interval','prefetch_batches',
        'max_requests','request_policy','options')}))
    if type(values['max_requests']) is not int or not 1 <= values['max_requests'] <= 2**40:
        raise ValueError('Managed embedding requires a finite positive request budget <= 2**40')
    if values['concurrency']+values['prefetch_batches'] > 64 or (values['queue_depth'] or 1) > 256 or values['batch_size'] > 10000:
        raise ValueError('Embedding concurrency, queue or batch exceeds managed bounds')
    options=values['options']
    if options['sqlite_journal'] is not None or options['profile_path'] is not None:
        raise ValueError('Managed embedding uses the original run journal and observations')
    if options['trust_env'] is not False:
        raise ValueError('Managed embedding requires trust_env=False')
    if any(options[k]>3600 for k in ('timeout_s','connect_timeout_s','read_timeout_s','write_timeout_s','pool_timeout_s')):
        raise ValueError('Embedding HTTP deadlines must be bounded within 3600 seconds')
    if any(options[k]>64 for k in ('io_workers','prepare_workers','response_workers')):
        raise ValueError('Embedding I/O worker counts exceed managed bounds')
    bindings=values['inputs']
    if type(bindings) is not dict or len(bindings)!=1 or next(iter(bindings)) not in {'text','image','document'} or any(type(v) is not str or not v for v in bindings.values()):
        raise ValueError('Embedding inputs require exactly one text/image/document column')
    destinations=[values[k] for k in ('output','call_output','error_output') if values[k] is not None]
    if not values['output'] or any(type(v) is not str or not v for v in destinations) or len(set(destinations))!=len(destinations):
        raise ValueError('Embedding output columns must be nonempty and distinct')
    values['label']=values['label'] or 'map_embeddings'
    if type(values['label']) is not str or len(values['label'])>256:
        raise ValueError('Embedding label must be a bounded string')
    return values


def analyze_embedding(arguments, *, inputs, callables, resources):
    from .model import EmbeddingModel
    a=arguments; model=EmbeddingModel(**a['model']); schema=inputs[0]
    require_fields(schema,list(a['inputs'].values()))
    modality,column=next(iter(a['inputs'].items()))
    if modality != 'text':
        raise ValueError('Managed image/document embedding still requires an explicit object-read resource binding')
    if schema[column]['type']['kind']!='str' or schema[column]['type'].get('nullable') and a['error_output'] is None:
        raise ValueError('Embedding text requires a non-null string field or explicit error_output')
    result=copy.deepcopy(schema)
    result[a['output']]=field({**scalar('list',nullable=a['error_output'] is not None),
                              'item':scalar('float'),'length':model.dimensions})
    for key in ('call_output','error_output'):
        target=a[key]
        if target is None:continue
        if target in schema:raise ValueError('Embedding evidence/error output cannot overwrite an input field')
        typ=scalar('str',nullable=True) if key=='error_output' else {
            **scalar('record',nullable=True), 'fields':{
                'request_id':field(scalar('str')), 'reused':field(scalar('bool')),
                'computed_at':field(scalar('str')), 'elapsed_s':field(scalar('float')),
                'index':field(scalar('int'))}}
        result[target]=field(typ)
    native=dict(model_contract=model.contract(),inputs=a['inputs'],output=a['output'],
                call_output=a['call_output'],error_output=a['error_output'],options=a['options'],
                max_requests=a['max_requests'],concurrency=a['concurrency'],
                endpoint=model.base_url,api_key_env=model.api_key_env,request_policy=a['request_policy'])
    workers=a['concurrency']+a['prefetch_batches']
    return {'fields':result,'driver':'stream','input_driver':'either','kind':'batch_map',
        'cardinality':'one','order':'unspecified',
        'native':{'contract':'demiflow.embeddings.v2','arguments':native},
        'stream_arguments':dict(max_batch=a['batch_size'],concurrency=workers,
            queue_depth=a['queue_depth'] or workers,flush_interval=a['flush_interval'],
            catch=[],label=a['label'],hard_timeout=None),
        'resources':[{'kind':'network','endpoint':model.base_url},
                     {'kind':'credential','name':model.api_key_env}],
        'request_budget':{'scope':'execution','maximum':a['max_requests']},
        'reserved_bytes':workers*8*(a['options']['max_request_bytes']+a['options']['max_response_bytes']),
        'drain_timeout_s':a['options']['timeout_s']+1,
        'required_distributions':['httpx']}
