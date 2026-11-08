"""Pure structural facts for native raw acquisition and its declarations."""
from functools import partial
from ..data.flow_contract import flow_api,field,scalar,require_fields,literal_type


def _decode(value):
    if type(value) is list:return [_decode(item) for item in value]
    if type(value) is dict:
        if set(value)=={'native_declaration','value'}:
            kind=value['native_declaration'];data=value['value']
            from ..secrets import Secret
            from ..objects import LocalObjectStore
            from .raw_contract import HttpSource,CosSource,FetchPolicy
            if kind=='secret':return Secret(data['secret_env'])
            if kind=='local_store':return LocalObjectStore(**{k:v for k,v in data.items() if k!='layout'})
            if kind=='http_source':return HttpSource(**_decode(data))
            if kind=='cos_source':return CosSource(**_decode(data))
            if kind=='fetch_policy':return FetchPolicy(**_decode(data))
            raise ValueError('Unknown native fetch declaration')
        if set(value)=={'secret_env'}:
            from ..secrets import Secret
            return Secret(value['secret_env'])
        return {key:_decode(item) for key,item in value.items()}
    return value


def normalize_declaration(kind,arguments):
    from ..secrets import Secret,public
    from ..objects import LocalObjectStore
    from .raw_contract import HttpSource,CosSource,FetchPolicy
    target={'secret':Secret,'local_store':LocalObjectStore,'http_source':HttpSource,
            'cos_source':CosSource,'fetch_policy':FetchPolicy}[kind]
    obj=target(**_decode(arguments))
    value=(obj.recovery_identity() if kind=='local_store' else public(obj) if kind=='secret' else obj.declaration())
    return {'native_declaration':kind,'value':value}


def analyze_declaration(a,*,inputs,callables,resources):
    return dict(fields={},driver='pull',cardinality='none',order='preserved',value=a,value_type=literal_type(a))


def declare_value(target,kind):
    return flow_api('declare_'+kind,role='value',normalize=partial(normalize_declaration,kind),
                    analyze=analyze_declaration)(target)


def normalize_fetch(arguments):
    from ..execution.raw_fetch import RawFetch
    actor=RawFetch(**_decode(arguments))
    return dict(request=actor.request,output=actor.output,store=actor.store.recovery_identity(),
        policy=actor.policy.declaration(),concurrency=actor.concurrency,queue_depth=actor.queue_depth,label=actor.label)


def fetch_resources(a):
    return [dict(kind='objects',parameter='objects',root=a['store']['directory'],relocations=[],
                 layout='sha256_prefix2',access='write')]


def record(types,*,nullable=False,optional=()):
    return {**scalar('record',nullable=nullable),'fields':{name:field(value,required=name not in optional) for name,value in types.items()}}


def receipt_type():
    text=scalar('str');number=scalar('int');boolean=scalar('bool');maybe=scalar('str',nullable=True)
    source=record(dict(uri=text,final_uri=maybe,final_origin=maybe,locator_redacted=boolean,redirect_hops=number,
        http_status=scalar('int',nullable=True),content_type=maybe,content_encoding=maybe,etag=maybe,cos_version_id=maybe,
        source_version=record({'kind':text,'value':text},nullable=True),pinned=boolean),
        optional=('http_status','content_type','content_encoding','etag','cos_version_id'))
    domain=record(dict(domain_id=text,read_domain=text,declared_domain=maybe,host_identity=text,
        store_id=text,physical_root=text,marker_digest=text),nullable=True)
    return record(dict(status=text,terminal=boolean,raw_ref=record({'uri':text,'sha256':text},nullable=True),
        size_bytes=scalar('int',nullable=True),partial_bytes=number,reason=maybe,detail=maybe,source=source,
        checks=record(dict(expected_size=scalar('int',nullable=True),expected_sha256=maybe,
            size_verified=boolean,sha256_verified=boolean)),attempts=number,http_requests=number,retrieved_at=text,
        acquisition_ref=record({'commit_id':text,'scope_id':text}),storage_domain=domain))


def analyze_fetch(a,*,inputs,callables,resources):
    from ..objects import LocalObjectStore
    from ..execution.raw_fetch import RawFetch,raw_fetch_facts
    schema=inputs[0];require_fields(schema,[a['request']])
    request=schema[a['request']]['type']
    if request['kind']!='record' or request.get('nullable'):
        raise ValueError('Fetch request must be a non-null record')
    require_fields(request['fields'],['uri'])
    uri=request['fields']['uri']['type']
    if uri['kind']!='str' or uri.get('nullable'):raise ValueError('Fetch request uri must be a non-null string')
    if a['output'] in schema:raise ValueError('Fetch output must not overwrite an input field')
    if a['store']['directory']!=resources['objects']['root']:raise ValueError('Fetch store differs from its registered object root')
    store=LocalObjectStore(**{k:v for k,v in a['store'].items() if k!='layout'})
    policy=_decode(a['policy'])
    from .raw_contract import CosSource
    policy['cos']=[CosSource(**item) if type(item) is dict else item for item in policy['cos']]
    actor=RawFetch(request=a['request'],output=a['output'],store=store,policy=policy,
                   concurrency=a['concurrency'],queue_depth=a['queue_depth'],label=a['label'])
    facts=raw_fetch_facts(actor)
    return dict(fields={**schema,a['output']:field(receipt_type())},cardinality='one',order='unspecified',driver='stream',
        kind='async_map',callable_form='async',placement='inline',scope='stage',native=facts['native'],
        stream_arguments=dict(concurrency=a['concurrency'],queue_depth=a['queue_depth'],catch=[],
            label=a['label'],hard_timeout=None,execution='inline'),reserved_bytes=facts['reserved_bytes'],
        resources=[resources['objects'],dict(kind='network',source='bounded_row_uri',schemes=['http','https','cos'],
            allow_private=a['policy']['http']['allow_private'],redirect_origins=a['policy']['http']['redirect_origins'])])
