"""Native search declarations and bounded response facts; no search scheduling."""
from ..data.datasink import Datasink
from ..execution.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity
from ..secrets import public
from pathlib import Path
from dataclasses import asdict


def is_native_search_actor(actor):
    # Only a search declaration can request this binding. Pure stream users
    # need neither the web operator package nor its optional HTTP client.
    if not hasattr(type(actor), 'bind_search_effects'):
        return False
    from .operators import SearchWeb
    return type(actor) is SearchWeb


def session_declaration(session):
    from .session import WebSession
    if type(session) is not WebSession:
        raise RecoveryContractError('Native search requires exactly WebSession')
    if session.service is not None:
        raise RecoveryContractError('Managed service startup is outside native search acquisition')
    if session.search_routes or session.search_session_pool or session.search_reuse_configs or session.search_reuse_session_pools or session.search_fallback:
        raise RecoveryContractError('Native Dataset search currently requires one bounded search configuration without route pools')
    if session.search_config is None:
        raise RecoveryContractError('Native Dataset search requires SearchConfig; legacy HTTP service remains a lower-level resource')
    declaration = dict(search=session.search_config.snapshot() if session.search_config else None,
        options={key:str(value.absolute()) if isinstance(value,Path) else public(value) for key,value in session.options.items()}, connection_policy=asdict(session.connection_policy),
        reuse=[c.snapshot() for c in session.search_reuse_configs],routes=public(session.search_routes),
        session_pool=public(session.search_session_pool),reuse_pools=public(session.search_reuse_session_pools),
        fallback=public(session.search_fallback),adaptive=public(session.search_adaptive),
        cooldown_s=session.search_route_cooldown_s,route_attempts=session.search_route_attempts,
        failure_limit=session.search_route_failure_limit,failure_scope=session.search_failure_scope,
        request_interval_s=session.search_request_interval_s)
    document(declaration,65536)
    return declaration


def search_facts(actor):
    declaration=actor.recovery_identity()
    config=actor.session.search_config
    if config is not None:
        if config.workers>8 or len(config.engines)>32 or len(config.networks)>16 or config.max_bytes>4*1024**2 or config.max_results>500 or config.retries>3:
            raise RecoveryContractError('Search resource declaration exceeds native source bounds')
        if any(source.get('backend')=='browser' for source in config.source_configs()):
            raise RecoveryContractError('Native Dataset search currently admits HTTP adapter sources; browser execution has a separate retained runtime')
        reserved=32*1024**2+config.workers*8*1024**2+actor.request_concurrency*len(config.engines)*8*config.max_bytes
    else:
        maximum=actor.session.options.get('max_bytes',2*1024**2)
        if type(maximum) is not int or not 1<=maximum<=4*1024**2:
            raise RecoveryContractError('Search response bound exceeds native source budget')
        reserved=16*1024**2+actor.request_concurrency*8*maximum
    return dict(native=declaration, effects=['network','source_response'],
                resources=[dict(kind='network',source='native_search',config=declaration['session'])],
                reserved_bytes=reserved)


class NativeSearchResultSink(Datasink):
    """The native prepared record itself is the immutable response artifact."""
    sink_id='search'

    def __init__(self,action):
        self.action=action
        self.maximum=min(1024**2,action.context.limits['max_row_bytes'])

    def write(self,blocks,context):
        raise RecoveryContractError('Search response facts require native commit')

    def prepare(self,blocks,context):
        raw=document(blocks,self.maximum)
        return dict(format=1,row=blocks,sha256=identity(blocks),bytes=len(raw))

    def _validate(self,prepared):
        if type(prepared) is not dict or set(prepared)!={'format','row','sha256','bytes'} or prepared['format']!=1:
            raise RecoveryContractError('Invalid native search response fact')
        raw=document(prepared['row'],self.maximum)
        if len(raw)!=prepared['bytes'] or identity(prepared['row'])!=prepared['sha256']:
            raise UnresolvedCommit('Fixed native search response changed')

    def inspect(self,prepared,context):
        self._validate(prepared)
        fact=self.action.context.journal.execution_commit(context.commit_id)
        if fact is None or fact['prepared'] is None:return None
        if fact['prepared']['sinks']!={'search':prepared}:
            raise UnresolvedCommit('Search response differs from its native prepared fact')
        return dict(commit_id=context.commit_id,sink_id=self.sink_id,status='committed',
                    data=dict(sha256=prepared['sha256'],bytes=prepared['bytes']))

    def commit(self,prepared,context):
        result=self.inspect(prepared,context)
        if result is None:raise UnresolvedCommit('Search response was not durably prepared')
        return result

    def abort(self,prepared,context):
        return None
