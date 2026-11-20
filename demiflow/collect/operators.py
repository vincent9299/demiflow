"""Native bounded search row transformation and WebSession lifecycle."""
from .connection_manager import WebOperatorLifecycle
from .session import bounded


class SearchWeb(WebOperatorLifecycle):
    concurrency = 1
    # Old sealed declarations predate the process-local effects binding.
    _effects = None
    node_id = None

    def __init__(self, requests, output, session, when, max_candidates, request_concurrency,
                 checkpoint=None, checkpoint_operator='search_web', checkpoint_lease_s=300.0):
        if checkpoint is not None or checkpoint_operator != 'search_web' or checkpoint_lease_s != 300.0:
            from ..execution.recovery_contract import RecoveryContractError
            raise RecoveryContractError('Explicit search checkpoint orchestration is retired')
        if not 1 <= max_candidates <= 500 or not 1 <= request_concurrency <= 4:
            raise ValueError('Native search requires <=500 candidates and <=4 concurrent requests')
        self.requests, self.output, self.session, self.when = requests, output, session, when
        self.max_candidates, self.request_concurrency = max_candidates, request_concurrency
        self.resources = (session,)

    def recovery_identity(self):
        from .search_contract import session_declaration
        from ..execution.recovery_identity import describe
        return dict(contract='demiflow.collect.search_web.v1', requests=self.requests, output=self.output,
                    max_candidates=self.max_candidates, request_concurrency=self.request_concurrency,
                    when=describe(self.when), session=session_declaration(self.session))

    def bind_execution(self, run, node_id):
        # Legacy stream restoration binds the node after starting its WebSession.
        self.node_id = node_id
        if self._effects is not None:
            self._effects.bind_node(run, node_id)

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        if state != {}:
            raise ValueError('Search progress belongs to native response facts')

    @property
    def started(self):
        return self._effects is not None or self._session_lease is not None

    async def astart(self, *, effects):
        self._effects = effects
        self.session._native_execution_admission = self._effects.admit_http
        self.session._native_control_bytes = self._effects.control_bytes
        self.session._native_max_hosts = 64
        await super().astart()

    async def aclose(self):
        try:
            await super().aclose()
        finally:
            if self._effects is not None:
                await self._effects.close()
                self._effects = None
            self.session._native_execution_admission = None
            self.session._native_control_bytes = None
            self.session._native_max_hosts = None

    async def _search(self, request):
        from ..execution.recovery_contract import UnresolvedCommit
        parameters = {key: request[key] for key in ('language', 'pageno', 'safesearch', 'time_range',
                      'engines', 'categories', 'engine_data', 'network', 'fallback_parameters') if key in request}
        result = await self.session.search(request['query'], **parameters)
        if result.get('status') == 'interrupted' or any(
                r.get('status') == 'interrupted' for r in result.get('engine_receipts', [])):
            raise UnresolvedCommit('Search request outcome is unresolved')
        return {**request, **result, 'candidates': result.get('candidates', [])[:self.max_candidates]}

    def validate_row(self, row):
        from ..execution.recovery_contract import RecoveryContractError
        if type(row) is not dict or self.output in row:
            raise RecoveryContractError('Search requires a record and an unused output field')

    async def search_row(self, row, max_bytes):
        from ..execution.recovery_contract import RecoveryContractError, document
        values = row[self.requests]
        if type(values) is not list or len(values) > 32:
            raise RecoveryContractError('Search request list exceeds 32 requests')
        document(values, min(65536, max_bytes))
        for value in values:
            if (type(value) is not dict or type(value.get('query')) is not str
                    or not 1 <= len(value['query']) <= 4096):
                raise RecoveryContractError('Search requires bounded query records')
        if self.when is not None and not self.when(row):
            return row
        results = await bounded(values, self._search, self.request_concurrency, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return {**row, self.output: results}

    async def __call__(self, row):
        return await self._effects.apply(self, row)
