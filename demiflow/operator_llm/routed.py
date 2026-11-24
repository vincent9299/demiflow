"""One bounded streaming node dispatching already assigned prompt rows.

Selection is a caller-owned row field. Each route retains its native prompt
actor, HTTP journal, budget, metrics and lifetime. No business selection here.
"""
import asyncio

from demiflow.services.request_limits import ServiceStopped, is_cooperative_stop_error
from ..services import ModelServiceError


class RoutedPromptActor:
    label = 'routed_prompt'

    def __init__(self, actors, limits, route, *, isolate_failures=False, max_requests=None):
        self.actors, self.limits, self.route = actors, limits, route
        self.isolate_failures = isolate_failures
        self.max_requests = max_requests
        self.gates = {}

    @property
    def cancel_drain_timeout_s(self):
        return max(a.cancel_drain_timeout_s for a in self.actors.values())

    async def astart(self):
        self.gates = {key: asyncio.Semaphore(n) for key, n in self.limits.items()}
        for actor in self.actors.values():
            await actor.astart()

    async def aclose(self):
        # Each child owns its clients/coordinator. Drain every route even when
        # another child's close fails; the original native journal is borrowed.
        failure = None
        for actor in self.actors.values():
            try:
                await actor.aclose()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                else:
                    failure.add_note('routed prompt cleanup: ' + str(exc))
        self.gates.clear()
        if failure is not None:
            raise failure

    async def __call__(self, row):
        key = row[self.route]
        actor = self.actors[key]
        async with self.gates[key]:
            try:
                result = await actor(row)
                error = result.get(actor.error_output) if actor.error_output else None
                if (self.isolate_failures and error
                        and (error.get('call') or {}).get('http_status') in {401, 403, 404}):
                    result = {**result, actor.error_output: {**error, 'route_unavailable': True}}
                return result
            except (ModelServiceError, ServiceStopped) as exc:
                # Global/user stop signals and storage/programming errors still
                # stop the graph. A provider gate only disables its own route.
                if (not self.isolate_failures or not actor.error_output
                        or is_cooperative_stop_error(exc)):
                    raise
                return {**row, actor.error_output: {
                    'category': 'model_unavailable', 'type': type(exc).__name__,
                    'detail': str(exc), 'call': getattr(exc, 'call', {}),
                    'route_unavailable': True}}
