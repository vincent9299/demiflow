"""One native task owns search actors and their shared WebSession lifecycle."""
import asyncio

from .row_scope import RowScope, set_row_scope
from .run_context import _ACTION


class SearchRuntime:
    def __init__(self, action, maximum):
        self.action = action
        self.maximum = maximum
        self.started = []
        self.queue = None
        self.task = None

    async def start(self):
        self.queue = asyncio.Queue(maxsize=self.maximum)
        self.task = asyncio.create_task(self._serve(), name='demiflow-native-search-owner')

    async def call(self, actor, command, row):
        result = asyncio.get_running_loop().create_future()
        await self.queue.put((actor, command, row, result))
        return await asyncio.shield(result)

    async def _serve(self):
        token = _ACTION.set(self.action)
        try:
            while True:
                actor, command, row, reply = await self.queue.get()
                if actor is None:
                    errors = []
                    for owner in reversed(self.started):
                        try:
                            await owner.aclose()
                        except BaseException as exc:
                            errors.append(exc)
                    self.started.clear()
                    if errors:
                        reply.set_exception(errors[0])
                    else:
                        reply.set_result(None)
                    return
                set_row_scope(RowScope(command.record_id))
                try:
                    if actor not in self.started:
                        self.started.append(actor)
                        actor.bind_execution(self.action.context, command.node_id)
                        await actor.astart()
                    reply.set_result(await actor(row))
                except BaseException as exc:
                    reply.set_exception(exc)
                finally:
                    set_row_scope(None)
        finally:
            _ACTION.reset(token)

    async def close(self):
        if self.task is None:
            return
        try:
            await self.call(None, None, None)
        finally:
            await self.task
            self.task = None
