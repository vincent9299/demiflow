"""State semantics supplied by actors; persistence owned by the run driver."""
import inspect
import asyncio
import json

from .recovery_contract import document, RecoveryContractError


def owner_of(callable):
    owner = getattr(callable, "_fn", callable)
    return getattr(owner, "__self__", owner)


def _admit_encoding(value, maximum):
    """Count escaped JSON before the encoder allocates a large string token."""
    remaining = maximum
    def charge(amount):
        nonlocal remaining
        remaining -= amount
        if remaining < 0:
            raise RecoveryContractError('State encoding exceeds aggregate byte budget')
    def visit(item):
        if type(item) is str:
            charge(2)
            for offset in range(0, len(item), 4096):
                charge(len(json.encoder.encode_basestring(item[offset:offset+4096]).encode('utf-8')) - 2)
        elif type(item) is dict:
            charge(2 + max(0, len(item)-1))
            for key, child in item.items():
                if type(key) is not str:
                    raise RecoveryContractError('State keys must be strings')
                visit(key)
                charge(1)
                visit(child)
        elif type(item) in (list, tuple):
            charge(2 + max(0, len(item)-1))
            for child in item:
                visit(child)
        elif item is None or type(item) in (int, float, bool):
            charge(len(json.dumps(item, allow_nan=False)))
        else:
            raise RecoveryContractError('State must be strict JSON')
    visit(value)


class OperatorStates:
    def __init__(self, context, restored=None):
        self.context, self.restored = context, restored or {}
        self.owners, self.identities, self.aliases = {}, {}, {}

    def validate(self, owner):
        if inspect.isfunction(owner) or inspect.isbuiltin(owner):
            return
        if not callable(getattr(owner, "snapshot_state", None)) or not callable(getattr(owner, "restore_state", None)):
            raise RecoveryContractError(
                "Actor " + type(owner).__name__ +
                " needs snapshot_state/restore_state; resource handles must not be serialized")

    async def register(self, owner, key):
        self.validate(owner)
        if inspect.isfunction(owner) or inspect.isbuiltin(owner):
            return
        if id(owner) in self.identities:
            self.aliases[key] = self.identities[id(owner)]
            return
        self.aliases[key] = key
        self.identities[id(owner)] = key
        self.owners[key] = owner
        bind = getattr(owner, 'bind_execution', None)
        if bind is not None:
            from .run_context import current_action
            bind(self.context, current_action().action_id + '-' + key)
        if len(self.owners) > 256:
            raise RecoveryContractError("Actor state participant count exceeds budget")
        if key in self.restored:
            value = owner.restore_state(self.restored[key])
            if inspect.isawaitable(value):
                await value
        else:
            value = owner.snapshot_state()
            if inspect.isawaitable(value):
                value = await value
            def validate():
                from ..data.runtime_contract import check_size
                maximum = self.context.limits['max_state_bytes']
                check_size(value, maximum)
                _admit_encoding(value, maximum)
                document(value, maximum)
            task = asyncio.create_task(asyncio.to_thread(validate))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise

    async def snapshot(self, prefixes=None, *, budget=None):
        values = {}
        total = 0
        maximum = self.context.limits['max_state_bytes']
        for key, owner in self.owners.items():
            if prefixes is not None and not any(
                    alias == prefix or alias.startswith(prefix + '-')
                    for alias, canonical in self.aliases.items() if canonical == key
                    for prefix in prefixes):
                continue
            value = owner.snapshot_state()
            if inspect.isawaitable(value):
                value = await value
            remaining = maximum - (budget.snapshot_bytes if budget is not None else total)
            def freeze(value=value, remaining=remaining):
                from ..data.runtime_contract import check_size
                # Account for the copied Python containers before allocating an
                # encoding or detached graph; the owner gate remains held.
                amount = check_size(value, remaining)
                _admit_encoding(value, remaining)
                raw = document(value, remaining)
                amount = max(amount, len(raw))
                return json.loads(raw), amount
            task = asyncio.create_task(asyncio.to_thread(freeze))
            try:
                detached, amount = await asyncio.shield(task)
            except asyncio.CancelledError:
                # Pure bounded encoding must finish before actor cleanup can
                # mutate the live graph supplied by snapshot_state().
                await task
                raise
            total += amount
            if budget is not None:
                budget.snapshot_bytes += amount
            values[key] = detached
        return values
