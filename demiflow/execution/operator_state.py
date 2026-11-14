"""State semantics supplied by actors; persistence owned by the run driver."""
import inspect
import asyncio
import json

from .recovery_contract import document, RecoveryContractError
from .operator_policy import (classify_operator_state, snapshot_auto_state,
                              restore_auto_state)


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
        # Per-owner C04 mode ("hooks"/"auto_json"); a cache of the single policy,
        # never a second checkpoint authority — persistence stays owners->snapshot.
        self.policies = {}

    def validate(self, owner):
        # A class is a factory blueprint, exempt until constructed; a function or
        # builtin carries no cross-row state. A hookless instance is NOT presumed
        # stateless: its recoverability is decided by the single C04 policy at
        # register (auto_json capture, or drop when it bears resources). The only
        # always-invalid declaration is a half-declared adapter — exactly one hook
        # — which the platform could neither snapshot nor restore safely.
        if inspect.isfunction(owner) or inspect.isbuiltin(owner) or inspect.isclass(owner):
            return
        snapshot = callable(getattr(owner, "snapshot_state", None))
        restore = callable(getattr(owner, "restore_state", None))
        if snapshot != restore:
            raise RecoveryContractError(
                "Actor " + type(owner).__name__ +
                " needs both snapshot_state/restore_state; resource handles must not be serialized")

    async def register(self, owner, key):
        self.validate(owner)
        if inspect.isfunction(owner) or inspect.isbuiltin(owner) or inspect.isclass(owner):
            return
        policy = classify_operator_state(owner, phase="instance")
        if policy.mode not in ("hooks", "auto_json"):
            # Native effects own their resources through their explicit recovery
            # adapter. An ordinary factory must not silently lose business state
            # just because constructing it exposed a non-JSON field.
            if not callable(getattr(owner, 'native_stream_recovery', None)):
                raise RecoveryContractError('Actor ' + type(owner).__name__ +
                    ' needs snapshot_state/restore_state: ' + str(policy.reason))
            return
        if id(owner) in self.identities:
            self.aliases[key] = self.identities[id(owner)]
            return
        self.aliases[key] = key
        self.identities[id(owner)] = key
        self.owners[key] = owner
        self.policies[key] = policy.mode
        bind = getattr(owner, 'bind_execution', None)
        if bind is not None:
            from .run_context import current_action
            bind(self.context, current_action().action_id + '-' + key)
        if len(self.owners) > 256:
            raise RecoveryContractError("Actor state participant count exceeds budget")
        maximum = self.context.limits['max_state_bytes']
        if key in self.restored:
            if policy.mode == "hooks":
                value = owner.restore_state(self.restored[key])
                if inspect.isawaitable(value):
                    await value
            else:
                def restore():
                    restore_auto_state(owner, self.restored[key], max_bytes=maximum)
                task = asyncio.create_task(asyncio.to_thread(restore))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
        elif policy.mode == "hooks":
            value = owner.snapshot_state()
            if inspect.isawaitable(value):
                value = await value
            def validate():
                from ..data.runtime_contract import check_size
                check_size(value, maximum)
                _admit_encoding(value, maximum)
                document(value, maximum)
            task = asyncio.create_task(asyncio.to_thread(validate))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        else:
            def admit():
                snapshot_auto_state(owner, max_bytes=maximum)
            task = asyncio.create_task(asyncio.to_thread(admit))
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
            remaining = maximum - (budget.snapshot_bytes if budget is not None else total)
            if self.policies.get(key) == "auto_json":
                def freeze_auto(owner=owner, remaining=remaining):
                    from ..data.runtime_contract import check_size
                    state = snapshot_auto_state(owner, max_bytes=remaining)
                    amount = check_size(state, remaining)
                    raw = document(state, remaining)
                    return json.loads(raw), max(amount, len(raw))
                task = asyncio.create_task(asyncio.to_thread(freeze_auto))
            else:
                value = owner.snapshot_state()
                if inspect.isawaitable(value):
                    value = await value
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
