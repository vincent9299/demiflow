"""State semantics supplied by actors; persistence owned by the run driver."""
import inspect
import asyncio
import json
import functools
import threading

from .recovery_contract import document, RecoveryContractError
from demiflow.execution.compiler_support.operator_policy import classify_operator_state, _json_fields
from demiflow.execution.kernels.operator_state import snapshot_auto_state, restore_auto_state


def runtime_resource_state(owner):
    """Recognize ordinary lifecycle resources, never pretend to snapshot them.

    Everything outside this small resource set must still be strict JSON. In
    particular an opaque business object is not made stateless by having close().
    """
    if type(owner).__getattribute__ is not object.__getattribute__:
        return False
    fields = getattr(owner, '__dict__', None)
    if type(fields) is not dict or len(fields) > 16384:
        return False
    resources = (type(threading.Lock()), type(threading.RLock()), asyncio.Task,
                 asyncio.Lock, asyncio.Event, threading.Event)
    plain = {k: v for k, v in fields.items() if not isinstance(v, resources)}
    return len(plain) != len(fields) and _json_fields(plain, 16 * 1024**2)[0]


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
    def __init__(self, context, restored=None, *, runtime_only_action=None):
        self.context, self.restored = context, restored or {}
        # Only the local driver may record an attempt-only restriction. Remote
        # workers and strict state consumers continue to reject missing adapters.
        self.runtime_only_action = runtime_only_action
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

    async def register(self, owner, key, *, concurrency=1):
        # partial is a binding wrapper, not an actor whose empty __dict__ can
        # hide the wrapped actor's state. Delegate to the actual callable owner.
        while isinstance(owner, functools.partial):
            if vars(owner):
                raise RecoveryContractError('Stateful partial needs a native state adapter')
            owner = owner_of(owner.func)
        self.validate(owner)
        if inspect.isfunction(owner) or inspect.isbuiltin(owner) or inspect.isclass(owner):
            return
        policy = classify_operator_state(owner, phase="instance")
        if policy.mode not in ("hooks", "auto_json"):
            # Native effects own their resources through their explicit recovery
            # adapter. An ordinary factory must not silently lose business state
            # just because constructing it exposed a non-JSON field.
            if not callable(getattr(owner, 'native_stream_recovery', None)):
                if self.runtime_only_action is not None and runtime_resource_state(owner):
                    # Persist before the first business call, even before the
                    # first checkpoint. A crash must never bypass this restriction.
                    action = self.runtime_only_action
                    self.context.journal.bind_source(self.context.fence,
                        action.action_id + '-state-recovery-restriction', {
                            'schema': 'demiflow.state_recovery_restriction.v1',
                            'recovery': 'attempt_only',
                            'reason': 'Actor runtime resources require snapshot_state/restore_state for resume'})
                    return
                raise RecoveryContractError('Actor ' + type(owner).__name__ +
                    ' needs snapshot_state/restore_state: ' + str(policy.reason))
            return
        if policy.mode == 'auto_json' and vars(owner) and concurrency != 1:
            raise RecoveryContractError('auto_json state requires concurrency 1; '
                                        'declare snapshot_state/restore_state for higher concurrency')
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
