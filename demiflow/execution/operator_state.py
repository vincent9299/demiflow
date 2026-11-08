"""State semantics supplied by actors; persistence owned by the run driver."""
import inspect

from .recovery_contract import document, RecoveryContractError


def owner_of(callable):
    owner = getattr(callable, "_fn", callable)
    return getattr(owner, "__self__", owner)


class OperatorStates:
    def __init__(self, context, restored=None):
        self.context, self.restored = context, restored or {}
        self.owners, self.identities = {}, {}

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
            return
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
            document(value, self.context.limits["max_state_bytes"])

    async def snapshot(self):
        values = {}
        total = 0
        for key, owner in self.owners.items():
            value = owner.snapshot_state()
            if inspect.isawaitable(value):
                value = await value
            total += len(document(value, self.context.limits["max_state_bytes"]))
            if total > self.context.limits["max_state_bytes"]:
                raise RecoveryContractError("Aggregate actor state exceeds byte budget")
            values[key] = value
        return values
