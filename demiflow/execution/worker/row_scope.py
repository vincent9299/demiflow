"""Native stream occurrence/ownership sidecar, never a business-row field."""
from contextvars import ContextVar
from dataclasses import dataclass
from demiflow.execution.contracts.recovery_contract import identity

_CURRENT = ContextVar('demiflow_native_row_scope', default=None)


@dataclass(frozen=True)
class RowScope:
    occurrence: str
    delivery: object = None

    def child(self, index):
        return RowScope(identity([self.occurrence,index]),self.delivery)


@dataclass(frozen=True)
class ScopedRow:
    row: object
    scope: RowScope


def current_row_scope():
    return _CURRENT.get()


def set_row_scope(scope):
    _CURRENT.set(scope)
