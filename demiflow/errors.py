"""Public demiflow errors detected by Demiflow and Pipeline carriers."""

from collections.abc import Mapping
from demiflow._compat.error_transport import make_error, validate_error


class DemiflowError(Exception):
    pass


class UnsupportedSourceError(DemiflowError):
    pass


class UnsupportedExecutionOptionError(DemiflowError):
    pass


class PhysicalPlanningError(DemiflowError):
    def __init__(
        self, code: str, *, responsibility: str, stage_ordinal: int = -1
    ) -> None:
        if responsibility not in {"candidate", "target", "runtime"}:
            raise ValueError("PhysicalPlanningError responsibility is invalid")
        self.code = str(code)
        self.responsibility = responsibility
        self.stage_ordinal = int(stage_ordinal)
        super().__init__(f"physical planning failed: {self.code}")


class InvalidLanceRequest(DemiflowError, ValueError):
    """A Demiflow-owned Lance source or write contract is invalid."""


class LanceUnavailable(DemiflowError):
    """The optional Lance runtime is unavailable."""


class LanceResourceNotFound(DemiflowError):
    """A physical Lance dataset is confirmed to be absent."""


class LanceWriteConflict(DemiflowError):
    """The exact expected Lance version is no longer current."""


class LanceExecutionError(DemiflowError):
    """Demiflow detected an invalid Lance or Arrow execution result."""


class LanceWriteError(DemiflowError):
    """A Lance write may have taken effect but cannot be proven committed."""

    def __init__(self, receipt) -> None:
        if receipt.status != "indeterminate" or receipt.error is None:
            raise ValueError("LanceWriteError requires an indeterminate receipt")
        self.receipt = receipt
        self.error = validate_error(receipt.error)
        self.reconciliation_required = True
        super().__init__(self.error["message"])


class AggregationError(DemiflowError):
    pass


class AggregateSerializationError(AggregationError):
    pass


class AggregateStateLimitExceeded(AggregationError):
    pass


class PipelineRunFailure(DemiflowError):
    """Existing Python API carrier for a transported Pipeline error value."""

    def __init__(self, error) -> None:
        self.error = (
            validate_error(error)
            if isinstance(error, Mapping)
            else make_error(
                module=type(self).__module__,
                type_name=type(self).__name__,
                message=str(error),
            )
        )
        super().__init__(self.error["message"])


class PipelineRunTimeout(PipelineRunFailure):
    pass


class PipelineRunCancelled(PipelineRunFailure):
    pass


class PipelineRunIndeterminate(PipelineRunFailure):
    """A remote execution effect exists but its terminal state is unknown."""

    def __init__(self, error, *, platform_execution_id: str) -> None:
        self.platform_execution_id = str(platform_execution_id or "")
        if not self.platform_execution_id:
            raise ValueError("indeterminate Run requires platform execution identity")
        super().__init__(error)
