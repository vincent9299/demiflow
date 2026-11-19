"""Explicit, observable prerequisites for subsequent native actions."""
from .flow_contract import flow_api, normalize_guard, analyze_guard


@flow_api('require', role='action', execution='action',
          normalize=normalize_guard, analyze=analyze_guard)
def require(condition: bool, message: str = 'Flow prerequisite failed') -> None:
    """Stop the Flow on a false condition; unlike assert, never optimized out."""
    from ..execution.dataset_runtime import execute_guard
    return execute_guard(condition, message)
