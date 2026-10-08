"""Public, declarative request admission shared by native model operators."""
from .execution.adaptive_requests import request_admission_policy


def request_admission_config(value=None, *, concurrency=None):
    """Normalize an adaptive policy without starting workers or model services.

    None preserves fixed operator concurrency. A policy only controls fresh
    requests; it does not change model inputs, retry calls, or resize services.
    """
    if concurrency is not None and (type(concurrency) is not int or concurrency < 1):
        raise ValueError('concurrency must be a positive integer')
    policy = request_admission_policy(value)
    if policy and concurrency is not None and policy['max_concurrency'] > concurrency:
        raise ValueError('Adaptive maximum must not exceed concurrency')
    return policy


def _resolve_request_gate(*, policy, gate, concurrency):
    if policy is not None and gate is not None:
        raise ValueError('Choose request_policy or request_gate, not both')
    normalized = request_admission_config(policy, concurrency=concurrency)
    if normalized is None:
        return gate
    from .execution.adaptive_requests import AdaptiveRequestGate
    return AdaptiveRequestGate(normalized)


__all__ = ['request_admission_config']
