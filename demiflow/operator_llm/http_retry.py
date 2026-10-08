"""Explicit bounded retries of complete saved HTTP errors, never uncertain calls."""
import math
from dataclasses import dataclass


def http_error_retry_policy(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {'statuses', 'max_retries', 'backoff_s'}:
        raise ValueError('http_error_retry requires statuses, max_retries and backoff_s')
    statuses, count, delays = value['statuses'], value['max_retries'], value['backoff_s']
    if (not isinstance(statuses, (list, tuple)) or not statuses
            or any(type(s) is not int or not 400 <= s <= 599 for s in statuses)
            or len(set(statuses)) != len(statuses)):
        raise ValueError('Retry statuses must be distinct HTTP errors')
    if type(count) is not int or not 1 <= count <= 5:
        raise ValueError('HTTP max_retries must be in 1..5')
    if (not isinstance(delays, (list, tuple)) or len(delays) != count
            or any(type(d) not in (int, float) or not math.isfinite(d) or not 0 <= d <= 60 for d in delays)):
        raise ValueError('Each HTTP retry requires a finite backoff_s in 0..60')
    return {'statuses': list(statuses), 'max_retries': count, 'backoff_s': list(delays)}


@dataclass(frozen=True)
class HTTPRetryCall:
    request: object
    expected_attempt: int
    expected_status: int
    max_attempts: int
    delay_s: float


def retry_call(request, error, policy):
    from .errors import PromptResponseContractError
    if policy is None or not isinstance(error, PromptResponseContractError):
        return None
    call = getattr(error, 'call', {})
    ref = call.get('response_ref') or {}
    code = getattr(error, 'http_status', None)
    attempt = ref.get('attempt', 1)
    if (call.get('reused') is not False or code not in policy['statuses']
            or not ref.get('journal_path') or ref.get('kind') != 'response'
            or type(attempt) is not int or not 1 <= attempt <= policy['max_retries']):
        return None
    return HTTPRetryCall(request, attempt, code, policy['max_retries'] + 1,
                         policy['backoff_s'][attempt - 1])
