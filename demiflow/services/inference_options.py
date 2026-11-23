"""Shared HTTP actor resource defaults; model/protocol options stay with adapters."""
import math


CALL_OPTION_DEFAULTS = dict(io_workers=2, prepare_workers=2, response_workers=2,
                            keepalive_expiry_s=5, collect_journal_totals=True)


def normalize_call_options(options):
    """Validate shared fields after the caller checks its protocol's allowed keys."""
    result = {**CALL_OPTION_DEFAULTS, **options}
    for key in ('io_workers', 'prepare_workers', 'response_workers'):
        if type(result[key]) is not int or result[key] < 1:
            raise ValueError(key + ' must be a positive integer')
    value = result['keepalive_expiry_s']
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('keepalive_expiry_s must be finite and nonnegative')
    if type(result['collect_journal_totals']) is not bool:
        raise ValueError('collect_journal_totals must be boolean')
    return result
