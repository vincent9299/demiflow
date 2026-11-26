"""A worker failure outside the fixed vocabulary reaches the caller as its nearest built-in base."""
import errno

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.services.contracts import ModelServiceConfigurationError, ModelServiceError


class BadRecord(ValueError):
    pass


class MissingField(KeyError):
    pass


class Unrelated(Exception):
    pass


class Infeasible(RuntimeError):
    code = 'RESOURCE_INFEASIBLE'


def reject(row):
    raise BadRecord('record %d has no id' % row['n'])


def _round_trip(error, **options):
    try:
        raise error
    except BaseException as caught:
        return restore_failure(failure_payload(caught, **options))


@pytest.mark.parametrize('error,expected', [
    (BadRecord('no id'), ValueError), (MissingField('id'), KeyError),
    (UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'invalid start byte'), ValueError),
    (ConnectionResetError(errno.ECONNRESET, 'reset'), OSError), (RecursionError('too deep'), RuntimeError)])
def test_worker_failure_outside_the_vocabulary_returns_as_its_builtin_base(error, expected):
    restored = _round_trip(error, builtin_base=True)
    assert type(restored) is expected
    assert str(error) in str(restored)
    # The original class stays readable in the worker traceback attached as a note.
    assert type(error).__name__ in ''.join(restored.__notes__)


def test_vocabulary_types_engine_codes_and_baseless_types_are_unchanged():
    assert type(_round_trip(ValueError('plain'), builtin_base=True)) is ValueError
    for error, code in ((DataflowError('BUDGET_EXCEEDED', 'row too large'), 'BUDGET_EXCEEDED'),
                        (Infeasible('does not fit'), 'RESOURCE_INFEASIBLE'), (Unrelated('no base'), 'INTERNAL_PROTOCOL')):
        restored = _round_trip(error, builtin_base=True)
        assert type(restored) is DataflowError and restored.code == code


def test_driver_replies_keep_unknown_types_as_engine_failures():
    # A reply from the driver to a worker: a row ``catch=(ValueError,)`` must not be
    # able to swallow an unknown driver failure as if it were a business error.
    restored = _round_trip(BadRecord('no id'))
    assert type(restored) is DataflowError and restored.code == 'INTERNAL_PROTOCOL'


@pytest.mark.parametrize('error', [ModelServiceError('readiness timed out'), ModelServiceConfigurationError('bad port')])
@pytest.mark.parametrize('options', [{}, {'builtin_base': True}])
def test_model_service_failures_keep_their_type(error, options):
    restored = _round_trip(error, **options)
    assert type(restored) is type(error) and str(restored) == str(error)


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_caller_catches_a_callback_error_by_its_builtin_base(tmp_path, mode):
    with pytest.raises(ValueError, match='record 1 has no id') as failure:
        with RunContext.start(tmp_path / 'run'), data.local_execution(workers=1, worker_mode=mode):
            data.from_items([{'n': 1}]).map(reject).materialize()
    assert type(failure.value) is ValueError
    assert 'BadRecord' in ''.join(failure.value.__notes__)
