"""Public observation must not defer native close/ACK to garbage collection."""
import pytest

from demiflow.observability import observe_rows, observe_batches


@pytest.mark.parametrize('observe', [observe_rows, observe_batches])
def test_closing_observation_closes_the_still_referenced_execution(observe):
    closed = []
    def owned():
        try:
            yield [{'id': 1}]
            yield [{'id': 2}]
        finally:
            closed.append(True)
    original = owned()  # Keep a strong reference: GC cannot supply the close.
    wrapped = observe('owned', original, object(), object(), object())
    assert next(wrapped) == [{'id': 1}]
    assert not closed
    wrapped.close()
    assert closed == [True]
    assert list(original) == []


@pytest.mark.parametrize('observe', [observe_rows, observe_batches])
@pytest.mark.parametrize('business_failure', [False, True])
def test_cleanup_errors_never_hide_business_failure_or_report_false_success(observe, business_failure):
    class Owned:
        def __iter__(self):
            return self
        def __next__(self):
            if business_failure:
                raise ValueError('original business failure')
            return [{'id': 1}]
        def close(self):
            raise RuntimeError('original close failure')
    wrapped = observe('owned', Owned(), object(), object(), object())
    if business_failure:
        with pytest.raises(ValueError, match='original business failure') as caught:
            next(wrapped)
        assert any('original close failure' in note for note in caught.value.__notes__)
    else:
        next(wrapped)
        with pytest.raises(RuntimeError, match='original close failure'):
            wrapped.close()
