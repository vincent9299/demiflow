import pytest
from demiflow.lance.records import LanceRecordStore
from demiflow.lance.blobs import LanceBlobStore
from demiflow.operator_llm.lance_journal import LancePromptJournal
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.operator_llm.errors import PromptBudgetExceededError


def test_records_pin_updates_and_replay_identity(tmp_path):
    store = LanceRecordStore(tmp_path, 'runs/a/metadata.lance')
    ref = store.put('a', {'x':1})
    store.put('b', {'x':2})
    assert store.put('a', {'x':1}) == ref
    with pytest.raises(ValueError,match='Immutable'): store.put('a', {'x':3})
    updated = store.put('a',{'x':3},immutable=False)
    assert ref.read(tmp_path) == {'x':1}
    assert updated.read(tmp_path) == {'x':3}
    assert store.keys(prefix='a') == {'a'}
    assert store.items(prefix='a') == {'a':{'x':3}}
    assert store.items() == {'a':{'x':3},'b':{'x':2}}


def test_journal_never_retries_uncertain_and_budget_is_durable(tmp_path):
    opts = dict(root=tmp_path, relative_uri='runs/a/calls.lance')
    j = LancePromptJournal(opts, 1)
    assert j.reserve({'prompt':'one'})
    with pytest.raises(UncertainPromptCall): LancePromptJournal(opts).lookup({'prompt':'one'})
    j.response({'prompt':'one'},{'result':'ok'})
    assert j.lookup({'prompt':'one'}) == {'result':'ok'}
    with pytest.raises(PromptBudgetExceededError): LancePromptJournal(opts,1).reserve({'prompt':'two'})


def test_blob_fixed_versions_and_replay(tmp_path):
    store = LanceBlobStore(tmp_path,'runs/a/assets.lance')
    first = store.put(b'first')
    store.put(b'second')
    assert first.read(tmp_path) == b'first'
    assert store.put(b'first') == first
