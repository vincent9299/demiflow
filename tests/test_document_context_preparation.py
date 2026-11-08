"""Selection equivalence and invocation-local reuse, without I/O or models."""
from copy import deepcopy
import hashlib
from types import SimpleNamespace

import pytest

from demiflow.collect.documents import canonical
import demiflow.collect.reading as reading


class ExactCounter:
    unit = 'chars'

    def __init__(self):
        self.prompts = 0
        self.texts = 0

    def text(self, value):
        self.texts += 1
        return len(value)

    def prompt(self, prompt, values):
        self.prompts += 1
        return 37 + len(canonical(values))


def fixture(case='shrink'):
    documents = []
    for di in range(3):
        ref = {'uri': f'file:///fixture/{di}', 'sha256': str(di + 1) * 64}
        record = {'document_ref': ref, 'url': f'https://fixture.invalid/{di}',
                  'bindings': ['a', 'b'], 'eligible': True}
        blocks = []
        for bi in range(12):
            text = (f'alpha topic{bi % 3} evidence{di} 中文条件说明。 ' * 3)
            blocks.append({'block_id': f'b{bi:06d}', 'position': bi,
                           'kind': 'heading' if bi in (0, 6) else 'paragraph',
                           'headings': [f'topic{bi % 3}'], 'text': text,
                           'heading_level': 1})
        documents.append((record, {'source': {'url': record['url'], 'title': f'Document{di}'},
                                   'blocks': blocks}))
    spec = {'questions': [{'id': 'a', 'text': 'alpha topic0'},
                          {'id': 'b', 'text': '中文条件 topic2'}],
            'new_chars': 8000, 'total_chars': 12000, 'retained': [], 'requests': [],
            'selection': {'heading_weight': 3, 'include_neighbors': False,
                          'excluded_kinds': ['heading'], 'min_matches': 1, 'fallback_blocks': 1}}
    failed = [{'document_ref': {'uri': 'file:///failed', 'sha256': 'f' * 64}, 'url': '',
               'title': '', 'block_count': 0, 'selected_block_ids': [], 'unread_ranges': [],
               'headings': [], 'heading_total': 0, 'status': 'read_failed', 'reason': 'fixture failed'}]
    maximum = 4800
    if case == 'fit':
        maximum = 100000
    elif case in ('mandatory', 'retained_over'):
        ref = documents[0][0]['document_ref']
        spec['retained'] = [{'evidence_id': ref['sha256'] + ':b000002', 'document_ref': ref,
                             'block_id': 'b000002', 'bindings': ['original']}]
        maximum = 10
        if case == 'retained_over':
            spec['total_chars'] = 1
    elif case == 'explicit':
        spec['requests'] = [{'request_id': 'section', 'document_ref': documents[0][0]['document_ref'],
                             'block_ids': ['b000000', 'b000002'], 'section_id': 'b000000',
                             'bindings': ['b', 'explicit', 'b']},
                            {'request_id': 'invalid', 'document_ref': documents[1][0]['document_ref'],
                             'block_ids': ['missing'], 'bindings': ['a']}]
    elif case == 'no_docs':
        documents = []
    elif case == 'no_questions':
        spec['questions'] = []
    elif case == 'zero_new':
        spec['new_chars'] = 0
    elif case == 'bindings':
        documents[0][0]['bindings'] = ['b']
        documents[1][0]['eligible'] = False
    elif case == 'fallback':
        spec['questions'] = [{'id': 'a', 'text': 'unmatchedzzz'}]
        spec['selection']['min_matches'] = 32
    elif case == 'invalid_policy':
        spec['selection'] = {'heading_weight': 0}
    elif case == 'negative_budget':
        spec['new_chars'] = -1
    elif case == 'missing_retained':
        spec['retained'] = [{'evidence_id': 'missing', 'document_ref': {},
                             'block_id': 'missing', 'bindings': []}]
    counter = ExactCounter()
    ctx = reading.PromptContext(object(), SimpleNamespace(counter=counter, max_input=maximum),
                                lambda row, result: {'question': 'fixed', 'reading': result})
    return spec, documents, failed, ctx


GOLDENS = {'fit': 'e929581baaae46787d57630fffce65dfb3c5fdc694e4547bc3ddd69a6cabff83',
 'shrink': '5238b1b49826860d3e688fd84abe1f32de47650beda86c3169f0e1129d35c7bc',
 'mandatory': 'd07570fbc934070306c561a368c4e8e2c880b3ac7bb269193ae56adb76fa894d',
 'retained_over': 'debf2a2d002207b7ca79924ea226536b5f1c10d2bed327ff5784d4da3266848e',
 'explicit': 'c2b56a38808b0ff9a0d5c943c24305583606a17fc8ce2597a87c5701be38c280',
 'no_docs': '0dbe7c5d2817d38ba496b657ac84c86a3e0e9f8b241fa70415d0feff8470567d',
 'no_questions': '3270d2fdf3d058d56e1723b3ee6642a0b6352f5a2dce55cd5a359265e181869a',
 'zero_new': '3270d2fdf3d058d56e1723b3ee6642a0b6352f5a2dce55cd5a359265e181869a',
 'bindings': '4a9ea368a1da48cc0c22c0340fc273e3fb909dfec3ad4181b2b801103b43a8c8',
 'fallback': 'c6c7e21fb6db56581ffedf67783c5a5bcaeedcf6c458a74b065d7e2f13ff36b4'}


@pytest.mark.parametrize('case', ['fit', 'shrink', 'mandatory', 'retained_over', 'explicit',
                                 'no_docs', 'no_questions', 'zero_new', 'bindings', 'fallback'])
def test_complete_selection_matches_frozen_native_result(case):
    spec, documents, failed, ctx = fixture(case)
    original = deepcopy((spec, documents, failed))
    out = reading.select_context({}, spec, documents, failed, ctx)
    assert hashlib.sha256(canonical(out).encode()).hexdigest() == GOLDENS[case]
    assert (spec, documents, failed) == original


@pytest.mark.parametrize('case, message', [('invalid_policy', 'below minimum'),
                                         ('negative_budget', 'nonnegative'),
                                         ('missing_retained', 'unavailable')])
def test_original_errors_are_preserved(case, message):
    spec, documents, failed, ctx = fixture(case)
    with pytest.raises(ValueError, match=message):
        reading.select_context({}, spec, documents, failed, ctx)


def test_shrink_reuses_ranking_but_keeps_exact_counting(monkeypatch):
    spec, documents, failed, ctx = fixture()
    calls = []
    original_terms = reading.terms
    def counted(value):
        calls.append(value)
        return original_terms(value)
    monkeypatch.setattr(reading, 'terms', counted)
    out = reading.select_context({}, spec, documents, failed, ctx)
    assert ctx.budget.counter.prompts == 8
    assert len(calls) == 3 * (2 * 12 + 2)
    assert out['prompt_chars'] <= ctx.budget.max_input
    assert out['material_chars'] <= spec['new_chars']
    assert ctx.budget.counter.texts > 8


def test_reuse_cannot_cross_calls_or_leak_returned_bindings():
    spec, documents, failed, ctx = fixture('fit')
    first = reading.select_context({}, spec, documents, failed, ctx)
    first['selected'][0]['bindings'].append('mutation')
    spec['questions'] = [{'id': 'a', 'text': 'topic1'}]
    second = reading.select_context({}, spec, documents, failed, ctx)
    assert second['selected']
    assert all(s['bindings'] == ['a'] for s in second['selected'])
    documents[0][1]['blocks'][1]['text'] = 'changed complete block'
    third = reading.select_blocks(spec, documents, ctx.budget.counter)
    assert any(m['text'] == 'changed complete block' for m in third['materials'])


def offline_bpe_counter():
    """Real local BPE with boundary merges; no encoding download/model claim."""
    tiktoken = pytest.importorskip('tiktoken')
    from demiflow.operator_llm.tokens import TextTokenCounter
    class Counter(TextTokenCounter):
        def __init__(self):
            ranks = {bytes([i]): i for i in range(256)}
            ranks.update({b'ab': 256, b'},': 257, b'},{': 258, b'\":': 259})
            self.encoding = tiktoken.Encoding(name='selection-fixture-local-bpe',
                pat_str=r'[A-Za-z]+|[0-9]+|[^A-Za-z0-9]+', mergeable_ranks=ranks, special_tokens={})
            self.profile = {'reply_tokens': 3, 'tokens_per_message': 3}
    return Counter()


def native_token_context(maximum):
    from demiflow.operator_llm.template import compile_template
    template = compile_template('Read: {{ payload | json }}')
    prompt = SimpleNamespace(name='local-bpe-fixture', version='1',
        model=SimpleNamespace(name='synthetic-no-model'), input_mode='template',
        arguments=template.arguments, template=template, response_schema=None,
        response_format='text', systemone={})
    return reading.PromptContext(prompt, SimpleNamespace(counter=offline_bpe_counter(), max_input=maximum),
                                 lambda row, out: {'payload': out})


def test_nonadditive_bpe_still_checks_complete_serialized_context():
    counter = offline_bpe_counter()
    assert counter.text('a') + counter.text('b') > counter.text('ab')
    spec, documents, failed, original_ctx = fixture('shrink')
    spec['new_tokens'] = spec.pop('new_chars')
    spec['total_tokens'] = spec.pop('total_chars')
    ctx = native_token_context(8000)
    out = reading.select_context({}, spec, documents, failed, ctx)
    before_metric = {k: v for k, v in out.items() if k != 'prompt_tokens'}
    assert out['prompt_tokens'] == counter.prompt(ctx.prompt, ctx.build_inputs({}, before_metric))
    assert out['prompt_tokens'] <= ctx.budget.max_input
    assert out['material_tokens'] == counter.text(canonical(out['materials']))
    assert out['material_tokens'] <= spec['new_tokens']
