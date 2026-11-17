"""Bounded row/group transforms. No models, Dataset actions, files or ledgers.

The five copied prompts retain their original text/schema. These adapters expose
the data needed by those prompts and retain failed experiment positions.
"""
import hashlib
import json

import pyarrow as pa


TIER_LIMITS = (('short', 50), ('medium', 100), ('long', 170))
TIERS = tuple(name for name, _ in TIER_LIMITS)
CHECKS = ('question_validity', 'reference_correctness', 'evidence_sufficiency', 'condition_applicability')
QUALITY_CHECKS = ('blur', 'degradation', 'exposure', 'cropping_occlusion', 'watermark',
                  'overlay_url', 'promotional_content', 'placeholder', 'grid_collage')
MODES = ('detail_only', 'detail_reference')
REF = pa.struct([('uri', pa.string()), ('sha256', pa.string())])
PAIR_SCHEMA = pa.schema([
    ('pair_id', pa.string()), ('concept_id', pa.string()), ('concept', pa.string()),
    ('description', pa.string()), ('slot', pa.string()), ('questions_json', pa.large_string()),
    ('reference_route', pa.string()), ('reference_instruction', pa.large_string()),
    ('reference_image', REF), ('ready', pa.bool_()), ('status', pa.string()),
    ('qualification', pa.string()), ('error', pa.large_string()),
    ('provenance_json', pa.large_string()), ('draft_json', pa.large_string()),
    ('pair_sha256', pa.string()), ('execution_kind', pa.string()),
])
RESULT_SCHEMA = pa.schema([
    ('row_id', pa.string()), ('record_kind', pa.string()), ('concept_id', pa.string()),
    ('pair_id', pa.string()), ('sample_id', pa.string()), ('tier', pa.string()),
    ('conditioning_mode', pa.string()), ('rubric', pa.string()), ('instruction', pa.large_string()),
    ('reference_image', REF), ('generated_image', REF), ('score', pa.float64()),
    ('status', pa.string()), ('error', pa.large_string()), ('details_json', pa.large_string()),
    ('delivery_json', pa.large_string()), ('summary_json', pa.large_string()),
    ('execution_kind', pa.string()),
])
PAIR_FIELDS = tuple(PAIR_SCHEMA.names)
RESULT_FIELDS = tuple(RESULT_SCHEMA.names)


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(packed(value).encode()).hexdigest()


def relation_passes(row):
    return row['relation_decision'] == 'pass'


def quality_input(row):
    return {**row, 'payload': {'images': [{'image_id': row['image_id']}]},
            'images': [row['pixels']] if row.get('pixels') is not None else []}


def has_images(row):
    return bool(row.get('images')) and not row.get('selection_error')


def quality_result(row):
    answer = row.get('quality_answer') or {}
    images = answer.get('images', [])
    valid = len(images) == 1 and images[0].get('image_id') == row['image_id']
    checks = images[0].get('checks', []) if valid else []
    valid = valid and [c.get('item') for c in checks] == list(QUALITY_CHECKS)
    passed = (valid and not row.get('quality_error') and not row.get('read_error')
              and images[0]['decision'] == 'pass' and all(c['decision'] == 'pass' for c in checks))
    problem = row.get('quality_error') or row.get('read_error')
    if not valid: problem = problem or 'Quality response did not retain the actual image and all nine checks'
    return {**row, 'quality_passed': bool(passed), 'quality_problem': str(problem) if problem else None}


def keep_quality_evidence(row):
    # A completed rejection may be omitted from candidates. An unavailable
    # review must reach the concept group, not turn into an invented empty pool.
    return row['quality_passed'] or bool(row['quality_problem'])


def collect_candidates(acc, row):
    members = list(acc['members']) if acc else []
    errors = list(acc['quality_errors']) if acc else []
    if len(members) + len(errors) >= 8 or any(m['image_id'] == row['image_id'] for m in [*members, *errors]):
        raise ValueError('Reference example admits at most eight unique candidate images per concept')
    if row['quality_problem']:
        errors.append({'image_id': row['image_id'], 'error': row['quality_problem']})
    else:
        members.append({k: row[k] for k in ('image_id', 'image', 'pixels')})
    return {'concept_id': row['concept_id'], 'members': sorted(members, key=lambda r: r['image_id']),
            'quality_errors': errors}


def focus_input(row):
    members = row.get('members') or []
    return {**row, 'members': members, 'payload': {
        'concept_id': row['concept_id'], 'concept': row['concept'], 'description': row['description'],
        'images': [{'image_id': m['image_id']} for m in members]},
        'images': [m['pixels'] for m in members],
        'selection_error': packed(row['quality_errors']) if row.get('quality_errors') else None}


def focus_result(row):
    answer = row.get('focus_answer') or {}
    members = row['members']; ids = [m['image_id'] for m in members]
    error = row.get('focus_error') or row.get('selection_error')
    selected = []
    if members and not error:
        rows = answer.get('images', [])
        if answer.get('concept_id') != row['concept_id'] or sorted(r.get('image_id') for r in rows) != sorted(ids):
            error = 'Focus response must retain each actual candidate once'
        else:
            selected = [r['image_id'] for r in rows if r['decision'] == 'select'
                        and all(r[k]['decision'] == 'pass' for k in ('focus', 'sufficiency', 'fidelity'))]
            if sorted(selected) != sorted(answer.get('selected_image_ids', [])):
                error = 'Focus selected IDs disagree with the three required checks'
    return {**row, 'selected': [m for m in members if m['image_id'] in selected] if not error else [],
            'selection_error': str(error) if error else None}


def author_input(row, *, execution_kind):
    refs = row['selected']
    context = {'phase': 'author', 'concept_name': row['concept'], 'concept_id': row['concept_id'],
        'identity_description': row['description'], 'materials': [], 'existing_pairs': [],
        'requested_case_ids': ['01', '02'], 'question_tiers': dict(TIER_LIMITS),
        'candidate_pool_count': len(refs), 'shown_candidate_count': len(refs),
        'candidate_images': [{'image_id': m['image_id'], 'image_number': i + 1} for i, m in enumerate(refs)]}
    return {**row, 'context': context, 'images': [m['pixels'] for m in refs], 'execution_kind': execution_kind}


def author_ready(row):
    return not row.get('selection_error')


def checked_questions(pair):
    questions = pair['questions']
    if set(questions) != set(TIERS):
        raise ValueError('Each pair requires exactly short, medium and long questions')
    lengths = [len(''.join(questions[t].split())) for t in TIERS]
    if not (0 < lengths[0] < lengths[1] < lengths[2]) or any(n > limit for n, (_, limit) in zip(lengths, TIER_LIMITS)):
        raise ValueError('Question tiers must grow within the original 50/100/170 limits')
    return questions


def checked_reuse(pair, candidates):
    if pair['reference_route'] != 'reuse' or pair['selected_image_id'] not in candidates:
        raise ValueError('Reuse must bind an actually supplied image')
    if pair.get('reference_instruction') is not None:
        raise ValueError('Reuse cannot manufacture a generation instruction')
    checks = pair.get('author_checks') or {}
    if set(checks) != set(CHECKS) or any(checks[k]['verdict'] != 'pass' or not checks[k]['evidence'] for k in CHECKS):
        raise ValueError('Reuse requires four explicit author checks; this is not independent evaluation')


def authored_pairs(row):
    answer = row.get('author_answer') or {}
    pairs = answer.get('pairs', [])
    failure = row.get('selection_error') or row.get('author_error')
    if [p.get('case_id') for p in pairs] != ['01', '02']:
        failure = failure or 'Author response did not retain both requested positions'
    candidates = {m['image_id']: m['image'] for m in row['selected']}
    for index, slot in enumerate(('01', '02')):
        out = dict(pair_id=digest([row['concept_id'], slot]), concept_id=row['concept_id'],
            concept=row['concept'], description=row['description'], slot=slot, questions_json=packed({}),
            reference_route=None, reference_instruction=None, reference_image=None, ready=False,
            status='author_failed', qualification=None, error=str(failure) if failure else None,
            provenance_json=packed({'author': row.get('author_call'), 'author_answer': answer}),
            draft_json=None, pair_sha256=None, execution_kind=row['execution_kind'])
        if not failure:
            pair = pairs[index]
            try:
                questions = checked_questions(pair)
                out.update(questions_json=packed(questions), reference_route=pair['reference_route'],
                           reference_instruction=pair.get('reference_instruction'), draft_json=packed(pair))
                if pair['status'] != 'ready':
                    out.update(status='author_needs_review', error=pair.get('issue') or 'Not ready')
                elif pair['reference_route'] == 'reuse':
                    checked_reuse(pair, candidates)
                    out.update(reference_image=candidates[pair['selected_image_id']], ready=True,
                               status='reused', qualification='author_with_existing_image')
                elif pair['reference_route'] == 'synthesize':
                    instruction = pair.get('reference_instruction')
                    if not instruction or pair.get('selected_image_id') is not None or pair.get('author_checks') is not None:
                        raise ValueError('Synthesis must declare a draft without fabricated actual-image checks')
                    if any(''.join(q.split()) == ''.join(instruction.split()) for q in questions.values()):
                        raise ValueError('Reference instruction cannot copy a question tier')
                    out.update(status='draft')
                else:
                    raise ValueError('Ready pair has no supported reference route')
            except (ValueError, TypeError, KeyError) as exc:
                out.update(ready=False, status='author_failed', error=str(exc))
        yield out


def is_reuse(row): return row['status'] == 'reused'
def is_synthesis(row): return row['status'] == 'draft'
def is_blocked(row): return row['status'] not in ('reused', 'draft')


def synthesis_input(row):
    return {**row, 'instruction': row['reference_instruction'], 'images': []}


def generated_reference(row):
    out = {k: row.get(k) for k in PAIR_FIELDS}
    error = row.get('synthesis_error')
    image = row.get('synthesized_image')
    provenance = json.loads(out['provenance_json']); provenance['synthesis'] = row.get('synthesis_call')
    out.update(reference_image=image, status='generated' if image and not error else 'generation_failed',
               error=str(error) if error else None, provenance_json=packed(provenance))
    return out


def finalize_input(row):
    image = row.get('reference_image')
    context = {'phase': 'finalize', 'concept_id': row['concept_id'], 'concept_name': row['concept'],
        'identity_description': row['description'], 'materials': [], 'existing_pairs': [],
        'requested_case_ids': [row['slot']], 'question_tiers': dict(TIER_LIMITS),
        'candidate_images': [{'image_id': image['sha256'], 'image_number': 1}] if image else [],
        'draft': json.loads(row['draft_json'])}
    return {**row, 'context': context, 'images': [row['pixels']] if row.get('pixels') else []}


def finalize_ready(row):
    return row['status'] == 'generated' and bool(row['images']) and not row.get('read_error')


def finalized_pair(row):
    out = {k: row.get(k) for k in PAIR_FIELDS}
    if row['status'] != 'generated': return out
    provenance = json.loads(out['provenance_json']); provenance.update(
        finalize=row.get('finalize_call'), finalize_answer=row.get('finalize_answer'))
    out['provenance_json'] = packed(provenance)
    try:
        error = row.get('finalize_error') or row.get('read_error')
        if error: raise ValueError(str(error))
        pairs = (row.get('finalize_answer') or {}).get('pairs', [])
        if len(pairs) != 1 or pairs[0]['case_id'] != row['slot']:
            raise ValueError('Finalize must retain exactly the supplied draft')
        pair = pairs[0]; questions = checked_questions(pair)
        if pair['status'] != 'ready': raise ValueError(pair.get('issue') or 'Actual image not qualified')
        checked_reuse(pair, {row['reference_image']['sha256']: row['reference_image']})
        out.update(questions_json=packed(questions), ready=True, status='finalized', error=None,
                   qualification='author_with_generated_image')
    except (ValueError, TypeError, KeyError) as exc:
        out.update(ready=False, status='finalization_failed', error=str(exc))
    return out


def lock_pair(row):
    out = {k: row.get(k) for k in PAIR_FIELDS}
    out['pair_sha256'] = digest({k: v for k, v in out.items() if k != 'pair_sha256'})
    return out


def expand_arms(row):
    expected = digest({k: row[k] for k in PAIR_FIELDS if k != 'pair_sha256'})
    if expected != row['pair_sha256']: raise ValueError('Fixed question/image delivery changed')
    questions = json.loads(row['questions_json'])
    for tier in TIERS:
        for mode in MODES:
            instruction = questions.get(tier, '')
            yield {**row, 'tier': tier, 'conditioning_mode': mode, 'instruction': instruction,
                'sample_id': digest([row['pair_sha256'], tier, mode]),
                'source_reference': row['reference_image'],
                'input_reference': row['reference_image'] if mode == 'detail_reference' and row['ready'] else None}


def boogu_input(row, *, profile):
    # Full task text is identical between arms. Only the explicit image list and
    # conditioning_mode differ. A real service must own its model/CFG adapter.
    return {**row, 'images': [row['reference_pixels']] if row.get('reference_pixels') else [],
            'sample_id': digest([row['sample_id'], profile]), 'generation_profile_json': packed(profile)}


def generation_ready(row):
    return row['ready'] and not row.get('reference_read_error')


def scoring_input(row):
    return {**row, 'generated_images': [row['generated_pixels']] if row.get('generated_pixels') else []}


def scoring_ready(row):
    return generation_ready(row) and bool(row['generated_images']) and not row.get('boogu_error') and not row.get('generated_read_error')


def score_result(row, *, rubric):
    answer = row.get(rubric + '_answer')
    error = (row.get('error') or row.get('reference_read_error') or row.get('boogu_error')
             or row.get('generated_read_error') or row.get(rubric + '_error'))
    score = None; status = 'failed'
    if not error and answer is not None:
        if rubric == 'concept':
            score = answer['score']
        else:
            values = [v['score'] for v in answer['dimension_scores'].values() if v['score'] != 'N/A']
            score = 10 * sum(values) / (2 * len(values)) if values and all(v is not None for v in values) else None
        if score is not None and (type(score) not in (int, float) or not 0 <= score <= 10):
            raise ValueError('Score must be null or a value in 0..10')
        status = 'scored' if score is not None else 'uncertain'
    return dict(row_id=digest([row['sample_id'], rubric]), record_kind='score', concept_id=row['concept_id'],
        pair_id=row['pair_id'], sample_id=row['sample_id'], tier=row['tier'], conditioning_mode=row['conditioning_mode'],
        rubric=rubric, instruction=row['instruction'], reference_image=row['input_reference'],
        generated_image=row.get('generated_image'), score=float(score) if score is not None else None,
        status=status, error=str(error or 'No usable model response') if status == 'failed' else None,
        details_json=packed({'answer': answer, 'call': row.get(rubric + '_call'),
                            'generation_call': row.get('boogu_call'), 'source_pair_sha256': row['pair_sha256'],
                            'generation_profile': json.loads(row['generation_profile_json'])}),
        delivery_json=packed(row['delivery']), summary_json=None, execution_kind=row['execution_kind'])


def summary_fold(acc, row):
    if acc is None:
        acc = {k: row[k] for k in ('concept_id', 'conditioning_mode', 'tier', 'rubric', 'execution_kind')}
        acc.update(seen=[], scored=0, failed=0, uncertain=0, total=0.0)
    if row['pair_id'] in acc['seen'] or len(acc['seen']) >= 2:
        raise ValueError('Comparison requires two distinct paired positions per concept/tier/arm/rubric')
    return {**acc, 'seen': [*acc['seen'], row['pair_id']], row['status']: acc[row['status']] + 1,
            'total': acc['total'] + (row['score'] if row['score'] is not None else 0)}


def summary_result(row):
    if len(row['seen']) != 2: raise ValueError('Missing experiment positions cannot shrink the denominator')
    report = {'expected': 2, 'observed': len(row['seen']), 'scored': row['scored'],
              'failed': row['failed'], 'uncertain': row['uncertain'],
              'mean_of_scored': row['total'] / row['scored'] if row['scored'] else None}
    out = {k: None for k in RESULT_FIELDS}
    out.update({k: row[k] for k in ('concept_id', 'conditioning_mode', 'tier', 'rubric', 'execution_kind')})
    out.update(row_id=digest(['summary', out['concept_id'], out['conditioning_mode'], out['tier'], out['rubric']]),
               record_kind='summary', status='complete' if row['scored'] == 2 else 'incomplete', summary_json=packed(report))
    return out
