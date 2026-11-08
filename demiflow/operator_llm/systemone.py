import base64
import json
import math
import re
from collections.abc import Mapping

from .errors import PromptResponseContractError
from .model import ImagePart, TextPart


def validate_definition(value):
    """The existing question, optionally repeated over a bounded row binding.

    Question instructions remain a versioned prompt template. Row values supply
    data only; they cannot supply a different question type or response contract.
    """
    if not isinstance(value, Mapping) or 'for_each' not in value:
        return validate_question(value)
    validate_question({k: v for k, v in value.items() if k != 'for_each'})
    policy = value['for_each']
    if not isinstance(policy, Mapping) or set(policy) != {'input', 'key', 'max_questions', 'max_bytes'}:
        raise ValueError('SystemOne for_each requires input/key/max_questions/max_bytes')
    for key in ('input', 'key'):
        if not isinstance(policy[key], str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', policy[key]):
            raise ValueError('SystemOne for_each input/key must be field identifiers')
    for key, maximum in (('max_questions', 128), ('max_bytes', 1024 * 1024)):
        if type(policy[key]) is not int or not 1 <= policy[key] <= maximum:
            raise ValueError(f'SystemOne for_each {key} must be 1..{maximum}')
    from .template import compile_template
    template = compile_template(value['instructions'])
    if any(p.kind.value not in {'text', 'json'} for p in template.placeholders):
        raise ValueError('SystemOne question instructions support text/JSON fields only')


def _bounded_json(value, maximum):
    """Bound containers/strings before serialization, then check exact UTF-8 bytes."""
    stack = [(value, 0)]
    nodes = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > 65536 or depth > 16:
            raise ValueError('SystemOne questions exceed container budget')
        if isinstance(item, str):
            if len(item) > maximum:
                raise ValueError('SystemOne questions exceed max_bytes')
        elif isinstance(item, Mapping):
            if len(item) > 65536 - nodes:
                raise ValueError('SystemOne questions exceed container budget')
            for key, val in item.items():
                if not isinstance(key, str):
                    raise ValueError('SystemOne question data requires string keys')
                stack.extend(((key, depth + 1), (val, depth + 1)))
        elif isinstance(item, (list, tuple)):
            if len(item) > 65536 - nodes:
                raise ValueError('SystemOne questions exceed container budget')
            stack.extend((val, depth + 1) for val in item)
        elif item is not None and type(item) not in (int, float, bool):
            raise ValueError('SystemOne question data must be JSON serializable')
        if len(stack) + nodes > 65536:
            raise ValueError('SystemOne questions exceed container budget')
    size = 0
    for chunk in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(value):
        size += len(chunk.encode('utf-8'))
        if size > maximum:
            raise ValueError('SystemOne questions exceed max_bytes')


def render_questions(definition, values):
    if 'for_each' not in definition:
        return definition
    from .template import compile_template, render_template
    policy = definition['for_each']
    items = values.get(policy['input'])
    if not isinstance(items, (list, tuple)) or not 1 <= len(items) <= policy['max_questions']:
        raise ValueError('SystemOne for_each requires 1..max_questions rows; skip empty input explicitly')
    _bounded_json(items, policy['max_bytes'])
    template = compile_template(definition['instructions'])
    questions = {}
    used = 2
    for item in items:
        if not isinstance(item, Mapping):
            raise ValueError('SystemOne for_each items must be objects')
        key = item.get(policy['key'])
        if not isinstance(key, str) or not key or len(key) > 256 or key in questions:
            raise ValueError('SystemOne question IDs must be unique nonempty strings of at most 256 characters')
        bound = {name: item[name] for name in template.arguments if name in item}
        # Reserve repeated template expansion before rendering, not after a
        # potentially large sequence has already been constructed.
        reservation = len(template.source.encode('utf-8'))
        for placeholder in template.placeholders:
            encoded = json.dumps(bound.get(placeholder.name), ensure_ascii=False, allow_nan=False)
            reservation += len(encoded.encode('utf-8'))
        if reservation + used > policy['max_bytes']:
            raise ValueError('SystemOne rendered questions exceed max_bytes')
        instructions = ''.join(part.text for part in render_template(template, bound))
        question = {k: v for k, v in definition.items() if k not in {'for_each', 'instructions'}}
        question['instructions'] = instructions
        validate_question(question)
        used += len(json.dumps({key: question}, ensure_ascii=False, allow_nan=False).encode('utf-8'))
        if used > policy['max_bytes']:
            raise ValueError('SystemOne rendered questions exceed max_bytes')
        questions[key] = question
    return {'questions': questions}


def identity_fields(request):
    """Question and criterion order affect native model inputs, even after JSON hashing."""
    if 'questions' not in request.systemone:
        return {}
    questions = request.systemone['questions']
    return {'systemone_question_order': list(questions),
            'systemone_question_criteria_order': {
                key: list(q['criteria']) for key, q in questions.items() if q['type'] == 'choice'}}


def validate_question(value):
    if not isinstance(value, Mapping):
        raise ValueError('SystemOne question must be an object')
    kind = value.get('type')
    expected = {'type','instructions'} | ({'criteria'} if kind=='choice' else set())
    if kind not in {'noul','choice'} or set(value) != expected:
        raise ValueError('SystemOne requires noul (type/instructions) or choice (type/instructions/criteria)')
    if not isinstance(value.get('instructions'), str) or not value['instructions'].strip():
        raise ValueError('SystemOne requires nonempty instructions')
    if kind=='choice':
        criteria = value['criteria']
        if (not isinstance(criteria, Mapping) or not 1 <= len(criteria) <= 255
                or any(not isinstance(k,str) or not k or len(k)>256
                       or v is not None and not isinstance(v,str) for k,v in criteria.items())):
            raise ValueError('SystemOne choice criteria requires 1..255 named options with text or null descriptions')


def request_payload(request):
    if not request.systemone:
        raise ValueError('SystemOne question is required')
    texts = [part for part in request.parts if isinstance(part, TextPart)]
    images = []
    for part in request.parts:
        if isinstance(part, TextPart):
            continue
        if not isinstance(part, ImagePart):
            raise ValueError('SystemOne noul accepts only text and image parts')
        uri = part.image.uri or (
            f'data:{part.image.media_type};base64,'
            f'{base64.b64encode(part.image.data or b"").decode("ascii")}'
        )
        if not uri.startswith('data:image/'):
            raise ValueError('SystemOne images require image data URLs')
        images.append(uri)
    if not texts or len(texts) + len(images) != len(request.parts):
        raise ValueError('SystemOne noul requires exactly one text state and optional images')
    return {
        'model': request.model,
        'state': ''.join(part.text for part in texts),
        'questions': (dict(request.systemone['questions']) if 'questions' in request.systemone
                      else {request.prompt_name: dict(request.systemone)}),
        **({'images': images} if images else {}),
    }


def response_result(request, body):
    multiple = ('systemone_question_order' in request.source if hasattr(request, 'source')
                else 'questions' in request.systemone)
    if multiple:
        questions = (request.source['payload']['questions'] if hasattr(request, 'source')
                     else request.systemone['questions'])
        answers = body.get('answers') if isinstance(body, dict) else None
        if not isinstance(answers, dict) or set(answers) - set(questions):
            raise PromptResponseContractError('SystemOne answers have unknown IDs or an invalid envelope')
        valid, errors = {}, {}
        for key, question in questions.items():
            if key not in answers:
                errors[key] = 'missing_answer'
                continue
            from types import SimpleNamespace
            try:
                answer = answers[key]
                if question['type'] == 'noul' and (not isinstance(answer, dict)
                        or type(answer.get('noul')) not in (int, float)):
                    raise PromptResponseContractError('noul must be a number')
                valid[key] = response_result(SimpleNamespace(prompt_name=key, systemone=question),
                                             {'answers': {key: answer}})['result']
            except PromptResponseContractError:
                errors[key] = 'invalid_answer'
        return {'result': {'answers': valid, 'errors': errors}}
    prompt_name = request.source['stage'] if hasattr(request, 'source') else request.prompt_name
    try:
        answer = body['answers'][prompt_name]
        if not isinstance(answer, dict):
            raise ValueError
        question = (request.source['payload']['questions'][prompt_name] if hasattr(request,'source')
                    else request.systemone)
        if question.get('type') == 'choice':
            probabilities = answer['probabilities']
            if (answer.get('type') != 'choice' or answer['choice'] not in question['criteria']
                    or not isinstance(probabilities, dict) or set(probabilities) != set(question['criteria'])):
                raise ValueError
            for value in probabilities.values():
                if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 0<=value<=1:
                    raise ValueError
            if not math.isclose(sum(probabilities.values()),1.0,rel_tol=1e-6,abs_tol=1e-6):
                raise ValueError
            return {'result':answer}
        probability = answer['noul']
        if answer.get('type') != 'noul' or isinstance(probability, bool):
            raise ValueError
        probability = float(probability)
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise PromptResponseContractError('Invalid SystemOne noul/choice response') from exc
    return {'result': answer}
