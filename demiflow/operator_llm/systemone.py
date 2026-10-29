import math

from .errors import PromptResponseContractError
from .model import TextPart


def request_payload(request):
    if not request.systemone:
        raise ValueError('SystemOne question is required')
    if len(request.parts) != 1 or not isinstance(request.parts[0], TextPart):
        raise ValueError('SystemOne noul requires exactly one text state')
    return {
        'model': request.model,
        'state': request.parts[0].text,
        'questions': {request.prompt_name: dict(request.systemone)},
    }


def response_result(request, body):
    prompt_name = request.source['stage'] if hasattr(request, 'source') else request.prompt_name
    try:
        answer = body['answers'][prompt_name]
        probability = answer['noul']
        if answer.get('type') != 'noul' or isinstance(probability, bool):
            raise ValueError
        probability = float(probability)
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError
    except (KeyError, TypeError, ValueError) as exc:
        raise PromptResponseContractError('Invalid SystemOne noul response') from exc
    return {'result': answer}
