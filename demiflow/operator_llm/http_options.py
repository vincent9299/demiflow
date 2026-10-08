"""Transport configuration for the existing prompt Dataset operators."""
import math
import json
import os
import re
from demiflow.execution.inference_options import CALL_OPTION_DEFAULTS, normalize_call_options


HTTP_OPTIONS = set(CALL_OPTION_DEFAULTS) | {
    'sqlite_journal', 'journal_dir', 'timeout_s', 'request_options', 'verify_model',
    'require_finish_reason_stop', 'trust_env', 'max_keepalive_connections', 'model_revision',
    'stream', 'gateway', 'connect_timeout_s', 'read_timeout_s', 'write_timeout_s',
    'pool_timeout_s', 'max_connections', 'stream_include_usage', 'stream_log_dir',
    'max_response_bytes', 'max_event_bytes', 'max_stream_events',
    'gateway_credentials_env',
    'http_error_retry', 'nonfatal_http_errors',
}


def validate_http_options(options):
    options = dict(options or {})
    unknown = set(options) - HTTP_OPTIONS
    if unknown:
        raise ValueError(f'Unknown prompt execution options: {sorted(unknown)}')
    options = normalize_call_options(options)
    from .http_retry import http_error_retry_policy
    if 'http_error_retry' in options:
        options['http_error_retry'] = http_error_retry_policy(options['http_error_retry'])
    rules = options.get('nonfatal_http_errors', [])
    if (not isinstance(rules, list) or len(rules) > 16 or any(
            not isinstance(rule, dict) or set(rule) != {'status', 'code'}
            or type(rule['status']) is not int or not 400 <= rule['status'] <= 499
            or not isinstance(rule['code'], str) or not 1 <= len(rule['code']) <= 128
            for rule in rules)):
        raise ValueError('nonfatal_http_errors requires at most 16 exact HTTP status/provider code pairs')
    if 'nonfatal_http_errors' in options:
        options['nonfatal_http_errors'] = [dict(rule) for rule in rules]
    revision = options.get('model_revision')
    if revision is not None and (not isinstance(revision, str) or not revision.strip()):
        raise ValueError('model_revision must be a nonempty string or None')
    verification = options.get('verify_model', False)
    if verification is not True and verification is not False and verification != 'listed':
        raise ValueError('verify_model must be true, false, or listed')
    for name in ('timeout_s', 'connect_timeout_s', 'read_timeout_s', 'write_timeout_s', 'pool_timeout_s'):
        if name in options and (type(options[name]) not in (int, float)
                or not math.isfinite(options[name]) or options[name] <= 0):
            raise ValueError(f'{name} must be a finite positive number')
    for name in ('max_connections', 'max_keepalive_connections', 'max_response_bytes', 'max_event_bytes', 'max_stream_events'):
        if name in options and (type(options[name]) is not int or options[name] < (0 if name == 'max_keepalive_connections' else 1)):
            raise ValueError(f'{name} has an invalid connection/size limit')
    if ('max_connections' in options and options.get('max_keepalive_connections', 0) > options['max_connections']):
        raise ValueError('max_keepalive_connections must not exceed max_connections')
    for name in ('stream', 'stream_include_usage', 'trust_env', 'require_finish_reason_stop'):
        if name in options and type(options[name]) is not bool:
            raise ValueError(f'{name} must be boolean')
    if options.get('gateway') not in (None, 'litellm'):
        raise ValueError('gateway must be None or litellm')
    credentials_env = options.get('gateway_credentials_env')
    if credentials_env is not None:
        if (options.get('gateway') != 'litellm' or not isinstance(credentials_env, str)
                or re.fullmatch('[A-Z_][A-Z0-9_]*', credentials_env) is None):
            raise ValueError('gateway_credentials_env requires litellm and an environment variable name')
    if options.get('stream_log_dir') is not None:
        if not isinstance(options['stream_log_dir'], str) or not options['stream_log_dir'].strip():
            raise ValueError('stream_log_dir must be a nonempty directory path')
        if not options.get('stream'):
            raise ValueError('stream_log_dir requires stream=True')
    if options.get('max_event_bytes', 1024 * 1024) > options.get('max_response_bytes', 8 * 1024 * 1024):
        raise ValueError('max_event_bytes must not exceed max_response_bytes')
    request = options.get('request_options', {})
    if not isinstance(request, dict):
        raise ValueError('request_options must be a mapping')
    if set(request) & {'model', 'messages', 'stream', 'stream_options'}:
        raise ValueError('request_options cannot replace model/messages/stream/stream_options')
    if options.get('stream') and (request.get('n', 1) != 1 or request.get('tools') or request.get('functions')):
        raise ValueError('Prompt streaming supports one text/JSON completion; tools are unsupported')
    if options.get('gateway') == 'litellm' and set(request) & {
            'num_retries', 'max_retries', 'fallbacks', 'context_window_fallbacks', 'content_policy_fallbacks'}:
        raise ValueError('The litellm adapter owns retry/fallback settings')
    if credentials_env and set(request) & {'api_key', 'extra_headers'}:
        raise ValueError('Gateway credentials must not be placed in recorded request_options')
    return options


def gateway_credentials(options):
    """Load transport-only credentials, never part of a request journal or hash.

    Rotation applies to the same deployment; a model/deployment change still
    requires a new model_revision. Only explicit LiteLLM transport uses this.
    """
    name = options.get('gateway_credentials_env')
    if name is None:
        return {}
    raw = os.environ.get(name, '')
    if not raw or len(raw) > 16384:
        raise ValueError(f'{name} must contain credentials JSON of at most 16384 characters')
    try:
        value = json.loads(raw)
    except ValueError:
        raise ValueError(f'{name} must contain valid credentials JSON') from None
    if (not isinstance(value, dict) or set(value) - {'api_key', 'extra_headers'}
            or not isinstance(value.get('api_key'), str) or not value['api_key'].strip()):
        raise ValueError(f'{name} requires api_key and optional extra_headers only')
    headers = value.get('extra_headers', {})
    if (not isinstance(headers, dict) or len(headers) > 16 or any(
            not isinstance(k, str) or not k or not isinstance(v, str)
            or '\r' in k + v or '\n' in k + v for k, v in headers.items())):
        raise ValueError(f'{name} has invalid extra_headers')
    return value


def validate_prompt_options(options):
    if options is None:
        return
    alternate = set(options) & {'offline_dir', 'offline_store', 'codex_exec', 'codex_agent'}
    if alternate:
        transport_keys = HTTP_OPTIONS - {'model_revision', 'timeout_s', 'journal_dir', 'sqlite_journal'}
        if set(options) & transport_keys:
            raise ValueError('HTTP transport options cannot be used with offline/codex execution')
    else:
        validate_http_options(options)


def request_options(options):
    result = dict(options.get('request_options', {}))
    if options.get('stream', False):
        result['stream'] = True
        if options.get('stream_include_usage', True):
            result['stream_options'] = {'include_usage': True}
    if options.get('gateway') == 'litellm':
        # Explicit adapter: never send gateway-specific fields to arbitrary providers.
        result.update(num_retries=0, max_retries=0, fallbacks=[],
                      context_window_fallbacks=[], content_policy_fallbacks=[])
    return result
