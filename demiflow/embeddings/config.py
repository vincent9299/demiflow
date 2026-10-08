"""Serializable execution configuration for native embeddings, independent of models."""
import math
from pathlib import Path
from demiflow.execution.inference_options import CALL_OPTION_DEFAULTS, normalize_call_options
from demiflow.inference import request_admission_config


def embedding_options(options=None):
    """Normalize native embedding runtime options without allocating resources."""
    defaults = dict(**CALL_OPTION_DEFAULTS,
                    png_compress_level=6, profile_path=None,
                    max_document_bytes=8 * 1024**2, max_document_blocks=100_000,
                    max_document_spans=4096, max_document_text_bytes=256 * 1024,
                    document_cache_bytes=128 * 1024**2, document_cache_entries=256,
                    max_image_bytes=64 * 1024**2,
                    max_decode_pixels=100_000_000, max_request_bytes=96 * 1024**2,
                    batch_request_bytes=None, batch_decode_pixels=None,
                    max_response_bytes=64 * 1024**2, timeout_s=300,
                    connect_timeout_s=10, read_timeout_s=120, write_timeout_s=120,
                    pool_timeout_s=10, trust_env=False, sqlite_journal=None)
    if options is not None and (not isinstance(options, dict) or set(options) - defaults.keys()):
        raise ValueError('Unknown embedding runtime options')
    result = normalize_call_options({**defaults, **(options or {})})
    for key in ('max_image_bytes', 'max_decode_pixels', 'max_document_bytes',
                'max_document_blocks', 'max_document_spans', 'max_document_text_bytes',
                'document_cache_entries',
                'max_request_bytes', 'max_response_bytes'):
        if type(result[key]) is not int or result[key] < 1:
            raise ValueError(key + ' must be a positive integer')
    if type(result['document_cache_bytes']) is not int or result['document_cache_bytes'] < 0:
        raise ValueError('document_cache_bytes must be a nonnegative integer')
    for key in ('timeout_s', 'connect_timeout_s', 'read_timeout_s', 'write_timeout_s', 'pool_timeout_s'):
        if type(result[key]) not in (int, float) or not math.isfinite(result[key]) or result[key] <= 0:
            raise ValueError(key + ' must be positive and finite')
    if type(result['trust_env']) is not bool:
        raise ValueError('trust_env must be boolean')
    if type(result['png_compress_level']) is not int or not 0 <= result['png_compress_level'] <= 9:
        raise ValueError('png_compress_level must be an integer from 0 to 9')
    if result['profile_path'] is not None:
        if not isinstance(result['profile_path'], (str, Path)) or not str(result['profile_path']).strip():
            raise ValueError('profile_path must be a nonempty path or None')
        result['profile_path'] = str(result['profile_path'])
    journal = result['sqlite_journal']
    target = result['batch_request_bytes']
    if target is not None and (type(target) is not int or not 1 <= target <= result['max_request_bytes']):
        raise ValueError('batch_request_bytes must be positive and at most max_request_bytes')
    pixels = result['batch_decode_pixels']
    if pixels is not None and (type(pixels) is not int or pixels < 1):
        raise ValueError('batch_decode_pixels must be a positive integer or None')
    if journal is not None:
        if (not isinstance(journal, dict) or not journal.get('path')
                or set(journal) - {'path', 'timeout_s', 'read_only'}):
            raise ValueError('sqlite_journal requires path, optional timeout_s and read_only')
        if not isinstance(journal['path'], (str, Path)) or not str(journal['path']).strip():
            raise ValueError('sqlite_journal path must be a nonempty path')
        if type(journal.get('read_only', False)) is not bool:
            raise ValueError('Journal read_only must be boolean')
        timeout = journal.get('timeout_s', 30)
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('Journal timeout_s must be positive and finite')
        result['sqlite_journal'] = {**journal, 'path': str(journal['path'])}
    return result


def embedding_execution_config(*, batch_size=16, concurrency=1, queue_depth=None,
                               flush_interval=None, prefetch_batches=0,
                               max_requests=None, request_policy=None, options=None):
    """Return validated JSON-compatible kwargs for Dataset.map_embeddings.

    Model identity, input/output columns and service placement are separate.
    Normalization has no image, journal, thread or service side effects.
    """
    for key, value in dict(batch_size=batch_size, concurrency=concurrency,
                           queue_depth=concurrency if queue_depth is None else queue_depth).items():
        if type(value) is not int or value < 1:
            raise ValueError(key + ' must be a positive integer')
    if type(prefetch_batches) is not int or prefetch_batches < 0:
        raise ValueError('prefetch_batches must be a nonnegative integer')
    if flush_interval is not None and (type(flush_interval) not in (int, float)
            or not math.isfinite(flush_interval) or flush_interval <= 0):
        raise ValueError('flush_interval must be positive and finite')
    if max_requests is not None and (type(max_requests) is not int or max_requests < 1):
        raise ValueError('max_requests must be positive or None')
    return dict(batch_size=batch_size, concurrency=concurrency, queue_depth=queue_depth,
                flush_interval=flush_interval, prefetch_batches=prefetch_batches,
                max_requests=max_requests,
                request_policy=request_admission_config(request_policy, concurrency=concurrency),
                options=embedding_options(options))
