"""Local tokenizer admission shares native counting without implicit downloads."""
import base64
import hashlib
from pathlib import Path

import pytest

from demiflow.forge.source import SourceTree
from demiflow.forge.project_bindings import resolve_flow_resources
from demiflow.objects import read_local_object
from demiflow.operator_llm.tokens import budget_declaration, input_budget


def declaration(root):
    path = root / 'flows/bytes.bpe'
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(b''.join(base64.b64encode(bytes([i])) + b' ' + str(i).encode() + b'\n' for i in range(256)))
    return {'unit': 'tokens', 'max_input': 10000, 'max_output': 100,
        'profile': {'encoding': 'fixture', 'tokens_per_message': 3, 'reply_tokens': 3,
            'models': ['mock-model'], 'context_tokens': 12000, 'max_output_tokens': 1000,
            'verification': 'Byte-vocabulary local test fixture', 'verified': True,
            'encoding_file': 'flows/bytes.bpe', 'encoding_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'pattern': '(?s).', 'special_tokens': {}}}


def test_local_budget_does_not_use_tokenizer_registry(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('implicit tokenizer lookup')
    monkeypatch.setattr('tiktoken.get_encoding', forbidden)
    declared = declaration(tmp_path)
    budget = input_budget(declared, resource_root=tmp_path)
    assert budget.counter.text('abc你好') == len('abc你好'.encode())
    assert budget_declaration(budget) == declared
    assert budget.counter.messages([{'role': 'user', 'content': 'abc'}]) == 13


def test_resource_binding_counts_bytes_deduplicates_and_checks_content(tmp_path):
    declared = declaration(tmp_path)['profile']
    ref = dict(kind='file', parameter='tokenizer', path=declared['encoding_file'],
               sha256=declared['encoding_sha256'], format='tiktoken_bpe_v1', max_bytes=16384)
    tree, bindings = SourceTree(tmp_path), {}
    resource = resolve_flow_resources(tmp_path, tree, [ref], bindings)['tokenizer']
    assert 'text' not in resource and tree.bytes == resource['size']
    resolve_flow_resources(tmp_path, tree, [ref], bindings)
    assert tree.bytes == resource['size']
    with pytest.raises(ValueError, match='SHA256'):
        resolve_flow_resources(tmp_path, tree, [{**ref, 'sha256': '0' * 64}], {})
    with pytest.raises(ValueError):
        resolve_flow_resources(tmp_path, SourceTree(tmp_path, {'total_bytes': 2}), [ref], {})
    with pytest.raises(ValueError, match='changed'):
        resolve_flow_resources(tmp_path, tree, [{**ref, 'max_bytes': 8192}], bindings)


@pytest.mark.parametrize('change', ['escape', 'symlink', 'parent_symlink', 'digest', 'size', 'fifo'])
def test_local_object_rejects_undeclared_or_unbounded_reads(tmp_path, change):
    import os
    root = tmp_path / 'root'
    root.mkdir()
    path = root / 'data'
    path.write_bytes(b'fixed')
    digest = hashlib.sha256(b'fixed').hexdigest()
    maximum = 8
    if change == 'escape':
        path = tmp_path / 'outside'
        path.write_bytes(b'fixed')
    elif change == 'symlink':
        alias = root / 'alias'
        alias.symlink_to(path)
        path = alias
    elif change == 'parent_symlink':
        alias = root / 'alias'
        alias.symlink_to(root, target_is_directory=True)
        path = alias / 'data'
    elif change == 'digest':
        digest = '0' * 64
    elif change == 'size':
        maximum = 4
    else:
        path.unlink()
        os.mkfifo(path)
    with pytest.raises((ValueError, OSError)):
        read_local_object(path.as_uri(), digest, root=root, max_bytes=maximum)


@pytest.mark.parametrize('change', ['duplicate', 'missing_byte', 'hash'])
def test_invalid_tokenizer_data_fails_before_counting(tmp_path, change):
    declared = declaration(tmp_path)
    path = tmp_path / declared['profile']['encoding_file']
    if change == 'duplicate':
        path.write_bytes(path.read_bytes() + b'AA== 0\n')
    else:
        path.write_bytes(b'AA== 0\n')
    if change != 'hash':
        declared['profile']['encoding_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        input_budget(declared, resource_root=tmp_path)
