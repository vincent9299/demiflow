"""Synchronous pure actors use the native callable contract; resource hooks are gated."""
import pytest
from test_forge_runs import base, root, custom, execute, rows


def test_native_pure_actor_and_early_resource_hook_rejection(root):
    flow = '''
from operators.custom import Prefix
def run(*, source, target):
    return data.read_lance(source['uri'], version=source['version']).map(Prefix,
        inputs={'text': 'question'}, output='prefixed', fn_constructor_args=['p:']).write_lance(
        target['uri'], mode='create', return_receipt=True)
'''
    operator = '''class Prefix:
    def __init__(self, prefix: str):
        self.prefix = prefix
    def __call__(self, text: str | None) -> str:
        return self.prefix + str(text)
'''
    result = execute(root, custom(root, flow, operators=operator))
    assert result['status'] == 'succeeded', result
    assert all(r['prefixed'].startswith('p:') for r in rows(root, result))
    with pytest.raises(ValueError, match='resource lifecycle'):
        custom(root, flow, operators=operator + '    def close(self) -> None:\n        return None\n')
