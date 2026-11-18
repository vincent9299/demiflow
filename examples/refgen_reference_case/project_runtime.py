"""Model connection profiles belong to deployment, not flow business arguments."""
import json
from pathlib import Path


def load_runtime():
    path = Path(__file__).parent / 'settings/runtime.json'
    with path.open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError('Runtime profile exceeds budget')
    value = json.loads(raw)
    if set(value) != {'models', 'execution_kind'} or value['execution_kind'] not in {'fixture', 'real'}:
        raise ValueError('Declare model profiles and honest fixture/real provenance')
    return value
