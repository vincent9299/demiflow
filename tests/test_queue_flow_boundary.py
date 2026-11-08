"""Queue discovery reports native facts without inventing a payload schema."""
from pathlib import Path

from demiflow import data
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.forge.flow_check import FlowChecker
from test_collect_flow_contract import base, root


def test_queue_catalog_and_typed_rejection_are_pure(root):
    path = root/'data/queue.sqlite'
    source = root/'flows/queue.py'
    source.write_text('from demiflow import data\nfrom demiflow.queue import QueueConfig,QueueLimits\n'
        'def run():\n'
        f"    return data.read_queue(QueueConfig(backend='sqlite',queue_id='q',path={str(path)!r},limits=QueueLimits())).run_stream()\n")
    catalog = inspect_modules(['demiflow.data', 'demiflow.queue'], Path(data.__file__).resolve().parents[1], LIMITS)
    assert catalog['inspection_complete'], catalog['diagnostics']
    report = FlowChecker(root, catalog, {}, []).inspect('flows/queue.py:run', {})
    assert report['status'] != 'checked'
    assert 'additional managed contract' in str(report['diagnostics']), report['diagnostics']
    assert not path.exists() and not list((root/'data').iterdir())
