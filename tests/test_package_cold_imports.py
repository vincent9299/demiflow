"""Cold public-entry regressions; each case starts a separate interpreter."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

STARTERS = (
    'from demiflow.lance.schema import schema',
    'from demiflow.lance.preview import snapshot_metadata',
    'from demiflow.data.flow_contract import flow_api',
    'from demiflow.data import schema',
    'from demiflow import data',
    'from demiflow import Dataset',
    'from demiflow.data import Dataset',
    'from demiflow.pipeline import Pipeline',
    'from demiflow.execution.executors.stream_checkpoint import StreamCheckpoint',
    'from demiflow.data.read_api import from_items',
    'from demiflow import *',
    'from demiflow.data import *',
)
FULL = all(importlib.util.find_spec(name) for name in ('pyarrow', 'yaml', 'lance'))


def invoke(program, *, package_parent=None, expected=None, extra=(), python=None):
    import demiflow
    parent = package_parent or Path(demiflow.__file__).resolve().parent.parent
    return subprocess.run([python or sys.executable, '-I', '-B', '-c', program,
                           str(parent), str(expected or ''), *map(str, extra)],
                          cwd=tempfile.gettempdir(), text=True, capture_output=True, timeout=30,
                          env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1', 'OPENBLAS_NUM_THREADS': '1', 'RAYON_NUM_THREADS': '2', 'TOKIO_WORKER_THREADS': '2', 'LANCE_CPU_THREADS': '2'})


LEAN = '''import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*args):
        if fullname.split('.')[0] in {'yaml','pyarrow','click','lance','demiurge'}:
            raise AssertionError('heavy or old dependency: '+fullname)
sys.meta_path.insert(0,Block())
from demiflow.lance.schema import schema
from demiflow.data.flow_contract import flow_api
from demiflow import data
assert data.schema is schema
assert set(data.__all__) <= set(dir(data))
assert 'Dataset' not in vars(data)
assert 'demiflow.data.dataset' not in sys.modules
assert 'demiflow.execution' not in sys.modules
print('LEAN_COLD_OK')
'''

IDENTITY = '''import importlib,importlib.abc,json,pathlib,sys
parent=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(parent))
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*args):
        if fullname.split('.')[0]=='demiurge':raise AssertionError('old package fallback')
sys.meta_path.insert(0,Block())
import demiflow
assert 'Dataset' not in vars(demiflow)
assert set(demiflow.__all__) <= set(dir(demiflow))
exec(sys.argv[3])
from demiflow import data
contract=json.loads(pathlib.Path(sys.argv[2]).read_text())
assert data.__all__==contract['all']
assert set(contract['all']) <= set(dir(data))
for name,(module,attribute) in contract['objects'].items():
    actual=getattr(importlib.import_module(module),attribute)
    assert getattr(data,name) is actual,(name,module)
namespace={};exec('from demiflow.data import *',namespace)
for name in contract['all']:assert namespace[name] is getattr(data,name),name
assert demiflow.Dataset is data.Dataset
from demiflow.pipeline import Pipeline
assert demiflow.Pipeline is Pipeline
try:getattr(data,'_unknown_public_export')
except AttributeError:pass
else:raise AssertionError('missing AttributeError')
for name,module in list(sys.modules.items()):
    if name=='demiflow' or name.startswith('demiflow.'):
        path=getattr(module,'__file__',None)
        if path:assert pathlib.Path(path).is_relative_to(parent/'demiflow'),(name,path)
print('COLD_PUBLIC_IDENTITY_OK')
'''

METADATA = '''import pathlib,resource,sys,tempfile
sys.path.insert(0,sys.argv[1])
from demiflow.lance.preview import snapshot_metadata
assert 'demiflow.data.dataset' not in sys.modules
import lance,pyarrow as pa
with tempfile.TemporaryDirectory(prefix='cold-snapshot-') as directory:
    uri=pathlib.Path(directory)/'result.lance'
    first=lance.write_dataset(pa.table({'id':[1,2]}),str(uri))
    version=first.version
    lance.write_dataset(pa.table({'id':[3]}),str(uri),mode='append')
    pa.set_cpu_count(2);pa.set_io_thread_count(2)
    resource.setrlimit(resource.RLIMIT_AS,(3*1024**3,3*1024**3))
    saved=snapshot_metadata(str(uri),version)
    assert saved['version']==version and saved['row_count']==2,saved
    assert saved['schema_hash'].startswith('sha256:') and len(saved['schema_hash'])==71,saved
    assert len(saved['schema_identity'])==64,saved
print('PINNED_METADATA_OK')
'''


class LeanColdImports(unittest.TestCase):
    def test_schema_and_contract_load_without_dataset_runtime(self):
        result=invoke(LEAN)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('LEAN_COLD_OK',result.stdout)


@unittest.skipUnless(FULL,'Full public API requires installed platform dependencies')
class PublicColdImports(unittest.TestCase):
    def test_each_cold_entry_preserves_all_existing_data_exports(self):
        expected=Path(__file__).parent/'fixtures/data-public-exports.json'
        for entry in STARTERS:
            with self.subTest(entry=entry):
                result=invoke(IDENTITY,expected=expected,extra=(entry,))
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertIn('COLD_PUBLIC_IDENTITY_OK',result.stdout)

    def test_fixed_output_metadata_after_real_commit(self):
        result=invoke(METADATA)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('PINNED_METADATA_OK',result.stdout)


if __name__=='__main__':unittest.main()
