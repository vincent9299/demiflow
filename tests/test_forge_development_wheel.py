"""Run with DEMIFORGE_TEST_WHEEL pointing at an offline, locally built wheel."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile


WHEEL = os.environ.get("DEMIFORGE_TEST_WHEEL")


@unittest.skipUnless(WHEEL, "Set DEMIFORGE_TEST_WHEEL to an offline wheel artifact")
class WheelInstallationTests(unittest.TestCase):
    def test_unpacked_wheel_without_checkout_or_old_repository(self):
        wheel = Path(WHEEL).resolve()
        expected = json.loads((Path(__file__).parent / "fixtures/development-standard-sha256.json").read_text())
        with tempfile.TemporaryDirectory(prefix="demiforge-wheel-test-") as directory:
            root = Path(directory)
            installed = root / "installed"
            with zipfile.ZipFile(wheel) as archive:
                standards = {Path(name).name: hashlib.sha256(archive.read(name)).hexdigest()
                             for name in archive.namelist() if name.startswith("demiflow/forge/development/standards/") and name.endswith(".md")}
                self.assertEqual(standards, expected)
                self.assertIn("demiflow/forge/development/skill/SKILL.md", archive.namelist())
                self.assertIn("demiflow/forge/development/skill/scripts/forge.py", archive.namelist())
                archive.extractall(installed)
            expected_file = root / "expected.json"
            expected_file.write_text(json.dumps(expected))
            installed_tests = root / "tests"
            installed_tests.mkdir()
            for name in ("test_forge_development.py", "test_forge_development_project_local.py",
                         "test_forge_development_workspace.py"):
                shutil.copy2(Path(__file__).parent / name, installed_tests / name)
            coverage_tests = root / "coverage_tests"
            coverage_tests.mkdir()
            shutil.copy2(Path(__file__).parent / "test_forge_development_source_coverage.py",
                         coverage_tests / "test_forge_development_source_coverage.py")
            program = r'''
import importlib.abc, json, os, pathlib, runpy, subprocess, sys
installed, expected_path, root = map(pathlib.Path, sys.argv[1:])
sys.path[:] = [str(installed), *[p for p in sys.path if p and 'site-packages' not in p and 'dist-packages' not in p]]
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'demiurge','click','pyarrow','yaml'}:
            raise AssertionError('Forbidden fallback/dependency: '+fullname)
sys.meta_path.insert(0,Block())
import demiflow
assert pathlib.Path(demiflow.__file__).is_relative_to(installed)
from demiflow.forge.development import tasks, history
from demiflow.forge.development.cli import main
from demiflow.forge.development.project import Project, STANDARDS, VERSION, TOOL_ROOT
from demiflow.forge.development.resources import PACKAGE_ROOT, resource_path, implementation_files
assert TOOL_ROOT == PACKAGE_ROOT == installed/'demiflow'
assert len(STANDARDS)==10
assert json.loads(expected_path.read_text()) == {p.name:__import__('hashlib').sha256(p.read_bytes()).hexdigest() for p in STANDARDS.values()}
assert resource_path('skill/SKILL.md').read_text().startswith('---')
assert all(p.is_relative_to(installed) and p.is_file() for p in implementation_files().values())
business=root/'business';business.mkdir();(business/'orders/tests').mkdir(parents=True)
(business/'AGENTS.md').write_text('DemiForge fixture only')
(business/'orders/README.md').write_text('DemiForge fixture')
(business/'orders/recipe.py').write_text('def select_orders(source): return source\n')
(business/'orders/rules.py').write_text('def keep(row): return True\n')
config=dict(schema='demiforge-project-v3',standard=VERSION,tool_root=str(PACKAGE_ROOT),
 sources={'project-rules':{'path':'AGENTS.md'}},watch=['orders'],readonly=[],
 projects={'orders':dict(path='orders',flows={'select':dict(path='orders/recipe.py',symbol='select_orders')},
 operators=['orders/rules.py'],prompts=[],notebooks=[],verify=[dict(id='fixture',argv=['/bin/python3','-c',"import sys;print(sys.argv[1])",'{tool}'],env={'TOOL':'{tool}'},timeout=5)])})
source=root/'bootstrap.json';source.write_text(json.dumps(config))
tasks.init(business,source);project=Project(business)
tasks.start(project,'wheel','orders','Test wheel',['orders'],['No production'])
with tasks.locked(project,'wheel') as (directory,state):
    for key,info in state['sources'].items():
        for page in range(1,info['pages']+1): assert tasks.context(project,state,key,page).startswith('# ')
    assert tasks.check(project,state)['passed']
    verification=tasks.verify(project,state,directory,True)
    assert verification['passed']
    assert verification['commands'][0]['argv'][-1] == str(PACKAGE_ROOT)
    assert pathlib.Path(business/verification['commands'][0]['log']).read_text().strip() == str(PACKAGE_ROOT)
state_path=business/'.demiforge/tasks/wheel/task.json'
old=json.loads(state_path.read_text());old['binding']='original-tool-binding';state_path.write_text(json.dumps(old))
(state_path.parent/'handoff.json').write_text(json.dumps({'status':'accepted-local'}))
before={str(p):p.read_bytes() for p in (business/'.demiforge/tasks').rglob('*') if p.is_file()}
saved=history.view(business,'wheel')['task']
assert saved['current_evidence']=='unverified' and saved['binding_status']=='stale' and saved['as_of_result']=='accepted-local'
assert before=={str(p):p.read_bytes() for p in (business/'.demiforge/tasks').rglob('*') if p.is_file()}
for script in [business/'.demiforge/forge',resource_path('skill/scripts/forge.py')]:
    result=subprocess.run(['/bin/python3','-I','-B',str(script),'--help'],cwd=root,capture_output=True,text=True,timeout=10)
    assert result.returncode==0,result.stderr
    assert 'history' in result.stdout
# Explicitly run the actual module entry after configuring only this installation.
sys.argv=['development','--project',str(business),'history','--id','wheel']
try: runpy.run_module('demiflow.forge.development',run_name='__main__')
except SystemExit as e: assert e.code==0,e.code
print('WHEEL_SMOKE_OK')
import unittest
suite=unittest.TestLoader().discover(str(root/'tests'))
assert suite.countTestCases()==53
assert unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful()
print('WHEEL_53_REGRESSIONS_OK')
suite=unittest.TestLoader().discover(str(root/'coverage_tests'))
assert suite.countTestCases() >= 40
assert unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful()
for name,module in tuple(sys.modules.items()):
    if name=='demiflow' or name.startswith('demiflow.'):
        path=getattr(module,'__file__',None)
        if path: assert pathlib.Path(path).is_relative_to(installed),(name,path)
print('WHEEL_SOURCE_COVERAGE_OK')
'''
            result = subprocess.run(["/bin/python3", "-I", "-B", "-c", program,
                                     str(installed), str(expected_file), str(root)], cwd=root,
                                    capture_output=True, text=True, timeout=60,
                                    env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("WHEEL_SMOKE_OK", result.stdout)
            self.assertIn("WHEEL_53_REGRESSIONS_OK", result.stdout)
            self.assertIn("WHEEL_SOURCE_COVERAGE_OK", result.stdout)
            full_python = os.environ.get("DEMIFORGE_TEST_FULL_PYTHON")
            if full_python:
                for statement in ("import demiflow.pipeline", "import demiflow.execution", "import demiflow.data",
                                  "from demiflow import Pipeline", "from demiflow import *"):
                    program = '''import importlib.abc,pathlib,sys
installed=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(installed))
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*args):
        if fullname.split('.')[0]=='demiurge':raise AssertionError('old repository fallback')
sys.meta_path.insert(0,Block())
import demiflow
assert pathlib.Path(demiflow.__file__).is_relative_to(installed)
assert set(demiflow.__all__) <= set(dir(demiflow))
assert 'Pipeline' not in vars(demiflow)
exec(sys.argv[2])
from demiflow import Pipeline
from demiflow.pipeline import Pipeline as direct
assert Pipeline is direct
for name,module in list(sys.modules.items()):
    if name=='demiflow' or name.startswith('demiflow.'):
        path=getattr(module,'__file__',None)
        if path:assert pathlib.Path(path).is_relative_to(installed),(name,path)
print('COLD_WHEEL_IMPORT_OK')
'''
                    cold = subprocess.run([full_python, "-I", "-B", "-c", program, str(installed), statement],
                                          cwd=root, capture_output=True, text=True, timeout=20,
                                          env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"})
                    self.assertEqual(cold.returncode, 0, cold.stdout + cold.stderr)
                    self.assertIn("COLD_WHEEL_IMPORT_OK", cold.stdout)


if __name__ == "__main__":
    unittest.main()
