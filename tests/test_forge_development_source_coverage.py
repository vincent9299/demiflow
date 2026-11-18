"""Candidate.9 local import and source-evidence boundaries, all in owned fixtures."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from demiflow.forge.development import coverage, tasks
from demiflow.forge.development.checks import structural
from demiflow.forge.development.project import ForgeError, Project, TOOL_ROOT, VERSION, file_hash, snapshot

# These fixtures exercise the preserved candidate.9 registration contract.
VERSION = '1.0.0-candidate.9'


class CoverageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.write('AGENTS.md', 'Follow DemiForge; temporary static fixtures only.')
        self.write('orders/README.md', 'DemiForge fixture.')
        self.write('orders/tests/test_fixture.py', '# fixture')
        self.write('orders/recipe.py', 'from .processing import keep\ndef select_orders(source): return source\n')
        self.write('orders/processing.py', 'def keep(row): return True\n')
        self.config = dict(schema='demiforge-project-v2', standard=VERSION, tool_root=str(TOOL_ROOT),
            sources={'project-rules': {'path': 'AGENTS.md'}}, watch=['orders'], readonly=[],
            pipelines={'orders':dict(path='orders',entry=dict(path='orders/recipe.py',symbol='select_orders'),
                operators=['orders/processing.py'],notebooks=[],verify=[dict(id='fixture',argv=[sys.executable,'-c',"print('fixture')"],env={},timeout=5)])})

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def save(self):
        self.write('.demiforge/project.json', json.dumps(self.config))
        return Project(self.root)

    def start(self, writes=None):
        project = self.save()
        tasks.start(project, 'test', 'orders', 'Bound source evidence', writes or ['orders'], ['No production'])
        return project

    def read_all(self, project, state):
        for key, info in state['sources'].items():
            for page in range(1, info['pages'] + 1):
                tasks.context(project, state, key, page)

    def problems(self):
        return ' | '.join(structural(self.save(), 'orders'))

    def named(self):
        self.write('orders/runs/recipe.py', 'from .processing import keep\ndef select_orders(source): return source\n')
        self.write('orders/runs/processing.py', 'def keep(row): return True\n')
        self.write('orders/runs/settings.json', '{"mode":1}\n')
        self.config['pipelines']['orders'].update(entry=dict(path='orders/runs/recipe.py',symbol='select_orders'),operators=['orders/runs/processing.py'])
        self.config['sources']['settings'] = {'path':'orders/runs/settings.json'}

    def test_missing_relative_terminal_intermediate_and_from_child(self):
        for code in ['from .missing import keep','from .missing.child import keep','from . import missing']:
            with self.subTest(code=code):
                self.write('orders/recipe.py', code+'\ndef select_orders(source): return source\n')
                result = self.problems()
                self.assertIn('missing local import module', result)
                observation = self.save().observe('orders')
                self.assertIn('orders/missing.py' if 'child' not in code else 'orders/missing/child.py', observation.tree)

    def test_missing_absolute_owned_module_is_not_external(self):
        self.write('orders/recipe.py','import orders.missing\ndef select_orders(source): return source\n')
        self.assertIn('missing local import module',self.problems())

    def test_restricted_and_deep_local_imports_fail_full_lifecycle_without_reads(self):
        protected=[self.root/'orders/venv',self.root/'orders/__pycache__']
        for directory in protected:
            self.write(str(directory.relative_to(self.root))+'/sentinel','DO NOT READ')
        opening,scanning=os.open,os.scandir
        def guard(path):
            if not isinstance(path,int) and any(Path(path).is_relative_to(p) for p in protected):
                self.fail('Protected import tree was accessed: '+str(path))
        def open_file(path,*args,**kwargs):
            guard(path);return opening(path,*args,**kwargs)
        def scan(path):
            guard(path);return scanning(path)
        codes=['import orders.venv.missing','from orders.__pycache__ import missing',
               'from orders import venv','from .__pycache__ import missing',
               'import '+'.'.join(['orders']+['x']*64)]
        with patch('os.open',side_effect=open_file),patch('os.scandir',side_effect=scan):
            for index,code in enumerate(codes):
                with self.subTest(code=code):
                    self.write('orders/recipe.py',code+'\ndef select_orders(source): return source\n')
                    project=self.save();task_id='restricted'+str(index)
                    tasks.start(project,task_id,'orders','Do not externalize local restrictions',['orders'],['No production'])
                    with tasks.locked(project,task_id) as (directory,state):
                        self.read_all(project,state)
                        result=tasks.check(project,state)
                        self.assertFalse(result['passed'])
                        self.assertFalse(result['source_coverage']['complete'])
                        self.assertEqual(result['source_coverage']['external_imports_uninspected'],[])
                        self.assertIn('DF-03',' '.join(result['violations']))
                        self.assertIn('depth budget exceeded' if index == 4 else 'protected local module path',
                                      ' '.join(result['violations']))
                        with self.assertRaisesRegex(ForgeError,'Mechanical check failed'):
                            tasks.verify(project,state,directory,True)
                        self.assertFalse(tasks.handoff(project,state,directory,True)['ready'])
                        baseline=copy.deepcopy(state['baseline'])
                        self.write('orders/recipe.py','def select_orders(source): return source\n')
                        tasks.refresh(project,state,'Remove invalid local dependency without rebasing')
                        self.read_all(project,state)
                        self.assertTrue(tasks.check(project,state)['passed'])
                        self.assertEqual(state['baseline'],baseline)

    def test_actual_local_prefix_keeps_restricted_and_deep_identity(self):
        self.write('helpers/__init__.py','# actual local prefix')
        for name in ['helpers.venv.missing','.'.join(['helpers']+['x']*64)]:
            with self.subTest(name=name):
                self.write('orders/recipe.py','import '+name+'\ndef select_orders(source): return source\n')
                observation=self.save().observe('orders')
                self.assertFalse(observation.coverage['complete'])
                self.assertEqual(observation.coverage['external_imports_uninspected'],[])
                self.assertIn('DF-03',' '.join(observation.facts.problems))

    def test_unregistered_external_venv_keeps_full_lifecycle_boundary(self):
        self.write('orders/recipe.py','import venv\nfrom venv import EnvBuilder\ndef select_orders(source): return source\n')
        project=self.start()
        with tasks.locked(project,'test') as (directory,state):
            self.read_all(project,state)
            result=tasks.check(project,state)
            self.assertTrue(result['passed'])
            self.assertEqual({p['module'] for p in result['source_coverage']['external_imports_uninspected']},{'venv'})
            self.assertTrue(tasks.verify(project,state,directory,True)['passed'])

    def test_missing_absolute_child_of_actual_local_package_is_not_external(self):
        self.write('helpers/__init__.py','# local package outside the registered owner')
        self.write('orders/recipe.py','import helpers.missing\ndef select_orders(source): return source\n')
        observation=self.save().observe('orders')
        self.assertIn('missing local import module',' '.join(observation.facts.problems))
        self.assertNotIn('helpers.missing',{x['module'] for x in observation.coverage['external_imports_uninspected']})

    def test_plain_functions_classes_constants_and_aliases_are_symbols(self):
        self.write('orders/processing.py','VALUE=42\nclass Row: pass\ndef keep(row): return True\n')
        for symbol in ['VALUE','Row','keep']:
            self.write('orders/recipe.py',f'from .processing import {symbol} as alias\ndef select_orders(source): return source\n')
            with self.subTest(symbol=symbol):
                observation = self.save().observe('orders')
                self.assertEqual(structural(self.save(),'orders'),[])
                self.assertFalse(any('/'+symbol+'.py' in path for path in observation.tree))

    def test_missing_symbol_in_plain_module_does_not_invent_submodule(self):
        self.write('orders/recipe.py','from .processing import absent\ndef select_orders(source): return source\n')
        observation = self.save().observe('orders')
        self.assertIn('missing local import symbol', ' '.join(observation.facts.problems))
        self.assertNotIn('orders/processing/absent.py',observation.tree)

    def test_attribute_mutation_does_not_invent_a_module_symbol(self):
        self.write('orders/processing.py','absent.value = 1\n')
        self.write('orders/recipe.py','from .processing import absent\ndef select_orders(source): return source\n')
        self.assertIn('missing local import symbol',self.problems())

    def test_namespace_and_real_child_module(self):
        self.write('orders/helpers/clean.py','def keep(row): return True\n')
        self.write('orders/recipe.py','from .helpers import clean\nfrom .helpers.clean import keep\ndef select_orders(source): return source\n')
        project=self.save();observation=project.observe('orders')
        self.assertEqual(structural(project,'orders'),[])
        self.assertEqual(observation.tree['orders/helpers/'],'directory')
        self.assertIn('orders/helpers/clean.py',observation.tree)

    def test_initializer_reexports_and_same_package_child(self):
        self.write('orders/helpers/__init__.py','from .origin import keep as exported\nfrom . import origin\n')
        self.write('orders/helpers/origin.py','def keep(row): return True\n')
        self.write('orders/recipe.py','from .helpers import exported, origin\ndef select_orders(source): return source\n')
        self.assertEqual(structural(self.save(),'orders'),[])

    def test_missing_reexport_provider_and_unresolved_cycle(self):
        self.write('orders/helpers/__init__.py','from .missing import keep\n')
        self.write('orders/recipe.py','from .helpers import keep\ndef select_orders(source): return source\n')
        self.assertIn('missing local import module',self.problems())
        self.write('orders/helpers/__init__.py','from .other import keep\n')
        self.write('orders/helpers/other.py','from . import keep\n')
        self.assertIn('re-export cycle',self.problems())

    def test_dynamic_exports_are_not_guessed(self):
        self.write('orders/helpers/__init__.py','def __getattr__(name): return lambda row: True\n')
        self.write('orders/recipe.py','from .helpers import keep\ndef select_orders(source): return source\n')
        self.assertIn('unsupported dynamic local export',self.problems())

    def test_real_child_does_not_prove_dynamic_export_identity(self):
        self.write('orders/helpers/__init__.py','def __getattr__(name): return 42\n')
        self.write('orders/helpers/clean.py','raise AssertionError("must not import candidate")\n')
        self.write('orders/recipe.py','from .helpers import clean\ndef select_orders(source): return source\n')
        self.assertIn('unsupported dynamic local export',self.problems())

    def test_empty_unrelated_namespace_does_not_shadow_external(self):
        (self.root/'external_pkg').mkdir()
        self.write('orders/recipe.py','import external_pkg\nfrom typing import Any\nimport venv\ndef select_orders(source): return source\n')
        project=self.save();observation=project.observe('orders')
        self.assertEqual(structural(project,'orders'),[])
        self.assertEqual({x['module'] for x in observation.coverage['external_imports_uninspected']},{'external_pkg','typing','venv'})

    def test_candidate_and_installed_imports_are_never_executed(self):
        self.write('orders/recipe.py','raise AssertionError("candidate import")\nfrom .processing import keep\nimport not_installed_external\ndef select_orders(source): return source\n')
        with patch('importlib.import_module', side_effect=AssertionError('candidate lookup executed')):
            # Project resources were already resolved on package import.
            self.assertEqual(structural(self.save(),'orders'),[])

    def test_operator_context_through_reexports_and_star_rejects_entry(self):
        self.write('orders/helpers/__init__.py','from ..recipe import select_orders as exported\n')
        self.write('orders/processing.py','from .helpers import exported\n')
        self.assertIn('operator imports entry in orders/helpers/__init__.py',self.problems())
        self.write('orders/processing.py','from .recipe import *\n')
        self.assertIn('operator imports entry',self.problems())

    def test_implicit_package_initializer_preserves_operator_direction(self):
        self.write('orders/__init__.py','def select_orders(source): return source\n')
        self.config['pipelines']['orders']['entry']['path']='orders/__init__.py'
        self.assertIn('operator imports entry',self.problems())

    def test_missing_other_entry_still_reports_direction(self):
        self.config['watch'].extend(['returns','flows','operators'])
        self.config['pipelines']['returns']=dict(path='returns',entry=dict(path='flows/returns.py',symbol='run'),operators=['operators/returns.py'],notebooks=[],standard='1.0.0-candidate.8',verify=self.config['pipelines']['orders']['verify'])
        for code in ['from flows.returns import run','from flows import returns']:
            with self.subTest(code=code):
                self.write('orders/processing.py',code+'\n')
                problems=self.problems()
                self.assertIn('cross-entry import',problems)
                self.assertIn('operator imports entry',problems)

    def test_shared_operator_owner_and_cross_entry(self):
        self.write('returns/README.md','DemiForge')
        self.write('returns/tests/test_fixture.py','# fixture')
        self.write('returns/dispatch.py','def run(source): return source\n')
        self.write('returns/rules.py','def keep(row): return True\n')
        self.config['watch'].append('returns')
        self.config['pipelines']['returns']=dict(path='returns',entry=dict(path='returns/dispatch.py',symbol='run'),operators=['returns/rules.py'],notebooks=[],verify=self.config['pipelines']['orders']['verify'])
        self.write('orders/recipe.py','from returns.rules import keep\ndef select_orders(source): return source\n')
        self.assertEqual(structural(self.save(),'orders'),[])
        self.write('orders/recipe.py','from returns.dispatch import run\ndef select_orders(source): return source\n')
        self.assertIn('cross-entry import',self.problems())

    def test_all_named_roles_and_config_enter_same_snapshot(self):
        self.named()
        self.write('orders/datasets/view.ipynb',json.dumps({'cells':[dict(cell_type='code',metadata={},source=['from orders.runs.recipe import select_orders\n'])]}))
        self.config['pipelines']['orders']['notebooks']=['orders/datasets/view.ipynb']
        project=self.save();observation=project.observe('orders')
        self.assertEqual(structural(project,'orders'),[])
        for path in ['orders/runs/recipe.py','orders/runs/processing.py','orders/runs/settings.json','orders/datasets/view.ipynb']:
            self.assertEqual(observation.tree[path],file_hash(self.root/path))
        self.assertTrue(observation.coverage['complete'])

    def test_precise_source_never_descends_unknown_history_sibling(self):
        self.named();self.write('orders/runs/history/table/sentinel','DO NOT READ')
        original=os.scandir
        def scoped(path):
            if Path(path).is_relative_to(self.root/'orders/runs/history'):
                raise AssertionError('history descent')
            return original(path)
        original_open=os.open
        def opening(path,*args,**kwargs):
            if Path(path).is_relative_to(self.root/'orders/runs/history'):
                raise AssertionError('history file opened')
            return original_open(path,*args,**kwargs)
        with patch('os.scandir',side_effect=scoped),patch('os.open',side_effect=opening):
            observation=self.save().observe('orders')
        self.assertIn('orders/runs/processing.py',observation.tree)
        self.assertEqual(observation.coverage['tree']['unverified_ranges'],['orders/runs/history'])
        self.assertIn('coverage is unverified',self.problems())

    def test_unlisted_configuration_is_partial_nonpassing(self):
        self.write('orders/runs/settings.json','{"mode":1}')
        project=self.save();observation=project.observe('orders')
        self.assertEqual(observation.coverage['tree']['unverified_ranges'],['orders/runs'])
        self.assertNotIn('orders/runs/settings.json',observation.tree)
        self.assertIn('coverage is unverified',self.problems())
        with self.assertRaisesRegex(ForgeError,'unverified'):
            tasks.start(project,'wide','orders','No fake baseline',['orders'],['No production'])

    def test_explicit_author_subtree_honors_nested_arbitrary_names(self):
        self.write('orders/runs/transforms/datasets/clean.py','def keep(row): return True\n')
        self.write('orders/recipe.py','from .runs.transforms.datasets.clean import keep\ndef select_orders(source): return source\n')
        self.config['pipelines']['orders']['operators']=['orders/runs/transforms']
        observation=self.save().observe('orders')
        self.assertEqual(structural(self.save(),'orders'),[])
        self.assertIn('orders/runs/transforms/datasets/clean.py',observation.tree)

    def test_necessary_helper_under_named_directory_is_taken_from_import(self):
        self.write('orders/datasets/helpers/clean.py','def keep(row): return True\n')
        self.write('orders/recipe.py','from .datasets.helpers.clean import keep\ndef select_orders(source): return source\n')
        observation=self.save().observe('orders')
        self.assertEqual(structural(self.save(),'orders'),[])
        self.assertIn('orders/datasets/helpers/clean.py',observation.tree)

    def test_global_registered_roles_are_not_lost_in_other_pipeline(self):
        self.write('returns/README.md','DemiForge')
        self.write('returns/tests/test_fixture.py','# fixture')
        self.write('returns/runs/main.py','def run(source): return source\n')
        self.write('returns/datasets/rules.py','def keep(row): return True\n')
        self.config['watch'].append('returns')
        self.config['pipelines']['returns']=dict(path='returns',entry=dict(path='returns/runs/main.py',symbol='run'),operators=['returns/datasets/rules.py'],notebooks=[],verify=self.config['pipelines']['orders']['verify'])
        tree=self.save().observe('orders').tree
        self.assertIn('returns/runs/main.py',tree);self.assertIn('returns/datasets/rules.py',tree)

    def test_readonly_explicit_named_file_and_unknown_subtree(self):
        self.write('platform/runs/api.py','# declared platform')
        self.config['readonly']=['platform/runs/api.py']
        self.assertIn('platform/runs/api.py',self.save().observe('orders').readonly)
        self.config['readonly']=['platform']
        self.assertIn('coverage is unverified (readonly)',self.problems())

    def test_declared_readonly_configuration_full_lifecycle_and_original_baselines(self):
        config_path='platform/runs/settings.json'
        self.write(config_path,'{"mode":1}\n')
        self.config['sources']['settings']={'path':config_path}
        self.config['readonly']=['platform']
        project=self.start()
        with tasks.locked(project,'test') as (directory,state):
            self.read_all(project,state)
            result=tasks.check(project,state)
            self.assertTrue(result['passed'])
            self.assertTrue(result['source_coverage']['readonly']['complete'])
            self.assertEqual(state['readonly_baseline'][config_path],file_hash(self.root/config_path))
            baseline,readonly=copy.deepcopy(state['baseline']),copy.deepcopy(state['readonly_baseline'])
            self.assertTrue(tasks.verify(project,state,directory,True)['passed'])
            original_fingerprint=tasks.current(project,state)[3]
            self.write(config_path,'{"mode":2}\n')
            self.assertNotEqual(tasks.current(project,state)[3],original_fingerprint)
            tasks.refresh(project,state,'Readonly source changed; preserve original facts')
            self.read_all(project,state)
            result=tasks.check(project,state)
            self.assertFalse(result['passed'])
            self.assertIn('readonly changed: '+config_path,' '.join(result['violations']))
            self.assertEqual(state['baseline'],baseline);self.assertEqual(state['readonly_baseline'],readonly)
            with self.assertRaisesRegex(ForgeError,'Mechanical check failed'):
                tasks.verify(project,state,directory,True)
            self.assertFalse(tasks.handoff(project,state,directory,True)['ready'])

    def test_readonly_inherits_only_matching_exact_sources_without_history_descent(self):
        self.write('platform/runs/settings.json','{}')
        self.write('platform/runs/history/table/sentinel','DO NOT READ')
        self.write('other/runs/config.json','{}')
        self.config['sources'].update(settings={'path':'platform/runs/settings.json'},other={'path':'other/runs/config.json'})
        self.config['readonly']=['platform']
        project=self.start()
        opening,scanning=os.open,os.scandir
        def guard(path):
            if not isinstance(path,int) and Path(path).is_relative_to(self.root/'platform/runs/history'):
                self.fail('Readonly precision expanded into history: '+str(path))
        def open_file(path,*args,**kwargs):
            guard(path);return opening(path,*args,**kwargs)
        def scan(path):
            guard(path);return scanning(path)
        with patch('os.open',side_effect=open_file),patch('os.scandir',side_effect=scan):
            with tasks.locked(project,'test') as (_,state):
                self.read_all(project,state)
                result=tasks.check(project,state)
                self.assertFalse(result['passed'])
                self.assertEqual(result['source_coverage']['readonly']['unverified_ranges'],['platform/runs/history'])
                observation=project.observe('orders')
                self.assertIn('platform/runs/settings.json',observation.readonly)
                self.assertFalse(any(p.startswith(('other','orders','AGENTS')) for p in observation.readonly))

    def test_readonly_exact_source_uses_absolute_and_relative_root_labels(self):
        self.write('platform/runs/settings.json','{}')
        self.config['sources']['settings']={'path':'platform/runs/settings.json'}
        for name in ['platform',str(self.root/'platform'),'platform/../platform','platform/']:
            with self.subTest(root=name):
                self.config['readonly']=[name]
                observation=self.save().observe('orders')
                self.assertTrue(observation.coverage['readonly']['complete'])
                self.assertEqual(observation.readonly[name+'/runs/settings.json'],file_hash(self.root/'platform/runs/settings.json'))

    def test_readonly_precise_configuration_keeps_first_snapshot_identity(self):
        self.write('platform/runs/settings.json','{}')
        self.config['sources']['settings']={'path':'platform/runs/settings.json'}
        self.config['readonly']=['platform']
        project=self.save()
        from demiflow.forge.development import project as project_module
        original=project_module.snapshot;calls=0
        def snapshotting(*args,**kwargs):
            nonlocal calls
            result=original(*args,**kwargs);calls+=1
            if calls==1:self.write('platform/runs/settings.json','{"changed":true}\n')
            return result
        with patch.object(project_module,'snapshot',side_effect=snapshotting),self.assertRaisesRegex(ForgeError,'changed between snapshots'):
            project.observe('orders')

    def test_missing_notebook_import_is_checked_without_executing_cells(self):
        self.write('orders/view.ipynb',json.dumps({'cells':[dict(cell_type='code',metadata={},source=['raise AssertionError("never execute")\nfrom orders.missing import keep\n'])]}))
        self.config['pipelines']['orders']['notebooks']=['orders/view.ipynb']
        self.assertIn('missing local import module',self.problems())

    def test_explicit_watch_root_named_runs_keeps_declared_full_scope(self):
        self.write('orders/runs/settings.json','{}')
        self.config['watch'].append('orders/runs')
        observation=self.save().observe('orders')
        self.assertIn('orders/runs/settings.json',observation.tree)
        self.assertTrue(observation.coverage['complete'])

    def test_unwatched_necessary_source_cannot_create_a_partial_baseline(self):
        self.write('extras/helper.py','def keep(row): return True\n')
        self.write('orders/recipe.py','from extras.helper import keep\ndef select_orders(source): return source\n')
        project=self.save()
        self.assertIn('not watched',self.problems())
        with self.assertRaisesRegex(ForgeError,'cannot capture'):
            tasks.start(project,'unsafe','orders','No partial baseline',['orders/recipe.py'],['No production'])
        self.assertFalse((self.root/'.demiforge/tasks/unsafe').exists())

    def test_watched_helper_without_owner_cannot_pass(self):
        self.write('extras/helper.py','def keep(row): return True\n')
        self.config['watch'].append('extras')
        self.write('orders/recipe.py','from extras.helper import keep\ndef select_orders(source): return source\n')
        self.assertIn('no declared operator owner',self.problems())

    def test_ambiguous_module_package_and_module_as_parent_are_nonpassing(self):
        self.write('orders/processing/__init__.py','def keep(row): return True\n')
        self.assertIn('ambiguous local module/package',self.problems())
        (self.root/'orders/processing/__init__.py').unlink()
        (self.root/'orders/processing').rmdir()
        self.write('orders/recipe.py','from .processing.child import keep\ndef select_orders(source): return source\n')
        self.assertIn('not a package',self.problems())

    def test_readonly_named_source_change_preserves_original_baseline(self):
        self.write('platform/runs/api.py','# platform')
        self.config['readonly']=['platform/runs/api.py']
        project=self.start()
        with tasks.locked(project,'test') as (_,state):
            original=copy.deepcopy(state['readonly_baseline'])
            self.write('platform/runs/api.py','# changed platform')
            tasks.refresh(project,state,'Do not move readonly baseline')
            self.read_all(project,state)
            self.assertIn('readonly changed',' '.join(tasks.check(project,state)['violations']))
            self.assertEqual(state['readonly_baseline'],original)

    def test_low_level_legacy_snapshot_and_path_policy_are_unchanged(self):
        self.write('orders/datasets/config.json','{}')
        self.assertNotIn('orders/datasets/config.json',snapshot(self.root,['orders']))
        self.config['standard']='1.0.0-candidate.8'
        self.config['watch'].extend(['flows','operators'])
        self.config['pipelines']['orders'].update(entry=dict(path='flows/order.py',symbol='run'),operators=['operators/order.py'])
        self.config['sources']['config']={'path':'orders/datasets/config.json'}
        with self.assertRaisesRegex(ForgeError,'Excluded'):
            self.save()

    def test_mixed_task_uses_its_own_old_standard(self):
        self.named()
        self.write('legacy/README.md','DemiForge')
        self.write('legacy/tests/test_fixture.py','# test')
        self.write('flows/legacy.py','def run(source): return source\n')
        self.write('operators/legacy.py','def keep(row): return True\n')
        self.config['watch'].extend(['legacy','flows','operators'])
        self.config['pipelines']['legacy']=dict(path='legacy',entry=dict(path='flows/legacy.py',symbol='run'),operators=['operators/legacy.py'],notebooks=[],standard='1.0.0-candidate.8',verify=self.config['pipelines']['orders']['verify'])
        project=self.save()
        with self.assertRaisesRegex(ForgeError,'Excluded'):
            project.observe('legacy')
        self.config['sources'].pop('settings')
        project=self.save()
        self.assertNotIn('orders/runs/recipe.py',project.observe('legacy').tree)
        self.assertIsNone(project.observe('legacy').coverage)
        self.assertIn('orders/runs/recipe.py',project.observe('orders').tree)

    def test_no_second_parse_during_current_check(self):
        project=self.start()
        with tasks.locked(project,'test') as (_,state):
            self.read_all(project,state)
            import ast
            original=ast.parse;seen=[]
            def parse(*args,**kwargs):
                seen.append(kwargs.get('filename'))
                return original(*args,**kwargs)
            with patch('ast.parse',side_effect=parse):
                self.assertTrue(tasks.check(project,state)['passed'])
            self.assertEqual(seen.count('orders/recipe.py'),1)
            self.assertEqual(seen.count('orders/processing.py'),1)

    def test_syntax_failure_is_still_snapshot_bound_and_draftable(self):
        self.write('orders/processing.py','def broken(:\n')
        project=self.start()
        observation=project.observe('orders')
        self.assertEqual(observation.tree['orders/processing.py'],file_hash(self.root/'orders/processing.py'))
        self.assertIn('invalid Python',self.problems())

    def test_new_observed_dependency_cannot_rebase_original_scope(self):
        self.write('orders/runs/helper.py','def keep(row): return True\n')
        project=self.start(['orders/recipe.py'])
        with tasks.locked(project,'test') as (_,state):
            original=copy.deepcopy(state['baseline']);original_readonly=copy.deepcopy(state['readonly_baseline'])
            self.write('orders/recipe.py','from .runs.helper import keep\ndef select_orders(source): return source\n')
            tasks.refresh(project,state,'New necessary dependency must retain original facts')
            self.read_all(project,state)
            result=tasks.check(project,state)
            self.assertFalse(result['passed']);self.assertIn('out-of-scope',' '.join(result['violations']))
            self.assertEqual(state['baseline'],original);self.assertEqual(state['readonly_baseline'],original_readonly)

    def test_deleting_unobserved_boundary_does_not_launder_baseline(self):
        file=self.write('orders/runs/settings.json','{}')
        project=self.start(['orders/recipe.py'])
        with tasks.locked(project,'test') as (_,state):
            file.unlink();file.parent.rmdir()
            tasks.refresh(project,state,'Must retain original unknown range')
            self.read_all(project,state)
            self.assertIn('coverage boundary changed', ' '.join(tasks.check(project,state)['violations']))

    def test_partial_handoff_exposes_coverage_without_claiming_acceptance(self):
        self.write('orders/runs/history/table/sentinel','not inspected')
        project=self.start(['orders/recipe.py'])
        with tasks.locked(project,'test') as (directory,state):
            self.read_all(project,state)
            result=tasks.handoff(project,state,directory,False)
            self.assertEqual(result['status'],'incomplete')
            report=json.loads((directory/'handoff.json').read_text())
            self.assertFalse(report['source_coverage']['complete'])
            self.assertEqual(report['source_coverage']['tree']['unverified_ranges'],['orders/runs'])
            self.assertNotIn('runs',report['excluded_directories'])
            self.assertIn('Policy: candidate.9-local-source-v1',(directory/'handoff.md').read_text())

    def test_new_helper_is_bound_and_stale_task_bytes_are_not_rewritten(self):
        project=self.start()
        from demiflow.forge.development import project as project_module
        from demiflow.forge.development.resources import implementation_files
        target=implementation_files()['demiflow/forge/development/coverage.py']
        state_path=project.control/'tasks/test/task.json'
        original=state_path.read_bytes();state=json.loads(original)
        hashing=project_module.file_hash
        with patch.object(project_module,'file_hash',side_effect=lambda path: 'changed' if path==target else hashing(path)):
            with self.assertRaisesRegex(ForgeError,'Tool, standard or project configuration changed'):
                tasks.current(project,state)
        self.assertEqual(state_path.read_bytes(),original)

    def test_lifecycle_config_dependency_init_and_notebook_output_invalidate(self):
        self.named()
        self.write('orders/runs/__init__.py','# initializer')
        notebook=self.write('orders/datasets/view.ipynb',json.dumps({'cells':[dict(cell_type='code',metadata={},source=[],outputs=[])]}))
        self.config['pipelines']['orders']['notebooks']=['orders/datasets/view.ipynb']
        project=self.start()
        with tasks.locked(project,'test') as (directory,state):
            self.read_all(project,state)
            self.assertTrue(tasks.verify(project,state,directory,True)['passed'])
            baseline=copy.deepcopy(state['baseline'])
            before=tasks.current(project,state)[3]
            for path,text in [('orders/runs/settings.json','{"mode":2}'),('orders/runs/processing.py','def keep(row): return False\n'),('orders/runs/__init__.py','# changed init'),('orders/datasets/view.ipynb',json.dumps({'cells':[dict(cell_type='code',metadata={},source=[],outputs=[{'data':{'text/plain':'saved output'}}])]}))]:
                self.write(path,text)
                after=tasks.current(project,state)[3]
                self.assertNotEqual(before,after,path);before=after
                tasks.refresh(project,state,'Refresh context only')
                self.assertEqual(state['baseline'],baseline)
            self.assertIsNone(state['verification'])
            self.assertIn('source_coverage',tasks.context(project,state))
            self.assertNotIn('datasets',tasks.context(project,state)['excluded_directories'])

    def test_verification_reobserves_trusted_fixture_source_mutation(self):
        self.named()
        self.config['pipelines']['orders']['verify'][0]['argv']=[sys.executable,'-c',"from pathlib import Path;Path('orders/runs/processing.py').write_text('def keep(row): return False\\n')"]
        project=self.start()
        with tasks.locked(project,'test') as (directory,state):
            self.read_all(project,state)
            result=tasks.verify(project,state,directory,True)
            self.assertFalse(result['passed']);self.assertTrue(result['source_changed_during_tests'])

    def test_static_snapshot_and_context_changes_are_rejected(self):
        project=self.save()
        from demiflow.forge.development import project as project_module
        original=project_module.file_hash;changed=False
        def hashing(path,**kwargs):
            nonlocal changed
            if Path(path)==self.root/'orders/processing.py' and not changed:
                changed=True;self.write('orders/processing.py','def keep(row): return False\n')
            return original(path,**kwargs)
        with patch.object(project_module,'file_hash',side_effect=hashing), self.assertRaisesRegex(ForgeError,'differ|changed'):
            project.observe('orders')

    def test_namespace_initializer_created_during_snapshot_is_rejected(self):
        project=self.save()
        from demiflow.forge.development import project as project_module
        original=project_module.file_hash;changed=False
        def hashing(path,**kwargs):
            nonlocal changed
            if not changed:
                changed=True;self.write('orders/__init__.py','# changed namespace identity')
            return original(path,**kwargs)
        with patch.object(project_module,'file_hash',side_effect=hashing),self.assertRaisesRegex(ForgeError,'changed'):
            project.observe('orders')

    def test_overlap_with_readonly_cannot_replace_earlier_observed_identity(self):
        self.named()
        self.config['readonly']=['orders/runs/settings.json']
        project=self.save()
        from demiflow.forge.development import project as project_module
        original=project_module.snapshot;calls=0
        def snapshotting(*args,**kwargs):
            nonlocal calls
            result=original(*args,**kwargs)
            calls+=1
            if calls==1:
                self.write('orders/runs/settings.json','{"changed":true}\n')
            return result
        with patch.object(project_module,'snapshot',side_effect=snapshotting),self.assertRaisesRegex(ForgeError,'changed between snapshots'):
            project.observe('orders')

    def test_context_excerpt_keeps_complete_configuration_bytes_in_fingerprint(self):
        self.named()
        self.write('orders/runs/settings.json','first line\nunselected original\n')
        self.config['sources']['settings']['lines']=[1,1]
        project=self.start()
        with tasks.locked(project,'test') as (_,state):
            before=tasks.current(project,state)
            self.write('orders/runs/settings.json','first line\nunselected changed\n')
            after=tasks.current(project,state)
            self.assertEqual(before[2]['settings'],after[2]['settings'])
            self.assertNotEqual(before[3],after[3])

    def test_overlap_is_hashed_once(self):
        self.config['watch'].extend(['orders/processing.py','orders/tests'])
        from demiflow.forge.development import project as project_module
        original=project_module.file_hash;seen=[]
        def hashing(path,**kwargs):
            seen.append(Path(path));return original(path,**kwargs)
        with patch.object(project_module,'file_hash',side_effect=hashing):
            self.save().observe('orders')
        self.assertEqual(seen.count(self.root/'orders/processing.py'),1)

    def test_small_entries_bytes_ast_and_time_fail_before_success(self):
        for limits in [{'entries':5},{'file_bytes':5},{'total_bytes':5}]:
            with self.subTest(limits=limits):
                self.config['snapshot_limits']=limits
                with self.assertRaisesRegex(ForgeError,'budget'):
                    self.save().observe('orders')
        self.config.pop('snapshot_limits')
        with patch.object(coverage,'AST_LIMIT',2),self.assertRaisesRegex(ForgeError,'AST'):
            self.save().observe('orders')
        project=self.save()
        with patch.object(coverage.time,'monotonic',side_effect=[0,0,10000]),self.assertRaises(ForgeError):
            coverage.SourceFacts(project,deadline=1)

    def test_symlink_special_file_and_control_names_remain_rejected(self):
        self.named();path=self.root/'orders/runs/processing.py';path.unlink();path.symlink_to(self.root/'orders/processing.py')
        with self.assertRaisesRegex(ForgeError,'Symlink'):
            self.save().observe('orders')
        path.unlink();os.mkfifo(path)
        with self.assertRaises(ForgeError):
            self.save().observe('orders')
        path.unlink()
        for name in ['orders/.demiforge/op.py','orders/__pycache__/op.py','orders/venv/op.py']:
            self.config['pipelines']['orders']['operators']=[name]
            with self.subTest(name=name),self.assertRaisesRegex(ForgeError,'Excluded'):
                self.save()


if __name__=='__main__':unittest.main()
