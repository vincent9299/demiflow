"""Project ownership is a role declaration, independent of directory spelling."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

from demiflow.forge.development import cli, tasks
from demiflow.forge.development.checks import structural
from demiflow.forge.development.project import ForgeError, Project, TOOL_ROOT, VERSION

# These fixtures exercise the preserved candidate.9 registration contract.
VERSION = '1.0.0-candidate.9'


class ProjectLocalTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.write('AGENTS.md', 'Follow DemiForge; no production execution.')
        self.write('orders/README.md', 'DemiForge: owned project with fixed inputs.')
        self.write('orders/tests/test_order.py', '# isolated fixture')
        self.write('orders/recipe.py', 'from .processing import keep\ndef select_orders(source): return source\n')
        self.write('orders/processing.py', 'def keep(row): return True\n')
        self.config = dict(schema='demiforge-project-v2', standard=VERSION, tool_root=str(TOOL_ROOT),
            sources={'project-rules': {'path': 'AGENTS.md'}}, watch=['orders'], readonly=[],
            pipelines={'orders': dict(path='orders', entry=dict(path='orders/recipe.py', symbol='select_orders'),
                operators=['orders/processing.py'], notebooks=[], verify=[dict(id='fixture',
                argv=[sys.executable, '-c', "print('isolated fixture')"], env={}, timeout=5)])})

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def save(self):
        self.write('.demiforge/project.json', json.dumps(self.config))
        return Project(self.root)

    def test_arbitrary_owned_module_and_subdirectory_names(self):
        for entry, operator in [('orders/recipe.py', 'orders/processing.py'),
                                ('orders/flows/main.py', 'orders/operators/clean.py'),
                                ('orders/wiring/select.py', 'orders/transforms/clean.py')]:
            with self.subTest(entry=entry):
                self.write(entry, 'raise RuntimeError("never import")\ndef select_orders(source): return source\n')
                self.write(operator, 'def keep(row): return True\n')
                self.config['pipelines']['orders'].update(entry=dict(path=entry, symbol='select_orders'), operators=[operator])
                self.assertEqual(structural(self.save(), 'orders'), [])

    def test_reject_source_outside_owner_even_when_watched(self):
        self.config['watch'].extend(['outside', 'orders_extra'])
        for path in ['outside/main.py', 'orders_extra/main.py', 'orders/../main.py', '/tmp/main.py']:
            with self.subTest(path=path):
                self.config['pipelines']['orders']['entry']['path'] = path
                with self.assertRaises(ForgeError): self.save()

    def test_reject_operator_and_notebook_outside_owner(self):
        self.config['watch'].append('outside')
        for role, paths in [('operators', ['outside/helper.py']), ('notebooks', ['outside/view.ipynb'])]:
            with self.subTest(role=role):
                original = copy.deepcopy(self.config)
                self.config['pipelines']['orders'][role] = paths
                with self.assertRaisesRegex(ForgeError, 'owner directory'): self.save()
                self.config = original

    def test_missing_watch_and_role_overlap(self):
        self.config['watch'] = ['orders/recipe.py', 'orders/processing.py']
        with self.assertRaisesRegex(ForgeError, 'fully watched'): self.save()
        self.config['watch'] = ['orders']
        for operators in [['orders'], ['orders/recipe.py'], ['orders/transforms', 'orders/transforms/sub']]:
            self.config['pipelines']['orders']['operators'] = operators
            with self.subTest(operators=operators), self.assertRaises(ForgeError): self.save()

    def test_symlink_and_readonly_sources_rejected(self):
        self.write('outside.py', '# external source')
        (self.root/'orders/processing.py').unlink()
        (self.root/'orders/processing.py').symlink_to(self.root/'outside.py')
        with self.assertRaisesRegex(ForgeError, 'Symlink'): self.save()
        (self.root/'orders/processing.py').unlink()
        self.write('orders/processing.py', '# immutable dependency')
        self.config['readonly'] = ['orders/processing.py']
        with self.assertRaisesRegex(ForgeError, 'readonly'): self.save()

    def test_reverse_import_absolute_relative_alias_and_star(self):
        for code in ['import orders.recipe as wiring', 'from .recipe import select_orders as run',
                     'from . import recipe', 'from orders.recipe import *']:
            self.write('orders/processing.py', code+'\n')
            with self.subTest(code=code):
                self.assertIn('operator imports entry', ' '.join(structural(self.save(), 'orders')))

    def test_indirect_reverse_import_through_local_helper(self):
        self.write('orders/processing.py', 'from .helper import alias\n')
        self.write('orders/helper.py', 'from .recipe import select_orders as alias\n')
        self.assertIn('operator imports entry in orders/helper.py', ' '.join(structural(self.save(), 'orders')))

    def test_package_initializer_reexport_cannot_hide_reverse_import(self):
        self.write('orders/processing.py', 'from .helpers import alias\n')
        self.write('orders/helpers/__init__.py', 'from ..recipe import select_orders as alias\n')
        self.assertIn('operator imports entry in orders/helpers/__init__.py', ' '.join(structural(self.save(), 'orders')))

    def add_other(self, legacy=False):
        self.write('returns/README.md', 'DemiForge')
        self.write('returns/tests/test_return.py', '# fixture')
        self.write('returns/rules.py', 'def keep(row): return True\n')
        self.write('returns/dispatch.py', 'def run(source): return source\n')
        self.config['watch'].append('returns')
        self.config['pipelines']['returns'] = dict(path='returns', entry=dict(path='returns/dispatch.py', symbol='run'),
            operators=['returns/rules.py'], notebooks=[], verify=self.config['pipelines']['orders']['verify'])
        if legacy:
            self.write('returns/old_pipeline.py', 'def config(): pass\ndef run_pipeline(c): pass\n')
            self.config['pipelines']['returns'] = dict(path='returns', entry='old_pipeline.py',
                standard='1.0.0-candidate.6', verify=self.config['pipelines']['orders']['verify'])

    def test_cross_entry_modules_and_symbols_including_legacy(self):
        self.add_other()
        for code in ['import returns.dispatch as other', 'from returns import dispatch', 'from returns.dispatch import run']:
            self.write('orders/recipe.py', code+'\ndef select_orders(source): return source\n')
            with self.subTest(code=code):
                self.assertIn('cross-entry', ' '.join(structural(self.save(), 'orders')))
        self.add_other(legacy=True)
        self.write('orders/recipe.py', 'from returns.old_pipeline import config\ndef select_orders(source): return source\n')
        self.assertIn('cross-entry', ' '.join(structural(self.save(), 'orders')))

    def test_shared_operator_import_uses_existing_owner(self):
        self.add_other()
        self.write('orders/recipe.py', 'from returns.rules import keep\ndef select_orders(source): return source\n')
        self.assertEqual(structural(self.save(), 'orders'), [])
        self.config['pipelines']['orders']['operators'].append('returns/rules.py')
        with self.assertRaisesRegex(ForgeError, 'owner directory'): self.save()

    def test_unowned_unwatched_and_unsafe_helpers_cannot_pass(self):
        self.write('orders/recipe.py', 'from extras.helper import keep\ndef select_orders(source): return source\n')
        self.write('extras/helper.py', 'def keep(row): return True\n')
        self.assertIn('not watched', ' '.join(structural(self.save(), 'orders')))
        self.config['watch'].append('extras')
        self.assertIn('no declared operator owner', ' '.join(structural(self.save(), 'orders')))
        self.write('orders/processing.py', 'from ..recipe import select_orders\n')
        self.assertIn('escapes project', ' '.join(structural(self.save(), 'orders')))

    def test_empty_directory_and_source_byte_budgets_fail_closed(self):
        self.config['pipelines']['orders']['operators'] = ['orders/transforms']
        for number in range(12): (self.root/f'orders/transforms/empty{number}').mkdir(parents=True)
        self.config['snapshot_limits'] = dict(entries=10)
        with self.assertRaisesRegex(ForgeError, 'budget'): structural(self.save(), 'orders')
        self.config['snapshot_limits'] = dict(file_bytes=10)
        self.config['pipelines']['orders']['operators'] = ['orders/processing.py']
        with self.assertRaisesRegex(ForgeError, 'byte budget'): structural(self.save(), 'orders')

    def test_mixed_old_standard_retains_original_rules(self):
        self.config['watch'].extend(['flows', 'operators', 'legacy'])
        self.write('flows/legacy.py', 'def run(source): return source\n')
        self.config['pipelines']['legacy'] = dict(path='legacy', entry=dict(path='flows/legacy.py', symbol='run'),
            operators=['operators/legacy.py'], notebooks=[], standard='1.0.0-candidate.8',
            verify=self.config['pipelines']['orders']['verify'])
        self.assertEqual(self.save().standard('legacy'), '1.0.0-candidate.8')
        self.config['pipelines']['legacy']['entry']['path'] = 'legacy/recipe.py'
        with self.assertRaisesRegex(ForgeError, 'Historical standard'): self.save()

    def test_optional_notebook_does_not_require_fixed_directory(self):
        self.config['pipelines']['orders']['notebooks'] = ['orders/review.ipynb']
        self.write('orders/review.ipynb', json.dumps({'cells':[dict(cell_type='code', source='print(1)')]}))
        self.assertEqual(structural(self.save(), 'orders'), [])
        self.write('orders/review.ipynb', json.dumps({'cells':[dict(cell_type='code', source='def hidden(): pass')]}))
        self.assertIn('defines execution', ' '.join(structural(self.save(), 'orders')))

    def test_cli_context_check_and_verification_on_owned_layout(self):
        project = self.save()
        def invoke(*args):
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = cli.main(['--project', str(self.root), *args])
            self.assertEqual(code, 0, err.getvalue() or out.getvalue())
            return json.loads(out.getvalue())
        invoke('start', '--id', 'owned-layout', '--pipeline', 'orders', '--goal', 'isolated layout acceptance',
               '--write', 'orders', '--non-goal', 'No production or platform changes')
        with tasks.locked(project, 'owned-layout') as (_, state):
            context = tasks.context(project, state)
        # Public context advertises all required sources/pages; no fake read receipts.
        for source_id, source in context['sources'].items():
            for page in range(1, source['pages'] + 1):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    self.assertEqual(cli.main(['--project',str(self.root),'context','--id','owned-layout',
                        '--read',source_id,'--page',str(page)]),0)
        self.assertTrue(invoke('check', '--id', 'owned-layout')['passed'])
        self.assertTrue(invoke('verify', '--id', 'owned-layout', '--allow-tests')['passed'])


if __name__ == '__main__':
    unittest.main()
