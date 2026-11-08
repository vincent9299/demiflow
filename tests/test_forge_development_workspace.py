"""Versioned registration, full integrity and bounded notebook projection."""
import json
from pathlib import Path
import sys
import tempfile
import tracemalloc
import unittest

from demiflow.forge.development import tasks
from demiflow.forge.development.checks import structural
from demiflow.forge.development.notebook import read_cells
from demiflow.forge.development.project import ForgeError, Project, TOOL_ROOT, VERSION, snapshot


class WorkspaceDevelopmentTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.write('AGENTS.md', 'Follow DemiForge. No production execution.')
        self.write('orders/README.md', 'DemiForge: fixed inputs and explicit writes.')
        self.write('orders/tests/test_order.py', '# fixture')
        self.write('flows/orders.py', 'def select_orders(source):\n    return source\n')
        self.write('operators/orders.py', 'def keep(row: dict) -> bool:\n    return True\n')
        self.config = dict(schema='demiforge-project-v2', standard='1.0.0-candidate.8', tool_root=str(TOOL_ROOT),
            sources={'project-rules': {'path': 'AGENTS.md'}}, watch=['orders', 'flows', 'operators'], readonly=[],
            pipelines={'orders': dict(path='orders', entry=dict(path='flows/orders.py', symbol='select_orders'),
                operators=['operators/orders.py'], notebooks=[], verify=[dict(id='fixture',
                argv=[sys.executable, '-c', "print('passed')"], env={}, timeout=5)])})
        self.save()

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def save(self):
        self.write('.demiforge/project.json', json.dumps(self.config))
        return Project(self.root)

    def test_modern_layout_needs_no_notebook_or_magic_function_name(self):
        project = self.save()
        self.assertEqual(structural(project, 'orders'), [])
        self.assertEqual(project.entry_path('orders'), 'flows/orders.py')
        self.config['pipelines']['orders']['entry']['symbol'] = 'missing'
        self.assertIn('registered top-level', ' '.join(structural(self.save(), 'orders')))

    def test_previous_workspace_standard_and_mixed_bindings_stay_valid(self):
        self.config['standard'] = '1.0.0-candidate.7'
        project = self.save()
        self.assertEqual(structural(project, 'orders'), [])
        self.assertIn('candidate.7.md', project.source_defs('orders')['standard']['path'])
        self.config['standard'] = '1.0.0-candidate.8'
        self.config['pipelines']['orders']['standard'] = '1.0.0-candidate.7'
        project = self.save()
        self.assertEqual(structural(project, 'orders'), [])
        self.assertEqual(project.standard('orders'), '1.0.0-candidate.7')
        self.config['pipelines']['orders']['standard'] = '1.0.0-candidate.8'
        project = self.save()
        self.assertEqual(structural(project, 'orders'), [])
        self.assertIn('candidate.8.md', project.source_defs('orders')['standard']['path'])

    def test_missing_watch_and_malformed_registration_rejected(self):
        self.config['watch'] = ['orders', 'flows']
        with self.assertRaisesRegex(ForgeError, 'fully watched'):
            self.save()
        self.config['watch'].append('operators')
        self.config['pipelines']['orders']['operators'] = [{}]
        with self.assertRaises(ForgeError):
            self.save()
        self.config['pipelines']['orders'] = None
        with self.assertRaises(ForgeError):
            self.save()

    def test_reverse_operator_import_and_cross_entry_are_rejected(self):
        self.write('operators/orders.py', 'from ..flows.orders import select_orders\n')
        self.assertIn('operator imports flows', ' '.join(structural(self.save(), 'orders')))
        self.write('operators/orders.py', 'from flows.other import another\n')
        self.config['pipelines']['other'] = dict(self.config['pipelines']['orders'], entry=dict(path='flows/other.py', symbol='another'))
        self.assertIn('cross-entry', ' '.join(structural(self.save(), 'orders')))

    def test_optional_notebook_remains_inspection_only(self):
        self.config['pipelines']['orders']['notebooks'] = ['orders/view.ipynb']
        self.write('orders/view.ipynb', json.dumps({'cells': [dict(cell_type='code', source=['print(1)'])]}))
        self.assertEqual(structural(self.save(), 'orders'), [])
        self.write('orders/view.ipynb', json.dumps({'cells': [dict(cell_type='code', source='def hidden(): pass')]}))
        self.assertIn('defines execution', ' '.join(structural(self.save(), 'orders')))

    def test_legacy_registration_retains_notebook_rules(self):
        p = self.config['pipelines']['orders']
        self.config['pipelines']['legacy'] = dict(path='orders', entry='orders_pipeline.py',
            standard='1.0.0-candidate.6', verify=p['verify'])
        self.write('orders/orders_pipeline.py', 'def config(): pass\ndef run_pipeline(c): pass\n')
        problems = structural(self.save(), 'legacy')
        self.assertTrue(any('orders_debug.ipynb' in p for p in problems))
        self.assertEqual(self.save().standard('legacy'), '1.0.0-candidate.6')

    def test_budget_changes_invalidate_task_binding(self):
        project = self.save()
        tasks.start(project, 'task', 'orders', 'Fixture only', ['orders'], ['No model calls'])
        self.config['snapshot_limits'] = dict(total_bytes=1024**3)
        project = self.save()
        with tasks.locked(project, 'task') as (_, state):
            with self.assertRaisesRegex(ForgeError, 'configuration changed'):
                tasks.current(project, state)

    def test_snapshot_budgets_fail_without_omission(self):
        self.write('orders/big.txt', 'x' * 1000)
        with self.assertRaisesRegex(ForgeError, 'bytes exceed'):
            snapshot(self.root, ['orders'], limits=dict(total_bytes=500))
        with self.assertRaisesRegex(ForgeError, 'Too many'):
            snapshot(self.root, ['orders'], limits=dict(entries=2))
        for budgets in (dict(total_bytes=17*1024**3), dict(seconds=True), dict(skip_outputs=1)):
            self.config['snapshot_limits'] = budgets
            with self.assertRaises(ForgeError):
                self.save()

    def test_large_saved_output_is_not_materialized_but_changes_snapshot(self):
        path = self.root / 'orders/view.ipynb'
        with path.open('w') as out:
            out.write('{"cells":[{"cell_type":"code","source":["print(1)"],"outputs":[{"text":"')
            for _ in range(32):
                out.write('x' * 1024**2)
            out.write('"}],"metadata":{"tags":["view"]}}]}')
        before = snapshot(self.root, ['orders/view.ipynb'])
        tracemalloc.start()
        try:
            cells = read_cells(path)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 4*1024**2)
        self.assertEqual(cells, [dict(cell_type='code', source=['print(1)'], metadata={'tags':['view']})])
        with path.open('r+b') as out:
            out.seek(1024); out.write(b'y')
        self.assertNotEqual(before, snapshot(self.root, ['orders/view.ipynb']))

    def test_projection_validates_skipped_json(self):
        for value in ('{"cells":[],"out":"\\z"}', '{"cells":[],"out":[1,]}',
                      '{"cells":[],"out":01}', '{"cells":[]} trailing', '{"cells":[],"cells":[]}',
                      '{"cells":[{"cell_type":"code","source":8}]}'):
            path = self.write('orders/view.ipynb', value)
            with self.subTest(value=value), self.assertRaises(ForgeError):
                read_cells(path)
        path = self.write('orders/view.ipynb', json.dumps({'cells':[dict(cell_type='code', source='print("中文")')]}))
        self.assertEqual(read_cells(path)[0]['source'], 'print("中文")')


if __name__ == '__main__':
    unittest.main()
