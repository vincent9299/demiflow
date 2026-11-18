"""Directory-scan links are inventory; targets are never followed implicitly."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from demiflow.forge.development import project as module, tasks
from demiflow.forge.development.coverage import Observation
from demiflow.forge.development.project import ForgeError, Project, TOOL_ROOT, VERSION, snapshot

# These fixtures exercise the preserved candidate.9 registration contract.
VERSION = '1.0.0-candidate.9'


class SnapshotLinkTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'business'
        self.root.mkdir()
        self.watch = self.root / 'orders'
        self.watch.mkdir()

    def test_target_content_is_not_read_and_retargeting_is_visible(self):
        target = self.base / 'large-image'
        with target.open('wb') as stream:
            stream.truncate(8 * 1024**2)
        link = self.watch / 'image.jpg'
        link.symlink_to(target)
        first = snapshot(self.root, ['orders'], limits={'total_bytes': 1024})
        self.assertTrue(first['orders/image.jpg'].startswith('symlink:'))
        target.write_bytes(b'changed image data, outside source inventory')
        self.assertEqual(snapshot(self.root, ['orders']), first)
        link.unlink()
        link.symlink_to(self.base / 'another-image')
        self.assertNotEqual(snapshot(self.root, ['orders']), first)

    def test_dangling_links_and_directory_cycles_do_not_recurse(self):
        (self.watch / 'missing.jpg').symlink_to(self.base / 'missing')
        (self.watch / 'cycle').symlink_to(self.watch, target_is_directory=True)
        result = snapshot(self.root, ['orders'])
        self.assertEqual(set(result), {'orders/', 'orders/missing.jpg', 'orders/cycle'})

    def test_links_still_obey_entry_and_byte_budgets(self):
        for name in ['a.jpg', 'b.jpg', 'c.jpg']:
            (self.watch / name).symlink_to(self.base / 'image')
        with self.assertRaisesRegex(ForgeError, 'entries'):
            snapshot(self.root, ['orders'], limits={'entries': 2})
        with self.assertRaisesRegex(ForgeError, 'bytes'):
            snapshot(self.root, ['orders'], limits={'total_bytes': 1})

    def test_link_replacement_during_scan_or_after_observation_is_rejected(self):
        link = self.watch / 'image.jpg'
        link.symlink_to(self.base / 'first')
        readlink = module.os.readlink

        def replace(path):
            result = readlink(path)
            link.unlink()
            link.symlink_to(self.base / 'second')
            return result

        with patch.object(module.os, 'readlink', side_effect=replace):
            with self.assertRaisesRegex(ForgeError, 'changed during snapshot'):
                snapshot(self.root, ['orders'])
        identities = {}
        tree = snapshot(self.root, ['orders'], identities=identities)
        observation = Observation(tree, {}, {}, identities=identities)
        observation.stable()
        link.unlink()
        link.write_text('replaced with a regular file')
        with self.assertRaisesRegex(ForgeError, 'changed during observation'):
            observation.stable()

    def test_actual_source_config_and_parent_path_checks_remain(self):
        target = self.base / 'source.py'
        target.write_text('def run(): pass\n')
        link = self.watch / 'source.py'
        link.symlink_to(target)
        with self.assertRaisesRegex(ForgeError, 'Symlink'):
            module.file_hash(link)
        with self.assertRaisesRegex(ForgeError, 'Symlink'):
            module.read_json(link)
        parent = self.root / 'linked-directory'
        parent.symlink_to(self.watch, target_is_directory=True)
        with self.assertRaisesRegex(ForgeError, 'Symlink'):
            snapshot(self.root, ['linked-directory/source.py'])

    def test_real_task_registration_with_legacy_and_modern_link_inventory(self):
        (self.root / 'AGENTS.md').write_text('Temporary fixture; no production execution.')
        (self.watch / 'README.md').write_text('DemiForge fixture.')
        (self.watch / 'recipe.py').write_text('def run(source): return source\n')
        (self.watch / 'processing.py').write_text('def keep(row): return True\n')
        (self.watch / 'orders_pipeline.py').write_text(
            'def config(**kw): return kw\ndef run_pipeline(cfg): return cfg\n')
        (self.watch / 'image.jpg').symlink_to(self.base / 'missing-image')
        control = self.root / '.demiforge'
        control.mkdir()
        command = dict(id='fixture', argv=[sys.executable, '-c', 'pass'], env={}, timeout=5)
        for standard in ['1.0.0-candidate.5', VERSION]:
            with self.subTest(standard=standard):
                pipeline = (dict(path='orders', entry='orders_pipeline.py', standard=standard,
                                 verify=[command]) if standard != VERSION else
                            dict(path='orders', entry=dict(path='orders/recipe.py', symbol='run'),
                                 operators=['orders/processing.py'], notebooks=[], verify=[command]))
                config = dict(schema='demiforge-project-v2', standard=VERSION, tool_root=str(TOOL_ROOT),
                    sources={'project-rules': {'path': 'AGENTS.md'}}, watch=['orders'], readonly=[],
                    pipelines={'orders': pipeline})
                (control / 'project.json').write_text(json.dumps(config))
                project = Project(self.root)
                task_id = 'links-' + standard.replace('.', '-')
                tasks.start(project, task_id, 'orders',
                    'Verify link inventory registration', ['orders/processing.py'], ['No production'])
                state = json.loads((control / 'tasks' / task_id / 'task.json').read_text())
                self.assertTrue(state['baseline']['orders/image.jpg'].startswith('symlink:'))


if __name__ == '__main__':
    unittest.main()
