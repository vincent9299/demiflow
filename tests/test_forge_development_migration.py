"""Migration boundaries, using only stdlib and owned temporary fixtures."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from demiflow.forge.development import history, tasks
from demiflow.forge.development.io import atomic_write_json
from demiflow.forge.development.project import ForgeError, MAX_TOTAL, Project, STANDARDS, TOOL_ROOT, VERSION
from demiflow.forge.development.resources import PACKAGE_ROOT, implementation_files, resource_path
from demiflow.forge.development.runner import run


def fixture(root, tool=TOOL_ROOT):
    (root / "AGENTS.md").write_text("Follow DemiForge; fixture only.")
    (root / "orders/tests").mkdir(parents=True)
    (root / "orders/README.md").write_text("DemiForge fixture.")
    (root / "orders/recipe.py").write_text("def select_orders(source): return source\n")
    (root / "orders/rules.py").write_text("def keep(row): return True\n")
    config = dict(schema="demiforge-project-v2", standard=VERSION, tool_root=str(tool),
                  sources={"project-rules": {"path": "AGENTS.md"}}, watch=["orders"], readonly=[],
                  pipelines={"orders": dict(path="orders", entry=dict(path="orders/recipe.py", symbol="select_orders"),
                      operators=["orders/rules.py"], notebooks=[], verify=[dict(id="fixture",
                          argv=[sys.executable, "-c", "print('fixture')"], env={}, timeout=5)])})
    source = root / "bootstrap.json"
    source.write_text(json.dumps(config))
    tasks.init(root, source)
    return Project(root)


class MigrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = fixture(self.root)

    def test_all_resources_and_finite_binding_files_exist(self):
        self.assertEqual(len(STANDARDS), 9)
        self.assertEqual(STANDARDS["1.0.0-candidate.1"].name, "pipeline-v1.md")
        for path in STANDARDS.values():
            self.assertTrue(path.read_bytes())
        expected = json.loads((Path(__file__).parent / "fixtures/development-standard-sha256.json").read_text())
        self.assertEqual({p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in STANDARDS.values()}, expected)
        for name, path in implementation_files().items():
            self.assertTrue(name.startswith("demiflow/"))
            self.assertTrue(path.is_file(), name)
        self.assertIn("demiflow/__init__.py", implementation_files())
        self.assertIn("demiflow/forge/io.py", implementation_files())
        self.assertTrue(resource_path("skill/SKILL.md").is_file())

    def test_binding_includes_shared_io_parent_and_launcher(self):
        from demiflow.forge.development import project
        original = project.file_hash
        before = self.project.binding()
        for target in [PACKAGE_ROOT / "__init__.py", PACKAGE_ROOT / "forge/io.py",
                       PACKAGE_ROOT / "forge/development/launcher.py"]:
            with patch.object(project, "file_hash", side_effect=lambda p: "changed" if p == target else original(p)):
                self.assertNotEqual(self.project.binding(), before)

    def test_package_root_is_supported_and_other_roots_are_rejected(self):
        path = self.project.path
        value = json.loads(path.read_text())
        value["tool_root"] = str(PACKAGE_ROOT)
        path.write_text(json.dumps(value))
        self.assertEqual(Project(self.root).tool_root, PACKAGE_ROOT)
        result = subprocess.run([sys.executable, "-B", str(self.root / ".demiforge/forge"), "--help"],
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        value["tool_root"] = str(PACKAGE_ROOT.parent.parent)
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ForgeError, "loaded demiflow"):
            Project(self.root)

    def test_runner_tool_expansion_is_the_validated_location(self):
        self.project.tool_root = PACKAGE_ROOT
        command = dict(id="paths", argv=[sys.executable, "-c",
            "import os,sys;print(sys.argv[1]);print(os.environ['TOOL'])", "{tool}"],
            env={"TOOL": "{tool}"}, timeout=5)
        result = run(command, self.project, self.root / "scratch")
        self.assertTrue(result["passed"])
        self.assertEqual(result["output"].splitlines(), [str(PACKAGE_ROOT)] * 2)

    def test_original_serialization_uses_shared_atomic_text_writer(self):
        value = {"path": Path("fixture"), "nan": float("nan")}
        expected = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        with patch("demiflow.forge.development.io.atomic_write_text") as writer:
            atomic_write_json(self.root / "record.json", value)
            writer.assert_called_once_with(self.root / "record.json", expected)
        atomic_write_json(self.root / "record.json", value)
        self.assertEqual((self.root / "record.json").read_text(), expected)
        self.assertFalse(expected.endswith("\n"))

    def test_stdlib_launcher_never_imports_yaml_arrow_click_or_old_package(self):
        launcher = self.root / ".demiforge/forge"
        code = '''import importlib.abc,runpy,sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'yaml','pyarrow','click','demiurge'}:
            raise AssertionError('forbidden dependency: '+fullname)
sys.meta_path.insert(0,Block())
sys.argv=[sys.argv[1],'--help']
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(launcher)],
                                cwd=self.root, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("history", result.stdout)

    def test_launcher_rejects_oversize_before_json_parsing(self):
        from demiflow.forge.development.launcher import SCRIPT
        self.assertIn(f"CONFIG_LIMIT = {MAX_TOTAL}\n", SCRIPT)
        with self.project.path.open("wb") as stream:
            stream.truncate(MAX_TOTAL + 1)  # Sparse fixture; no large data allocation.
        code = '''import json,runpy,sys
def forbidden(*args,**kwargs):
    raise AssertionError('JSON parsing happened before byte admission')
json.loads=forbidden
sys.argv=[sys.argv[1],'list']
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", code,
                                 str(self.root / ".demiforge/forge")],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("configuration byte limit", result.stderr)
        self.assertNotIn("JSON parsing happened", result.stderr)


class HistoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = fixture(self.root)
        tasks.start(self.project, "old", "orders", "Retain unresolved facts", ["orders"], ["No production"])
        self.directory = self.root / ".demiforge/tasks/old"
        self.path = self.directory / "task.json"
        self.state = json.loads(self.path.read_text())
        self.state["binding"] = "original-tool-binding"
        self.state["notes"] = [dict(kind="question", text="Original unresolved question")]
        self.path.write_text(json.dumps(self.state))
        (self.directory / "handoff.json").write_text(json.dumps({"status": "accepted-local"}))

    def saved(self):
        return {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}

    def test_history_is_read_only_and_never_instantiates_project_or_locks(self):
        before = self.saved()
        with patch("demiflow.forge.development.cli.Project", side_effect=AssertionError("Project used")), \
             patch.object(tasks, "locked", side_effect=AssertionError("lock used")):
            from demiflow.forge.development.cli import execute, parser
            result, code = execute(parser().parse_args(["--project", str(self.root), "history", "--id", "old"]))
        self.assertEqual(code, 0)
        self.assertEqual(result["task"]["as_of_result"], "accepted-local")
        self.assertEqual(result["task"]["binding_status"], "stale")
        self.assertEqual(result["task"]["current_evidence"], "unverified")
        self.assertIn("missing_from_record", result["context"]["status"])
        self.assertIn("Original unresolved question", json.dumps(result))
        self.assertEqual(before, self.saved())

    def test_history_does_not_open_any_file_for_writing(self):
        original = os.open
        def read_only(path, flags, *args, **kwargs):
            self.assertFalse(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            return original(path, flags, *args, **kwargs)
        with patch.object(os, "open", side_effect=read_only), \
             patch.object(Path, "mkdir", side_effect=AssertionError("mkdir")):
            history.view(self.root, "old")

    def test_saved_artifacts_page_without_modifying_bytes(self):
        self.state["goal"] = "x" * (history.PAGE_SIZE + 20)
        self.path.write_text(json.dumps(self.state))
        before = self.saved()
        one = history.view(self.root, "old", artifact="task")
        pages = [history.view(self.root, "old", artifact="task", page=i)["text"] for i in range(1, one["pages"] + 1)]
        self.assertEqual("".join(pages), self.path.read_text())
        self.assertEqual(one["sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertEqual(before, self.saved())
        self.assertTrue(history.view(self.root, "old", artifact="handoff-md")["missing"])

    def test_invalid_current_config_does_not_block_saved_history(self):
        self.project.path.write_text("{invalid")
        result = history.view(self.root, "old")
        self.assertEqual(result["task"]["binding_status"], "unavailable")
        self.assertEqual(result["task"]["as_of_result"], "accepted-local")

    def test_matching_tool_binding_does_not_claim_current_acceptance(self):
        self.state["binding"] = self.project.binding()
        self.path.write_text(json.dumps(self.state))
        (self.root / "orders/rules.py").write_text("changed without verification")
        result = history.view(self.root, "old")["task"]
        self.assertEqual(result["binding_status"], "matches_tool_config_only")
        self.assertEqual(result["current_evidence"], "unverified")

    def test_expired_task_cannot_be_refreshed_and_baseline_is_unchanged(self):
        before = self.path.read_bytes()
        result = subprocess.run([sys.executable, "-B", str(self.root / ".demiforge/forge"),
            "refresh", "--id", "old", "--reason", "Must not rebind"], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(before, self.path.read_bytes())

    def test_list_uses_bounded_pages_and_retains_as_of_label(self):
        for name in ["another", "third"]:
            directory = self.directory.parent / name
            directory.mkdir()
            (directory / "task.json").write_text(json.dumps({**self.state, "id": name}))
        result = history.view(self.root, limit=2)
        self.assertEqual([v["id"] for v in result["tasks"]], ["another", "old"])
        self.assertEqual(result["next_offset"], 2)
        self.assertEqual(history.view(self.root, offset=2, limit=2)["tasks"][0]["id"], "third")

    def test_symlink_and_special_file_are_rejected(self):
        original = self.path.read_bytes()
        self.path.unlink()
        external = self.root / "external.json"
        external.write_bytes(original)
        self.path.symlink_to(external)
        with self.assertRaisesRegex(ForgeError, "Symlink"):
            history.view(self.root, "old")
        self.path.unlink()
        os.mkfifo(self.path)
        with self.assertRaisesRegex(ForgeError, "regular"):
            history.view(self.root, "old")

    def test_entry_file_total_json_and_output_budgets_fail_closed(self):
        for option, value, expected in [("MAX_ENTRIES", 0, "entry"), ("MAX_FILE_BYTES", 1, "byte"),
            ("MAX_TOTAL_BYTES", 1, "byte"), ("MAX_JSON_TOKENS", 1, "complexity"),
            ("MAX_OUTPUT_BYTES", 1, "output"), ("MAX_SECONDS", -1, "time")]:
            with self.subTest(option=option), patch.object(history, option, value), self.assertRaisesRegex(ForgeError, expected):
                history.view(self.root)
        self.path.write_text('{"x":' * 70 + '0' + '}' * 70)
        with self.assertRaisesRegex(ForgeError, "complexity"):
            history.view(self.root, "old")

    def test_invalid_ids_and_page_arguments_are_rejected(self):
        for kwargs in [dict(task_id="../old"), dict(limit=51), dict(offset=-1), dict(page=0),
                       dict(artifact="task"), dict(task_id="old", artifact="task", page=99999)]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ForgeError):
                history.view(self.root, **kwargs)


if __name__ == "__main__":
    unittest.main()
