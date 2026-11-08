"""Acceptance-path and failure-path tests using only temporary project fixtures."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from demiflow.forge.development import tasks
from demiflow.forge.development.checks import structural
from demiflow.forge.development.project import (ForgeError, Project, REVIEW_RULES,
                                                   TOOL_ROOT, VERSION, snapshot)
from demiflow.forge.development.runner import OUTPUT_LIMIT, run


class DevelopmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "business"
        self.root.mkdir()
        (self.base / "platform").mkdir()
        (self.base / "platform/api.py").write_text("# read-only dependency\n")
        self.write("AGENTS.md", "Read business spec. Platform is read-only.\n")
        self.write("BUSINESS.md", "One output per input; preserve identity.\n")
        self.write("orders/orders_pipeline.py", "def config(**kw):\n    return kw\ndef run_pipeline(options):\n    return options\n")
        self.write("orders/README.md", "# Orders\n\nFollow DemiForge pipeline-v1.md and BUSINESS.md.\n")
        self.write("orders/operators/map.py", "# existing dirty/untracked business source\n")
        self.write("orders/tests/test_order.py", "# fixture test\n")
        self.write("orders/orders_debug.ipynb", json.dumps({"cells": [
            {"cell_type": "code", "metadata": {"tags": [role]}, "source": source}
            for role, source in zip(("pipeline-run", "pipeline-monitor", "pipeline-results"),
                (["from orders.orders_pipeline import config, run_pipeline\n", "run_pipeline(config())"],
                 ["RUN_ID = 'fixture'\nprint(RUN_ID)"], ["RUN_ID = 'fixture'\nprint(RUN_ID)"]))]}))
        self.config = {"schema": "demiforge-project-v1", "standard": "1.0.0-candidate.6",
                       "tool_root": str(TOOL_ROOT),
                       "sources": {"project-rules": {"path": "AGENTS.md"}, "business": {"path": "BUSINESS.md"}},
                       "watch": ["orders"], "readonly": ["../platform"],
                       "pipelines": {"orders": {"path": "orders", "entry": "orders_pipeline.py",
                           "verify": [{"id": "fixture", "argv": [sys.executable, "-c", "print('fixture passed')"],
                                       "env": {}, "timeout": 5}]}}}
        self.configfile = self.base / "bootstrap.json"
        self.configfile.write_text(json.dumps(self.config))
        tasks.init(self.root, self.configfile)
        self.project = Project(self.root)
        self.start()

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def start(self, task_id="test", writes=None):
        tasks.start(self.project, task_id, "orders", "Test isolated development", writes or ["orders"], ["No production/platform changes"])

    def read_all(self, state):
        for key, info in state["sources"].items():
            for page in range(1, info["pages"] + 1):
                text = tasks.context(self.project, state, key, page)
                self.assertTrue(text.startswith("# " + key))

    def review(self, state):
        claims = {rule: {"status": "pass", "reason": "Reviewed fixture-only task scope and contract",
                         "evidence": ["orders/orders_pipeline.py"]} for rule in REVIEW_RULES}
        path = self.base / "review.json"
        path.write_text(json.dumps(claims))
        return tasks.review(self.project, state, path, "test self-review")

    def ready(self, directory, state):
        self.read_all(state)
        self.assertTrue(tasks.verify(self.project, state, directory, True)["passed"])
        self.review(state)

    def cli(self, *args):
        return subprocess.run([sys.executable, "-B", str(self.root / ".demiforge/forge"), "--project", str(self.root), *args],
                              capture_output=True, text=True, timeout=20)

    def test_full_lifecycle_and_new_process_resume(self):
        with tasks.locked(self.project, "test") as (directory, state):
            self.ready(directory, state)
            result = tasks.handoff(self.project, state, directory, True)
            self.assertEqual(result["status"], "accepted-local")
        result = self.cli("context", "--id", "test")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["evidence"], {"check": "passed", "review": "passed", "verification": "passed"})
        result = self.cli("handoff", "--id", "test", "--accept")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_requires_all_context_pages_and_explicit_execution(self):
        with tasks.locked(self.project, "test") as (directory, state):
            self.assertFalse(tasks.check(self.project, state)["passed"])
            with self.assertRaisesRegex(ForgeError, "allow-tests"):
                tasks.verify(self.project, state, directory, False)
            with self.assertRaisesRegex(ForgeError, "Mechanical"):
                tasks.verify(self.project, state, directory, True)

    def test_dirty_baseline_not_compared_to_git_head(self):
        self.start("narrow", ["orders/tests"])
        with tasks.locked(self.project, "narrow") as (_, state):
            self.read_all(state)
            self.assertTrue(tasks.check(self.project, state)["passed"])
            self.write("orders/tests/new_test.py", "# new authorized test\n")
            self.assertTrue(tasks.check(self.project, state)["passed"])
            self.write("orders/operators/map.py", "# out of scope change\n")
            self.assertIn("out-of-scope", " ".join(tasks.check(self.project, state)["violations"]))
            tasks.refresh(self.project, state, "Cannot launder an out of scope change")
            self.read_all(state)
            self.assertFalse(tasks.check(self.project, state)["passed"])

    def test_readonly_changes_fail(self):
        with tasks.locked(self.project, "test") as (_, state):
            self.read_all(state)
            (self.base / "platform/api.py").write_text("changed")
            self.assertIn("readonly changed", " ".join(tasks.check(self.project, state)["violations"]))

    def test_code_and_test_changes_stale_evidence(self):
        with tasks.locked(self.project, "test") as (directory, state):
            self.ready(directory, state)
            self.write("orders/tests/new_test.py", "# acceptance after this needs a new test run\n")
            result = tasks.handoff(self.project, state, directory, True)
            self.assertEqual(result["status"], "incomplete")
            self.assertEqual(result["gates"]["verification"], "stale")
            self.assertEqual(result["gates"]["review"], "stale")

    def test_context_changes_require_explicit_refresh(self):
        with tasks.locked(self.project, "test") as (_, state):
            self.read_all(state)
            self.write("orders/README.md", "# Orders\nDemiForge updated business contract\n")
            with self.assertRaisesRegex(ForgeError, "refresh"):
                tasks.context(self.project, state, "pipeline-readme")
            tasks.refresh(self.project, state, "Reviewed changed business contract")
            self.assertNotIn("pipeline-readme", state["reads"])
            self.assertIn("project-rules", state["reads"])
            self.read_all(state)
            self.assertTrue(tasks.check(self.project, state)["passed"])

    def test_config_change_cannot_replace_verification(self):
        self.config["pipelines"]["orders"]["verify"][0]["argv"] = ["true"]
        self.project.path.write_text(json.dumps(self.config))
        with tasks.locked(Project(self.root), "test") as (_, state):
            with self.assertRaisesRegex(ForgeError, "configuration changed"):
                tasks.refresh(Project(self.root), state, "Changed tests")

    def test_new_pipeline_can_start_before_scaffolding(self):
        self.config["watch"].append("new")
        self.config["pipelines"]["new"] = copy.deepcopy(self.config["pipelines"]["orders"])
        self.config["pipelines"]["new"].update(path="new", entry="new_pipeline.py")
        self.project.path.write_text(json.dumps(self.config))
        project = Project(self.root)
        tasks.start(project, "new", "new", "new pipeline", ["new"], ["No production"])
        with tasks.locked(project, "new") as (_, state):
            self.assertIn("MISSING", tasks.context(project, state, "pipeline-entry"))
            self.assertFalse(tasks.check(project, state)["passed"])

    def test_scope_and_task_path_traversal_rejected(self):
        for name in ("../platform", "/tmp/outside", "orders/../outside", "orders/datasets", "orders//tests"):
            with self.subTest(name=name), self.assertRaises(ForgeError):
                self.start("bad", [name])
        with self.assertRaises(ForgeError):
            self.start("../escape")
        with self.assertRaises(FileExistsError):
            self.start()
        with self.assertRaises(ForgeError):
            tasks.init(self.root, self.configfile)

    def test_snapshot_inventories_symlinks_and_rejects_special_files(self):
        (self.root / "orders/linked.py").symlink_to(self.base / "platform/api.py")
        self.assertTrue(self.project.tree()['orders/linked.py'].startswith('symlink:'))
        (self.root / "orders/linked.py").unlink()
        os.mkfifo(self.root / "orders/fifo")
        with self.assertRaisesRegex(ForgeError, "Special file"):
            self.project.tree()

    def test_runtime_directories_pruned_and_deletions_detected(self):
        self.write("orders/datasets/table/never-read", "production")
        (self.root / "orders/datasets/platform-link").symlink_to(self.base / "platform")
        with tasks.locked(self.project, "test") as (_, state):
            self.read_all(state)
            self.assertTrue(tasks.check(self.project, state)["passed"])
            (self.root / "orders/orders_pipeline.py").unlink()
            result = tasks.check(self.project, state)
            self.assertFalse(result["passed"])
            self.assertIn("orders/orders_pipeline.py", result["changed"])

    def test_structural_notebook_and_cross_pipeline_failures(self):
        self.write("orders/operators/map.py", "from other.other_pipeline import run_pipeline\n")
        self.write("orders/orders_debug.ipynb", json.dumps({"cells": [{"cell_type": "code", "source": ["def hidden(): pass"]}]}))
        result = " ".join(structural(self.project, "orders"))
        self.assertIn("cross-pipeline", result)
        self.assertIn("defines execution", result)

    def test_notebook_context_reads_code_without_large_saved_outputs(self):
        self.write("orders/large.ipynb", json.dumps({"cells": [{"cell_type": "code",
            "source": ["print('visible code')"], "outputs": ["x" * (2 * 1024 * 1024)]}]}))
        text = self.project.source({"path": "orders/large.ipynb"})
        self.assertIn("visible code", text)
        self.assertLess(len(text), 1000)
        self.assertIn("full file remains snapshot-bound", text)

    def test_overlapping_watch_roots_do_not_double_count_source_bytes(self):
        from demiflow.forge.development import project as module
        self.write("small/file.py", "x" * 60)
        with patch.object(module, "MAX_TOTAL", 100):
            result = snapshot(self.root, ["small", "small/file.py", "small"])
        self.assertEqual(set(result), {"small/", "small/file.py"})

    def test_previous_standard_keeps_its_original_notebook_contract(self):
        config = dict(self.project.config, standard="1.0.0-candidate.1")
        self.write(".demiforge/project.json", json.dumps(config))
        self.write("orders/orders_debug.ipynb", json.dumps({"cells": [
            {"cell_type": "code", "source": ["print('legacy')"]}]}))
        legacy = Project(self.root)
        self.assertFalse(structural(legacy, "orders"))
        self.assertTrue(legacy.source_defs("orders")["standard"]["path"].endswith("pipeline-v1.md"))
        self.assertIn("1.0.0-candidate.1", legacy.source(legacy.source_defs("orders")["standard"]))

    def test_notebook_rejects_added_cells_wrong_roles_and_inspection_submission(self):
        path = self.root / "orders/orders_debug.ipynb"
        original = json.loads(path.read_text())
        expanded = json.loads(json.dumps(original))
        expanded["cells"].append(expanded["cells"][-1])
        path.write_text(json.dumps(expanded))
        self.assertIn("exactly three", " ".join(structural(self.project, "orders")))
        original["cells"][1]["metadata"]["tags"] = ["pipeline-results"]
        original["cells"][1]["source"] = ["run_pipeline({})"]
        path.write_text(json.dumps(original))
        errors = " ".join(structural(self.project, "orders"))
        self.assertIn("requires role tag", errors)
        self.assertIn("must be read-only", errors)

    def test_large_legacy_notebook_is_hashed_but_not_accepted(self):
        # Historical candidate.2 keeps the original 16MiB output contract.
        self.project.config['standard'] = '1.0.0-candidate.2'
        path = self.root / "orders/orders_debug.ipynb"
        doc = json.loads(path.read_text())
        doc["cells"][0]["outputs"] = [{"text": "x" * 4096}]
        path.write_text(json.dumps(doc))
        with patch("demiflow.forge.development.project.MAX_FILE", 2048), \
             patch("demiflow.forge.development.checks.MAX_FILE", 2048):
            first = snapshot(self.root, ["orders/orders_debug.ipynb"])
            doc["cells"][0]["outputs"][0]["text"] += "changed"
            path.write_text(json.dumps(doc))
            self.assertNotEqual(first, snapshot(self.root, ["orders/orders_debug.ipynb"]))
            self.assertIn("archive large outputs", " ".join(structural(self.project, "orders")))
            self.write("orders/oversized.py", "#" * 4096)
            with self.assertRaisesRegex(ForgeError, "Source exceeds"):
                snapshot(self.root, ["orders/oversized.py"])

    def test_candidate3_and_4_inline_output_budget_does_not_relax_python_source(self):
        path = self.root / 'orders/orders_debug.ipynb'
        original = json.loads(path.read_text())
        for version in ('1.0.0-candidate.3', '1.0.0-candidate.4', '1.0.0-candidate.5', '1.0.0-candidate.6'):
            with self.subTest(standard=version):
                self.project.config['standard'] = version
                doc = copy.deepcopy(original)
                doc['cells'][2]['outputs'] = [{'text': 'x' * 4096}]
                path.write_text(json.dumps(doc))
                with patch('demiflow.forge.development.checks.MAX_FILE', 2048):
                    self.assertFalse(structural(self.project, 'orders'))
                    doc['cells'][2]['source'] = ['#' * 4096]
                    path.write_text(json.dumps(doc))
                    self.assertIn('source exceeds source budget', ' '.join(structural(self.project, 'orders')))

    def test_review_requires_all_rules_and_watched_evidence(self):
        path = self.base / "bad-review.json"
        path.write_text("{}")
        with tasks.locked(self.project, "test") as (_, state):
            with self.assertRaisesRegex(ForgeError, "cover exactly"):
                tasks.review(self.project, state, path, "reviewer")
            claims = {r: {"status": "pass", "reason": "reviewed", "evidence": ["/etc/passwd"]} for r in REVIEW_RULES}
            path.write_text(json.dumps(claims))
            with self.assertRaisesRegex(ForgeError, "snapshotted"):
                tasks.review(self.project, state, path, "reviewer")

    def test_corrupt_log_and_unresolved_question_block_acceptance(self):
        with tasks.locked(self.project, "test") as (directory, state):
            self.ready(directory, state)
            state["notes"].append({"kind": "question", "text": "Unconfirmed scope"})
            self.assertFalse(tasks.handoff(self.project, state, directory, True)["ready"])
            state["notes"][0]["resolved"] = "User clarified fixture-only scope"
            log = self.root / state["verification"]["commands"][0]["log"]
            log.write_text("tampered")
            result = tasks.handoff(self.project, state, directory, True)
            self.assertEqual(result["gates"]["verification"], "corrupt")

    def test_verification_cannot_certify_source_changes_made_by_tests(self):
        cmd = self.config["pipelines"]["orders"]["verify"][0]
        cmd["argv"] = [sys.executable, "-c", "from pathlib import Path; Path('orders/tests/test_order.py').write_text('changed')"]
        self.project.path.write_text(json.dumps(self.config))
        self.project = Project(self.root)
        self.start("mutates")
        with tasks.locked(self.project, "mutates") as (directory, state):
            self.read_all(state)
            result = tasks.verify(self.project, state, directory, True)
            self.assertFalse(result["passed"])
            self.assertTrue(result["source_changed_during_tests"])

    def test_runner_timeout_output_cap_and_no_inherited_secrets(self):
        commands = [("timeout", "import time; time.sleep(30)", "timeout"),
                    ("output", f"print('x' * {OUTPUT_LIMIT + 1})", "output_limit"),
                    ("secret", "import os; assert 'FORGE_TEST_SECRET' not in os.environ", None)]
        previous = os.environ.get("FORGE_TEST_SECRET")
        os.environ["FORGE_TEST_SECRET"] = "do not forward"
        try:
            for name, code, expected in commands:
                outcome = run({"id": name, "argv": [sys.executable, "-c", code], "env": {}, "timeout": 1}, self.project, self.base / name)
                self.assertEqual(outcome["failure"], expected)
                self.assertLessEqual(len(outcome["output"]), OUTPUT_LIMIT)
        finally:
            if previous is None:
                os.environ.pop("FORGE_TEST_SECRET")
            else:
                os.environ["FORGE_TEST_SECRET"] = previous

    def test_concurrent_task_writer_is_rejected(self):
        with tasks.locked(self.project, "test"):
            result = self.cli("context", "--id", "test")
            self.assertEqual(result.returncode, 2)
            self.assertIn("busy", result.stderr)

    def test_decision_invalidates_acceptance_and_incomplete_handoff_is_saved(self):
        with tasks.locked(self.project, "test") as (directory, state):
            self.ready(directory, state)
            state["notes"].append({"kind": "decision", "text": "User changed the required output contract"})
            result = tasks.handoff(self.project, state, directory, True)
            self.assertEqual(result["gates"]["review"], "stale")
            self.assertTrue((directory / "handoff.json").is_file())
        result = self.cli("handoff", "--id", "test", "--accept")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout)["status"], "incomplete")

    def test_changed_tool_or_standard_cannot_refresh_away_old_binding(self):
        with tasks.locked(self.project, "test") as (_, state):
            with patch.object(self.project, "binding", return_value="changed-tool-or-standard"):
                with self.assertRaisesRegex(ForgeError, "read-only history"):
                    tasks.refresh(self.project, state, "Cannot ignore tool/standard revision")

    def test_excerpt_keeps_full_source_in_watched_fingerprint(self):
        self.write("BUSINESS.md", "Selected contract\nHistorical decision\n")
        self.config["sources"]["business"]["lines"] = [1, 1]
        self.project.path.write_text(json.dumps(self.config))
        self.project = Project(self.root)
        self.start("excerpt")
        with tasks.locked(self.project, "excerpt") as (_, state):
            before = tasks.current(self.project, state)
            self.assertNotIn("Historical", tasks.context(self.project, state, "business"))
            self.write("BUSINESS.md", "Selected contract\nChanged historical decision\n")
            after = tasks.current(self.project, state)
            self.assertEqual(before[2], after[2])
            self.assertNotEqual(before[3], after[3])

    def test_source_limits_fail_without_partial_acceptance(self):
        with patch("demiflow.forge.development.project.MAX_TOTAL", 8):
            with self.assertRaisesRegex(ForgeError, "bytes exceed"):
                self.project.tree()
        with patch("demiflow.forge.development.project.MAX_FILES", 1):
            with self.assertRaisesRegex(ForgeError, "Too many"):
                self.project.tree()

    def test_review_cannot_mark_mandatory_rules_not_applicable(self):
        claims = {r: {"status": "pass", "reason": "reviewed", "evidence": ["orders/README.md"]} for r in REVIEW_RULES}
        claims["DF-01"]["status"] = "not_applicable"
        path = self.base / "review-na.json"
        path.write_text(json.dumps(claims))
        with tasks.locked(self.project, "test") as (_, state):
            with self.assertRaisesRegex(ForgeError, "mandatory"):
                tasks.review(self.project, state, path, "reviewer")


if __name__ == "__main__":
    unittest.main()
