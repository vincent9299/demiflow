"""Existing public exports and the thin installed CLI adapter."""
import importlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


AVAILABLE = all(importlib.util.find_spec(name) for name in ("pyarrow", "yaml", "click"))


@unittest.skipUnless(AVAILABLE, "Public platform API requires its installed dependencies")
class PublicIntegrationTests(unittest.TestCase):
    def test_cold_import_orders_preserve_pipeline_and_root_api(self):
        import demiflow
        package_parent = str(Path(demiflow.__file__).resolve().parent.parent)
        for statement in ("import demiflow.pipeline", "import demiflow.execution", "import demiflow.data",
                          "from demiflow import Pipeline", "from demiflow import *"):
            program = ("import sys; sys.path.insert(0, sys.argv[1]); import demiflow; "
                       "assert set(demiflow.__all__) <= set(dir(demiflow)); "
                       "assert 'Pipeline' not in vars(demiflow); " + statement + "; "
                       "from demiflow import Pipeline; from demiflow.pipeline import Pipeline as direct; "
                       "assert Pipeline is direct")
            with self.subTest(statement=statement):
                result = subprocess.run([sys.executable, "-I", "-B", "-c", program, package_parent],
                                        cwd=tempfile.gettempdir(), capture_output=True, text=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_all_public_exports_keep_original_identity_and_discoverability(self):
        import demiflow
        expected = ["Pipeline", "PipelineProgram", "ResourceAPI", "StreamCheckpoint", "AdmissionQuotaReader",
                    "data", "Dataset", "MaterializedDataset", "Datasource", "Datasink", "ReadTask",
                    "BlockMetadata", "WriteResult", "AggregateFnV2", "AbsMax", "Count", "Max", "Mean", "Min",
                    "Std", "Sum", "PipelineBackend", "PipelineBundleRef", "PipelineExecutionEnvironment",
                    "PipelineRunObservation", "PipelineRunReadiness", "PipelineRunRequest", "PipelineRunResult",
                    "PipelineExecutionTarget", "PlatformRuntimeIdentity"]
        self.assertEqual(demiflow.__all__, expected)
        namespace = {}
        exec("from demiflow import *", namespace)
        for name in expected:
            if name in {"Pipeline", "PipelineProgram", "ResourceAPI"}:
                module = importlib.import_module("demiflow.pipeline")
            elif name == "StreamCheckpoint":
                module = importlib.import_module("demiflow.execution.stream_checkpoint")
            elif name == "AdmissionQuotaReader":
                module = importlib.import_module("demiflow.execution.stream_admission")
            elif name.startswith("Pipeline") or name == "PlatformRuntimeIdentity":
                module = importlib.import_module("demiflow.execution")
            else:
                module = importlib.import_module("demiflow.data")
            value = module if name == "data" else getattr(module, name)
            self.assertIn(name, dir(demiflow))
            self.assertIs(getattr(demiflow, name), value)
            self.assertIs(namespace[name], value)
        with self.assertRaises(AttributeError):
            getattr(demiflow, "_unknown_public_object")

    def test_forge_dev_delegates_help_list_and_exit_codes(self):
        from click.testing import CliRunner
        from demiflow.cli import main
        from test_forge_development_migration import fixture
        runner = CliRunner()
        help_result = runner.invoke(main, ["forge", "dev", "--help"])
        self.assertEqual(help_result.exit_code, 0, help_result.output)
        self.assertIn("history", help_result.output)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            result = runner.invoke(main, ["forge", "dev", "--project", str(root), "list"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(json.loads(result.output)["read_only"])
            result = runner.invoke(main, ["forge", "dev", "--project", str(root), "start", "--id", "incomplete"])
            self.assertEqual(result.exit_code, 2)
            self.assertIn("--pipeline", result.output)


if __name__ == "__main__":
    unittest.main()
