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
                module = importlib.import_module('demiflow.execution.executors.stream_checkpoint')
            elif name == "AdmissionQuotaReader":
                module = importlib.import_module('demiflow.operators.admission')
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
