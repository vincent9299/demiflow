"""Fixed source/provisioner refusal; no real kernel or isolation evidence."""
import ast
import hashlib
from unittest.mock import AsyncMock, patch

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import kernel_worker
from demiflow.workbench.analysis import sandbox
from demiflow.workbench.analysis.provisioner import SandboxProvisioner
from demiflow.workbench.analysis.provisioner import kernel_environment
from demiflow.forge.schema import ForgeError


def command(source="print(1)", language="python", result_name=None):
    return dict(operation="execute", execution_id="execution-one", generation="generation-one", cell_id="one",
                source=source, source_sha256=hashlib.sha256(source.encode()).hexdigest(), language=language, result_name=result_name)


def test_python_source_is_exact_and_sql_is_a_literal_helper_call_with_stable_request():
    python = command("x = 3\nprint(x)")
    assert kernel_worker.execution_command(python, generation="generation-one") == python["source"]
    sql = "select '\\' || '); import os; print(7); --' as text"
    compiled = kernel_worker.execution_command(command(sql, "sql", "summary"), generation="generation-one")
    tree = ast.parse(compiled)
    assert len(tree.body) == 2 and isinstance(tree.body[0], ast.Assign)
    assert tree.body[0].targets[0].id == "summary"
    call = tree.body[0].value
    assert isinstance(call, ast.Call) and call.args[0].value == sql
    assert call.keywords[0].arg == "request_id" and call.keywords[0].value.value == "execution-one"


def test_old_generation_altered_source_and_bad_result_names_refuse():
    with pytest.raises(ForgeError) as error:
        kernel_worker.execution_command(command(), generation="other-generation")
    assert error.value.code == "conflict"
    altered = command(); altered["source"] = "changed"
    with pytest.raises(ForgeError, match="source differs"):
        kernel_worker.execution_command(altered, generation="generation-one")
    for name in ("class", "None", "__builtins__", "name.with.dot", "x; print(7)"):
        with pytest.raises(ForgeError):
            kernel_worker.execution_command(command("select 1", "sql", name), generation="generation-one")


def test_named_sql_notebook_export_import_preserves_language_and_binding_name():
    document = dict(title="SQL+Python", cells=[dict(id="sql", language="sql", source="select count(*) from orders", result_name="orders_summary")])
    exported = c.export_notebook(document)
    assert "orders_summary = sql(" in exported["cells"][0]["source"]
    assert c.import_notebook(exported, title="SQL+Python") == document


@pytest.mark.asyncio
async def test_provisioner_refuses_before_popen_when_outer_sandbox_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(sandbox, "JOB_PATH", tmp_path/"missing.json")
    provisioner = SandboxProvisioner()
    with patch("jupyter_client.provisioning.local_provisioner.LocalProvisioner.pre_launch", new_callable=AsyncMock) as parent:
        with pytest.raises(ForgeError, match="no host execution fallback"):
            await provisioner.pre_launch(env=kernel_environment(), cwd="/work")
        parent.assert_not_called()


def test_kernel_entrypoint_refuses_before_connecting_or_launching_manager(tmp_path, monkeypatch):
    monkeypatch.setattr(sandbox, "JOB_PATH", tmp_path/"missing.json")
    monkeypatch.setattr(kernel_worker, "_manager", lambda: pytest.fail("kernel must not start"))
    with pytest.raises(ForgeError, match="no host execution fallback"):
        kernel_worker.main()


def test_output_entry_without_trusted_context_keeps_original_oci_guard(tmp_path,monkeypatch):
    from demiflow.workbench.analysis import output_worker
    from demiflow.workbench.analysis import job_context
    monkeypatch.setattr(job_context,'_active',None)
    monkeypatch.setattr(sandbox,'JOB_PATH',tmp_path/'missing.json')
    monkeypatch.setattr(output_worker,'execute',lambda *a,**k:pytest.fail('unadmitted reader must not open output'))
    with pytest.raises(ForgeError,match='no host execution fallback'):
        output_worker.run()
