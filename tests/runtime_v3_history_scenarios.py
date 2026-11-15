"""Measured historical domains; this is not an assertion that all V029 is done.

Only Git b4dd417's sealed_plan_facts.v1 / aligned-barrier-v2 native writer is
generated here. Those are two carrier/protocol names, not two engine versions.
Current-runtime continuation of this native plan remains explicitly unsupported.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

WORKTREE = Path(__file__).resolve().parents[1]
GOLDEN = Path(__file__).parent / "fixtures/runtime_v3_history/prompt-identities.json"
MAX_ARCHIVE = 64 * 1024**2


def _digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prompt_identities():
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.execution.recovery_contract import identity
    from demiflow.execution.recovery_identity import plan_identity
    from demiflow.execution.plan_facts import operation_facts
    from demiflow.execution.chain import plan_code_pin

    golden = json.loads(GOLDEN.read_text())
    text = golden["prompt"]
    assert hashlib.sha256(text.encode()).hexdigest() == golden["prompt_sha256"]
    measured = {}
    for size in (1024, 1025, 1500):
        configured = text.replace("result: {type: string}", "result: {type: string, enum: "
            + json.dumps([str(index) for index in range(size)]) + "}")
        dataset = data.from_items([{"item": "one"}]).map_prompt_async("enrich",
            config=parse_prompt_pack(configured), inputs={"payload": "item"}, output="answer")
        native = operation_facts(dataset._plan.operations[-1])["native"]
        action = plan_identity("materialize", dataset._source, dataset._plan)
        measured[str(size)] = {"native_id": identity(native),
            "operation_id": identity(action["operations"][0]), "action_id": identity(action),
            "code_operations_id": identity(plan_code_pin(dataset._plan)["operations"])}
        assert measured[str(size)] == golden["identities"][str(size)], (size, measured[str(size)])
    return {"original_commit": golden["original_commit"], "fixture_sha256": _digest(GOLDEN),
            "identities": measured}


def _extract_original(root, revision):
    # Fixed, trusted local Git object; no clone/network/environment installation.
    listing = subprocess.check_output(["git", "ls-tree", "-r", "-l", revision, "demiflow"],
                                      cwd=WORKTREE, text=True, timeout=10).splitlines()
    assert 0 < len(listing) <= 2000
    assert sum(int(line.split(None, 4)[3]) for line in listing) <= MAX_ARCHIVE
    package = root / "original-package"
    package.mkdir()
    archive = root / "original.tar"
    subprocess.run(["git", "archive", "--format=tar", "--output=" + str(archive),
                    revision, "demiflow"], cwd=WORKTREE, check=True, timeout=20)
    assert archive.stat().st_size <= MAX_ARCHIVE
    archive_digest = _digest(archive)
    total = count = 0
    with tarfile.open(archive) as source:
        for member in source:
            destination = package / member.name
            assert destination.resolve().is_relative_to(package.resolve())
            assert member.isdir() or member.isfile(), member.name
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            total += member.size
            count += 1
            assert total <= MAX_ARCHIVE and count <= 2000
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as original, destination.open("wb") as target:
                shutil.copyfileobj(original, target, 1024**2)
            assert destination.stat().st_size == member.size
    archive.unlink()
    return package, {"commit": revision, "archive_sha256": archive_digest,
                     "files": count, "bytes": total}


def _file_digests(root):
    paths = [path for path in root.rglob("*") if path.is_file()
             and not path.name.endswith(("-wal", "-shm"))]
    assert len(paths) <= 4096
    assert sum(path.stat().st_size for path in paths) <= 128 * 1024**2
    return {str(path.relative_to(root)): _digest(path) for path in paths}


class OriginalCarrierReader:
    """Actual, bounded, read-only filesystem port used by the compatibility unit.

    Content hashes remain strict. In particular the old semantic definition_ref
    is not silently relabelled as the digest of its archived .py source.
    """
    def __init__(self, root):
        self.root = root.resolve()

    def read(self, ref, *, max_bytes):
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        path = self.root / ref.key
        if (not path.resolve().is_relative_to(self.root)
                or any(part.is_symlink() for part in (path, *path.parents))):
            raise DataflowError("INVALID_CONTRACT", "unsafe original carrier")
        if not path.is_file() or path.stat().st_size > min(max_bytes, 4 * 1024**2):
            raise DataflowError("CORRUPT_ARTIFACT", "missing or oversized original carrier")
        if _digest(path) != ref.digest:
            raise DataflowError("CORRUPT_ARTIFACT", "original semantic digest is not a content hash")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(64 * 1024), b""):
                yield chunk

    def inspect(self, ref):
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        try:
            return sum(map(len, self.read(ref, max_bytes=4 * 1024**2))) == ref.size_bytes
        except (DataflowError, OSError):
            return False


def original_native_history(root):
    """Real old producer, current read/refusal, then old compatible continuation."""
    import demiflow
    import lance
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RunRef, RecoveryContractError
    from demiflow.execution.recovery_identity import platform_identity
    from demiflow.execution.dataflow_history import load_execution_version, validate_resume_compatibility
    from demiflow.workbench.runs.facts import read_sealed_plan
    from demiflow.workbench.runs.manager import _native_execution_receipt

    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)
    assert root.resolve().is_relative_to(Path("/tmp"))
    root.mkdir()
    revision = json.loads(GOLDEN.read_text())["original_commit"]
    package, origin = _extract_original(root, revision)
    # Stable owned business-code path throughout original first run and resume.
    runner = root / "old-runner.py"
    shutil.copyfile(Path(__file__).with_name("runtime_v3_history_native_runner.py"), runner)
    environment = dict(os.environ, PYTHONPATH=str(package), PYTHONDONTWRITEBYTECODE="1",
                       DEMIFLOW_RESOURCE_ROOT=str(root / "capacity"))

    def run_original(directory, mode):
        command = [sys.executable, "-B", str(runner), str(package), str(directory), mode]
        with (root / (directory.name + "-" + mode + ".log")).open("wb") as output:
            subprocess.run(command, cwd=root, env=environment, stdout=output,
                           stderr=subprocess.STDOUT, timeout=60, check=True)
        return command

    cases = {}
    for name, mode in (("completed", "complete"), ("interrupted", "interrupt")):
        directory = root / name
        command = run_original(directory, mode)
        journal = RunJournal(directory / "run")
        before = _file_digests(directory)
        original_binding = journal.recovery_identity()
        assert original_binding["stream_protocol"] == "aligned-barrier-v2"
        history = load_execution_version(directory / "run", "action-1", max_bytes=4 * 1024**2)
        assert history.schema_version == "demiflow.sealed_plan_facts.v1"
        # The old producer writes K6 after commit but does not bind it in prepared.state.
        assert history.restore_image is None
        plan = read_sealed_plan(directory / "run", "action-1")
        assert plan["available"]
        original_seal = journal.source_binding("action-1-sealed-plan")
        assert plan["dag_id"] == original_seal["dag_id"]
        receipt = _native_execution_receipt(journal, "action-1")
        assert receipt["committed_version"] == 1
        assert receipt["input_rows"] == receipt["written_rows"] == 2
        dataset = lance.dataset(str(directory / "out.lance"), version=1)
        assert dataset.to_table().to_pylist() == [{"value": 2}, {"value": 3}]
        report = validate_resume_compatibility(history, current_runtime=platform_identity(),
                                               store=OriginalCarrierReader(directory / "run"))
        assert not report.supported
        assert any("journal-proven recovery image" in reason for reason in report.reasons)
        assert any("original engine environment" in reason for reason in report.reasons)
        reference = RunRef(**json.loads((directory / "run-ref.json").read_text()))
        try:
            RunContext.resume(reference)
        except RecoveryContractError as error:
            assert "requires the original engine revision" in str(error)
            refusal = str(error)
        else:
            raise AssertionError("current runtime admitted incompatible original history")
        after = _file_digests(directory)
        assert before == after, "read-only/refused resume rewrote historical bytes"
        commits = journal.execution_commits("action-1")
        assert len(commits) == 1 and commits[0]["status"] == "acked"
        item = {"command": command, "history": asdict(history), "compatibility": asdict(report),
                "current_refusal": refusal, "sealed": original_seal, "receipt": receipt,
                "read_only_before": before, "read_only_after": after,
                "original_recovery_contract": original_binding}
        if name == "interrupted":
            assert journal.execution_action("action-1")["result"] is None
            fault = json.loads((directory / "fault.json").read_text())
            assert fault["commit_id"] == commits[0]["commit_id"]
            item["resume_command"] = run_original(directory, "resume")
            resumed = json.loads((directory / "resumed.json").read_text())
            assert resumed == receipt
            assert journal.execution_commits("action-1") == commits
            assert journal.source_binding("action-1-sealed-plan") == original_seal
            assert journal.execution_action("action-1")["result"] is not None
            # Only journal lifecycle progresses. Sealed artifacts, program bytes,
            # native publication and the original RunRef stay byte-identical.
            continued = _file_digests(directory)
            for key, value in before.items():
                if key != "run/run.sqlite" and not key.startswith("run/control"):
                    assert continued[key] == value, key
            item["original_environment_resumed"] = resumed
        assert [entry["version"] for entry in lance.dataset(str(directory / "out.lance")).versions()] == [1]
        cases[name] = item
    evidence = {"origin": origin, "cases": cases,
                "unsupported": ["Current v3 continuation of native sealed_plan_facts.v1",
                                "Unlisted historical carrier families; not inferred from protocol labels"]}
    (root / "history-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence
