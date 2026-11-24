"""Owned subprocess entry: run the *original* Git package, never the current one."""
from pathlib import Path
from dataclasses import asdict
import json
import sys

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RunRef


def increment(row):
    return {"value": row["value"] + 1}


def pipeline(root):
    return data.from_items([{"value": 1}, {"value": 2}]).map(increment).write_lance(
        str(root / "out.lance"), mode="create", return_receipt=True)


if __name__ == "__main__":
    import demiflow
    package, root, mode = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
    assert Path(demiflow.__file__).resolve().is_relative_to(package.resolve())
    print("IMPORT_TREE", demiflow.__file__, flush=True)
    if mode == "resume":
        reference = RunRef(**json.loads((root / "run-ref.json").read_text()))
        with RunContext.resume(reference):
            result = pipeline(root)
        (root / "resumed.json").write_text(json.dumps(result.to_dict(), indent=2))
    else:
        assert mode in ("complete", "interrupt")
        root.mkdir(exist_ok=False)
        if mode == "interrupt":
            from demiflow.execution import commit

            def fault(name, commit_id):
                if name == "after_acked":
                    (root / "fault.json").write_text(json.dumps({"name": name, "commit_id": commit_id}))
                    raise RuntimeError("owned-original-ack-interruption")

            commit.fault_point = fault
        try:
            with RunContext.start(root / "run") as run:
                (root / "run-ref.json").write_text(json.dumps(asdict(run.ref)))
                result = pipeline(root)
            (root / "completed.json").write_text(json.dumps(result.to_dict(), indent=2))
        except RuntimeError as exc:
            if mode != "interrupt" or str(exc) != "owned-original-ack-interruption":
                raise
            print("EXPECTED_INTERRUPTION", flush=True)
