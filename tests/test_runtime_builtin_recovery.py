"""R5/R6: native built-ins and fixed batches on the one commit protocol."""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext, RecoveryContractError
from demiflow.execution.contracts.recovery_contract import RunRef
from demiflow.execution.journal.run_journal import RunJournal
from test_collect_flow_contract import document, picture


def scope(directory, limits):
    binding = RunJournal(directory).recovery_identity()
    return (
        RunContext.resume(RunRef(str(directory), binding["run_id"]))
        if binding
        else RunContext.start(directory, limits=limits)
    )


def collect_graph(root, family):
    if family == "wiki":
        return data.from_items(
            [
                {"id": i, "text": None if i == 3 else "[[A|label]] {{lang|en|word}}"}
                for i in range(5)
            ]
        ).parse_wikitext(
            column="text",
            output="syntax",
            max_chars=4096,
            max_bytes=65536,
            concurrency=1,
            queue_depth=1,
        )
    if family == "lookup":
        return data.from_items(
            [
                {"id": key, "occurrence": i}
                for i, key in enumerate(["a", "a", "b", "missing", "a"])
            ]
        ).lookup_lance(
            {"uri": str(root / "table"), "version": 1},
            on="id",
            columns=["value"],
            max_matches=1,
            max_bytes=4096,
        )
    raw = (root / "object").read_bytes()
    ref = {"uri": (root / "object").as_uri(), "sha256": hashlib.sha256(raw).hexdigest()}
    graph = data.from_items([{"id": i, "ref": ref} for i in range(5)]).read_object(
        reference="ref",
        output="raw",
        root=str(root),
        max_bytes=65536,
        concurrency=1,
        queue_depth=1,
    )
    if family == "document":
        return graph.decode_document(
            column="raw",
            output="document",
            max_bytes=65536,
            concurrency=1,
            queue_depth=1,
        )
    return graph.encode_image(
        column="raw",
        output="image",
        max_side=17,
        profile="oriented_white_bicubic_v1",
        max_bytes=65536,
        memory_bytes=268435456,
        timeout_s=5,
        concurrency=1,
        queue_depth=1,
    )


def run_collect(root, family, window=None):
    import demiflow.execution.commit as protocol

    root = Path(root)
    directory = root / "run"
    original = protocol.fault_point

    def fault(name, commit_id):
        if name == window:
            fact = RunJournal(directory).execution_commit(commit_id)
            if fact["intent"]["sinks"] == ["materialize"] and fact["sequence"] == 1:
                os._exit(37)

    protocol.fault_point = fault
    try:
        with scope(
            directory,
            {
                "memory_bytes": 1024**3,
                "checkpoint_rows": 2,
                "checkpoint_interval_ms": 60000,
                "max_row_bytes": 1024**2,
                "max_batch_bytes": 2 * 1024**2,
            },
        ) as run:
            result = collect_graph(root, family).materialize().take_all()
            return result, run.ref
    finally:
        protocol.fault_point = original


def child(root, function, *arguments):
    repo = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(repo), str(repo / "tests")]),
        "PYTHONDONTWRITEBYTECODE": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
    }
    return subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import sys; from test_runtime_builtin_recovery import "
            + function
            + "; "
            + function
            + "(sys.argv[1], *sys.argv[2:])",
            str(root),
            *arguments,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )


@pytest.mark.parametrize("family", ["document", "image", "wiki", "lookup"])
def test_builtin_partial_receipts_restore_without_serializing_resources(
    tmp_path, family
):
    from demiflow.execution.contracts import state_codec

    if family == "lookup":
        lance.write_dataset(
            pa.table({"id": ["a", "b"], "value": ["pinned-a", "pinned-b"]}),
            tmp_path / "table",
        )
    elif family != "wiki":
        (tmp_path / "object").write_bytes(
            picture() if family == "image" else json.dumps(document()).encode()
        )
    crashed = child(tmp_path, "run_collect", family, "after_checkpoint_committed")
    assert crashed.returncode == 37, crashed.stdout + crashed.stderr
    journal = RunJournal(tmp_path / "run")
    before = journal.execution_commits("action-1")
    assert len(before) == 1
    first_ref = before[0]["prepared"]["state"]["dataflow-cut-v3"]
    first_bytes = (journal.directory / first_ref["path"]).read_bytes()
    first_cut = json.loads(first_bytes)
    assert not first_cut["cut"]["final"]
    assert [source["next_sequence"] for part in first_cut["contributions"]
            for source in part["sources"]] == [2]
    if family == "lookup":
        lance.write_dataset(
            pa.table({"id": ["a", "b"], "value": ["new-a", "new-b"]}),
            tmp_path / "table",
            mode="overwrite",
        )
    result, reference = run_collect(tmp_path, family)
    assert len(result) == 5 and journal.execution_status()["attempt"] == 2
    facts = journal.execution_commits("action-1")
    assert len(facts) >= 2 and all(f["status"] == "acked" for f in facts)
    assert facts[0]["commit_id"] == before[0]["commit_id"]
    assert facts[0]["prepared"]["state"]["dataflow-cut-v3"] == first_ref
    assert (journal.directory / first_ref["path"]).read_bytes() == first_bytes
    last_ref = facts[-1]["prepared"]["state"]["dataflow-cut-v3"]
    last_cut = json.loads((journal.directory / last_ref["path"]).read_bytes())
    assert last_cut["cut"]["final"]
    sources = [source for part in last_cut["contributions"] for source in part["sources"]]
    assert [(source["next_sequence"], source["end_of_input"]) for source in sources] == [(5, True)]
    kernel_states = []
    for part in last_cut["contributions"]:
        for ref in part["operator_states"].values():
            if ref["media_type"] == "application/x-demiflow-kernel-image-v3":
                path = Path(reference.directory) / ".dataflow-store" / ref["store_id"] / "objects" / ref["key"]
                raw = path.read_bytes()
                assert hashlib.sha256(raw).hexdigest() == ref["digest"]
                kernel_states.append(state_codec.loads(raw, max_bytes=ref["size_bytes"])["state"])
    assert kernel_states and all(state == {"mode": "hooks", "value": {}} for state in kernel_states)
    if family == "lookup":
        assert [r["matches"] for r in result] == [
            [{"value": "pinned-a"}],
            [{"value": "pinned-a"}],
            [{"value": "pinned-b"}],
            [],
            [{"value": "pinned-a"}],
        ]
        assert [r["occurrence"] for r in result] == list(range(5))
    elif family == "document":
        assert all(r["document"]["blocks"] == document()["blocks"] for r in result)
    elif family == "wiki":
        assert result[3]["syntax"] is None
        assert all(
            {"Wikilink", "Template"} <= {n["kind"] for n in r["syntax"]["nodes"]}
            for r in result
            if r["syntax"] is not None
        )
    else:
        import base64

        for row in result:
            encoded = base64.b64decode(row["image"]["data_url"].split(",", 1)[1])
            assert hashlib.sha256(encoded).hexdigest() == row["image"]["sha256"]
        assert len({r["image"]["sha256"] for r in result}) == 1


def test_lookup_rebuilds_pinned_handle_and_discards_cache_on_restore_and_close(
    tmp_path,
):
    from demiflow.operators.lookup import LanceLookup

    lance.write_dataset(pa.table({"id": ["a"], "value": ["old"]}), tmp_path / "table")
    actor = LanceLookup(
        {"uri": str(tmp_path / "table"), "version": 1},
        "id",
        ["value"],
        "matches",
        1,
        4096,
    )

    async def exercise():
        await actor.astart()
        assert (await actor({"id": "a"}))["matches"] == [{"value": "old"}]
        assert actor.cache and actor.cache_bytes > 0
        assert actor.snapshot_state() == {}
        actor.restore_state({})
        assert not actor.cache and actor.cache_bytes == 0 and actor.table is not None
        await actor.aclose()
        assert actor.table is None and not actor.cache and actor.cache_bytes == 0
        lance.write_dataset(
            pa.table({"id": ["a"], "value": ["new"]}),
            tmp_path / "table",
            mode="overwrite",
        )
        await actor.astart()
        try:
            assert (await actor({"id": "a"}))["matches"] == [{"value": "old"}]
            with pytest.raises(ValueError, match="semantic state"):
                actor.restore_state({"cache": "invalid"})
        finally:
            await actor.aclose()

    asyncio.run(exercise())


def expand(row):
    return [{**row, "child": child} for child in range(2)]


async def fixed_batch(rows):
    trace = os.environ.get("R6_BATCH_TRACE")
    if trace:
        with open(trace, "a") as output:
            output.write(str(rows[0]["id"]) + "\n")
    if os.environ.get("R6_WINDOW") == "during_batch" and rows[0]["id"] >= 120:
        os._exit(37)
    return [
        {**row, "group_start": rows[0]["id"], "group_size": len(rows)} for row in rows
    ]


def batch_graph():
    rows = [
        {"id": i, "payload": b"original\x00" if i % 3 else None} for i in range(536)
    ]
    rows += [dict(rows[-1]), dict(rows[-1])]
    return (
        data.from_items(rows)
        .flat_map(expand)
        .batch_map(fixed_batch, max_batch=7, concurrency=1, queue_depth=1)
        .materialize()
    )


def run_batch(root, window=None):
    import demiflow.execution.commit as protocol

    root = Path(root)
    directory = root / "run"
    os.environ["R6_BATCH_TRACE"] = str(root / "calls")
    os.environ["R6_WINDOW"] = window or ""
    original = protocol.fault_point

    def fault(name, commit_id):
        fact = RunJournal(directory).execution_commit(commit_id)
        if name == window and fact["intent"]["sinks"] == ["output"]:
            os._exit(37)

    protocol.fault_point = fault
    try:
        with scope(directory, {"max_row_bytes": 4096, "max_batch_bytes": 8192}) as run:
            result = batch_graph().take_all()
            return result, run.ref
    finally:
        protocol.fault_point = original
        os.environ.pop("R6_BATCH_TRACE", None)
        os.environ.pop("R6_WINDOW", None)


@pytest.mark.parametrize(
    "window", ["during_batch", "after_prepared", "after_sink_commit_before_receipt"]
)
def test_fixed_batches_keep_original_groups_across_native_checkpoints_on_resume(
    tmp_path, window
):
    crashed = child(tmp_path, "run_batch", window)
    assert crashed.returncode == 37, crashed.stdout + crashed.stderr
    calls = len((tmp_path / "calls").read_text().splitlines())
    journal = RunJournal(tmp_path / "run")
    before = journal.execution_commits("action-1")
    if window == "during_batch":
        assert not before  # Source capture alone cannot ACK partial batches.
    else:
        assert len(before) == 1 and before[0]["status"] == "prepared"
    result, reference = run_batch(tmp_path)
    additional_calls = len((tmp_path / "calls").read_text().splitlines()) - calls
    assert additional_calls == (154 if window == "during_batch" else 8)
    source = [
        {"id": i, "payload": b"original\x00" if i % 3 else None} for i in range(536)
    ]
    source += [dict(source[-1]), dict(source[-1])]
    expanded = [child for row in source for child in expand(row)]
    expected = [
        {**row, "group_start": group[0]["id"], "group_size": len(group)}
        for start in range(0, len(expanded), 7)
        for group in [expanded[start : start + 7]]
        for row in group
    ]
    assert result == expected and len(result) == 1076
    facts = journal.execution_commits("action-1")
    assert len(facts) == 2 and all(f["status"] == "acked" for f in facts)
    assert facts[-1]["intent"]["position"]["next_ordinal"] == 538
    assert journal.execution_status()["attempt"] == 2
    assert not list(Path(reference.directory).rglob("checkpoint.json"))


@pytest.mark.parametrize("arguments", [{"concurrency": 2}, {"flush_interval": 0.01}])
def test_other_buffered_recovery_remains_explicitly_gated(tmp_path, arguments):
    with pytest.raises(RecoveryContractError, match="P3 state adapter"):
        with RunContext.start(
            tmp_path / "run", limits={"max_row_bytes": 4096, "max_batch_bytes": 8192}
        ):
            data.from_items([{"id": 1}]).batch_map(
                fixed_batch, max_batch=2, **arguments
            ).materialize()


from test_forge_runs import base, root


def test_original_flat_batch_default_residency_still_rejected(root):
    from demiflow.forge.schema import ForgeError
    from test_operator_foundation_runs import test_managed_native_families

    # The checked Forge path also reserves observation/control facts. Its
    # original fixture exceeds the deployment budget; an ordinary native
    # stream alone does not have that same additional reservation.
    with pytest.raises(
        ForgeError,
        match="Stream payload reservation 268445696 exceeds memory budget 268435456",
    ):
        test_managed_native_families(root, "flat_batch")


async def cancel_batch(rows):
    from demiflow.execution import current_run

    RunContext.cancel(current_run().ref)
    return rows


def test_fixed_batch_cancel_does_not_confirm_source_or_publish_partial_output(tmp_path):
    with pytest.raises(RecoveryContractError, match="cancellation"):
        with RunContext.start(
            tmp_path / "run", limits={"max_row_bytes": 4096, "max_batch_bytes": 8192}
        ):
            data.from_items([{"id": i} for i in range(7)]).batch_map(
                cancel_batch, max_batch=2, concurrency=1
            ).materialize()
    journal = RunJournal(tmp_path / "run")
    assert journal.execution_status()["status"] == "cancelled"
    assert journal.execution_commits("action-1") == []
    assert journal.execution_commits("action-1-source")[0]["status"] == "acked"


@pytest.mark.parametrize("limits", [{"max_output_rows": 8}, {"max_output_bytes": 256}])
def test_fixed_batch_output_budget_failure_does_not_ack_terminal(tmp_path, limits):
    with pytest.raises(RecoveryContractError, match="budget"):
        with RunContext.start(
            tmp_path / "run",
            limits={"max_row_bytes": 4096, "max_batch_bytes": 8192, **limits},
        ):
            batch_graph()
    assert RunJournal(tmp_path / "run").execution_commits("action-1") == []


@pytest.mark.parametrize("count", [0, 1])
def test_fixed_batch_empty_and_short_eof_commit_exact_source(tmp_path, count):
    with RunContext.start(
        tmp_path / "run", limits={"max_row_bytes": 4096, "max_batch_bytes": 8192}
    ):
        result = (
            data.from_items([{"id": i} for i in range(count)])
            .batch_map(fixed_batch, max_batch=7, concurrency=1)
            .materialize()
            .take_all()
        )
    assert result == ([{"id": 0, "group_start": 0, "group_size": 1}] if count else [])
    facts = RunJournal(tmp_path / "run").execution_commits("action-1")
    assert len(facts) == 1 and facts[0]["status"] == "acked"
    assert facts[0]["intent"]["position"]["next_ordinal"] == count


def test_fixed_batch_native_aggregate_byte_limit_precedes_callback(tmp_path):
    from demiflow.data.runtime_contract import RuntimeContractError

    calls = []

    async def callback(rows):
        calls.append(len(rows))
        return rows

    with pytest.raises(RuntimeContractError, match="Stream batch byte budget exceeded"):
        with RunContext.start(
            tmp_path / "run", limits={"max_row_bytes": 4096, "max_batch_bytes": 4096}
        ):
            data.from_items(
                [{"id": i, "text": "x" * 2200} for i in range(4)]
            ).batch_map(callback, max_batch=4, concurrency=1).materialize()
    assert calls == []
    assert RunJournal(tmp_path / "run").execution_commits("action-1") == []
