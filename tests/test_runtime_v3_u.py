"""Lane U unit gate (G-U): Dataset public entry migration to the unified v3 facade.

Each test calls the real production symbol in this worktree. The cross-lane E
facade (dataflow_api) and C action DTO (contracts.dataflow) are spec-bound
recording doubles injected in-process by ``_runtime_v3_u_fakes``; every other
module (checked.action, the local executor, local_kernel, model/document/Lance
actors) is the real baseline implementation. In-process tree binding is asserted
in every test. The recording facade enforces the frozen provider/action-kind/Json
admission contract, so a wiring regression fails the test rather than silently
falling back to an old executor path.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TREE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_TREE / "tests"))
from _runtime_v3_u_fakes import (  # noqa: E402
    RECORD,
    ActionProgram,
    assert_tree_binding,
    install_fakes,
)

install_fakes()

import pyarrow as pa  # noqa: E402
import lance  # noqa: E402

from demiflow import data  # noqa: E402
from demiflow.data.dataset import Dataset, MaterializedDataset  # noqa: E402
from demiflow.data.plan import LogicalPlan  # noqa: E402
from demiflow.data.sources import MaterializedSource, LanceSource  # noqa: E402
from demiflow.execution.pipeline.local_kernel import KeyedSource, UnionSource  # noqa: E402
from demiflow.execution.contracts.recovery_contract import RecoveryContractError  # noqa: E402
from demiflow.errors import LanceWriteError  # noqa: E402
from demiflow.lance.model import LanceWriteReceipt  # noqa: E402
from demiflow._compat.error_transport import make_error  # noqa: E402
from demiflow.operator_llm.parser import parse_prompt_pack  # noqa: E402
from demiflow.operator_llm.tokens import CharacterBudget  # noqa: E402
from demiflow.embeddings import EmbeddingModel  # noqa: E402
from demiflow.agent import AgentConfig  # noqa: E402
from demiflow.environment import OperatorEnvironment  # noqa: E402
from demiflow.objects import LocalObjectStore  # noqa: E402
from demiflow.collect.document_library import DocumentLibrary  # noqa: E402
from demiflow.collect.reading import PromptContext  # noqa: E402
from demiflow.execution.contracts.queue_contract import QueueConfig, ProducerRef, ProducerSourceSpec

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: v1
    model:
      name: mock-model
      transport: openai_compatible
      base_url_env: TEST_PROMPT_URL
      api_key_env: TEST_PROMPT_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: |
      {{ payload | json }}
'''


def _bind():
    return assert_tree_binding()


@pytest.fixture(autouse=True)
def _reset_record(monkeypatch):
    # This gate proves public delegation only. Scope recording call boundaries
    # to each test, keeping real DTOs and modules installed for other gates.
    import _runtime_v3_u_fakes as recording
    from demiflow.execution.executors import dataflow_api
    real_make = dataflow_api.make_action_request
    def request(*args, **kwargs):
        recorded = recording.make_action_request(*args, **kwargs)
        return real_make(*args, **kwargs) if kwargs.get("kind") == "run_stream" else recorded
    monkeypatch.setattr(dataflow_api, "make_action_request", request)
    for name in ("make_native_operation", "run_dataset_action"):
        monkeypatch.setattr(dataflow_api, name, getattr(recording, name))
    # An SDK entry now preflights its declaration through the pure validator and
    # discards the result; record the declaration at that boundary as well.
    from demiflow.planning import dataflow_declaration
    def preflight(kind, configuration, *, bindings):
        recording.make_native_operation(kind, configuration, bindings=bindings)
    monkeypatch.setattr(dataflow_declaration, "validate_native_operation", preflight)
    RECORD.reset()
    yield
    RECORD.reset()


@pytest.fixture(scope="module")
def pack():
    return parse_prompt_pack(PACK)


@pytest.fixture(scope="module")
def model():
    # Config-only fixture model: construction performs no network I/O.
    return EmbeddingModel("fixture", "fixed-revision", 3, "http://127.0.0.1:1/v1")


@pytest.fixture(scope="module")
def lance_uri(tmp_path_factory):
    uri = str(tmp_path_factory.mktemp("lance") / "t.lance")
    lance.write_dataset(pa.table({"k": ["a", "b"], "vec": [[1.0, 2.0], [3.0, 4.0]], "x": [1, 2]}), uri)
    return uri


def _ds(rows=None):
    return data.from_items(rows if rows is not None else [{"x": 1, "k": "a", "item": {"x": 1}, "text": "hi"}])


def _op_names(ds):
    return [type(op).__name__ for op in ds._plan.operations]


class _RayExecutor:
    """Test-domain non-local executor: proves removed public-entry backend guards.

    It carries no live kernel, client or model; only the attributes the migrated
    public entries read while constructing a pure declaration.
    """
    NAME = "ray_core"
    _local_kernel = None
    resource_root = Path.cwd()


def _ray_ds(rows=None):
    ex = _RayExecutor()
    handle = ex_handle(rows if rows is not None else [{"x": 1, "k": "a"}])
    return MaterializedDataset(MaterializedSource(handle, 1), LogicalPlan(), ex)


class _Handle:
    def __init__(self, rows):
        self._rows = list(rows)
        self.row_count = len(self._rows)
        self.blocks = ()
        self.released = False

    def num_blocks(self):
        return 7

    def rows(self):
        return list(self._rows)

    def release(self):
        self.released = True


def ex_handle(rows):
    return _Handle(rows)


# ---------------------------------------------------------------------------
# U001 join
# ---------------------------------------------------------------------------
def test_u001():
    _bind()
    left = _ds([{"k": "a", "x": 1}])
    right = _ds([{"k": "a", "y": 2}])
    result = left.join(right, on="k")
    assert isinstance(result, Dataset)
    assert isinstance(result._source, KeyedSource)
    assert result._source.kind == "join"
    assert RECORD.native == []  # relational source compiled by G, no driver I/O


# ---------------------------------------------------------------------------
# U002 union
# ---------------------------------------------------------------------------
def test_u002(monkeypatch, tmp_path):
    _bind()
    with data.local_execution(worker_mode="thread"):
        a = data.from_items([{"x": 1}])
        b = data.from_items([{"x": 2}])
        result = a.union(b)
        assert isinstance(result._source, UnionSource)
        assert len(result._source.inputs) == 2
        # Removed backend guard: a non-local executor no longer pre-rejects union.
        ra = _ray_ds([{"x": 1}])
        rb = Dataset(ra._source, ra._plan, ra._executor)
        assert isinstance(ra.union(rb)._source, UnionSource)
        # Inputs are declarations; the terminal chooses the hosting provider.
        other = _ray_ds([{"x": 3}])
        mixed = a.union(other)
        assert mixed._source.inputs[0].source is a._source
        assert mixed._source.inputs[1].source is other._source
        assert mixed._source.inputs[1].plan is other._plan
        assert mixed._executor is a._executor
        # Leave the facade recording double and execute the actual Local engine.
        monkeypatch.undo()
        assert mixed.take_all() == [{"x": 1}, {"x": 3}]
        with pytest.raises((TypeError, AttributeError)):
            a.union(object())


# ---------------------------------------------------------------------------
# U003 reduce_by_key
# ---------------------------------------------------------------------------
def test_u003():
    _bind()
    result = _ds([{"k": "a", "x": 1}]).reduce_by_key("k", lambda acc, row: acc)
    assert isinstance(result._source, KeyedSource)
    assert result._source.kind == "reduce"
    with pytest.raises(TypeError, match="reducer must be callable"):
        _ds().reduce_by_key("k", None)


# ---------------------------------------------------------------------------
# U004 group_batches
# ---------------------------------------------------------------------------
def test_u004():
    _bind()
    result = _ds([{"k": "a", "x": 1}]).group_batches("k", max_rows=2)
    assert isinstance(result._source, KeyedSource)
    assert result._source.kind == "groups"
    with pytest.raises(ValueError, match="invalid grouping"):
        _ds().group_batches("k", max_rows=0)


# ---------------------------------------------------------------------------
# U005 exclude_keys
# ---------------------------------------------------------------------------
def test_u005(lance_uri):
    _bind()
    scan = data.read_lance(lance_uri)
    result = scan.exclude_keys(data.read_lance(lance_uri), on="k")
    isinstance(result, Dataset)
    # Kept business guard: a transformed scan is rejected (not a backend guard).
    with pytest.raises(ValueError, match="untransformed read_lance scan"):
        scan.map(lambda r: r).exclude_keys(data.read_lance(lance_uri), on="k")


# ---------------------------------------------------------------------------
# U006 sort
# ---------------------------------------------------------------------------
def test_u006():
    _bind()
    result = _ds().sort("x")
    assert _op_names(result) == ["SortOp"]
    assert "either host" in Dataset.sort.__doc__


# ---------------------------------------------------------------------------
# U007 repartition
# ---------------------------------------------------------------------------
def test_u007():
    _bind()
    result = _ds().repartition(num_blocks=2)
    assert _op_names(result) == ["RepartitionOp"]
    with pytest.raises(ValueError, match="exactly one"):
        _ds().repartition()
    with pytest.raises(ValueError, match="exactly one"):
        _ds().repartition(num_blocks=2, target_num_rows_per_block=10)


# ---------------------------------------------------------------------------
# U008 random_shuffle
# ---------------------------------------------------------------------------
def test_u008():
    _bind()
    result = _ds().random_shuffle(seed=1)
    assert _op_names(result) == ["RandomShuffleOp"]
    with pytest.raises(ValueError, match="num_blocks must be positive"):
        _ds().random_shuffle(num_blocks=0)


# ---------------------------------------------------------------------------
# U009 randomize_block_order
# ---------------------------------------------------------------------------
def test_u009():
    _bind()
    result = _ds().randomize_block_order(seed=1)
    assert _op_names(result) == ["RandomizeBlockOrderOp"]


# ---------------------------------------------------------------------------
# U010 map_prompt
# ---------------------------------------------------------------------------
def test_u010(pack):
    _bind()
    result = _ds().map_prompt("enrich", config=pack, inputs={"payload": "item"}, output="answer")
    assert _op_names(result) == ["OperatorLLMMapOp"]
    assert RECORD.native_kinds() == ["prompt"]
    decl = RECORD.native[0]
    assert decl.configuration["prompt_name"] == "enrich"
    assert decl.bindings == {"payload": "item"}


# ---------------------------------------------------------------------------
# U011 map_prompt_async
# ---------------------------------------------------------------------------
def test_u011(pack):
    _bind()
    result = _ds().map_prompt_async("enrich", config=pack, inputs={"payload": "item"}, output="answer")
    assert _op_names(result)[-1] == "AsyncMapOp"
    assert "prompt" in RECORD.native_kinds()
    assert result._stages  # config-only prompt actor staged, no driver request


# ---------------------------------------------------------------------------
# U012 agentmap_async
# ---------------------------------------------------------------------------
def test_u012(pack):
    _bind()
    env = OperatorEnvironment(resources="resources")
    config = AgentConfig(pack, env, {}, 1000)
    result = _ds().agentmap_async("enrich", config=config, inputs={"payload": "item"}, output="answer")
    assert _op_names(result)[-1] == "AgentMapOp"
    assert RECORD.native_kinds()[-1] == "agent"
    assert RECORD.native[-1].bindings == {"prompt": "enrich"}


# ---------------------------------------------------------------------------
# U013 map_embeddings
# ---------------------------------------------------------------------------
def test_u013(model):
    _bind()
    result = _ds().map_embeddings(model=model, inputs={"text": "text"})
    assert _op_names(result) == ["EmbeddingMapOp"]
    assert RECORD.native_kinds() == ["embeddings"]
    # Removed backend guard: a non-local executor without prompt_actor constructs.
    ray = _ray_ds([{"text": "hi"}])
    ray_result = ray.map_embeddings(model=model, inputs={"text": "text"})
    assert _op_names(ray_result) == ["EmbeddingMapOp"]


# ---------------------------------------------------------------------------
# U014 document_embeddings
# ---------------------------------------------------------------------------
def test_u014(model):
    _bind()
    result = _ds([{"document_input": "hi"}]).document_embeddings(model=model)
    assert _op_names(result) == ["EmbeddingMapOp"]
    assert RECORD.native_kinds() == ["embeddings"]


# ---------------------------------------------------------------------------
# U015 search_vectors
# ---------------------------------------------------------------------------
def test_u015(lance_uri):
    _bind()
    result = _ds([{"vec": [1.0, 2.0]}]).search_vectors(
        query="vec", output="hits", uri=lance_uri, vector_column="vec", columns=["x"])
    assert _op_names(result) == ["VectorSearchOp"]
    assert RECORD.native_kinds() == ["search_vectors"]
    # Removed backend guard: non-local executor without materialize_stream constructs.
    ray = _ray_ds([{"vec": [1.0, 2.0]}])
    ray_result = ray.search_vectors(query="vec", output="hits", uri=lance_uri,
                                    vector_column="vec", columns=["x"])
    assert _op_names(ray_result) == ["VectorSearchOp"]


# ---------------------------------------------------------------------------
# U016 map_cached
# ---------------------------------------------------------------------------
def test_u016(tmp_path):
    _bind()
    result = _ds().map_cached(lambda r: r, cache_dir=str(tmp_path / "cache"), version="v1")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["cache"]
    assert RECORD.native[0].configuration["version"] == "v1"


# ---------------------------------------------------------------------------
# U017 lookup
# ---------------------------------------------------------------------------
def test_u017():
    _bind()
    result = _ds().lookup({"a": 1}, key="k")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["lookup"]
    assert RECORD.native[0].bindings == {"key": "k"}


# ---------------------------------------------------------------------------
# U018 lookup_lance
# ---------------------------------------------------------------------------
def test_u018(lance_uri):
    _bind()
    result = _ds().lookup_lance({"uri": lance_uri, "version": 1}, on="k")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["lookup"]
    assert RECORD.native[0].configuration["on"] == "k"


# ---------------------------------------------------------------------------
# U019 admit_rows
# ---------------------------------------------------------------------------
def test_u019(tmp_path):
    _bind()
    result = _ds().admit_rows(path=str(tmp_path / "admit"), key="k")
    assert isinstance(result, Dataset)
    assert "admit" in RECORD.native_kinds()
    admit = [op for op in RECORD.native if op.kind == "admit"][0]
    assert admit.bindings == {"key": "k"}


# ---------------------------------------------------------------------------
# U020 deduplicate
# ---------------------------------------------------------------------------
def test_u020(tmp_path):
    _bind()
    result = _ds().deduplicate("k", path=str(tmp_path / "dedup"), key="k")
    assert isinstance(result, Dataset)
    assert "deduplicate" in RECORD.native_kinds()
    dedup = [op for op in RECORD.native if op.kind == "deduplicate"][0]
    assert dedup.configuration["on"] == ["k"]


# ---------------------------------------------------------------------------
# U021 search_web
# ---------------------------------------------------------------------------
def test_u021():
    _bind()

    class Session:
        def search(self, *a, **k):
            return []

        async def aclose(self):
            pass

    result = _ds([{"request": "r"}]).search_web(requests="request", output="o", session=Session())
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["search_web"]
    assert RECORD.native[0].bindings == {"requests": "request"}


# ---------------------------------------------------------------------------
# U022 read_object
# ---------------------------------------------------------------------------
def test_u022(tmp_path):
    _bind()
    result = _ds().read_object(reference="ref", output="o", root=str(tmp_path))
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["read_object"]
    assert RECORD.native[0].configuration["root"] == str(tmp_path)


# ---------------------------------------------------------------------------
# U023 decode_document
# ---------------------------------------------------------------------------
def test_u023():
    _bind()
    result = _ds().decode_document(column="x", output="o")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["decode_document"]


# ---------------------------------------------------------------------------
# U024 encode_image
# ---------------------------------------------------------------------------
def test_u024():
    _bind()
    result = _ds().encode_image(column="x", output="o", profile="thumbnail_lanczos_v1")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["encode_image"]
    assert RECORD.native[0].configuration["max_side"] == 1536


# ---------------------------------------------------------------------------
# U025 parse_wikitext
# ---------------------------------------------------------------------------
def test_u025():
    _bind()
    result = _ds().parse_wikitext(column="x", output="o")
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["parse_wikitext"]


# ---------------------------------------------------------------------------
# U026 fetch
# ---------------------------------------------------------------------------
def test_u026(tmp_path):
    _bind()
    result = _ds().fetch(store=LocalObjectStore(str(tmp_path / "objs")))
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["fetch"]
    assert RECORD.native[0].bindings == {"request": "request"}


# ---------------------------------------------------------------------------
# U027 register_documents
# ---------------------------------------------------------------------------
def test_u027(tmp_path):
    _bind()
    lib = DocumentLibrary(index_path=str(tmp_path / "idx"), object_directory=str(tmp_path / "objs"))
    result = _ds([{"request": "r"}]).register_documents(request="request", output="o", library=lib)
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["register_documents"]
    with pytest.raises(TypeError, match="DocumentLibrary"):
        _ds().register_documents(request="request", output="o", library=object())


# ---------------------------------------------------------------------------
# U028 read_documents
# ---------------------------------------------------------------------------
def test_u028(pack):
    _bind()
    ctx = PromptContext(prompt=pack.prompt_definitions["enrich"],
                        budget=CharacterBudget(6000), build_inputs=lambda row, reading: {})
    result = _ds([{"request": "r"}]).read_documents(request="request", output="o", context=ctx)
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["read_documents"]


# ---------------------------------------------------------------------------
# U029 prepend
# ---------------------------------------------------------------------------
def test_u029():
    _bind()
    result = _ds().prepend(_ds([{"x": 0}]), max_rows=10)
    assert _op_names(result)[-1] == "StreamPrependOp"
    assert RECORD.native_kinds() == ["prepend"]
    assert RECORD.native[0].configuration["max_rows"] == 10


# ---------------------------------------------------------------------------
# U030 save_lance
# ---------------------------------------------------------------------------
def test_u030(lance_uri):
    _bind()
    schema = pa.schema([("k", pa.string()), ("x", pa.int64())])
    result = _ds().save_lance(lance_uri, schema=schema, key="k", stage="s")
    assert _op_names(result)[-1] == "SaveLanceOp"
    assert RECORD.native_kinds() == ["save_lance"]
    assert RECORD.native[0].bindings == {"stage": "s"}


# ---------------------------------------------------------------------------
# U031 run_stream
# ---------------------------------------------------------------------------
def test_u031():
    _bind()
    # Kept retired-checkpoint guard fires before any declaration.
    with pytest.raises(RecoveryContractError, match="checkpoint mode is retired"):
        _ds().run_stream(checkpoint="x")
    assert RECORD.native == []
    # Removed public-entry backend guard: the run_stream declaration is emitted
    # and the plan drives to completion through the unified entry (local lambda).
    stats = _ds([{"x": 1}, {"x": 2}]).map_async(lambda r: r, concurrency=1).run_stream()
    assert ("run_stream", {"action": "run_stream"}) in [
        (op.kind, op.configuration) for op in RECORD.native]
    assert stats.emitted == 2


# ---------------------------------------------------------------------------
# U032 enqueue
# ---------------------------------------------------------------------------
def test_u032(tmp_path):
    _bind()
    qc = QueueConfig(backend="sqlite", queue_id="q", path=str(tmp_path / "q.db"))
    source = ProducerSourceSpec(kind="records", source_id="src", uri="file:///tmp/x", version=1)
    producer = ProducerRef(queue_uuid="qu", pool="pool", generation="gen",
                           producer_id="pid", source=source, epoch=0, token="tok")
    result = _ds().enqueue(qc, producer=producer)
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["enqueue"]
    assert RECORD.native[0].configuration["messages"] == "messages"


# ---------------------------------------------------------------------------
# U033 ack_queue
# ---------------------------------------------------------------------------
def test_u033(tmp_path):
    _bind()
    qc = QueueConfig(backend="sqlite", queue_id="q", path=str(tmp_path / "q.db"))
    result = _ds().ack_queue(qc)
    assert isinstance(result, Dataset)
    assert RECORD.native_kinds() == ["ack_queue"]
    assert RECORD.native[0].configuration["keep_row"] is True


# ---------------------------------------------------------------------------
# U034 materialize
# ---------------------------------------------------------------------------
def test_u034():
    _bind()
    handle = _Handle([{"x": 1}])
    RECORD.results["materialize"] = handle
    result = _ds().materialize()
    assert isinstance(result, MaterializedDataset)
    assert RECORD.request_kinds() == ["materialize"]
    assert RECORD.last_request().arguments == {}
    assert result._source.handle is handle


# ---------------------------------------------------------------------------
# U035 take
# ---------------------------------------------------------------------------
def test_u035():
    _bind()
    RECORD.results["take"] = [{"x": 1}, {"x": 2}]
    assert _ds([{"x": 1}, {"x": 2}, {"x": 3}]).take(2) == [{"x": 1}, {"x": 2}]
    assert RECORD.request_kinds() == ["take"]
    assert RECORD.last_request().arguments == {"limit": 2}
    # take(0) is a zero-read bounded collection, not a truncation of a full read.
    RECORD.reset()
    RECORD.results["take"] = []
    assert _ds([{"x": 1}]).take(0) == []
    assert RECORD.last_request().arguments == {"limit": 0}
    with pytest.raises(ValueError, match="take limit must be non-negative"):
        _ds().take(-1)


# ---------------------------------------------------------------------------
# U036 take_all
# ---------------------------------------------------------------------------
def test_u036(monkeypatch):
    _bind()
    RECORD.results["take_all"] = [{"x": 1}, {"x": 2}]
    assert _ds([{"x": 1}, {"x": 2}]).take_all() == [{"x": 1}, {"x": 2}]
    assert RECORD.request_kinds() == ["take_all"]
    assert RECORD.last_request().arguments == {"limit": None}
    # The unified terminal owns bounded collection and the overflow probe.
    # This gate proves forwarding; real overflow and exact-limit capacity are
    # exercised by test_runtime_v3_terminal_collection with actual workers.
    RECORD.reset()
    RECORD.results["take_all"] = [{"x": 1}, {"x": 2}]
    assert _ds([{"x": 1}, {"x": 2}]).take_all(limit=2) == [{"x": 1}, {"x": 2}]
    assert RECORD.request_kinds() == ["take_all"]
    assert RECORD.last_request().arguments == {"limit": 2}
    from demiflow.execution.executors import dataflow_api
    overflow = ValueError("Dataset contains more than the take_all limit of 2 rows")
    def reject(request):
        assert request.kind == "take_all"
        assert request.arguments == {"limit": 2}
        raise overflow
    monkeypatch.setattr(dataflow_api, "run_dataset_action", reject)
    with pytest.raises(ValueError, match="more than the take_all limit"):
        _ds([{"x": 1}, {"x": 2}, {"x": 3}]).take_all(limit=2)
    assert RECORD.last_request().arguments == {"limit": 2}
    with pytest.raises(ValueError, match="take_all limit must be non-negative"):
        _ds().take_all(limit=-1)


# ---------------------------------------------------------------------------
# U037 take_batch
# ---------------------------------------------------------------------------
def test_u037():
    _bind()
    RECORD.results["take_batch"] = [{"x": 1}]
    _ds().take_batch(1, batch_format="pandas")
    assert RECORD.request_kinds() == ["take_batch"]
    assert RECORD.last_request().arguments["batch_size"] == 1
    with pytest.raises(ValueError):
        _ds().take_batch(0, batch_format="pandas")


# ---------------------------------------------------------------------------
# U038 iter_rows
# ---------------------------------------------------------------------------
def test_u038():
    _bind()
    RECORD.results["iter_rows"] = [{"x": 1}, {"x": 2}]
    assert list(_ds().iter_rows()) == [{"x": 1}, {"x": 2}]
    assert RECORD.request_kinds() == ["iter_rows"]


# ---------------------------------------------------------------------------
# U039 iter_batches
# ---------------------------------------------------------------------------
def test_u039():
    _bind()
    RECORD.results["iter_batches"] = [[{"x": 1}]]
    assert list(_ds().iter_batches(batch_size=1)) == [[{"x": 1}]]
    assert RECORD.request_kinds() == ["iter_batches"]
    assert RECORD.last_request().arguments["batch_size"] == 1
    with pytest.raises(ValueError):
        _ds().iter_batches(batch_size=0)


# ---------------------------------------------------------------------------
# U040 count
# ---------------------------------------------------------------------------
def test_u040():
    _bind()
    RECORD.results["count"] = 3
    assert _ds().count() == 3
    assert RECORD.request_kinds() == ["count"]
    assert RECORD.last_request().arguments == {}


# ---------------------------------------------------------------------------
# U041 aggregate
# ---------------------------------------------------------------------------
def test_u041():
    _bind()
    from demiflow.data import Count
    RECORD.results["aggregate"] = {"n": 3}
    agg = Count("x")
    assert _ds().aggregate(agg) == {"n": 3}
    assert RECORD.request_kinds() == ["aggregate"]
    request = RECORD.last_request()
    assert request.arguments == {"names": [agg.name]}
    assert len(request.programs) == 1
    assert request.programs[0].kind == "aggregate"
    with pytest.raises(ValueError, match="at least one aggregation"):
        _ds().aggregate()


# ---------------------------------------------------------------------------
# U042 schema
# ---------------------------------------------------------------------------
def test_u042():
    _bind()
    RECORD.results["schema"] = pa.schema([("x", pa.int64())])
    _ds().schema()
    assert RECORD.request_kinds() == ["schema"]
    assert RECORD.last_request().arguments == {"fetch_if_missing": True}


# ---------------------------------------------------------------------------
# U043 columns
# ---------------------------------------------------------------------------
def test_u043():
    _bind()
    RECORD.results["columns"] = ["x"]
    assert _ds().columns() == ["x"]
    assert RECORD.request_kinds() == ["columns"]


# ---------------------------------------------------------------------------
# U044 require_columns
# ---------------------------------------------------------------------------
def test_u044():
    _bind()
    RECORD.results["columns"] = ["x"]
    _ds().require_columns(["x"])
    assert RECORD.request_kinds() == ["columns"]
    assert RECORD.last_request().arguments == {"fetch_if_missing": True}
    with pytest.raises(ValueError):
        _ds().require_columns(["missing"])


# ---------------------------------------------------------------------------
# U045 size_bytes
# ---------------------------------------------------------------------------
def test_u045():
    _bind()
    RECORD.results["size_bytes"] = 10
    assert _ds().size_bytes() == 10
    assert RECORD.request_kinds() == ["size_bytes"]


# ---------------------------------------------------------------------------
# U046 stats (executor diagnostic, not a registered terminal)
# ---------------------------------------------------------------------------
def test_u046():
    _bind()
    text = _ds().stats()
    assert isinstance(text, str)
    assert RECORD.requests == []  # diagnostics never route through the action facade


# ---------------------------------------------------------------------------
# U047 execution_metadata (executor diagnostic, not a registered terminal)
# ---------------------------------------------------------------------------
def test_u047():
    _bind()
    _ds().execution_metadata()
    assert RECORD.requests == []


# ---------------------------------------------------------------------------
# U048 collect_summary
# ---------------------------------------------------------------------------
def test_u048():
    _bind()
    RECORD.results["iter_rows"] = [{"x": 1}]
    assert _ds().collect_summary(max_rows=10) == [{"x": 1}]
    assert RECORD.request_kinds() == ["iter_rows"]
    # Bounded: overflow fails, never truncates.
    RECORD.reset()
    RECORD.results["iter_rows"] = [{"x": 1}, {"x": 2}]
    with pytest.raises(ValueError, match="row budget exceeded"):
        _ds().collect_summary(max_rows=1)


# ---------------------------------------------------------------------------
# U049 write_datasink
# ---------------------------------------------------------------------------
def test_u049():
    _bind()
    from demiflow.data.datasink import Datasink, WriteResult

    class Sink(Datasink):
        def recovery_identity(self):
            return {"target": "fixture"}

    RECORD.results["write_datasink"] = WriteResult(written_rows=1, failed_rows=0, blocks_written=1, target="fixture")
    assert _ds().write_datasink(Sink()) is None
    assert RECORD.request_kinds() == ["write_datasink"]
    assert isinstance(RECORD.last_request().sink, Sink)
    # A sink without recovery_identity fails closed (kept guard).
    with pytest.raises(RecoveryContractError, match="recovery_identity"):
        _ds().write_datasink(object())


# ---------------------------------------------------------------------------
# U050 write_lance
# ---------------------------------------------------------------------------
def test_u050(lance_uri):
    _bind()
    schema = pa.schema([("x", pa.int64())])
    committed = LanceWriteReceipt(request_hash="h", uri=lance_uri, expected_version=None,
                                  committed_version=1, input_rows=1, written_rows=1,
                                  schema_hash="s", status="committed")
    RECORD.results["write_lance"] = committed
    assert _ds().write_lance(lance_uri, schema=schema) is None
    request = RECORD.last_request()
    assert request.kind == "write_lance"
    assert request.programs[0].kind == "sink"
    assert request.programs[0].value.uri == committed.uri
    # return_receipt surfaces the real receipt.
    assert _ds().write_lance(lance_uri, schema=schema, return_receipt=True) is committed
    # Indeterminate commit raises LanceWriteError, never silently retries.
    RECORD.results["write_lance"] = LanceWriteReceipt(
        request_hash="h", uri=lance_uri, expected_version=None, committed_version=None,
        input_rows=1, written_rows=None, schema_hash="s", status="indeterminate",
        error=make_error(module="m", type_name="T", message="m"))
    with pytest.raises(LanceWriteError):
        _ds().write_lance(lance_uri, schema=schema)


# ---------------------------------------------------------------------------
# U051 write_parquet
# ---------------------------------------------------------------------------
def test_u051(tmp_path):
    _bind()
    from demiflow.data.datasink import WriteResult
    RECORD.results["write_file"] = WriteResult(written_rows=1, failed_rows=0, blocks_written=1, target="t")
    assert _ds().write_parquet(str(tmp_path / "o.parquet")) is None
    request = RECORD.last_request()
    assert request.kind == "write_file"
    assert request.arguments["format"] == "parquet"


# ---------------------------------------------------------------------------
# U052 write_json
# ---------------------------------------------------------------------------
def test_u052(tmp_path):
    _bind()
    from demiflow.data.datasink import WriteResult
    RECORD.results["write_file"] = WriteResult(written_rows=1, failed_rows=0, blocks_written=1, target="t")
    assert _ds().write_json(str(tmp_path / "o.json")) is None
    assert RECORD.last_request().arguments["format"] == "json"


# ---------------------------------------------------------------------------
# U053 write_csv
# ---------------------------------------------------------------------------
def test_u053(tmp_path):
    _bind()
    from demiflow.data.datasink import WriteResult
    RECORD.results["write_file"] = WriteResult(written_rows=1, failed_rows=0, blocks_written=1, target="t")
    assert _ds().write_csv(str(tmp_path / "o.csv")) is None
    assert RECORD.last_request().arguments["format"] == "csv"


# ---------------------------------------------------------------------------
# U054 MaterializedDataset.release
# ---------------------------------------------------------------------------
def test_u054():
    _bind()
    handle = _Handle([{"x": 1}])
    md = MaterializedDataset(MaterializedSource(handle, 1), LogicalPlan(), _ds()._executor)
    md.release()
    assert handle.released is True
    # A handle without release fails closed (kept guard).
    md2 = MaterializedDataset(MaterializedSource(object(), 1), LogicalPlan(), _ds()._executor)
    with pytest.raises(NotImplementedError, match="explicit release"):
        md2.release()


# ---------------------------------------------------------------------------
# U055 MaterializedDataset.num_blocks
# ---------------------------------------------------------------------------
def test_u055():
    _bind()
    handle = _Handle([{"x": 1}])
    md = MaterializedDataset(MaterializedSource(handle, 1), LogicalPlan(), _ds()._executor)
    assert md.num_blocks() == 7
    assert RECORD.requests == []  # handle delegation, not an action facade call
