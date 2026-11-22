"""Business source bytes archived by v3 match the sealed dependency identities."""
import importlib.util
import os
import sys
import uuid

import pytest

from demiflow import data
from demiflow.data.plan import CallableSpec
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.seal.business_sources import business_sources, stage_sources
from demiflow.execution.run_context import RunContext
from runtime_v3_program_probe import program_store


def _biz(tmp_path, monkeypatch, sources, entry):
    bizdir = tmp_path / ("l043biz_" + uuid.uuid4().hex[:8])
    bizdir.mkdir()
    paths, specs, mods = {}, {}, {}
    for name, src in sources.items():
        path = bizdir / (name + ".py")
        path.write_text(src)
        paths[name] = path
        specs[name] = importlib.util.spec_from_file_location(name, path)
        mods[name] = importlib.util.module_from_spec(specs[name])
        monkeypatch.setitem(sys.modules, name, mods[name])
    monkeypatch.syspath_prepend(str(bizdir))
    monkeypatch.setenv("PYTHONPATH", str(bizdir) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    for name in sources:
        specs[name].loader.exec_module(mods[name])
    return mods[entry], paths


def _source_graph():
    return {
        "l043_helper": "def delta(row):\n    return row['v'] + 1\n",
        "l043_entry": ("import l043_helper\n\n"
                       "def fold(acc, row):\n    return acc + l043_helper.delta(row)\n"),
    }


def _source(entry):
    return data.from_items([{"k": "a", "v": 1}]).reduce_by_key("k", entry.fold)._source


def test_source_and_callable_archive_refs_bind_original_bytes(tmp_path, monkeypatch):
    entry, paths = _biz(tmp_path, monkeypatch, _source_graph(), "l043_entry")
    with RunContext.start(tmp_path / 'run') as run:
        with run.action({'kind': 'source-archive'}) as action:
            with program_store(action) as (store, _):
                source = _source(entry)
                discovered = business_sources('source', source)
                callable_discovered = business_sources('callable', CallableSpec.create(entry.fold))
                assert set(discovered) == set(callable_discovered) == set(paths)
                archived = stage_sources(discovered, store=store)
                assert archived == stage_sources(callable_discovered, store=store)
                for name, item in archived.items():
                    ref = ArtifactRef(**item)
                    assert ref.digest == discovered[name].digest
                    assert b''.join(store.read(ref, max_bytes=ref.size_bytes)) == paths[name].read_bytes()
                # A new source revision gets distinct immutable bytes. An already
                # discovered source cannot be staged under its earlier hash.
                original_ref = ArtifactRef(**archived['l043_helper'])
                original_bytes = paths['l043_helper'].read_bytes()
                paths['l043_helper'].write_text("def delta(row):\n    return row['v'] + 9999\n")
                from demiflow.execution.contracts.dataflow_errors import DataflowError
                with pytest.raises(DataflowError, match="changed during sealing"):
                    stage_sources(discovered, store=store)
                changed = stage_sources(business_sources('source', source), store=store)
                assert changed['l043_helper']['digest'] != original_ref.digest
                assert b''.join(store.read(original_ref, max_bytes=original_ref.size_bytes)) == original_bytes
            action.complete({'kind': 'archived'})
