import copy
from importlib import metadata
import json
import os
from pathlib import Path
import types

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.forge.platform import bounded_json, limits_with
from demiflow.forge.platform_environment import environment_inventory, read_bounded
from demiflow.forge import platform_snapshot as snapshots
from demiflow.forge.schema import ForgeError


@pytest.fixture
def small_platform(tmp_path, monkeypatch):
    root = tmp_path / "package" / "demiflow"
    root.mkdir(parents=True)
    (root / "__init__.py").write_text("# public exports\n")
    (root / "_private.py").write_text("VALUE = 1\n")
    (root.parent / "pyproject.toml").write_text('[project]\nname="demiflow"\nversion="0.0.1"\ndependencies=[]\n')
    monkeypatch.setattr(snapshots, "__file__", str(root / "forge" / "platform_snapshot.py"))
    catalog = {"format": "demiflow_forge_platform_catalog_v3", "modules": ["demiflow"],
               "inspection_complete": True, "objects": [{
                   "id": "demiflow:example", "kind": "function",
                   "origin": {"kind": "platform", "module": "demiflow", "path": "__init__.py"},
                   "defined_name": "example", "member_owner": None,
                   "signature": {"status": "available", "text": "(value=1)", "parameters": []},
                   "documentation": {"status": "available", "text": "Original contract."},
                   "native_semantics": {"status": "unknown"}, "diagnostics": []}], "diagnostics": []}
    monkeypatch.setattr(snapshots, "discover_catalog", lambda *a, **kw: copy.deepcopy(catalog))
    return root, catalog


def build():
    return snapshots.build_snapshot(["demiflow"])


def test_snapshot_stability_and_private_content_drift(small_platform, tmp_path):
    root, _ = small_platform
    before = build()
    path = tmp_path / "platform.json"
    snapshots.write_snapshot(path, before)
    assert snapshots.load_snapshot(path)["digests"] == before["digests"]
    assert snapshots.check_snapshot(path)["status"] == "current"
    # Same length and restored mtime must not defeat content identity.
    private = root / "_private.py"
    initial = private.stat()
    private.write_text("VALUE = 2\n")
    os.utime(private, ns=(initial.st_atime_ns, initial.st_mtime_ns))
    changed = snapshots.check_snapshot(path)
    assert changed["status"] == "stale"
    assert changed["changes"] == [{"kind": "platform_file_changed", "path": "_private.py"}]
    (root / "new_resource.json").write_text("{}")
    assert {item["kind"] for item in snapshots.check_snapshot(path)["changes"]} == {"platform_file_changed", "platform_file_added"}
    assert json.loads(path.read_bytes())["digests"] == before["digests"]


def test_interface_and_standard_differences_are_explained(small_platform):
    _, catalog = small_platform
    old = build()
    catalog["objects"][0]["signature"]["text"] = "(value=2)"
    catalog["objects"][0]["documentation"]["text"] = "Changed contract."
    catalog["objects"].append({**copy.deepcopy(catalog["objects"][0]), "id": "demiflow:added"})
    new = build()
    result = snapshots.compare_snapshots(old, new)
    assert result["status"] == "stale"
    assert {item["kind"] for item in result["changes"]} == {"signature_changed", "documentation_changed", "interface_added"}
    new["standard"]["version"] = "3.0.0"
    snapshots._seal(new)
    assert "standard_changed" in {item["kind"] for item in snapshots.compare_snapshots(old, new)["changes"]}
    assert "interface_removed" in {item["kind"] for item in snapshots.compare_snapshots(new, old)["changes"]}


def test_incomplete_comparison_retains_known_changes_and_refuses_publication(small_platform, tmp_path):
    root, catalog = small_platform
    old = build()
    (root / "_private.py").write_text("changed")
    catalog["inspection_complete"] = False
    catalog["diagnostics"] = [{"code": "inspection_incomplete", "module": "demiflow"}]
    incomplete = build()
    result = snapshots.compare_snapshots(old, incomplete)
    assert result["status"] == "incomplete" and result["changes"] and result["diagnostics"]
    with pytest.raises(ForgeError, match="incomplete"):
        snapshots.write_snapshot(tmp_path / "bad.json", incomplete)
    assert not (tmp_path / "bad.json").exists()


def test_source_change_during_inspection_is_rejected(small_platform, monkeypatch):
    root, catalog = small_platform
    def discovery(*a, **kw):
        (root / "_private.py").write_text("changed during discovery")
        return catalog
    monkeypatch.setattr(snapshots, "discover_catalog", discovery)
    with pytest.raises(ForgeError, match="source_changed"):
        build()


def test_relocation_and_provenance_do_not_affect_identity(small_platform, tmp_path, monkeypatch):
    root, _ = small_platform
    old = build()
    import shutil
    relocated = tmp_path / "relocated"
    shutil.copytree(root.parent, relocated)
    monkeypatch.setattr(snapshots, "__file__", str(relocated / "demiflow" / "forge" / "platform_snapshot.py"))
    new = build()
    assert new["digests"] == old["digests"]
    assert new["provenance"] != old["provenance"]


@pytest.mark.parametrize("mutation", ["digest", "scope", "limit", "path", "duplicate", "format", "shape"])
def test_invalid_snapshot_cannot_be_checked(small_platform, tmp_path, mutation):
    value = build()
    if mutation == "digest":
        value["catalog"]["objects"][0]["documentation"]["text"] = "tampered"
    elif mutation == "scope":
        value["scope"]["modules"] = ["evil.module"]
    elif mutation == "limit":
        value["limits"]["timeout_seconds"] = 999
    elif mutation == "path":
        value["platform"]["files"][0]["path"] = "../escape"
    elif mutation == "duplicate":
        value["catalog"]["objects"] *= 2
    elif mutation == "format":
        value["format"] = "demiflow_forge_contract_v1"
    else:
        value["catalog"] = None
    if mutation != "digest":
        snapshots._seal(value)
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(value))
    assert snapshots.check_snapshot(path)["status"] == "invalid"


def test_read_budget_symlink_and_special_file(tmp_path):
    regular = tmp_path / "large.json"
    regular.write_bytes(b"1234")
    with pytest.raises(ForgeError, match="budget"):
        read_bounded(regular, 3)
    link = tmp_path / "link"
    link.symlink_to(regular)
    with pytest.raises(ForgeError, match="Symlink"):
        read_bounded(link, 100)
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ForgeError, match="regular file"):
        read_bounded(fifo, 100)


@pytest.mark.parametrize("limit,value", [("entries", 1), ("file_bytes", 1), ("source_bytes", 1), ("depth", 1)])
def test_source_inventory_budgets(tmp_path, limit, value):
    (tmp_path / "one.py").write_text("content")
    nested = tmp_path / "nested" / "deeper"
    nested.mkdir(parents=True)
    (nested / "two.py").write_text("content")
    with pytest.raises(ForgeError, match="budget"):
        snapshots.source_inventory(tmp_path, limits_with({limit: value}))


def test_source_inventory_refuses_links_and_specials(tmp_path):
    (tmp_path / "link").symlink_to(tmp_path.parent)
    with pytest.raises(ForgeError, match="symlink"):
        snapshots.source_inventory(tmp_path, limits_with())
    (tmp_path / "link").unlink()
    os.mkfifo(tmp_path / "fifo")
    with pytest.raises(ForgeError, match="special file"):
        snapshots.source_inventory(tmp_path, limits_with())


def test_atomic_publication_never_overwrites_or_leaves_temporary(small_platform, tmp_path, monkeypatch):
    root, _ = small_platform
    value = build()
    path = tmp_path / "platform.json"
    snapshots.write_snapshot(path, value)
    original = path.read_bytes()
    with pytest.raises(ForgeError, match="already exists"):
        snapshots.write_snapshot(path, value)
    assert path.read_bytes() == original
    with pytest.raises(ForgeError, match="outside"):
        snapshots.write_snapshot(root / "self.json", value)
    def fail(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(os, "link", fail)
    with pytest.raises(OSError, match="disk full"):
        snapshots.write_snapshot(tmp_path / "failed.json", value)
    assert not (tmp_path / "failed.json").exists()
    assert not list(tmp_path.glob(".forge-write-*"))


def test_cli_scope_exit_codes_and_paginated_diffs(small_platform, tmp_path, monkeypatch):
    root, _ = small_platform
    runner = CliRunner()
    path = tmp_path / "platform.json"
    def invoke(*args):
        result = runner.invoke(main, ["forge", "platform", *map(str, args)])
        return result.exit_code, json.loads(result.output)
    assert invoke("snapshot", "--module", "demiflow", "--out", path)[0] == 0
    assert invoke("check", path)[1]["status"] == "current"
    assert invoke("snapshot", "--module", "demiflow", "--out", path)[0] == 1
    for i in range(4):
        (root / f"new{i}.py").touch()
    code, result = invoke("check", path, "--offset", 1, "--limit", 2)
    assert code == 1 and result["status"] == "stale"
    assert result["page"] == {"total": 4, "offset": 1, "limit": 2, "has_more": True}
    assert result["change_counts"] == {"platform_file_added": 4}
    assert len(result["changes"]) == 2
    assert result["scope"]["modules"] == ["demiflow"]
    assert runner.invoke(main, ["forge", "platform", "check", str(path), "--module", "demiflow"]).exit_code == 2
    assert invoke("check", tmp_path / "missing")[1]["status"] == "invalid"


def test_real_cli_list_show_and_pagination():
    runner = CliRunner()
    result = runner.invoke(main, ["forge", "platform", "list", "--module", "demiflow.agent", "--limit", "1", "--json"])
    assert result.exit_code == 0, result.output
    value = json.loads(result.output)
    assert len(value["objects"]) == 1 and value["page"]["has_more"]
    result = runner.invoke(main, ["forge", "platform", "show", "demiflow.agent:load_agent_config", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["object"]["signature"]["status"] == "available"
    assert runner.invoke(main, ["forge", "platform", "list", "--limit", "201"]).exit_code == 2
    assert runner.invoke(main, ["forge", "platform", "show", "os:system"]).exit_code == 1


def fake_distribution(tmp_path, name, version="1.0", requires=()):
    path = tmp_path / (name + ".dist-info")
    path.mkdir(exist_ok=True)
    (path / "METADATA").write_text(f"Name: {name}\nVersion: {version}\n" + "".join(f"Requires-Dist: {value}\n" for value in requires))
    (path / "RECORD").write_text("example.py,sha256=original,10\n")
    return types.SimpleNamespace(_path=path)


def declaration(requires, extras=None):
    return {"mode": "source", "name": "demiflow", "version": "1", "requires": requires,
            "extras": extras or {}, "project_sha256": "0" * 64}


def test_sealed_installed_platform_uses_bound_metadata_and_still_detects_changes(tmp_path):
    original=fake_distribution(tmp_path,'demiflow')
    other=tmp_path/'other';other.mkdir()
    replacement=fake_distribution(other,'demiflow',version='99')
    dependency=fake_distribution(tmp_path,'alpha')
    config={'mode':'installed','name':'demiflow','version':'1.0','requires':['alpha'],
            'extras':{},'metadata_root':str(original._path)}
    def inventory(own):
        return environment_inventory(config,limits_with(),distribution=lambda name: dependency if name=='alpha' else own)
    before=inventory(original)
    assert inventory(replacement)==before
    (dependency._path/'RECORD').write_text('dependency changed')
    after=inventory(replacement)
    assert after!=before
    (original._path/'METADATA').write_text('Name: demiflow\nVersion: 2.0\n')
    assert inventory(replacement)!=after


def test_dependency_closure_markers_extras_and_metadata_changes(tmp_path):
    distributions = {"alpha": fake_distribution(tmp_path, "alpha", requires=['beta>=1; extra == "feature"', 'absent; python_version < "2"']),
                     "beta": fake_distribution(tmp_path, "beta")}
    def installed(name):
        if name not in distributions:
            raise metadata.PackageNotFoundError(name)
        return distributions[name]
    config = declaration(["alpha[feature]>=1"], {"optional": ["missing>=1"]})
    def inventory(**kwargs):
        return environment_inventory(config, limits_with(), distribution=installed, **kwargs)
    first = inventory()
    assert [item["name"] for item in first["dependencies"]] == ["alpha", "beta"]
    assert first["diagnostics"] == [] and first["dependency_assurance"] == "metadata_only"
    assert inventory(extras=["optional"])["diagnostics"] == [{"code": "dependency_missing", "name": "missing"}]
    (distributions["alpha"]._path / "RECORD").write_text("example.py,sha256=changed,10\n")
    assert inventory() != first
    fake_distribution(tmp_path, "alpha", version="0.5")
    assert any(item["code"] == "dependency_version_mismatch" for item in inventory()["diagnostics"])
    with pytest.raises(ForgeError, match="Unknown"):
        inventory(extras=["unregistered"])


def test_direct_sources_are_hashed_without_leaking_credentials(tmp_path):
    dist = fake_distribution(tmp_path, "alpha", requires=["beta @ https://user:secret@example.test/x?token=hidden"])
    beta = fake_distribution(tmp_path, "beta")
    config = declaration(["alpha"])
    direct = dist._path / "direct_url.json"
    def inventory():
        return environment_inventory(config, limits_with(), distribution=lambda name: dist if name == "alpha" else beta)
    direct.write_text(json.dumps({"url": "https://user:secret@example.test:443/pkg?token=hidden", "archive_info": {"hash": "sha256=abc"}}))
    first = inventory()
    assert not any(token in json.dumps(first) for token in ("secret", "hidden", "https://"))
    direct.write_text(json.dumps({"url": "https://example.test:444/pkg?token=hidden", "archive_info": {"hash": "sha256=abc"}}))
    assert inventory() != first
    direct.write_text(json.dumps({"url": "file:///one/path", "dir_info": {"editable": True}}))
    local = inventory()
    direct.write_text(json.dumps({"url": "file:///another/path", "dir_info": {"editable": True}}))
    assert inventory() == local


@pytest.mark.parametrize("limit,value", [("distributions", 1), ("metadata_file_bytes", 1), ("metadata_bytes", 1)])
def test_environment_budgets(tmp_path, limit, value):
    dist = fake_distribution(tmp_path, "alpha", requires=["beta"])
    beta = fake_distribution(tmp_path, "beta")
    with pytest.raises(ForgeError, match="budget"):
        environment_inventory(declaration(["alpha"]), limits_with({limit: value}), distribution=lambda name: dist if name == "alpha" else beta)


def test_v1_snapshot_retains_its_catalog_and_standard_version(tmp_path):
    value=snapshots.build_snapshot(['demiflow.data'],version=1)
    path=tmp_path/'v1.json';snapshots.write_snapshot(path,value)
    loaded=snapshots.load_snapshot(path)
    assert loaded['format']=='demiflow_forge_platform_snapshot_v1'
    assert loaded['standard']['version']=='1.0.0'
    assert all(x['native_semantics']=={'status':'unknown'} for x in loaded['catalog']['objects'])
    assert snapshots.check_snapshot(path)['status']=='current'
