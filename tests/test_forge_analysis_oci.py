"""Fixed OCI descriptions only, without create/start/mount/cgroup mutations."""
from pathlib import Path

import pytest

from demiflow.workbench.analysis import oci
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.workbench.analysis.resource_profile import seccomp
from demiflow.forge.schema import ForgeError


def paths(tmp_path):
    return oci.FixedPaths(tmp_path/"image", tmp_path/"state", tmp_path/"cgroup", 23456, 23456)


def test_kernel_bundle_has_no_sources_private_database_native_slots_or_host_credentials(tmp_path):
    value = oci.bundle(paths(tmp_path), identifier="j"+"a"*32, role="kernel")
    assert value["root"]["readonly"] and value["process"]["noNewPrivileges"]
    assert all(not items for items in value["process"]["capabilities"].values())
    destinations = {mount["destination"] for mount in value["mounts"]}
    assert destinations == {"/proc", "/sys/fs/cgroup", "/scratch", "/work", "/dev", "/job", "/broker"}
    assert value["linux"]["uidMappings"] == [{"containerID": 0, "hostID": 23456, "size": 1}]
    assert {item["type"] for item in value["linux"]["namespaces"]} == {"user", "mount", "pid", "network", "ipc", "uts", "cgroup"}
    assert value["linux"]["resources"]["unified"]["memory.swap.max"] == "0"
    assert not any("TOKEN=" in item or "KEY=" in item or "PYTHONPATH=" in item for item in value["process"]["env"])
    assert not any(path.exists() for path in (tmp_path/"state", tmp_path/"cgroup"))


def test_native_bundle_uses_only_explicit_private_objects_and_analysis_slot_root(tmp_path):
    fixed = paths(tmp_path)
    identifier = "o"+"b"*32
    value = oci.bundle(fixed, identifier="j"+"a"*32, role="native", source_ids=[identifier])
    source = next(item for item in value["mounts"] if item["destination"] == "/sources/" + identifier)
    assert source["source"] == str(fixed.state/"objects"/identifier) and "ro" in source["options"]
    assert next(item for item in value["mounts"] if item["destination"].endswith("native-slots"))["source"] == str(fixed.state/"native-slots")
    assert value["linux"]["resources"]["memory"]["limit"] == 1024**3
    assert value["linux"]["resources"]["pids"]["limit"] == 16


def test_closed_builder_rejects_kernel_lance_mounts_unimplemented_roles_and_path_escape(tmp_path):
    fixed = paths(tmp_path)
    for kwargs in (dict(role="kernel", source_ids=["o"+"a"*32]), dict(role="root-shell"),
                   dict(role="native", source_ids=["../outside"]), dict(role="native", source_ids=["o"+"a"*32]*2)):
        with pytest.raises(ForgeError):
            oci.bundle(fixed, identifier="j"+"a"*32, **kwargs)
    with pytest.raises(ForgeError):
        oci.bundle(fixed, identifier="../../run", role="native")
    with pytest.raises(ForgeError):
        oci.FixedPaths(tmp_path/"image", tmp_path/"image/state", tmp_path/"cgroup", 23456, 23456)
    with pytest.raises(ForgeError):
        oci.FixedPaths(tmp_path/"image", tmp_path/"state", tmp_path/"cgroup", 0, 0)


def test_profile_arithmetic_and_namespace_creation_syscalls_remain_closed():
    resource, _ = profile(); policy, _ = seccomp()
    assert resource["activation_allowed"] is False and resource["status"] == "not_allocated_not_calibrated"
    assert sum(resource["memory"][key] for key in ("kernel", "native", "media", "jobs_headroom")) == resource["memory"]["jobs"]
    assert resource["memory"]["jobs"] + resource["memory"]["controller"] + resource["memory"]["watchdog"] == resource["memory"]["analysis"]
    unconditional = {name for item in policy["syscalls"] if item["action"] == "SCMP_ACT_ALLOW" and "args" not in item for name in item["names"]}
    assert not unconditional & {"unshare", "setns", "mount", "umount2", "ptrace", "bpf", "userfaultfd", "clone3", "clone", "socket"}
    clone = next(item for item in policy["syscalls"] if item["names"] == ["clone"])
    assert clone["args"] == [{"index": 0, "value": 0x7e020000, "valueTwo": 0, "op": "SCMP_CMP_MASKED_EQ"}]


def test_media_has_one_readonly_artifact_no_native_slots_or_external_paths(tmp_path):
    fixed=paths(tmp_path);output='o'+'b'*32
    value=oci.bundle(fixed,identifier='j'+'a'*32,role='media',output_id=output)
    mounts={item['destination']:item for item in value['mounts']}
    assert '/objects/'+output in mounts
    assert mounts['/objects/'+output]['source']==str(fixed.state/'objects'/output)
    assert 'ro' in mounts['/objects/'+output]['options']
    assert '/var/lib/demiflow-analysis/native-slots' not in mounts
    assert value['process']['args'][-1]=='demiflow.workbench.analysis.media_worker'
    assert value['linux']['resources']['memory']['limit']==256*1024**2
    assert value['linux']['resources']['pids']['limit']==8
    for kwargs in (dict(role='media'),dict(role='media',output_id='../escape'),
                   dict(role='media',output_id=output,source_ids=[output]),
                   dict(role='media',output_id=output,input_id='i'+'a'*32),dict(role='kernel',output_id=output)):
        with pytest.raises(ForgeError):oci.bundle(fixed,identifier='j'+'a'*32,**kwargs)
