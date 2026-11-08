"""One closed persistent owned-sandbox lease shared by controller/watchdog."""
from pathlib import Path

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.oci import job_id

FORMAT = "demiflow_analysis_sandbox_lease_v3"
MEDIA_FORMAT = "demiflow_analysis_sandbox_lease_v4"
FIELDS = frozenset({"format", "job_id", "role", "state", "project", "execution_id", "controller", "web_parent",
    "delegation_identity", "jobs_identity", "cgroup", "unit_identity", "init_identity", "reason", "deadline", "owner_digest"})


def load(root, identifier):
    job_id(identifier)
    value = c.decode(_read(Path(root)/identifier/"lease.json"))
    return validate(value, identifier)


def validate(value, identifier=None):
    """Same closed lease for durable recovery and trusted controller frames."""
    media = type(value) is dict and value.get("format") == MEDIA_FORMAT
    c.fields(value, FIELDS | ({"owner_kind"} if media else set()), label="persistent owned sandbox lease")
    job_id(value["job_id"])
    if value["format"] not in {FORMAT, MEDIA_FORMAT} or identifier is not None and value["job_id"] != identifier:
        c.reject("Owned sandbox control format/identity differs; no adoption", code="conflict")
    c.one_of(value["role"], {"kernel", "native", "media"}, "owned sandbox role")
    if media:
        if value["owner_kind"] not in {'media-read','case-read','media-probe'} or value["role"]!={'media-read':'media','case-read':'native','media-probe':'media'}[value["owner_kind"]]:
            c.reject("Read lease kind cannot control another resource role", code="conflict")
    elif value["role"] == "media":
        c.reject("Old generation-owned media lease requires explicit reconciliation", code="conflict")
    c.sha(value["owner_digest"], "original generation control owner digest")
    c.identity(value["execution_id"], "owning analysis execution")
    from demiflow.workbench.projects.registry import identity as project_identity
    project_identity(value["project"])
    for name in ("controller", "web_parent", "init_identity"):
        identity=value[name]
        if identity is None and name=="init_identity":
            continue
        c.fields(identity,{"pid","boot","start"},label="original "+name)
        c.integer(identity["pid"],"owned process PID",2**31-1,1)
        c.text(identity["boot"],"owned process boot",64);c.text(identity["start"],"owned process start",32)
    if type(value["deadline"]) not in {int,float} or not 0 <= value["deadline"] < float("inf"):
        c.reject("Owned sandbox deadline must be finite")
    if value["reason"] is not None:
        c.text(value["reason"],"owned sandbox reason",256)
    c.one_of(value["state"], {"creating", "created", "started", "cleanup_pending", "cleanup_removing_cgroup", "cleanup_complete"}, "owned sandbox state")
    for field in ("delegation_identity", "jobs_identity", "unit_identity"):
        c.fields(value[field], {"device", "inode"}, label="original " + field)
        c.integer(value[field]["device"], "owned device", 2**63-1)
        c.integer(value[field]["inode"], "owned inode", 2**63-1, 1)
    if value["cgroup"] is not None:
        group = value["cgroup"]
        c.fields(group, {"job_id", "role", "cgroup_identity", "delegation_identity", "jobs_identity"}, label="original owned cgroup")
        if any(group[field] != value[field] for field in ("job_id", "role", "delegation_identity", "jobs_identity")):
            c.reject("Lease cgroup belongs to a different original identity", code="conflict")
        c.fields(group["cgroup_identity"], {"device", "inode"}, label="owned leaf identity")
        c.integer(group["cgroup_identity"]["device"], "owned leaf device", 2**63-1)
        c.integer(group["cgroup_identity"]["inode"], "owned leaf inode", 2**63-1, 1)
    return value
