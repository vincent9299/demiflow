"""One versioned numeric profile for worker checks and OCI construction."""
import hashlib
from importlib.resources import files

from demiflow.forge.host_partition import register_analysis_profile_provider
from demiflow.workbench.analysis.contracts import decode


def profile():
    with files(__package__).joinpath("resource-profile-v1.json").open("rb") as source:
        raw = source.read(65537)
    return decode(raw), hashlib.sha256(raw).hexdigest()


register_analysis_profile_provider(profile)


def seccomp():
    with files(__package__).joinpath("seccomp-v1.json").open("rb") as source:
        raw = source.read(65537)
    return decode(raw), hashlib.sha256(raw).hexdigest()
