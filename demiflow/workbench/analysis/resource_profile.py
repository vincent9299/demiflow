"""One versioned numeric profile for worker checks and OCI construction."""
import hashlib
from importlib.resources import files

from demiflow.workbench.analysis.contracts import decode


def profile():
    with files(__package__).joinpath("resource-profile-v1.json").open("rb") as source:
        raw = source.read(65537)
    return decode(raw), hashlib.sha256(raw).hexdigest()


def seccomp():
    with files(__package__).joinpath("seccomp-v1.json").open("rb") as source:
        raw = source.read(65537)
    return decode(raw), hashlib.sha256(raw).hexdigest()
