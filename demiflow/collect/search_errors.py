"""Local search admission exceptions, shared without route/runtime cycles."""


class RouteLeaseExpired(RuntimeError):
    """A local generation expired before HTTP admission, not a target failure."""
