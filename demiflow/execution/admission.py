"""Engine-side data admission: role minimums, fair activation, fair input turns.

ResourceManager/Provider report placement facts and execute reservations; the
engine data scheduler owns activation order, parallelism decisions and input
fairness (resource protocol v1 §07). Admission output is a pure plan: atomicity
comes from the journal accept_plan budget CAS plus the provider reserve, never
from probing free capacity and then starting as if that were atomic.
"""
from dataclasses import dataclass

_DOWNSTREAM_ROLES = frozenset({"consume", "model"})
_UPSTREAM_ROLES = frozenset({"produce"})
_CAPACITY_KEYS = ("cpu_millis", "memory_bytes")


@dataclass(frozen=True)
class RoleDemand:
    allocation_id: str
    role: str
    cpu_millis: int
    memory_bytes: int
    fairness_key: str = ""
    accelerators: tuple = ()

    def __post_init__(self):
        if self.role not in _DOWNSTREAM_ROLES | _UPSTREAM_ROLES:
            raise ValueError("unknown admission role: " + str(self.role))
        for name in _CAPACITY_KEYS:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive int")


def _capacity(value, name, allow_zero=False):
    if type(value) is not dict or set(value) != set(_CAPACITY_KEYS):
        raise ValueError(name + " needs exactly cpu_millis/memory_bytes")
    for key in _CAPACITY_KEYS:
        item = value[key]
        if type(item) is not int or item < 0 or (item == 0 and not allow_zero):
            raise ValueError(name + "." + key + " must be a nonnegative int")
    return dict(value)


def plan_activation(demands, *, capacity, control_reserve=None):
    """Order worker demands so downstream/control progress capacity comes first.

    Downstream roles (consume/model) are admitted before upstream producers; a
    downstream shortfall blocks every producer (upstream must never hold the
    resources a waiting downstream needs). Within a tier, demands interleave
    round-robin across fairness_key groups (model/queue identity), so one model
    or queue cannot exhaust the tier ahead of its peers. Control/lease/commit
    progress capacity is reserved off the top before any worker is considered.

    Returns a pure plan; the caller enacts it through ResourceManager.apply.
    """
    capacity = _capacity(capacity, "capacity")
    control_reserve = _capacity(control_reserve or {k: 0 for k in _CAPACITY_KEYS},
                                "control_reserve", allow_zero=True)
    available = {k: capacity[k] - control_reserve[k] for k in _CAPACITY_KEYS}
    if any(v < 0 for v in available.values()):
        raise ValueError("control reserve exceeds worker capacity")
    demands = list(demands)
    if len({d.allocation_id for d in demands}) != len(demands):
        raise ValueError("duplicate allocation in demands")

    def tier(demand):
        return 0 if demand.role in _DOWNSTREAM_ROLES else 1

    groups = {}
    for demand in demands:
        groups.setdefault((tier(demand), demand.fairness_key or demand.allocation_id), []).append(demand)
    ordered = []
    for tier_rank in (0, 1):
        keys = sorted(k for k in groups if k[0] == tier_rank)
        buckets = [sorted(groups[k], key=lambda d: d.allocation_id) for k in keys]
        while any(buckets):
            for bucket in buckets:
                if bucket:
                    ordered.append(bucket.pop(0))
    admitted, shortfall = [], []
    reserved = {k: 0 for k in _CAPACITY_KEYS}
    downstream_short = False
    for demand in ordered:
        fits = all(reserved[k] + getattr(demand, k) <= available[k] for k in _CAPACITY_KEYS)
        if tier(demand) == 1 and downstream_short:
            fits = False
        if fits:
            admitted.append(demand.allocation_id)
            for k in _CAPACITY_KEYS:
                reserved[k] += getattr(demand, k)
        else:
            shortfall.append(demand.allocation_id)
            if tier(demand) == 0:
                downstream_short = True
    return {"admitted": admitted, "shortfall": shortfall, "reserved": reserved,
            "control_reserve": control_reserve}


class FairTurns:
    """Rotating pass order over a fixed member set.

    Each pass starts one member past the previous start, so a fast member is
    never systematically first; a slow member only exhausts its own credit.
    """

    def __init__(self, members):
        self._members = tuple(members)
        if not self._members:
            raise ValueError("FairTurns needs at least one member")
        self._next = 0

    def pass_order(self):
        count = len(self._members)
        start = self._next
        self._next = (start + 1) % count
        return [self._members[(start + i) % count] for i in range(count)]