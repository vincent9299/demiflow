"""Physical 1:1 channel materialization and local route tables (U3/S1, D1/D2).

The frozen logical plan (dag.py) stays at edge granularity: one logical channel
per edge with producer/consumer partition lists and a routing scheme. This
additive layer derives the physical channels underneath it: every physical
channel carries exactly one producer partition -> one consumer partition (D2),
so the frozen K5 ChannelSequence/Cursor semantics apply unchanged. Derivation is
deterministic (D1): identity([logical_channel_id, producer_partition,
consumer_partition]); K6 participants.channels enumerate these physical ids.

Routing decisions are local and stateless: distribution derives from the
producer's data ordinal (recoverable from the cut), never from hidden mutable
counters, so a replayed record routes identically. Business target classes are
declared, never guessed; the platform only picks among same-class replicas.
"""
from ..execution.contracts.errors import ProtocolError
from ..execution.recovery_contract import document, identifier, identity
from .dag import validate_channel_plan

_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_ROUTINGS = frozenset({"forward", "target", "rebalance", "hash", "broadcast", "fan_out", "fan_in"})
_PHYSICAL_KEYS = frozenset({
    "channel_id", "logical_channel_id", "producer_partition", "consumer_partition",
    "exchange_mode", "routing", "credit_budget", "durability",
})


def physical_channel_id(logical_channel_id, producer_partition, consumer_partition):
    identifier(logical_channel_id)
    identifier(producer_partition)
    identifier(consumer_partition)
    return identity([logical_channel_id, producer_partition, consumer_partition])


def physical_pairs(channel):
    """The 1:1 (producer, consumer) pairs a routing scheme realizes."""
    validate_channel_plan(channel)
    producers = channel["producer_partitions"]
    consumers = channel["consumer_partitions"]
    routing = channel["routing"]
    if routing == "forward":
        if len(producers) != len(consumers):
            raise ValueError("forward routing needs matching partition counts")
        return list(zip(producers, consumers))
    if routing == "fan_in":
        if len(consumers) != 1:
            raise ValueError("fan_in routing needs exactly one consumer partition")
        return [(producer, consumers[0]) for producer in producers]
    if routing == "fan_out":
        if len(producers) != 1:
            raise ValueError("fan_out routing needs exactly one producer partition")
        return [(producers[0], consumer) for consumer in consumers]
    return [(producer, consumer) for producer in producers for consumer in consumers]


def validate_physical_channel(value):
    if type(value) is not dict or not _PHYSICAL_KEYS <= set(value) \
            or set(value) - _PHYSICAL_KEYS - {"seal_scope"}:
        raise ValueError("physical channel key set is unsupported")
    for name in ("channel_id", "logical_channel_id", "producer_partition", "consumer_partition"):
        identifier(value[name])
    if value["channel_id"] != physical_channel_id(value["logical_channel_id"],
                                                  value["producer_partition"],
                                                  value["consumer_partition"]):
        raise ValueError("physical channel_id does not bind its endpoints")
    if value["exchange_mode"] not in _MODES or value["routing"] not in _ROUTINGS:
        raise ValueError("unsupported exchange mode/routing")
    budget = value["credit_budget"]
    if type(budget) is not dict or set(budget) != {"rows", "bytes"} \
            or any(type(budget[name]) is not int or budget[name] < 1 for name in ("rows", "bytes")):
        raise ValueError("credit budget needs positive rows/bytes")
    durability = value["durability"]
    if type(durability) is not dict or set(durability) != {"declared", "read_domain"} \
            or durability["declared"] not in ("ephemeral", "durable") \
            or type(durability["read_domain"]) is not str or not durability["read_domain"]:
        raise ValueError("durability declaration is unsupported")
    if (value["exchange_mode"] == "finite_sealed") != ("seal_scope" in value):
        raise ValueError("seal scope belongs to finite_sealed only")
    if "seal_scope" in value:
        scope = value["seal_scope"]
        if type(scope) is not dict or set(scope) != {"scope_id", "membership", "generation"}:
            raise ValueError("seal scope key set is unsupported")
        identifier(scope["scope_id"])
        if type(scope["generation"]) is not int or scope["generation"] < 0:
            raise ValueError("seal generation must be nonnegative")
    document(value)
    return value


def materialize_channels(channel):
    """Expand one logical channel plan into its deterministic physical channels."""
    pairs = physical_pairs(channel)
    physical = []
    for producer, consumer in pairs:
        value = {
            "channel_id": physical_channel_id(channel["channel_id"], producer, consumer),
            "logical_channel_id": channel["channel_id"],
            "producer_partition": producer, "consumer_partition": consumer,
            "exchange_mode": channel["exchange_mode"], "routing": channel["routing"],
            "credit_budget": dict(channel["credit_budget"]),
            "durability": dict(channel["durability"]),
        }
        if "seal_scope" in channel:
            value["seal_scope"] = channel["seal_scope"]
        physical.append(validate_physical_channel(value))
    return physical


class RouteTable:
    """Compile-time materialized routing; runtime lookups are local and stateless.

    ``targets`` returns the physical channel ids a record enters at producer
    egress. ``ordinal`` is the producer's data ordinal (recoverable from the
    cut): distribution schemes derive the consumer from it, so replay after
    restore routes identically. ``route_value`` carries the declared hash key
    tuple or the business target class; it is never guessed from payloads.
    """

    def __init__(self, channel, *, key_fields=None, target_classes=None):
        self._channel = validate_channel_plan(channel)
        self._routing = channel["routing"]
        self._physical = materialize_channels(channel)
        self._ids = {(item["producer_partition"], item["consumer_partition"]): item["channel_id"]
                     for item in self._physical}
        self._by_producer = {}
        for item in self._physical:
            self._by_producer.setdefault(item["producer_partition"], []).append(item["consumer_partition"])
        self._key_fields = None
        if self._routing == "hash":
            if type(key_fields) not in (list, tuple) or not key_fields \
                    or any(type(field) is not str for field in key_fields):
                raise ValueError("hash routing requires declared key fields")
            self._key_fields = tuple(key_fields)
        self._target_classes = None
        if self._routing == "target":
            self._target_classes = self._validate_targets(channel, target_classes)

    @staticmethod
    def _validate_targets(channel, target_classes):
        if type(target_classes) is not dict or not target_classes:
            raise ValueError("target routing requires declared target classes")
        consumers = channel["consumer_partitions"]
        seen = set()
        validated = {}
        for target_class, replicas in target_classes.items():
            if type(target_class) is not str or not target_class:
                raise ValueError("target class must be a nonempty string")
            if type(replicas) not in (list, tuple) or not replicas:
                raise ValueError("target class needs a nonempty replica list")
            for partition in replicas:
                identifier(partition)
                if partition not in consumers:
                    raise ValueError("target replica is not a consumer partition")
                if partition in seen:
                    raise ValueError("a partition serves exactly one target class")
                seen.add(partition)
            validated[target_class] = list(replicas)
        if seen != set(consumers):
            raise ValueError("every consumer partition must belong to a target class")
        return validated

    @property
    def physical_channels(self):
        return list(self._physical)

    @property
    def channel_ids(self):
        return tuple(item["channel_id"] for item in self._physical)

    @property
    def multiplicity(self):
        if self._routing == "broadcast":
            return len(self._channel["consumer_partitions"])
        return 1

    def targets(self, producer_partition, ordinal, *, route_value=None):
        consumers = self._by_producer.get(producer_partition)
        if consumers is None:
            raise ProtocolError("INVALID_SPEC", "not a producer partition of this channel")
        if type(ordinal) is not int or ordinal < 0:
            raise ProtocolError("INVALID_SPEC", "ordinal must be a nonnegative int")
        if self._routing in ("forward", "fan_in"):
            return [self._ids[(producer_partition, consumers[0])]]
        if self._routing in ("fan_out", "rebalance"):
            return [self._ids[(producer_partition, consumers[ordinal % len(consumers)])]]
        if self._routing == "hash":
            if type(route_value) not in (list, tuple) or len(route_value) != len(self._key_fields):
                raise ProtocolError("INVALID_SPEC", "hash route_value must match the declared key fields")
            index = int(identity(list(route_value)), 16) % len(consumers)
            return [self._ids[(producer_partition, consumers[index])]]
        if self._routing == "target":
            if type(route_value) is not str or route_value not in self._target_classes:
                raise ProtocolError("INVALID_SPEC", "undeclared business target class")
            replicas = self._target_classes[route_value]
            return [self._ids[(producer_partition, replicas[ordinal % len(replicas)])]]
        return [self._ids[(producer_partition, consumer)] for consumer in consumers]