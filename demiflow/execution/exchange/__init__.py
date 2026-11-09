"""Lightweight exports for the first-batch exchange endpoints (U2/S4).

Carrier is the existing staging block file descriptor (D3): no new physical
data transport is built here. K4/K5 protocol models stay in the frozen
``contracts/exchange.py``; this package only wires bounded endpoints over them.
"""
from .segments import (
    read_domain, descriptor_refs, refs_descriptor, validate_reader_domain,
)
from .channel import ChannelProducer, ChannelConsumer

__all__ = [
    "read_domain", "descriptor_refs", "refs_descriptor", "validate_reader_domain",
    "ChannelProducer", "ChannelConsumer",
]