"""Internal key-only Lance projection used by the public exclude_keys operator."""
from dataclasses import dataclass
from demiflow.data.sources import LanceSource
@dataclass(frozen=True)
class LanceKeySource(LanceSource):
    """The query columns are keys; payload columns remain on the consumer node."""
