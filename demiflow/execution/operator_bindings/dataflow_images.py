"""Native image ownership through the driver and the existing object/row sink."""
from dataclasses import asdict

from demiflow.execution.contracts.dataflow import BoundImageWriter


class RemoteImageStore(BoundImageWriter):
    """Only encoded bytes cross this port; workers cannot publish objects."""

    def __init__(self, journal, store, options):
        from demiflow.operator_llm.media import media_limits
        self.journal, self.store = journal, store
        self.maximum = media_limits(options)['max_image_bytes']

    def store_image(self, raw, options=None):
        if type(raw) is not bytes or len(raw) > self.maximum:
            raise ValueError('Image exceeds max_image_bytes')
        ref = self.store.put(iter((raw,)), max_bytes=self.maximum, media_type='application/octet-stream')
        request = {'image_ref': asdict(ref)}
        return self.journal._rpc(self.journal._command('stage_image', request))
