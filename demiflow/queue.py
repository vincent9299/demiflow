"""Public embedded queue controls; Dataset owns publication, reading and ACKs."""
from .execution.sqlite_channel import SQLiteChannel, channel_config, bounded_json

def queue_status(config, *, pool='default'):
    """Read bounded counts and archive cursors without initializing/writing a queue."""
    queue=SQLiteChannel.read_only(**config)
    return {**queue.state(pool),'input_sequence':queue.high_water(pool),
            'result_sequence':queue.high_water(pool,results=True)}


__all__ = ['SQLiteChannel', 'channel_config', 'bounded_json', 'queue_status']
