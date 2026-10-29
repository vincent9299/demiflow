"""Result-only map cache with bounded cross-process local coordination."""
import asyncio
from contextlib import contextmanager
import fcntl
import hashlib
import inspect
import json
import math
from pathlib import Path
import time

from ..kv import SQLiteKV, encode
from .plan import CallableSpec, normalize_bound_inputs, normalize_outputs, validate_bound_signature


def selected(predicate, value):
    if predicate is None:
        return True
    result = predicate(value)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError('cache_when must return a boolean synchronously')
    if type(result) is not bool:
        raise TypeError('cache_when must return a boolean synchronously')
    return result


class CacheCall:
    def __init__(self, fn, directory, version, *, inputs=None, output=None, outputs=None,
                 when=None, namespace='default', cache_when=None, wait_timeout_s=120.,
                 max_input_bytes=8*1024**2, max_result_bytes=8*1024**2,
                 max_entries=2_000_000, max_cache_bytes=8*1024**3):
        if not callable(fn):
            raise TypeError('map_cached requires a callable')
        if not isinstance(directory,(str,Path)) or not str(directory).strip() or '://' in str(directory):
            raise ValueError('map_cached requires a local cache_dir')
        if not isinstance(version, str) or not 1 <= len(version) <= 1024:
            raise ValueError('map_cached requires a nonempty bounded version')
        if type(wait_timeout_s) not in (int, float) or not math.isfinite(wait_timeout_s) or not 0 < wait_timeout_s <= 3600:
            raise ValueError('Invalid cache wait_timeout_s')
        if type(max_input_bytes) is not int or not 1 <= max_input_bytes <= 64*1024**2:
            raise ValueError('Invalid cache max_input_bytes')
        if when is not None and not callable(when):
            raise TypeError('cache when must be callable')
        if inputs is None and (output is not None or outputs is not None):
            raise TypeError('map_cached output(s) require inputs')
        if output is not None and (not isinstance(output, str) or not output or outputs is not None):
            raise TypeError('map_cached requires one output or outputs')
        self.inputs = normalize_bound_inputs(inputs) if inputs is not None else None
        self.outputs = normalize_outputs(outputs)
        if self.inputs is not None:
            validate_bound_signature(CallableSpec.create(fn), self.inputs)
        self.fn, self.version, self.directory = fn, version, Path(directory).expanduser().resolve()
        self.output, self.when, self.cache_when = output, when, cache_when
        self.wait_timeout_s, self.max_input_bytes = wait_timeout_s, max_input_bytes
        self.store = SQLiteKV(self.directory/'results.sqlite', namespace, max_result_bytes,
                              max_entries, max_cache_bytes, missing='empty')
        self.resources = getattr(fn, 'resources', ())
        for name, default in (('label', 'map_cached'), ('concurrency', 1), ('queue_depth', 1), ('catch', ())):
            setattr(self, name, getattr(fn, name, default))
        self.hard_timeout = getattr(fn, 'hard_timeout', None)

    def prepare(self, row):
        value = row if self.inputs is None else {arg: row[field] for arg, field in self.inputs.items()}
        identity = {'version': self.version, 'input': value}
        config = getattr(self.fn, 'cache_identity', None)
        if config is not None:
            identity['config'] = config
        payload = encode(identity, self.max_input_bytes)
        return value, hashlib.sha256(payload).hexdigest()

    def attach(self, row, result):
        if self.inputs is None:
            return result
        if self.output is not None:
            return {**row, self.output: result}
        if not isinstance(result, dict):
            raise TypeError('field-bound map_cached without output requires a mapping result')
        updates = result if self.outputs is None else {field: result[key] for key, field in self.outputs.items()}
        return {**row, **updates}

    def call(self, value):
        return self.fn(value) if self.inputs is None else self.fn(**value)

    @contextmanager
    def lock(self, key):
        if self.directory.exists() and not (self.directory/'results.sqlite').exists():
            if any((self.directory/f'{number:02x}').is_dir() for number in range(256)):
                raise ValueError('Legacy map_cached layout requires an explicit cache migration or a new cache_dir')
        directory = self.directory/'locks'
        directory.mkdir(parents=True, exist_ok=True)
        # Fixed 4096 inodes. Collisions serialize calls but never mix results.
        with (directory/key[:3]).open('a+b') as handle:
            yield handle

    def cached(self, key):
        result = self.store.get(key)
        if result['hit'] and selected(self.cache_when, result['value']):
            return result
        return {'hit': False, 'value': None}

    def save(self, key, result):
        if selected(self.cache_when, result):
            self.store.put_many([(key, result)])

    def error(self, key, error):
        # One last error per lock shard, not an unbounded exception history.
        path = self.directory/(key[:3]+'.error.json')
        payload = json.dumps({'type': type(error).__name__, 'error': str(error)[:2048]})
        try:
            path.write_text(payload)
        except OSError as exc:
            error.add_note('Cache error diagnostic could not be written: '+type(exc).__name__)


class CachedSyncMap(CacheCall):
    def __init__(self, fn, *args, **kwargs):
        if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, '__call__', None)):
            raise TypeError('synchronous map_cached rejects async callables')
        super().__init__(fn, *args, **kwargs)

    def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        value, key = self.prepare(row)
        with self.lock(key) as handle:
            deadline = time.monotonic() + self.wait_timeout_s
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('map_cached lock wait exceeded')
                    time.sleep(.02)
            cached = self.cached(key)
            if cached['hit']:
                return self.attach(row, cached['value'])
            try:
                result = self.call(value)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise TypeError('synchronous map_cached rejects async callables')
                self.save(key, result)
                return self.attach(row, result)
            except Exception as exc:
                self.error(key, exc)
                raise


class CachedMap(CacheCall):
    async def _io(self, fn, *args):
        task = asyncio.create_task(asyncio.to_thread(fn, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        value, key = self.prepare(row)
        with self.lock(key) as handle:
            deadline = time.monotonic() + self.wait_timeout_s
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('map_cached lock wait exceeded')
                    await asyncio.sleep(.02)
            cached = await self._io(self.cached, key)
            if cached['hit']:
                return self.attach(row, cached['value'])
            try:
                result = self.call(value)
                if inspect.isawaitable(result):
                    result = await result
                await self._io(self.save, key, result)
                return self.attach(row, result)
            except Exception as exc:
                await self._io(self.error, key, exc)
                raise

    async def _hook(self, name):
        hook = getattr(self.fn, name, None)
        if hook is not None:
            value = hook()
            if inspect.isawaitable(value):
                await value

    async def astart(self):
        await self._hook('astart')

    async def astop(self):
        await self._hook('astop')

    async def aclose(self):
        await self._hook('aclose')
