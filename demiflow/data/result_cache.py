"""Result-only map cache with bounded cross-process local coordination."""
import asyncio
from contextlib import contextmanager
import fcntl
import hashlib
import inspect
import functools
import json
import math
from pathlib import Path
import time
import uuid

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


def _state_owner(value):
    while isinstance(value, functools.partial):
        if vars(value):
            raise TypeError('Cached partial with mutable wrapper attributes needs an explicit adapter')
        value = value.func
    return value.__self__ if inspect.ismethod(value) else value


def _state_policy(value):
    """Retain function bindings at the existing native function-state boundary."""
    from ..execution.operator_policy import classify_operator_state
    from ..execution.kernels.task_scope import function_residency_kind, snapshot_resident_function
    owner = _state_owner(value)
    if owner is None:
        return owner, 'none', None
    policy = classify_operator_state(owner, phase='instance')
    if policy.mode == 'unsupported':
        return owner, policy.mode, policy.reason
    if policy.mode == 'function':
        kind = function_residency_kind(value)
        if kind == 'nonrecoverable':
            image = snapshot_resident_function(value, max_bytes=1)
            return owner, kind, image['reason']
        return owner, 'function_slots_v1' if kind == 'adapter' else 'none', None
    # Actor hooks own the actor, not mutable values bound outside that actor.
    # Reuse the existing function call-binding admission without serializing an
    # actor into a new state format. Immutable partial bindings remain usable.
    partial = value
    while isinstance(partial, functools.partial):
        if function_residency_kind(lambda: None, call_args=partial.args,
                                   call_kwargs=partial.keywords) != 'none':
            return owner, 'nonrecoverable', 'Cached partial actor has unadapted bound argument state'
        partial = partial.func
    from ..operator_llm.native_actor import is_native_prompt_actor
    if is_native_prompt_actor(owner):
        return owner, 'none', None  # Rebound to the native journal in the worker.
    return owner, policy.mode, policy.reason


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

    def bind_checkpoint_cache(self, owner):
        """Bind stable native state ownership, without changing result keys.

        A result's creator tag is only provenance. The authenticated operator
        image is the sole authority for whether this owner's state includes it.
        Other owners keep the original result-only sharing semantics.
        """
        if (self.validate_runtime_state() is not None
                or not any(_state_policy(value)[1] not in ('none', 'nonrecoverable')
                           for value in (self.fn, self.when, self.cache_when))):
            return
        if hasattr(self, '_cache_owner'):
            if self._cache_owner != owner:
                raise ValueError('Cached callable state ownership differs')
            return
        self._cache_owner = owner
        self._cache_incarnation = uuid.uuid4().hex
        self._cache_generation = 1
        self._cache_restored_generation = 0

    def _cache_origin(self, result, *, replayable=True):
        return json.dumps({'owner': self._cache_owner, 'generation': self._cache_generation,
                           'incarnation': self._cache_incarnation, 'replayable': replayable,
                           'result_sha256': hashlib.sha256(encode(result, self.store.max_value_bytes)).hexdigest()},
                          separators=(',', ':'))

    def _requires_state_replay(self, origin, result):
        if origin is None:
            return False  # Historical result-only entries remain readable.
        from ..execution.contracts.dataflow_errors import DataflowError
        try:
            value = json.loads(origin)
            valid = (type(value) is dict and set(value) == {'owner', 'generation', 'incarnation', 'replayable', 'result_sha256'}
                     and type(value['owner']) is str and len(value['owner']) == 64
                     and all(c in '0123456789abcdef' for c in value['owner'])
                     and type(value['incarnation']) is str and len(value['incarnation']) == 32
                     and all(c in '0123456789abcdef' for c in value['incarnation'])
                     and type(value['generation']) is int and 1 <= value['generation'] < 2**63
                     and type(value['replayable']) is bool
                     and value['result_sha256'] == hashlib.sha256(encode(result, self.store.max_value_bytes)).hexdigest())
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise DataflowError('CORRUPT_ARTIFACT', 'Invalid cache creator provenance')
        replay = (value['owner'] == self._cache_owner
                  and value['incarnation'] != self._cache_incarnation
                  and value['generation'] > self._cache_restored_generation)
        if replay and not value['replayable']:
            raise DataflowError('INCOMPATIBLE_HISTORY',
                'Cache replacement discarded the rejected predicate input needed for state replay')
        return replay

    async def _hook(self, name):
        hook = getattr(_state_owner(self.fn), name, None)
        if hook is not None:
            result = hook()
            if inspect.isawaitable(result):
                await result

    async def astart(self):
        await self._hook('astart')

    async def astop(self):
        await self._hook('astop')

    async def aclose(self):
        await self._hook('aclose')

    def validate_runtime_state(self, concurrency=1):
        from ..execution.recovery_contract import RecoveryContractError
        from ..execution.kernels.task_scope import _resident_slots
        restriction = None
        functions, mutable = set(), set()
        cells, owned_cells = {}, set()
        for value in (self.fn, self.when, self.cache_when):
            owner, mode, reason = _state_policy(value)
            if mode == 'unsupported':
                raise RecoveryContractError('Cached callable requires snapshot_state/restore_state: ' + str(reason))
            if mode == 'auto_json' and vars(owner) and concurrency != 1:
                raise RecoveryContractError('auto_json state requires concurrency 1')
            if mode == 'nonrecoverable':
                restriction = reason
            if mode in ('none', 'function_slots_v1') and inspect.isfunction(owner) and id(value) not in functions:
                functions.add(id(value))
                for cell in owner.__closure__ or ():
                    cells.setdefault(id(cell), set()).add(id(value))
                if mode == 'none':
                    continue
                slots, bindings, _reason = _resident_slots(value)
                owned_cells.update(id(binding[1]) for binding in bindings.values() if binding[0] == 'cell')
                pending = list(slots.values())
                while pending:
                    item = pending.pop()
                    if type(item) in (list, dict):
                        if id(item) in mutable:
                            restriction = 'Cached callbacks share mutable function state; declare one state adapter'
                        mutable.add(id(item))
                        pending.extend(item.values() if type(item) is dict else item)
        if any(len(cells[cell]) > 1 for cell in owned_cells):
            restriction = 'Cached callbacks share an owned closure cell; declare one state adapter'
        return restriction

    async def snapshot_state(self):
        from ..execution.operator_policy import snapshot_auto_state
        from ..execution.kernels.task_scope import snapshot_resident_function
        result = {}
        restriction = self.validate_runtime_state()
        if restriction is not None:
            return {'fn': {'mode': 'nonrecoverable', 'reason': restriction},
                    'when': None, 'cache_when': None}
        for name in ('fn', 'when', 'cache_when'):
            value = getattr(self, name)
            owner, mode, reason = _state_policy(value)
            if mode == 'none':
                result[name] = None
            elif mode == 'function_slots_v1':
                result[name] = snapshot_resident_function(value, max_bytes=self.max_input_bytes)
            elif mode == 'nonrecoverable':
                result[name] = {'mode': mode, 'reason': reason}
            elif mode == 'hooks':
                state = owner.snapshot_state()
                if inspect.isawaitable(state):
                    state = await state
                result[name] = {'mode': 'hooks', 'value': state}
            elif mode == 'auto_json':
                result[name] = {'mode': 'auto_json', 'value': snapshot_auto_state(owner,
                    max_bytes=self.max_input_bytes)}
        if hasattr(self, '_cache_owner'):
            result['cache_frontier'] = {'owner': self._cache_owner,
                                        'generation': self._cache_generation}
            # All callbacks have drained at the native cut. Work admitted while
            # that cut publishes belongs to the next generation even before ACK.
            self._cache_generation += 1
        return result

    async def restore_state(self, state):
        from ..execution.operator_policy import restore_auto_state
        from ..execution.recovery_contract import RecoveryContractError
        from ..execution.contracts.dataflow_errors import DataflowError
        from ..execution.kernels.task_scope import restore_resident_function
        if set(state) - {'cache_frontier'} != {'fn', 'when', 'cache_when'}:
            raise RecoveryContractError('Cached callable state ownership differs')
        for name in ('fn', 'when', 'cache_when'):
            _owner, mode, reason = _state_policy(getattr(self, name))
            image = state[name]
            if mode == 'nonrecoverable' or isinstance(image, dict) and image.get('mode') == 'nonrecoverable':
                raise DataflowError('INCOMPATIBLE_HISTORY',
                    'Cached callable is attempt_only: ' + str(reason or image['reason']))
            if (image is None and mode != 'none'
                    or image is not None and (type(image) is not dict or image.get('mode') != mode)):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Cached callable state adapter differs')
        frontier = state.get('cache_frontier')
        if hasattr(self, '_cache_owner'):
            if frontier is None:
                raise DataflowError('INCOMPATIBLE_HISTORY',
                    'Stateful cache history has no authenticated cache frontier')
            if (type(frontier) is not dict or set(frontier) != {'owner', 'generation'}
                    or frontier['owner'] != self._cache_owner
                    or type(frontier['generation']) is not int
                    or not 1 <= frontier['generation'] < 2**63 - 1):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Cached callable frontier differs')
            self._cache_restored_generation = frontier['generation']
            self._cache_generation = frontier['generation'] + 1
        elif frontier is not None:
            raise RecoveryContractError('Cached callable state ownership differs')
        for name in ('fn', 'when', 'cache_when'):
            image = state[name]
            if image is None:
                continue
            value = getattr(self, name)
            owner = _state_owner(value)
            if image['mode'] == 'function_slots_v1':
                restore_resident_function(value, image, max_bytes=self.max_input_bytes)
            elif image['mode'] == 'auto_json':
                restore_auto_state(owner, image['value'], max_bytes=self.max_input_bytes)
            elif image['mode'] == 'hooks':
                restored = owner.restore_state(image['value'])
                if inspect.isawaitable(restored):
                    await restored
            else:
                raise RecoveryContractError('Unknown cached callable state mode')

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
        result = self.store.get(key, _with_origin=True)
        if result['hit'] and hasattr(self, '_cache_owner') and self._requires_state_replay(result.get('origin'), result['value']):
            return {'hit': False, 'value': result['value'], 'state_replay': True}
        if result['hit'] and selected(self.cache_when, result['value']):
            return result
        if result['hit'] and result.get('origin') is not None:
            # Refuse before invoking the business callable: replacing another
            # owner's evidence would make that owner's later resume unsafe.
            origin = json.loads(result['origin'])
            if type(origin) is not dict or origin.get('owner') != getattr(self, '_cache_owner', None):
                from ..execution.contracts.dataflow_errors import DataflowError
                raise DataflowError('INCOMPATIBLE_HISTORY',
                    'Replacing another state owner cache result requires a new version or namespace')
        return {'hit': False, 'value': None, 'rejected_hit': result['hit']}

    def save(self, key, result, prior):
        admitted = selected(self.cache_when, result)
        replay = prior.get('state_replay', False)
        if replay:
            from ..execution.contracts.dataflow_errors import DataflowError
            if not admitted or encode(result, self.store.max_value_bytes) != encode(prior['value'], self.store.max_value_bytes):
                raise DataflowError('INCOMPATIBLE_HISTORY',
                    'State replay differs from the completed shared cache result')
        if admitted:
            origin = self._cache_origin(result, replayable=not prior.get('rejected_hit', False)) if hasattr(self, '_cache_owner') else None
            self.store.put_many([(key, result)], _origin=origin, _origin_only=replay)

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
                self.save(key, result, cached)
                return self.attach(row, result)
            except Exception as exc:
                if not cached.get('state_replay'):
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
                await self._io(self.save, key, result, cached)
                return self.attach(row, result)
            except Exception as exc:
                if not cached.get('state_replay'):
                    await self._io(self.error, key, exc)
                raise
