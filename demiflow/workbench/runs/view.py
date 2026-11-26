"""Workbench web read projections of native run facts; no reconciliation on GET."""
from contextlib import ExitStack, contextmanager
import base64
import json

from demiflow.execution.host.inspection import OBSERVATION_COUNT, RunReadError, read_run, series as telemetry_series
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.runs import manager as runs
from demiflow.forge.runs.context import context_facts, control_state, snapshot
from demiflow.forge.schema import ForgeError


class ObservationResetRequired(ForgeError):
    def __init__(self, reason):
        super().__init__('Observation page must be reset: ' + reason, code='conflict')
        self.reset_required = True
        self.reason = reason


@contextmanager
def _reading(root, run_id, *, deadline=None, cursor=None):
    with ExitStack() as stack:
        try:
            target = runs.directory(root, run_id)
            reader = stack.enter_context(read_run(target, deadline=deadline))
        except (OSError, ForgeError, RunReadError) as exc:
            if cursor:
                raise ObservationResetRequired('history_unavailable') from exc
            if isinstance(exc, RunReadError):
                raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
            raise
        try:
            yield target, reader
        except RunReadError as exc:
            raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc


def observations(root, run_id, *, attempt, node_id=None, action_id=None, cursor=None, limit=32):
    """One 96KiB occurrence page, pinned to an explicit observation revision."""
    if type(limit) is not int or not 1 <= limit <= 64 or type(attempt) is not int:
        raise ForgeError('Invalid observation page', code='invalid_request')
    for value in (node_id, action_id):
        if value is not None and (type(value) is not str or not 1 <= len(value) <= 64 or any(ord(c) < 32 for c in value)):
            raise ForgeError('Invalid observation selector', code='invalid_request')
    decoded = None
    after = None
    if cursor:
        if type(cursor) is not str or len(cursor) > 2048:
            raise ForgeError('Observation cursor exceeds budget', code='invalid_request')
        try:
            decoded = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
            if type(decoded) is not dict or set(decoded) != {'v', 'binding', 'observation_revision', 'after'} or decoded['v'] != 1:
                raise ValueError('Invalid cursor format')
            after = decoded['after']
            if (type(after) is not list or len(after) != 3 or type(after[0]) is not str
                    or type(after[1]) is not str or type(after[2]) is not int
                    or not 1 <= len(after[0]) <= 64 or not 1 <= len(after[1]) <= 64 or not 0 <= after[2] < OBSERVATION_COUNT):
                raise ValueError('Invalid position')
        except (ValueError, KeyError, TypeError, RecursionError) as exc:
            raise ObservationResetRequired('cursor_invalid') from exc
    with _reading(root, run_id, cursor=cursor) as (target, reader):
        identity = reader.manifest_identity()
        if (identity['root'] != str(runs.Path(root).absolute()) or identity['run_id'] != run_id
                or type(identity['digest']) is not str or len(identity['digest']) != 64):
            if cursor:
                raise ObservationResetRequired('sealed_identity_changed')
            raise ForgeError('Run observation seal belongs to a different project', code='conflict')
        _, control = runs.ws.load_workspace_control(root)
        binding = dict(workspace_id=runs.ws.workspace_id(root, control),
                       project_root_digest=content_digest(str(runs.Path(root).absolute())),
                       run_id=run_id, attempt=attempt, seal_digest=identity['digest'],
                       node_id=node_id, action_id=action_id)
        if decoded is not None and decoded['binding'] != binding:
            raise ObservationResetRequired('cursor_binding_changed')
        if not reader.observations_supported():
            if cursor:
                raise ObservationResetRequired('historical_observation_contract')
            return dict(run_id=run_id, attempt=attempt, observations=[], availability='unsupported',
                        reason='historical_observation_contract', scope='correlated_node_observations', has_more=False)
        try:
            reader.state(attempt)
        except RunReadError as exc:
            if cursor:
                raise ObservationResetRequired('attempt_history_unavailable') from exc
            raise
        revision, count, _ = reader.observation_revision(attempt)
        if decoded is not None and decoded['observation_revision'] != revision:
            raise ObservationResetRequired('observations_changed')
        page_scope = reader.observation_scope(attempt, node_id=node_id, action_id=action_id)
        result = dict(format='demiflow_forge_node_observation_page_v1', run_id=run_id, attempt=attempt,
                      node_id=node_id, action_id=action_id, binding=binding, observation_revision=revision,
                      observations=[], availability='available', scope='correlated_node_observations',
                      cursor=None, has_more=False, coverage='one_page_at_observation_revision',
                      attempt_observation_count=count)
        if page_scope is not None:
            result.update(page_scope)
        rows = result['observations']
        used = len(bounded_json(result, 96 * 1024)) + 2064
        last = after
        for row in reader.observation_rows(attempt, node_id=node_id, action_id=action_id, after=after, limit=limit + 1):
            size = len(bounded_json(row, 16384)) + 1
            if len(rows) == limit or used + size > 96 * 1024:
                result['has_more'] = True
                break
            rows.append(row)
            used += size
            last = [row['action_id'], row['node_id'], row['occurrence']]
        if last:
            encoded = bounded_json(dict(v=1, binding=binding, observation_revision=revision, after=last), 1536)
            result['cursor'] = base64.urlsafe_b64encode(encoded).decode()
            if len(result['cursor']) > 2048:
                raise ForgeError('Observation cursor exceeds encoding budget', code='resource_limit')
        bounded_json(result, 96 * 1024)
        return result


def events(root, run_id, *, attempt=None, cursor=None, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForgeError('Invalid control event page size', code='invalid_request')
    with _reading(root, run_id) as (target, reader):
        state, _ = reader.state(attempt)
        attempt = state.get('attempt')
        high = reader.control_sequence(attempt)
        if high is None:
            return dict(events=[], availability='unsupported')
        after = 0
        if cursor:
            parts = cursor.split(':')
            if (len(parts) != 3 or parts[:2] != [run_id, str(attempt)]
                    or not parts[2].isdigit() or len(parts[2]) > 20):
                raise ForgeError('Control cursor belongs to a different run or attempt', code='conflict')
            after = int(parts[2])
        if after > high:
            raise ForgeError('Control cursor is outside retained history', code='conflict')
        rows = []
        size = 0
        for sequence, raw in reader.control_events(attempt, after=after, limit=limit):
            size += len(raw.encode())
            if size > 120 * 1024:
                break
            rows.append(dict(sequence=sequence, **json.loads(raw)))
        last = rows[-1]['sequence'] if rows else after
        return dict(
            run_id=run_id, attempt=attempt, events=rows,
            cursor=f'{run_id}:{attempt}:{last}', has_more=last < high,
            gap=False, scope='control_events_only',
        )


def metrics(root, run_id, *, attempt=None, points=600, resolution='raw'):
    with _reading(root, run_id) as (target, reader):
        state, _ = reader.state(attempt)
    return telemetry_series(target, attempt=state.get('attempt'), points=points, resolution=resolution)
