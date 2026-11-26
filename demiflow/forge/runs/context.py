"""Pure, bounded operational context and read-only control/snapshot facts for Forge runs."""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from pathlib import Path
import re
import time

from demiflow.execution.host.inspection import RunReadError, latest as telemetry_latest, read_run
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.runs.authority import LocalAuthority
from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_workbench_run_context_v1'
MAX_BYTES = 65536
MAX_READS = 8
MAX_REFERENCES = 32
MAX_SECONDS = 10


@contextmanager
def _reading(root, run_id, *, deadline=None):
    from demiflow.forge.runs import manager as runs
    with ExitStack() as stack:
        try:
            target = runs.directory(root, run_id)
            reader = stack.enter_context(read_run(target, deadline=deadline))
        except RunReadError as exc:
            raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
        try:
            yield target, reader
        except RunReadError as exc:
            raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc


def context_facts(root, run_id, *, attempt=None, deadline=None):
    """A <=4 KiB projection of native facts. No reconciliation or code execution."""
    if attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise ForgeError('Invalid attempt', code='invalid_request')
    with _reading(root, run_id, deadline=deadline) as (_, reader):
        identity = reader.manifest_identity()
        if (identity['root'] != str(Path(root).absolute()) or identity['run_id'] != run_id
                or type(identity['digest']) is not str
                or not re.fullmatch(r'[0-9a-f]{64}', identity['digest'])
                or type(identity['format']) is not str):
            raise ForgeError('Run context identity differs', code='conflict')
        selected, current = reader.state_projection(attempt)
        result = dict(run_id=run_id, attempt=selected['attempt'], current_attempt=current['attempt'],
                      historical=selected['attempt'] != current['attempt'], seal_digest=identity['digest'],
                      source_format=identity['format'], state=selected)
        bounded_json(result, 4096)
        return result


def control_state(root, run_id):
    from demiflow.forge.runs import manager as runs
    with _reading(root, run_id) as (target, reader):
        summary = reader.runtime_summary()
        if summary is None:
            return runs._load(root, run_id, detail='identity')[0:2]
        if summary['root'] != str(Path(root).absolute()) or summary['run_id'] != run_id:
            raise ForgeError('Run projection identity differs')
        state, _ = reader.state()
        return target, {'state': state, 'managed': True}


def snapshot(root, run_id, *, attempt=None, revision=None):
    from demiflow.forge.runs import manager as runs
    with _reading(root, run_id) as (target, reader):
        summary = reader.runtime_summary()
        if summary is None:
            return dict(
                run_id=run_id,
                availability='unsupported',
                reason='historical_runtime_contract',
            )
        if summary['root'] != str(Path(root).absolute()) or summary['run_id'] != run_id:
            raise ForgeError('Run projection identity differs')
        state, current = reader.state(attempt)
        attempt = state['attempt']
        action_sequence = reader.action_sequence(attempt)
        observation_revision, observation_count, node_sequence = reader.observation_revision(attempt)
        observations = reader.observation_summaries(attempt)
        outputs = []
        for name, spec in summary['outputs'].items():
            projection = reader.output_projection(attempt, spec['action_id'])
            retained = projection['retained_from_attempt']
            status = ('committed' if projection['committed'] else
                      ('writing' if state['status'] in runs.ACTIVE and not projection['failed'] else 'unknown')
                      if projection['has_intent'] else 'not_started')
            training = reader.training_outputs(retained or attempt, spec['action_id'])
            ref = projection['result_ref']
            outputs.append(
                dict(
                    name=name,
                    **spec,
                    status=status,
                    **({'training': training} if training else {}),
                    retained_from_attempt=retained,
                    result=({'ref': ref, 'location_id': spec['location_id']} if ref else None),
                    result_digest=content_digest(ref) if ref else None,
                )
            )
    telemetry = telemetry_latest(target, attempt)
    alive = (
        runs.process_matches(state.get('supervisor'))
        if attempt == current and state['status'] in runs.ACTIVE
        else None
    )
    sample = telemetry.get('sample') or {}
    current_revision = content_digest(
        dict(
            state=state,
            actions=action_sequence,
            nodes=observation_revision,
            training=[output['training'] for output in outputs if 'training' in output],
            telemetry=[sample.get('epoch'), sample.get('sequence')],
            supervisor_alive=alive,
        )
    )
    freshness = dict(telemetry_age_s=telemetry.get('age_s'), supervisor_alive=alive)
    if revision == current_revision:
        return dict(
            run_id=run_id,
            attempt=attempt,
            revision=current_revision,
            unchanged=True,
            freshness=freshness,
        )
    result = dict(
        format='demiflow_forge_runtime_v1',
        availability='available',
        run_id=run_id,
        attempt=attempt,
        current_attempt=current,
        revision=current_revision,
        state=state,
        status=state['status'],
        entry=summary['entry'],
        digest=summary['digest'],
        limits=summary['limits'],
        outputs=outputs,
        observations=observations[:64],
        observations_truncated=len(observations) > 64,
        telemetry=telemetry,
        supervisor_alive=alive,
        freshness=freshness,
        action_sequence=action_sequence,
        node_sequence=node_sequence,
        observation_revision=observation_revision,
        observation_count=observation_count,
    )
    bounded_json(result, 128 * 1024)
    return result


def query(root, run_id, *, attempt=None, logs=False, offset=0, max_bytes=MAX_BYTES, authority=None):
    if (type(logs) is not bool or type(offset) is not int or not 0 <= offset < 2**64
            or type(max_bytes) is not int or not 4096 <= max_bytes <= MAX_BYTES):
        raise ForgeError('Invalid context budget or log selector', code='invalid_request')
    from demiflow.forge import operation_budget
    from demiflow.forge.bridge import run_tools as tools
    from demiflow.forge.runs import manager as run_manager
    root = Path(root).absolute()
    authority = authority if authority is not None else LocalAuthority(str(root))
    deadline = time.monotonic() + MAX_SECONDS
    response = dict(format=VERSION, owner='project_run', observed_at=time.time(), atomic_snapshot=False,
                    facts=[], references=[], diagnostics=[], coverage=['run_identity', 'run_state'],
                    excluded=['engineering_document_delivery', 'notebook', 'sql', 'case', 'development_receipts'],
                    budget=dict(bytes=max_bytes, reads=MAX_READS, references=MAX_REFERENCES, seconds=MAX_SECONDS),
                    reads=0, complete=False)

    def check():
        if time.monotonic() >= deadline or response['reads'] >= MAX_READS:
            raise ForgeError('Context deadline/read budget exceeded', code='resource_limit')
        operation_budget.check()
        authority.require(root, 'read')

    with operation_budget.scope(deadline=deadline):
        check()
        facts = context_facts(root, run_id, attempt=attempt, deadline=deadline)
        response['reads'] += 1
        ref = dict(owner='pipeline_run', kind='run_attempt', run_id=run_id, attempt=facts['attempt'],
                   version=facts['state']['revision'], seal_digest=facts['seal_digest'],
                   source_format=facts['source_format'], observed_at=time.time(), historical=facts['historical'])
        response['references'].append(ref)
        response['facts'].append(dict(source=0, value=facts))
        descriptions = tools.describe()
        if len(bounded_json(descriptions, 24576)) + len(bounded_json(response, MAX_BYTES)) + 1024 <= max_bytes:
            response['tools'] = descriptions
            response['coverage'].append('run_tools')
        else:
            response['diagnostics'].append(dict(section='run_tools', reason='requires_separate_read', operation='run tools'))
        if logs:
            check()
            remaining = max_bytes - len(bounded_json(response, MAX_BYTES)) - 2048
            if remaining < 6:
                response['diagnostics'].append(dict(section='logs', reason='budget_exhausted', next_offset=offset))
            else:
                page = run_manager.logs(root, run_id, attempt=facts['attempt'], offset=offset,
                                        limit=min(4096, remaining // 6))
                response['reads'] += 1
                response['references'].append(dict(ref, kind='run_log_page', observed_at=time.time(), offset=offset,
                                                    next_offset=page['next_offset']))
                response['facts'].append(dict(source=1, value=page))
                response['coverage'].append('run_log_page')
        response['complete'] = not response['diagnostics']
        bounded_json(response, max_bytes)
        return response
