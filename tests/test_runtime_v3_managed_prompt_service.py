"""Real native Prompt/service/cache execution; only model HTTP is a fixture."""
import json
import sqlite3
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from runtime_v3_managed_http_scenarios import managed_fixture, events, assert_reaped
from test_map_prompt_async import PACK


def fixture(root, monkeypatch, request, *, mode='success'):
    value = managed_fixture(root, monkeypatch, request=request, mode=mode)
    return prompt_transport(value, monkeypatch)


def prompt_transport(value, monkeypatch):
    script = value['script'].read_text()
    old = "{'model':'fixture','data':[\n                {'index':i,'embedding':gpu_vector if mode.startswith('cuda') else [3,4,0]} for i in range(len(body['input']))]}"
    assert old in script
    value['script'].write_text(script.replace(old,
        "{'choices':[{'message':{'content':json.dumps({'result': ('cuda:'+str(gpu_vector)) if mode.startswith('cuda') else 'ok'})},'finish_reason':'stop'}]}"))
    value['pack'] = parse_prompt_pack(PACK.replace('mock-model', 'fixture').replace(
        'base_url_env: TEST_PROMPT_URL', 'base_url: '+value['service'].base_url))
    monkeypatch.setenv('TEST_PROMPT_KEY', 'test')
    return value


def dataset(value, root, *, cached=True, maximum=None):
    options = {'trust_env': False, 'timeout_s': 5, 'max_response_bytes': 65536,
               'max_request_bytes': 65536, 'max_event_bytes': 65536}
    if cached:
        options['sqlite_journal'] = {'path': str(root/'cache.sqlite')}
    return data.from_items([{'item': i} for i in range(8)]).map_prompt_async(
        'enrich', config=value['pack'], inputs={'payload': 'item'}, output='answer',
        service=value['service'], concurrency=4, options=options, max_requests=maximum,
        error_output='error', call_output='call')


@pytest.mark.parametrize('cached', [False, True])
def test_prompt_service_commits_original_results_and_reuses_cache(tmp_path, monkeypatch, request, cached):
    value = fixture(tmp_path, monkeypatch, request)
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        saved = dataset(value, tmp_path, cached=cached).materialize()
        rows = saved.take_all()
        assert [row['answer'] for row in rows] == ['ok']*8
        assert saved.take_all() == rows
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert_reaped(value, requests=8)
    if cached:
        with sqlite3.connect(tmp_path/'cache.sqlite') as db:
            before = db.execute('SELECT request_key,request_json,response_json FROM calls ORDER BY request_key').fetchall()
        assert len(before) == 8 and all('native_occurrence' not in json.loads(row[1]) for row in before)
        value['script'].unlink()  # A cache-only execution must never start it.
        with RunContext.start(tmp_path/'replay'), data.local_execution():
            rows = dataset(value, tmp_path, maximum=0).materialize().take_all()
            assert [row['answer'] for row in rows] == ['ok']*8
        assert_reaped(value, requests=8)
        with sqlite3.connect(tmp_path/'cache.sqlite') as db:
            assert db.execute('SELECT request_key,request_json,response_json FROM calls ORDER BY request_key').fetchall() == before


def test_zero_prompt_budget_starts_no_service(tmp_path, monkeypatch, request):
    value = fixture(tmp_path, monkeypatch, request)
    with RunContext.start(tmp_path/'run'), data.local_execution():
        rows = dataset(value, tmp_path, maximum=0).take_all()
    assert len(rows) == 8 and all(row['error']['type'] == 'PromptBudgetExceededError' for row in rows)
    assert events(value) == []


def test_service_facts_keep_native_identity_and_private_environment_in_program(tmp_path, monkeypatch, request):
    from demiflow.operator_llm.flow_contract import prompt_facts
    from demiflow.execution.operator_bindings.dataflow_model_factory import native_model_declaration
    from demiflow.planning.dataflow_models import service_kernel
    from demiflow.planning.dataflow_models import build_model_factory
    from demiflow.execution.contracts.dataflow import thaw_record_value
    from demiflow.execution.scheduler.native_limits import resolve_runtime_policy
    from demiflow.forge.pipeline_policy import DEFAULT_LIMITS
    value = fixture(tmp_path, monkeypatch, request)
    value['service'].env['FIXTURE_PRIVATE_TOKEN'] = 'fixture-secret-only'
    op = dataset(value, tmp_path)._plan.operations[-1]
    actor = op.callable.target.__self__
    facts = prompt_facts(actor)
    from copy import copy
    plain = copy(actor); plain._runtime = copy(actor._runtime)
    plain._runtime._service = None
    del plain._sealed_service_declaration
    assert prompt_facts(plain) == facts
    factory = native_model_declaration(op)
    native_op, settings = factory.constructor_args
    _, bounds = resolve_runtime_policy({}, DEFAULT_LIMITS, provider='local')
    kernel = service_kernel(build_model_factory(native_op, program_ref=None, bounds=bounds), settings)
    assert 'fixture-secret-only' not in json.dumps(thaw_record_value(kernel.configuration))
    assert kernel.configuration['managed_http_service_digest'] == actor._sealed_service_declaration
    assert settings['service']['parameters']['env']['FIXTURE_PRIVATE_TOKEN'] == 'fixture-secret-only'


@pytest.mark.parametrize('mutation', ['environment', 'removed', 'owner', 'method'])
def test_prompt_service_mutations_are_rejected_before_start(tmp_path, monkeypatch, request, mutation):
    from demiflow.execution.operator_bindings.dataflow_model_factory import native_model_declaration
    value = fixture(tmp_path, monkeypatch, request)
    op = dataset(value, tmp_path)._plan.operations[-1]
    actor = op.callable.target.__self__
    if mutation == 'environment': value['service'].env['CHANGED'] = 'yes'
    elif mutation == 'removed': actor._runtime._service = None
    elif mutation == 'owner': actor._runtime._service_owner = object()
    else: value['service'].bind = lambda model: None
    with pytest.raises((ValueError, DataflowError), match='differ|removed|modified'):
        native_model_declaration(op)
    assert events(value) == []


def test_prompt_cancel_reaps_original_service_and_leaves_no_success(tmp_path, monkeypatch, request):
    value = fixture(tmp_path, monkeypatch, request, mode='slow')
    seen, errors = threading.Event(), []
    with pytest.raises(Exception, match='[Cc]ancel'), RunContext.start(
            tmp_path/'run', limits={'timeout_s': 30}) as run, data.local_execution():
        def cancel():
            try:
                deadline = time.monotonic()+20
                while time.monotonic() < deadline:
                    if any(row['event'] == 'request' for row in events(value)):
                        seen.set(); RunContext.cancel(run.ref); return
                    time.sleep(.01)
                raise AssertionError('Actual Prompt HTTP request did not start')
            except BaseException as exc:
                errors.append(repr(exc))
        thread = threading.Thread(target=cancel); thread.start()
        try:
            dataset(value, tmp_path, cached=False).materialize()
        finally:
            thread.join(22)
    assert not thread.is_alive() and seen.is_set() and not errors
    assert run.journal.execution_action('action-1')['result'] is None
    assert_reaped(value, requests=sum(r['event']=='request' for r in events(value)))


from test_runtime_v3_managed_service_gpu import gpu_service
from demiflow.execution.contracts.dataflow_errors import DataflowError


@pytest.mark.parametrize('gpu_service', ['cancel'], indirect=True)
def test_prompt_service_runs_on_its_actual_original_gpu_grant(tmp_path, monkeypatch, gpu_service):
    import os
    import subprocess
    from pathlib import Path
    from demiflow.execution.hosts.devices import device_pool
    from test_runtime_v3_managed_service_gpu import _released
    value = prompt_transport(gpu_service, monkeypatch)
    observed, errors, done = [], [], threading.Event()
    limits = {'memory_bytes': 4*1024**3, 'max_rss_bytes': 2*1024**3, 'timeout_s': 60}

    def observe():
        try:
            deadline = time.monotonic()+40
            while time.monotonic() < deadline and not done.is_set():
                entries = events(value)
                if any(row['event']=='request' for row in entries):
                    cuda = next(row for row in entries if row['event']=='cuda')
                    assert cuda['values']==[3.,4.,0.] and cuda['allocated'] > 0
                    snapshot = device_pool('local').snapshot()
                    assert len(snapshot['active']) == 1
                    active = snapshot['active'][0]
                    assert [d['device_id'] for d in active['devices']] == [value['expected_uuid']]
                    record = json.loads((Path(os.environ['DEMIFLOW_RESOURCE_ROOT'])/'capacity/allocations'/
                        (active['allocation_id']+'.json')).read_text())
                    assert record['effective']['memory_bytes'] == 2*1024**3
                    nvml = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,gpu_uuid',
                        '--format=csv,noheader,nounits'], text=True)
                    assert any(line.split(',')[0].strip()==str(cuda['pid']) and
                        line.split(',')[1].strip()==value['expected_uuid'] for line in nvml.splitlines())
                    observed.append({'cuda': cuda, 'pool': snapshot, 'memory': record, 'nvml': nvml})
                    return
                time.sleep(.01)
            raise AssertionError('Actual Prompt GPU service request was not observed')
        except BaseException as exc:
            errors.append(repr(exc))

    with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(
            workers=4, memory_bytes=4*1024**3, native_resources={'max_rss_bytes': 2*1024**3}):
        thread = threading.Thread(target=observe); thread.start()
        try:
            saved = dataset(value, tmp_path, cached=False).materialize()
            assert [row['answer'] for row in saved.take_all()] == ['cuda:[3.0, 4.0, 0.0]']*8
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        finally:
            done.set(); thread.join(42)
    assert observed and not errors and not thread.is_alive()
    released = _released(value)
    (tmp_path/'prompt-gpu-proof.json').write_text(json.dumps({'observed': observed, 'released': released}, indent=2))
