"""Inert tuning policy and frozen execution evidence; no GPU runtime imports."""
import json
import math
import os
from pathlib import Path

from .deployment import canonical, deployment_config, fingerprint, service_directory, deployment_id

SCHEMA = 'demiflow_execution_tuning_v1'
POLICY = 'independent-native-exact-v1'
PROFILES = ('pplx', 'amos', 'd3', 'autotrust-jev')
MAX_REPORT = 1024 * 1024


def recipe(config):
    """Backend capability declaration, independent of model names/business data."""
    profile = config['profile']
    if profile not in PROFILES or config.get('replicas', 1) != 1 or 'batch_evidence' in config:
        raise ValueError('No exact independent-request tuning recipe for this backend')
    if 'cuda_graphs' not in config['options']:
        raise ValueError('Tuning requires performance: auto or optimized')
    # AMOS 3/4-stream native execution has reproduced illegal GPU accesses.
    # A lucky finite benchmark cannot rehabilitate a known unsafe recipe.
    return dict(profile=profile, streams=(1, 2) if profile == 'amos' else (1, 2, 3, 4),
                cpu_threads=profile != 'autotrust-jev', quality_policy=POLICY)


def limits(value=None):
    defaults = dict(max_candidates=13, timeout_s=1800, rounds=2, warmup_rounds=2,
                    request_timeout_s=120, max_p95_ms=2000, objective='throughput')
    if not isinstance(value or {}, dict) or set(value or {}) - defaults.keys():
        raise ValueError('Unknown tuning limits')
    result = {**defaults, **(value or {})}
    for key, low, high in [('max_candidates', 1, 16), ('timeout_s', 30, 7200),
                           ('rounds', 2, 5), ('warmup_rounds', 1, 5),
                           ('request_timeout_s', 1, 600), ('max_p95_ms', 1, 600000)]:
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise ValueError(f'tuning.{key} must be an integer in {low}..{high}')
    if result['objective'] not in {'throughput', 'latency'}:
        raise ValueError('Tuning objective must be throughput or latency')
    return result


def environment_identity():
    return {key: value for key, value in os.environ.items()
            if key.startswith(('VLLM_', 'STARTLUX_', 'TORCH_', 'NVIDIA_', 'CUBLAS_',
                               'CUBLASLT_', 'FLASHINFER_', 'CUDA_', 'TRITON_', 'FLA_'))
            or key in {'OMP_NUM_THREADS', 'MKL_NUM_THREADS'}}


def hardware_identity(gpus):
    from .preparation_io import run_bounded
    raw = run_bounded(['nvidia-smi', '--id=' + ','.join(map(str, gpus)),
        '--query-gpu=uuid,name,driver_version,memory.total', '--format=csv,noheader,nounits'],
        timeout_s=10, output_bytes=16384)
    devices = []
    for line in raw.decode().splitlines():
        uuid, name, driver, total = [part.strip() for part in line.split(',')]
        devices.append(dict(uuid=uuid, name=name, driver=driver, memory_mib=int(total)))
    if len(devices) != len(gpus): raise ValueError('Incomplete tuning hardware identity')
    return devices


def context(manifest, config, hardware):
    return dict(model_contract=manifest['model_contract'], weights_signature=manifest['weights_signature'],
                adapter_version=manifest['adapter_version'], runtime=manifest['runtime'],
                declaration=fingerprint(config), hardware=hardware,
                cpu=dict(count=os.cpu_count(), affinity=sorted(os.sched_getaffinity(0))
                         if hasattr(os,'sched_getaffinity') else None),
                environment=fingerprint(environment_identity()), quality_policy=POLICY)


def execution_config(base, execution):
    """Validate execution-only overrides without changing the logical contract."""
    caps = recipe(base)
    fields = {'mode', 'streams', 'request_concurrency', 'cpu_threads', 'cache_memory_mib',
              'memory_limit_mib', 'max_entries', 'min_observations'}
    if not isinstance(execution, dict) or set(execution) != fields:
        raise ValueError('Invalid frozen execution fields')
    if execution['mode'] not in {'native', 'graphs'}:
        raise ValueError('Unknown execution mode')
    if execution['streams'] not in caps['streams']:
        raise ValueError('Execution width is not admitted by the backend tuning recipe')
    graph = base['options']['cuda_graphs']
    for key, low, high in [
        ('streams', 1, 4), ('request_concurrency', 1, base['request_concurrency']),
        ('cpu_threads', 1, base['options'].get('cpu_threads', 8)),
        ('cache_memory_mib', 64, graph['cache_memory_mib']),
        ('memory_limit_mib', 1024, graph['memory_limit_mib']),
        ('max_entries', 1, graph['max_entries']), ('min_observations', 1, 100)]:
        if type(execution[key]) is not int or not low <= execution[key] <= high:
            raise ValueError('Execution override exceeds declared budget: ' + key)
    if not caps['cpu_threads'] and execution['cpu_threads'] != 8:
        raise ValueError('JEV uses its fixed validated runtime thread policy')
    if execution['mode'] == 'native' and (execution['streams'] != 1 or execution['request_concurrency'] != 1):
        raise ValueError('Independent native reference must be serial')
    result = json.loads(canonical(base))
    result['performance'] = 'optimized'
    result['request_concurrency'] = execution['request_concurrency']
    if caps['cpu_threads']: result['options']['cpu_threads'] = execution['cpu_threads']
    result['options']['cuda_graphs'].update({key: execution[key] for key in
        ('streams', 'cache_memory_mib', 'memory_limit_mib', 'max_entries', 'min_observations')})
    return deployment_config(result)


def candidates(config, hardware, budget):
    caps = recipe(config); graph = config['options']['cuda_graphs']
    base = dict(mode='graphs', streams=1, request_concurrency=1,
                cpu_threads=config['options'].get('cpu_threads', 8),
                cache_memory_mib=graph['cache_memory_mib'],
                memory_limit_mib=min(graph['memory_limit_mib'], min(d['memory_mib'] for d in hardware)-2048),
                max_entries=graph['max_entries'], min_observations=graph['min_observations'])
    values = [dict(base, mode='native')]
    # Test useful execution widths first; request pressure and secondary policy
    # variants stay finite and never increase the configured total budgets.
    for streams in caps['streams']:
        if streams <= config['request_concurrency']:
            values.append(dict(base, streams=streams, request_concurrency=min(2*streams, config['request_concurrency'])))
    for streams in caps['streams']:
        if streams <= config['request_concurrency']:
            values.append(dict(base, streams=streams, request_concurrency=streams))
    common = dict(base, streams=min(2,config['request_concurrency']),
                  request_concurrency=min(4,config['request_concurrency']))
    if caps['cpu_threads']:
        for threads in (2, 4):
            if threads <= base['cpu_threads']: values.append(dict(common, cpu_threads=threads))
    values.extend([dict(common, min_observations=2),
                   dict(common, cache_memory_mib=max(64,base['cache_memory_mib']//2),
                        max_entries=max(1,base['max_entries']//2))])
    result = []; seen = set()
    for value in values:
        key = fingerprint(value)
        if key not in seen:
            execution_config(config, value); result.append(value); seen.add(key)
        if len(result) == budget['max_candidates']: break
    return result


def finite_positive(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def digest(value):
    return isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)


def check_execution(status, execution):
    """One quality/resource gate for live trials and persisted evidence."""
    if (not isinstance(status,dict) or status.get('failed')
            or type(status.get('mismatches')) is not int or status['mismatches']!=0
            or type(status.get('streams')) is not int or status['streams']!=execution['streams']):
        raise ValueError('Backend failed its exact quality or execution-width gate')
    for key, maximum in [('cache_bytes',execution['cache_memory_mib']*1024**2),
                         ('allocator_peak_bytes',execution['memory_limit_mib']*1024**2),
                         ('entries',execution['max_entries'])]:
        if type(status.get(key)) is not int or not 0<=status[key]<=maximum:
            raise ValueError('Backend exceeded or omitted resource evidence: '+key)
    if type(status.get('hits')) is not int or status['hits']<0:
        raise ValueError('Missing graph replay measurements')
    if execution['mode']=='graphs' and (status.get('graph_disabled') is not False or status['hits']==0):
        raise ValueError('Candidate did not exercise a healthy CUDA graph path')
    if execution['mode']=='native' and (status.get('graph_disabled') is not True or status['hits']!=0):
        raise ValueError('Native reference unexpectedly used candidate graphs')


def score(record, budget):
    if not isinstance(record,dict) or record.get('passed') is not True: return None
    rounds = record.get('rounds', [])
    if not isinstance(rounds,list) or len(rounds) != budget['rounds']: return None
    for row in rounds:
        if (not isinstance(row,dict) or not finite_positive(row.get('rps')) or not finite_positive(row.get('p95_ms'))
                or row['p95_ms'] > budget['max_p95_ms'] or row.get('mismatches') != 0): return None
    # Conservative throughput: every measured repetition must be useful.
    rate = min(row['rps'] for row in rounds)
    tail = max(row['p95_ms'] for row in rounds)
    return rate, tail


def rank(records, budget):
    eligible = [(row, score(row, budget)) for row in records]
    eligible = [(row, metrics) for row, metrics in eligible if metrics is not None]
    if budget['objective'] == 'latency':
        eligible.sort(key=lambda pair:(pair[1][1], -pair[1][0]))
    else:
        eligible.sort(key=lambda pair:-pair[1][0])
        if eligible:
            best = eligible[0][1][0]
            # Within 2% of best throughput, prefer shorter tails/resources.
            near = [p for p in eligible if p[1][0] >= .98*best]
            rest = [p for p in eligible if p[1][0] < .98*best]
            near.sort(key=lambda pair:(pair[1][1],pair[0]['execution']['request_concurrency'],
                                       pair[0]['execution']['streams']))
            eligible = near + rest
    return [row for row, _ in eligible]


def report_path(name, config):
    return service_directory(config)/'deployments'/deployment_id(name,config)/'tuning.json'


def read_report(path):
    with Path(path).open('rb') as stream: raw = stream.read(MAX_REPORT+1)
    if len(raw) > MAX_REPORT: raise ValueError('Tuning report exceeds 1 MiB')
    return json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Non-finite tuning JSON')))


def validated_report(report, expected, config):
    if (not isinstance(report,dict) or report.get('schema_version') != SCHEMA
            or report.get('context') != expected or report.get('passed') is not True
            or report.get('quality_policy') != POLICY):
        raise ValueError('Tuning result is missing, stale or incompatible; run services tune')
    body = {key:value for key,value in report.items() if key != 'sha256'}
    if report.get('sha256') != fingerprint(body): raise ValueError('Tuning report digest differs')
    budget = limits(report.get('limits'))
    records = report.get('candidates', [])
    if not isinstance(records,list) or not 1 <= len(records) <= budget['max_candidates']:
        raise ValueError('Invalid tuning candidate coverage')
    selected = report.get('selected')
    if selected not in records or score(selected,budget) is None:
        raise ValueError('Selected execution lacks passing measurements')
    if not isinstance(selected.get('execution'),dict): raise ValueError('Missing selected execution')
    for key in ('search_count','holdout_count'):
        if type(report.get(key)) is not int or not 200 <= report[key] <= 1800:
            raise ValueError('Tuning requires distinct search and holdout cases')
    if (report['search_count']+report['holdout_count']>2000
            or not digest(report.get('cases_sha256'))
            or report.get('search_sha256') == report.get('holdout_sha256')):
        raise ValueError('Invalid tuning sample identity')
    validation = report.get('validation', {})
    if (not isinstance(validation,dict) or validation.get('execution') != selected['execution'] or validation.get('passed') is not True
            or validation.get('mismatches') != 0 or validation.get('rows') != report['holdout_count']):
        raise ValueError('Selected execution lacks independent holdout validation')
    for key in ('search_sha256','holdout_sha256'):
        if not digest(report.get(key)):
            raise ValueError('Invalid tuning sample digest')
    baseline = records[0]
    if (not isinstance(baseline,dict) or not isinstance(baseline.get('execution'),dict)
            or baseline['execution'].get('mode')!='native'):
        raise ValueError('Missing independent native reference')
    execution_config(config,baseline['execution'])
    check_execution(baseline.get('execution_status'),baseline['execution'])
    reference = baseline.get('reference',{})
    if (not isinstance(reference,dict) or reference.get('rows')!=report['search_count']+report['holdout_count']
            or not digest(reference.get('outputs_sha256'))):
        raise ValueError('Incomplete independent native reference coverage')
    for record, count, rounds in ((selected,report['search_count'],budget['rounds']),
                                   (validation,report['holdout_count'],1)):
        execution_config(config,record['execution'])
        measurements = record.get('rounds',[])
        if not isinstance(measurements,list) or len(measurements)!=rounds: raise ValueError('Incomplete tuning measurements')
        for metric in measurements:
            if (not isinstance(metric,dict) or metric.get('rows')!=count or metric.get('mismatches')!=0
                    or not finite_positive(metric.get('wall_s'))
                    or not finite_positive(metric.get('p95_ms'))
                    or metric['p95_ms']>budget['max_p95_ms']
                    or not finite_positive(metric.get('rps'))):
                raise ValueError('Tuning measurements failed quality or latency constraints')
        warmup = record.get('warmup',[])
        if (not isinstance(warmup,list) or len(warmup)!=budget['warmup_rounds']
                or any(not isinstance(row,dict) or row.get('rows')!=report['search_count'] or row.get('mismatches')!=0 for row in warmup)):
            raise ValueError('Incomplete tuning warmup quality coverage')
        check_execution(record.get('execution_status'),record['execution'])
    return execution_config(config, selected['execution'])


def resolve(name, config, manifest):
    path = report_path(name,config)
    try: report = read_report(path)
    except FileNotFoundError as exc: raise ValueError('Auto deployment requires services tune --config ... --cases ...') from exc
    expected = context(manifest,config,hardware_identity(config['gpus']))
    resolved = validated_report(report,expected,config)
    return resolved, dict(report_sha256=report['sha256'], selected=report['selected']['execution'],
                          quality_policy=POLICY, cases_sha256=report['cases_sha256'])
