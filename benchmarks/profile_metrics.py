"""Bounded, opt-in benchmark instrumentation; never imported by production."""
import contextlib
import cProfile
import functools
import json
import pstats
import threading
import time
from pathlib import Path


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.local = threading.local()
        self.reset(False)

    def reset(self, profile):
        with self.lock:
            if getattr(self, 'active', 0): raise RuntimeError('Reset requires an idle benchmark')
            self.active, self.profile = 0, profile
            self.rows, self.profiles = {}, {}

    def add(self, label, wall, cpu, own_cpu=None):
        with self.lock:
            if label not in self.rows:
                if len(self.rows) >= 128: raise RuntimeError('Metric cardinality exceeded')
                self.rows[label] = dict(calls=0, wall_s=0., cpu_s=0., own_cpu_s=0., samples=[])
            row = self.rows[label]
            row['calls'] += 1; row['wall_s'] += wall; row['cpu_s'] += cpu
            row['own_cpu_s'] += cpu if own_cpu is None else own_cpu
            if len(row['samples']) < 4096: row['samples'].append(wall)

    @contextlib.contextmanager
    def measure(self, label, profile_root=False):
        stack = getattr(self.local, 'stack', None)
        if stack is None: stack = self.local.stack = []
        started, cpu = time.perf_counter(), time.thread_time()
        frame = [0.]; stack.append(frame)
        profiler = None
        with self.lock: self.active += 1
        if profile_root and self.profile and not getattr(self.local, 'profiling', False):
            ident = threading.get_ident()
            with self.lock:
                profiler = self.profiles.setdefault(ident, cProfile.Profile(timer=time.thread_time_ns, timeunit=1e-9))
            self.local.profiling = True; profiler.enable()
        try: yield
        finally:
            if profiler is not None:
                profiler.disable(); self.local.profiling = False
            elapsed, used = time.perf_counter()-started, time.thread_time()-cpu
            stack.pop()
            if stack: stack[-1][0] += used
            self.add(label, elapsed, used, max(0., used-frame[0]))
            with self.lock: self.active -= 1

    def wrap(self, obj, name, label, profile_root=False):
        original = getattr(obj, name)
        @functools.wraps(original)
        def measured(*args, **kwargs):
            with self.measure(label, profile_root): return original(*args, **kwargs)
        setattr(obj, name, measured)

    def export(self, directory):
        directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
        with self.lock:
            if self.active: raise RuntimeError('Export requires idle requests')
            rows = {}
            for label, value in self.rows.items():
                row = {k:v for k,v in value.items() if k!='samples'}
                samples = sorted(value['samples'])
                row['sample_count'] = len(samples)
                row['p95_wall_s'] = samples[min(len(samples)-1, int(len(samples)*.95))] if samples else 0
                rows[label] = row
            profiles = list(self.profiles.values())
        result = {'stages': rows, 'cpu_profile': self.profile}
        if profiles:
            stats = pstats.Stats(profiles[0])
            for p in profiles[1:]: stats.add(p)
            if len(stats.stats)>50000: raise RuntimeError('Profile function budget exceeded')
            stats.dump_stats(str(directory/'cpu.pstats'))
            functions=[]
            for (file,line,name),(primitive,calls,own,total,callers) in stats.stats.items():
                functions.append(dict(file=file,line=line,name=name,calls=calls,own_cpu_s=own,total_cpu_s=total))
            functions.sort(key=lambda r:r['own_cpu_s'],reverse=True)
            result['profile_total_cpu_s']=stats.total_tt
            result['top_cpu']=functions[:500]
            result['platform_cpu']=[r for r in functions if '/demiflow/demiflow/' in r['file']][:500]
        (directory/'metrics.json').write_text(json.dumps(result,indent=2))
        return result
