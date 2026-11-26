"""Fixed local run supervisor/worker entry. No graph scheduler or shell input."""
from pathlib import Path
import os
import sys

# Invoked by absolute trusted script under Python -I, including sealed copies.
_PLATFORM_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_PLATFORM_ROOT))
if __name__ == '__main__' and sys.argv[1:2] == ['execute']:
    from importlib.util import find_spec
    _spec = find_spec('demiflow')
    if _spec is None or not _spec.origin or Path(_spec.origin).resolve() != _PLATFORM_ROOT / 'demiflow/__init__.py':
        raise RuntimeError('Execution loaded DemiFlow outside the sealed platform source')

from demiflow.workbench.process import process_identity


def allocation_guard(limits, *, native_execution=None):
    import resource
    # Import the trusted native libraries before establishing virtual headroom.
    import pyarrow as pa
    import lance
    import datafusion
    import cloudpickle
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    baseline = int(Path('/proc/self/statm').read_text().split()[0]) * os.sysconf('SC_PAGE_SIZE')
    ceiling = baseline + min(limits['max_rss_bytes'] // 2, 4 * limits['memory_bytes'])
    if native_execution is not None:
        from demiflow.workbench.runs.native_resources import validate
        # CUDA reserves large virtual mappings even for a tiny tensor. This
        # finite sealed ceiling does not alter real RSS/device/disk admission
        # or the supervisor's whole-descendant-tree accounting and termination.
        ceiling = validate(native_execution)['virtual_address_bytes']
        _, inherited_hard = resource.getrlimit(resource.RLIMIT_AS)
        if inherited_hard != resource.RLIM_INFINITY:
            ceiling = min(ceiling, inherited_hard)
    if ceiling - baseline < 256 * 1024**2:
        raise MemoryError('Insufficient worker allocation headroom')
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(1024, hard), hard))


def execute(root, run_id, attempt=None):
    from demiflow.workbench.runs.manager import directory
    from demiflow.execution.host.control import RunJournal
    # Callbacks and stop polls own separate bounded connections from the engine
    # driver. Keep them for the process lifetime, without retaining transactions.
    with RunJournal(directory(root, run_id), attempt=attempt).connection_scope() as run:
        return _execute(root, run_id, run, attempt)


def _execute(root, run_id, run, attempt=None):
    import importlib
    import signal
    import time
    from demiflow.workbench.runs import manager as runs
    from demiflow.forge.platform_worker import bind_parent
    from demiflow.execution.host.control import RunJournal
    from demiflow.execution.host.control import CheckedExecution
    from demiflow.execution.host.control import local_execution
    from demiflow.execution.host.control import RunContext

    bind_parent()
    target, value = runs._load(root, run_id)
    manifest = value['manifest']
    run.execution_status()
    work = runs.attempt_directory(target, 'work', attempt)
    stopped = [False]
    registration_failed = [False]
    signal.signal(signal.SIGTERM, lambda *_: stopped.__setitem__(0, True))
    last = [0.0]

    class Cancelled(RuntimeError):
        pass

    def check_stop():
        now = time.monotonic()
        if now - last[0] > 0.1:
            last[0] = now
            stopped[0] |= run.execution_status()['status'] == 'stopping'
        if stopped[0]:
            raise Cancelled('Run cancellation requested')

    try:
        dependencies = manifest.get('project_dependencies')
        report = runs.verify_seal(target, manifest, environment=dependencies is None)
        if dependencies is not None:
            _verify_project_environment(dependencies)
        allocation_guard(manifest['limits'], native_execution=manifest.get('native_execution'))
        check_stop()
        run.transition({'starting'}, 'running', worker=process_identity(os.getpid()), running_at=time.time())
        from demiflow.forge.runtime import SharedSourceImports, bound_files, package_parts, read_sealed, sealed_root
        binding = report['binding']
        package = package_parts(binding.get('source_package'))
        source = sealed_root(target, binding)
        _source_imports(source, binding['sources'], package=package)
        shared = {name: (target / relative, digest) for name, _, relative, digest in bound_files(binding)}
        shared_paths = {path for path, _ in shared.values()}
        if shared:
            project_roots = {Path(name).parts[0].removesuffix('.py') for name in binding['sources']}
            def read_shared(name):
                return read_sealed(name, *shared[name])
            imports = SharedSourceImports({name: path for name, (path, _) in shared.items()},
                                         read=read_shared, project_roots=project_roots)
            imports.set_project_package(package)
            sys.meta_path.insert(0, imports)
        if dependencies and dependencies['modules']:
            from demiflow.forge.runtime import import_bootstrap
            bootstrap = import_bootstrap(work / 'project-imports', {
                name: {**proof, 'path': str(shared[name][0])}
                for name, proof in dependencies['modules'].items()
            }, platform_root=_PLATFORM_ROOT, project_roots=project_roots, project_package=package)
            previous = os.environ.get('PYTHONPATH')
            os.environ['PYTHONPATH'] = os.pathsep.join(filter(None, (str(bootstrap), previous)))
        module, function = binding.get('source_entry', manifest['entry']).split(':')
        module_name = binding.get('entry_module', module[:-3].replace('/', '.'))
        entry = getattr(importlib.import_module(module_name), function)
        import inspect
        if Path(inspect.getfile(entry)).resolve() != source / module:
            raise RuntimeError('Flow entry is not from the sealed source closure')
        import cloudpickle
        # Native transports serialize the already checked definitions, rather
        # than re-resolving candidate namespace packages against site-packages.
        for loaded in tuple(sys.modules.values()):
            filename = getattr(loaded, '__file__', None)
            if filename and (Path(filename).is_relative_to(source) or Path(filename) in shared_paths):
                cloudpickle.register_pickle_by_value(loaded)
        limits = runs.remaining_limits(manifest['limits'], value['state'])
        work.mkdir(parents=True, exist_ok=True)
        def record(action_id, kind, fact):
            run.action_event(action_id, kind, fact)
            if kind == 'stage_commit' or kind == 'receipt' and fact['status'] == 'committed':
                errors = runs.register_results(root, run_id, attempt=attempt)
                if errors:
                    registration_failed[0] = True
                    raise RuntimeError('Committed output registration failed: ' + '; '.join(errors))
        context_type = CheckedExecution
        context_options = {}
        if manifest.get('native_submission'):
            from demiflow.execution.host.control import NativeExecution
            context_type = NativeExecution
            context_options['contract'] = report['native_contract']
        context = context_type(**context_options, nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
            operators=report['operators'], source_root=source, resource_root=target, work_root=work, limits=limits,
            record=record, check_stop=check_stop, observe=run.observe_nodes,
            targets=report['binding']['targets'], report_digest=manifest['report_digest'])
        from demiflow.workbench.pipeline_policy import native_limits
        # Use the unified bounded host default; the manifest still owns all resource ceilings.
        with RunContext.attach(run, entry_identity=manifest.get('source_digest'), training_project_root=root,
                limits=manifest['limits'], explicit_limits=manifest.get('limits_explicit'), check_stop=check_stop), local_execution(
                worker_mode=(manifest.get('native_execution') or {}).get('worker_mode', 'thread'), partitions=1,
                batch_rows=limits['batch_rows'], memory_bytes=limits['memory_bytes'],
                inherited_native_resources=native_limits(limits), resource_root=source,
                diagnostics_root=target / 'diagnostics', block_size=limits['batch_rows']):
            with context.bind_context():
                entry(**report['binding']['arguments'])
        errors = runs.register_results(root, run_id, attempt=attempt)
        outputs = runs.show(root, run_id, attempt=attempt)['outputs']
        if errors or any(o['result'] is None for o in outputs):
            registration_failed[0] = True
            raise RuntimeError('Output registration incomplete: ' + '; '.join(errors))
        runs.verify_seal(target, manifest, environment=dependencies is None)
        if dependencies is not None:
            _verify_project_environment(dependencies)
        run.finish(dict(outcome='succeeded', finished_at=time.time()))
    except BaseException as exc:
        import traceback
        traceback.print_exc(limit=-12)
        # Previous independent writes remain available even if a later action failed.
        try:
            runs.register_results(root, run_id, attempt=attempt)
        except Exception:
            pass
        from demiflow.errors import LanceWriteError
        from demiflow.errors import PipelineRunTimeout
        from demiflow.errors import StallError
        run.finish(dict(outcome='cancelled' if isinstance(exc, Cancelled) else 'failed',
            recoverable=registration_failed[0] or isinstance(exc, (LanceWriteError, PipelineRunTimeout, StallError)),
            error={'type': type(exc).__name__, 'message': str(exc)[:2048]}, finished_at=time.time()))
        return 1
    return 0


def _verify_project_environment(binding):
    from demiflow.forge.runtime import current_environment
    actual = current_environment(binding, strict=True)
    expected = binding['environment']
    if 'startup_sha256' not in expected.get('interpreter', {}):
        actual = {**actual, 'interpreter': {k: v for k, v in actual['interpreter'].items() if k != 'startup_sha256'}}
    if actual != expected:
        raise RuntimeError('Sealed project Python environment changed')


def _source_imports(source, files, *, package=()):
    """Resolve candidate packages only from the inspected closure, including namespaces."""
    import importlib.abc
    import importlib.machinery
    import importlib.util
    roots = {Path(name).parts[0].removesuffix('.py') for name in files}
    if package:
        roots.add(package[0])
    import_root = source.parents[len(package) - 1] if package else source
    for name in roots:
        if name in sys.modules:
            raise ImportError('Candidate module collides with a loaded runtime module: ' + name)

    class SourceFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            parts = tuple(fullname.split('.'))
            if parts[0] not in roots:
                return None
            if parts == package[:len(parts)] and parts != package:
                spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                spec.submodule_search_locations = [str(import_root.joinpath(*parts))]
                return spec
            qualified = parts[:len(package)] == package if package else False
            if package and parts[0] == package[0] and not qualified:
                raise ImportError('Candidate import is outside the checked closure: ' + fullname)
            relative = '/'.join(parts[len(package):] if qualified else parts)
            initializer = f'{relative}/__init__.py' if relative else '__init__.py'
            for name in (initializer, relative + '.py'):
                if name in files:
                    return importlib.util.spec_from_file_location(fullname, source / name,
                        submodule_search_locations=[str(source / relative)] if name == initializer else None)
            if not relative or any(name.startswith(relative + '/') for name in files):
                spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                spec.submodule_search_locations = [str(source / relative)]
                return spec
            raise ImportError('Candidate import is outside the checked closure: ' + fullname)

    sys.meta_path.insert(0, SourceFinder())
    # Native child processes may deserialize references to these checked modules.
    sys.path.insert(1, str(source))
    if package:
        sys.path.insert(2, str(import_root))
        os.environ['PYTHONPATH'] = os.pathsep.join(filter(None, [os.environ.get('PYTHONPATH'), str(source)]))


def _tree_rss(pid):
    from demiflow.resources.process_observation import descendants
    return sum(row['rss_bytes'] for row in descendants(pid, include_owner=True).values())


def _adopt_descendants():
    """Keep orphaned native children owned by this dedicated supervisor."""
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # Linux PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), 'Cannot own native worker descendants')


def _stop_group(pid, signum):
    try:
        os.killpg(pid, signum)
    except ProcessLookupError:
        pass


def _drain_descendants(process, *, timeout_s=5, retained_units=()):
    from demiflow.resources.process_observation import drain_owned
    return drain_owned(process, timeout_s=timeout_s, retained_units=retained_units)


class RunLog:
    """A bounded tail with stable byte cursors across segment rotation."""
    def __init__(self, path, budget):
        self.path, self.size = path, max(1, budget // 4)
        self.count = min(4, budget)
        self.offset, self.used = 0, 0
        self.files = []
        self.stream = None

    def write(self, raw):
        while raw:
            if self.stream is None or self.used == self.size:
                self.close()
                filename = self.path / f'{self.offset:020d}.log'
                self.stream = filename.open('xb')
                self.files.append(filename)
                self.used = 0
                while len(self.files) > self.count:
                    self.files.pop(0).unlink()
            chunk, raw = raw[:self.size - self.used], raw[self.size - self.used:]
            self.stream.write(chunk)
            self.stream.flush()
            self.used += len(chunk)
            self.offset += len(chunk)

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None


def _supervise(root, run_id, lease, attempt=None, *, run):
    import selectors
    import signal
    import subprocess
    import time
    from demiflow.workbench.runs import manager as runs
    from demiflow.execution.host.control import RunJournal
    from demiflow.resources.process_observation import inherited_unit
    from demiflow.resources.process_observation import signal_owned
    from demiflow.resources.process_observation import UNIT_ENV

    target, value = runs._load(root, run_id)
    manifest = value['manifest']
    limits = runs.remaining_limits(manifest['limits'], value['state'])
    prior_usage = value['state'].get('usage', {})
    from demiflow.workbench.runs.storage import object_locations
    from demiflow.workbench.runs.storage import scratch_bytes
    from demiflow.workbench.runs.storage import clean_stopped, cleanup_deadline, remove_attempt_work
    object_roots = object_locations(manifest=manifest, target=target)
    run.execution_status()
    work = runs.attempt_directory(target, 'work', attempt)
    logs = runs.attempt_directory(target, 'logs', attempt)
    process = None
    observer = None
    observer_started = None
    output_watch = None
    log = None
    selector = selectors.DefaultSelector()
    killed_reason = None
    stop_at = None
    started = None
    accounting_valid = True
    children_drained = True
    retained_units = ()
    final_status, final_details = None, {}
    peaks = dict(rss_bytes=0, scratch_bytes=0, output_bytes=0, evidence_bytes=0)
    terminate = [False]
    signal.signal(signal.SIGTERM, lambda *_: terminate.__setitem__(0, True))
    supervisor_identity = process_identity(os.getpid())
    run.transition({'starting'}, 'starting', supervisor=supervisor_identity)

    def sample_output():
        nonlocal accounting_valid
        try:
            return output_watch.poll()
        except BaseException:
            accounting_valid = False
            raise

    def record_usage(output_bytes, *, complete=False):
        elapsed = 0 if started is None else int((time.monotonic() - started) * 1000)
        run.record_usage(elapsed_ms=prior_usage.get('elapsed_ms', 0) + elapsed,
                         output_bytes=prior_usage.get('output_bytes', 0) + output_bytes, complete=complete)

    try:
        runs.verify_seal(target, manifest)
        runs.retain_logs(target,attempt,min(limits['log_bytes'],64*1024**2))
        logs.mkdir(parents=True, exist_ok=True)
        work.mkdir(parents=True, exist_ok=True)
        runs.validate_output_targets(value)
        for output in manifest['outputs'].values():
            Path(output['uri']).parent.mkdir(parents=True, exist_ok=True)
        from demiflow.execution.journal.output_watch import OutputWatch
        # A new native output directory may also contain business delivery
        # files (for example image archives). Account the admitted directory,
        # while published table identities remain the exact compiled URIs.
        output_watch = OutputWatch(list(dict.fromkeys(
            output.get('allocation_root', output['uri']) for output in manifest['outputs'].values())))
        env = runs.worker_environment(target, limits, manifest.get('credential_names', []),
                                      native_execution=manifest.get('native_execution'),
                                      training_deployment=manifest.get('training_deployment'),
                                      capacity_domain=manifest.get('capacity_domain'),
                                      executable_dirs=manifest.get('executable_dirs', []),
                                      project_root=root)
        env['DEMIFLOW_INSPECTION_PARENT'] = str(os.getpid())
        env['TMPDIR'] = str(work)
        unit_fds = inherited_unit()
        if unit_fds:
            env[UNIT_ENV] = str(unit_fds[0])
        dependencies = manifest.get('project_dependencies')
        python = sys.executable
        if dependencies is not None:
            from demiflow.forge.runtime import verify_interpreter
            python = verify_interpreter(dependencies['environment']['interpreter'])
        command = [python, '-I', '-B', __file__, 'execute', str(root), run_id]
        if attempt is not None:
            command.append(str(attempt))
        started = time.monotonic()
        record_usage(0)  # Durable open interval before any child can write.
        _adopt_descendants()
        process = subprocess.Popen(command,
            cwd=target, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True, pass_fds=unit_fds)
        children_drained = False
        run.transition({'starting'}, 'starting', worker=process_identity(process.pid))
        # Optional sampling is isolated from protection/control. Its loss may
        # remove charts, but can never close accounting or finalize execution.
        if manifest.get('telemetry_enabled',True):
            try:
                observer = subprocess.Popen([sys.executable, '-I', '-B', __file__, 'observe',
                    str(root), run_id, str(attempt), str(process.pid), str(time.monotonic()+limits['timeout_s']+30)],
                    env=env, cwd=target, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, start_new_session=True, pass_fds=unit_fds)
                observer_started = time.monotonic()
            except OSError:
                observer = None

        os.set_blocking(process.stdout.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ)
        last_budget = 0.0
        log = RunLog(logs, min(limits['log_bytes'],64*1024**2))
        try:
            while process.poll() is None or selector.get_map():
                for key, _ in selector.select(timeout=0.1):
                    raw = os.read(key.fileobj.fileno(), 65536)
                    if not raw:
                        selector.unregister(key.fileobj)
                        continue
                    log.write(raw)
                now = time.monotonic()
                state = run.execution_status()
                reason = 'cancelled' if state['status'] == 'stopping' else None
                if terminate[0]:
                    reason = 'supervisor_stopped'
                if now - started > limits['timeout_s']:
                    reason = 'timeout'
                if now - last_budget > 0.2:
                    last_budget = now
                    rss = _tree_rss(process.pid)
                    control_rss = int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
                    if control_rss > 256*1024**2:
                        raise RuntimeError('Supervisor control memory budget exceeded')
                    if observer is not None and observer.poll() is None:
                        from demiflow.execution.host.inspection import latest
                        observed = latest(target, attempt)
                        stalled = now-observer_started > 5 and (observed.get('age_s', 10)>5)
                        if stalled or _tree_rss(observer.pid)>128*1024**2:
                            observer.kill()
                    scratch = scratch_bytes(target, work, object_roots)
                    evidence = runs._tree_bytes(target / 'evidence') + runs._tree_bytes(target / 'checkpoints')
                    output_bytes = sample_output()
                    record_usage(output_bytes)
                    for name, size in [('rss_bytes', rss), ('scratch_bytes', scratch), ('output_bytes', output_bytes), ('evidence_bytes', evidence)]:
                        peaks[name] = max(peaks[name], size)
                    if rss > limits['max_rss_bytes']:
                        reason = 'memory_budget'
                    if scratch > limits['max_scratch_bytes']:
                        reason = 'scratch_budget'
                    if output_bytes > limits['max_output_bytes']:
                        reason = 'output_disk_budget'
                    if evidence > limits['max_evidence_bytes']:
                        reason = 'evidence_disk_budget'
                if reason and reason.endswith('_budget'):
                    killed_reason = reason
                if reason and stop_at is None:
                    killed_reason, stop_at = reason, now
                    run.transition(runs.ACTIVE, 'stopping', reason=reason, stop_phase='terminating')
                    if reason == 'cancelled':
                        # The worker owns a bounded cooperative drain window.
                        # Its decoder children may still be making a received
                        # model response durable. Hard expiry still kills all.
                        process.send_signal(signal.SIGTERM)
                    else:
                        signal_owned(os.getpid(), signal.SIGTERM)
                if stop_at is not None and now - stop_at >= limits['cancel_grace_s']:
                    signal_owned(os.getpid(), signal.SIGKILL)
                    if state.get('stop_phase') != 'killing':
                        run.transition(runs.ACTIVE, 'stopping', stop_phase='killing')
                    if now - stop_at >= limits['cancel_grace_s'] + 5:
                        raise RuntimeError('Native process group did not drain within cancellation budget')
            process.wait()
        finally:
            log.close()
        if process.returncode and not killed_reason and manifest.get('training_deployment'):
            from demiflow.execution.host.control import retained_process_units
            # If a group cannot be identified, never broaden the retry into
            # unconditional SIGKILL against a potentially separate allocation.
            retained_units = None
            retained_units = retained_process_units(run, expected_attempt=attempt)
        children_drained = _drain_descendants(process, retained_units=retained_units)
        if not children_drained:
            accounting_valid = False
            raise RuntimeError('Owned native descendants did not stop; final accounting remains unknown')
        output_bytes = sample_output()
        record_usage(output_bytes, complete=True)
        peaks['output_bytes'] = max(peaks['output_bytes'], output_bytes)
        if output_bytes > limits['max_output_bytes']:
            killed_reason = 'output_disk_budget'
        complete = run.execution_view()['completion']
        verification_error = None
        if killed_reason and killed_reason.endswith('_budget'):
            status = 'failed'
        elif complete and complete['outcome'] == 'succeeded' and process.returncode == 0:
            status = 'succeeded'
            if dependencies is not None:
                try:
                    runs.verify_seal(target, manifest)
                except (OSError, ValueError) as exc:
                    status = 'failed'
                    verification_error = dict(type=type(exc).__name__, message=str(exc)[:2048])
        elif killed_reason == 'cancelled':
            status = 'cancelled'
        elif killed_reason:
            status = 'failed' if killed_reason != 'supervisor_stopped' else 'interrupted'
        else:
            status = complete['outcome'] if complete else 'interrupted'
        details = dict(finished_at=time.time(), returncode=process.returncode, sampled_peaks=peaks)
        if killed_reason:
            details['reason'] = killed_reason
        if verification_error is not None:
            details['error'] = verification_error
        elif complete and complete.get('error'):
            details['error'] = complete['error']
            details['recoverable'] = not killed_reason and complete.get('recoverable', False)
        final_status, final_details = status, details
    except BaseException as exc:
        import traceback
        traceback.print_exc(limit=-12)
        accounting_valid = process is None
        final_status = 'failed'
        final_details = dict(error={'type': type(exc).__name__, 'message': str(exc)[:2048]},
                             recoverable=process is None, finished_at=time.time())
    finally:
        if process is not None:
            if not children_drained and retained_units is not None:
                try:
                    children_drained = _drain_descendants(process, retained_units=retained_units)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    children_drained = False
            accounting_valid = accounting_valid and children_drained
            process.stdout.close()
        selector.close()
        # Close accounting only after the complete process group is stopped and
        # notifications are drained. A lost supervisor or overflow stays open.
        if accounting_valid:
            try:
                record_usage(sample_output() if output_watch is not None else 0, complete=True)
            except (OSError, ValueError):
                pass
        if output_watch is not None:
            output_watch.close()
        # Work is private to this stopped attempt; committed tables and recovery
        # facts live elsewhere. Never follow symlinks or remove external paths.
        maintenance_deadline = None
        try:
            cleanup = 'pending'
            if children_drained:
                maintenance_deadline = cleanup_deadline()
                def stopped_snapshot(snapshot):
                    return (children_drained and (process is None or process.poll() is not None)
                        and snapshot['attempt'] == attempt
                        and snapshot['supervisor'] == supervisor_identity
                        and snapshot['unit_identity'] == value['state'].get('unit_identity'))

                clean_stopped(run, confirm_stopped=stopped_snapshot, deadline=maintenance_deadline,
                              preserve_jobs=final_status == 'interrupted')
                remove_attempt_work(work, deadline=maintenance_deadline)
                cleanup = 'complete'
        except Exception as exc:
            cleanup = 'pending'
            final_details['cleanup_error'] = dict(type=type(exc).__name__, message=str(exc)[:1024])
        try:
            if children_drained:
                import resource
                own=resource.getrusage(resource.RUSAGE_SELF);children=resource.getrusage(resource.RUSAGE_CHILDREN)
                final_details['execution_resources']=dict(scope='supervisor_and_reaped_descendants',
                    cpu_user_s=own.ru_utime+children.ru_utime,cpu_system_s=own.ru_stime+children.ru_stime,
                    io_input_blocks=own.ru_inblock+children.ru_inblock,io_output_blocks=own.ru_oublock+children.ru_oublock,
                    supervisor_maxrss_bytes=own.ru_maxrss*1024,child_maxrss_bytes=children.ru_maxrss*1024)
                final_details.update(execution_stopped=True, stop_phase='confirmed', cleanup=cleanup)
                run.transition(runs.ACTIVE, final_status or 'interrupted', **final_details)
            else:
                run.transition(runs.ACTIVE, 'stopping', execution_stopped=False,
                               stop_phase='unconfirmed', cleanup='pending', reason='owned_processes_unconfirmed')
        except (OSError, ValueError):
            pass
        if maintenance_deadline is None or time.monotonic() < maintenance_deadline:
            runs.refresh_index(root, run_id, deadline=maintenance_deadline)


def supervise(root, run_id, lease, attempt=None):
    # The inherited execution lease covers initialization and every finalization
    # failure, including accounting, cleanup and index publication.
    try:
        from demiflow.workbench.runs.manager import directory
        from demiflow.execution.host.control import RunJournal
        with RunJournal(directory(root, run_id), attempt=attempt).connection_scope() as run:
            return _supervise(root, run_id, lease, attempt, run=run)
    finally:
        os.close(lease)


class SupervisorLog:
    def __init__(self, path):
        path.mkdir(parents=True, exist_ok=True)
        self.log = RunLog(path, 64*1024)
    def write(self, text):
        self.log.write(text.encode('utf-8', errors='replace'))
        return len(text)
    def flush(self):
        pass
    def close(self):
        self.log.close()


if __name__ == '__main__':
    mode, root, run_id = sys.argv[1:4]
    if mode == 'supervise':
        from contextlib import redirect_stderr
        import traceback
        from demiflow.workbench.runs.manager import directory
        attempt = int(sys.argv[5]) if len(sys.argv)>5 else None
        logger = SupervisorLog(directory(root,run_id)/'supervisor-logs'/str(attempt))
        try:
            with redirect_stderr(logger):
                try:
                    supervise(Path(root), run_id, int(sys.argv[4]), attempt)
                except BaseException:
                    traceback.print_exc(limit=-12)
                    raise
        finally:
            logger.close()
    elif mode == 'observe':
        from demiflow.forge.platform_worker import bind_parent
        from demiflow.workbench.runs.manager import directory
        from demiflow.execution.host.control import observe
        bind_parent()
        observe(directory(root,run_id),int(sys.argv[4]),int(sys.argv[5]),float(sys.argv[6]))
    elif mode == 'execute':
        raise SystemExit(execute(Path(root), run_id, int(sys.argv[4]) if len(sys.argv) > 4 else None))
    else:
        raise ValueError('Unsupported run worker mode')
