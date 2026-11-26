"""Native CLI transport for Forge engineering turns, without a Workbench runtime."""
import ctypes
import fcntl
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import time

import click

from .development_state import DevelopmentState
from .platform import content_digest
from .platform_environment import read_bounded
from .project_io import decode_control
from .schema import ForgeError
from .review import REVIEW_SECONDS, MAX_REVIEW_SECONDS


def agent_binding(provider, executable=None, profile=None, model=None):
    command = executable or shutil.which('codex' if provider == 'codex' else 'dsh')
    if not command or not Path(command).is_file():
        raise ForgeError('The configured native author CLI is unavailable', code='not_found')
    path = Path(command).resolve()
    info = path.stat()
    binding = dict(provider=provider, executable=str(path), profile=profile, model=model,
                   executable_identity=dict(device=info.st_dev, inode=info.st_ino, size=info.st_size,
                                            modified=info.st_mtime_ns))
    # Pin native configuration by digest without copying credentials into receipts.
    if provider == 'codex':
        home = Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex'))).resolve()
        files = [home / 'config.toml']
        if profile:
            files.append(home / (profile + '.config.toml'))
    else:
        home = Path(os.environ.get('DSH_HOME', str(Path.home() / '.dsh'))).resolve()
        files = [home / 'profiles' / (profile or 'headless') / name for name in ('package.json', 'cordis.patch.yml')]
    binding['home'] = str(home.absolute())
    binding['configuration'] = {str(path): content_digest(read_bounded(path, 256 * 1024).decode())
                                for path in files if path.exists()}
    return binding


def author_command(binding, session):
    command = [binding['executable']]
    if binding['provider'] == 'codex':
        command += ['exec']
        if binding['profile']:
            command += ['--profile', binding['profile']]
        if binding['model']:
            command += ['--model', binding['model']]
        if session:
            command += ['resume', session]
        command += ['--json', '--skip-git-repo-check', '-']
    else:
        command += ['--profile', binding['profile'] or 'headless', '--json']
        if session:
            command += ['--session-id', session]
        command += ['-']
    return command


def native_author(root, prompt, *, binding, session, lease_fd, deadline, notify, control_path=None, extra_path=None,
                  sandbox_spec=None, control_socket=None):
    """Run one native interval; session creation is distinct from input acknowledgement."""
    from demiflow.resources.process_observation import UNIT_ENV, descendants, drain_owned, signal_owned
    if len(prompt.encode()) > 128 * 1024:
        raise ForgeError('Author input exceeds its budget', code='resource_limit')
    if time.time() >= deadline:
        raise ForgeError('Development deadline exhausted', code='resource_limit')
    # This CLI process owns only its author/reviewer children, never a host service.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise ForgeError('Cannot establish author descendant ownership', code='operation_rejected')
    if sandbox_spec is not None:
        _forbidden = {'LD_PRELOAD', 'LD_LIBRARY_PATH', 'PYTHONHOME', 'PYTHONPATH'}
        _priority_prefixes = ('PATH', 'LANG', 'LC_', 'TERM', 'SSL_', 'REQUESTS_', 'CURL_',
                              'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                              'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
                              'CODEX_', 'DSH_', 'OPENAI_', 'DEEPSEEK_', 'ANTHROPIC_', 'DEMIFLOW_')
        env = {}
        for k, v in os.environ.items():
            if k in _forbidden or not isinstance(k, str) or not isinstance(v, str):
                continue
            if k.startswith(_priority_prefixes):
                env[k] = v
        for k, v in os.environ.items():
            if len(env) >= 120:
                break
            if k in _forbidden or k in env or not isinstance(k, str) or not isinstance(v, str):
                continue
            env[k] = v
    else:
        env = dict(os.environ)
    if extra_path:
        env['PATH'] = str(extra_path) + (os.pathsep + env['PATH'] if env.get('PATH') else '')
    if agent_binding(binding['provider'], binding['executable'], binding['profile'], binding['model']) != binding:
        raise ForgeError('Original author configuration changed before dispatch', code='conflict')
    env['CODEX_HOME' if binding['provider'] == 'codex' else 'DSH_HOME'] = binding['home']
    from .project_guard import CONTROL_ENV
    env.pop(CONTROL_ENV, None)
    if control_path is not None:
        env[CONTROL_ENV] = str(control_path)
    env[UNIT_ENV] = str(lease_fd)
    proc = None
    bound = session
    acknowledged = completed = final_seen = started = False
    count = total = sent = 0
    buffer = bytearray()
    encoded = prompt.encode()
    cleanup = 'unknown'
    outcome = dict(completed=False, acknowledged=False, cleanup='unknown', session=session,
                   provider=binding['provider'])
    try:
        notify(dict(delivery='sending'))
        if sandbox_spec is not None:
            from .sandbox import launch_sandbox
            proc = launch_sandbox(
                author_command(binding, session),
                spec=sandbox_spec,
                env=env,
                cwd=str(root),
                lease_fd=lease_fd,
                control_socket=control_socket,
                stdin=subprocess.PIPE,
            )
        else:
            from .native_exec import launch_command
            proc = subprocess.Popen(launch_command(author_command(binding, session)), cwd=root, env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                start_new_session=True, pass_fds=(lease_fd,))
        with selectors.DefaultSelector() as selector:
            for stream, interest in ((proc.stdin, selectors.EVENT_WRITE), (proc.stdout, selectors.EVENT_READ),
                                     (proc.stderr, selectors.EVENT_READ)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, interest)
            while selector.get_map() or proc.poll() is None:
                if time.time() >= deadline:
                    raise ForgeError('Author turn timed out', code='resource_limit')
                for key, _ in selector.select(.1):
                    if key.fileobj is proc.stdin:
                        try:
                            sent += os.write(key.fd, encoded[sent:sent + 16384])
                            if sent < len(encoded):
                                continue
                        except BrokenPipeError:
                            pass
                        selector.unregister(key.fd)
                        proc.stdin.close()
                        continue
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fd)
                        continue
                    total += len(chunk)
                    if total > 8 * 1024**2:
                        raise ForgeError('Author output budget exhausted', code='resource_limit')
                    if key.fileobj is proc.stderr:
                        continue
                    buffer.extend(chunk)
                    while (end := buffer.find(b'\n')) >= 0:
                        if end > 256 * 1024:
                            raise ForgeError('Native author frame exceeds budget', code='resource_limit')
                        raw = bytes(buffer[:end]); del buffer[:end + 1]
                        if not raw.strip() or binding['provider'] == 'deepseek' and raw.startswith(b'[llm-deepseek-account] '):
                            continue
                        event = decode_control(raw)
                        count += 1
                        if not isinstance(event, dict) or count > 20000:
                            raise ForgeError('Invalid native author event stream', code='resource_limit')
                        kind = event.get('type')
                        if kind in ('thread.started', 'session'):
                            sid = event.get('thread_id') if kind == 'thread.started' else event.get('sessionId')
                            if not isinstance(sid, str) or not sid or len(sid) > 256 or bound and sid != bound:
                                raise ForgeError('Original author session changed', code='conflict')
                            if kind == 'session' and event.get('cwd') != str(root):
                                raise ForgeError('Native author root changed', code='conflict')
                            bound = sid
                            notify(dict(session=bound))
                        elif binding['provider'] == 'codex':
                            if kind == 'turn.started':
                                if not bound or started:
                                    raise ForgeError('Author started without a bound session', code='conflict')
                                started = True
                            elif (kind == 'item.completed' and bound and not started and
                                  isinstance(event.get('item'), dict) and event['item'].get('type') == 'error'):
                                # The pinned CLI emits startup diagnostics between thread.started and turn.started.
                                # Only an active turn can acknowledge the submitted author input.
                                continue
                            elif kind in ('item.started', 'item.completed', 'turn.completed'):
                                if not started or completed:
                                    raise ForgeError('Author output is outside its active turn', code='conflict')
                                acknowledged = True
                                notify(dict(delivery='acknowledged'))
                                if kind == 'turn.completed':
                                    completed = True
                            elif kind in ('turn.failed', 'error'):
                                raise ForgeError('Native author did not complete its turn', code='inspection_incomplete')
                        elif kind == 'status' and event.get('phase') == 'turn_end':
                            if not bound or (event.get('reason') or {}).get('kind') != 'completed':
                                raise ForgeError('Native author did not complete its turn', code='inspection_incomplete')
                            acknowledged = completed = True
                            notify(dict(delivery='acknowledged'))
                        elif kind == 'final':
                            if not completed:
                                raise ForgeError('Native author final has no completed turn', code='conflict')
                            final_seen = True
                        elif kind == 'error':
                            raise ForgeError('Native author failed', code='inspection_incomplete')
                    if len(buffer) > 256 * 1024:
                        raise ForgeError('Native author frame exceeds budget', code='resource_limit')
        if (buffer.strip() or proc.returncode != 0 or not completed or not bound
                or binding['provider'] == 'deepseek' and not final_seen):
            raise ForgeError('Native author ended without a complete receipt', code='inspection_incomplete')
        outcome.update(completed=True, acknowledged=acknowledged, session=bound)
    except (OSError, ValueError, KeyboardInterrupt) as exc:
        outcome.update(reason=str(exc)[:1024] or 'Author cancelled', acknowledged=acknowledged, session=bound)
    finally:
        if proc is not None:
            signal_owned(os.getpid(), signal.SIGKILL)
            cleanup = 'complete' if drain_owned(proc, timeout_s=5) else 'unknown'
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()
        else:
            # An exec failure from Popen leaves no live child; no input was sent.
            cleanup = 'complete'
        outcome['cleanup'] = cleanup
    return outcome


def _cli_writable_roots(root: Path) -> tuple[str, ...]:
    from .sandbox import FORBIDDEN_DIRECT_WRITE_ROOTS, PROTECTED_PROJECT_NAMES
    protected = set(PROTECTED_PROJECT_NAMES) | {p[0] for p in FORBIDDEN_DIRECT_WRITE_ROOTS}
    names: list[str] = []
    if root.is_dir():
        for entry in sorted(os.scandir(root), key=lambda e: e.name):
            if entry.name.startswith('.') or entry.name in protected:
                continue
            if entry.is_symlink():
                continue
            if entry.is_dir(follow_symlinks=False) or entry.is_file(follow_symlinks=False):
                names.append(entry.name)
    return tuple(names)


def register(forge, output):
    @forge.command()
    @click.option('--root', required=True, type=click.Path(path_type=Path, file_okay=False))
    @click.option('--state-dir', required=True, type=click.Path(path_type=Path, file_okay=False),
                  help='Private operation directory outside the author project.')
    @click.option('--goal', help='Complete development goal; required for a new operation.')
    @click.option('--agent', type=click.Choice(['codex', 'deepseek']), default='codex')
    @click.option('--executable', type=click.Path(path_type=Path, dir_okay=False))
    @click.option('--profile', help='Existing native author profile; no privilege override is added.')
    @click.option('--model', help='Native Codex author model (DeepSeek uses its configured profile).')
    @click.option('--review/--no-review', default=True, help='Enable independent model review after code changes.')
    @click.option('--python', 'project_python', default=None, type=click.Path(path_type=Path, dir_okay=False),
                  help='Explicit project Python interpreter for live B-class package imports and A-class checks.')
    @click.option('--read-root', 'read_roots', multiple=True, type=click.Path(path_type=Path, file_okay=False),
                  help='Authorized external read directory for local_ref / vendor packages.')
    @click.option('--mode', 'execution_mode', type=click.Choice(['restricted', 'trusted_local']), default=None,
                  help='Sandbox execution mode (restricted uses Forge Landlock/seccomp policy shared with Workbench).')
    @click.option('--timeout', type=click.IntRange(1, 7200), default=3600, show_default=True)
    @click.option('--review-timeout', type=click.IntRange(1, MAX_REVIEW_SECONDS), default=REVIEW_SECONDS, show_default=True)
    @click.option('--resume', is_flag=True, help='Continue a definitely unsent action from this receipt.')
    @click.option('--status', is_flag=True, help='Read the persisted receipt without invoking an agent.')
    @click.option('--json', 'as_json', is_flag=True)
    def develop(root, state_dir, goal, agent, executable, profile, model, review, project_python, read_roots,
                execution_mode, timeout, review_timeout, resume, status, as_json):
        """Run check/review/feedback/repair with the same native coding session."""
        import sys
        import tempfile
        from uuid import uuid4
        from .development import run_development
        from .review_deepseek import run_reviewer
        from .sandbox import (
            SandboxShims,
            authorized_read_roots,
            backend_readiness,
            build_development_sandbox_spec,
            default_runtime_paths,
        )

        def perform():
            state = DevelopmentState(state_dir, root)
            if status:
                return state.load()
            with state.lock():
                binding = agent_binding(agent, executable, profile, model)
                if agent == 'deepseek' and model:
                    raise ForgeError('Select the DeepSeek model in its native profile', code='invalid_request')
                py_str = str(project_python.absolute()) if project_python is not None else None
                raw_read_roots = tuple(str(Path(p).resolve()) for p in read_roots)
                effective_mode = execution_mode
                if effective_mode is None:
                    if (py_str or raw_read_roots) and sys.platform == 'linux' and backend_readiness().get('available'):
                        effective_mode = 'restricted'
                    else:
                        effective_mode = 'trusted_local'
                shims = None
                bridge = None
                priv_dir = None
                created_empty_pyproject = False
                try:
                    if effective_mode == 'restricted':
                        if not backend_readiness().get('available'):
                            raise ForgeError('Restricted forge develop requires the Linux sandbox backend', code='operation_rejected')
                        from .bridge.server import BridgeServer

                        priv_dir = tempfile.TemporaryDirectory(prefix='forge-dev-home-')
                        priv_home = Path(priv_dir.name)
                        priv_home.chmod(0o700)
                        denied = (Path(state_dir).resolve(),)
                        allowed_roots = authorized_read_roots(
                            root, read_roots=raw_read_roots, denied_paths=denied, private_home=priv_home,
                        )
                        turn_id = 'cli-' + uuid4().hex[:16]
                        epoch = 'e-' + uuid4().hex[:12]

                        class _CliActiveTurn:
                            tid = turn_id
                            def __init__(self, ep):
                                self.epoch = ep
                            def require_active(self):
                                return None

                        bridge = BridgeServer(
                            _CliActiveTurn(epoch),
                            project_root=root,
                            allowed_read_roots=allowed_roots,
                            denied_paths=denied,
                        )
                        writables = _cli_writable_roots(root)
                        project_view = {
                            'root': str(Path(root).absolute()),
                            'kind': 'pipeline',
                            'phase': 'implementation',
                            'writes': list(writables),
                            'protected': [],
                            'python': py_str,
                            'read_roots': list(raw_read_roots),
                        }
                        shims = SandboxShims(
                            project_view,
                            execution_mode='restricted',
                            project_python=py_str,
                            allowed_read_roots=allowed_roots,
                            denied_paths=denied,
                        )
                        shims.set_control_binding(bridge.token, turn_id, epoch)
                        rt_paths = default_runtime_paths(
                            executable=binding['executable'],
                            project_python=py_str,
                            extra_paths=(*shims.runtime_paths,),
                        )
                        extra_reads = list(shims.read_paths)
                        if Path(binding['home']).exists():
                            extra_reads.append(binding['home'])
                        pyproj = Path(root) / 'pyproject.toml'
                        had_pyproject = pyproj.exists() or pyproj.is_symlink()
                        dev_spec = build_development_sandbox_spec(
                            root,
                            writes=writables,
                            protected=(),
                            read_roots=raw_read_roots,
                            extra_read_paths=extra_reads,
                            runtime_paths=rt_paths,
                            denied_paths=denied,
                            private_home=priv_home,
                            kind='pipeline',
                            phase='implementation',
                            readonly=False,
                            notebook_op=False,
                            direct_network=True,
                            seconds=min(86400, max(1, int(timeout))),
                            ensure_pyproject_placeholder=True,
                        )
                        created_empty_pyproject = (not had_pyproject) and pyproj.is_file()

                        def author_fn(*a, **kw):
                            return native_author(
                                *a, extra_path=shims.bin, sandbox_spec=dev_spec,
                                control_socket=bridge.path, **kw,
                            )

                        def reviewer_fn(prompt, *, timeout=review_timeout):
                            return run_reviewer(
                                prompt, timeout=timeout, restricted=True,
                                author_spec=dev_spec, denied_paths=denied,
                            )

                        return run_development(
                            state, goal=goal, binding=binding, author=author_fn,
                            reviewer=reviewer_fn, auto_review=review, seconds=timeout,
                            review_seconds=review_timeout, resume=resume, project_python=py_str,
                            allowed_roots=allowed_roots, denied_paths=denied,
                        )

                    author_fn = native_author
                    if py_str:
                        shims = SandboxShims({'root': str(root)}, execution_mode='trusted_local', project_python=py_str)

                        def author_fn(*a, **kw):
                            return native_author(*a, extra_path=shims.bin, **kw)

                    return run_development(
                        state, goal=goal, binding=binding, author=author_fn,
                        reviewer=run_reviewer, auto_review=review, seconds=timeout,
                        review_seconds=review_timeout, resume=resume, project_python=py_str,
                        allowed_roots=authorized_read_roots(root, read_roots=raw_read_roots) if raw_read_roots else None,
                    )
                finally:
                    if created_empty_pyproject:
                        pyproj = Path(root) / 'pyproject.toml'
                        try:
                            if pyproj.is_file() and not pyproj.is_symlink() and pyproj.stat().st_size == 0:
                                pyproj.unlink()
                        except OSError:
                            pass
                    if bridge is not None:
                        bridge.close()
                    if shims is not None:
                        shims.close()
                    if priv_dir is not None:
                        priv_dir.cleanup()
        output(root, as_json, perform)
