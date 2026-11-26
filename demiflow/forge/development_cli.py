"""Native CLI transport for Forge engineering turns, without a Workbench runtime."""
import ctypes
import json
from contextlib import closing, contextmanager, nullcontext
import fcntl
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time

import click

from .development_state import DevelopmentState
from .platform import content_digest
from .platform_environment import read_bounded
from .project_io import decode_control
from .schema import ForgeError
from .review import REVIEW_SECONDS, MAX_REVIEW_SECONDS


def agent_binding(provider, executable=None, profile=None, model=None, *, restricted=False):
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
                                for path in files if not restricted and path.exists()}
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


def _restricted_command(binding, session, home, channel):
    """Build a native author command using only its private state and turn-scoped model token."""
    env = dict(HOME=str(home), PATH=os.pathsep.join((str(Path(sys.executable).parent), '/usr/bin', '/bin')),
               LANG='C.UTF-8', PYTHONDONTWRITEBYTECODE='1')
    provider = binding['provider']
    env['CODEX_HOME' if provider == 'codex' else 'DSH_HOME'] = str(home)
    command = author_command(binding, session)
    if channel is None:
        return command, env
    config = channel.child_config
    token = config['_boundary']['token']
    if provider == 'codex':
        overrides = {
            'model': config['model'], 'model_provider': 'forge',
            'model_providers.forge.name': 'Forge',
            'model_providers.forge.base_url': 'http://__DEMIFLOW_MODEL_ENDPOINT__/v1',
            'model_providers.forge.wire_api': 'responses', 'model_providers.forge.env_key': 'DEMIFLOW_MODEL_TOKEN',
            'model_providers.forge.supports_websockets': False, 'model_providers.forge.requires_openai_auth': False,
        }
        command = [binding['executable']]
        for key, value in overrides.items():
            command.extend(('-c', key + '=' + json.dumps(value)))
        command.extend(author_command(dict(binding, profile=None, model=None), session)[1:])
        env['DEMIFLOW_MODEL_TOKEN'] = token
    else:
        from .bridge.model_channel import plugin_path
        from .review_deepseek import _installation
        exe, node = _installation(config)
        route = config.get('model_provider', 'deepseek-official')
        profile = Path(home) / 'profiles' / 'forge-author'
        profile.mkdir(mode=0o700, parents=True, exist_ok=True)
        manifest = {'name': 'forge-author', 'private': True, 'dsh': {'profile': {
            'bundles': ['@deepseek-ai/dsh-base', '@deepseek-ai/dsh-headless']}}}
        proxy = dict(id='forge-model', name=str(plugin_path('deepseek_model_proxy.cjs')), config=dict(
            provider=route, model=config['model'], token=token, reasoningEffort=config.get('effort', 'high'),
            requestSeconds=config['_request_seconds'], llmModule=str(exe.parents[1] / 'node_modules/@deepseek-ai/dsh-llm/lib/index.js')))
        overlay = [dict(id='llm-deepseek', disabled=True), dict(id='llm-deepseek-account', disabled=True),
                   dict(id='agent-default-model', config=dict(provider=route, model=config['model'])), dict(insert=[proxy])]
        (profile / 'package.json').write_text(json.dumps(manifest))
        (profile / 'cordis.patch.yml').write_text(json.dumps(overlay))
        command = [str(node), str(exe), *author_command(dict(binding, profile='forge-author'), session)[1:]]
    return command, env


def native_author(root, prompt, *, binding, session, lease_fd, deadline, notify, control_path=None, extra_path=None,
                  sandbox_spec=None, control_socket=None, channel=None):
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
        command, env = _restricted_command(binding, session, Path(sandbox_spec.private_home), channel)
    else:
        command = author_command(binding, session)
        env = dict(os.environ)
        env['CODEX_HOME' if binding['provider'] == 'codex' else 'DSH_HOME'] = binding['home']
    if extra_path:
        env['PATH'] = str(extra_path) + (os.pathsep + env['PATH'] if env.get('PATH') else '')
    original = {key: value for key, value in binding.items() if key != 'execution'}
    if agent_binding(binding['provider'], binding['executable'], binding['profile'], binding['model'],
                     restricted=sandbox_spec is not None) != original:
        raise ForgeError('Original author configuration changed before dispatch', code='conflict')
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
                command,
                model_socket=channel.socket_path if channel else None,
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
    @click.option('--model-config', type=click.Path(path_type=Path, dir_okay=False), envvar='DEMIFLOW_FORGE_MODEL_CONFIG',
                  help='Host provider settings for restricted inference. Credentials stay on the host.')
    @click.option('--mode', 'execution_mode', type=click.Choice(['restricted', 'trusted_local']), default='restricted', show_default=True,
                  help='Sandbox execution mode (restricted uses Forge Landlock/seccomp policy shared with Workbench).')
    @click.option('--timeout', type=click.IntRange(1, 7200), default=3600, show_default=True)
    @click.option('--review-timeout', type=click.IntRange(1, MAX_REVIEW_SECONDS), default=REVIEW_SECONDS, show_default=True)
    @click.option('--resume', is_flag=True, help='Continue a definitely unsent action from this receipt.')
    @click.option('--status', is_flag=True, help='Read the persisted receipt without invoking an agent.')
    @click.option('--json', 'as_json', is_flag=True)
    def develop(root, state_dir, goal, agent, executable, profile, model, review, project_python, read_roots,
                execution_mode, model_config, timeout, review_timeout, resume, status, as_json):
        """Run check/review/feedback/repair with the same native coding session."""
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
                if agent == 'deepseek' and model:
                    raise ForgeError('Select the DeepSeek model in its native profile', code='invalid_request')
                py_str = str(project_python.absolute()) if project_python is not None else None
                raw_read_roots = tuple(str(Path(p).resolve()) for p in read_roots)
                shims = None
                bridge = None
                created_empty_pyproject = False
                try:
                    if execution_mode == 'restricted':
                        if not backend_readiness().get('available'):
                            raise ForgeError('Restricted forge develop needs an available Linux sandbox. Configure the backend or explicitly select --mode trusted_local.', code='operation_rejected')
                        from .bridge.server import BridgeServer
                        from .bridge.model_channel import local_channel
                        from .development_state import _private
                        from .io import no_symlinks
                        providers = {}
                        if model_config is not None:
                            value = decode_control(read_bounded(model_config.absolute(), 1024**2))
                            if not isinstance(value, dict) or not isinstance(value.get('providers'), dict):
                                raise ForgeError('Model settings require a providers object', code='invalid_request')
                            providers = value['providers']
                            if not isinstance(providers.get(agent), dict):
                                raise ForgeError('The selected author has no host provider configuration')
                        elif executable is None:
                            raise ForgeError('Configure --model-config for restricted inference or supply an offline --executable')
                        settings = providers.get(agent)
                        if settings and profile:
                            raise ForgeError('The restricted model channel supplies its own private profile')
                        if agent == 'deepseek' and settings:
                            from .review_deepseek import _installation
                            _installation(settings)
                        review_settings = providers.get('deepseek')
                        if review and not review_settings and os.getenv('DEMIFLOW_FORGE_REVIEW_CONFIG'):
                            from .review_deepseek import _local_config
                            review_settings = _local_config()
                        native = settings.get('executable') if settings else None
                        if executable and native and Path(executable).resolve() != Path(native).resolve():
                            raise ForgeError('Author executable differs from the configured provider')
                        binding = agent_binding(agent, executable or native, profile, model, restricted=True)
                        if settings and (not isinstance(settings.get('model'), str) or not settings['model']
                                         or model and model != settings['model']):
                            raise ForgeError('Author model must match its host provider configuration')
                        priv_home = _private(no_symlinks(state.directory.with_name(state.directory.name + '.author-home')))
                        native_home = Path(binding['home']).resolve()
                        denied = [state.directory]
                        for provider_config in (*providers.values(), review_settings):
                            if isinstance(provider_config, dict) and provider_config.get('native_credentials'):
                                denied.append(Path(provider_config['native_credentials']).resolve())
                        for path in (model_config, os.getenv('DEMIFLOW_FORGE_REVIEW_CONFIG')):
                            if path:
                                denied.append(Path(path).resolve())
                        # A native installation may live under DSH_HOME. Grant only the
                        # verified executable/runtime there, never that home as project input.
                        forbidden_inputs = (*denied, native_home, priv_home)
                        if any(root.resolve().is_relative_to(p) or p.is_relative_to(root.resolve()) for p in forbidden_inputs):
                            raise ForgeError('Project source overlaps private author or host settings', code='operation_rejected')
                        allowed_roots = authorized_read_roots(root, read_roots=raw_read_roots,
                                                              denied_paths=forbidden_inputs)
                        if agent == 'deepseek':
                            denied.extend((native_home / '.credentials.yaml', native_home / 'profiles'))
                        else:
                            denied.append(native_home)
                        binding['execution'] = dict(mode='restricted', python=py_str, reads=list(allowed_roots),
                                                    model_settings=content_digest(dict(providers=providers, review=review_settings)),
                                                    home=str(priv_home))
                        turn_id = 'cli-' + uuid4().hex[:16]

                        class ActiveTurn:
                            tid = turn_id
                            def __init__(self):
                                self.epoch = uuid4().hex
                            def require_active(self):
                                return None

                        bridge = BridgeServer(None)

                        @contextmanager
                        def controls(snapshot):
                            active = ActiveTurn()
                            bridge.set_turn(active, project_root=root, protected_snapshot=snapshot,
                                            allowed_read_roots=allowed_roots, denied_paths=denied)
                            shims.set_control_binding(bridge.token, active.tid, active.epoch)
                            paths = []
                            try:
                                yield paths
                            finally:
                                paths.extend(bridge.finish_turn())

                        @contextmanager
                        def model_connection(provider, config, deadline, request_seconds):
                            owner = state.load()['id']
                            with tempfile.TemporaryDirectory(prefix='model-', dir=state.directory) as home:
                                with closing(local_channel(provider, config, state.directory, owner, home=home,
                                                           deadline=deadline, request_seconds=request_seconds)) as channel:
                                    yield channel
                        writables = _cli_writable_roots(root)
                        project_view = {
                            'root': str(Path(root).absolute()),
                            'kind': 'pipeline',
                            'phase': 'implementation',
                            'writes': list(writables),
                            'protected': [],
                            'python': py_str,
                            'read_roots': list(allowed_roots),
                        }
                        shims = SandboxShims(
                            project_view,
                            execution_mode='restricted',
                            project_python=py_str,
                            allowed_read_roots=allowed_roots,
                            denied_paths=denied,
                        )
                        rt_paths = default_runtime_paths(
                            executable=binding['executable'],
                            project_python=py_str,
                            extra_paths=(*shims.runtime_paths, *((settings['node'],) if agent == 'deepseek' and settings else ())),
                        )
                        extra_reads = list(shims.read_paths)
                        pyproj = Path(root) / 'pyproject.toml'
                        had_pyproject = pyproj.exists() or pyproj.is_symlink()
                        dev_spec = build_development_sandbox_spec(
                            root,
                            writes=writables,
                            protected=(),
                            read_roots=allowed_roots,
                            extra_read_paths=extra_reads,
                            runtime_paths=rt_paths,
                            denied_paths=denied,
                            private_home=priv_home,
                            kind='pipeline',
                            phase='implementation',
                            readonly=False,
                            notebook_op=False,
                            direct_network=False,
                            seconds=min(86400, max(1, int(timeout))),
                            ensure_pyproject_placeholder=True,
                        )
                        created_empty_pyproject = (not had_pyproject) and pyproj.is_file()

                        def author_fn(*a, **kw):
                            connection = (model_connection(agent, settings, kw['deadline'], min(timeout, 3600))
                                          if settings else nullcontext())
                            with connection as channel:
                                return native_author(*a, extra_path=shims.bin, sandbox_spec=dev_spec,
                                                     control_socket=bridge.path, channel=channel, **kw)

                        def reviewer_fn(prompt, *, timeout=review_timeout):
                            if not isinstance(review_settings, dict):
                                raise ForgeError('Configure the DeepSeek reviewer in host settings', code='inspection_incomplete')
                            with model_connection('deepseek', review_settings, time.time() + timeout, timeout) as channel:
                                return run_reviewer(prompt, timeout=timeout, config=review_settings, channel=channel, restricted=True,
                                                    author_spec=dev_spec, denied_paths=denied)

                        return run_development(
                            state, goal=goal, binding=binding, author=author_fn,
                            reviewer=reviewer_fn, auto_review=review, seconds=timeout,
                            review_seconds=review_timeout, resume=resume, project_python=py_str,
                            allowed_roots=allowed_roots, denied_paths=denied, author_controls=controls,
                        )

                    binding = agent_binding(agent, executable, profile, model)
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
        output(root, as_json, perform)
