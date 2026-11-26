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


def native_author(root, prompt, *, binding, session, lease_fd, deadline, notify, control_path=None, extra_path=None):
    """Run one native interval; session creation is distinct from input acknowledgement."""
    from demiflow.resources.process_observation import UNIT_ENV, descendants, drain_owned, signal_owned
    if len(prompt.encode()) > 128 * 1024:
        raise ForgeError('Author input exceeds its budget', code='resource_limit')
    if time.time() >= deadline:
        raise ForgeError('Development deadline exhausted', code='resource_limit')
    # This CLI process owns only its author/reviewer children, never a host service.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise ForgeError('Cannot establish author descendant ownership', code='operation_rejected')
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
    @click.option('--timeout', type=click.IntRange(1, 7200), default=3600, show_default=True)
    @click.option('--review-timeout', type=click.IntRange(1, MAX_REVIEW_SECONDS), default=REVIEW_SECONDS, show_default=True)
    @click.option('--resume', is_flag=True, help='Continue a definitely unsent action from this receipt.')
    @click.option('--status', is_flag=True, help='Read the persisted receipt without invoking an agent.')
    @click.option('--json', 'as_json', is_flag=True)
    def develop(root, state_dir, goal, agent, executable, profile, model, review, project_python, timeout, review_timeout,
                resume, status, as_json):
        """Run check/review/feedback/repair with the same native coding session."""
        from .development import run_development
        from .review_deepseek import run_reviewer
        def perform():
            state = DevelopmentState(state_dir, root)
            if status:
                return state.load()
            with state.lock():
                binding = agent_binding(agent, executable, profile, model)
                if agent == 'deepseek' and model:
                    raise ForgeError('Select the DeepSeek model in its native profile', code='invalid_request')
                shims = None
                py_str = str(project_python.absolute()) if project_python is not None else None
                try:
                    author_fn = native_author
                    if py_str:
                        from .sandbox import SandboxShims
                        shims = SandboxShims({'root': str(root)}, execution_mode='trusted_local', project_python=py_str)
                        def author_fn(*a, **kw):
                            return native_author(*a, extra_path=shims.bin, **kw)
                    return run_development(state, goal=goal, binding=binding, author=author_fn,
                        reviewer=run_reviewer, auto_review=review, seconds=timeout,
                        review_seconds=review_timeout, resume=resume, project_python=py_str)
                finally:
                    if shims is not None:
                        shims.close()
        output(root, as_json, perform)
