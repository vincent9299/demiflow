"""Run one independent, evidence-only review with the installed DeepSeek Harness."""

import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import tempfile
import time

from .platform_environment import read_bounded
from .project_io import decode_control
from .schema import ForgeError
from .review import REVIEW_SECONDS, MAX_REVIEW_SECONDS


MAX_INPUT = 2 * 1024**2
MAX_OUTPUT = 2 * 1024**2
PROFILE = 'forge-review'


def _unavailable(message):
    return ForgeError(message, code='inspection_incomplete')


def _local_config():
    path = os.getenv('DEMIFLOW_FORGE_REVIEW_CONFIG')
    if not path:
        raise _unavailable('Configure the DeepSeek reviewer with DEMIFLOW_FORGE_REVIEW_CONFIG')
    try:
        config = decode_control(read_bounded(Path(path).absolute(), 1024**2))
    except (ValueError, OSError) as exc:
        raise _unavailable('Cannot read the configured DeepSeek reviewer settings') from exc
    if isinstance(config, dict) and isinstance(config.get('providers'), dict):
        return config['providers'].get('deepseek')
    return config


def _installation(config):
    if not isinstance(config, dict) or not all(isinstance(config.get(k), str) and config[k]
            for k in ('executable', 'node', 'model', 'version')):
        raise _unavailable('The reviewer requires configured DeepSeek executable, node, model and version')
    executable = Path(config['executable'])
    node = Path(config['node'])
    if not executable.is_absolute() or not node.is_absolute() or not node.is_file():
        raise _unavailable('The reviewer requires absolute installed DeepSeek and Node executable paths')
    executable = executable.resolve()
    if executable.name != 'bin.js' or not executable.is_file():
        raise _unavailable('DeepSeek headless review requires its installed CLI package')
    try:
        package = decode_control(read_bounded(executable.parents[1] / 'package.json', 65536))
    except (ValueError, OSError) as exc:
        raise _unavailable('Cannot verify the installed DeepSeek CLI package') from exc
    if not isinstance(package, dict) or (package.get('name'), package.get('version')) != (
            '@deepseek-ai/dsh', config['version']):
        raise _unavailable('Installed DeepSeek CLI package differs from reviewer configuration')
    # This headless plugin composition and terminal event protocol are qualified
    # independently of versions accepted by interactive development sessions.
    if package['version'] != '0.2.1-rc.2':
        raise _unavailable('Installed DeepSeek CLI version is not qualified for independent review')
    return executable, node


def prepare_launch(config, home, *, model_channel=None, broker=False):
    """Prepare a native dsh command. A confined host supplies only its fixed model channel."""
    home = Path(home)
    executable, node = _installation(config)
    # Start from an empty plugin graph. "tools" is the empty registry required by
    # the native agent loop. No shell, file, PTC, MCP or other tool is mounted.
    rows = []

    def plugin(name, **settings):
        row = dict(id=name, name='@deepseek-ai/dsh-' + name)
        if settings:
            row['config'] = settings
        rows.append(row)

    plugin('llm')
    route = config.get('model_provider', 'deepseek-official')
    if not broker:
        plugin('session')
        plugin('session-projection')
        plugin('agent')
        plugin('system-prompt', personaPrefix='You are an independent pipeline reviewer.')
        plugin('tools', mode='native')
        plugin('agent-loop', agents=[])
        plugin('agent-default-model', provider=route, model=config['model'])
    env = dict(PATH=str(node.parent) + ':/usr/bin:/bin', HOME=str(home),
               DSH_HOME=str(home), TMPDIR=str(home), LANG='C.UTF-8')
    seconds = config.get('_request_seconds', REVIEW_SECONDS)
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= MAX_REVIEW_SECONDS:
        raise ForgeError('Invalid DeepSeek review time budget', code='invalid_request')
    options = {'reasoningEffort': config.get('effort', 'high'), 'streamIdleTimeoutMs': seconds * 1000}
    if route not in ('deepseek-official', 'deepseek-account'):
        raise _unavailable('Unsupported DeepSeek reviewer route')
    boundary = config.get('_boundary')
    if model_channel is not None:
        if (not Path(model_channel).is_absolute() or not Path(model_channel).is_file()
                or not isinstance(boundary, dict) or set(boundary) != {'token'}
                or not isinstance(boundary['token'], str) or not re.fullmatch('[a-f0-9]{64}', boundary['token'])
                or any(config.get(k) for k in ('native_credentials', 'credential_env', 'base_url', 'inherit_proxy'))):
            raise _unavailable('DeepSeek review requires the fixed model channel of its host')
        module = executable.parents[1] / 'node_modules/@deepseek-ai/dsh-llm/lib/index.js'
        rows.append(dict(id='review-model', name=str(model_channel), config=dict(provider=route,
            model=config['model'], token=boundary['token'], reasoningEffort=config.get('effort', 'high'),
            requestSeconds=seconds,
            llmModule=str(module))))
    elif boundary is not None:
        raise _unavailable('The host model channel must be attached before starting review')
    elif route == 'deepseek-account':
        source = config.get('native_credentials')
        if not isinstance(source, str) or not Path(source).is_absolute():
            raise _unavailable('DeepSeek account review requires the configured native credential file')
        try:
            # Some account authentication failures remove stale tokens. Isolate
            # that behaviour from the credentials used by the developer.
            private = home / '.credentials.yaml'
            private.write_bytes(read_bounded(Path(source), 65536))
            private.chmod(0o600)
        except (OSError, ValueError) as exc:
            raise _unavailable('Cannot access the configured DeepSeek account credentials') from exc
        plugin('authorization')
        plugin('credentials-local', path=str(private), watch=False)
        plugin('deepseek-account-platform')
        plugin('llm-deepseek-account', **options)
    else:
        key_name = config.get('credential_env', 'DEEPSEEK_API_KEY')
        if not isinstance(key_name, str) or not os.environ.get(key_name):
            raise _unavailable('DeepSeek review API credential is unavailable')
        env['DEEPSEEK_API_KEY'] = os.environ[key_name]
        if config.get('base_url'):
            options['baseURL'] = config['base_url']
        plugin('llm-deepseek-api-key', apiKeyEnv='DEEPSEEK_API_KEY', **options)
    if config.get('inherit_proxy') is True:
        for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy'):
            if name in os.environ:
                env[name] = os.environ[name]
        env['NODE_USE_ENV_PROXY'] = '1'

    profile_name = PROFILE
    if broker:
        from .bridge.model_channel import plugin_path
        profile_name = 'forge-model'
        rows.append(dict(id='model-broker', name=str(plugin_path('deepseek_model_broker.cjs')),
                         config=dict(provider=route, model=config['model'],
                                     reasoningEffort=config.get('effort', 'high'), requestSeconds=seconds)))
    else:
        plugin('headless', task='-', json=True)
    profile = home / 'profiles' / profile_name
    profile.mkdir(parents=True, mode=0o700)
    manifest = {'name': PROFILE, 'private': True, 'dsh': {'profile': {'bundles': []}}}
    (profile / 'package.json').write_text(json.dumps(manifest))
    (profile / 'cordis.patch.yml').write_text(json.dumps([{'insert': rows}]))
    return [str(node), str(executable), '--profile', profile_name], env


def _encoded(prompt, timeout):
    if not isinstance(prompt, str) or not prompt.strip():
        raise ForgeError('DeepSeek review requires evidence', code='invalid_request')
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= MAX_REVIEW_SECONDS:
        raise ForgeError('Invalid DeepSeek review time budget', code='invalid_request')
    if len(prompt) > MAX_INPUT or len(encoded := prompt.encode('utf-8')) > MAX_INPUT:
        raise ForgeError('DeepSeek review evidence exceeds its byte budget', code='resource_limit')
    return encoded


def _communicate(proc, encoded, deadline, check_cancel):
    output = bytearray()
    consumed = line_start = 0
    with selectors.DefaultSelector() as selector:
        channels = ((proc.stdin, selectors.EVENT_WRITE), (proc.stdout, selectors.EVENT_READ),
                    (proc.stderr, selectors.EVENT_READ))
        for stream, interest in channels:
            if stream is not None:
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, interest)
        while selector.get_map() or proc.poll() is None:
            if check_cancel is not None:
                check_cancel()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _unavailable('DeepSeek review timed out')
            for key, _ in selector.select(min(0.1, remaining)):
                if key.fileobj is proc.stdin:
                    try:
                        consumed += os.write(key.fd, encoded[consumed:consumed + 16384])
                        if consumed < len(encoded):
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
                if key.fileobj is proc.stderr:
                    continue
                if len(output) + len(chunk) > MAX_OUTPUT:
                    raise ForgeError('DeepSeek review output exceeds its byte budget', code='resource_limit')
                output.extend(chunk)
                while (end := output.find(b'\n', line_start)) >= 0:
                    # Stop before an unsolicited tool request causes more model steps.
                    if output.startswith(b'{"type":"tool_call",', line_start):
                        raise _unavailable('DeepSeek review attempted to use an unavailable tool')
                    line_start = end + 1
    return output


def extract_report(content):
    """Accept one complete object with optional prose/fences; never choose among objects."""
    depth = 0
    quoted = escaped = False
    start = None
    objects = []
    for index, char in enumerate(content):
        if depth:
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    objects.append(content[start:index + 1])
        elif char == '{':
            depth, start = 1, index
    if depth or len(objects) != 1:
        raise _unavailable('DeepSeek reviewer returned an invalid JSON report (expected one object)')
    result = decode_control(objects[0])
    if not isinstance(result, dict):
        raise _unavailable('DeepSeek reviewer returned an invalid JSON report')
    return result


def _result(returncode, raw, model, home):
    session_id = None
    completed = False
    report = None
    format_error = None
    for line in raw.splitlines():
        # 0.2.1-rc.2 account model listing logs to stdout even in JSON mode.
        if line.startswith(b'[llm-deepseek-account] '):
            continue
        try:
            event = decode_control(line)
        except ValueError as exc:
            raise _unavailable('Invalid DeepSeek CLI event') from exc
        if not isinstance(event, dict):
            raise _unavailable('Invalid DeepSeek CLI event')
        kind = event.get('type')
        if report is not None:
            raise _unavailable('Unexpected event after DeepSeek review completion')
        if kind == 'session':
            sid = event.get('sessionId')
            if session_id or event.get('cwd') != str(home) or not isinstance(sid, str) or not re.fullmatch(
                    r'session-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', sid):
                raise _unavailable('Unexpected DeepSeek reviewer session')
            session_id = sid
        elif not session_id or kind in ('tool_call', 'tool_result', 'error'):
            raise _unavailable('DeepSeek review failed or attempted to use an unavailable tool')
        elif kind == 'status' and event.get('phase') == 'turn_end':
            reason = event.get('reason')
            if completed or not isinstance(reason, dict):
                raise _unavailable('Invalid DeepSeek review terminal event')
            if reason.get('kind') != 'completed':
                kind_label = reason.get('kind') if isinstance(reason.get('kind'), str) else ''
                error = reason.get('error')
                code = error.get('code') if isinstance(error, dict) else ''
                msg = error.get('message') if isinstance(error, dict) and isinstance(error.get('message'), str) else ''
                if any(w in msg.lower() for w in ('http://', 'https://', 'bearer ', 'token', 'sk-')):
                    msg = ''
                parts = []
                if isinstance(code, str) and re.fullmatch('[A-Z_]{1,48}', code) and code != 'UNKNOWN':
                    parts.append(code)
                elif kind_label and kind_label != 'error':
                    parts.append(kind_label[:48])
                if msg:
                    parts.append(msg.strip()[:140])
                suffix = ' (' + ': '.join(parts) + ')' if parts else ''
                raise _unavailable('DeepSeek review turn failed' + suffix)
            completed = True
        elif kind == 'final':
            if not completed or not isinstance(event.get('text'), str):
                raise _unavailable('DeepSeek review final response does not match a completed turn')
            content = event['text'].strip()
            try:
                report = extract_report(content)
            except ValueError as exc:
                # Keep a bounded single-line excerpt so the 评审 panel shows what the model returned.
                excerpt = ' '.join(content.split())[:300]
                if any(w in excerpt.lower() for w in ('http://', 'https://', 'bearer ', 'token', 'sk-')):
                    excerpt = ''
                suffix = f' (excerpt: {excerpt}{"…" if len(content) > 300 else ""})' if excerpt else ''
                format_error = 'DeepSeek reviewer returned an invalid JSON report' + suffix
                report = content
            if not isinstance(report, dict) and format_error is None:
                raise _unavailable('DeepSeek reviewer returned an invalid report')
        elif kind not in ('status', 'text', 'thinking'):
            raise _unavailable('Unexpected DeepSeek CLI event')
    if returncode or not session_id or report is None:
        raise _unavailable('DeepSeek review ended without a completed report')
    response = dict(provider='deepseek', model=model, session_id=session_id, report=report)
    if format_error:
        from .review import ReviewFormatError
        raise ReviewFormatError(format_error, response)
    return response


def collect_review(process, prompt, *, cwd, model, timeout=REVIEW_SECONDS, check_cancel=None):
    """Own stdio, cancellation and cleanup of the newly admitted CLI process."""
    try:
        encoded = _encoded(prompt, timeout)
        raw = _communicate(process, encoded, time.monotonic() + timeout, check_cancel)
        return _result(process.returncode, raw, model, Path(cwd))
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        # Do not poll/reap the group leader before signalling: a failed CLI
        # can leave a child holding a pipe open after its own exit.
        if process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def run_reviewer(prompt, *, timeout=REVIEW_SECONDS, config=None, check_cancel=None,
                 restricted=False, author_spec=None, denied_paths=(), channel=None):
    """Start a fresh native headless agent with only its supplied text evidence."""
    if check_cancel is not None:
        check_cancel()
    if config is None:
        config = _local_config()
    with tempfile.TemporaryDirectory(prefix='demiflow-forge-review-') as directory:
        home = Path(directory)
        home.chmod(0o700)
        if restricted and channel is None:
            raise _unavailable('Restricted review requires a host model channel')
        from .bridge.model_channel import plugin_path
        command, env = prepare_launch(dict(channel.child_config if channel else config, _request_seconds=timeout),
                                      home, model_channel=plugin_path('deepseek_model_proxy.cjs') if channel else None)
        try:
            if restricted:
                from .sandbox import build_review_sandbox_spec, default_runtime_paths, launch_sandbox
                exe_path = Path(config['executable']).resolve()
                node_path = Path(config['node']).resolve()
                review_rt = default_runtime_paths(
                    executable=exe_path,
                    extra_paths=(node_path.parent, exe_path.parents[1]),
                )
                spec = build_review_sandbox_spec(
                    author_spec,
                    private_home=home,
                    seconds=max(1.0, float(timeout)),
                    extra_runtime_paths=review_rt,
                    denied_paths=denied_paths,
                    direct_network=False,
                )
                proc = launch_sandbox(command, spec=spec, env=env, cwd=str(home), model_socket=channel.socket_path, stdin=subprocess.PIPE)
            else:
                from .native_exec import launch_command
                proc = subprocess.Popen(launch_command(command), cwd=home, env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise _unavailable('Failed to launch the configured DeepSeek CLI') from exc
        return collect_review(proc, prompt, cwd=home, model=config['model'], timeout=timeout, check_cancel=check_cancel)
