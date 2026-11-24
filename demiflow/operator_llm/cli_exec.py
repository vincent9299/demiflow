"""Configurable local CLI transport for native prompt nodes.

Runs an explicit argv ``command`` template with bounded placeholder substitution,
process-group reaping, durable journaling/replay, and optional artifact collection.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time

from .client import _journal_io, request_messages
from .codex_files import collect_artifacts, verify_artifacts
from .errors import PromptResponseContractError
from .model import OperatorLLMRequestUsage, OperatorLLMResponse
from .sqlite_journal import SQLitePromptJournal
from ..objects import LocalObjectStore

_PLACEHOLDER_RE = re.compile(r'\{([^{}]+)\}')
_ENV_NAME_RE = re.compile(r'[A-Z_][A-Z0-9_]*')
_FENCE_RE = re.compile(r'\A\s*```(?:json)?\s*\n(.*?)\n\s*```\s*\Z', re.DOTALL | re.IGNORECASE)

ALLOWED_PLACEHOLDERS = frozenset({
    'model',
    'prompt_file',
    'messages_file',
    'schema_file',
    'output_file',
    'work_dir',
    'images_dir',
    'artifacts_dir',
})

_COMMAND_KEYS = frozenset({
    'command', 'input', 'output', 'cwd', 'env_passthrough',
    'artifact_store', 'max_artifact_files', 'max_artifact_bytes',
})


def _validate_argv(value, *, label='cli.command'):
    if not isinstance(value, (list, tuple)) or not value or len(value) > 64:
        raise ValueError(f'{label} must be a nonempty list of at most 64 strings')
    result = []
    for item in value:
        if type(item) is not str or not item or '\x00' in item:
            raise ValueError(f'{label} entries must be nonempty strings without NUL bytes')
        result.append(item)
    return result


def _validate_env_passthrough(value):
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or len(value) > 32:
        raise ValueError('env_passthrough must be a list of at most 32 environment variable names')
    names = []
    for item in value:
        if type(item) is not str or _ENV_NAME_RE.fullmatch(item) is None:
            raise ValueError(f'Invalid env_passthrough variable name: {item!r}')
        if item not in names:
            names.append(item)
    return tuple(names)


def _extract_json_content(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return text
    match = _FENCE_RE.match(stripped)
    if match:
        candidate = match.group(1).strip()
        try:
            json.loads(candidate)
        except ValueError:
            return text
        return candidate
    return stripped


def validate_cli_options(options):
    """Validate options['cli'] eagerly at declaration time (no subprocess or journal I/O)."""
    if set(options) - {'cli', 'sqlite_journal', 'timeout_s'}:
        raise ValueError('Unknown CLI execution options')
    if 'timeout_s' in options:
        timeout = options['timeout_s']
        if type(timeout) not in (int, float) or timeout <= 0:
            raise ValueError('CLI timeout must be positive')
    raw = options.get('cli')
    if not isinstance(raw, dict):
        raise ValueError('options.cli must be a mapping')
    unknown = set(raw) - _COMMAND_KEYS
    if unknown:
        raise ValueError(f'Unknown cli settings: {sorted(unknown)}')
    command = _validate_argv(raw.get('command'))
    input_mode = raw.get('input', 'stdin')
    if input_mode not in {'stdin', 'file'}:
        raise ValueError("cli.input must be 'stdin' or 'file'")
    output_mode = raw.get('output', 'stdout')
    if output_mode not in {'stdout', 'file'}:
        raise ValueError("cli.output must be 'stdout' or 'file'")
    placeholders = set()
    for arg in command[1:]:
        for name in _PLACEHOLDER_RE.findall(arg):
            if name not in ALLOWED_PLACEHOLDERS:
                raise ValueError(f'Unsupported CLI command placeholder: {{{name}}}')
            placeholders.add(name)
    if 'artifacts_dir' in placeholders and raw.get('artifact_store') is None:
        raise ValueError('{artifacts_dir} placeholder requires artifact_store')
    if output_mode == 'file' and 'output_file' not in placeholders:
        raise ValueError("cli.output='file' requires {output_file} in cli.command")
    if 'env_passthrough' in raw:
        _validate_env_passthrough(raw['env_passthrough'])
    if 'cwd' in raw and raw['cwd'] is not None:
        if type(raw['cwd']) is not str or not Path(raw['cwd']).is_absolute():
            raise ValueError('cli.cwd must be an absolute directory path')


def create_cli_prompt_client(model, options, max_requests=None, *, native_journal=None):
    return CliExecPromptClient(model, options, max_requests, native_journal=native_journal)


class CliExecPromptClient:
    """Generic non-interactive local CLI transport with argv template substitution."""

    def __init__(self, model, options, max_requests=None, *, native_journal=None):
        validate_cli_options(options)
        if native_journal is not None:
            from demiflow.execution.contracts.dataflow import (
                PrivatePromptJournalPort, PromptJournalPort, inherits_bound_port,
            )
            if (not inherits_bound_port(native_journal, PromptJournalPort)
                    or inherits_bound_port(native_journal, PrivatePromptJournalPort)
                    or native_journal.read_only or type(native_journal.limit) is not int
                    or native_journal.limit < 0):
                raise ValueError('CLI native journal requires a writable bounded RemotePromptJournal')
            if options.get('sqlite_journal'):
                raise ValueError('Native remote journal cannot coexist with a private CLI journal')
        self.options = dict(options)
        self.settings = dict(options['cli'])

        command = _validate_argv(self.settings.get('command'))
        self.command_template = tuple(command)
        self.executable = shutil.which(command[0])
        if not self.executable:
            raise ValueError(f'CLI executable not found: {command[0]!r}')

        self.input_mode = self.settings.get('input', 'stdin')
        self.output_mode = self.settings.get('output', 'stdout')

        self.artifact_options = self.settings.get('artifact_store')
        self.artifact_store = None
        self.max_artifact_files = self.settings.get('max_artifact_files', 8)
        self.max_artifact_bytes = self.settings.get('max_artifact_bytes', 64 * 1024 * 1024)
        for value in (self.max_artifact_files, self.max_artifact_bytes):
            if type(value) is not int or value < 1:
                raise ValueError('Artifact limits must be positive integers')
        if self.artifact_options is not None:
            if (not isinstance(self.artifact_options, dict)
                    or set(self.artifact_options) != {'directory'}
                    or not isinstance(self.artifact_options['directory'], str)
                    or not Path(self.artifact_options['directory']).is_absolute()):
                raise ValueError('artifact_store requires an absolute directory for independent objects')
            self.artifact_options = {'directory': str(Path(self.artifact_options['directory']).resolve())}
            self.artifact_store = LocalObjectStore(**self.artifact_options)
        elif {'max_artifact_files', 'max_artifact_bytes'} & set(self.settings):
            raise ValueError('Artifact limits require artifact_store')

        cwd = self.settings.get('cwd')
        if cwd is not None:
            if not Path(cwd).is_dir():
                raise ValueError('cli.cwd must be an existing absolute directory path')
            cwd = str(Path(cwd).resolve())
        self.cwd = cwd
        self.env_passthrough = _validate_env_passthrough(self.settings.get('env_passthrough'))

        self.model = model
        self.timeout = options.get('timeout_s', 600)
        self.journal = (SQLitePromptJournal(**options['sqlite_journal'], max_requests=max_requests)
                        if options.get('sqlite_journal') else native_journal)
        self.processes = set()

    def record(self, request):
        execution = {'input': self.input_mode, 'output': self.output_mode}
        if self.cwd is not None:
            execution['cwd'] = self.cwd
        if self.artifact_store:
            execution.update(
                artifact_protocol='cli-files/1',
                artifact_store=self.artifact_options,
                max_artifact_files=self.max_artifact_files,
                max_artifact_bytes=self.max_artifact_bytes,
            )
        return {
            'transport': 'cli',
            'stage': request.prompt_name,
            'prompt_version': request.prompt_version,
            'model': request.model,
            'command': list(self.command_template),
            'messages': request_messages(request),
            'response_schema': dict(request.response_schema),
            'schema_attempt': request.schema_attempt,
            'execution': execution,
        }

    def _collect_artifacts(self, directory, record):
        collect_artifacts(
            directory, record, store=self.artifact_store,
            max_files=self.max_artifact_files, max_bytes=self.max_artifact_bytes,
        )

    def decode(self, request, record, reused=False, refs=None):
        metadata = {
            'transport': 'cli',
            'model': request.model,
            'model_source': 'requested',
            'command': self.command_template[0],
            'elapsed_s': record['elapsed_s'],
            'reused': reused,
            'exit_code': record['exit_code'],
            'execution_status': record['status'],
        }
        if self.artifact_store:
            metadata.update(artifacts=record.get('artifacts', []), artifact_store=self.artifact_options)
        if record.get('artifact_error'):
            metadata['artifact_error'] = record['artifact_error']
        if self.journal:
            metadata.update(self.journal.references(self.record(request)))
        content = record.get('content', '')
        if (record['status'] != 'completed' or record['exit_code'] != 0 or not content.strip()):
            error = PromptResponseContractError(
                'CLI command did not complete a valid response '
                f'(status={record["status"]}, exit={record["exit_code"]}); raw CLI logs saved'
            )
            error.call = metadata
            raise error
        if reused and self.artifact_store:
            try:
                verify_artifacts(
                    record.get('artifacts', []),
                    max_files=self.max_artifact_files,
                    max_bytes=self.max_artifact_bytes,
                )
            except Exception as exc:
                error = PromptResponseContractError('Cached CLI artifact is unavailable: ' + str(exc))
                error.call = metadata
                raise error from exc
        if request.response_format == 'json':
            content = _extract_json_content(content)
        return OperatorLLMResponse(
            content, OperatorLLMRequestUsage(), endpoint='cli', metadata=metadata,
        )

    def lookup(self, request):
        if self.journal:
            record = self.journal.lookup(self.record(request))
            if record is not None:
                return self.decode(request, record, reused=True)
        return None

    @staticmethod
    async def _stop(process):
        """Reap the process group, including CLI-spawned children."""
        if process.returncode is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), 1)
        except asyncio.TimeoutError:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()

    @staticmethod
    def _render_args(template, replacements):
        def replace(match):
            return replacements[match.group(1)]
        return [_PLACEHOLDER_RE.sub(replace, arg) for arg in template]

    async def execute(self, request):
        source = self.record(request)
        if self.journal and not await _journal_io(self.journal.reserve, source):
            return await _journal_io(self.decode, request, await _journal_io(self.journal.lookup, source), True)
        started = time.monotonic()
        record = {'status': 'failed', 'exit_code': None, 'stdout': '', 'stderr': '', 'content': ''}
        if self.artifact_store:
            record['artifacts'] = []
        cancelled = None
        try:
            with tempfile.TemporaryDirectory(prefix='demiflow-cli-') as directory:
                work = Path(directory)
                images_dir = work / 'images'
                images_dir.mkdir()
                artifacts_dir = work / 'artifacts'
                if self.artifact_store:
                    artifacts_dir.mkdir()
                schema_path = work / 'response.schema.json'
                schema_path.write_text(json.dumps(source['response_schema'], ensure_ascii=False))
                output_path = work / 'answer.txt'

                text_parts, input_images, serialized_messages = [], [], []
                for message in source['messages']:
                    role = message['role']
                    text_parts.append(role.upper() + ':')
                    parts = message['content']
                    rendered_parts = []
                    for part in ([{'type': 'text', 'text': parts}] if isinstance(parts, str) else parts):
                        if part['type'] == 'text':
                            text_parts.append(part['text'])
                            rendered_parts.append({'type': 'text', 'text': part['text']})
                        elif part['type'] == 'image_url':
                            url = part['image_url']['url']
                            header, encoded = url.split(',', 1)
                            if not header.startswith('data:image/') or not header.endswith(';base64'):
                                raise ValueError('CLI request images must be bound data URLs')
                            mime = header[5:-7]
                            extension = {
                                'image/png': '.png', 'image/jpeg': '.jpg',
                                'image/webp': '.webp', 'image/gif': '.gif',
                            }.get(mime, '.img')
                            image_path = images_dir / f'image-{len(input_images) + 1}{extension}'
                            image_path.write_bytes(base64.b64decode(encoded, validate=True))
                            image_info = {
                                'image_number': len(input_images) + 1,
                                'path': str(image_path),
                                'mime': mime,
                            }
                            input_images.append(image_info)
                            rendered_parts.append({'type': 'image_file', **image_info})
                        else:
                            raise ValueError('Unsupported CLI message part')
                    serialized_messages.append({'role': role, 'content': rendered_parts})

                if self.artifact_store:
                    record['file_context'] = {
                        'input_images': input_images,
                        'artifact_directory': str(artifacts_dir),
                        'max_artifact_files': self.max_artifact_files,
                        'max_artifact_bytes': self.max_artifact_bytes,
                    }
                    text_parts.append(
                        'CLI_FILE_CONTEXT\n' + json.dumps(record['file_context'], ensure_ascii=False)
                    )

                prompt_text = '\n\n'.join(text_parts)
                prompt_path = work / 'prompt.txt'
                prompt_path.write_text(prompt_text, encoding='utf-8')
                messages_path = work / 'messages.json'
                messages_path.write_text(json.dumps(serialized_messages, ensure_ascii=False), encoding='utf-8')

                replacements = {
                    'model': request.model,
                    'prompt_file': str(prompt_path),
                    'messages_file': str(messages_path),
                    'schema_file': str(schema_path),
                    'output_file': str(output_path),
                    'work_dir': str(work),
                    'images_dir': str(images_dir),
                    'artifacts_dir': str(artifacts_dir),
                }
                argv = [self.executable, *self._render_args(self.command_template[1:], replacements)]

                process = None
                stdout_file_path = work / 'stdout.txt'
                stderr_file_path = work / 'stderr.txt'
                with stdout_file_path.open('wb') as stdout_fh, stderr_file_path.open('wb') as stderr_fh:
                    try:
                        stdin_arg = asyncio.subprocess.PIPE if self.input_mode == 'stdin' else asyncio.subprocess.DEVNULL
                        process = await asyncio.create_subprocess_exec(
                            *argv,
                            cwd=self.cwd or str(work),
                            stdin=stdin_arg,
                            stdout=stdout_fh,
                            stderr=stderr_fh,
                            start_new_session=True,
                        )
                        self.processes.add(process)
                        if self.input_mode == 'stdin':
                            await asyncio.wait_for(process.communicate(prompt_text.encode('utf-8')), self.timeout)
                        else:
                            await asyncio.wait_for(process.wait(), self.timeout)
                        record['status'] = 'completed'
                    except asyncio.TimeoutError:
                        record['status'] = 'timed_out'
                        if process is not None:
                            await self._stop(process)
                    except asyncio.CancelledError as error:
                        record['status'] = 'cancelled'
                        cancelled = error
                        if process is not None:
                            await asyncio.shield(self._stop(process))
                    finally:
                        if process is not None:
                            if process.returncode is None:
                                await asyncio.shield(self._stop(process))
                            self.processes.discard(process)
                            record['exit_code'] = process.returncode

                stdout_text = stdout_file_path.read_text(errors='replace')
                stderr_text = stderr_file_path.read_text(errors='replace')
                content = (
                    output_path.read_text(errors='replace') if output_path.exists() else ''
                ) if self.output_mode == 'file' else stdout_text
                record.update(stdout=stdout_text, stderr=stderr_text, content=content)

                if self.artifact_store:
                    try:
                        await _journal_io(self._collect_artifacts, artifacts_dir, record)
                    except asyncio.CancelledError as error:
                        cancelled = error
                        record['status'] = 'cancelled'
                    except Exception as error:
                        record['artifact_error'] = type(error).__name__ + ': ' + str(error)
                        record['stderr'] += '\nArtifact export failed: ' + record['artifact_error']
                        if record['status'] == 'completed':
                            record['status'] = 'artifact_failed'
        except Exception as error:
            if record['status'] == 'completed':
                record['status'] = 'failed'
            record['stderr'] += '\n' + type(error).__name__ + ': ' + str(error)
        record['elapsed_s'] = time.monotonic() - started
        if self.journal:
            await _journal_io(self.journal.response, source, record)
        if cancelled is not None:
            raise cancelled
        return self.decode(request, record)

    async def aclose(self):
        await asyncio.gather(*(self._stop(process) for process in tuple(self.processes)))
        if self.journal:
            await _journal_io(self.journal.close)
