"""Non-interactive local Codex CLI transport for native prompt nodes.

Each request uses an ephemeral session in a temporary working root. Optional
artifact exports permit workspace writes and are persisted before cleanup.
The caller supplies the model; this is a CLI process, not a collaboration agent.
"""
import asyncio
import base64
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import time

from .client import request_messages, _journal_io
from .codex_files import collect_artifacts, verify_artifacts
from ..objects import LocalObjectStore
from .errors import PromptResponseContractError
from .sqlite_journal import SQLitePromptJournal
from .model import OperatorLLMResponse, OperatorLLMRequestUsage


class CodexExecPromptClient:
    def __init__(self, model, options, max_requests=None):
        if set(options) - {'codex_exec', 'sqlite_journal', 'timeout_s'}:
            raise ValueError('Unknown Codex execution options')
        self.settings = dict(options['codex_exec'])
        if set(self.settings) - {'bin', 'reasoning_effort', 'output_schema', 'web_search',
                                 'image_generation', 'artifact_store', 'max_artifact_files', 'max_artifact_bytes'}:
            raise ValueError('Unknown codex_exec settings')
        # None preserves the original CLI default and request identity for T2I.
        self.image_generation = self.settings.get('image_generation')
        if self.image_generation is not None and type(self.image_generation) is not bool:
            raise ValueError('Codex image_generation must be boolean')
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
        elif self.image_generation is True:
            raise ValueError('image_generation requires artifact_store for durable output')
        elif {'max_artifact_files', 'max_artifact_bytes'} & set(self.settings):
            raise ValueError('Artifact limits require artifact_store')
        self.sandbox = 'workspace-write' if self.artifact_store else 'read-only'
        # 显式记录检索模式，避免检索能力改变后仍复用旧请求的答案。
        self.web_search = self.settings.get('web_search', 'cached')
        if self.web_search not in {'disabled', 'cached', 'live'}:
            raise ValueError('Codex web_search must be disabled, cached or live')
        executable = self.settings.get('bin') or 'codex'
        self.executable = shutil.which(executable)
        if not self.executable:
            raise ValueError('Codex CLI not found; configure codex_bin explicitly')
        self.model = model
        self.timeout = options.get('timeout_s', 600)
        if self.timeout <= 0:
            raise ValueError('Codex timeout must be positive')
        self.journal = (SQLitePromptJournal(**options['sqlite_journal'], max_requests=max_requests)
                        if options.get('sqlite_journal') else None)
        self.processes = set()

    def record(self, request):
        execution = {'sandbox': self.sandbox, 'ephemeral': True,
                     'ignore_user_config': True, 'project_doc_max_bytes': 0, 'web_search': self.web_search}
        if self.image_generation is not None:
            execution['image_generation'] = self.image_generation
        if self.artifact_store:
            execution.update(artifact_protocol='codex-files/2', artifact_store=self.artifact_options,
                             max_artifact_files=self.max_artifact_files, max_artifact_bytes=self.max_artifact_bytes,
                             exclude_slash_tmp=True, exclude_tmpdir_env_var=True, network_access=False)
        return {'transport': 'codex_exec', 'stage': request.prompt_name,
                'prompt_version': request.prompt_version, 'model': request.model,
                'reasoning_effort': self.settings.get('reasoning_effort'),
                'messages': request_messages(request),
                'response_schema': dict(request.response_schema),
                'output_schema': self.settings.get('output_schema', dict(request.response_schema)),
                'schema_attempt': request.schema_attempt,
                'execution': execution}

    def _collect_artifacts(self, directory, record):
        collect_artifacts(directory, record, store=self.artifact_store,
                          max_files=self.max_artifact_files, max_bytes=self.max_artifact_bytes)

    def decode(self, request, record, reused=False):
        events = []
        malformed = False
        for line in record['stdout'].splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError('event is not an object')
                events.append(event)
            except (ValueError, TypeError):
                malformed = True
        completed = [e for e in events if e.get('type') == 'turn.completed']
        usage = completed[-1].get('usage') or {} if completed else {}
        metadata = {'transport': 'codex_exec', 'model': request.model, 'model_source': 'requested',
                    'reasoning_effort': self.settings.get('reasoning_effort'),
                    'web_search': self.web_search,
                    'elapsed_s': record['elapsed_s'], 'usage': usage, 'reused': reused,
                    'exit_code': record['exit_code'], 'execution_status': record['status']}
        if self.image_generation is not None:
            metadata['image_generation'] = self.image_generation
        if self.artifact_store:
            metadata.update(artifacts=record.get('artifacts', []), artifact_store=self.artifact_options)
        if record.get('artifact_error'):
            metadata['artifact_error'] = record['artifact_error']
        if self.journal:
            metadata.update(self.journal.references(self.record(request)))
        if (record['status'] != 'completed' or record['exit_code'] != 0 or malformed
                or not completed or any(e.get('type') == 'turn.failed' for e in events)
                or not record['content'].strip()):
            error = PromptResponseContractError(
                'Codex CLI did not complete a valid response '
                f'(status={record["status"]}, exit={record["exit_code"]}); raw CLI logs saved')
            error.call = metadata
            raise error
        if reused and self.artifact_store:
            # A missing/corrupt persisted file is a delivery error, never a reason
            # to silently repeat a paid generation. lookup already runs off-loop.
            try:
                verify_artifacts(record.get('artifacts', []), max_files=self.max_artifact_files,
                                 max_bytes=self.max_artifact_bytes)
            except Exception as exc:
                error = PromptResponseContractError('Cached Codex artifact is unavailable: ' + str(exc))
                error.call = metadata
                raise error from exc
        return OperatorLLMResponse(record['content'], OperatorLLMRequestUsage.from_value(usage),
                                   endpoint='codex_exec', metadata=metadata)

    def lookup(self, request):
        if self.journal:
            record = self.journal.lookup(self.record(request))
            if record is not None:
                return self.decode(request, record, reused=True)
        return None

    @staticmethod
    async def _stop(process):
        """Reap the process group, including CLI-spawned tool children."""
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
        # The CLI may exit on TERM before a tool child does; finish the group too.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()

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
            with tempfile.TemporaryDirectory(prefix='demiflow-codex-') as directory:
                work = Path(directory)
                artifacts = work / 'artifacts'
                if self.artifact_store:
                    artifacts.mkdir()
                schema = work / 'response.schema.json'
                schema.write_text(json.dumps(source['output_schema'], ensure_ascii=False))
                output = work / 'answer.json'
                command = [self.executable, 'exec', '--ephemeral', '--json', '--ignore-user-config',
                           '--skip-git-repo-check', '--sandbox', self.sandbox, '--cd', directory,
                           '-c', 'approval_policy="never"', '-c', 'project_doc_max_bytes=0',
                           '-c', 'web_search=' + json.dumps(self.web_search),
                           '--model', request.model, '--output-schema', str(schema),
                           '--output-last-message', str(output)]
                if self.image_generation is not None:
                    command += ['-c', 'features.image_generation=' + json.dumps(self.image_generation)]
                if self.artifact_store:
                    command += ['-c', 'sandbox_workspace_write.exclude_slash_tmp=true',
                                '-c', 'sandbox_workspace_write.exclude_tmpdir_env_var=true',
                                '-c', 'sandbox_workspace_write.network_access=false']
                if self.settings.get('reasoning_effort'):
                    command += ['-c', 'model_reasoning_effort=' + json.dumps(self.settings['reasoning_effort'])]
                text, input_images = [], []
                for message in source['messages']:
                    text.append(message['role'].upper() + ':')
                    parts = message['content']
                    for part in ([{'type': 'text', 'text': parts}] if isinstance(parts, str) else parts):
                        if part['type'] == 'text':
                            text.append(part['text'])
                        elif part['type'] == 'image_url':
                            url = part['image_url']['url']
                            header, encoded = url.split(',', 1)
                            if not header.startswith('data:image/') or not header.endswith(';base64'):
                                raise ValueError('Codex request images must be bound data URLs')
                            mime = header[5:-7]
                            extension = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/webp': '.webp',
                                         'image/gif': '.gif'}.get(mime, '.img')
                            image_path = work / f'image-{len(input_images) + 1}{extension}'
                            image_path.write_bytes(base64.b64decode(encoded, validate=True))
                            input_images.append({'image_number': len(input_images) + 1,
                                                 'path': str(image_path), 'mime': mime})
                            command += ['--image', str(image_path)]
                        else:
                            raise ValueError('Unsupported Codex message part')
                if self.artifact_store:
                    record['file_context'] = {'input_images': input_images, 'artifact_directory': str(artifacts),
                                              'max_artifact_files': self.max_artifact_files,
                                              'max_artifact_bytes': self.max_artifact_bytes}
                    text.append('Runtime file context: input_images map attached image numbers to actual files. '
                                'Do not modify input files. Export only final selected files to artifact_directory, '
                                'as regular files with flat names (copy bytes, do not create links). '
                                'Refer to exported filenames in your structured answer. Files outside this directory '
                                'will not be delivered. This context is transport metadata, not task evidence.')
                    text.append('CODEX_EXEC_FILE_CONTEXT\n' + json.dumps(record['file_context'], ensure_ascii=False))
                command.append('-')
                process = None
                with (work / 'stdout.jsonl').open('wb') as stdout, (work / 'stderr.txt').open('wb') as stderr:
                    try:
                        process = await asyncio.create_subprocess_exec(
                            *command, cwd=directory, stdin=asyncio.subprocess.PIPE,
                            stdout=stdout, stderr=stderr, start_new_session=True)
                        self.processes.add(process)
                        await asyncio.wait_for(process.communicate('\n\n'.join(text).encode()), self.timeout)
                        record['status'] = 'completed'
                    except asyncio.TimeoutError:
                        record['status'] = 'timed_out'
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
                record.update(stdout=(work / 'stdout.jsonl').read_text(errors='replace'),
                              stderr=(work / 'stderr.txt').read_text(errors='replace'),
                              content=output.read_text() if output.exists() else '')
                if self.artifact_store:
                    try:
                        # Drain storage on cancellation before TemporaryDirectory removes files.
                        await _journal_io(self._collect_artifacts, artifacts, record)
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
