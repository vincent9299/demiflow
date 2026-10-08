"""Bounded file transport shared by Codex exec and app-server runtimes."""
import base64
import json
import os
from pathlib import Path
import stat

from demiflow.objects import ObjectRef


def collect_artifacts(directory, record, *, store, max_files, max_bytes):
    """Persist flat regular files after the agent process group has stopped."""
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        names = []
        with os.scandir(descriptor) as entries:
            for entry in entries:
                names.append(entry.name)
                if len(names) > max_files:
                    raise ValueError('Exported artifact count exceeds max_artifact_files')
        exports, total = [], 0
        for name in sorted(names):
            if '\\' in name:
                raise ValueError('Artifact names must be single filenames')
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError('Artifacts must be regular files, not links or directories')
                remaining = max_bytes - total
                if info.st_size > remaining:
                    raise ValueError('Exported artifacts exceed max_artifact_bytes')
                content = stream.read(remaining + 1)
                if len(content) > remaining:
                    raise ValueError('Exported artifacts exceed max_artifact_bytes')
                total += len(content)
                exports.append((name, content))
        for name, content in exports:
            ref = store.put(content)
            record['artifacts'].append({'name': name, 'byte_size': len(content), 'object_ref': ref.to_dict()})
    finally:
        os.close(descriptor)


def verify_artifacts(artifacts, *, max_files, max_bytes):
    """Reject unavailable cached objects without starting a replacement session."""
    if len(artifacts) > max_files:
        raise ValueError('Cached artifact count exceeds max_artifact_files')
    total = 0
    for artifact in artifacts:
        size = artifact['byte_size']
        if type(size) is not int or size < 0 or total + size > max_bytes:
            raise ValueError('Cached artifacts exceed max_artifact_bytes')
        total += size
        raw = ObjectRef(**artifact['object_ref']).read(max_bytes=max(1, size))
        if len(raw) != size:
            raise ValueError('Artifact byte size changed')


def file_context(messages, directory, *, max_files, max_bytes):
    """Map actual attachments to files; caller has bounded encoded input bytes."""
    work = Path(directory)
    artifacts = work / 'artifacts'
    artifacts.mkdir()
    inputs = []
    for message in messages:
        if isinstance(message['content'], str):
            continue
        for part in message['content']:
            if part['type'] != 'image_url':
                continue
            header, encoded = part['image_url']['url'].split(',', 1)
            if not header.startswith('data:image/') or not header.endswith(';base64'):
                raise ValueError('Codex request images must be bound data URLs')
            mime = header[5:-7]
            extension = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/webp': '.webp',
                         'image/gif': '.gif'}.get(mime, '.img')
            path = work / f'image-{len(inputs) + 1}{extension}'
            path.write_bytes(base64.b64decode(encoded, validate=True))
            inputs.append({'image_number': len(inputs) + 1, 'path': str(path), 'mime': mime})
    return {'input_images': inputs, 'artifact_directory': str(artifacts),
            'max_artifact_files': max_files, 'max_artifact_bytes': max_bytes}


def context_message(context):
    # Keep the established marker so existing Codex business prompts work unchanged.
    return ('Runtime file context: input_images map attached image numbers to actual files. '
            'Do not modify input files. Export only final selected files to artifact_directory, '
            'as regular files with flat names (copy bytes, do not create links). '
            'Refer to exported filenames in your structured answer. Files outside this directory '
            'will not be delivered. This context is transport metadata, not task evidence.\n'
            'CODEX_EXEC_FILE_CONTEXT\n' + json.dumps(context, ensure_ascii=False))
