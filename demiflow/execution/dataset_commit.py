"""Durable append intent around a caller's standard Dataset writer.

Business code owns its schema, fingerprint and output receipts. This control
journal closes the gap between the data commit and the business receipt: a
verified result is reused; a lost writer result stops with an uncertain intent.
It never guesses that an interrupted append failed and never retries it.
"""
from pathlib import Path
import time
import uuid
from .artifacts import digest, immutable, read, run_lock


class UncertainDatasetCommit(RuntimeError):
    pass


class DatasetCommit:
    def __init__(self, directory, identity, uri, *, mode='append'):
        from ..lance.storage import normalize_lance_uri
        uri = normalize_lance_uri(str(uri))
        self.directory = Path(directory) / digest([str(uri), identity])
        self.intent = {'identity': identity, 'uri': str(uri), 'mode': mode}
        self.output = None
        self._lock = None
        self._entered = False

    def __enter__(self):
        if self.intent['mode'] != 'append':
            self._entered = True
            return self
        self._lock = run_lock(self.directory)
        self._lock.__enter__()
        try:
            intent_path = self.directory / 'intent.json'
            complete = self.directory / 'committed.json'
            if intent_path.exists():
                if {k: read(intent_path)[k] for k in self.intent} != self.intent:
                    raise ValueError('Append intent differs')
                if not complete.exists():
                    raise UncertainDatasetCommit(
                        'Append result is uncertain; inspect the target and intent before recovery: '
                        + str(intent_path))
                receipt = read(complete)
                self.output = {'uri': receipt['uri'], 'version': receipt['committed_version']}
            else:
                immutable(intent_path, {**self.intent, 'base_version': self._current_version()})
            self._entered = True
            return self
        except BaseException:
            self._lock.__exit__(None, None, None)
            self._lock = None
            raise

    def confirm(self, receipt):
        if not self._entered:
            raise RuntimeError('Use DatasetCommit as a context manager')
        if receipt.status != 'committed' or receipt.uri != self.intent['uri']:
            raise UncertainDatasetCommit('Writer did not return a verified commit: ' + str(receipt))
        if self.intent['mode'] == 'append':
            immutable(self.directory / 'committed.json', receipt.to_dict())
        self.output = {'uri': receipt.uri, 'version': receipt.committed_version}
        return self.output

    def __exit__(self, *args):
        self._entered = False
        if self._lock is not None:
            return self._lock.__exit__(*args)

    def _current_version(self):
        from urllib.parse import urlsplit
        if urlsplit(self.intent['uri']).scheme not in ('', 'file'):
            raise ValueError('Durable append intents currently require a local filesystem target')
        from ..lance.storage import resolve_local_uri
        path = resolve_local_uri(self.intent['uri'])
        if not path.exists():
            return None
        import lance
        return lance.dataset(str(path)).version

    def abort_uncommitted(self, *, actor, reason):
        """Explicit operator recovery after establishing the old writer stopped.

        Allowed only when the target still has its pre-write version (including
        no table). Any new version or a verified receipt blocks this operation.
        The intent is archived before another attempt can be reserved.
        """
        if not actor.strip() or not reason.strip():
            raise ValueError('actor and reason are required')
        with run_lock(self.directory):
            if (self.directory / 'committed.json').exists():
                raise ValueError('Cannot abort a committed write')
            path = self.directory / 'intent.json'
            intent = read(path)
            if {k: intent[k] for k in self.intent} != self.intent:
                raise ValueError('Append intent differs')
            if 'base_version' not in intent or self._current_version() != intent['base_version']:
                raise UncertainDatasetCommit('Target changed; cannot prove that append was uncommitted')
            operation_id = uuid.uuid4().hex
            receipt = {'operation_id': operation_id, 'actor': actor, 'reason': reason,
                       'time': time.time(), 'intent': intent}
            immutable(self.directory / 'recovery' / (operation_id + '.json'), receipt)
            path.unlink()
            return receipt
