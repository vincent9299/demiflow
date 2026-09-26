"""Lance run records and stage commits; execution is the native Dataset API.

The application provides a row schema and pure row encoders. This class stores
run identity, stage references and revisions; it does not schedule business steps.
"""
from pathlib import Path
from .records import LanceRecordStore
from .refs import DatasetRef
from .registry import Catalog
from .checkpoint import read_checkpoint_record
from .storage import schema_hash
from ..execution.artifacts import digest
from ..data.api import DataAPI


class LanceRun:
    def initialize(self, *, root, relative, manifest, schema, schema_name, schema_version,
                   encode_row, decode_row):
        self.storage_root, self.relative = Path(root), relative
        self.row_schema, self.schema_name, self.schema_version = schema, schema_name, schema_version
        self.encode_row, self.decode_row = encode_row, decode_row
        self.records = LanceRecordStore(root, relative + '/metadata.lance')
        self.records.put('manifest', manifest)
        self.manifest = manifest
        self.previous = self.version = digest(manifest)
        self.stages, self.reused, self.new = {}, [], []

    def checkpoint_lance_args(self, name, extra=None):
        if not name or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in name):
            raise ValueError('stage name must be a safe identifier')
        upstream = self.previous
        version = digest({'upstream': upstream, 'stage': name, 'extra': extra,
                          'implementation': self.manifest.get('implementation')})
        relative = self.relative + '/checkpoints/' + name + '/' + version + '.lance'
        uri = str(self.storage_root / relative)
        record = read_checkpoint_record(uri)
        if record is not None and record['fingerprint'] != version:
            raise ValueError('checkpoint fingerprint drift')
        (self.reused if record is not None else self.new).append(name)
        self.stages[name] = {'version': version, 'fingerprint': version,
                             'upstream_identity': upstream, 'relative_uri': relative}
        self.previous = {'stage': name, 'stage_version': version}
        return {'uri': uri, 'relative_uri': relative, 'fingerprint': version,
                'version': version, 'replayed': record is not None}

    def commit_stage_ref(self, name, args, committed_version, row_count):
        ref = DatasetRef(args['relative_uri'][:-6], args['relative_uri'], committed_version,
                         self.schema_name, self.schema_version, schema_hash(self.row_schema), row_count)
        Catalog(self.storage_root).register(ref)
        self.stages[name]['dataset_ref'] = ref.to_dict()
        return ref

    def lance_checkpoint(self, dataset, name, extra=None, row_transform=None):
        args = self.checkpoint_lance_args(name, extra)
        upstream = self.stages[name]['upstream_identity']
        transform = row_transform or (lambda row: row)
        prepared = dataset.map(lambda row: self.encode_row(transform(row), name, upstream, 0))
        result = prepared.checkpoint_lance(args['uri'], schema=self.row_schema,
                                          fingerprint=args['fingerprint'])
        record = read_checkpoint_record(args['uri'])
        self.commit_stage_ref(name, args, record['committed_version'], record['row_count'])
        self.records.put('revision/' + args['version'], {'stages': self.stages})
        return result.map(self.decode_row)

    def replay_stage(self, name, data=None):
        ref = DatasetRef.from_dict(self.stages[name]['dataset_ref'])
        return (data or DataAPI()).read_lance(ref.resolve(self.storage_root),
                        version=ref.lance_version).map(self.decode_row)

    def finish(self):
        state = {'branch': self.branch, 'stages': self.stages,
                 'reused_stages': self.reused, 'new_stages': self.new}
        self.records.put('latest', state, immutable=False)
        return state
