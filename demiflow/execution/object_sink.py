"""Run-owned objects using the existing Datasink/RunJournal commit protocol.

This adapter stages binary artifacts, not model requests/results. It has no
HTTP, image execution, scheduling or recovery engine. The first slice owns
    only this run's namespace under a stable logical object destination.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid
import threading

from ..data.datasink import Datasink
from ..objects import LocalObjectStore
from .block_sink import BlockSink, storage_usage
from .recovery_contract import RecoveryContractError, UnresolvedCommit, document, identifier, identity
from .state_artifacts import atomic_bytes


def fault_point(name, commit_id):
    """Internal test seam, never a caller-selected recovery mode."""


def image_request_budget(action):
    return 'image-requests-'+identity(action.action_id)


def check_image_replay(action):
    """One action watermark in the existing prepared artifact descriptor.

    Legacy participants use the action watermark. Native row-local prompt
    participants reconcile each occurrence through the existing model journal.
    """
    if getattr(action, "native_image_effects", False):
        return  # Each occurrence is reconciled from the same native commit journal.
    with action.context._object_resource_lock:
        if not getattr(action,'replaying',False) or getattr(action,'_image_replay_checked',False):return
        confirmed=0
        for fact in action.context.journal.execution_commits(action.action_id):
            if fact['prepared'] is None:continue
            for prepared in fact['prepared']['sinks'].values():
                objects=prepared.get('objects',{})
                if type(objects) is dict:confirmed=max(confirmed,objects.get('image_requests',0))
        charged=action.context.journal.budget_consumed(image_request_budget(action))
        if charged!=confirmed:
            raise UnresolvedCommit('Image external result is unresolved; automatic regeneration is forbidden')
        action._image_replay_checked=True


class ObjectSink(Datasink):
    """One action's immutable blobs; bytes and metadata have separate quotas."""
    def __init__(self, action, *, logical_root=None, sink_id='objects', max_object_bytes=32*1024**2, max_objects=128,
                 object_action_id=None):
        self.action, self.run = action, action.context
        self.sink_id = identifier(sink_id)
        if type(max_object_bytes) is not int or not 1 <= max_object_bytes <= 64*1024**2:
            raise RecoveryContractError('Object byte ceiling must be 1..64 MiB')
        if type(max_objects) is not int or not 1 <= max_objects <= 1024:
            raise RecoveryContractError('Object manifest count exceeds ceiling')
        self.maximum, self.count = max_object_bytes, max_objects
        from .object_resources import namespace_record
        self.resource=namespace_record(self.run,logical_root or self.run.directory/'objects')
        owner = object_action_id or action.action_id
        if owner not in {action.action_id, action.action_id + '-stream'}:
            raise RecoveryContractError('Object owner must be the action or its native stream child')
        self.root=Path(self.resource['physical_root'])/owner/sink_id
        self.store = LocalObjectStore(self.root)
        if self.store.directory != self.root:
            raise RecoveryContractError('Object write root must not use relocation or symlinks')
        self.objects = {}
        self.manifest = None
        self._objects_lock=threading.RLock()
        from collections import OrderedDict
        self._synced_directories = OrderedDict()

    def recovery_identity(self):
        return dict(format=1, logical_root=self.resource['logical_root'], root=self._name(self.root),
                    max_object_bytes=self.maximum, max_objects=self.count)

    def _name(self,path):
        return str(Path('materializations')/('@objects-'+self.resource['root_id'])/Path(path).relative_to(self.resource['physical_root']))

    def write(self, blocks, context):
        raise RecoveryContractError('Objects require the native commit protocol')

    def reserve_capacity(self,maximum,byte_limit,node_id):
        """Conservative admission; actual bytes remain separately metered."""
        self.run.reserve('output_admission_bytes',maximum*byte_limit,
            reservation_id='images-'+identity(node_id),ceiling=self.run.limits['max_output_bytes'])
        self.run.reserve('image_scratch_admission_bytes',maximum*(3*byte_limit+65536)+1024,
            reservation_id='images-'+identity(node_id),ceiling=self.run.limits['max_scratch_bytes'])
        storage_usage(self.run)

    def stage(self, raw, *, admitted=False):
        self.run.check_stop()
        if type(raw) is not bytes or len(raw) > self.maximum:
            raise RecoveryContractError('Object exceeds its encoded byte ceiling')
        self.run.object_namespace(self.resource['logical_root'])
        sha = hashlib.sha256(raw).hexdigest()
        ref = self.store.reference(sha)
        target = self.root / sha[:2] / sha
        fact = dict(path=self._name(target), bytes=len(raw), sha256=sha)
        # Do not adopt files created by another producer, even if they happen
        # to have the same content name inside this run's directory.
        with self.run.journal.execution_guard(self.run.fence) as db:
            owned = db.execute('SELECT 1 FROM runtime_artifacts WHERE path=?', (fact['path'],)).fetchone()
            if target.exists() and owned is None:
                raise RecoveryContractError('Object path is not owned by this run')
        self.run.reserve_output('object_bytes', len(raw), reservation_id='object-' + identity(fact),admitted=admitted)
        if target.exists():
            self._verify(fact)
            self._record_object(sha, fact)
            return ref.to_dict()
        temporary = target.parent / ('.pending-' + uuid.uuid4().hex)
        self.run.allocate_artifact(target, size=len(raw), atomic=False)
        self.run.allocate_artifact(temporary, size=len(raw), atomic=False)
        with self.run.journal.publication_guard(self.run.fence):
            if any(p.is_symlink() for p in (target, *target.parents)):
                raise RecoveryContractError('Object path contains a symlink')
            try:
                self.store.put(raw, sha256=sha, temporary=temporary, max_bytes=len(raw))
            except ValueError as exc:
                raise RecoveryContractError('Object publication content differs') from exc
            self._verify(fact)
            from .object_resources import sync_directories
            sync_directories(self.resource,target.parent, synced=self._synced_directories)
        self.run.release_temporaries([temporary, target.with_name(target.name + ".staging")])
        self._record_object(sha, fact)
        return ref.to_dict()

    def _record_object(self, sha, fact):
        with self._objects_lock:
            if sha not in self.objects and len(self.objects) >= min(self.count, 256):
                self._flush_page()
            self.objects[sha] = fact

    def adopt(self, prepared):
        """Carry durable child references into the final publication receipt."""
        self._validate(prepared)
        from .artifact_manifest import entries
        for fact in entries(self.run, prepared['objects']):
            self._record_object(fact['sha256'], fact)

    def _verify(self, fact):
        if type(fact) is not dict or set(fact) != {'path', 'bytes', 'sha256'}:
            raise RecoveryContractError('Invalid object descriptor')
        sha, size = fact['sha256'], fact['bytes']
        try:
            self.store.reference(sha)
        except (TypeError, ValueError) as exc:
            raise RecoveryContractError('Invalid object checksum') from exc
        expected = self.root / sha[:2] / sha
        if fact['path'] != self._name(expected):
            raise RecoveryContractError('Object reference escapes its owner')
        if type(size) is not int or not 0 <= size <= self.maximum:
            raise RecoveryContractError('Object descriptor exceeds byte ceiling')
        # Held directory descriptors prevent parent substitution into symlinks.
        parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in expected.parts[1:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                os.close(parent)
                parent = child
            descriptor = os.open(expected.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        except OSError as exc:
            raise UnresolvedCommit('Object is missing or inaccessible') from exc
        finally:
            os.close(parent)
        with os.fdopen(descriptor, 'rb') as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise RecoveryContractError('Object size differs')
            digest, consumed = hashlib.sha256(), 0
            while chunk := source.read(min(65536, size-consumed+1)):
                consumed += len(chunk)
                if consumed > size:
                    raise RecoveryContractError('Object size changed during inspection')
                digest.update(chunk)
            after = os.fstat(source.fileno())
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise RecoveryContractError('Object changed during inspection')
        if consumed != size or digest.hexdigest() != sha:
            raise RecoveryContractError('Object checksum differs')

    def _validate(self, prepared):
        from .object_resources import inspect_namespace
        inspect_namespace(self.resource)
        required = {'format', 'logical_root', 'root', 'max_object_bytes', 'max_objects', 'objects','image_requests'}
        if type(prepared) is not dict or set(prepared) != required:
            raise RecoveryContractError('Invalid prepared object set')
        if {k: v for k, v in prepared.items() if k not in {'objects','image_requests'}} != self.recovery_identity():
            raise RecoveryContractError('Prepared object policy differs')
        if type(prepared['image_requests']) is not int or not 0<=prepared['image_requests']<=self.run.limits['max_requests']:
            raise RecoveryContractError('Invalid external request watermark')
        values = prepared['objects']
        if type(values) is list and len(values) > self.count:
            raise RecoveryContractError('Prepared object count exceeds ceiling')
        document(prepared)
        from .artifact_manifest import manifest_pages
        pages = [(None, values)] if type(values) is list else manifest_pages(self.run.directory, values,
            maximum=self.run.limits['max_artifacts'])
        for _, page in pages:
            if len(page) > self.count:
                raise RecoveryContractError('Prepared object page exceeds ceiling')
            hashes = []
            for fact in page:
                self._verify(fact)
                hashes.append(fact['sha256'])
            if hashes != sorted(set(hashes)):
                raise RecoveryContractError('Object manifest identity is not canonical')

    def _flush_page(self):
        from .artifact_manifest import ArtifactManifest
        if self.manifest is None:
            self.manifest = ArtifactManifest(self.run,
                self.run.directory / 'materializations' / self.action.action_id / (self.sink_id + '-index'))
        if self.objects:
            self.manifest.append([self.objects[k] for k in sorted(self.objects)])
            self.objects = {}

    def _descriptor(self):
        if self.manifest is None:
            return [self.objects[k] for k in sorted(self.objects)]
        self._flush_page()
        return self.manifest.seal()

    def _marker(self, context):
        return self.root / '.receipts' / (identifier(context.commit_id) + '.json')

    def prepare(self, blocks, context):
        self.run.object_namespace(self.resource['logical_root'])
        if blocks is not None:
            for raw in blocks:
                self.stage(raw)
        with self._objects_lock:
            prepared = {**self.recovery_identity(), 'objects': self._descriptor(),
                        'image_requests':getattr(self, 'image_requests', None)}
            if prepared['image_requests'] is None:
                prepared['image_requests'] = self.run.journal.budget_consumed(image_request_budget(self.action))
            # Seal transfers this generation. Later producers append to fresh
            # pages; reset of this receipt cannot erase their equal-hash rows.
            self.objects, self.manifest = {}, None
        self._validate(prepared)
        self.run.reserve('object_metadata_bytes', len(document(prepared)),
            reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id}),
            ceiling=self.run.limits['max_control_bytes'])
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return prepared

    def inspect(self, prepared, context):
        self._validate(prepared)
        marker = self._marker(context)
        try:
            if any(p.is_symlink() for p in (marker, *marker.parents)):
                raise UnresolvedCommit('Object receipt path contains a symlink')
            with marker.open('rb') as stream:
                raw = stream.read(1024**2+1)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise UnresolvedCommit('Object receipt is inaccessible') from exc
        if len(raw) > 1024**2:
            raise UnresolvedCommit('Object receipt exceeds metadata ceiling')
        expected = dict(commit_id=context.commit_id, sink_id=self.sink_id, status='committed', data=prepared)
        try:
            value = json.loads(raw)
        except ValueError as exc:
            raise UnresolvedCommit('Object receipt is corrupt') from exc
        if value != expected:
            raise UnresolvedCommit('Object receipt identity differs')
        return value

    def commit(self, prepared, context):
        existing = self.inspect(prepared, context)
        if existing is not None:
            return existing
        receipt = dict(commit_id=context.commit_id, sink_id=self.sink_id, status='committed', data=prepared)
        raw = document(receipt)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id, size=len(raw))
        atomic_bytes(self._marker(context), raw)
        from .object_resources import sync_directories
        sync_directories(self.resource,self._marker(context).parent, synced=self._synced_directories)
        return receipt

    def abort(self, prepared, context):
        # Execution cleanup is intentionally centralized. Only after all started
        # I/O has drained can RunContext.clean_orphans reclaim registered files;
        # prepared/committed references protect shared hashes within this run.
        self._validate(prepared)

    def detach(self):
        from copy import copy
        with self._objects_lock:
            sealed = copy(self)
            sealed._objects_lock = threading.RLock()
            sealed.image_requests = self.run.journal.budget_consumed(image_request_budget(self.action))
            self.objects = {}
            self.manifest = None
            return sealed

    def reset(self,prepared=None):
        with self._objects_lock:
            if prepared is None:
                self.objects={}
                self.manifest=None



def image_object_specs(plan):
    """Native resource declarations; no clients or filesystem writes."""
    from ..operator_llm.native_actor import is_native_prompt_actor
    from ..operator_llm.media import media_limits
    specs={}
    for operation in plan.operations:
        target=getattr(getattr(operation,'callable',None),'target',None)
        target=getattr(target,'__self__',target)
        if is_native_prompt_actor(target) and target._prompt.response_format == 'image':
            options=target._runtime.options
            root=options['object_store']
            specs[root]=max(specs.get(root,0),media_limits(options)['max_image_bytes'])
    return specs


class ObjectRowsSink(Datasink):
    """A single native participant: objects publish before their row references."""
    def __init__(self, action, *, object_specs=None, rows_sink=None, input_factory=None,
                 object_action_id=None, **object_limits):
        self.rows = rows_sink or BlockSink(action,'rows')
        self.sink_id=getattr(rows_sink,'sink_id','output')
        self.input_factory=input_factory
        object_action_id = object_action_id or (action.action_id + '-stream' if input_factory is not None else None)
        self.object_sinks={}
        if object_specs:
            for index,(root,maximum) in enumerate(sorted(object_specs.items())):
                self.object_sinks[root]=ObjectSink(action,logical_root=root,max_object_bytes=maximum,
                    max_objects=1024,sink_id='objects' if index==0 else 'objects-'+str(index), object_action_id=object_action_id)
            self.objects=next(iter(self.object_sinks.values()))
        else:
            self.objects=ObjectSink(action,object_action_id=object_action_id,**object_limits)
            self.object_sinks[self.objects.resource['logical_root']]=self.objects

    @classmethod
    def for_inspection(cls, action, rows_sink, prepared):
        """Rebuild only native inspection participants from bounded run facts.

        Forge observes the same objects/rows receipt as the driver. It neither
        claims a namespace nor prepares/publishes artifacts during projection.
        Every object location must already belong to this run's registry.
        """
        if type(prepared) is not dict or set(prepared)-{'additional_objects'}!={'rows','objects'}:
            raise RecoveryContractError('Invalid object/rows preparation')
        document(prepared)
        more=prepared.get('additional_objects',{})
        if type(more) is not dict or len(more)>63 or any(type(k) is not str for k in more):
            raise RecoveryContractError('Invalid bounded object resource set')
        parts=[prepared['objects']]+[more[k] for k in sorted(more)]
        registered={r['logical_root']:r for r in action.context.journal.object_roots()}
        from .object_resources import namespace_record
        names=[]
        owners=[]
        for index,part in enumerate(parts):
            if type(part) is not dict or type(part.get('logical_root')) is not str:
                raise RecoveryContractError('Invalid prepared object resource')
            name=part['logical_root'];names.append(name)
            if index and name!=sorted(more)[index-1]:
                raise RecoveryContractError('Additional object resource identity differs')
            record=namespace_record(action.context,name)
            if name!=record['logical_root'] or registered.get(name)!=record:
                raise UnresolvedCommit('Object inspection requires its registered run namespace')
            root_parts = Path(part.get('root', '')).parts
            if len(root_parts) != 4 or root_parts[2] not in {action.action_id, action.action_id + '-stream'}:
                raise UnresolvedCommit('Prepared objects have an unrelated action owner')
            owner = root_parts[2]
            if owner != action.action_id:
                child = action.context.journal.execution_action(owner)
                if child is None or child['declaration'].get('parent') != action.semantic_id:
                    raise UnresolvedCommit('Prepared objects have no bound native stream child')
            owners.append(owner)
        if names!=sorted(set(names)):
            raise RecoveryContractError('Object resource order or identity differs')
        def limits(part):
            return dict(logical_root=part['logical_root'],max_object_bytes=part.get('max_object_bytes'),
                        max_objects=part.get('max_objects'))
        sink=cls(action,rows_sink=rows_sink,object_action_id=owners[0],**limits(parts[0]))
        for index,part in enumerate(parts[1:],1):
            sink.object_sinks[part['logical_root']]=ObjectSink(action,sink_id='objects-'+str(index),object_action_id=owners[index],**limits(part))
        return sink

    def append(self,row):
        return self.rows.append(row)

    def reset(self,prepared=None):
        self.rows.reset()
        for key,store in self.object_sinks.items():
            part=(prepared['objects'] if store is self.objects else prepared['additional_objects'][key]) if prepared else None
            store.reset(part)

    def recovery_identity(self):
        return dict(format=1, objects=self.objects.recovery_identity(), rows='native-blocks')

    def write(self, blocks, context):
        raise RecoveryContractError('Object rows require the native commit protocol')

    def prepare(self, blocks, context):
        # Row production may stage real objects; both descriptors become durable
        # in one existing prepared record before either publication receipt.
        if self.input_factory is not None:blocks=self.input_factory(context)
        rows = self.rows.prepare(blocks, context)
        objects = self.objects.prepare(None, context)
        prepared=dict(rows=rows,objects=objects)
        more={key:store.prepare(None,context) for key,store in self.object_sinks.items() if store is not self.objects}
        if more:prepared['additional_objects']=more
        return prepared

    def _parts(self, prepared, context):
        if type(prepared) is not dict or set(prepared)-{'additional_objects'} != {'rows', 'objects'}:
            raise RecoveryContractError('Invalid object/rows preparation')
        objects = self.objects.inspect(prepared['objects'], context)
        rows = self.rows.inspect(prepared['rows'], context)
        expected={key:store for key,store in self.object_sinks.items() if store is not self.objects}
        if set(prepared.get('additional_objects',{}))!=set(expected):
            raise RecoveryContractError('Object resource set changed')
        more={key:store.inspect(prepared['additional_objects'][key],context) for key,store in expected.items()}
        if rows is not None and (objects is None or any(v is None for v in more.values())):
            raise UnresolvedCommit('Rows exist without their object publication receipt')
        return objects, rows,more

    def _receipt(self, objects, rows, context, more):
        data=dict(objects=objects['data'],rows=rows['data'])
        if more:data['additional_objects']={k:v['data'] for k,v in more.items()}
        return dict(commit_id=context.commit_id,sink_id=self.sink_id,status='committed',data=data)

    def inspect(self, prepared, context):
        objects, rows,more = self._parts(prepared, context)
        return self._receipt(objects,rows,context,more) if objects is not None and rows is not None and all(v is not None for v in more.values()) else None

    def commit(self, prepared, context):
        objects, rows,more = self._parts(prepared, context)
        if objects is None:
            objects = self.objects.commit(prepared['objects'], context)
        if self.objects.inspect(prepared['objects'], context) != objects:
            raise UnresolvedCommit('Object publication returned without inspect evidence')
        for key,receipt in more.items():
            store=self.object_sinks[key]
            if receipt is None:receipt=store.commit(prepared['additional_objects'][key],context)
            if store.inspect(prepared['additional_objects'][key],context)!=receipt:
                raise UnresolvedCommit('Object publication returned without inspect evidence')
            more[key]=receipt
        fault_point('objects_committed_before_rows', context.commit_id)
        if rows is None:
            rows = self.rows.commit(prepared['rows'], context)
        fault_point('rows_committed_before_return', context.commit_id)
        return self._receipt(objects,rows,context,more)

    def abort(self, prepared, context):
        self.objects.abort(prepared['objects'], context)
