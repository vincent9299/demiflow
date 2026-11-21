"""Explicit deployment grants; independent of registry allow_runs."""
from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
import copy
from pathlib import Path
import os

from demiflow.workbench.conversations.common import integer
from demiflow.workbench.conversations.common import plain_path
from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import strict_json
from demiflow.workbench.conversations.common import text
from demiflow.workbench.conversations.execution import execution_facts
from demiflow.workbench.conversations.versions import is_qualified

PROXY_ENVIRONMENT = ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'NO_PROXY', 'no_proxy')


def proxy_environment(config):
    """Explicit transport opt-in; proxy values never enter persisted policy."""
    if not config.get('inherit_proxy'):
        return {}
    return {name: os.environ[name] for name in PROXY_ENVIRONMENT if name in os.environ}


@dataclass(frozen=True)
class Limits:
    active: int = 2
    sessions: int = 256
    turns: int = 128
    event_bytes: int = 65536
    events_per_turn: int = 20000
    output_bytes: int = 16 * 1024**2
    session_bytes: int = 64 * 1024**2
    project_bytes: int = 256 * 1024**2
    global_bytes: int = 512 * 1024**2
    database_bytes: int = 768 * 1024**2
    control_reserve: int = 16 * 1024**2
    provider_bytes: int = 256 * 1024**2
    private_total_bytes: int = 512 * 1024**2
    turn_seconds: int = 900
    input_seconds: int = 600
    rss_bytes: int = 2 * 1024**3
    processes: int = 32
    frame_bytes: int = 1024**2
    stderr_bytes: int = 256 * 1024
    context_bytes: int = 128 * 1024
    context_total: int = 512 * 1024

    @classmethod
    def parse(cls, value):
        defaults = asdict(cls())
        if type(value) is not dict or value.keys() - defaults.keys():
            reject('Unknown development resource limit')
        # Keep the default prompt small; deployments may explicitly reserve a
        # larger bounded prompt for real projects with long inherited guidance.
        ceilings = {**defaults, 'context_bytes': 512 * 1024}
        for key, number in value.items():
            integer(number, 1, ceilings[key])
        result = cls(**value)
        if result.context_bytes > result.context_total:
            reject('Initial development context exceeds total context budget')
        if result.database_bytes < 128 * 1024:
            reject('Development database budget must cover at least 32 SQLite pages')
        if not result.output_bytes <= result.session_bytes <= result.project_bytes <= result.global_bytes:
            reject('Development byte budgets are inconsistent')
        return result


class Deployment:
    def __init__(self, value):
        required = {'format', 'mode', 'state_root', 'projects', 'providers', 'host_budget'}
        if type(value) is not dict or not required <= value.keys() or value.keys() - required - {'limits', 'runtime_domain'}:
            reject('Invalid development deployment fields')
        if value['format'] != 'demiflow_development_deployment_v1' or value['mode'] not in {'restricted', 'trusted_local'}:
            reject('Unsupported development deployment')
        value = copy.deepcopy(value)
        self.value = value
        self.mode = value['mode']
        self.limits = Limits.parse(value.get('limits', {}))
        self.state = Path(text(value['state_root'], 2048)).absolute()
        if self.state != self.state.resolve():
            reject('State path must not traverse symbolic links')
        self.projects, self.providers = value['projects'], value['providers']
        if type(self.projects) is not dict or not 1 <= len(self.projects) <= 128:
            reject('Configure 1..128 explicitly permitted projects')
        for key, project in self.projects.items():
            required = {'root', 'kind', 'phase', 'writes', 'watch', 'sources', 'non_goals', 'authorization'}
            optional = {'protected'}
            if type(project) is not dict or not required <= project.keys() or project.keys() - required - optional:
                reject('Invalid project development grant')
            project['identity'] = root_identity(project['root'])
            root = Path(project['root'])
            if self.state == root or root in self.state.parents or self.state in root.parents:
                reject('Development state and project roots must be disjoint')
            if self.mode == 'restricted' and value.get('runtime_domain'):
                domain = Path(value['runtime_domain']).absolute()
                if (domain == root or domain in root.parents or root in domain.parents or
                        domain == self.state or domain in self.state.parents or self.state in domain.parents):
                    reject('Restricted runtime domain, private state and project roots must be disjoint')
            if project['kind'] not in {'platform', 'pipeline'}:
                reject('Explicit development kind required')
            if project['phase'] not in {'design', 'implementation'}:
                reject('Explicit development phase required')
            text(project['authorization'], 4096)
            for name in ('writes', 'watch', 'sources', 'non_goals'):
                if type(project[name]) is not list or len(project[name]) > 128:
                    reject('Scope list exceeds budget')
            for goal in project['non_goals']: text(goal, 4096)
            if project['phase'] == 'design' and any(not p.endswith('.md') for p in project['writes']):
                reject('Design phase permits only explicitly scoped Markdown documents')
            for path in project['writes'] + project['watch'] + project['sources']:
                safe_path(root, path, missing=True)
            for path in project['writes']:
                parts = plain_path(path).split('/')
                if any(p.startswith('.') for p in parts) or parts[-1] in {'AGENTS.md', 'workspace.json', 'PIPELINE_SPEC.md'}:
                    reject('Control files require a separate development grant')
                if not any(path == w or path.startswith(w + '/') for w in project['watch']):
                    reject('Writable scope must be watched')
            protected = project.get('protected', [])
            if type(protected) is not list or len(protected) > 128:
                reject('Protected source list exceeds budget')
            for name in protected: safe_path(root, name, missing=True)
        if type(self.providers) is not dict or not 1 <= len(self.providers) <= 2:
            reject('Configure at most two providers')
        for name, provider in self.providers.items():
            if name not in {'codex', 'deepseek'} or type(provider) is not dict:
                reject('Unsupported provider')
            required = {'executable', 'version', 'model'}
            if not required <= provider.keys() or provider.keys() - required - {'model_provider', 'base_url', 'credential_env', 'native_credentials', 'inherit_proxy', 'effort', 'node'}:
                reject('Invalid provider configuration')
            if type(provider.get('inherit_proxy', False)) is not bool:
                reject('Proxy inheritance must be explicitly true or false')
            path = Path(provider['executable'])
            if not path.is_absolute() or not path.is_file():
                reject('Provider executable must be an installed absolute path')
            if not is_qualified(name, provider['version']):
                reject('Provider version has not been qualified')
            text(provider['model'], 128)
            if provider.get('native_credentials'):
                if name != 'deepseek' or provider.get('credential_env'):
                    reject('Native credentials require one explicit DeepSeek authentication source')
                credential = Path(text(provider['native_credentials'], 2048))
                # Only metadata is inspected here. The installed CLI resolves
                # its own credentials; no secret enters our config or database.
                if not credential.is_absolute() or credential != credential.resolve(strict=True):
                    reject('Native credential reference must be a physical absolute path')
                info = credential.stat()
                if not credential.is_file() or info.st_mode & 0o077 or info.st_size > 65536:
                    reject('Native credential reference must be a private bounded file')
                for project in self.projects.values():
                    root = Path(project['root'])
                    if root == credential or root in credential.parents:
                        reject('Native credentials must be outside development projects')
            if name == 'deepseek' and provider.get('model_provider', 'deepseek-official') not in {'deepseek-official', 'deepseek-account'}:
                reject('Unsupported DeepSeek authentication route')
            if provider.get('effort', 'xhigh' if name == 'codex' else 'high') not in ({'low', 'medium', 'high', 'xhigh'} if name == 'codex' else {'off', 'low', 'high', 'max'}):
                reject('Unsupported reasoning effort')

        self._host_budget()
        self.runtime_partition = self.validate_runtime()

    def _host_budget(self):
        b = self.value['host_budget']
        expected = {'memory_bytes', 'run_memory_bytes', 'other_memory_bytes', 'disk_bytes', 'run_disk_bytes', 'other_disk_bytes', 'device_path'}
        if type(b) is not dict or set(b) != expected:
            reject('A fixed run/chat/other host budget partition is required')
        for key in expected - {'device_path'}:
            integer(b[key], 0, 2**60)
        l = self.limits
        # SQLite rollback can approach database size; provider scratch is sampled.
        self.memory_reservation = l.active * (l.rss_bytes + 64 * 1024**2) + 64 * 1024**2
        # Bounded policy snapshots, control directory and SQLite metadata have
        # their own 4 MiB allowance beyond the emergency preallocated reserve.
        self.disk_reservation = 2 * l.database_bytes + l.private_total_bytes + l.control_reserve + 4 * 1024**2
        if b['run_memory_bytes'] + b['other_memory_bytes'] + self.memory_reservation > b['memory_bytes']:
            reject('Combined run/development memory reservation exceeds host partition')
        if b['run_disk_bytes'] + b['other_disk_bytes'] + self.disk_reservation > b['disk_bytes']:
            reject('Combined run/development disk reservation exceeds host partition')
        if not Path(b['device_path']).exists():
            reject('Disk device accounting path must exist')

    @classmethod
    def load(cls, path):
        return cls(strict_json(read_file(path, 65536)))

    def validate_runtime(self, *, required=False, admit=False):
        domain = self.value.get('runtime_domain')
        if domain is None:
            if required: reject('Combined service requires an explicit runtime_domain and shared host partition')
            return None
        from demiflow.workbench import host_partition
        path = Path(text(domain, 2048))
        if not path.is_absolute() or path != path.resolve(): reject('Runtime domain must be a physical absolute path')
        partition = host_partition.load(path, host_partition.policy(path), required=True, admit=admit)
        configured = partition['value']; device = partition['devices'][partition['development_device']]
        if str(self.state) != configured['development_state_root']:
            reject('Development state differs from the single fixed host partition owner')
        if (self.memory_reservation > configured['development_memory_bytes'] or
                self.disk_reservation > configured['development_disk_bytes']):
            reject('Development limits exceed the shared host reservation', 'resource_limit')
        expected = dict(memory_bytes=configured['memory_bytes'], run_memory_bytes=partition['run_memory_bytes'],
            other_memory_bytes=configured['other_memory_bytes'], disk_bytes=device['bytes'],
            run_disk_bytes=partition['run_disk_bytes'], other_disk_bytes=device['other_bytes'])
        if any(self.value['host_budget'][k] != v for k, v in expected.items()):
            reject('Development host budget differs from actual runtime/history partition', 'conflict')
        if Path(self.value['host_budget']['device_path']).stat().st_dev != partition['development_device']:
            reject('Development host device differs from shared partition')
        return partition

    def project(self, workspace, root):
        project = self.projects.get(workspace)
        if project is None:
            reject('Development is not enabled for this project')
        if root_identity(root) != project['identity']:
            reject('Registered development root identity changed', 'conflict')
        return project

    def capabilities(self, workspace, root):
        try:
            project = self.project(workspace, root)
            permitted = True
        except ValueError:
            project = {}
            permitted = False
        execution = execution_facts(self.mode)
        from .model_broker import capabilities as model_capabilities
        providers = []
        for name, config in self.providers.items():
            model = model_capabilities(name, config)
            ready = execution['available'] and (self.mode != 'restricted' or model['available'])
            reason = model['reason'] if self.mode == 'restricted' and not model['available'] else ''
            providers.append(dict(id=name, version=config['version'], model=config['model'], available=permitted and ready,
                **({'effort': config['effort']} if 'effort' in config else {}),
                compaction='native', input_requests='supported' if name == 'codex' else 'unsupported',
                history_replay='supported' if name == 'codex' else 'unsupported',
                reason=reason))
        available = any(p['available'] for p in providers)
        return dict(format='demiflow_development_chat_v1', enabled=permitted, start=available,
            conversation_stats=True, multiple_conversations=True, notebook_cells=self.mode == 'restricted',
            mode=self.mode, filesystem_enforcement=execution['filesystem_enforcement'], reason=execution['reason'],
            providers=providers,
            limits=asdict(self.limits), host_partition=dict(self.value['host_budget'],
                development_memory_bytes=self.memory_reservation, development_disk_bytes=self.disk_reservation),
            guarantees=dict(resource_protection='bytes_and_counts_atomic; process_tree_RSS_and_disk_sampled',
                            provider_internal_requests='root_turn_transport_max_64' if self.mode == 'restricted' else 'unknown',
                            cost_cap='unsupported'))
