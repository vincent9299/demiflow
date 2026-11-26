"""One private connection configuration; project execution grants stay immutable."""
from __future__ import annotations

import copy
import os
import re
from pathlib import Path
import secrets
import sys
from urllib.parse import urlsplit

from .common import digest, read_file, reject, root_identity, safe_path, strict_json, text, wire
from .policy import Deployment

FORMAT = 'demiflow_global_development_v1'
FIELDS = {'expected', 'provider', 'model', 'effort', 'authentication', 'base_url', 'api_key'}


def _atomic(path, raw):
    from demiflow.forge.io import fsync_directory, no_symlinks
    no_symlinks(path)
    temporary = path.with_name('.settings-' + secrets.token_hex(12))
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path); fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _endpoint(value):
    if not value: return ''
    text(value, 2048)
    p = urlsplit(value)
    if (p.scheme not in {'https', 'http'} or not p.hostname or p.username or p.password or p.query or p.fragment
            or (p.scheme == 'http' and p.hostname not in {'127.0.0.1', 'localhost', '::1'})):
        reject('模型地址需要 HTTPS；本机地址可使用 HTTP。', 'invalid_request')
    return value.rstrip('/')


def automatic_project(root):
    """Grant source work within the selected project, independent of its ancestors."""
    root = Path(root).absolute(); root_identity(root)
    grant = dict(root=str(root), kind='pipeline', phase='implementation', writes=[], watch=[], sources=[],
        non_goals=['修改项目外文件、运行结果、原始数据、Notebook输出和平台控制文件'],
        authorization='工作台全局 AI 开发：仅按当前会话真人要求修改本项目源代码。')
    scopes = {'README.md', 'tests', 'docs', 'parameters', 'configs', 'src', 'lib'}
    from demiflow.forge.project_rules import ROLE_DIRS
    scopes.update(ROLE_DIRS)
    with os.scandir(root) as entries:
        for count, entry in enumerate(entries):
            if count >= 4096: reject('项目目录条目过多。', 'resource_limit')
            if not entry.name.startswith('.') and entry.is_file(follow_symlinks=False) and Path(entry.name).suffix in {'.py', '.js', '.ts', '.yaml', '.yml', '.json', '.toml', '.md'}:
                scopes.add(entry.name)
    scopes.difference_update({'AGENTS.md', 'PIPELINE_SPEC.md', 'workspace.json', 'runs', 'datasets', 'data', 'notebooks', 'data_tools', 'results', 'outputs'})
    grant['watch'] = sorted(scopes)
    # Landlock write grants are additive. Grant only existing subtrees with no
    # protected descendants, so logs, notebooks and caches stay read-only.
    protected, existing = [], set()
    pending = list(scopes); seen = 0
    while pending:
        name = pending.pop(); seen += 1
        if seen > 4096: reject('项目代码范围扫描超过预算。', 'resource_limit')
        path = safe_path(root, name, missing=True)
        # Bytecode / test caches inside source dirs must not block granting write
        # access to their parent directory (which otherwise explodes writes into
        # dozens of single-file entries and blocks creating new files).
        if path.name in {'__pycache__', '.pytest_cache', '.mypy_cache'}:
            continue
        if (path.name.startswith('.') or path.name in {'AGENTS.md','PIPELINE_SPEC.md','workspace.json','data','datasets','runs','results','outputs','notebooks','data_tools','logs','node_modules','.venv'}
                or path.suffix in {'.ipynb','.lance','.parquet','.arrow','.db','.sqlite','.sqlite3'}):
            protected.append(name); continue
        if not path.exists():
            continue
        existing.add(name)
        if path.is_dir():
            with os.scandir(path) as entries:
                for entry in entries:
                    if seen + len(pending) >= 4096: reject('项目代码范围扫描超过预算。', 'resource_limit')
                    pending.append(name + '/' + entry.name)
    blocked = {parent.as_posix() for name in protected for parent in Path(name).parents}
    eligible = existing - blocked
    grant['writes'] = sorted(name for name in eligible if Path(name).parent.as_posix() not in eligible)
    if protected: grant['protected'] = sorted(protected)
    return grant


class GlobalSettings:
    def __init__(self, service, inventory):
        self.service, self.inventory = service, inventory
        self.path = service.root / 'global-development-settings.json'
        self.templates = copy.deepcopy(service.deployment.providers)
        self.bindings = {}
        self.value = self._read()
        self._load_secret(self.value)

    def _read(self):
        if not self.path.exists(): return None
        if self.path.stat().st_mode & 0o077: reject('全局 AI 设置文件必须私有。')
        value = strict_json(read_file(self.path, 16384))
        if type(value) is not dict or set(value) != {'format','provider','config','secret'} or value['format'] != FORMAT:
            reject('Invalid global development settings')
        if value['provider'] not in self.templates: reject('全局 AI 代理不在服务已安装清单中。')
        config = value['config']
        template = self.templates[value['provider']]
        if type(config) is not dict or any(config.get(k)!=template.get(k) for k in ('executable','node','version','inherit_proxy')):
            reject('全局设置不能更换已安装的代理程序。')
        candidate=copy.deepcopy(self.service.deployment.value)
        for project in candidate['projects'].values(): project.pop('identity',None)
        candidate['providers']={value['provider']:config}
        Deployment(candidate)
        return value

    def _load_secret(self, value):
        if value and value['secret']:
            name = value['secret']
            if type(name) is not str or not re.fullmatch(r'ai-key-[0-9a-f]{32}', name):
                reject('Invalid private credential reference')
            expected='DEMIFLOW_WORKBENCH_KEY_'+name[7:].upper()
            if value['config'].get('credential_env')!=expected: reject('Invalid credential environment reference')
            path = self.service.root / name
            if path.stat().st_mode & 0o077: reject('API key 文件必须私有。')
            secret = read_file(path, 4096).decode('utf-8')
            if not secret or any(c in secret for c in '\r\n\0'): reject('Invalid private credential')
            os.environ[expected] = secret

    def public(self):
        with self.service.lock:
            self.value = self._read(); self._load_secret(self.value)
            name = self.value['provider'] if self.value else next(iter(self.templates))
            config = self.value['config'] if self.value else self.templates[name]
            authentication = 'login' if config.get('native_credentials') else 'api_key'
            return dict(format=FORMAT, writable=True, revision=digest(self.value), inherited=True,
                provider=name, model=config['model'], effort=config.get('effort','high'), authentication=authentication,
                base_url=config.get('base_url','https://api.deepseek.com' if name=='deepseek' and authentication=='api_key' else ''),
                has_credentials=bool(config.get('native_credentials') or config.get('credential_env') and os.environ.get(config['credential_env'])),
                providers=[dict(id=k, label='DeepSeek' if k=='deepseek' else 'Codex',
                                native_login=bool(v.get('native_credentials')), model=v['model']) for k,v in self.templates.items()])

    def save(self, request):
        if type(request) is not dict or set(request) != FIELDS: reject('Invalid global settings fields', 'invalid_request')
        for key in FIELDS:
            if type(request[key]) is not str: reject('设置字段必须是文本。', 'invalid_request')
        name = request['provider']
        if name not in self.templates: reject('请使用服务已安装的开发代理。', 'invalid_request')
        model = text(request['model'].strip(),128)
        effort = request['effort']
        if effort not in ({'off','low','high','max'} if name=='deepseek' else {'low','medium','high','xhigh'}):
            reject('推理强度不受此代理支持。', 'invalid_request')
        key = request['api_key'].strip()
        if len(key.encode())>4096 or any(c in key for c in '\r\n\0'): reject('API key 格式不正确。', 'invalid_request')
        endpoint = _endpoint(request['base_url'].strip())
        with self.service.lock:
            previous = self._read(); self._load_secret(previous)
            if request['expected'] != digest(previous): reject('全局设置已被其他页面修改，请重新读取。', 'conflict')
            self.service._reap()
            if self.service.store.unresolved(): reject('有开发会话正在运行，配置草稿已保留；完成后再保存。', 'busy')
            config = copy.deepcopy(self.templates[name]); secret = None
            config.update(model=model, effort=effort)
            if request['authentication'] == 'login':
                if not config.get('native_credentials'): reject('此代理没有可复用的服务端登录。', 'invalid_request')
                config.pop('credential_env',None);config.pop('base_url',None)
            elif request['authentication'] == 'api_key':
                config.pop('native_credentials',None)
                if name=='deepseek': config['model_provider']='deepseek-official'
                if not endpoint: reject('请填写模型服务地址。', 'invalid_request')
                config['base_url']=endpoint
                if key:
                    secret = 'ai-key-' + secrets.token_hex(16)
                    config['credential_env']='DEMIFLOW_WORKBENCH_KEY_'+secret[7:].upper()
                else:
                    kept = previous if previous and previous['provider']==name else None
                    existing = kept['config'] if kept else self.templates[name]
                    env = existing.get('credential_env')
                    if not env or not os.environ.get(env): reject('请填写 API key。', 'invalid_request')
                    config['credential_env']=env;secret=kept['secret'] if kept else None
            else: reject('不支持的认证方式。', 'invalid_request')
            # Full original deployment validation before any durable configuration write.
            candidate = copy.deepcopy(self.service.deployment.value)
            for project in candidate['projects'].values(): project.pop('identity',None)
            candidate['providers']={name:config};Deployment(candidate)
            value=dict(format=FORMAT,provider=name,config=config,secret=secret)
            if key and request['authentication']=='api_key':
                count=0
                with os.scandir(self.service.root) as entries:
                    for index, entry in enumerate(entries):
                        if index>self.service.deployment.limits.sessions+128: reject('配置目录超过预算。','resource_limit')
                        if entry.name.startswith('ai-key-'): count+=1
                if count>=32: reject('私有凭据版本已满，需要维护历史。','resource_limit')
                _atomic(self.service.root/secret,key.encode())
            # A failed fsync can follow a successful rename: retain its private key
            # rather than delete a credential the committed settings may reference.
            _atomic(self.path,wire(value,16384))
            self.value=value;self._load_secret(value);self.bindings.clear()
            return self.public()

    def binding(self, workspace, root, *, seal=False):
        with self.service.lock:
            inventory=self.inventory()['projects']; row=inventory.get(workspace)
            if row is None or Path(row['root']).absolute()!=Path(root).absolute(): reject('项目不在当前工作台中。','not_found')
            identity=root_identity(root)
            explicit=self.service.deployment.projects.get(workspace)
            if not explicit or explicit['identity']!=identity:
                matches=[p for p in self.service.deployment.projects.values() if p['identity']==identity]
                if len(matches)>1: reject('项目存在多个开发范围，请保留原登记身份。','conflict')
                explicit=matches[0] if matches else None
            configured_python = row.get('python')
            if configured_python and not (Path(configured_python).is_file() and os.access(configured_python, os.X_OK)):
                configured_python = ''
            configured_dev_network = bool(row.get('dev_network', False))
            dependency_lock = row.get('dependency_lock')
            if (self.value is None and configured_python is None and not configured_dev_network and not dependency_lock
                    and workspace in self.service.deployment.projects
                    and explicit is self.service.deployment.projects[workspace]
                    and explicit['identity'] == identity):
                return self.service.deployment,self.service.policy_hash,self.service.config_path
            effective=self.value or dict(provider=next(iter(self.templates)),config=next(iter(self.templates.values())))
            cache_key=(workspace,digest(identity),digest(self.value),configured_python,configured_dev_network,dependency_lock)
            if cache_key not in self.bindings:
                if len(self.bindings)>=128: reject('自动项目绑定超过预算。','resource_limit')
                if explicit and explicit['identity']==identity:
                    project=copy.deepcopy(explicit);project.pop('identity',None)
                else: project=automatic_project(root)
                if configured_python is not None:
                    project['python'] = configured_python
                else:
                    project.pop('python', None)
                if dependency_lock:
                    project['dependency_lock'] = dependency_lock
                else:
                    project.pop('dependency_lock', None)
                if configured_dev_network:
                    project['dev_network'] = True
                else:
                    project.pop('dev_network', None)
                value=copy.deepcopy(self.service.deployment.value)
                value['projects']={workspace:project};value['providers']={effective['provider']:effective['config']}
                deployment=Deployment(value)
                normalized=copy.deepcopy(deployment.value)
                for project in normalized['projects'].values(): project.pop('identity',None)
                revision=digest(normalized)
                self.bindings[cache_key]=(deployment,revision,self.service.root/(revision+'.json'))
            deployment,revision,path=self.bindings[cache_key]
            if seal:
                sealed_revision,sealed_path=self.service.seal_policy(deployment)
                if (sealed_revision,sealed_path)!=(revision,path): reject('Project policy identity changed','conflict')
            return deployment,revision,path


def unavailable():
    return dict(format=FORMAT,writable=False,reason='当前工作台尚未安装本地开发服务。',providers=[])
