"""Closed declarations for acquiring one complete raw object per input row.

Construction is pure: no credentials, paths, clients or network are opened.
Physical store admission and secret resolution belong to actor startup.
"""
from dataclasses import dataclass, field
import math
from pathlib import Path
import re
from urllib.parse import parse_qsl, urlsplit

from ..secrets import Secret, public
from ..execution.recovery_contract import document, identifier


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer in {low}..{high}')
    return value


def origin(uri):
    parsed = urlsplit(uri)
    if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError('Expected an HTTP(S) URL without userinfo')
    port = parsed.port
    host = parsed.hostname.lower()
    if ':' in host:
        host = '[' + host + ']'
    return parsed.scheme + '://' + host + (':' + str(port) if port and port != (443 if parsed.scheme == 'https' else 80) else '')


def stable_uri(uri):
    if type(uri) is not str or not 1 <= len(uri.encode('utf8')) <= 4096 or any(ord(c) < 33 for c in uri):
        raise ValueError('Stable object URI exceeds its contract')
    p = urlsplit(uri)
    if p.scheme not in {'http', 'https', 'cos'} or not p.hostname or p.username is not None or p.password is not None or p.fragment:
        raise ValueError('Object URI must be stable HTTP(S)/COS without userinfo/fragment')
    if p.scheme != 'cos':
        origin(uri)
    elif p.query or p.port:
        raise ValueError('COS URI accepts a bucket and object key only')
    # Explicitly signed query URLs cannot be stable public row identities.
    if any(re.search(r'(^|[-_])(signature|credential|token|secret|password|authorization|api[-_]?key|access[-_]?key)([-_]|$)|^q-sign|^sig$|^key$', k, re.I)
           for k, _ in parse_qsl(p.query, keep_blank_values=True)):
        raise ValueError('Signed object URI requires a stable redirect or declared credential resource')
    return uri


def request(value):
    if type(value) is not dict or not {'uri'} <= set(value) <= {'uri', 'expected_size', 'expected_sha256', 'source_version'}:
        raise ValueError('Fetch request requires uri and only supported integrity constraints')
    document(value, 16384)
    out = dict(value)
    stable_uri(out['uri'])
    if 'expected_size' in out:
        integer(out['expected_size'], 'expected_size', 0, 2**63-1)
    if 'expected_sha256' in out and (type(out['expected_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}', out['expected_sha256'])):
        raise ValueError('expected_sha256 must be lowercase SHA256')
    if 'source_version' in out:
        v = out['source_version']
        if type(v) is not dict or set(v) != {'kind', 'value'} or v['kind'] not in {'manifest_label', 'http_etag', 'cos_version_id'}:
            raise ValueError('Invalid source_version')
        if type(v['value']) is not str or not 1 <= len(v['value'].encode('utf8')) <= 1024 or any(ord(c) < 32 for c in v['value']):
            raise ValueError('Invalid source version value')
        if v['kind'] == 'http_etag' and not re.fullmatch(r'"[\x21\x23-\x7e]*"', v['value']):
            raise ValueError('http_etag must be a strong quoted ETag')
        if v['kind'] == 'cos_version_id' and urlsplit(out['uri']).scheme != 'cos':
            raise ValueError('cos_version_id requires a COS source')
    return out


@dataclass(frozen=True)
class HttpSource:
    headers: dict = field(default_factory=dict)
    credential_origin: str | None = None
    redirect_origins: tuple = ()
    ca_file: str | None = None
    proxy: Secret | None = None
    allow_private: bool = False

    def __post_init__(self):
        if type(self.headers) is not dict or len(self.headers) > 32:
            raise ValueError('HTTP headers exceed declaration budget')
        copied = {}
        for k, v in self.headers.items():
            if type(k) is not str or not re.fullmatch('[!#$%&\'*+.^_`|~0-9A-Za-z-]{1,128}', k):
                raise ValueError('Invalid declared header name')
            k = k.lower()
            if k in {'host', 'content-length', 'transfer-encoding', 'range', 'if-match', 'accept-encoding'} or k in copied:
                raise ValueError('Transport owns framing, version and representation headers')
            if k in {'authorization', 'cookie', 'proxy-authorization'} and type(v) is not Secret:
                raise ValueError('Credential headers require Secret')
            if type(v) not in {str, Secret} or type(v) is str and (len(v.encode('utf8')) > 4096 or '\r' in v or '\n' in v):
                raise ValueError('Invalid declared header value')
            copied[k] = v
        if any(type(v) is Secret for v in copied.values()) and self.credential_origin is None:
            raise ValueError('Credential headers require one explicit credential_origin')
        if self.credential_origin is not None and origin(self.credential_origin) != self.credential_origin:
            raise ValueError('credential_origin must be a canonical origin')
        if type(self.redirect_origins) not in {tuple, list} or len(self.redirect_origins) > 16:
            raise ValueError('At most 16 redirect origins may be declared')
        for uri in self.redirect_origins:
            if origin(uri) != uri or not uri.startswith('https://'):
                raise ValueError('Cross-origin redirect targets must be HTTPS origins')
        if self.ca_file is not None and (type(self.ca_file) is not str or not self.ca_file.startswith('/') or len(self.ca_file) > 4096):
            raise ValueError('ca_file must be an absolute path')
        if self.proxy is not None and type(self.proxy) is not Secret:
            raise ValueError('Proxy resource must use Secret')
        if type(self.allow_private) is not bool:
            raise ValueError('allow_private must be boolean')
        object.__setattr__(self, 'headers', copied)
        object.__setattr__(self, 'redirect_origins', tuple(self.redirect_origins))
        document(self.declaration(), 65536)

    def declaration(self):
        return {k: public(v) for k, v in vars(self).items()}


@dataclass(frozen=True)
class CosSource:
    bucket: str
    region: str
    secret_id: Secret
    secret_key: Secret
    session_token: Secret | None = None
    endpoint: str | None = None

    def __post_init__(self):
        for name in ('bucket', 'region'):
            if type(getattr(self, name)) is not str or not re.fullmatch('[A-Za-z0-9-]{1,128}', getattr(self, name)):
                raise ValueError('Invalid COS ' + name)
        if any(type(v) is not Secret for v in (self.secret_id, self.secret_key)) or self.session_token is not None and type(self.session_token) is not Secret:
            raise ValueError('COS credentials require Secret')
        endpoint = self.endpoint or f'https://{self.bucket}.cos.{self.region}.myqcloud.com'
        if origin(endpoint) != endpoint or not endpoint.startswith('https://'):
            raise ValueError('COS endpoint must be an HTTPS origin')
        object.__setattr__(self, 'endpoint', endpoint)

    def declaration(self):
        return {k: public(v) for k, v in vars(self).items()}


@dataclass(frozen=True)
class FetchPolicy:
    max_object_bytes: int = 1024**3
    chunk_bytes: int = 65536
    max_attempts: int = 3
    connect_timeout_s: float = 10
    read_idle_timeout_s: float = 30
    attempt_timeout_s: float = 300
    total_timeout_s: float = 600
    redirects: int = 5
    backoff_initial_s: float = 1
    backoff_cap_s: float = 30
    max_retry_after_s: float = 60
    per_host_concurrency: int = 2
    max_hosts: int = 64
    max_request_bytes: int = 16384
    max_receipt_bytes: int = 16384
    ephemeral_redirect_refreshes: int = 1
    http: HttpSource = field(default_factory=HttpSource)
    cos: tuple = ()

    def __post_init__(self):
        for name, low, high in [('max_object_bytes',1,1024**4), ('chunk_bytes',16384,1024**2),
                ('max_attempts',1,8), ('redirects',0,8), ('per_host_concurrency',1,64),
                ('max_hosts',1,64), ('max_request_bytes',1024,16384), ('max_receipt_bytes',1024,16384),
                ('ephemeral_redirect_refreshes',0,1)]:
            integer(getattr(self,name),name,low,high)
        for name in ('connect_timeout_s','read_idle_timeout_s','attempt_timeout_s','total_timeout_s',
                     'backoff_initial_s','backoff_cap_s','max_retry_after_s'):
            v = getattr(self, name)
            if type(v) not in (int,float) or not math.isfinite(v) or not 0 < v <= 86400:
                raise ValueError('Fetch time budget must be finite and positive: ' + name)
        http = HttpSource(**self.http) if type(self.http) is dict else HttpSource(**vars(self.http)) if type(self.http) is HttpSource else self.http
        if type(http) is not HttpSource:
            raise ValueError('http requires HttpSource')
        if type(self.cos) not in (tuple,list) or len(self.cos) > 16 or any(type(v) is not CosSource for v in self.cos):
            raise ValueError('cos requires at most 16 declared CosSource resources')
        if len({v.bucket for v in self.cos}) != len(self.cos):
            raise ValueError('COS bucket resources must be unique')
        object.__setattr__(self,'http',http)
        object.__setattr__(self,'cos',tuple(self.cos))
        document(self.declaration(),65536)

    def declaration(self):
        return {k: v.declaration() if k == 'http' else [s.declaration() for s in v] if k == 'cos' else v
                for k,v in vars(self).items()}


def fetch_policy(value=None):
    if value is None:
        return FetchPolicy()
    if type(value) is FetchPolicy:
        # Revalidate mutable mappings owned by declarations at the boundary.
        return FetchPolicy(**vars(value))
    if type(value) is dict:
        return FetchPolicy(**value)
    raise TypeError('policy must be FetchPolicy or a mapping')


def wire_quantum(bound, redirects):
    return min(256*1024**2, max(1024**2, (bound + 1 + (redirects+1)*4096 + 127)//128))


def validate_result(result, object_fact, domain, *, context=None):
    required={'status','terminal','raw_ref','size_bytes','partial_bytes','reason','detail','source',
              'checks','attempts','http_requests','retrieved_at','acquisition_ref','storage_domain'}
    if type(result) is not dict or set(result)!=required:
        raise ValueError('Invalid native raw receipt schema')
    document(result,16384)
    integer(result['attempts'],'attempts',0,8)
    integer(result['http_requests'],'http_requests',0,72)
    integer(result['partial_bytes'],'partial_bytes',0,1024**4)
    if result['http_requests']>result['attempts']*9:
        raise ValueError('Raw receipt request count exceeds attempt contract')
    ref=result['acquisition_ref']
    if type(ref) is not dict or set(ref)!={'commit_id','scope_id'}:
        raise ValueError('Invalid raw acquisition reference')
    for value in ref.values():identifier(value)
    if context is not None and (ref['commit_id']!=context.commit_id or ref['scope_id']!=context.action_id):
        raise ValueError('Raw receipt belongs to another native acquisition')
    if type(result['source']) is not dict or type(result['checks']) is not dict:
        raise ValueError('Raw receipt lacks source/check facts')
    stable_uri(result['source']['uri'])
    if result['status']=='ok':
        if object_fact is None or domain is None or result['terminal'] is not True or result['reason'] is not None:
            raise ValueError('Successful raw receipt requires its immutable object')
        expected={'uri':(Path(domain['physical_root'])/object_fact['sha256'][:2]/object_fact['sha256']).as_uri(),
                  'sha256':object_fact['sha256']}
        if result['raw_ref']!=expected or result['size_bytes']!=object_fact['bytes'] or result['partial_bytes']!=0 or result['storage_domain']!=domain:
            raise ValueError('Raw receipt does not describe its prepared object')
        checks=result['checks']
        if checks.get('expected_size') is not None and (checks['expected_size']!=object_fact['bytes'] or checks.get('size_verified') is not True):
            raise ValueError('Raw size verification facts differ')
        if checks.get('expected_sha256') is not None and (checks['expected_sha256']!=object_fact['sha256'] or checks.get('sha256_verified') is not True):
            raise ValueError('Raw checksum verification facts differ')
    elif result['status'] in {'failed','retryable'}:
        if object_fact is not None or domain is not None or result['raw_ref'] is not None or result['size_bytes'] is not None or result['storage_domain'] is not None:
            raise ValueError('Failed raw receipt must not publish an object')
        if type(result['terminal']) is not bool or result['terminal']!=(result['status']=='failed') or type(result['reason']) is not str:
            raise ValueError('Raw failure has an invalid terminal outcome')
    else:
        raise ValueError('Unknown raw result status')
    return result


__all__ = ['HttpSource','CosSource','FetchPolicy']
from .fetch_flow import declare_value as _declare_fetch_value
for _value,_kind in ((HttpSource,'http_source'),(CosSource,'cos_source'),(FetchPolicy,'fetch_policy')):
    _declare_fetch_value(_value,_kind)
