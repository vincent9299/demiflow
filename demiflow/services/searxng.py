"""Platform-owned SearXNG deployment profile and Wikipedia keyword adapter.

SearXNG itself is a declared installed dependency, never imported from a business
package. No installation or modification of an existing deployment is implicit.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
from .shared_http import SharedHTTPService


class SearxNGService:
    def __init__(self, *, root, name, python, runtime_directory, runtime_revision,
                 port=8080, engines=('wikisearch','wikipedia'), proxy_url=None, request_concurrency=8):
        from .manage import _directory
        _directory(root,name)
        if not runtime_revision: raise ValueError('Declare the installed SearXNG runtime revision')
        if type(port) is not int or not 1<=port<=65535: raise ValueError('Invalid service port')
        if not engines or any(not isinstance(e,str) or not e for e in engines): raise ValueError('Declare search engines')
        self.root,self.name,self.python=str(Path(root).resolve()),name,str(python)
        self.runtime_directory=str(Path(runtime_directory).resolve())
        self.port,self.engines,self.proxy_url=port,tuple(engines),proxy_url
        self.request_concurrency=request_concurrency
        self.profile={'runtime':runtime_revision,'engines':list(engines),'proxy':proxy_url,'profile_version':'demiflow-searxng-1'}
        self.search_url=f'http://127.0.0.1:{port}/search'
    @property
    def fingerprint(self):
        engine=Path(__file__).with_name('engines')/'wikisearch.py'
        return hashlib.sha256(json.dumps(self.profile,sort_keys=True).encode()+engine.read_bytes()).hexdigest()
    def bind(self): return _SearxOwner(self)


class _SearxOwner:
    def __init__(self,spec): self.spec=spec;self.owner=None
    async def ensure_ready(self):
        if self.owner is None:
            import yaml
            from .manage import _directory
            from ..execution.artifacts import run_lock
            spec=self.spec; directory=_directory(spec.root,spec.name)
            settings=directory/(spec.fingerprint+'.yaml')
            engine=str((Path(__file__).with_name('engines')/'wikisearch').resolve())
            config={'use_default_settings':{'engines':{'keep_only':list(spec.engines)}},
                'server':{'port':spec.port,'bind_address':'127.0.0.1','base_url':f'http://127.0.0.1:{spec.port}/',
                          'limiter':False,'public_instance':False,'secret_key':spec.fingerprint},
                'search':{'formats':['json','html'],'safe_search':1},
                'outgoing':{'request_timeout':10,'max_request_timeout':15},
                'engines':[({'name':name,'engine':engine,'categories':'general','shortcut':'wks','disabled':False}
                            if name=='wikisearch' else {'name':name,'disabled':False}) for name in spec.engines]}
            if spec.proxy_url: config['outgoing']['proxies']={'all://':spec.proxy_url}
            with run_lock(directory/'profile'):
                if not settings.exists(): settings.write_text(yaml.safe_dump(config,allow_unicode=True))
            platform_path=str(Path(__file__).resolve().parents[2])
            declaration=SharedHTTPService(root=spec.root,name=spec.name,request_concurrency=spec.request_concurrency,
                configuration={'command':[spec.python,'-m','demiflow.services.searxng',spec.runtime_directory,str(settings)],
                    'base_url':f'http://127.0.0.1:{spec.port}','ready_path':'/healthz',
                    'env':{'PYTHONPATH':platform_path},'startup_timeout_s':60})
            self.owner=declaration.bind()
        await self.owner.ensure_ready()
    def request_slot(self): return self.owner.request_slot()
    async def aclose(self):
        if self.owner is not None: await self.owner.aclose()


if __name__=='__main__':
    runtime,settings=sys.argv[1:]
    sys.path.insert(0,runtime)
    os.environ['SEARXNG_SETTINGS_PATH']=settings
    from searx.webapp import run
    run()
