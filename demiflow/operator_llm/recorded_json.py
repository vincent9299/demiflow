"""Bounded local-only JSON calls with original requests/responses and token usage."""
import asyncio
import json
import time
from pathlib import Path
import httpx
from demiflow.execution.artifacts import digest,immutable,read

class CallBudgetExceeded(RuntimeError):pass
class UncertainPreviousCall(RuntimeError):pass

class RecordedJSONClient:
    def __init__(self,run,config, *, request_options=None):
        self.run=Path(run);self.config=config
        self.request_options = dict(request_options or {})
        self.http=httpx.AsyncClient(trust_env=False,follow_redirects=False,timeout=config.get('timeout_s',240),limits=httpx.Limits(max_connections=1))
        self.checked=False;self.calls=0;self.reused=0
    async def check(self):
        if not self.checked:
            r=await self.http.get(self.config['base_url'].rstrip('/')+'/models');r.raise_for_status()
            names=[m['id'] for m in r.json()['data']]
            if names!=[self.config['model']]:raise ValueError(f'Endpoint model differs: {names}')
            self.checked=True
    async def json(self,stage,messages):
        payload={'model':self.config['model'],'messages':messages,'temperature':0,
                 'max_tokens':self.config['max_output_tokens'], **self.request_options}
        key=digest({'stage':stage,'payload':payload,'endpoint':self.config['base_url']})
        folder=self.run/'calls';request=folder/f'{key}.request.json';response=folder/f'{key}.response.json'
        if response.exists():record=read(response);self.reused+=1
        else:
            if request.exists():raise UncertainPreviousCall('Request already reserved without a saved response; do not silently repeat it')
            if len(list(folder.glob('*.request.json')))>=self.config['max_calls']:raise CallBudgetExceeded('Configured local-call budget exhausted')
            await self.check()
            # Recheck after the await: other stages may have reserved calls meanwhile.
            if request.exists():raise UncertainPreviousCall('Another task already reserved this request')
            if len(list(folder.glob('*.request.json')))>=self.config['max_calls']:raise CallBudgetExceeded('Configured local-call budget exhausted')
            immutable(request,{'stage':stage,'endpoint':self.config['base_url'],'payload':payload})
            t=time.monotonic()
            try:
                r=await self.http.post(self.config['base_url'].rstrip('/')+'/chat/completions',json=payload)
                record={'status_code':r.status_code,'elapsed_s':time.monotonic()-t,'body':r.json() if r.headers.get('content-type','').startswith('application/json') else r.text}
                immutable(response,record);self.calls+=1
            except Exception as e:
                immutable(folder/f'{key}.transport_error.json',{'error':type(e).__name__,'detail':str(e),'elapsed_s':time.monotonic()-t})
                raise
        if record['status_code']!=200:raise ValueError('Local model HTTP error; original response saved')
        body=record['body'];choice=body['choices'][0]
        if choice.get('finish_reason')!='stop':raise ValueError('Incomplete/truncated model output; original response saved')
        result=json.loads(choice['message']['content'])
        if not isinstance(result,dict):raise ValueError('Model output must be an object')
        return result,{'request_path':str(request),'response_path':str(response),'usage':body.get('usage',{}),'elapsed_s':record['elapsed_s'],'model':body.get('model')}
    async def aclose(self):await self.http.aclose()
