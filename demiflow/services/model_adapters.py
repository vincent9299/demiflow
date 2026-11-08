"""Lightweight SystemOne adapters, also importable by the isolated worker script.

No model framework is imported until an adapter is explicitly constructed.
The batch methods are candidates for validation, not claims of model quality.
"""
import math
import json
import threading
from contextlib import nullcontext


def validate_request(value):
    if not isinstance(value, dict) or set(value)-{'model','state','questions','images'}:
        raise ValueError('Expected model/state/questions and optional images')
    questions = value.get('questions')
    if not isinstance(questions,dict) or not 1 <= len(questions) <= 128:
        raise ValueError('Expected 1..128 questions')
    for key, question in questions.items():
        if not isinstance(key,str) or not key or len(key)>256 or not isinstance(question,dict):
            raise ValueError('Invalid question identity')
        kind = question.get('type')
        if kind not in {'noul','choice'}:
            raise ValueError('Only noul and choice questions are supported')
        if not isinstance(question.get('instructions'),str) or not question['instructions'].strip():
            raise ValueError('Question instructions are required')
        if kind=='choice':
            choices=question.get('criteria')
            if (not isinstance(choices,dict) or not 1<=len(choices)<=255
                    or any(not isinstance(k,str) or not k or v is not None and not isinstance(v,str)
                           for k,v in choices.items())):
                raise ValueError('Invalid choice criteria')
    images=value.get('images',[])
    if (not isinstance(images,list) or len(images)>16
            or any(not isinstance(i,str) or not i.startswith('data:image/') for i in images)):
        raise ValueError('Images must be bounded image data URLs')
    if images:
        import base64
        import io
        from PIL import Image
        pixels=0
        for encoded in images:
            header,separator,data=encoded.partition(',')
            if not separator or not header.endswith(';base64') or len(data)>12*1024*1024:
                raise ValueError('Image must be base64 and at most 9 MiB decoded')
            try:
                with Image.open(io.BytesIO(base64.b64decode(data,validate=True))) as image:
                    count=image.width*image.height; pixels+=count
                    if count>16_000_000 or pixels>32_000_000:
                        raise ValueError('Image pixels exceed service budget')
            except (OSError,Image.DecompressionBombError) as exc:
                raise ValueError('Invalid image or image exceeds pixel budget') from exc
    return {k:v for k,v in value.items() if k!='model'}


def error_result(error, status=422):
    return {'error':{'type':type(error).__name__,'category':'input_error' if status==422 else 'provider_error',
                     'detail':str(error)[:2048],'status':status}}


class PplxAdapter:
    def __init__(self, decider):
        self.decider=decider
        self.lock=threading.Lock()

    def single(self, request):
        request=validate_request(request)
        with self.lock:
            return self.decider.decide(request)

    def batch(self, requests):
        results=[None]*len(requests)
        # Native encoding may reject one otherwise well-formed request. Validate
        # each before the shared decide(list), preserving the native tokenizer.
        with self.lock:
            group=[]; tokens=0
            def flush():
                if not group: return
                responses=self.decider.decide([request for _,request in group])
                if not isinstance(responses,list) or len(responses)!=len(group):
                    raise RuntimeError('Native batch response count differs')
                for (index,_),response in zip(group,responses,strict=True): results[index]=response
                group.clear()
            for index,request in enumerate(requests):
                try:
                    request=validate_request(request)
                    count=0
                    if hasattr(self.decider,'encoder'):
                        plan=self.decider.encoder.plan(request,self.decider.max_length)
                        count=sum(chunk.length for chunk in plan)
                        del plan
                        if count>262144: raise ValueError('Request exceeds 262144 encoded tokens')
                except (ValueError,TypeError,KeyError) as exc:
                    results[index]=error_result(exc)
                    continue
                if tokens+count>262144:
                    flush(); tokens=0
                group.append((index,request)); tokens+=count
            flush()
        return results


class JEVProtocol:
    """One formatter/calibration contract shared by native HTTP and GPU lanes."""
    def __init__(self, formatter, tokenizer, readout, max_model_len=32768):
        self.formatter,self.tokenizer,self.readout=formatter,tokenizer,readout
        self.render_lock=threading.Lock()
        self.max_model_len=max_model_len

    def _prepare(self, request, *, tokens=False):
        if isinstance(request,dict) and request.get('images'):
            raise ValueError('JEV v1.1 is text-only')
        request=validate_request(request)
        result=[]
        with self.render_lock:
            for key,question in request['questions'].items():
                count=len(self.formatter.options(question)[0])
                if count>len(self.readout['readout_token_ids']): raise ValueError('Too many options')
                prompt=self.formatter.render(self.tokenizer,request.get('state'),question,self.readout['codes'])
                token_ids=self.tokenizer.encode(prompt,add_special_tokens=False)
                length=len(token_ids)
                if length+1>self.max_model_len: raise ValueError('JEV request exceeds model context; no truncation')
                result.append((key,question,prompt,count,length)+((token_ids,) if tokens else ()))
        return result

    def calibrate(self, logits):
        temperature=float(self.readout.get('temperature',1.0))
        if not math.isfinite(temperature) or temperature<=0: raise ValueError('Invalid readout temperature')
        if not logits or any(not math.isfinite(x) for x in logits): raise ValueError('Invalid option log probabilities')
        peak=max(logits); weights=[math.exp((x-peak)/temperature) for x in logits]
        return [value/sum(weights) for value in weights]


class JEVAdapter(JEVProtocol):
    def __init__(self, formatter, tokenizer, readout, backend, model, client, max_model_len=32768, *, serialize_backend=False):
        super().__init__(formatter,tokenizer,readout,max_model_len)
        self.backend,self.model,self.client=backend.rstrip('/'),model,client
        self.backend_serialized=serialize_backend
        self.backend_lock=threading.Lock() if serialize_backend else None

    def _read(self,prompts,count, *, single=False):
        ids=self.readout['readout_token_ids'][:count]
        # Keep native engine settings and at most one backend request in flight.
        # Encoding and CPU calibration can overlap that GPU request.
        with (self.backend_lock or nullcontext()):
            with self.client.stream('POST',self.backend+'/completions',json={
                'model':self.model,'prompt':prompts[0] if single else prompts,'max_tokens':1,'temperature':1.0,
                'logprobs':count,'allowed_token_ids':ids,'return_tokens_as_token_ids':True}) as response:
                response.raise_for_status(); raw=bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if len(raw)+len(chunk)>16*1024**2: raise ValueError('JEV backend response exceeds 16 MiB')
                    raw.extend(chunk)
        payload=json.loads(raw)
        usage=payload.get('usage')
        if usage is not None:
            if (not isinstance(usage,dict) or any(type(usage.get(k)) is not int or usage[k]<0
                    for k in ('prompt_tokens','completion_tokens'))):
                raise RuntimeError('Invalid vLLM usage metadata')
            usage={'input_tokens':usage['prompt_tokens'],'output_tokens':usage['completion_tokens']}
        choices=payload['choices']
        if len(choices)!=len(prompts): raise ValueError('Backend response count differs')
        indexed={choice['index']:choice for choice in choices}
        if set(indexed)!=set(range(len(prompts))): raise ValueError('Backend response indices differ')
        output=[]
        for index in range(len(prompts)):
            top=indexed[index]['logprobs']['top_logprobs'][0]
            logits=[float(top[f'token_id:{token}']) for token in ids]
            output.append(self.calibrate(logits))
        return output,usage

    def single(self,request):
        answers={}; totals={'input_tokens':0,'output_tokens':0}; known=True
        for key,question,prompt,count,_ in self._prepare(request):
            probabilities,usage=self._read([prompt],count,single=True)
            answers[key]=self.formatter.to_answer(question,probabilities[0])
            if usage is None: known=False
            else:
                for name in totals: totals[name]+=usage[name]
        return {'answers':answers,**({'usage':totals} if known else {})}

    def batch(self,requests):
        results=[{'answers':{}} for _ in requests]
        totals=[{'input_tokens':0,'output_tokens':0} for _ in requests]; known=[True]*len(requests)
        groups={}
        for index,request in enumerate(requests):
            try:
                prepared=self._prepare(request)
                count_tokens=sum(item[4] for item in prepared)
                if count_tokens>262144:
                    raise ValueError('JEV request exceeds 262144 tokens')
                for key,question,prompt,count,length in prepared:
                    groups.setdefault(count,[]).append((index,key,question,prompt,length))
            except (ValueError,TypeError,KeyError) as exc: results[index]=error_result(exc)
        for count,items in groups.items():
            items=[item for item in items if 'error' not in results[item[0]]]
            if not items: continue
            position=0
            while position<len(items):
                end=position; tokens=0
                while end<len(items) and end-position<512 and tokens+items[end][4]<=262144:
                    tokens+=items[end][4]; end+=1
                group=items[position:end]; position=end
                probabilities,usage=self._read([item[3] for item in group],count)
                # Aggregate provider usage is only attributable to rows when its
                # totals agree with the exact prompts and one generated token each.
                if usage is not None and usage!={'input_tokens':sum(item[4] for item in group),'output_tokens':len(group)}:
                    raise RuntimeError('Cannot attribute vLLM batch usage to individual requests')
                for (index,key,question,_,length),probability in zip(group,probabilities,strict=True):
                    if usage is None: known[index]=False
                    else:
                        totals[index]['input_tokens']+=length; totals[index]['output_tokens']+=1
                    try: results[index]['answers'][key]=self.formatter.to_answer(question,probability)
                    except (ValueError,TypeError,KeyError) as exc: results[index]=error_result(exc,502)
        for index,result in enumerate(results):
            if 'error' not in result and known[index]: result['usage']=totals[index]
        return results


class AmosAdapter:
    """Native calibration/state handling; cross-request batch stays unsupported."""
    def __init__(self, engine): self.engine=engine

    def single(self, request):
        return self.engine(validate_request(request))


class D3Adapter:
    """Keep d3's native packing/readout/media handling behind bounded HTTP."""
    def __init__(self, decider): self.decider=decider

    def single(self, request):
        questions=request.get('questions')
        if not isinstance(questions,dict) or not 1<=len(questions)<=128:
            raise ValueError('Expected 1..128 questions')
        return self.decider.answer(request)
