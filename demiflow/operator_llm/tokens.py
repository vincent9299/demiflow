"""Explicit tokenizer profiles for text-only prompt admission.

No model alias is guessed. The profile owner must verify the encoding, chat
framing, context capacity and output accounting against the actual service.
"""
from dataclasses import dataclass
from demiflow.operator_llm.client import request_messages


class InputTokenBudgetExceeded(ValueError):
    pass


class CharacterCounter:
    """Count actual textual context; images are excluded, no token claim."""
    unit = 'chars'

    def text(self, text):
        return len(text)

    def messages(self, messages):
        total = 0
        for message in messages:
            content = message.get('content', '')
            total += len(message.get('role', ''))
            if isinstance(content, str):
                total += len(content)
            else:
                total += sum(len(part.get('text', '')) for part in content if part.get('type') == 'text')
        return total

    def prompt(self, prompt, values):
        from .model import OperatorLLMRequest
        from .messages import render_input
        from .systemone import render_questions
        parts, messages = render_input(prompt, values)
        request = OperatorLLMRequest(prompt.name, prompt.version, prompt.model.name,
            parts, prompt.response_schema,
            response_format=prompt.response_format, messages=messages,
            systemone=render_questions(prompt.systemone, values))
        return self.messages(request_messages(request))


@dataclass(frozen=True)
class CharacterBudget:
    """Text character admission, independent of model/provider token capacity."""
    max_input: int
    counter: CharacterCounter = CharacterCounter()

    def __post_init__(self):
        if type(self.max_input) is not int or self.max_input < 1:
            raise ValueError('CharacterBudget.max_input must be positive')

    def validate_model(self, model):
        pass

    def validate(self, request):
        count = self.counter.messages(request_messages(request))
        if count > self.max_input:
            raise InputTokenBudgetExceeded(f'complete text input {count} exceeds {self.max_input} characters')
        return count


class TextTokenCounter:
    def __init__(self, profile, *, resource_root=None):
        import tiktoken
        self.profile = dict(profile)
        required = {'encoding','tokens_per_message','reply_tokens','models','context_tokens',
                    'max_output_tokens','verification','verified'}
        local = {'encoding_file', 'encoding_sha256', 'pattern', 'special_tokens'}
        if not required<=set(self.profile) or set(self.profile)-required-{'role_content_copies'}-local:
            raise ValueError('token profile requires '+str(sorted(required)))
        copies=self.profile.get('role_content_copies',{})
        if not isinstance(copies,dict) or any(not isinstance(k,str) or type(v) is not int or v<1 for k,v in copies.items()):
            raise ValueError('role_content_copies must map roles to positive integer multiplicities')
        for key in ('tokens_per_message','reply_tokens','context_tokens','max_output_tokens'):
            if type(self.profile[key]) is not int or self.profile[key]<0: raise ValueError('invalid token profile '+key)
        if type(self.profile['verified']) is not bool or not isinstance(self.profile['models'],list) or not all(isinstance(m,str) and m for m in self.profile['models']): raise ValueError('invalid profile model/verification fields')
        if not self.profile['models'] or not self.profile['verification']: raise ValueError('token profile requires model binding and verification provenance')
        if local & self.profile.keys():
            self.encoding, self.encoding_bytes = _local_encoding(self.profile, resource_root)
        else:
            self.encoding = tiktoken.get_encoding(self.profile['encoding'])
            self.encoding_bytes = 0

    def text(self,text):
        return len(self.encoding.encode(text,disallowed_special=()))

    def messages(self,messages):
        count=self.profile['reply_tokens']
        for message in messages:
            if not isinstance(message.get('content'),str):
                raise ValueError('TextTokenCounter only admits text messages')
            copies=self.profile.get('role_content_copies',{}).get(message['role'],1)
            count+=self.profile['tokens_per_message']+self.text(message['role'])+copies*self.text(message['content'])
        return count

    def prompt(self,prompt,values):
        """Count a rendered native prompt without creating a model client."""
        from demiflow.operator_llm.model import OperatorLLMRequest
        from demiflow.operator_llm.messages import render_input
        from demiflow.operator_llm.systemone import render_questions
        parts, messages = render_input(prompt, values)
        request=OperatorLLMRequest(prompt.name,prompt.version,prompt.model.name,
            parts,prompt.response_schema,
            response_format=prompt.response_format, messages=messages,
            systemone=render_questions(prompt.systemone, values))
        return self.messages(request_messages(request))


class HuggingFaceTokenCounter:
    """Local tokenizer and chat template for a declared text-only deployment.

    Loads tokenizer files only, never downloads files or loads model weights.
    The caller binds the same template kwargs and revision used by the server.
    Counting includes the rendered schema/system message and generation prefix.
    """
    unit = 'tokens'

    def __init__(self, tokenizer_path, *, models, context_tokens, max_output_tokens,
                 revision, chat_template_kwargs=None):
        from transformers import AutoTokenizer
        if (not isinstance(models, (list, tuple)) or not models
                or any(not isinstance(m, str) or not m for m in models)
                or not isinstance(revision, str) or not revision.strip()):
            raise ValueError('local tokenizer requires model names and deployment revision')
        if any(type(v) is not int or v < 1 for v in (context_tokens, max_output_tokens)):
            raise ValueError('token capacities must be positive integers')
        if max_output_tokens >= context_tokens:
            raise ValueError('output capacity must leave room for input')
        self.template_kwargs = dict(chat_template_kwargs or {})
        if set(self.template_kwargs) & {'tokenize', 'add_generation_prompt', 'return_tensors', 'return_dict'}:
            raise ValueError('tokenizer template kwargs cannot override token counting')
        self.tokenizer = AutoTokenizer.from_pretrained(str(tokenizer_path), local_files_only=True,
                                                      trust_remote_code=False)
        if not self.tokenizer.chat_template:
            raise ValueError('local tokenizer has no chat template')
        self.profile = dict(models=list(models), context_tokens=context_tokens,
                            max_output_tokens=max_output_tokens, verification=revision)

    def text(self, text):
        return len(self.tokenizer.encode(text, add_special_tokens=False))

    def messages(self, messages):
        if any(not isinstance(m.get('content'), str) for m in messages):
            raise ValueError('HuggingFaceTokenCounter only admits text messages')
        return len(self.tokenizer.apply_chat_template(messages, tokenize=True,
            add_generation_prompt=True, return_dict=False, **self.template_kwargs))

    prompt = TextTokenCounter.prompt


@dataclass(frozen=True)
class TokenBudget:
    counter: TextTokenCounter
    max_input: int
    max_output: int

    def __post_init__(self):
        if any(type(v) is not int or v<0 for v in (self.max_input,self.max_output)):
            raise ValueError('Token budgets must be nonnegative integers')
        p=self.counter.profile
        if self.max_input+self.max_output>p['context_tokens'] or self.max_output>p['max_output_tokens']:
            raise InputTokenBudgetExceeded('requested input/output exceed verified service capacity')

    def validate_model(self,model):
        if model not in self.counter.profile['models']:
            raise ValueError('token profile is not bound to this model')

    def validate(self,request):
        p=self.counter.profile
        self.validate_model(request.model)
        if self.max_input+self.max_output>p['context_tokens'] or self.max_output>p['max_output_tokens']:
            raise InputTokenBudgetExceeded('requested input/output exceed verified service capacity')
        count=self.counter.messages(request_messages(request))
        if count>self.max_input: raise InputTokenBudgetExceeded(f'complete input {count} exceeds {self.max_input} tokens')
        return count


def budget_declaration(value):
    """Pure, bounded description of the two native admission units.

    A token profile is an owner's explicit claim about a deployment. Binding it
    does not establish that the remote service actually uses that tokenizer.
    No tokenizer, file, model client or environment is loaded by this analyzer.
    """
    from collections.abc import Mapping
    if value is None:
        return None
    if type(value) is CharacterBudget and type(value.counter) is CharacterCounter:
        value = {'unit': 'chars', 'max_input': value.max_input}
    elif type(value) is TokenBudget and type(value.counter) is TextTokenCounter:
        value = {'unit': 'tokens', 'max_input': value.max_input, 'max_output': value.max_output,
                 'profile': value.counter.profile}
    if not isinstance(value, Mapping):
        raise ValueError('Managed input budget requires a native character/token declaration')
    value = dict(value)
    unit = value.get('unit')
    if unit == 'chars' and set(value) == {'unit', 'max_input'}:
        if type(value['max_input']) is int and 1 <= value['max_input'] <= 16 * 1024**2:
            return value
        raise ValueError('Character input budget must be 1..16 Mi characters')
    if unit != 'tokens' or set(value) != {'unit', 'max_input', 'max_output', 'profile'}:
        raise ValueError('Token budget requires unit/max_input/max_output/profile')
    if any(type(value[k]) is not int or not 0 <= value[k] <= 1024**2 for k in ('max_input', 'max_output')):
        raise ValueError('Token budgets must be 0..1 Mi tokens')
    p = value['profile']
    required = {'encoding', 'tokens_per_message', 'reply_tokens', 'models', 'context_tokens',
                'max_output_tokens', 'verification', 'verified'}
    local = {'encoding_file', 'encoding_sha256', 'pattern', 'special_tokens'}
    if type(p) is not dict or not required <= p.keys() or p.keys() - required - {'role_content_copies'} - local:
        raise ValueError('Token profile has missing or unknown fields')
    if p.keys() & local:
        _local_encoding_declaration(p)
    if (p['verified'] is not True or type(p['verification']) is not str or not 1 <= len(p['verification']) <= 1024
            or type(p['encoding']) is not str or not 1 <= len(p['encoding']) <= 128
            or type(p['models']) is not list or not 1 <= len(p['models']) <= 256
            or any(type(m) is not str or not 1 <= len(m) <= 256 for m in p['models'])):
        raise ValueError('Managed token profile requires bounded model/encoding identities and verification provenance')
    for name in ('tokens_per_message', 'reply_tokens', 'context_tokens', 'max_output_tokens'):
        if type(p[name]) is not int or not 0 <= p[name] <= 16 * 1024**2:
            raise ValueError('Invalid token profile capacity')
    copies = p.get('role_content_copies', {})
    if (type(copies) is not dict or len(copies) > 16 or any(type(k) is not str or len(k) > 64
            or type(v) is not int or not 1 <= v <= 16 for k, v in copies.items())):
        raise ValueError('Invalid token profile framing')
    if value['max_input'] + value['max_output'] > p['context_tokens'] or value['max_output'] > p['max_output_tokens']:
        raise ValueError('Token budget exceeds declared deployment capacity')
    return {**value, 'profile': {**p, 'models': list(p['models']), **({'role_content_copies': dict(copies)} if 'role_content_copies' in p else {})}}


def input_budget(declaration, *, resource_root=None):
    """Build the existing native budget from a closed data declaration."""
    value = budget_declaration(declaration)
    if value is None:
        return None
    if value['unit'] == 'chars':
        return CharacterBudget(value['max_input'])
    return TokenBudget(TextTokenCounter(value['profile'], resource_root=resource_root), value['max_input'], value['max_output'])


def _local_encoding_declaration(profile):
    from pathlib import PurePosixPath
    import re
    if not {'encoding_file', 'encoding_sha256', 'pattern', 'special_tokens'} <= profile.keys():
        raise ValueError('Local tokenizer requires file/hash/pattern/special_tokens')
    value = profile['encoding_file']
    if type(value) is not str:
        raise ValueError('Tokenizer file must be a relative resource path')
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or '..' in path.parts or str(path) != value or '\\' in value:
        raise ValueError('Tokenizer file must be a relative resource path')
    if type(profile['encoding_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}', profile['encoding_sha256']):
        raise ValueError('Tokenizer requires an exact SHA256')
    special = profile['special_tokens']
    if (type(profile['pattern']) is not str or not 1 <= len(profile['pattern']) <= 16384
            or type(special) is not dict or len(special) > 4096 or any(type(k) is not str or not 1 <= len(k) <= 256
            or type(v) is not int or not 0 <= v < 2**31 for k, v in special.items())
            or len(set(special.values())) != len(special)):
        raise ValueError('Tokenizer pattern/special tokens exceed their declaration bounds')


def _local_encoding(profile, resource_root):
    """Bounded local BPE data + public tiktoken.Encoding; never name lookup/download.

    The file uses tiktoken's base64-token/integer-rank format. Only loading is
    bounded here; tokenization and message counting use the original engine.
    """
    import base64
    import io
    from pathlib import Path
    import tiktoken
    from ..objects import read_local_object
    _local_encoding_declaration(profile)
    root = Path(resource_root or Path.cwd()).absolute()
    raw = read_local_object((root / profile['encoding_file']).as_uri(), profile['encoding_sha256'],
                            root=root, max_bytes=16 * 1024**2)
    ranks, ids = {}, set()
    for index, line in enumerate(io.BytesIO(raw)):
        if index >= 500000 or len(line) > 16384:
            raise ValueError('Tokenizer vocabulary count/token length exceeds budget')
        parts = line.split()
        if len(parts) != 2 or len(parts[1]) > 10:
            raise ValueError('Invalid local BPE record')
        token, rank = base64.b64decode(parts[0], validate=True), int(parts[1])
        if not token or token in ranks or rank in ids or not 0 <= rank < 2**31:
            raise ValueError('Invalid or duplicate local BPE token/rank')
        ranks[token] = rank
        ids.add(rank)
    if any(bytes([b]) not in ranks for b in range(256)) or set(profile['special_tokens'].values()) & ids:
        raise ValueError('Local BPE vocabulary must cover every byte and have nonconflicting special IDs')
    return (tiktoken.Encoding(profile['encoding'], pat_str=profile['pattern'],
                             mergeable_ranks=ranks, special_tokens=profile['special_tokens']), len(raw))
