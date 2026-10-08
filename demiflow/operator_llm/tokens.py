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
        parts, messages = render_input(prompt, values)
        request = OperatorLLMRequest(prompt.name, prompt.version, prompt.model.name,
            parts, prompt.response_schema,
            response_format=prompt.response_format, messages=messages)
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
    def __init__(self, profile):
        import tiktoken
        self.profile = dict(profile)
        required = {'encoding','tokens_per_message','reply_tokens','models','context_tokens',
                    'max_output_tokens','verification','verified'}
        if not required<=set(self.profile) or set(self.profile)-required-{'role_content_copies'}:
            raise ValueError('token profile requires '+str(sorted(required)))
        copies=self.profile.get('role_content_copies',{})
        if not isinstance(copies,dict) or any(not isinstance(k,str) or type(v) is not int or v<1 for k,v in copies.items()):
            raise ValueError('role_content_copies must map roles to positive integer multiplicities')
        for key in ('tokens_per_message','reply_tokens','context_tokens','max_output_tokens'):
            if type(self.profile[key]) is not int or self.profile[key]<0: raise ValueError('invalid token profile '+key)
        if type(self.profile['verified']) is not bool or not isinstance(self.profile['models'],list) or not all(isinstance(m,str) and m for m in self.profile['models']): raise ValueError('invalid profile model/verification fields')
        if not self.profile['models'] or not self.profile['verification']: raise ValueError('token profile requires model binding and verification provenance')
        self.encoding = tiktoken.get_encoding(self.profile['encoding'])

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
        parts, messages = render_input(prompt, values)
        request=OperatorLLMRequest(prompt.name,prompt.version,prompt.model.name,
            parts,prompt.response_schema,
            response_format=prompt.response_format, messages=messages)
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
