"""Generic embedding contract, independent of deployment and business tables."""
from dataclasses import dataclass, field
import hashlib
import json
from urllib.parse import urlsplit


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class EmbeddingModel:
    """An immutable revision and its /v1/embeddings protocol.

    ``input_format='text'`` sends standard ``input: [str]``; ``'chat'`` sends
    vLLM's batched ``messages: [[message]]`` for text or images. Request options
    carry model-specific processor/pooling settings. ``encoding_parameters``
    declares backend version, precision and template identity for cache safety.
    Deployment URLs, credentials and GPU placement do not define vector space.
    """
    name: str
    revision: str
    dimensions: int
    base_url: str
    input_format: str = 'text'
    normalize: bool = True
    api_key_env: str = ''
    request_options: dict = field(default_factory=dict)
    encoding_parameters: dict = field(default_factory=dict)
    image_transport: str = 'png'

    # Same endpoint binding protocol as the native prompt actor.
    transport = 'openai_compatible'
    base_url_env = ''

    def __post_init__(self):
        for name in ('name', 'revision', 'base_url'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError('EmbeddingModel requires ' + name)
        url = urlsplit(self.base_url)
        if (url.scheme not in {'http', 'https'} or not url.hostname or url.username
                or url.password or url.query or url.fragment):
            raise ValueError('Embedding endpoint must be an HTTP(S) API base URL')
        if type(self.dimensions) is not int or self.dimensions < 1:
            raise ValueError('Embedding dimensions must be a positive integer')
        if self.input_format not in {'text', 'chat'} or type(self.normalize) is not bool:
            raise ValueError('Invalid embedding input format or normalization')
        if not isinstance(self.api_key_env, str):
            raise ValueError('api_key_env must be an environment variable name')
        if self.image_transport not in {'png', 'original_if_compatible'}:
            raise ValueError('image_transport must be png or original_if_compatible')
        for key in ('request_options', 'encoding_parameters'):
            if not isinstance(getattr(self, key), dict):
                raise ValueError(key + ' must be a JSON object')
            object.__setattr__(self, key, json.loads(canonical(getattr(self, key))))
        if set(self.request_options) & {'model', 'input', 'messages', 'encoding_format', 'stream'}:
            raise ValueError('request_options cannot override embedding protocol fields')

    def contract(self):
        return dict(protocol='embeddings-v1', name=self.name, revision=self.revision,
                    dimensions=self.dimensions, input_format=self.input_format,
                    normalize=self.normalize, request_options=self.request_options,
                    encoding_parameters=self.encoding_parameters,
                    image_preprocessing=('verified-sha256-first-frame-exif-rgb-png-v1'
                        if self.image_transport == 'png' else
                        'verified-sha256-first-frame-exif-rgb-original-compatible-v2'))

    @property
    def fingerprint(self):
        return hashlib.sha256(canonical(self.contract()).encode()).hexdigest()
