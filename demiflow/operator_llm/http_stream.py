"""Bounded SSE decoding and per-request Chat Completions assembly.

Transport chunks never become Dataset rows. A completion requires both the
provider's finish marker and [DONE]; EOF is not a successful completion.
"""
import codecs
import json

from .errors import PromptStreamError


class ChatCompletionStream:
    def __init__(self, options):
        self.max_bytes = options.get('max_response_bytes', 8 * 1024 * 1024)
        self.max_event = options.get('max_event_bytes', 1024 * 1024)
        self.max_events = options.get('max_stream_events', 100000)
        self.decoder = codecs.getincrementaldecoder('utf-8')('strict')
        self.buffer = ''
        self.data = []
        self.event_size = 0
        self.content = []
        self.reasoning = []
        self.usage = None
        self.envelope = {}
        self.finish = None
        self.done = False
        self.provider_error = None
        self.last_event = None
        self.last_chunk = None
        self.metrics = dict(bytes_received=0, events_received=0, first_byte_s=None,
                            first_event_s=None, first_content_s=None, max_body_gap_s=0.0)

    def feed(self, chunk, elapsed):
        self.metrics['bytes_received'] += len(chunk)
        if self.metrics['bytes_received'] > self.max_bytes:
            raise PromptStreamError('Response exceeds max_response_bytes')
        if chunk:
            if self.last_chunk is None:
                self.metrics['first_byte_s'] = elapsed
            else:
                self.metrics['max_body_gap_s'] = max(self.metrics['max_body_gap_s'], elapsed - self.last_chunk)
            self.last_chunk = elapsed
        if self.done:
            return
        try:
            decoded = self.decoder.decode(chunk)
        except UnicodeDecodeError as exc:
            raise PromptStreamError('Invalid UTF-8 in SSE response') from exc
        if self.metrics['bytes_received'] == len(chunk):
            decoded = decoded.removeprefix('\ufeff')
        # A UTF-8 BOM can itself cross network chunks.
        if not self.buffer and not self.data and not self.metrics['events_received']:
            decoded = decoded.removeprefix('\ufeff')
        self.buffer += decoded
        while not self.done:
            positions = [p for p in (self.buffer.find('\n'), self.buffer.find('\r')) if p >= 0]
            if not positions:
                break
            end = min(positions)
            if self.buffer[end] == '\r' and end == len(self.buffer) - 1:
                break  # CRLF may be split between chunks.
            line = self.buffer[:end]
            step = 2 if self.buffer[end:end+2] == '\r\n' else 1
            self.buffer = self.buffer[end+step:]
            self._line(line, elapsed)
        if len(self.buffer.encode('utf-8')) + self.event_size > self.max_event and not self.done:
            raise PromptStreamError('SSE event exceeds max_event_bytes')

    def _line(self, line, elapsed):
        self.event_size += len(line.encode('utf-8')) + 1
        if self.event_size > self.max_event:
            raise PromptStreamError('SSE event exceeds max_event_bytes')
        if line:
            field, separator, value = line.partition(':')
            if field == 'data':
                self.data.append(value[1:] if value.startswith(' ') else value)
            return
        self.event_size = 0
        if not self.data:
            return  # comments / heartbeat
        raw = '\n'.join(self.data)
        self.data = []
        self.last_event = raw
        self.metrics['events_received'] += 1
        if self.metrics['events_received'] > self.max_events:
            raise PromptStreamError('SSE response exceeds max_stream_events')
        if self.metrics['first_event_s'] is None:
            self.metrics['first_event_s'] = elapsed
        if raw == '[DONE]':
            self.done = True
            return
        try:
            event = json.loads(raw)
        except ValueError as exc:
            raise PromptStreamError('Malformed JSON in SSE event') from exc
        if not isinstance(event, dict):
            raise PromptStreamError('SSE data must contain a JSON object')
        if 'error' in event:
            self.provider_error = event
            raise PromptStreamError('Provider returned an error inside HTTP 200 SSE')
        for field in ('id', 'model', 'created', 'system_fingerprint'):
            if field in event:
                if field in self.envelope and self.envelope[field] != event[field]:
                    raise PromptStreamError(f'SSE completion {field} changed within one response')
                self.envelope[field] = event[field]
        if event.get('usage') is not None:
            if not isinstance(event['usage'], dict):
                raise PromptStreamError('Invalid SSE usage object')
            self.usage = event['usage']  # cumulative snapshot; never sum chunks
        choices = event.get('choices', [])
        if not isinstance(choices, list) or len(choices) > 1:
            raise PromptStreamError('Expected one streamed completion choice')
        for choice in choices:
            if not isinstance(choice, dict) or choice.get('index') != 0:
                raise PromptStreamError('Unexpected streamed completion choice index')
            delta = choice.get('delta', {})
            if not isinstance(delta, dict):
                raise PromptStreamError('Invalid streamed delta')
            if delta.get('tool_calls') or delta.get('function_call') or delta.get('refusal') or delta.get('audio'):
                raise PromptStreamError('Unsupported non-text completion delta')
            for field, target in (('content', self.content), ('reasoning_content', self.reasoning), ('reasoning', self.reasoning)):
                part = delta.get(field)
                if part is None:
                    continue
                if not isinstance(part, str):
                    raise PromptStreamError(f'Streamed {field} must be a string')
                if self.finish is not None and part:
                    raise PromptStreamError('Content arrived after finish_reason')
                if part:
                    target.append(part)
                    if field == 'content' and self.metrics['first_content_s'] is None:
                        self.metrics['first_content_s'] = elapsed
            finish = choice.get('finish_reason')
            if finish is not None:
                if self.finish is not None:
                    raise PromptStreamError('Duplicate finish_reason in stream')
                self.finish = finish

    def body(self):
        message = {'role': 'assistant', 'content': ''.join(self.content)}
        if self.reasoning:
            message['reasoning_content'] = ''.join(self.reasoning)
        return {**self.envelope, 'object': 'chat.completion',
                'choices': [{'index': 0, 'message': message, 'finish_reason': self.finish}],
                **({'usage': self.usage} if self.usage is not None else {})}

    def complete(self):
        if not self.done or self.finish != 'stop':
            raise PromptStreamError(f'Incomplete SSE response: done={self.done}, finish_reason={self.finish!r}')
        return self.body()

    def snapshot(self):
        return {'transport': 'sse', 'stream': dict(self.metrics), 'stream_complete': self.done and self.finish == 'stop',
                'usage': self.usage or {}, 'partial_response': self.body(),
                'pending_event': '\n'.join(self.data) + self.buffer,
                'last_event': self.last_event,
                **({'provider_error': self.provider_error} if self.provider_error is not None else {})}
