"""Bind the Workbench turn identity, time budget and provider to its fixed model channel."""
import json
import math
from pathlib import Path

from demiflow.forge.bridge.model_channel import ModelChannel, capabilities
from .common import identifier
from .deepseek import DeepSeek
from .policy import turn_deadline


def _quota(home, turn):
    owner = identifier(json.loads(turn.get('forge_options') or '{}').get('quota_turn', turn['id']))
    return Path(home).parent.parent / (owner + '.model-budget.json'), owner


class ModelBroker(ModelChannel):
    def __init__(self, deployment, turn, session, home, *, defer_handshake=False, request_seconds=180, deadline=None):
        provider = session['provider']
        expires = turn_deadline(turn, deployment.limits) if turn is not None else math.inf
        super().__init__(provider, deployment.providers[provider], home,
                         deadline=min(expires, deadline) if deadline is not None else expires,
                         maximum=deployment.limits.output_bytes, stderr_bytes=deployment.limits.stderr_bytes,
                         quota=_quota(home, turn) if turn is not None else None,
                         rpc_launch=DeepSeek.launch, defer_handshake=defer_handshake,
                         request_seconds=request_seconds)

    def begin_turn(self, turn, limits):
        self.bind_turn(turn_deadline(turn, limits), _quota(self.home, turn))


def start_model_broker(deployment, turn, session, home, *, defer_handshake=False, request_seconds=180, deadline=None):
    return ModelBroker(deployment, turn, session, home, defer_handshake=defer_handshake,
                       request_seconds=request_seconds, deadline=deadline)
