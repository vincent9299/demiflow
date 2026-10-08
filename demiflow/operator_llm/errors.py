"""Prompt contract and execution errors."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class PromptContractDiagnostic:
    code: str
    message: str
    field_path: str = ""
    prompt: str = ""
    line: int = 0
    column: int = 0



class PromptError(Exception):
    """Base error for demiflow prompt capabilities."""


class PromptPackError(PromptError, ValueError):
    """A prompt pack is structurally invalid."""

    def __init__(self, message: str, diagnostics=()):
        self.diagnostics = tuple(diagnostics)
        super().__init__(message)


class PromptPackVersionError(PromptPackError):
    pass


class PromptDefinitionError(PromptPackError):
    pass


class PromptTemplateSyntaxError(PromptPackError):
    pass


class PromptRoleNotFoundError(PromptError, KeyError):
    pass


class PromptArgumentError(PromptError, ValueError):
    pass


class PromptArgumentMissingError(PromptArgumentError):
    pass


class PromptArgumentUnexpectedError(PromptArgumentError):
    pass


class PromptArgumentTypeError(PromptArgumentError, TypeError):
    pass


class PromptCapabilityUnavailable(PromptError, RuntimeError):
    pass


class PromptProviderUnavailableError(PromptError, RuntimeError):
    pass


class PromptProviderCapabilityError(PromptError, RuntimeError):
    pass


class PromptBudgetExceededError(PromptError, RuntimeError):
    pass


class PromptReplayMissError(PromptError, RuntimeError):
    """Read-only replay has no saved response for the exact request."""


class PromptResponseParseError(PromptError, ValueError):
    pass


class PromptResponseContractError(PromptError, ValueError):
    pass


class PromptStreamError(PromptError, RuntimeError):
    """The HTTP response did not deliver one complete supported SSE completion."""


def error_category(error):
    """Stable technical categories, including legacy serialized error records.

    These are execution outcomes, not business review states. HTTP failure takes
    precedence over parse/contract errors caused by a provider error envelope.
    """
    if not isinstance(error,dict):
        error={'type':type(error).__name__,'call':getattr(error,'call',{})}
    if error.get('category'): return error['category']
    status=(error.get('call') or {}).get('http_status')
    if status is not None and status!=200: return 'provider_error'
    return {'PromptResponsePending':'pending_response','PromptResponseContractError':'invalid_response',
            'PromptResponseParseError':'invalid_response','InputTokenBudgetExceeded':'input_budget',
            'PromptBudgetExceededError':'request_budget','UncertainPromptCall':'uncertain_call',
            'PromptStreamError':'incomplete_response'}.get(error.get('type'),'provider_error')
