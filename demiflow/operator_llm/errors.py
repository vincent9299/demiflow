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


class PromptResponseParseError(PromptError, ValueError):
    pass


class PromptResponseContractError(PromptError, ValueError):
    pass
