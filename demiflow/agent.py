"""The single configuration entry for agentmap; no business argument conversion."""
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import yaml

from .environment import OperatorEnvironment, LIMITS
from .operator_llm.errors import PromptPackError
from .operator_llm.model import PromptModel, PromptPack, PromptPackVersion, parse_prompt_model
from .operator_llm.parser import _inspect_prompt


@dataclass(frozen=True)
class AgentConfig:
    """Resolved agent entry. Execution settings live here, row mappings on the node."""
    prompt_pack: PromptPack
    environment: OperatorEnvironment
    options: dict
    max_requests: int

    def __post_init__(self):
        if not isinstance(self.prompt_pack, PromptPack) or not isinstance(self.environment, OperatorEnvironment):
            raise TypeError('AgentConfig requires a prompt definition and an operator environment')
        if type(self.max_requests) is not int or self.max_requests < 0:
            raise ValueError('Agent max_requests must be a finite nonnegative integer')
        options = deepcopy(self.options)
        if not isinstance(options, dict):
            raise TypeError('Agent options must be a mapping')
        if set(options) & {'codex_exec', 'offline_dir', 'offline_store', 'sqlite_journal', 'journal_dir'}:
            raise ValueError('Agent configuration owns runtime settings; journals/replay belong to the node')
        if self.environment.runtime == 'codex':
            from .operator_llm.codex_agent import validate_options
            options.setdefault('codex_agent', {})
            validate_options(options)
        else:
            from .operator_llm.http_options import validate_http_options
            validate_http_options(options)
            if any(options.get('request_options', {}).get(key) for key in
                   ('tools', 'functions', 'tool_choice', 'function_call', 'web_search_options')):
                raise ValueError('Agent external interactions must use environment operators')
        for prompt in self.prompt_pack.prompts:
            if (prompt.model.transport == 'codex') != (self.environment.runtime == 'codex'):
                raise ValueError('Agent model transport must match its runtime')
            self.environment.prompt(prompt)
        object.__setattr__(self, 'options', options)

    def node_options(self, options=None):
        """Only storage/replay may vary per invocation; no second runtime config."""
        storage = deepcopy(dict(options or {}))
        if set(storage) - {'sqlite_journal', 'journal_dir', 'offline_store', 'offline_dir'}:
            raise ValueError('Agent model, tools and runtime options belong in config, not node options')
        offline = set(storage) & {'offline_store', 'offline_dir'}
        if offline:
            if self.environment.runtime == 'codex' or len(storage) != 1:
                raise ValueError('Offline submission requires a demiflow agent and one offline store')
            return storage
        result = {**deepcopy(self.options), **storage}
        if self.environment.runtime == 'codex':
            from .operator_llm.codex_agent import validate_options
            validate_options(result)
        return result


def load_agent_config(path: str | Path) -> AgentConfig:
    """Read one complete agent file; never load a second task/prompt file."""
    with Path(path).open('rb') as source:
        encoded = source.read(1024 * 1024 + 1)
    if len(encoded) > 1024 * 1024:
        raise PromptPackError('Agent configuration exceeds 1 MiB')
    raw = yaml.safe_load(encoded)
    fields = {'schema_version', 'runtime', 'model', 'tasks', 'operators', 'operator_settings',
              'resources', 'budgets', 'options'}
    if not isinstance(raw, dict) or raw.get('schema_version') != 'demiflow_agent_v2' or set(raw) - fields:
        raise PromptPackError('agentmap config requires demiflow_agent_v2 (not a prompt pack or v1 policy)')
    runtime = raw.get('runtime')
    model_raw = raw.get('model')
    if runtime == 'codex':
        if (not isinstance(model_raw, dict) or set(model_raw) != {'name'}
                or not isinstance(model_raw['name'], str) or not model_raw['name'].strip()):
            raise PromptPackError('Codex model requires only a nonempty name; provider belongs to Codex')
        model = PromptModel(model_raw['name'], 'codex')
    elif runtime == 'demiflow':
        model = parse_prompt_model(model_raw, label='agent model')
    else:
        raise PromptPackError('Agent runtime must be codex or demiflow')
    operators = raw.get('operators')
    if not isinstance(operators, (list, dict)):
        raise PromptPackError('Agent operators must be a mapping or a list of standard names')
    budgets = raw.get('budgets')
    if (not isinstance(budgets, dict) or 'max_requests' not in budgets
            or set(budgets) - {'max_requests', 'max_operator_calls', *LIMITS}):
        raise PromptPackError('Agent budgets require max_requests and only declared environment limits')
    if runtime == 'codex' and set(budgets) & {'max_turns', 'max_calls_per_turn'}:
        raise PromptPackError('Codex owns model turns; use session/operator budgets')
    environment = OperatorEnvironment(runtime=runtime, operators=tuple(operators) if isinstance(operators, list) else operators, resources=raw.get('resources'),
        operator_settings=raw.get('operator_settings', {}),
        **{key: value for key, value in budgets.items() if key != 'max_requests'})
    tasks = raw.get('tasks')
    if not isinstance(tasks, dict) or not tasks or len(tasks) > 64:
        raise PromptPackError('Agent tasks must contain 1..64 inline definitions, not a file reference')
    task_fields = {'version', 'template', 'response_schema', 'schema_retries', 'response_format'}
    definitions = []
    for name, task in tasks.items():
        if not isinstance(name, str) or not name.strip() or not isinstance(task, dict) or set(task) - task_fields:
            raise PromptPackError('Inline tasks contain only version, template and response contract fields')
        # Reuse the task/schema validator. The model has already been validated
        # for this runtime; it is not a fictitious HTTP configuration.
        definition, issues = _inspect_prompt(name, task, model=model)
        if issues:
            raise PromptPackError('; '.join(issue.message for issue in issues), issues)
        definitions.append(definition)
    identity = hashlib.sha256(json.dumps(raw, sort_keys=True,
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    pack = PromptPack(PromptPackVersion.V2, tuple(definitions), 'sha256:' + identity)
    return AgentConfig(pack, environment, raw.get('options', {}), budgets['max_requests'])
