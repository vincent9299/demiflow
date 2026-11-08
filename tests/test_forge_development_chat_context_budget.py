import hashlib

import pytest

from demiflow.workbench.conversations.context import assemble
from demiflow.workbench.conversations.policy import Limits
from demiflow.forge.schema import ForgeError


def test_large_inherited_guidance_requires_explicit_budget_and_is_delivered_whole(tmp_path):
    repository = tmp_path / 'repository'
    project = repository / 'pipeline'
    project.mkdir(parents=True)
    guidance = ('Keep original data and notebook output.\n' * 8000)
    specification = '# Pipeline specification\nUse the existing operators.\n'
    (repository / 'AGENTS.md').write_text(guidance)
    (repository / 'PIPELINE_SPEC.md').write_text(specification)
    (project / 'README.md').write_text('Original pipeline.\n')
    grant = dict(root=str(project), kind='unregistered', phase='implementation',
                 writes=['code.py'], watch=['code.py'], sources=['README.md'],
                 non_goals=['production execution'], authorization='Explicit fixture development')
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    with pytest.raises(ForgeError, match='context exceeds'):
        assemble(grant, 'Implement only the requested fixture.', scratch, Limits())
    limits = Limits.parse({'context_bytes': 512 * 1024})
    manifest, prompt = assemble(grant, 'Implement only the requested fixture.', scratch, limits)
    assert guidance in prompt and specification in prompt
    assert len(prompt.encode()) <= limits.context_bytes <= limits.context_total
    for name in ('AGENTS.md', 'PIPELINE_SPEC.md'):
        path = repository / name
        rows = [row for row in manifest['sources'] if row['canonical_path'] == str(path)]
        assert len(rows) == 1
        assert rows[0]['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert (repository / 'AGENTS.md').read_text() == guidance
    assert Limits.parse({}).context_bytes == 128 * 1024
    (repository / 'AGENTS.md').write_text('x' * (512 * 1024))
    with pytest.raises(ForgeError, match='context exceeds'):
        assemble(grant, 'Still bounded.', scratch, limits)


@pytest.mark.parametrize('value', [
    {'context_bytes': 512 * 1024 + 1},
    {'context_bytes': 512 * 1024, 'context_total': 128 * 1024},
    {'context_total': 512 * 1024 + 1},
])
def test_context_budget_rejects_excessive_or_inconsistent_limits(value):
    with pytest.raises(ForgeError):
        Limits.parse(value)
