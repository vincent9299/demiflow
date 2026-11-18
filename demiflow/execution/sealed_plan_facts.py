"""Sealed plan facts of one action, for hosts that display history.

A host showing "the plan this action actually ran" should not need to know that
a v3 seal is recorded under ``<action>-dataflow-sealed``, an original F1 seal
under ``<action>-sealed-plan`` and an identity bridge under
``<action>-identity-bridge``, nor which loader and view builder belong to which
generation. These readers name those facts once, in the engine.

Read-only: the journal is only read, sealed artifacts are loaded through the
existing bounded readers, nothing is recompiled and no plan is re-sealed.
"""
from .plan_interpreter import PlanContractError

__all__ = ['PlanContractError', 'recovery_restriction', 'sealed_plan_view',
           'sealed_plan_binding', 'identity_bridge']

_V3_SEAL = '-dataflow-sealed'
_F1_SEAL = '-sealed-plan'
_IDENTITY_BRIDGE = '-identity-bridge'
_RECOVERY_RESTRICTION = '-state-recovery-restriction'


def _journal(directory, journal):
    if journal is not None:
        return journal
    from .run_journal import RunJournal
    return RunJournal(directory)


def recovery_restriction(directory, action_id, *, journal=None):
    """The recorded state-recovery restriction of an action, or ``None``."""
    return _journal(directory, journal).source_binding(action_id + _RECOVERY_RESTRICTION)


def sealed_plan_view(directory, action_id, *, journal=None):
    """The v3 sealed plan of an action as the single plan view, or ``None``.

    ``None`` means the action has no v3 seal (an original F1 run, or no seal at
    all); use :func:`sealed_plan_binding` for the original generation. The
    original artifacts are read back; current code is never recompiled to
    impersonate history.
    """
    journal = _journal(directory, journal)
    marker = journal.source_binding(action_id + _V3_SEAL)
    if marker is None:
        return None
    from .sealed_plans import load_sealed_plan
    from .dataflow_history import build_plan_view
    bundle = load_sealed_plan(directory, action_id)
    view = build_plan_view(bundle['graph'], bundle['sealed'], bundle['physical'], observations=())
    return dict(version='v3', action_id=action_id,
                dag_id=bundle['graph'].dag_id, plan_id=bundle['physical'].plan_id,
                sealed_at=marker.get('sealed_at') if type(marker) is dict else None,
                plan_view=view,
                recovery_restriction=journal.source_binding(action_id + _RECOVERY_RESTRICTION))


def sealed_plan_binding(directory, action_id, *, journal=None):
    """The original (F1) sealed-plan binding of an action, or ``None``.

    Returns ``{'facts': <recorded binding>, 'engine_run_id': <recovery identity
    run id or None>}``. The binding is returned as recorded; the host validates
    its schema and reads the carriers it references.
    """
    journal = _journal(directory, journal)
    facts = journal.source_binding(action_id + _F1_SEAL)
    if facts is None:
        return None
    binding = journal.recovery_identity()
    return dict(facts=facts, engine_run_id=binding.get('run_id') if type(binding) is dict else None)


def identity_bridge(directory, action_id, *, max_bytes, journal=None):
    """The sealed identity bridge of an action, or ``None`` when none was sealed.

    Returns ``{'ref': <recorded reference>, 'bridge': <artifact document>}``.
    Raises ``PlanContractError`` (with ``reason`` and ``detail``) when a recorded
    bridge cannot be read safely. Schema and identity checks of the document
    stay with the host. The bridge is never generated here.
    """
    ref = _journal(directory, journal).source_binding(action_id + _IDENTITY_BRIDGE)
    if ref is None:
        return None
    from .sealed_plans import read_plan_artifact
    return dict(ref=ref, bridge=read_plan_artifact(directory, ref, max_bytes=max_bytes))
