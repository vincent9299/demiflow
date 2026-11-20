"""Compatibility entry for queue settlement.

The migration contract keeps the chain/graph/queue_chain public entries and has them hand
the request to the v3 runtime. The driver-side transform carrier that used to live here (a
per-task shuttle to a managed worker, and its eligibility probe) is retired: queue
acquisition, the pure-maps suffix and ``finalize_task`` settlement run through
``execute_action``.
"""


def run_local_queue_chain(executor, source_plan, plan, sink, action, *,
                          resource_root=None, control_timeout_s=120):
    """Compat entry: translate legacy queue settlement into one v3 write_datasink action.

    The independent driver resource/transform-carrier/commit loop is retired; queue
    acquisition, the pure-maps suffix and ``finalize_task`` settlement now run
    through the unified ``execute_action`` runtime (E001). The confirmed sink
    ``WriteResult`` is returned unchanged. ``resource_root``/``control_timeout_s``
    are kept for signature compatibility and no longer select a separate worker.
    """
    from .action_driver.dataflow import execute_action
    from .dataflow_api import make_action_request
    from ..planning.dataflow_policy import resolve_runtime_policy

    request = make_action_request(executor, source_plan, plan, kind="write_datasink",
                                  arguments={}, sink=sink)
    policy, bounds = resolve_runtime_policy({}, {}, provider="local")
    return execute_action(request, policy=policy, bounds=bounds).result()
