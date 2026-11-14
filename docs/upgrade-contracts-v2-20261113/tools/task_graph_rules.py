"""Check planned wiring authority; does not prove runtime reachability."""


def wiring_errors(tasks):
    by_id = {task['id']: task for task in tasks}
    errors = []
    for producer in tasks:
        if producer['baseline_line'] is not None or producer['owner'] == 'V':
            continue
        callers = [by_id[cid] for cid in producer['caller_tasks']]
        if not any(caller['mode'] == 'implement' for caller in callers):
            errors.append(f"{producer['id']}: new symbol has no implement caller")
        for caller in callers:
            if caller['mode'] == 'verify_preserve':
                errors.append(f"{caller['id']} -> {producer['id']}: preserve caller cannot add new wiring")
    return errors
