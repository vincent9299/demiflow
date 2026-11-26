# Training and evaluation in a Dataset graph

Training data and the holdout set can each go through Dataset transformations. `fit` creates a reference to the training result, `checkpoints.select` names the model steps to read, and `predict` joins the model reference and the holdout set into one graph and returns a Dataset. Execution is triggered by the existing Dataset actions.

```python
from demiflow import data

train = data.read_lance(train_uri, version=train_version).filter(accept_training_pair)
validation = data.read_lance(validation_uri, version=validation_version)
holdout = data.read_lance(holdout_uri, version=holdout_version).filter(is_scored)

fitted = train.fit(recipe=recipe, monitors={"validation": validation})
scores = (
    fitted.checkpoints.select(steps=[400, 800])
    .predict(on=holdout, inputs={"prompt": "question"})
    .map(score_sample)
)
scores.write_lance(scores_uri, mode="create")
```

The URIs, data versions, row functions and the recipe in this example come from the project. Every selected model reads the whole holdout. The validation set used inside training changes the training identity. When only the downstream holdout, the scoring function or the selected steps change, a committed training result can be reused.

## What the project author provides

| Interface | Contract |
|---|---|
| `data.TrainingRecipe(...)` | Names the `trainer`, the `train_schema`, the named `monitor_schemas`, the effective parameters and their JSON Schema, content digests of the base model and the environment, the optimizer update budget, the save steps, the numeric topology, the model format and the default predictor. Training and monitor inputs are pinned to versions before the job is claimed. The content of external files is verified only for explicitly declared `media_fields`; the deployment provides the roots that may be accessed. |
| `trainer(request, context)` | Needs no import of any engine-internal contract class. `request.inputs` holds the pinned data versions and proofs; write training weights to `context.model_dir` and temporary working files to `context.work_dir`; respond to a stop with `context.check_cancelled()`; use `context.run_group(...)` when a process group is needed. The public ways to deliver checkpoints:<br>1. **Function return**: return `{step: {file name: path}}` (write the integer `step` explicitly even for a single step; to commit resume state as well, write `{"files": {...}, "resume_files": {...}}` for that step), or return `{"checkpoints": {step: ...}, "metrics": {...}}`.<br>2. **Reporting on the way (optional)**: for a step declared in `save_steps`, call `context.report_snapshot(step, files={...}, resume_files={...})` to put the recovery evidence on disk atomically; only when the final step of this job (`budget["optimizer_updates"]`) is in `save_steps` and has been reported may the trainer simply `return None` or `return {"metrics": {...}}`, otherwise the final step must be supplied in the return value. Reported files must keep their content unchanged until the job ends (naming them per step is recommended) so that the engine can verify and import them. Only eligible `interrupted` jobs resume automatically; an active cancellation still goes through termination.<br>3. **Reconciling native snapshots (optional)**: `trainer.reconcile(request, *, model_dir, work_dir)` may return `{"step": int, "files": {...}, "resume_files": {...}}` or `None`; all paths must lie inside the `model_dir` of that launch. |
| `data.PredictorSpec(...)` | Declares the actor factory, the mapping of named inputs to column names, the `input_schema`, the output schema, the compatible model formats, parameters, batch and resources. The actor constructor accepts `model`, `inputs` and `parameters`. It loads the model named by the manifest inside the worker and implements `__call__(batch)`. |
| Prediction batch output | One row per input, in the original order. With `batch_size > 1` the batch input also contains `__demiflow_prediction_input_id`; the actor must return it unchanged row by row, and the worker removes it after checking. For several seeds, return a list of results per row and expand it with `Dataset.flat_map`. The project keeps every failed sample and its reason as detail rows. |
| Model artifacts | The engine verifies that files lie inside the `model_dir` of this launch, computes SHA-256 and byte counts in a stream, and packages, deduplicates and imports them into the project's durable model store. On resume the parent snapshot is used only to read the resume inputs, and the verified earlier steps are merged by the engine. The score table pins the model version used through the actual publication receipt of Lance. |

A minimal `trainer` with no internal imports:

```python
def train_model(request, context):
    checkpoints = {}
    for step in request.semantics["save_steps"]:
        context.check_cancelled()
        weights = context.model_dir / f"weights_step_{step}.bin"
        weights.write_bytes(f"step-{step}".encode())
        # Optional: report as soon as a long training has written a step, so an interruption
        # does not waste the work (the file must keep its content until the import is done)
        context.report_snapshot(step, files={"weights": weights})
        checkpoints[step] = {"weights": weights}
    return {"checkpoints": checkpoints, "metrics": {"final_step": step}}
    # If the final step is in save_steps and every step was reported with report_snapshot,
    # `return None` or `return {"metrics": {...}}` is also allowed
```

For the exact constructors and runnable examples see the [public declarations](../../demiflow/data/training.py) and the [test that writes a table through the public action](../../tests/test_training_dataset_action.py).

## Where it runs

| Deployment | Launch and takeover |
|---|---|
| `EngineManaged` | The engine allocates a whole group of resources from the action's resource limits and the device pool. Only after the unit is ready can the driver that holds the write permission of the original run authorize the project code to run. The built-in implementation supports a single machine with CPU or several GPUs. The deployment provides per-job CPU, memory, interpreter and an allow-list of child process modules; the numeric topology is declared separately by the recipe. |
| `ProviderManaged` | The deployment provides `submit / lookup / inspect / result / cancel` with a fixed scope, an implementation digest and a deadline per call. The third party is responsible for devices and processes. When the submission receipt is unclear, query with the original request key. Resending under the same key requires the service's idempotency commitment, evidence that the original request was not accepted, and a validity period. A paid deployment must also have an enforceable cost limit. |

Python can pass `TrainingDeployment(project_id, binding, provider)` to `RunContext.start(..., training=...)`. The Workbench reads the absolute path of the deployment JSON from the environment variable `DEMIFLOW_TRAINING_DEPLOYMENT`; the configuration declares `project_root`, `project_id` and `binding`, and a third party can name a trusted `provider_factory`. The project index uses SQLite on the same control host, and model import currently uses a durable project directory reachable from the local machine. The `allocated_gpu_seconds` of an engine-managed job run from the actual allocation in the original device pool to the confirmed release, including the time GPUs were held by every training launch of the same instance. See the [deployment configuration](../../demiflow/execution/pipeline/training_deployment.py).

## When the model quota is used up

Four quotas are approved in one place, the project control directory the deployment points to. Several `project_id`s sharing one `fit.sqlite` share the quotas. For isolation, configure a separate control directory and model store for each.

| Deployment field | What it measures |
|---|---|
| `max_model_bytes`, `max_files` | Total bytes and number of objects of model files and manifests after content deduplication, including reservations for imports in progress. |
| `max_instances` | Total number of training instances in the index, in any state. Reusing an instance needs no new slot. |
| `max_references` | Total of pending or confirmed references from score tables, parent-snapshot references and references still in use. |

The administrator keeps the original trusted deployment file and stores a new deployment with higher quotas for later runs. A new run that uses this deployment for the first time applies the quotas when its training action is registered. The administrator can also apply them in advance and then directly resume an eligible original run.

```python
from demiflow.execution.pipeline.training_deployment import load_training_deployment

deployment = load_training_deployment(deployment_v2_path, project_root=project_root)
deployment.apply_project_quotas()
```

The four quotas are compared one by one inside one transaction of the project index; each may stay equal or rise. If any of them falls, the whole change is rolled back. The original control host, the model store and the cost limit of an already registered currency are still checked strictly. Existing instances, weights, score tables and references are kept.

After raising the quotas, first check the state of the original run and instance. Resuming from the Workbench still verifies the path and SHA-256 of the original deployment file.

| Recorded state | What to do next |
|---|---|
| The fit is registered, the original training can still be taken over, and the score table allows recovery | Resume the original run; it uses the approved quotas with the original request key and the original training and model identities. |
| The training was accepted, the driver has stopped, and the original score table does not meet the replay conditions | After raising the quotas run `demiflow workbench run reconcile ROOT RUN_ID --expected-attempt N` on the original run, replacing N with the actual number of the original attempt. Reconciliation can publish the model manifest of the original job; the returned value is still the state of the original run and may remain `interrupted`. Once the model is published, start a new scoring action with the same training key and a writable target. |
| The original run was interrupted before the fit was first registered | The four quotas of the original deployment must match the established project index exactly. If they have changed, start a new run with the current deployment. |
| The training instance was cancelled or failed and has no published manifest | To train again, the new run names a new `retrain_id` explicitly. |
| The model is published and the score table failed for lack of reference slots | After raising the reference quota, start a new scoring action with the same training key and reuse the original model. The scoring target must still satisfy the usual write rules. |

An older version may have registered the deployment digest and exited before `fit.sqlite` was first created. Only when it is certain that this control directory never had an index and never submitted a training may the administrator run `apply_project_quotas()` with the original trusted deployment to create it and then resume. When an index or weights that were in use are lost, restore the original database and model objects from backup first.

## Reuse, retraining and recovery

- Within one project the training identity is determined by the pinned training and monitor data, the recipe code, the environment, the base model, the effective training parameters, the budget, the save steps, the numeric topology and an explicit parent snapshot. Concurrent consumers share one producer, and only the producer requests training resources. Selecting a step that was not saved is an error. An explicit retraining uses a stable `retrain_id`; an explicit continuation passes `resume_from` and declares the new total update budget in the recipe.
- When the driver exits before the first prediction row is committed, resume the **original run** from the Workbench. The engine takes the accepted fit back along the original action. An explicit `create` can continue when the absence of the original target was registered and it can still be shown at recovery that nothing external wrote to the target. With the default `append` and no row committed yet, or when the target already holds a foreign table, the engine refuses to replay. An accepted job can be reconciled under the authorization of the original run; continuing the evaluation requires the recovery conditions of the original terminal.
- Once a row recovery point exists, prediction, ordinary row processing and table writing share the existing row recovery protocol. The weight reference must agree with the prediction node actually processed at that cut. The recoverability of an actor follows the state check of ordinary batch operators. An actor holding model tensors or other state that cannot be saved automatically should provide a matching pair of `snapshot_state` / `restore_state`; a Qwen actor that can be rebuilt saves and re-verifies only the identities of the model and the predictor. The training framework manages optimizer and RNG snapshots. Only after the exit of the old training group is confirmed may a verified snapshot be used to recover, and the number of recoveries is bounded by the deployment.
- The training budget can state `optimizer_updates` and `max_epochs`. When an epoch limit is declared, also provide `topology.effective_batch`; by default the samples available per epoch are estimated from the row count of the pinned training table, and a project with a verified traversal rule can provide `budget.examples_per_epoch`. An unreachable number of updates is refused before training resources are allocated.
