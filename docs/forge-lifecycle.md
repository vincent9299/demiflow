# Forge v1 生命周期与 CLI

本文说明旧 v1 项目的 Check → Verify → Review → Freeze 流程。当前原项目的新建、登记、统一检查与受管实验使用 [工作区指南](forge-workspace.md) 中的 console/workspace/flow 入口；登记或识别结果不继承本页的生命周期状态。

## 开发到运行

```bash
demiflow forge schema request
demiflow forge new request.json --out my-pipeline --json
demiflow forge context my-pipeline --json

# 人类或 Coding Agent 只实现候选文件。
demiflow forge manifest my-pipeline --json
demiflow forge check my-pipeline --json
demiflow forge dry-run my-pipeline --config my-pipeline/run.example.json --json
demiflow forge verify my-pipeline --json
demiflow forge status my-pipeline --json
```

`check` 是快速静态门禁；`verify` 另外执行 dry-run 和受控 pytest，并比较测试前后的源码快照。测试修改项目也会失败。`status` 实时报告 `draft/implemented/tested/reviewed/frozen`、每个门禁的 `missing/stale/failed/corrupt/passed` 和下一步；日志丢失或内容变化会使 Verify 为 `corrupt` 并阻断运行。

## Review 与 Freeze

当前 Verify 通过后，由人类或独立 reviewer 提交：

```json
{
  "schema_version": "demiflow_forge_review_v1",
  "revision": "<status 显示的当前 revision>",
  "reviewer": "reviewer-name",
  "review_type": "independent",
  "status": "passed",
  "summary": "实现、契约和真实行为测试已审查。",
  "findings": []
}
```

执行 `demiflow forge review my-pipeline --file review.json --json`，再执行 `demiflow forge freeze my-pipeline --json`。通过 Review 不能包含 blocker；`review_type=self` 只表示自审，不冒充独立审查。Freeze 后源码或契约变化会使 Verify、Review 和 Freeze 全部过期。

## 本地真实运行

```bash
demiflow forge run my-pipeline --config my-pipeline/run.example.json --json
```

只有当前 revision 已 Freeze 才能运行。结果包含 Lance committed version、输入/写入行数和目标 URI。首版只允许项目内 `datasets/` 的本地 Lance 读写，不执行生产 Pipeline。

## 命令与退出码

其他 introspection 命令为 `rules`、`context`、`describe`、`manifest`。成功返回 0；契约、检查、验证或生命周期失败返回 1；Click 参数用法错误返回 2。JSON 错误写 stderr，不混入结果 stdout。现有 `demiflow run --root ... --backend ...` 是不可变 bundle 的旧命令，与 `demiflow forge run` 的项目生命周期不同，二者不能混用。

证据位于项目外 `.demiflow/forge/<project-name>/`。它不是防篡改签名，也不是业务质量或操作系统安全证明。业务效果需要单独固定评测；首版不自动执行评测或修订循环。
