# 完整 Dataset 标杆：参考图、联合出题、作答与两路评分

这是独立副本，不导入 `demiwtg` 的生产 launcher，不改原业务代码、训练服务或数据。
完整流程入口为 `flows/reference.py:run`，独立审核入口为 `flows/quality.py:run`。
预览和原生执行读取同一份 Dataset 声明：**一个最终
`write_lance` action**，中途通过原生 `save_lance` 固定题图交付，再把原行与实际提交的
URI/version 送往下游。没有先在 Python 中运行多个阶段、再拼一张展示图。

默认示例的模型响应全部来自 `fixture.py` 本地 HTTP 服务。它不是 Boogu，也不会做真实
视觉评审。实际执行的部分是 Dataset 编译、分支、共享前缀、HTTP transport、图片对象、
原生模型 journal、固定 Lance 交付和最终提交。输出每行持久化
`execution_kind=fixture`，响应理由也标明模拟；这些分数不能用于模型质量判断。

## 真实语义与算子

| 用户看到的阶段 | 实际 Dataset 操作 | 边界 |
|---|---|---|
| 概念身份、已有图、关系 | 三个固定版本 `read_lance`，关系 `filter`，两个 `join` | 没有 callback 偷读表 |
| 图片质量 | `read_object` → 原版质量 `map_prompt_async` → `filter` | 图片与回复 ID 对齐，九项检查齐备 |
| 概念参考价值 | 有界 `reduce_by_key` → 概念 `left join` → 原版 focus prompt | 无候选概念仍保留；每概念至多 8 张 |
| 联合出题 v4.1 | 原版 v4.1 prompt → `flat_map` 两组方案 | 每组包含短/中/长三档；50/100/170 上限 |
| 复用/合成/未完成三路 | 三个真实 `filter` 分支 | 分支共享同一个出题 Dataset，未完成位置不消失 |
| 合成参考 | 原生 image prompt → SHA 对象引用 → `read_object` | 不复制 started/complete 业务账本，不调用生产 CLI wrapper |
| 合成图看图定稿 | 原版 v4.1 `phase=finalize` prompt | 必须只选当前实际合成图，四项检查通过才可作答；不再合成 |
| 固定题图交付 | `union` → 内容摘要 → `save_lance` | 保留所有两组位置，提供实际微批 URI/version；非整 action 原子发布 |
| Boogu 有参/无参 | `flat_map` 三档×两条件 → 原生 image prompt | 每对条件题面相同，只改变是否提供固定参考图 |
| 概念呈现 | 共用实际作答图 → 原版概念 v22 prompt | 不用参考图当 GT，不添加评分子问题 |
| 文本遵循 | 同一张作答图 → 原版 alignment v1 prompt | 保留原始十三类维度及原始回答 |
| 汇总对照 | 评分 `union` → 有界 `reduce_by_key` → 最终 `write_lance` | 每概念/档位/条件/评分路固定分母 2，失败与 uncertain 分列 |

模型 label 是实际算子的呈现信息，不产生额外逻辑节点或另一份 DAG。最终评分细节和
汇总共享同一个评分 Dataset；不会为汇总再次作答或评分。

`prompt_sources.json` 记录五份提示词的来源和原字节 SHA256。构造时核对副本摘要，
只替换显式模型 profile，不改原模板或 schema。复用的是业务意图、提示词和纯字段校验；
原生产运行目录、可恢复状态文件、训练加载器与自定义并发器没有带入本例。

## 运行与工作台

在本目录启动以下独立测试服务；`--root` 必须是新的 `/tmp/demiflow-*` 空目录：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/absolute/isolated/demiflow \
  /absolute/env/bin/python -B fixture.py \
  --root /tmp/demiflow-refgen-reference-example
```

服务显式创建三份小型测试源和 `params.json`（外层为 `{"arguments": ...}`），打印 endpoint 后保持运行。
工作台项目根目录取本例目录，按明确登记的 `flows/` 入口显示两条流程。
部署时把生成配置中的业务值分别写入各流程唯一的 `settings/reference.json`、
`settings/quality.json`，把 `models` / `execution_kind` 写入 `settings/runtime.json`。
参数文档沿用 `{"arguments": ...}` 格式，模型部署文档是这两个键组成的对象。
工作台不提供参数文件选择器；`flow.py:run` 只为原测试和直接调用保留兼容。
实际执行环境设置公开模拟值
`REFGEN_FIXTURE_KEY=synthetic-fixture-only`；它不是服务密钥，禁止改用真实业务 token。
source、对象、题图表、结果表与运行目录均应在本次专属 `/tmp/demiflow-*` 下；不要指向
生产目录。只读预览不需要启动模型服务或读表，但必须使用同一份明确的配置。

`presentation.json` 提供 v2 结果阅读预设：题面、参考图、生成图、条件、评分路、分数与分母。
生成图和本路分数属于同一输出方案，查看时并列呈现；汇总另存为只含分母材料的视图。
登记到工作台时由真实固定表产生 `source_snapshot` 和 Case 身份，不能自行编造摘要。
图片字段需要显式绑定本次公开对象根目录和 SHA。默认不投影较大的 `details_json`，
完整调用记录仍在结果表中，按单行读取即可；不会修改或删减执行结果。评分与汇总
使用不同视图，均显式标为模拟模型响应。保存视图只是呈现配置，不代表受管运行认证。

入口参数：

| 参数 | 形态 |
|---|---|
| `sources` | `concepts` / `images` / `relations` 各为 `{uri: 绝对路径, version: 正整数}` |
| `object_root` | 显式允许读取图片对象的绝对根；完整实验的输出目录也位于此根内。更换输出子目录不改变输入图片位置 |
| `output_root` | 本次专属绝对目录；产生 `questions.lance` / `results.lance` / 原生对象目录 |
| `max_concepts` | 1..4；本例的有限容量声明，默认 fixture 取 1 |

独立审核使用 `max_images`（1..32）代替 `max_concepts`。部署文档中的 `models`
包含八个显式 profile；`execution_kind` 必须为 `fixture` 或 `real`，原样进入持久结果，
不在流程业务参数中混入模型连接、算子默认配置或 Prompt 内容。

三份输入分别为：`concept_id/concept/description`、`image_id/image{uri,sha256}`、
`concept_id/image_id/relation_decision`。输入图片需位于显式 `object_root`，逐张 SHA 验证。
本例的图片读取上限为 64 KiB，模型输入数和请求数均有有限上限，面向小型验收和读图。
扩大到生产图片规模需要明确调整本例配置与资源验收，不能把模拟验收解释为生产规模已通过。

真实模式必须提供支持原生 image/JSON 协议的显式服务入口与固定模型版本。Boogu 服务
应自行承担模型加载、权重/adapter 固定和其推理语义；不能在普通行 callback 中装入
旧训练 launcher。本例**没有验证真实 Boogu 权重、CFG、无种子采样或真实评分质量**。
有参/无参的题面与图片绑定已显式表达；真实服务是否准确实现相应条件需单独验收。
fixture 的请求 profile 记录原联合实验的 50 步、text guidance 4.0、image guidance 1.0、
seed=null 与 unseeded-native-v1，透传到请求并进入结果详情。模拟服务只记录这些字段，
不会执行这些推理步骤；这不是原 Boogu sampler/CFG 已被实现的证据。

## 验收

`tests/test_refgen_reference_case.py` 包含五项：

1. 捕获真实编译结果，断言唯一 terminal、所有八个模型阶段只出现一次、真实分支与
   双源边完整；不存在源文件时也不得读取输入、启动 HTTP 或 worker。
2. 一个概念、两组题、三档×两条件的完整真实链路：2 次质量、1 次参考价值、1 次出题、
   1 次参考合成、1 次看图定稿、12 次作答、24 次评分。核对图片对象、固定题图版本、
   24 条细节与 12 条汇总；最终 action 已 ACK、外层随后中断时，恢复不得追加请求或表版本。
   整个 RunContext 已完成时直接读取其固定结果，不创建新的 resume attempt。
3. 控制一张作答图 transport 失败，两路评分都保留失败位置，24 条评分分母不缩小，
   其余 22 次评分真实走同一个 HTTP transport。
4. 质量审核 transport 失败时，不把审核未完成当作无合格图；阻断该概念后续出题，仍
   留下两个明确失败的题组位置。明确的质量 reject 与请求失败分别处理。
5. 输入图片不可读时不调用模型，仍提交 24 个失败评分位置与 12 个固定分母汇总。
   完整黄金路径另核对输入对象根与输出子目录分离，恢复仍不重复调用或提交。

执行时从隔离代码树通过 `python -m pytest` 并显式绑定 `PYTHONPATH`，测试临时根取
`--basetemp=/tmp/demiflow-refgen-tests-<unique>`。测试只替换外部模型响应，没有替换
Dataset、编译器、journal、writer、图或执行结果。验收结果应以当次日志和源码摘要为准，
本说明不自行宣称测试已经通过。

## 作为 Forge 规范样例的约束

- 规范检查对象是入口和配置可达的实际调用链，不是“一行一个算子”。允许纯 helper、
  有界组聚合、当前行指定对象的有界读取和编码；禁止普通 callback 隐藏子 Dataset、
  整阶段运行、模型服务调用、明细表写出或决定模型重放的业务状态文件。
- 固定输入、显式条件分支、模型节点与业务交付必须映射到真实编译图。检查共享前缀只
  执行一次、配置的阶段是否确实可达、失败分母是否保留，不能靠节点数量“认证”。
- 静态 import/ownership 检查不能证明任意 Python helper 无副作用；应把配置、源码、
  提示词摘要和受控 fixture 的运行证据绑定后才给验收结论。本例已按 candidate.10
  登记明确的 flows/operators/prompts，尚未取得完整 Forge 项目验收认证，不能继承
  原生产 pipeline 的 certification。
- 分阶段生产入口各有一个真实 action 时，first-action 预览只展示该入口当前 action，
  不能声称自动追踪多个历史表和独立运行。本例通过一个真实 lazy 图覆盖完整流程。

## 多流程与可复用定义（2026-11-18）

工作台直接读取本项目。`flows/reference.py:run` 是完整参考图对照实验；
`flows/quality.py:run` 是独立图片质量审核。二者共用 `refgen_operators.py`
与 `prompts/quality.yaml`，不互相调用流程入口。原 `flow.py` 只保留旧夹具调用兼容入口。

每个流程只绑定一个当前业务参数文件：`settings/reference.json`、
`settings/quality.json`。文件由隔离预览部署生成，不提交地址或运行状态。
业务参数包含固定输入 Dataset、输出位置及业务数量上限；模型连接配置属于
`settings/runtime.json`，算子默认值位于代码签名，Prompt 定义与输出协议位于
`prompts/`。`image.yaml` 显式管理原先内联的合成与 Boogu 图片指令。

工作台“算子”读取声明定义与真实 Git 历史；“管道”选择具体流程，显示
引擎编译图并可编辑该流程的唯一当前业务参数。预览只编译，运行使用原生
Dataset/RunContext。标杆 HTTP 服务仅模拟外部模型响应；数据读取、算子、
图片对象、提交与结果均走真实引擎。这不表示已验证真实模型效果。
