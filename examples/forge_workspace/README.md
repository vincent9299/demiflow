# 题目实验工作区

用途：查看题目 case，规范化题面、关联评分，并比较 0.80 / 0.90 两种筛选配置。

数据位置：data/ 为本工作区拥有的数据。candidates 固定 v3，scores 固定 v2。
comparison_fixture 是独立准备的审核对照 fixture，并非 Flow 的运行结果：c02 有两个匹配项，c03 没有匹配项。

接线：flows/questions.py:select。两份配置位于 flows/select_a.json 与 flows/select_b.json。
算子：operators/question_rules.py，使用普通 Python 函数，没有业务注册或包装类。

创建脚本只准备自有数据、登记流程并做静态检查，不执行候选流程。
在工作台的“管道”中可以查看实际图、配置、源码和检查结果，并显式提交运行。
输出写入每次 run 自己的目录，不覆盖 data/ 下的输入表，也不会创建原配置中的 selected_a / selected_b 表。
“数据”中可查看输入表、c01 图片，以及运行后固定版本的结果。

Notebook 是可选的普通项目代码；接线规范仍统一放在 flows/。

从主仓创建自有演示（使用已安装 demiflow 与 images extra 的 Python 环境）：

```sh
PYTHONPATH=. python examples/forge_workspace/make_demo.py /tmp/forge-stage2-demo
PYTHONPATH=. python -m demiflow.cli workbench view /tmp/forge-stage2-demo \
  --enable-runs --enable-config-write --workspace-config-write
```

打开启动时显示的本机地址。运行和配置写入分别显式开放，默认查看模式不会修改配置或启动作业。

可重复的配置、检查与运行示例：

1. 选择“管道”、`flows/questions.py:select` 和 `flows/select_a.json`，点击“整图配置”。
2. 把 `scores/alias` 从 `scores` 改为已存在的 `candidates`，保存后点击“检查”。这个表没有 `score` 字段，页面应显示 `Missing field(s): ['score']`，不会把局部图当成检查通过。
3. 点击“返回配置与图”，把 `scores/alias` 改回 `scores`，同时把 `minimum` 改为 `0.9`。保存并重新检查，确认当前配置的接线检查通过。
4. 点击“运行”，等待选中 run 的尝试 1 成功。在“输出结果”中查看固定版本结果；应只有 `c01` 一行，评分 `0.95`，题面已去除多余空白。

`minimum=0.8` 会保留 `c01`、`c02` 两行；`minimum=0.9` 只保留 `c01`。再次点击“运行”会建立独立 run，输入固定版本与之前的结果保持原样。
