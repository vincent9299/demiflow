---
name: demiforge
description: 用 DemiFlow 公开 SDK 解读、新建或修改 Pipeline 项目中的 Flow、共享算子和 Prompt。用于需要查询开发规范、创建标准文件、登记入口或检查源码的开发任务。
---

# DemiForge 项目开发

1. 确认用户选定的项目根和业务目标。运行 `demiflow forge context --root DIR`，分别查看已登记入口与角色目录。已有目标可加 `--path REL`。需要范例时加 `--topic flow`、`--topic operator` 或 `--topic prompt`。需要精确签名时加 `--api demiflow.data:Dataset.filter`。根据出处阅读本次相关的源码、参数和测试。纯解读任务到此即可回答。
2. 新项目用 `demiflow forge init --root DIR`。接纳已有源码要显式使用 `--existing`。新增文件用 `demiflow forge add flow NAME --root DIR` 或 `add operator NAME --root DIR`。新建 Prompt 时先查 Prompt 主题，按实际原生格式准备内容，再用 `add prompt NAME --root DIR --from FILE` 创建。已经存在的文件直接按需求修改。
3. 运行 `demiflow forge check --root DIR --path REL`；对指定入口可用 `--entry REL.py:SYMBOL` 并按需加 `--params REL.json`。根据具体诊断修正并重查。用针对业务目的的小样本测试核对输出。分别报告已检查和仍未核实的行为。
4. 交付新增或实质修改的 Flow、算子、Prompt、跨管道输入或输出前，运行 `demiflow forge review --root DIR --entry REL.py:SYMBOL --goal "本次业务目标"`。复用本次参数，可用 `--evidence REL` 附小样本测试和真实结果。审一组改动时用 `--base REF`，聚焦文件时可重复给出 `--path REL`。工作台开发会话使用宿主提供的 `request_pipeline_review`，结果回到本会话。评审依据随 Forge 分发的《Forge Pipeline 评审指南》。对影响交付的发现修正并复核，将报告中的证据缺口带入交付说明。纯解读或文字修订沿用轻量路径。
5. 准备让工作台使用的新入口，运行 `demiflow forge add flow --root DIR --register --entry REL.py:SYMBOL`；显式参数绑定用同一 `--params`。交付实际登记的入口、源码改动、测试结果、评审结论和剩余限制。已有入口修改后使用原登记。

工作台可在开发全程关闭。项目自己的业务约束从用户要求、实际输入输出、源码和测试获取。
