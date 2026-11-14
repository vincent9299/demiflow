# 函数级任务索引

设计契约以[02-contracts.md](02-contracts.md)和[contracts.d.ts](contracts.d.ts)为准；同名已有函数指所在IIFE/闭包内的函数，非要求全部export。每项的调用方列代表必须接线的任务，**测试项的此列是被验实现**。CSS/入口装配/类型是显式非函数任务，不伪称函数。

共 **93 项**：A 14项, B 17项, C 8项, D 15项, E 11项, F 17项, V 11项。

| 路 | 文件数 | 任务书 |
|---|---:|---|
| A | 4 | [公共壳与数据上下文](tasks/A-公共壳与数据上下文.md) |
| B | 3 | [管道完整用户链路](tasks/B-管道完整用户链路.md) |
| C | 3 | [可读图与纯呈现](tasks/C-可读图与纯呈现.md) |
| D | 6 | [Notebook与通用输出](tasks/D-Notebook与通用输出.md) |
| E | 2 | [Case与持久评测体验](tasks/E-Case与持久评测体验.md) |
| F | 9 | [历史固定版本只读贯通](tasks/F-历史固定版本只读贯通.md) |
| V | 3 | [独立集成验收](tasks/V-独立集成验收.md) |

## 每个文件一个主写方

| 文件 | 路 | 函数/装配任务 |
|---|---|---|
| `demiflow/workbench/analysis/catalog_profile.py` | F | F13 |
| `demiflow/workbench/analysis/catalog_versions.py` | F | F01, F02, F17 |
| `demiflow/workbench/analysis/media_source_export.py` | F | F16 |
| `demiflow/workbench/analysis/source_export.py` | F | F11 |
| `demiflow/workbench/analysis/sources.py` | F | F07 |
| `demiflow/workbench/analysis/web_sources.py` | F | F08, F09, F10 |
| `demiflow/workbench/api.py` | F | F12 |
| `demiflow/workbench/projects/workspace.py` | F | F04, F05, F06, F14, F15 |
| `demiflow/workbench/web/analysis-case.css` | E | E10 |
| `demiflow/workbench/web/analysis-case.js` | E | E01, E02, E03, E04, E05, E06, E07, E08, E09, E11 |
| `demiflow/workbench/web/analysis-charts.js` | D | D10, D11 |
| `demiflow/workbench/web/analysis-output.css` | D | D14 |
| `demiflow/workbench/web/analysis-output.js` | D | D08, D09, D15 |
| `demiflow/workbench/web/analysis-render.js` | D | D12 |
| `demiflow/workbench/web/analysis.css` | D | D13 |
| `demiflow/workbench/web/analysis.js` | D | D01, D02, D03, D04, D05, D06, D07 |
| `demiflow/workbench/web/app.js` | A | A01, A02, A03, A04, A05, A06, A07, A08, A09, A13, A14 |
| `demiflow/workbench/web/index.html` | A | A12 |
| `demiflow/workbench/web/style.css` | A | A11 |
| `demiflow/workbench/web/view.js` | A | A10 |
| `demiflow/workbench/web/workbench-graph.css` | C | C08 |
| `demiflow/workbench/web/workbench-graph.js` | C | C05, C06, C07 |
| `demiflow/workbench/web/workbench-presentation.js` | C | C01, C02, C03, C04 |
| `demiflow/workbench/web/workbench-state.js` | B | B11 |
| `demiflow/workbench/web/workbench.css` | B | B12 |
| `demiflow/workbench/web/workbench.js` | B | B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B13, B14, B15, B16, B17 |
| `demiflow/workbench/worker.py` | F | F03 |
| `tests/d19_fixtures.py` | V | V10, V11 |
| `tests/test_workbench_d19_browser.py` | V | V01, V03, V04, V05, V06, V07, V08, V09 |
| `tests/test_workbench_d19_versions.py` | V | V02 |

## 跨路接线

- A12加载C的新presentation、D的新charts和C的graph CSS；B01/B02经B14统一调用C01–C06。
- A05维持D01/E01装配，并提供可选toolbarHost；D01保持向ForgeCase转发该host和setSource路径。
- A06/A07/A09及F09/F10/F11/F13消费F的固定版本链；不只改提供方。
- A08调D07；D08调D10/D11；E06调D12；D08的onCase仍进入E的checkedPresentation。
- A/B共同CSS迁移仅按C1列出的所有权变更；最终唯一规则，无全文件互相覆盖。
- V01–V09均列为必跑精确nodeid，见04-acceptance，不存在写了测试却零命令的卡。
