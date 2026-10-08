# Lance 12 / Arrow 58.4 offset 回补

本目录维护 `pylance==12.0.0+demiflow.arrowfix1` 的源码补丁。2026-10-03 已在当前 CPython 3.11、PyArrow 25.0.1、DataFusion 54.0.0 环境构建、验收并安装。

`arrow-data-58.4.0-with-tests.patch` 回补 Apache Arrow Rust [10835](https://github.com/apache/arrow-rs/pull/10835) 和 [10709](https://github.com/apache/arrow-rs/pull/10709)：分别修复 struct 切片重复应用 offset，以及错误地要求独立 null bitmap 按数据 offset 扩展。原有 raw null buffer 尺寸检查保留。

`lance-12.0.0-cargo-backport.patch` 将 Lance v12.0.0 的 `arrow-data` 指向 `vendor/arrow-data`，保留其他锁定依赖。`lance-12.0.0-build-identity.patch` 增加原生 `lance.__build_version__`，其值为完整本地版本；公开 API 版本 `lance.__version__` 保持 `12.0.0`，让已启动的冻结运行时继续兼容。新平台检查原生构建标记，不能误用未修复 wheel。包管理器显示完整本地版本。

## 产物和安装

当前 wheel、来源校验值、构建日志、测试与安装回执位于工作区 `_demiflow/lance_arrow_repair_20261002/`。wheel 文件名为 `pylance-12.0.0+demiflow.arrowfix1-cp310-abi3-manylinux_2_35_x86_64.whl`，SHA256：`d3d1cc8921dfa8a41aecaad8ba6fc4d9429fd7d38e054238a50a68d1c100a3c6`。这是本地构建，公共包索引不提供此版本。

新环境先校验 wheel，然后安装：

```bash
python -m pip install --no-deps /absolute/path/to/pylance-12.0.0+demiflow.arrowfix1-cp310-abi3-manylinux_2_35_x86_64.whl
python -c 'import lance; print(lance.__version__, lance.__build_version__)'
```

本次安装等待共享原生执行池空闲，逐文件原子替换原生库和唯一变化的 Python 文件，保留旧 inode 与安装元数据用于回退；没有重启其他任务。已加载旧模块的长任务仍使用旧映射，后续新进程使用新库。原有公共 Lance 数据版本未重写。

## 重建

1. 解压官方 Lance `v12.0.0` 源码；将官方 `arrow-data-58.4.0.crate` 解压为其 `vendor/arrow-data`。来源 URL、SHA256 见上述构建清单。
2. 在 `vendor/arrow-data` 应用 Arrow 补丁，在 Lance 根应用另两个补丁；核对其余 `Cargo.lock` 版本不变。原生测试应使用 Lance 锁定的依赖，不能使用 crate 自带的不同锁文件。
3. 使用 Rust 1.97.0、maturin 1.13.3、protoc 30.2；在 `python/` 执行 `maturin build --release --locked --strip --interpreter /path/to/python --out /path/to/wheels`。本次验证了全部 772 个 registry crate 的锁文件校验值，vendoring 后以 `--offline` 编译；构建工具副本保存在产物目录的 `build_tools/`。
4. 保持有限构建预算。本次 `CARGO_BUILD_JOBS=4`，覆盖 release 配置为 `CARGO_PROFILE_RELEASE_LTO=false`、`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`、`CARGO_PROFILE_RELEASE_DEBUG=0`；进程树 RSS 20 GiB、临时盘 80 GiB、总时限 3 小时，超限仅终止自己的构建进程组。实测编译约 26 分钟，RSS 峰值约 5.65 GiB。这些参数与官方 wheel 的优化配置可能不同，未宣称性能等同。

## 验收与兼容层退役

Rust 33 项通过；平台 44 项通过，覆盖直接嵌套切片、父子 null／空列表、非对齐文件边界、125 万行逐值比较、固定回调分组、合并与失败不提交。安装后另外复核 8 项。原 16 行和 32 行崩溃复现直接调用 Lance，无复制或对齐适配。

已删除 `aligned_record_batches` 和文件行数整除约束。`bounded_record_batches` 保留行数及 8 MiB Arrow 逻辑字节准入，小批透传，大批零拷贝切分，单行超限失败；`fixed_row_tables` 以 chunks 保持 Python 回调固定组大小。它们不是 offset 兼容层，也不能代表进程 RSS 硬上限。reader 父缓冲、writer 编码、背压与进程保护仍须独立管理。
