# 工作台原生 GPU 计算

受管运行沿用 Dataset、原 v3 compiler、DevicePool、RunJournal/ACK 和 Lance 提交。工作台接受的是算子资源请求，设备清单仍由部署指定；资源请求不授予文件、网络、模型加载或其他外部效果权限。

## 配置与执行

1. 工作区以原平台快照绑定 `gpu` extra。当前声明范围为 `torch>=2.13,<2.14`，本机验证版本为 2.13.0。绑定使用既有 `workspace.bind_platform(root, expected=当前平台摘要, extras=[..., 'gpu'])`；节点的 `required_distributions` 要求 Torch 在该快照内，原 `verify_seal` 继续核对依赖元数据。没有安装包的副作用，也没有第二份环境清单。
2. 服务启动环境显式指定 `DEMIFLOW_DEVICE_INVENTORY`、`DEMIFLOW_RESOURCE_ROOT`、`DEMIFLOW_WORKBENCH_GPU_VIRTUAL_BYTES`。inventory 是工作区外的绝对路径、最多 64 KiB、沿原 DevicePool schema 的有限清单；resource root 是所有受管运行共用的部署资源目录，不能放在 flow 工作区内。服务配置不能通过 flow 参数或 HTTP run-prepare 字段传入。
3. 算子通过现有 `backend_options={'ray': {'worker_resources': {...}}}` 声明资源。`ray` 在这个兼容拼写中不表示调用 Ray Data，也不替代 Local/Ray provider。受管路径只接入 operator 的 resource-only 选项；compute、tasks/writers、worker ranges 和未知选项继续拒绝。source/sink 的 managed backend_options 尚未扩展。

例如普通有类型函数：

```python
import torch

def gpu_sum(offset: int) -> float:
    values = torch.arange(4096, dtype=torch.float32, device='cuda')
    return ((values + offset) * 2).sum().item()

# 在原 flow 内，rows 来自已绑定的固定版本输入。
result = rows.map(
    gpu_sum, inputs={'offset': 'n'}, output='sum',
    backend_options={'ray': {'worker_resources': {
        'gpu': 1, 'memory_bytes': 2 * 1024**3,
    }}},
)
```

静态检查与实际 operation 使用同一个资源规范化函数；CheckedExecution 核对两侧完整请求，不能用一个“已验证”布尔值绕过。原 compiler 将请求变成真实 profile。新增 manifest 可选字段封存 inventory 内容、共享资源根、process host 与有限虚拟地址上限，并参与原 manifest digest；旧 manifest 不补字段、不改历史摘要，保持原 CPU/thread 路径。

## 已支持的张量协议

原 OperatorChecker 继续检查完整函数体，没有 opaque 函数通行证。支持 `arange`、`tensor`、`zeros`、`ones`、`full` 的有限 literal 形状/数据，显式 dtype 与 `cpu`/`cuda`/`cuda:0`；同 shape/dtype/device 的张量或数值标量 `+ - * /`，`sum`/`mean`、单元素 `item`、有界 `tolist`。dtype 当前为 float32/float64/int32/int64/bool；bool 张量只做构造及值输出，不开放算术。无参数 `torch.cuda.device_count()` 和 `is_available()` 只观察原 worker 设备可见性，不选择设备。

构造必须有显式 host `memory_bytes`；GPU 构造必须有本节点的 GPU 请求。静态计入构造及中间张量，并保守计入 tolist 的 Python 值开销；每个调用最多 64 MiB 累计张量/结果窗口、最多 1,048,576 元素、rank ≤4，还受声明内存除以并发数的较小值限制。调用 helper 共享同一资源声明和累计窗口，不重新获得额度。

未知 shape/API、tensor loop/comprehension、隐式 dtype promotion、随机全局状态、IO、模型加载、设备切换、反射及未声明 GPU 均拒绝。张量不能直接成为公开业务端口；边界仍返回原有明确类型的 scalar/list。这里没有声称支持任意 PyTorch 模型、训练循环或所有 Torch API。

## 资源保证与历史

`memory_bytes` 是 host RAM 请求，`gpu=1` 是一张设备请求，两者不同。没有 device memory 限额时，原 DevicePool 按原规则独占该设备。inventory 使用真实可用 free 容量，不能用 total-used 忽略驱动 reserved。CPU stage 由原 host 清空 CUDA visibility；GPU stage 只看到原 grant 的设备；nested execution 不能重新声明父 GPU 或建第二个池。

CUDA 虚拟映射不是实际 RAM/显存。在本机 H100 上，真实 4096 元素运算的 Torch 峰值 tensor allocation 为 33,280 字节、reserved 为 2 MiB，而进程虚拟映射达到约 22.3 GB。测试部署因此明确配置 64 GiB 有限 AS ceiling；它不是普适默认值，也不是 64 GiB RAM 准入。普通 CPU allocation guard 保持；GPU 新运行仍受原 RSS、scratch、输出、超时和整棵进程树监督。单个测试部署的资源设置不会修改共享服务或默认额度。

图中资源画像直接展示原封存 profile，嵌套 accelerators/custom 以 JSON 显示，能够读到设备类型和数量。source/report 图不会伪装成已经执行的计划；原 bridge、run/attempt、ACK 和固定结果继续各自绑定同一封存运行。

平台环境依然是原 metadata-only 保证；没有声称对安装目录二进制进行全量重哈希。历史运行继续使用其原封存代码、平台环境与身份，不因新 GPU extra 或可选 manifest 字段重写旧事实。
