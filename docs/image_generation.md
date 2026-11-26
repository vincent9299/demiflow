# 图片生成

图片生成沿用 `Dataset.map_prompt_async`，没有单独的 API：在 prompt pack 里把 `response_format` 设为 `image`，每次调用产出一张图片，写入对象目录，行里得到这张图片的引用 `{uri, sha256}`。

## 快速开始

```json
{
  "schema_version": "demiflow_prompt_pack_v2",
  "prompts": {
    "generate": {
      "version": "illustration-v1",
      "response_format": "image",
      "model": {
        "backend": "modelhub",
        "model": "your-image-model",
        "api": "images_json",
        "base_url": "http://127.0.0.1:8016/v1",
        "api_key_env": "IMAGE_API_KEY",
        "revision": "your-provider-model-revision",
        "timeout_s": 60
      },
      "template": "{{ instruction }}{{ references | image }}"
    }
  }
}
```

```python
images = rows.map_prompt_async(
    "generate", config="flows/images.json",
    inputs={"instruction": "instruction", "references": "references"},
    output="image", call_output="call", error_output="error",
    max_requests=8, concurrency=1, queue_depth=1,
    options={
        "object_store": "assets/images",
        "image_encoding": "preserve",
        "limits": {
            "max_images": 3, "max_image_bytes": 1024 * 1024,
            "max_pixels": 1024 * 1024, "max_total_pixels": 3 * 1024 * 1024,
            "max_text_bytes": 16384,
            "max_request_bytes": 8 * 1024 * 1024,
            "max_response_bytes": 2 * 1024 * 1024,
        },
    },
)
images.write_lance("/data/generated.lance", mode="create")
```

`references` 列是参考图的字节列表（`list[bytes]`），顺序和重复都会保留；没有参考图时模板里去掉这个占位符即可。

## 必须给出的参数

图片输出比文本输出多几项强制要求：

| 参数 | 要求 |
|---|---|
| `output`（或 `outputs={"image": "列名"}`） | 存放图片引用的列 |
| `call_output`、`error_output` | 必须都给出 |
| `max_requests` | 必须是有限值 |
| `options["object_store"]` | 图片写入的目录 |
| `concurrency` | 1 到 8；`queue_depth` 默认 1，最多 8 |

图片输出不支持 `routes`、`request_policy`、`token_budget` 和 `schema_retries`。

## 接口协议

用 `model.api` 明确选择模型服务的协议：

| `model.api` | 发出的请求 |
|---|---|
| `images_json` | JSON 形式的 `/images/generations` 或 `/images/edits`，参考图按顺序以 data URL 传入 |
| `images` | multipart 形式的图片接口，保留参考图原始字节和类型 |
| `chat` | `/chat/completions`，文本在前，图片按顺序在后 |
| `chat_interleaved` | `/chat/completions`，保持模板里图文的先后顺序 |
| `openrouter_images` | `/images`，参考图按顺序放在 `input_references` |

这些是协议形式上的兼容，具体厂商的接口是否可用需要自行验证。每次响应必须正好包含一张完整的图片。

## 输出

| 列 | 内容 |
|---|---|
| `output` 列 | `{uri, sha256}`；这一行出错时为 null |
| `call_output` 列 | 这次调用的记录；出错时可能为 null |
| `error_output` 列 | 错误说明字符串；成功时为 null |

- `image_encoding="preserve"` 保留模型返回的 JPEG、PNG、WebP 编码；其他格式转成 PNG。
- `when=函数` 返回 `False` 的行不调用模型，也不增加这些列。
- 返回的 `uri` 在同一条管线里立即可读，可以直接接后续节点，例如读取像素、打分或再次编辑：

```python
images.read_object(reference="image", output="pixels", root="assets/images",
                   layout="sha256_prefix2", max_bytes=1024 * 1024)
```

## 文件放在哪里

- `object_store` 是一个逻辑目录，多次运行可以一直用同一个；已有内容不会被动。
- 平台在其下的 `.demiflow-objects/` 里按运行分配实际路径，并在行里返回真实、不可变的 `uri`。不需要自己传运行编号或管理清理。
- 内容相同的图片会复用存储，但不会因此少调一次模型。

## 失败与恢复

- 单行的模型或图片错误（返回不是图片、图片被截断、是动图、超出大小）写入 `error_output`，不会产出图片。
- 存储错误、资源超限、取消会终止整个动作。清理只删除本次运行尚未提交的文件。
- 恢复时会重新检查已提交的图片。图片丢失或损坏、或者某次已经计费但结果不明的请求，会阻止自动恢复，而不是悄悄重新生成。

## 资源上限

`options["limits"]` 里的各项都是硬上限，超出即失败：

| 项 | 含义 |
|---|---|
| `max_images` | 单次请求的参考图数量 |
| `max_image_bytes` | 单张图片（参考图和产出图）的字节数 |
| `max_pixels` / `max_total_pixels` | 单张图片 / 单次请求全部图片的像素数 |
| `max_text_bytes` | 提示文本的字节数 |
| `max_request_bytes` / `max_response_bytes` | 单个请求 / 响应的字节数 |

运行前平台按 `max_requests × max_image_bytes` 预留输出空间，所以这两个值不要设得远大于实际需要。这些上限约束的是请求和图片本身，不是进程内存的硬上限。

## 需要知道的限制

- 只支持一次调用返回一张图片的同步接口；异步任务式的接口、多图返回不支持。
- 不会在失败时自动换另一个模型或服务。
- 目前只支持本地执行。
