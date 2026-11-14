# F：历史固定版本只读贯通

新增能力仅平台读接口；禁止临时attach/改目录/换当前绑定。schema/rows/media/annotation/export所有消费者必须同时迁移。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/analysis/catalog_profile.py`
- `demiflow/workbench/analysis/catalog_versions.py`
- `demiflow/workbench/analysis/media_source_export.py`
- `demiflow/workbench/analysis/source_export.py`
- `demiflow/workbench/analysis/sources.py`
- `demiflow/workbench/analysis/web_sources.py`
- `demiflow/workbench/api.py`
- `demiflow/workbench/projects/workspace.py`
- `demiflow/workbench/worker.py`

## F01 · `VersionResolution`

- 文件：`demiflow/workbench/analysis/catalog_versions.py`；动作：`add`。
- 签名：`@dataclass(frozen=True) VersionResolution(alias_binding,binding,reference,ref,uri,metadata)`。
- 契约：C4；调用/接线任务：F02。
- 输入：@dataclass(frozen=True) VersionResolution(alias_binding,binding,reference,ref,uri,metadata)。
- 输出：只读内部数据对象；nested mapping深冻结或复制后仅只读视图，ref保持原DatasetRef不可变约束；wire只输出C4规定字段，不把Path暴露为新权限输入。

只读内部数据对象；nested mapping深冻结或复制后仅只读视图，ref保持原DatasetRef不可变约束；wire只输出C4规定字段，不把Path暴露为新权限输入。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F02 · `resolve_version`

- 文件：`demiflow/workbench/analysis/catalog_versions.py`；动作：`add`。
- 签名：`resolve_version(root, *, alias, expected_binding, version, expected_version_binding=None) -> VersionResolution`。
- 契约：C4；调用/接线任务：F03, F11, F16。
- 输入：resolve_version(root, *, alias, expected_binding, version, expected_version_binding=None) -> VersionResolution。
- 输出：严格C4.1：当前alias守卫、Catalog同身份registered version、实际schema/rows、virtual control用preview_binding；前后校验、run输出只封存版本。

严格C4.1：当前alias守卫、Catalog同身份registered version、实际schema/rows、virtual control用preview_binding；前后校验、run输出只封存版本。无权限/未登记/被删/漂移fail closed，不创建Catalog记录。只能在原worker中调用native读取。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F03 · `dispatch`

- 文件：`demiflow/workbench/worker.py`；动作：`modify`。
- 签名：`dispatch(request) -> existing worker result`。
- 契约：C4；调用/接线任务：F04, F05, F06, F12。
- 输入：dispatch(request) -> existing worker result。
- 输出：table_versions selected增固定binding/ref；table/sample/media分支成对取version/version_binding，调用F02后原reader按selected ref读；旧分支严格保持。

table_versions selected增固定binding/ref；table/sample/media分支成对取version/version_binding，调用F02后原reader按selected ref读；旧分支严格保持。table_profile继续走F11；所有异常用既有ForgeError协议，不能在HTTP线程直接Lance读。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F04 · `describe_table`

- 文件：`demiflow/workbench/projects/workspace.py`；动作：`modify`。
- 签名：`describe_table(root,alias,*,expected_binding=None,version=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F09, F12。
- 输入：describe_table(root,alias,*,expected_binding=None,version=None,version_binding=None) -> dict。
- 输出：新参数成对透传worker，不自行推version或改alias；缺省不把None新字段送旧协议，保持旧输出/摘要。

新参数成对透传worker，不自行推version或改alias；缺省不把None新字段送旧协议，保持旧输出/摘要。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F05 · `sample_table`

- 文件：`demiflow/workbench/projects/workspace.py`；动作：`modify`。
- 签名：`sample_table(root,alias,*,columns=None,offset=0,limit=20,row_id=None,match=None,expected_binding=None,version=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F09, F12。
- 输入：sample_table(root,alias,*,columns=None,offset=0,limit=20,row_id=None,match=None,expected_binding=None,version=None,version_binding=None) -> dict。
- 输出：保持所有现有默认/row_id字符串/columns上限；有新对传worker验证固定版本，返回same binding及rows；不得利用match或版本切换跨过权限/资源限制。

保持所有现有默认/row_id字符串/columns上限；有新对传worker验证固定版本，返回same binding及rows；不得利用match或版本切换跨过权限/资源限制。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F06 · `profile_table`

- 文件：`demiflow/workbench/projects/workspace.py`；动作：`modify`。
- 签名：`profile_table(root,alias,*,expected_binding,columns,version=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F12。
- 输入：profile_table(root,alias,*,expected_binding,columns,version=None,version_binding=None) -> dict。
- 输出：透传历史版本对到原table_profile worker；project_profile调用不变；统计coverage/budget保持。

透传历史版本对到原table_profile worker；project_profile调用不变；统计coverage/budget保持。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F07 · `locator`

- 文件：`demiflow/workbench/analysis/sources.py`；动作：`modify`。
- 签名：`locator(value) -> validated locator`。
- 契约：C4；调用/接线任务：F09, F10, F11。
- 输入：locator(value) -> validated locator。
- 输出：旧workspace_table/project_table字段闭集不变；workspace_table只新增version/alias_binding成对可选，严格int/SHA/columns。

旧workspace_table/project_table字段闭集不变；workspace_table只新增version/alias_binding成对可选，严格int/SHA/columns。历史binding为版本digest；不改旧source_locator记录，不接受uri/path/rows/schema自证。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F08 · `_snapshot`

- 文件：`demiflow/workbench/analysis/web_sources.py`；动作：`modify`。
- 签名：`_snapshot(metadata,value) -> existing SourceSnapshot`。
- 契约：C4；调用/接线任务：F09, F10。
- 输入：_snapshot(metadata,value) -> existing SourceSnapshot。
- 输出：沿用value.binding（历史为版本digest）和真实version metadata构造source，旧输入输出摘要必须逐字同基线。

沿用value.binding（历史为版本digest）和真实version metadata构造source，旧输入输出摘要必须逐字同基线。禁止将alias_binding替换真实binding或新增污染摘要的UI字段。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F09 · `resolve`

- 文件：`demiflow/workbench/analysis/web_sources.py`；动作：`modify`。
- 签名：`resolve(root,value) -> {source,locator}`。
- 契约：C4；调用/接线任务：F10。
- 输入：resolve(root,value) -> {source,locator}。
- 输出：历史locator经ws.describe_table(expected_binding=alias_binding,version,version_binding=binding)核对；旧分支原行为，原项目不变。

历史locator经ws.describe_table(expected_binding=alias_binding,version,version_binding=binding)核对；旧分支原行为，原项目不变。source由F08生成，返回相同locator不悄悄归一到当前版本。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F10 · `verify`

- 文件：`demiflow/workbench/analysis/web_sources.py`；动作：`modify`。
- 签名：`verify(root,*,locator,source,target) -> {verified:true}`。
- 契约：C4；调用/接线任务：E08。
- 输入：verify(root,*,locator,source,target) -> {verified:true}。
- 输出：先F09校验整份source，再用F05在同version做真实Case membership；target/group原proof规则保留。

先F09校验整份source，再用F05在同version做真实Case membership；target/group原proof规则保留。不允许只resolve历史但verify读当前。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F11 · `fixed_input`

- 文件：`demiflow/workbench/analysis/source_export.py`；动作：`modify`。
- 签名：`fixed_input(root,value) -> (Path,version,SourceSnapshot)`。
- 契约：C4；调用/接线任务：F13。
- 输入：fixed_input(root,value) -> (Path,version,SourceSnapshot)。
- 输出：worker-only历史locator走F02，旧分支完整保持；交回原read/export/media共同入口同version。

worker-only历史locator走F02，旧分支完整保持；交回原read/export/media共同入口同version。不得递归启动worker或依赖浏览器metadata。宽字段Arrow预算/文件FD/提交等原实现不动。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V05, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F12 · `Handler.do_GET`

- 文件：`demiflow/workbench/api.py`；动作：`modify`。
- 签名：`Handler.do_GET() -> existing authenticated response`。
- 契约：C4；调用/接线任务：A06, A07, A09。
- 输入：Handler.do_GET() -> existing authenticated response。
- 输出：GET_FIELDS/table/sample/table-profile/media扩成对字段；HTTP query严格正int/SHA、别名/权限仍原resolve流程。

GET_FIELDS/table/sample/table-profile/media扩成对字段；HTTP query严格正int/SHA、别名/权限仍原resolve流程。调用F04/05/06或worker media，table-versions原selected增强。保持token、origin、超时/队列与原request body限制。 table-evidence也纳入同一版本对校验和F15透传；media明确调F14。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F13 · `profile`

- 文件：`demiflow/workbench/analysis/catalog_profile.py`；动作：`modify`。
- 签名：`profile(root,*,kind,columns,alias=None,expected_binding=None,identity=None,expected=None,version=None,binding=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F03。
- 输入：profile(root,*,kind,columns,alias=None,expected_binding=None,identity=None,expected=None,version=None,binding=None,version_binding=None) -> dict。
- 输出：workspace_table历史分支将version/version_binding转F07 locator，binding仍保留project_table语义；version_binding仅workspace，project禁止。

workspace_table历史分支将version/version_binding转F07 locator，binding仍保留project_table语义；version_binding仅workspace，project禁止。fixed_input与read必须同source_digest，返回binding实际版本；原采样覆盖不变。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F14 · `table_media`

- 文件：`demiflow/workbench/projects/workspace.py`；动作：`modify`。
- 签名：`table_media(root,alias,*,row_id,field=None,field_path=None,expected_binding=None,version=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F12。
- 输入：table_media(root,alias,*,row_id,field=None,field_path=None,expected_binding=None,version=None,version_binding=None) -> dict。
- 输出：历史版本对透传原media worker；field与field_path仍互斥，图片读取与row数据用同一selected ref；旧缺省逐字兼容。

历史版本对透传原media worker；field与field_path仍互斥，图片读取与row数据用同一selected ref；旧缺省逐字兼容。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F15 · `table_evidence`

- 文件：`demiflow/workbench/projects/workspace.py`；动作：`modify`。
- 签名：`table_evidence(root,alias,*,row_id,field_path,expected_binding=None,version=None,version_binding=None) -> dict`。
- 契约：C4；调用/接线任务：F12。
- 输入：table_evidence(root,alias,*,row_id,field_path,expected_binding=None,version=None,version_binding=None) -> dict。
- 输出：历史版本对透传table_evidence，原资源声明/路径预算保持；case只读证据与同row/version匹配，不用当前版本的附件替换。

历史版本对透传table_evidence，原资源声明/路径预算保持；case只读证据与同row/version匹配，不用当前版本的附件替换。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F16 · `_selection`

- 文件：`demiflow/workbench/analysis/media_source_export.py`；动作：`modify`。
- 签名：`_selection(root,*,source_locator,source_snapshot,case_key,field) -> existing selection`。
- 契约：C4；调用/接线任务：F11。
- 输入：_selection(root,*,source_locator,source_snapshot,case_key,field) -> existing selection。
- 输出：现有fixed_input后另读当前ref并要求ref.version相等会拒历史版本；新历史分支使用F02核验出的selected ref采样，当前alias control仅授予当前公开资源声明。

现有fixed_input后另读当前ref并要求ref.version相等会拒历史版本；新历史分支使用F02核验出的selected ref采样，当前alias control仅授予当前公开资源声明。root/declaration/resource_binding与_recheck完全保留；没有声明或历史媒体hash不符仍拒绝。旧分支byte-equivalent witness。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## F17 · `VersionResolution.to_wire`

- 文件：`demiflow/workbench/analysis/catalog_versions.py`；动作：`add`。
- 签名：`VersionResolution.to_wire(self) -> VersionResolutionWire`。
- 契约：C4；调用/接线任务：F03, F11。
- 输入：VersionResolution.to_wire(self) -> VersionResolutionWire。
- 输出：将内部只读mapping/tuple转换为独立JSON dict/list，返回version/alias_binding/binding/reference/metadata，复用原worker输出预算；不返回Path或native DatasetRef对象。

将内部只读mapping/tuple转换为独立JSON dict/list，返回version/alias_binding/binding/reference/metadata，复用原worker输出预算；不返回Path或native DatasetRef对象。F03响应与F11调用_snapshot使用此普通JSON元信息，避免MappingProxy不能序列化或被type(dict)拒绝。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
