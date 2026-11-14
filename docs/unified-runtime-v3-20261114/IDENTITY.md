# v3身份与快照公式

本文件只定义新域；历史v1/v2的describe、source_identity、prompt/embedding request key逐字保持。canonical使用UTF-8、按键排序的紧凑JSON、有限数值、固定枚举与有序数组，禁止NaN/Infinity和Python hash。Json配置不承载业务tuple/bytes或IEEE特殊值；这些只在版本化Record codec中保真。

1. `declare_action_v3`的输出为kind=`dataflow-v3`、terminal kind/arguments、source语义、全部业务算子事实、输入端口和共享别名索引、程序语义code_identity、sink/aggregate声明；不含action_id/dag_id/host/attempt。交原RunContext.action绑定唯一action_id。只新增这个kind的声明，不改变旧kind的identity。
2. 稳定遍历为源/端口构造次序的前序遍历；第一次出现共享前缀赋整数索引，后续引用同索引；不按函数名或内容相等合并。`occurrence_id = sha256(canonical(['dataflow-occurrence-v3',action_id,index]))`。`shared_token`就是该首次索引的十进制文本，不能是Python id。
3. `semantic_id = sha256(canonical(['dataflow-semantic-v3',原操作或来源语义事实,业务参数,code_identity]))`。profile、host、store绝对路径、程序pickle字节、运行期随机种子均不参与；业务显式seed/batch_size参与。
4. `edge_id = sha256(canonical(['dataflow-edge-v3',producer,output_port,consumer,input_port]))`。join端口left/right不同，union端口按0..n-1排列。
5. `dag_id = sha256(canonical(['dataflow-dag-v3',action_id,有序nodes的(occurrence_id,semantic_id,kind,shared_token),有序edges完整字段,terminals,sources]))`。所有算法在compute_graph_digest实现一次，消费者不复制。
6. `plan_id = sha256(canonical(['dataflow-physical-v3',PhysicalDataflow除plan_id的规范字段]))`；包含dag_id、plan_revision、profiles、partition/routing、scope/residency、sources/splits、policy/bounds/recovery、constants_ref内容摘要；ArtifactRef逻辑投影使用digest/bytes/media_type，不含物理存储key和节点cache路径。改宿主但布局语义不变不要求改plan_id；改partition/资源/路由必须改。
7. source_id是source节点occurrence_id；split_id=hash([source_id,稳定partition locator])；record_id=hash([source_id,split_id,固定source position])。flat_map子项使用parent_record_id+occurrence_id+子序号；join记录包含两侧record_id及端口；union保留各边delivery身份，不去重业务多重性。
8. cut_id=hash(['dataflow-cut-v3',action_id,plan_revision,route_epoch,cut_sequence])；participant=stage_id+partition_id，boot作为当前贡献准入而不参与历史cut身份。同cut同内容幂等、异内容拒绝。
9. 新通用RPC operation_id绑定action/occurrence/record/operation/原请求digest，重试与恢复不换key；原模型/队列操作依其已有native key，v3封装不改变原付费效果identity。

程序封存先形成CompiledDataflow内存声明，再stage真实程序，程序ref不反过来改变已定语义身份。恢复使用原constants_ref、source snapshot和代码，不重新运行seed=None/头版本解析。符号卡、fixtures和实际编译器都必须用以上单一公式。
