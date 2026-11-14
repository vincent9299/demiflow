"""Third-review wiring and progress-publication contracts. Design only."""
import ast


def apply(tasks, add, params):
    byid = {task['id']: task for task in tasks}
    params.update({
        'sealed': '原 sink.detach() 的独占结果；不是已提交数据或单纯 descriptor；回执前不能回收',
        'states': '该切点已验证的有界状态载体；含 stream-progress 与 node-*，并非一律为文件引用',
        'checkpoint_id': '已有 graph driver 指定的 checkpoint ID；属于图提交协议，不是本地 barrier 对象标识',
        'producer_sequence_bound': 'int>=0；已有祖先源 cut frontier；不能替换为 egress/channel cursor',
        'epoch': 'int>=0；本次 graph cut epoch，保留原控制命令语义',
        'consumed_rows': 'int>=0；target reader 已交给 pipeline 的输入行数，不是预读游标',
    })
    for tid in ['L017', 'L018']:
        t = byid[tid]
        t['mode'] = 'implement'
        t['behavior'] += (' 必须迁移本函数内真实 derive_prefetch 调用点；Local 与 Ray 各有一处，'
                          '不能只改另一条路线。窄改封存前联合求解和恢复验证；保持原 K6、路由、'
                          '提交、资源退出协议。显式冲突在源读取和 worker 启动前拒绝。')
        t['review_revision'] = '20261114-r3'

    # A local barrier is already the shared object delivered through each FIFO.
    # A new durable cut_id has no producer here and would conflate namespaces.
    t = byid['R019']
    t['signature'] = 'async def arrive(self, stage, batch=None, *, worker_slot=None):'
    node = ast.parse(t['signature'] + '\n    pass').body[0]
    t['input'] = {a.arg: params[a.arg] for a in [*node.args.args, *node.args.kwonlyargs]}
    for tid in ['R017', 'R018', 'R019', 'R020']:
        byid[tid]['behavior'] = byid[tid]['behavior'].replace('错cut/slot', '错误barrier实例/slot') + (
            ' 本地cut归属就是队列传递的同一个barrier对象；arrivals按该对象+stage+worker_slot隔离。'
            '不新增持久cut_id字段，不把graph checkpoint_id注入普通chain/本地barrier。'
            '已失败/发布对象不得接受新到达；新barrier不能复用上一对象的slot登记。')
        byid[tid]['review_revision'] = '20261114-r3'

    def existing(file, symbol, output, behavior, *, mode='verify_preserve', calls=(), effects='runtime_state'):
        add('R', file, symbol, output, behavior, ['BUFFER', 'RECOVERY', 'GRAPH'],
            contract='C06', calls=calls, mode=mode, effects=effects)
        tasks[-1]['review_revision'] = '20261114-r3'
        return tasks[-1]

    common = ('遵守 C13 SealBarrierView/PublicationThunk；只搬运已由 align 验证的状态，'
              '不发明 buffer payload、不丢 node-* 键。seal 同步 detach 输出并捕获本 cut 的 '
              'next_ordinal/final/states，发布只能由 barrier 调用一次；后续 append/reset '
              '不能改变已交出的载体。内存不靠整份 deepcopy 翻倍，沿用 detach/独占所有权和状态预算。')
    existing('demiflow/execution/stream_materialize.py', 'StreamMaterialization.seal',
             'PublicationThunk：调用后返回 Awaitable[None]',
             common + ' final 只能由控制器确定，必须核固定源已耗尽；object_sinks 按 root 键传递。'
             '本方法不直接提交，thunk 调唯一 _commit。', mode='implement',
             calls=['StreamMaterialization._commit'])
    existing('demiflow/execution/stream_materialize.py', 'StreamMaterialization._commit',
             'Awaitable[None]；receipt 成功后更新确认进度',
             '核验保留：唯一 driver CommitCoordinator.commit；输出、SourcePosition 与全部状态同一提交。'
             '只有真回执后更新 inputs/rows/sequence 与通知；异常/取消不推进确认，不补造ACK。',
             effects='durable_write')
    existing('demiflow/execution/stream_materialize.py', 'StreamMaterialization.finish',
             'None；成功完成当前 action',
             '核验保留：inputs==固定源行数且未提交 row_sink 为空才 complete。'
             '不能依据 barrier.final 或已发送 contribution 就标记动作成功。', effects='durable_write')
    existing('demiflow/execution/worker/progress.py', 'WorkerStreamProgress.seal',
             'PublicationThunk：调用后返回 Awaitable[原 CommitReceipt dict]',
             common + ' fixed source EOF 门不变；只调用 L034 的异步 request_commit。'
             'worker 不构造 CommitCoordinator、不读写第二提交账本；保留所有 root 对象。',
             mode='implement', calls=['WorkerChainExecutor._request_commit'])
    existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress.seal',
             'PublicationThunk：无真实 cut 返回 None；发布返回 Awaitable[原 graph receipt dict]',
             common + '本方法有同步副作用：允许 router.barrier 注入控制帧；不等于持久提交。'
             'ancestor 的 final 仅由固定源 EOF 确认；target contribution.final 可带 driver hint，'
             'barrier.final 只能由输入真实 EOI 确认。timer/压力但无命名 cut 且非真正 final 时不得 detach/发布。'
             '保留 action namespace 的确定 checkpoint_id、source_progress/channel_cuts/egress ordinal。'
             'thunk 只将原 stage 状态经 StateArtifacts 写为 occurrence 的 state_ref，再调 L035；'
             '快照文件存在不代表 checkpoint 已确认。', mode='implement',
             calls=['GraphWorkerProgress._channel_cuts', 'GraphWorkerExecutor._request_contribution'],
             effects='durable_write')
    existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress.request_cut',
             'None；登记已有 driver cut 并唤醒控制器',
             '核验保留 driver checkpoint_id/epoch/源 bound/final hint 到已有字段；'
             '只请求切点，不推进 confirmed inputs、不产生ACK、不把 hint 认定为 EOF。'
             '生产调用者 GraphWorkerExecutor._apply_cut_commands；只读保留该调度，非新增接口。')
    existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress.note_driver_cut',
             'None；登记 target 的 final hint',
             '核验保留 checkpoint_id→final hint；仅供 align_cut 关联同一 graph cut，'
             '不能推进源位置或把未收到 EOI 的 reader 标完成。现有调用者 _apply_cut_commands。')
    existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress.align_cut',
             'threading.Event；目标 reader 在回执前等待',
             '核验保留 InputGate 对齐后的 checkpoint_id/consumed_rows；pending_cut 用已喂入的行数。'
             '事件只归当前 outstanding cut，reader 不能提前释放 post-cut data。调用者 '
             'GraphWorkerExecutor._run 的 set_cut_hook；不在这里造提交。')
    existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress._channel_cuts',
             'list[原 K6 channel_cut]；保持字段结构',
             '核验保留 producer.next_sequence 与 consumer cursor 的边界换算；这些是通道坐标，'
             '不能用 source ordinal、egress ordinal 或 batch ordinal 替代。InputGate/ChannelSequence '
             '仍为通道权威；本方法不操作 ACK/GC。')
    ack = existing('demiflow/execution/worker/graph_progress.py', 'GraphWorkerProgress.note_contribution_acked',
             'None；确认 inputs 并释放当前 reader event',
             '核验保留：只能由 L035 在 operation future 与 checkpoint_id 均匹配且 status=acked 后调用。'
             'inputs 为该贡献中 stream-progress.inputs；迟到/错 cut/超时回执不调用本方法，'
             '不释放新 cut 的 event。由单 outstanding cut 的控制器和 reader 等待约束保证关联。')
    ack['input']['inputs'] = 'int>=0；该已确认贡献的输入 frontier；不是固定输入引用映射或 channel cursor'
    for tid in ['L034', 'L035']:
        byid[tid]['input']['sealed'] = params['sealed']
        byid[tid]['input']['states'] = params['states']
        byid[tid]['behavior'] += (' 与 C13 publication thunk 对接；确认前保持 output/state 的所有权。'
                                 '超时/取消使 pending future 失效，迟到回执不能推进后续 cut。')
    byid['L035']['behavior'] += ' 必须验证 receipt.checkpoint_id==contribution.checkpoint_id；不能只检查 status。'
    byid['L035']['calls'] += ['GraphWorkerProgress.note_contribution_acked']
    byid['R019']['calls'] += ['StreamMaterialization.seal', 'WorkerStreamProgress.seal', 'GraphWorkerProgress.seal']
    byid['V012']['acceptance_assertions'] = [
        '三个真实 progress 类继承的 align 与各自 seal 联跑：全部合法状态/多 object root 保留；后续追加不污染已封存输出。',
        '本地 _commit 失败不前进 inputs/rows/sequence；chain 与 graph worker 只发请求，driver 唯一持久提交。',
        'graph source 提前 final hint、target hint 先于 EOI、无命名压力 cut 三反例；hint 不能提前终止/错发 checkpoint。',
        'operation/checkpoint 匹配的ACK才释放 reader post-cut data 和GC；错 checkpoint/超时后迟到ACK不得推进新cut，event-loop heartbeat仍运行。',
        'queue pure maps suffix无progress仍由driver finalize_task结算；每任务结果与ACK同原协议，worker零提交权威。',
    ]
    byid['V008']['acceptance_assertions'][0] = (
        '普通buffer c=2各有pending，全入同barrier；重复slot、错stage、已终局barrier拒绝；'
        '相邻两个barrier的slot登记不串用，EOF参与者不死等。持久buffer payload仍待冻结。')
    byid['V018']['acceptance_assertions'] += [
        'Local与Ray真实graph入口分别验证slots=1/default credit=64封存前一致收紧；显式冲突两路线均先拒绝；恢复不改原封存credit。Ray未运行须标not_run。'
    ]
    byid['R015']['behavior'] += (' 这是一个包含闭包的复合实现单元，并不声称是一项简单函数；'
                               '同一负责人覆盖所有分支，scope guard授权整个函数不证明嵌套分支正确。')
    for tid in ['L034', 'L035', 'R015', 'V008', 'V012', 'V018']:
        byid[tid]['review_revision'] = '20261114-r3'
