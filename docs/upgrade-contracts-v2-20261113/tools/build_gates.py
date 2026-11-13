from pathlib import Path
import json
R=Path(__file__).resolve().parents[3];O=R/'docs/upgrade-contracts-v2-20261113';ts=json.loads((O/'plan.json').read_text())['tasks']
files={
'FACTS':['test_runtime_builtin_recovery','test_c_write_seam'],
'ADMISSION':['test_native_delivery_barrier','test_stream_admission'],
'DELIVERY':['test_native_delivery_barrier'],
'STATE':['test_u4_state_recovery','test_runtime_builtin_recovery'],
'IDENTITY':['test_execution_recovery_identity','test_code_pin_serialization'],
'CODEC':['test_u3_graph_type_fidelity','test_execution_recovery_stream'],
'VECTOR':['test_vector_search_stream','test_search_recovery'],
'EMBED':['test_embeddings','test_document_embeddings'],
'BUFFER':['test_stream_grouping_lookup','test_native_barrier'],
'PREPEND':['test_native_barrier','test_stream_grouping_lookup'],
'PROMPT':['test_prompt_http_stream','test_prompt_stream_recovery','test_inference_execution'],
'UNKNOWN':['test_prompt_stream_recovery','test_managed_image_execution'],
'LAYOUT':['test_execution_layout','test_sealed_execution_bindings'],
'RELATIONAL':['test_local_join_chain','test_c_write_seam'],
'WRITE':['test_gpt_write_recovery','test_execution_recovery_lance','test_c_write_seam'],
'GRAPH':['test_u3_graph_local','test_u3_graph_restore','test_u3_graph_code_pin','test_u3_graph_router'],
'QUEUE':['test_queue_v2_crash','test_mvp2_queue_unified'],
'RECOVERY':['test_u2_chain_faults','test_u3_graph_faults','test_native_barrier'],
'STREAM':['test_stream','test_stream_timeout_propagation'],
'WORKBENCH':['test_workbench_sealed_facts','test_forge_workbench_config_run_browser','test_forge_workbench_runtime_browser'],
'NOTEBOOK':['test_forge_analysis_browser','test_forge_analysis_sessions','test_forge_analysis_kernel_contract'],
'CASE':['test_forge_analysis_case_browser','test_forge_analysis_case_groups_ui_browser','test_forge_analysis_case_query','test_forge_analysis_case_media_browser'],
'DEMO':['test_forge_analysis_case_demo_browser','test_forge_workbench_config_run_browser'],
'DEVELOPMENT':['test_forge_analysis_authority_rpc','test_forge_analysis_frontend_state'],
'BOUNDS':['test_native_resources','test_u2_resources_local','test_forge_analysis_storage_limits'],
'RAY':['test_u2_chain_ray','test_u3_graph_ray'],
'HISTORY':['test_execution_recovery_identity','test_workbench_sealed_facts','test_code_pin_serialization'],
}
rootmap={
'R.1.1':['DEMO','DEVELOPMENT'],'R.1.2':['DEMO'],'R.1.3':['DEMO','WORKBENCH'],
'R.2.1':['WORKBENCH','LAYOUT','HISTORY'],'R.2.2':['ADMISSION','WORKBENCH','WRITE'],
'R.2.3':['WORKBENCH','RECOVERY','HISTORY'],'R.3.1':['NOTEBOOK','WORKBENCH'],
'R.3.2':['NOTEBOOK'],'R.3.3':['CASE','NOTEBOOK'],'R.4.1':['CASE'],'R.4.2':['CASE'],
'R.4.3':['CASE'],'R.5.1':['LAYOUT','ADMISSION'],'R.5.2':['BOUNDS','RECOVERY'],'R.5.3':['HISTORY','DEVELOPMENT','NOTEBOOK'],
'P.correctness':['FACTS','STATE','IDENTITY','CODEC','RELATIONAL'],
'P.unified_execution':['LAYOUT','GRAPH','QUEUE','RAY','STREAM'],
'P.recovery':['RECOVERY','BUFFER','UNKNOWN','WRITE','HISTORY'],
'P.business':['PROMPT','EMBED','VECTOR','PREPEND','WORKBENCH','CASE'],
'P.resources_performance':['BOUNDS','DELIVERY'],
}
gates={}
for g in sorted({g for t in ts for g in t['gates']}):
 paths=['tests/'+f+'.py' for f in files[g]]
 assert all((R/f).is_file() for f in paths),(g,paths)
 gates[g]={'required_existing_files':paths,'leaf_tasks':[t['id'] for t in ts if g in t['gates']], 'new_acceptance_tests':[t['symbol'] for t in ts if t['owner']=='V' and g in t['gates']], 'status':'not_run','require_real_environment':g in ['RAY','WORKBENCH','DEMO','CASE','NOTEBOOK','DEVELOPMENT','BOUNDS']}
(O/'gates.json').write_text(json.dumps(gates,ensure_ascii=False,indent=2)+'\n');(O/'root-map.json').write_text(json.dumps(rootmap,ensure_ascii=False,indent=2)+'\n')
# No test execution: this is a command matrix for the integration owner.
lines=['# 验收命令索引','\n本文件列未来验收命令，**不是已通过报告**。python 使用自己的已验证环境，PYTHONPATH 必须指向隔离集成树。','\n执行环境变量：`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD"`。pytest 额外加 `-p no:cacheprovider --basetemp=/tmp/demiflow-v2-<gate>`；不得在主仓运行。不同进程使用不同 basetemp，重用路径前保留证据。','\n新验收文件 `tests/test_upgrade_v2_acceptance.py` 尚待 V 实现，不能因为不存在而跳过验收。']
for g,v in gates.items():
 lines += ['\n## '+g,'\n```bash','python -m pytest -q -p no:cacheprovider --basetemp=/tmp/demiflow-v2-'+g.lower()+' '+' '.join(v['required_existing_files']),'```','\n叶任务：'+', '.join(v['leaf_tasks'])+'。']
(O/'TEST-COMMANDS.md').write_text('\n'.join(lines)+'\n')
print(len(gates),'gates;',len(rootmap),'root leaves')
