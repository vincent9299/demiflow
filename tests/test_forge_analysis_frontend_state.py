"""Source/patch/draft/recovery boundaries in the actual browser state module."""
from pathlib import Path
import subprocess


def test_source_patch_import_and_request_recovery_fences():
    root=Path(__file__).resolve().parents[1]
    script=r'''
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const data=new Map(),storage={getItem:key=>data.get(key)??null,setItem:(key,value)=>data.set(key,value)};
const context=vm.createContext({TextEncoder,localStorage:storage});
vm.runInContext(fs.readFileSync('demiflow/workbench/web/analysis-state.js','utf8'),context);
const S=context.ForgeAnalysisState,P='w'+'1'.repeat(32),Q='w'+'2'.repeat(32);
const original={title:'source',cells:[{id:'one',language:'python',source:'x=1'},{id:'two',language:'sql',source:'select "a\\b"\nfrom data',result_name:'answer'}]};
const changed=JSON.parse(JSON.stringify(original));changed.cells[0].source='x=2';changed.cells[1].source='select 2';changed.title='draft title';
const change=S.patch(original,changed,'one'),applied=S.apply(original,change);
assert.equal(applied.cells[0].source,'x=2');assert.equal(applied.cells[1].source,original.cells[1].source);assert.equal(applied.title,'source');
assert.equal(JSON.stringify(S.imported(S.exported(original),'source')),JSON.stringify(original));
assert.throws(()=>S.cell({id:'bad',language:'sql',source:'select 1',result_name:'class'}));
assert.throws(()=>S.cell({id:'bad',language:'python',source:'中'.repeat(6000)}));
assert.throws(()=>S.cell({id:'bad',language:'python',source:'\ud800'}));
const store=new S.Store(storage);store.saveDraft(P,'n1',{revision:1,document:original});
original.cells[0].source='mutated caller';assert.equal(store.draft(P,'n1').document.cells[0].source,'x=1');
assert.equal(store.draft(Q,'n1'),null);
const incomplete={title:'',cells:[{id:'incomplete',language:'sql',source:'select 1',result_name:'class'}]};
store.saveDraft(P,'draft-incomplete',{revision:0,document:incomplete});
assert.equal(store.draft(P,'draft-incomplete').document.cells[0].result_name,'class');
assert.throws(()=>S.document(incomplete));
const payload={request_id:'same',session_id:'session',expected_generation:'old',revision:1,cell_id:'one',source_sha256:'a'.repeat(64)};
store.prepare(P,'cell-submit',payload,{notebook:'n1'});
assert.throws(()=>store.prepare(P,'cell-submit',{...payload,expected_generation:'new'},{notebook:'n1'}));
store.finish(Q,'same');assert.equal(store.pending(P).length,1);store.finish(P,'same');assert.equal(store.pending(P).length,0);
for(let n=0;n<8;n++)store.prepare(P,'notebook-patch',{request_id:'u'+n},{notebook:'n1'});
assert.throws(()=>store.prepare(P,'notebook-patch',{request_id:'overflow'},{notebook:'n1'}));
assert.equal(store.pending(Q).length,0);
const disk=new Map(),denied={getItem:key=>disk.get(key)??null,setItem:()=>{throw new Error('quota');}};
const windowStore=new S.Store(denied);
windowStore.saveDraft(P,'quota-source',{revision:7,document:incomplete,bindings:{fixed:'b'.repeat(64)},session:{id:'session',generation:'old'}});
const recovered=new S.Store(denied).draft(P,'quota-source');
assert.equal(recovered.revision,7);assert.equal(recovered.document.cells[0].result_name,'class');assert.equal(recovered.bindings.fixed,'b'.repeat(64));
assert(windowStore.hasVolatileSources());
assert.throws(()=>windowStore.prepare(P,'cell-submit',{request_id:'never-dispatched'},{notebook:'quota-source'}));
assert.equal(windowStore.pending(P).length,0);
console.log('source/partial-patch/import/no-output/draft/project/generation/nonce/quota fences passed');
'''
    result=subprocess.run(['node','-'],input=script,text=True,cwd=root,capture_output=True,timeout=10)
    assert result.returncode==0,result.stdout+result.stderr


def test_portable_saved_case_view_is_paired_revision_only_and_preserves_legacy_routes():
    root=Path(__file__).resolve().parents[1]
    script=r'''const fs=require('fs'),vm=require('vm'),assert=require('assert');
const context=vm.createContext({TextEncoder,URLSearchParams});
vm.runInContext(fs.readFileSync('demiflow/workbench/web/view.js','utf8'),context);
// Build objects in the module realm to exercise its plain-object contract.
vm.runInContext(`
const V=ForgeView,workspace='w'+'1'.repeat(32),view_id='v'+'2'.repeat(32);
const reference={v:2,workspace,kind:'tables',view_id,revision:3};
if(JSON.stringify(V.decodeView(V.encodeView(reference)))!==JSON.stringify(reference))throw Error('round trip');
const rejects=value=>{try{V.validateView(value);return false;}catch{return true;}};
for(const value of [
 {v:2,workspace,kind:'tables',view_id},{v:2,workspace,kind:'tables',revision:3},
 {...reference,revision:0},{...reference,revision:1.5},{...reference,revision:2147483648},
 {...reference,view_id:'../view'},{...reference,view_id:'x'.repeat(97)},
 {...reference,kind:'flows'},{...reference,v:1,workspace:'/legacy'},
 {...reference,descriptor:{}},{...reference,source_snapshot:'a'.repeat(64)}
])if(!rejects(value))throw Error('invalid saved view locator admitted');
V.validateView({v:1,workspace:'/legacy',kind:'tables'});
V.validateView({v:2,workspace,kind:'tables'});
if(V.viewSchema.anyOf.filter(spec=>spec.properties.kind.const==='tables').length!==2)throw Error('legacy schema lost');
`,context);
'''
    result=subprocess.run(['node','-'],input=script,text=True,cwd=root,capture_output=True,timeout=10)
    assert result.returncode==0,result.stdout+result.stderr
