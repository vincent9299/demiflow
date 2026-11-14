/* W027 · logical_selection state protocol. Fake-free pure state checks. */
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const path=require('node:path');
const context=vm.createContext({TextEncoder,TextDecoder,Date});
vm.runInContext(fs.readFileSync(path.join(__dirname,'../../demiflow/workbench/web/workbench-state.js'),'utf8'),context);
const A='w'+'a'.repeat(32),id='r'+'1'.repeat(24);
const clean=value=>JSON.parse(JSON.stringify(value));
let groups=0;
const state=context.ForgeWorkbenchState.create(A);
const source={entry:'flows/test.py:run',params:'flows/a.json'};
const ticket=state.transition(source);
assert(state.acceptGraph(ticket,{binding:{workspace_id:A,basis_kind:'source',entry:source.entry,params:source.params},nodes:[{id:'n1'},{id:'n2'}]}));

// 1. initial logical_selection is null
assert.equal(state.current().logical_selection,null);groups++;

// 2. chooseNode without selection: node set, logical_selection null, detail-bound ticket
const plain=state.chooseNode('n1');
assert.equal(state.current().node,'n1');assert.equal(state.current().logical_selection,null);
assert.equal(plain.detail!==null,true);groups++;

// 3. two-key selection stored with exact shape
state.chooseNode('n2',{logical_node_id:'ln7',occurrence_id:'occ2'});
assert.deepEqual(clean(state.current().logical_selection),{logical_node_id:'ln7',occurrence_id:'occ2'});
assert.equal(state.current().node,'n2');groups++;

// 4. occurrence_id null is allowed (logical vertex selected, no occurrence yet)
state.chooseNode('n1',{logical_node_id:'ln0',occurrence_id:null});
assert.deepEqual(clean(state.current().logical_selection),{logical_node_id:'ln0',occurrence_id:null});groups++;

// 5. extra keys are stripped (exact Selection shape enforced)
state.chooseNode('n1',{logical_node_id:'ln0',occurrence_id:'occ0',bogus:'x',nested:{a:1}});
assert.deepEqual(Object.keys(state.current().logical_selection).sort(),['logical_node_id','occurrence_id']);groups++;

// 6. invalid selections are refused
for(const bad of [[], 'ln0', 42, {occurrence_id:'occ0'}, {logical_node_id:'',occurrence_id:null},
                  {logical_node_id:7,occurrence_id:null}, {logical_node_id:'ln0',occurrence_id:9},
                  {logical_node_id:'ln0'}])
  assert.throws(()=>state.chooseNode('n1',bad));
groups++;

// 7. a node outside the current graph is refused
assert.throws(()=>state.chooseNode('n9',{logical_node_id:'ln0',occurrence_id:null}));
assert.throws(()=>state.chooseNode('n9'));groups++;

// 8. chooseNode bumps detail (not epoch): stale detail ticket invalid, graph ticket valid
const graphTicket=state.capture();
const detailTicket=state.chooseNode('n1',{logical_node_id:'ln0',occurrence_id:'occ0'});
state.chooseNode('n2',{logical_node_id:'ln7',occurrence_id:'occ2'});
assert.equal(state.valid(detailTicket),false);
assert.equal(state.valid(graphTicket),true);
assert.equal(detailTicket.epoch,graphTicket.epoch);groups++;

// 9. transition clears node + logical_selection + graph and bumps epoch
const beforeTransition=state.capture(true);
state.transition({run:id,attempt:1});
assert.equal(state.current().node,null);
assert.equal(state.current().logical_selection,null);
assert.equal(state.current().graph,null);
assert.equal(state.valid(beforeTransition),false);groups++;

// 10. defensive copy: mutating the input selection afterwards never changes stored value
const t2=state.transition({run:null,attempt:null,report:null,entry:source.entry,params:source.params});
state.acceptGraph(t2,{binding:{workspace_id:A,basis_kind:'source',entry:source.entry,params:source.params},nodes:[{id:'n1'}]});
const input={logical_node_id:'ln0',occurrence_id:'occ0'};
state.chooseNode('n1',input);
input.logical_node_id='tampered';input.occurrence_id='tampered';
assert.deepEqual(clean(state.current().logical_selection),{logical_node_id:'ln0',occurrence_id:'occ0'});groups++;

state.dispose();
console.log(JSON.stringify({unit_groups:groups,status:'passed',scope:'pure logical_selection state protocol; real browser and native acceptance pending'}));