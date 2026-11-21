/* Conversation turns use the existing project provider, while Python runs in Jupyter. */
const utf8=new TextEncoder();
const limited=(value,length)=>value.length<=length?value:value.slice(0,length)+'…';

function promptFor(request,context,tools){
  const parts=[request,'当前 Notebook：'+context.path,
    '要调整的单元格：\n'+limited(context.source,3000),
    '此前相邻代码：\n'+limited(context.before,2000),
    '当前卡片可见结果：'+limited(context.output,550),
    '引用的模块：'+JSON.stringify(context.selected.map(i=>[i.path,i.symbol])),
    '数据字典（固定版本）：'+limited(JSON.stringify(context.tables.slice(0,30)),3000)];
  let text=parts.join('\n\n');
  for(const item of tools){
    const line='\n'+JSON.stringify([item.path,item.symbol,item.tool_kind,limited(item.signature||'',280),limited(item.description||'',140)]);
    if(utf8.encode(text+line).length>30000)break;
    text+=line;
  }
  if(utf8.encode(text).length>32768)throw Error('当前要求与上下文较长，请缩短要求再试。');
  return text;
}

async function answer(api,sid,turn,after,active){
  let item=null,epoch=null,content='',paged=false,incomplete=false,terminal=null,state;
  while(active()){
    const response=await api('dev-events',{session_id:sid,after_sequence:after,limit:50,wait_ms:1200});
    if(!active())throw Error('已离开此工作区，可稍后取回回复。');
    for(const event of response.events){
      if(event.turn_id!==turn||event.session_id!==sid)continue;
      const data=event.data;
      if(event.type==='turn.state'){
        if(data.state)state=data.state;
        if(data.cleanup==='complete'||data.cleanup==='unknown')terminal=data;
      }
      if(['process','summary'].includes(data.channel))continue;
      const complete=event.type==='message.completed';
      if(!(complete||event.type==='message.delta'||(event.type==='output.delta'&&paged&&data.item_id===item&&event.provider_epoch===epoch)))continue;
      if(complete||item!==data.item_id||epoch!==event.provider_epoch){content='';paged=false;incomplete=false;}
      item=data.item_id;epoch=event.provider_epoch;
      if(complete){paged=!!data.paged;incomplete=!!data.interrupted||data.status==='incomplete';}
      content+=data.text||'';
      if(utf8.encode(content).length>65536)throw Error('该回复超过单张卡片的大小，请在开发会话中查看。');
    }
    after=response.next_sequence;
    if(terminal){
      if(terminal.state!=='completed'||incomplete||terminal.cleanup!=='complete')throw Object.assign(Error('本轮未完成。'+(terminal.reason||state)),{finished:true});
      const tokens=globalThis.markdownit('commonmark').parse(content,{});
      const blocks=tokens.filter(t=>t.type==='fence'&&/^(python|py|python3)$/i.test(t.info.trim()));
      if(blocks.length!==1||!blocks[0].content.trim())return {message:content,code:null};
      const [start,end]=blocks[0].map,lines=content.split('\n');
      const closer=lines[end-1]?.trim()||'',marker=blocks[0].markup;
      if(closer.length<marker.length||[...closer].some(ch=>ch!==marker[0]))return {message:content,code:null};
      return {message:[...lines.slice(0,start),...lines.slice(end)].join('\n').trim(),code:blocks[0].content};
    }
    if(after>=response.last_sequence&&['waiting_input','waiting_approval'].includes(state))throw Error('助手需要补充信息，可在开发会话中处理后取回回复。');
  }
  throw Error('稍后可取回这次回复。');
}

export function notebookConversation({api,project,path,cell,active}){
  const key='demiflow.notebook-assistant.cell.'+JSON.stringify([project,path,cell]);
  let record={};
  try{
    const saved=sessionStorage.getItem(key);
    if(saved)record=JSON.parse(saved)||{};
    else{
      // A previous version kept one pending turn per notebook. Finish it on
      // its original cell, then start a dedicated session for future prompts.
      const oldKey='demiflow.notebook-assistant.'+project+'.'+path;
      const old=JSON.parse(sessionStorage.getItem(oldKey)||'{}');
      if(old?.pending?.cell===cell){
        record={...old,legacyPending:true};
        sessionStorage.setItem(key,JSON.stringify(record));sessionStorage.removeItem(oldKey);
      }
    }
  }catch{}
  const save=()=>{try{sessionStorage.setItem(key,JSON.stringify(record));}catch{}};
  function finishTurn(){
    if(record.legacyPending)record={};
    else delete record.pending;
    save();
  }
  async function generate(request,context){
    if(context.cell!==cell)throw Error('此对话属于另一张卡片。');
    let pending=record.pending;
    if(pending&&pending.cell!==cell)throw Error('该请求属于另一张卡片。');
    if(!pending?.turn){
      const caps=await api('dev-capabilities');
      const provider=caps.providers?.find(p=>p.available);
      if(!caps.enabled||!caps.start||!caps.notebook_cells||!provider)throw Error('本项目当前无法生成单元格。'+(caps.reason||''));
      record.provider||=provider.id;
    }
    if(!pending){
      const index=await api('project-data-tools');
      pending={cell:context.cell,source:context.source,request,prompt:promptFor(request,context,index.definitions),id:crypto.randomUUID()};
      record.pending=pending;save();
    }
    if(!record.sid){
      record.createId||=crypto.randomUUID();save();
      const session=await api('dev-session-create',{}, {request_id:record.createId,provider_id:record.provider,new_conversation:true});
      record.sid=session.id;save();
    }
    if(!pending.turn){
      if(pending.revision===undefined){
        const session=await api('dev-session',{session_id:record.sid});
        pending.revision=session.revision;pending.after=session.sequence||0;save();
      }
      try{
        const turn=await api('dev-turn-start',{}, {session_id:record.sid,submission_id:pending.id,expected_revision:pending.revision,text:pending.prompt,operation:'notebook_cell'});
        pending.turn=turn.id;save();
      }catch(error){
        if(['conflict','invalid_request','operation_rejected','resource_limit','busy','not_found','permission_denied'].includes(error.code)){finishTurn();}
        throw error;
      }
    }
    try{
      const result=await answer(api,record.sid,pending.turn,pending.after,active);
      finishTurn();
      return {...result,source:pending.source,request:pending.request,turn:pending.turn};
    }catch(error){if(error.finished){finishTurn();}throw error;}
  }
  return {generate,get pending(){return record.pending;},get sessionId(){return record.sid;}};
}
