// Project the saved events of the selected cell with the development timeline.
export function notebookTranscript(flow, original, api, onChange) {
  let session=null, sequence=0, loading=false, desired=0, generation=0;
  let source=null;
  function resetOriginal() {
    source=null;original.replaceChildren();original.hidden=true;
  }
  function showOriginal(item) {
    resetOriginal();
    const ref=item.sourceRef;
    if(!ref || !session)return;
    const sid=session, selection={};source=selection;
    const text=document.createElement('pre'), next=document.createElement('button'), close=document.createElement('button');
    next.textContent='下一页';close.textContent='关闭原文';close.onclick=resetOriginal;
    original.hidden=false;original.append(close,next,text);
    let after=ref.from_sequence-1;
    async function read() {
      next.disabled=true;
      try {
        const page=await api('dev-output',{session_id:sid,...ref,after_sequence:after});
        if(source!==selection)return;
        text.textContent=page.chunks.map(c=>c.text).join('');
        after=page.next_sequence;next.hidden=after>=page.last_sequence;
      } catch(error) {if(source===selection)text.textContent=error.message;}
      finally {next.disabled=false;}
    }
    next.onclick=read;read();
  }
  const chat=globalThis.ForgeDevelopmentChat.create({flow,onRaw:showOriginal,onChange});
  function project(event) {
    if(event.session_id!==session || event.sequence<=sequence)return;
    sequence=event.sequence;
    if(event.type==='message.user'){
      const text=event.data.text.split(/\n\n当前 Notebook：[^\n]*\n\n本轮运行许可：(?:允许|仅编辑)\n\n要调整的单元格：/,1)[0];
      chat.project({...event,data:{...event.data,text}});
    }else chat.project(event);
  }
  async function history() {
    if(loading||!session)return;
    loading=true;const current=generation;
    try {
      do {
        const page=await api('dev-events',{session_id:session,after_sequence:sequence,limit:50});
        if(generation!==current)return;
        for(const event of page.events)project(event);
        desired=Math.max(desired,page.last_sequence);
        if(!page.events.length)break;
      }while(sequence<desired);
    }catch(error){if(current===generation){original.hidden=false;original.textContent=error.message;}}
    finally{if(current===generation)loading=false;}
  }
  function select(sid) {
    if(sid===session)return;
    ++generation;session=sid;sequence=desired=0;loading=false;
    chat.reset();resetOriginal();
    if(sid)history();
  }
  function receive(sid,events) {
    if(session!==sid || !events.length)return;
    desired=Math.max(desired,events[events.length-1].sequence);
    if(loading)return;
    if(events[0].sequence>sequence+1){history();return;}
    for(const event of events)project(event);
  }
  return {select,receive,dispose(){++generation;session=null;resetOriginal();chat.dispose();}};
}
