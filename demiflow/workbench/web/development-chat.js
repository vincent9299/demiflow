/* Framework-free adaptation of DeepSeek Harness 0.2.1-rc.2 ui-chat:
 * TurnProcessNodeView, ChatGroupSeat, ReasoningRow, MessageItem and
 * MessageIconActions. Provider events and Forge acceptance remain external.
 *
 * MIT License
 * Copyright (c) 2026 DeepSeek
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
(() => {
  'use strict';
  const LIMITS=Object.freeze({items:100,itemBytes:65536,textBytes:1048576,summary:256});
  const encoder=new TextEncoder(),decoder=new TextDecoder();
  const el=(tag,text,className)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(className)node.className=className;return node;};
  const paths={copy:'M8 7V5a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-2M5 7h9a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2Z',check:'m5 12 4 4L19 6',chevron:'m6 9 6 6 6-6',bash:'m4 6 6 6-6 6m10 0h6',file:'M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9zM14 3v6h6M8 13h8M8 17h5',notice:'M12 8v5m0 4h.01M12 3 2 21h20Z'};
  function icon(name){
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');svg.classList.add('dfc-icon');
    if(name==='think'){
      for(const angle of [0,60,120]){const oval=document.createElementNS(svg.namespaceURI,'ellipse');for(const [k,v]of Object.entries({cx:12,cy:12,rx:4.4,ry:10,transform:'rotate('+angle+' 12 12)'}))oval.setAttribute(k,v);svg.append(oval);}
    }else{const path=document.createElementNS(svg.namespaceURI,'path');path.setAttribute('d',paths[name]||paths.file);svg.append(path);}
    return svg;
  }
  function action(label,name){const button=el('button',undefined,'dfc-action');button.type='button';button.title=label;button.setAttribute('aria-label',label);button.append(icon(name));return button;}
  const firstLine=text=>text.slice(0,text.indexOf('\n')<0?text.length:text.indexOf('\n'));
  // Directly adapted from the official ReasoningRow: advance the live summary
  // only when a paragraph's first line completes, never rewrite the transcript.
  function latestCompletedParagraphFirstLine(text){
    let summary='',paragraphStart=0;const separator=/\r?\n(?:[\t ]*\r?\n)+/g;
    while(true){const next=separator.exec(text),end=next===null?text.length:next.index+next[0].indexOf('\n'),newline=text.indexOf('\n',paragraphStart);if(newline!==-1&&newline<=end){const candidate=text.slice(paragraphStart,newline).trim();if(candidate!=='')summary=candidate;}if(next===null)return summary;paragraphStart=next.index+next[0].length;}
  }
  function clip(text,maximum){
    let bytes=0,end=0;maximum=Math.max(0,maximum);
    for(const char of text){const point=char.codePointAt(0),size=point<128?1:point<2048?2:point<65536?3:4;if(bytes+size>maximum)break;bytes+=size;end+=char.length;}
    const truncated=end<text.length;
    // A clipped preview must not keep a huge backing string alive.
    return {text:truncated?decoder.decode(encoder.encode(text.slice(0,end))):text,bytes,truncated};
  }
  const excerpt=text=>firstLine(String(text||'')).replaceAll('**','').slice(0,LIMITS.summary);
  const scope=(event,kind,id)=>JSON.stringify([event.session_id,event.turn_id,event.provider_epoch||'',kind,id]);
  const turnKey=event=>JSON.stringify([event.session_id,event.turn_id]);
  const timestamp=value=>Number.isFinite(value)&&value>0?value:null;
  function localTime(seconds){if(!seconds)return '';const date=new Date(seconds*1000);return (date.getMonth()+1)+'/'+date.getDate()+' '+String(date.getHours()).padStart(2,'0')+':'+String(date.getMinutes()).padStart(2,'0');}
  function duration(seconds){const value=Math.max(0,Math.floor(seconds));return value<60?value+'s':value<3600?Math.floor(value/60)+'m '+value%60+'s':Math.floor(value/3600)+'h '+Math.floor(value%3600/60)+'m';}
  function create({flow,onRaw=()=>{},onChange=()=>{},onTrim=()=>{}}={}){
    if(!(flow instanceof Element))throw new TypeError('A chat flow element is required');
    const items=new Map(),turns=new Map(),textItems=new Map(),toolItems=new Map(),seen=new Map();
    let textBytes=0,trimmed=0,disposed=false,lastText=null,pagedText=null,latestTurn=null,renderTimer=null;
    flow.classList.add('dfc-chat');
    const stats=()=>({items:items.size,turns:turns.size,textBytes,trimmed});
    const changed=()=>{if(!disposed)onChange(stats());};
    function disclosure(label,className,onToggle){
      const node=el('details',undefined,className),heading=el('summary'),leading=el('span',undefined,'dfc-leading'),name=el('span',label,'dfc-label'),body=el('div',undefined,'dfc-disclosure-body');
      leading.append(icon('chevron'));heading.append(leading,name);node.append(heading,body);
      const result={node,heading,leading,label:name,body,manual:null};
      heading.onclick=event=>{event.preventDefault();result.manual=!node.open;node.open=result.manual;onToggle?.();queueRender();changed();};
      return result;
    }
    function showDefault(group,open){group.node.open=group.manual===null?open:group.manual;}
    function paintTurn(turn){
      const live=!turn.done,elapsed=turn.started===null||turn.done&&turn.ended===null?null:(turn.ended??Date.now()/1000)-turn.started;
      const label=turn.done?(turn.state==='completed'?(elapsed===null?'Completed':'Completed in '+duration(elapsed)):turn.state==='cancelled'?'Stopped':turn.state==='interrupted'?'Interrupted':'Failed'):(elapsed===null?'Working':'Working · '+duration(elapsed));
      turn.process.label.textContent=live&&turn!==latestTurn?'Previous activity':label;
      turn.process.node.hidden=!turn.processCount;
      turn.step.label.textContent=turn.toolCount?(turn.commandCount?(turn.done?'Ran commands':'Running commands'):(turn.done?'Used tools':'Using tools')):(turn.done?'Thought for a while':'Thinking');
      turn.process.node.dataset.running=String(live);turn.step.node.dataset.running=String(live);
      showDefault(turn.process,live||turn===latestTurn);
      showDefault(turn.step,live||turn===latestTurn);
    }
    function newTurn(event){
      const key=turnKey(event);let turn=turns.get(key);if(turn)return turn;
      while(turns.size>=LIMITS.items){const empty=[...turns.values()].find(value=>!value.ids.size);if(empty){empty.node.remove();turns.delete(empty.key);if(latestTurn===empty)latestTurn=null;}else trimOne();}
      const node=el('section',undefined,'dfc-turn'),user=el('div',undefined,'dfc-user-seat'),answers=el('div',undefined,'dfc-answers');node.dataset.turnId=event.turn_id;
      const process=disclosure('Working','dfc-turn-process'),step=disclosure('Thinking','dfc-step-process');process.body.append(step.node);node.append(user,process.node,answers);flow.append(node);
      turn={key,id:event.turn_id,session:event.session_id,node,user,answers,process,step,ids:new Set(),processCount:0,toolCount:0,commandCount:0,candidate:null,done:false,state:'running',started:event.type==='message.user'?timestamp(event.observed_at):null,ended:null};
      turns.set(key,turn);const prior=latestTurn;latestTurn=turn;if(prior)paintTurn(prior);paintTurn(turn);return turn;
    }
    function trimOne(){
      let victim=null;
      for(const pair of items.entries()){
        const item=pair[1];
        if(item.inProcess&&item.turn!==latestTurn&&!item.turn.process.node.open){victim=pair;break;}
      }
      if(!victim){
        for(const pair of items.entries()){
          const item=pair[1];
          if(item.inProcess&&item.turn!==latestTurn){victim=pair;break;}
        }
      }
      const [key,item]=victim||items.entries().next().value;item.view?.dispose();item.node.remove();textBytes-=item.bytes;items.delete(key);item.turn.ids.delete(key);
      if(item.textKey&&textItems.get(item.textKey)===key)textItems.delete(item.textKey);
      if(item.toolKey&&toolItems.get(item.toolKey)===key)toolItems.delete(item.toolKey);
      if(item.turn.candidate===item)item.turn.candidate=null;
      if(item.inProcess)item.turn.processCount--;if(item.kind==='tool')item.turn.toolCount--;if(item.command)item.turn.commandCount--;
      if(!item.turn.ids.size){item.turn.node.remove();turns.delete(item.turn.key);if(latestTurn===item.turn)latestTurn=[...turns.values()].at(-1)||null;}
      else paintTurn(item.turn);
      if(lastText?.id===key)lastText=null;trimmed++;onTrim();
    }
    function source(item,event,rawId){
      if(!item.sourceRef)item.sourceRef={item_id:rawId,turn_id:event.turn_id,...(event.provider_epoch?{provider_epoch:event.provider_epoch}:{}),from_sequence:event.sequence,through_sequence:event.sequence};
      else item.sourceRef.through_sequence=event.sequence;
    }
    function addItem(key,kind,event,rawId){
      while(items.size>=LIMITS.items)trimOne();
      const turn=newTurn(event),isRow=['think','tool','system'].includes(kind);
      const row=isRow?disclosure(kind==='think'?'Think':kind==='tool'?'Bash':'Notice','dfc-row dfc-'+kind):null;
      const node=row?.node||el('article',undefined,'dfc-message dfc-'+kind),body=row?.body||el('div',undefined,kind==='user'?'dfc-bubble':'dfc-message-body');
      if(!row)node.append(body);node.dataset.sequence=event.sequence;
      const actions=el('div',undefined,'dfc-actions'),copy=action('Copy message','copy'),raw=el('button','View original','dfc-raw');raw.type='button';raw.hidden=true;
      if(kind==='user'){const time=el('time',localTime(timestamp(event.observed_at)),'dfc-time');if(timestamp(event.observed_at))time.dateTime=new Date(event.observed_at*1000).toISOString();actions.append(time);}
      actions.append(copy,raw);(row?body:node).append(actions);
      const content=row?el('div',undefined,'dfc-row-content'):body;if(row)body.prepend(content);
      const item={key,node,body:content,actions,copy,source:raw,text:'',bytes:0,role:kind,kind,turn,row,dirty:false,truncated:false,view:null,running:false,inProcess:isRow};
      if(row){row.leading.replaceChildren(icon(kind==='think'?'think':kind==='tool'?'bash':'notice'),icon('chevron'));const preview=el('span',undefined,'dfc-summary');row.heading.append(el('span','·','dfc-separator'),preview);item.preview=preview;}
      if(kind==='user')turn.user.append(node);else if(isRow){turn.step.body.append(node);turn.processCount++;if(kind==='tool')turn.toolCount++;}else turn.answers.append(node);
      turn.ids.add(key);items.set(key,item);if(rawId!==null)source(item,event,rawId);
      copy.onclick=async()=>{try{await navigator.clipboard.writeText(item.text);if(items.get(key)===item){copy.replaceChildren(icon('check'));copy.title='Copied';}}catch{copy.title='Copy unavailable';}};
      raw.onclick=()=>onRaw(item);paintTurn(turn);return item;
    }
    function write(item,value,append=false){
      if(typeof value!=='string')value='';
      const capacity=Math.min(LIMITS.itemBytes,LIMITS.textBytes-textBytes+item.bytes),prior=item.bytes;
      if(!append||!item.truncated){const part=clip(value,append?capacity-item.bytes:capacity);item.text=(append?item.text:'')+part.text;item.bytes=(append?item.bytes:0)+part.bytes;item.truncated=part.truncated;}
      textBytes+=item.bytes-prior;item.dirty=true;
      if(item.kind==='user')item.body.textContent=item.text;
      else if(!item.view&&!item.body.firstChild)item.body.append(el('pre',item.text,'dfc-plain'));
      if(item.preview){const summary=item.kind==='think'?(item.running?latestCompletedParagraphFirstLine(item.text)||firstLine(item.text):firstLine(item.text)):item.summary||firstLine(item.text);item.preview.textContent=excerpt(summary);}
      item.copy.title=item.truncated?'Copy visible preview':'Copy message';item.copy.setAttribute('aria-label',item.copy.title);
      if(item.truncated&&!item.overflow){item.overflow=el('span','Preview limit reached','dfc-limit');item.actions.prepend(item.overflow);}
      else if(!item.truncated&&item.overflow){item.overflow.remove();item.overflow=null;}
      queueRender();
    }
    function visible(item){return !item.node.closest('details:not([open])')&&!item.node.closest('[hidden]');}
    function renderVisible(){
      renderTimer=null;if(disposed)return;
      for(const item of items.values()){
        item.source.hidden=!item.sourceRef||!(item.truncated||item.kind==='answer'||item.row?.node.open);
        if(item.kind==='user')continue;
        if(!visible(item)){item.view?.dispose();item.view=null;item.dirty=true;continue;}
        if(!item.dirty)continue;item.dirty=false;
        if(item.kind==='tool'||item.kind==='system'){item.body.replaceChildren(el('pre',item.text,'dfc-tool-output'));continue;}
        item.body.classList.add('dfc-markdown');
        if(globalThis.ForgeReading?.createMarkdownView){
          if(item.view)item.view.update(item.text);
          else item.view=ForgeReading.createMarkdownView({element:item.body,text:item.text,onChange:changed});
        }else item.body.replaceChildren(el('pre',item.text,'dfc-plain'));
      }
      changed();
    }
    function queueRender(){
      if(disposed||renderTimer!==null)return;
      let streaming=false;
      for(const item of items.values())if(item.dirty&&item.running){streaming=true;break;}
      renderTimer=setTimeout(renderVisible,streaming?33:25);
    }
    function finishText(){if(!lastText)return;const item=items.get(lastText.id);if(item){item.running=false;item.node.classList.remove('dfc-streaming');if(item.preview)item.preview.textContent=excerpt(item.text);item.dirty=true;}lastText=null;queueRender();}
    function archiveCandidate(turn){
      const item=turn.candidate;if(!item||item.inProcess)return;item.inProcess=true;item.node.classList.add('dfc-progress');turn.step.body.append(item.node);turn.processCount++;turn.candidate=null;paintTurn(turn);
    }
    function incomplete(item,value){
      if(value&&!item.incomplete){item.incomplete=el('span','Incomplete','dfc-incomplete');(item.row?item.row.heading:item.actions).append(item.incomplete);}
      if(!value&&item.incomplete){item.incomplete.remove();item.incomplete=null;}
    }
    function message(event){
      const data=event.data,kind=['process','summary'].includes(data.channel)?'think':'answer',base=scope(event,'message:'+(data.channel||'answer'),data.item_id),complete=event.type==='message.completed';
      let key=complete?textItems.get(base):lastText?.base===base?lastText.id:null,item=key&&items.get(key);
      if(!item){finishText();const turn=newTurn(event);archiveCandidate(turn);key=base+':'+event.sequence;item=addItem(key,kind,event,data.item_id);item.textKey=base;textItems.set(base,key);if(kind==='answer')turn.candidate=item;}
      else source(item,event,data.item_id);
      if(complete){
        item.running=false;item.node.classList.remove('dfc-streaming');incomplete(item,!!data.interrupted||data.status==='incomplete');item.sourceRef.from_sequence=event.sequence;
        write(item,data.paged?'':data.text,false);if(data.paged){lastText={base,id:key};pagedText=base;}else if(lastText?.id===key)lastText=null;
      }else{item.running=true;item.node.classList.add('dfc-streaming');lastText={base,id:key};write(item,data.text,true);}
      return item;
    }
    function toolLabel(data){const raw=data.rawInput||{};return data.type==='fileChange'?'Edit':data.command||raw.command||raw.cmd||/bash|command|shell/i.test(data.title||data.name||'')?'Bash':data.title||data.name||'Tool';}
    function tool(event,output=false){
      finishText();const data=event.data,rawId=output?data.item_id:data.id,key=scope(event,'tool',rawId);let item=items.get(toolItems.get(key));
      if(!item){archiveCandidate(newTurn(event));item=addItem(key,'tool',event,rawId);item.toolKey=key;toolItems.set(key,key);}
      else source(item,event,rawId);
      if(output){item.hasOutput=true;write(item,data.text,true);return;}
      const raw=data.rawInput||{},label=toolLabel(data);item.row.label.textContent=excerpt(label).slice(0,40);
      const commandKind=label==='Bash';if(commandKind!==!!item.command){item.turn.commandCount+=commandKind?1:-1;item.command=commandKind;paintTurn(item.turn);}
      item.summary=excerpt(raw.description||data.description||(data.title&&data.title!==label?data.title:null)||data.command||raw.command||raw.cmd||data.name||'');
      item.row.node.dataset.status=data.status||'running';
      const command=data.command||raw.command||raw.cmd;
      const content=Array.isArray(data.content)?data.content.map(value=>value.text||value.content?.text||'').filter(Boolean).join('\n'):data.output;
      if(content||!item.hasOutput)write(item,[command,content].filter(Boolean).join('\n\n')||JSON.stringify(data,null,2));
      else if(item.preview)item.preview.textContent=item.summary;
    }
    function system(event,label,body,rawId=null,identity=event.sequence){finishText();const key=scope(event,'system',identity),item=items.get(key)||addItem(key,'system',event,rawId);item.row.label.textContent=label;item.summary=excerpt(body);write(item,body);return item;}
    function review(event){
      finishText();
      const data=event.data,key=scope(event,'system','pipeline-review');
      let item=items.get(key);
      if(!item){
        item=addItem(key,'system',event,null);
        item.turn.review=item;
        item.turn.answers.append(item.node);
        item.inProcess=false;item.turn.processCount--;paintTurn(item.turn);
      }
      item.row.label.textContent='独立评审';
      if(event.type==='review.detail'){
        const value=data.item,location=value?.path?value.path+(value.line?':'+value.line:''):'';
        const body=typeof value==='string'?value:[location,value?.trigger,value?.impact,value?.suggestion,value?.reason,value?.detail].filter(Boolean).join(' · ');
        const label={findings:value?.severity==='blocking'?'需修正':'建议',gaps:'待核实',checks:'已核对',diagnostics:'说明'}[data.field]||'';
        write(item,(item.text?'\n':'')+label+'  '+body,true);
      }else{
        const status={passed:'已完成',invalid:'需要修改',incomplete:'未完成'}[data.status];
        item.reviewClosed=!!status;
        item.summary=status||(event.type==='review.requested'?'已提交':'正在评审');
        item.preview.textContent=item.summary;
        if(status==='需要修改')showDefault(item.row,true);
      }
    }
    function closeReview(turn,event,result={}){
      const item=turn.review;
      if(item?.reviewClosed||(!item&&!result.pipeline_review))return;
      review({...event,type:'review.completed',data:{status:result.pipeline_review||'incomplete'}});
      if(result.review_reason)write(turn.review,result.review_reason,true);
    }
    function project(event){
      if(disposed||!event||typeof event.type!=='string'||!event.data)return false;
      if(Number.isSafeInteger(event.sequence)){if(event.sequence<=(seen.get(event.session_id)||0))return true;seen.set(event.session_id,event.sequence);if(seen.size>LIMITS.items)seen.delete(seen.keys().next().value);}
      const data=event.data,bodyChunk=event.type==='output.delta'&&pagedText===scope(event,'message:'+(data.channel||'answer'),data.item_id);if(!bodyChunk)pagedText=null;
      if(event.type==='message.user'){
        finishText();const key=scope(event,'user',event.turn_id),item=addItem(key,'user',event,event.turn_id+':user');write(item,data.text);item.turn.started=timestamp(event.observed_at);paintTurn(item.turn);
      }else if(event.type==='message.delta'||event.type==='message.completed')message(event);
      else if(bodyChunk)message({...event,type:'message.delta'});
      else if(event.type==='output.delta')tool(event,true);
      else if(event.type.startsWith('item.'))tool(event);
      else if(event.type==='turn.state'){
        const turn=newTurn(event);if(data.state)turn.state=data.state;
        if(data.cleanup==='complete'){finishText();turn.done=true;turn.ended=timestamp(event.observed_at);for(const key of turn.ids){const item=items.get(key);if(item){item.running=false;item.node.classList.remove('dfc-streaming');if(item.preview)item.preview.textContent=excerpt(item.summary||item.text);}}}
        paintTurn(turn);
        try{
          const result=(typeof data.result==='string'?JSON.parse(data.result):data.result)||{};
          if(turn.done||['failed','cancelled','interrupted'].includes(turn.state))closeReview(turn,event,result);
          if(result.reason)system(event,'Run details',result.reason);
          for(const [key,label] of [['native_flow','原生数据流检查'],['project_layout','项目目录检查']]){
            const check=result.pipeline_checks?.[key];if(!check)continue;
            const status={passed:'通过',failed:'发现问题',incomplete:'待核对',not_applicable:'不适用'}[check.status]||'待核对';
            const details=(check.diagnostics||[]).map(d=>[d.rule,d.path+(d.line?':'+d.line:''),d.detail].filter(Boolean).join(' · '));
            const omitted=check.diagnostic_count-details.length;if(omitted>0)details.push('另有 '+omitted+' 项诊断');
            system(event,label,[status,...details].join('\n'),null,'pipeline:'+key);
          }
        }catch{}
      }else if(['review.requested','review.started','review.detail','review.completed'].includes(event.type))review(event);
      else if(event.type==='change.observed')system(event,'Changed files',(data.files||[]).map(file=>file.path+'\n'+(file.diff||file.diff_unavailable||'')).join('\n'),'change:'+event.sequence);
      else if(event.type==='check.reference')system(event,'Checks',JSON.stringify(data.result||data,null,2),'check:'+event.sequence);

      else if(event.type==='questions.observed')system(event,'Questions',JSON.stringify(data,null,2));
      else if(event.type==='provider.error')system(event,'Provider error',JSON.stringify(data,null,2));
      else if(event.type==='history.gap')system(event,'History','The provider did not replay history. These are the application’s saved events.');
      else if(event.type==='compaction.completed')system(event,'History archived',data.status==='compacted'?'Earlier context was compacted; original messages remain available.':'Compaction was not confirmed.');
      else return false;
      queueRender();changed();return true;
    }
    function updateSession(session){
      if(disposed||!session?.turn)return;const value=session.turn,turn=turns.get(JSON.stringify([session.id,value.id]));if(!turn)return;
      const nextStarted=timestamp(value.created)??turn.started,nextState=value.state||turn.state,nextDone=turn.done||value.cleanup==='complete';
      if(nextStarted===turn.started&&nextState===turn.state&&nextDone===turn.done)return;
      turn.started=nextStarted;turn.state=nextState;
      // A current session receipt can close a truncated history window, but no
      // duration is invented when the terminal event is outside that window.
      turn.done=nextDone;
      if(turn.review&&!turn.review.reviewClosed&&(nextDone||['failed','cancelled','interrupted'].includes(nextState))){
        const item=turn.review;item.reviewClosed=true;item.summary='未完成';item.preview.textContent=item.summary;
      }
      paintTurn(turn);queueRender();changed();
    }
    function reset(){clearTimeout(renderTimer);renderTimer=null;for(const item of items.values())item.view?.dispose();items.clear();turns.clear();textItems.clear();toolItems.clear();seen.clear();flow.replaceChildren();textBytes=0;lastText=pagedText=latestTurn=null;changed();}
    const timer=setInterval(()=>{if(!disposed)for(const turn of turns.values())if(!turn.done)paintTurn(turn);},1000);
    function dispose(){if(disposed)return;disposed=true;clearInterval(timer);reset();flow.classList.remove('dfc-chat');}
    return {project,updateSession,reset,dispose,items,get textBytes(){return textBytes;},get stats(){return stats();}};
  }
  globalThis.ForgeDevelopmentChat=Object.freeze({create,limits:LIMITS});
})();
