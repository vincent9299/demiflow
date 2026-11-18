/* One editable current argument set per flow; no parameter-profile chooser. */
(() => {
  'use strict';
  const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
  function mount({host,request,canWrite,onChange,onSaved,onVisibility=()=>{}}){
    const panel=el('section',null,'flow-arguments');panel.hidden=true;panel.setAttribute('aria-label','流程业务参数');host.append(panel);
    const drafts=new Map();let entry=null,sequence=0,disposed=false;
    const dirty=()=>!!entry&&!!drafts.get(entry)?.dirty;
    const hide=()=>{panel.hidden=true;onVisibility();};
    function paint(){
      const draft=drafts.get(entry);if(!draft)return;
      panel.replaceChildren();const head=el('header'),info=el('div');info.append(el('span','当前流程','op-eyebrow'),el('h2','业务参数'));
      const close=el('button','×','op-text-button');close.setAttribute('aria-label','关闭业务参数');close.onclick=hide;head.append(info,close);panel.append(head);
      const form=el('form'),status=el('p',draft.dirty?'有未保存修改':'','flow-argument-status');status.setAttribute('role','status');
      const inputs=new Map();
      for(const field of draft.state.fields){
        const label=el('label'),name=el('span',field.name);label.append(name);
        const value=draft.values[field.name];let input;
        if(typeof value==='boolean'||field.annotation==='bool'){input=el('input');input.type='checkbox';input.checked=value;}
        else if(typeof value==='object'&&value!==null||['dict','list'].includes(field.annotation)){input=el('textarea');input.rows=5;input.value=JSON.stringify(value??(field.annotation==='list'?[]:{}),null,2);input.dataset.structured='true';}
        else{input=el('input');input.type=(typeof value==='number'||['int','float'].includes(field.annotation))?'number':'text';if(input.type==='number')input.step='any';input.value=value==null?'':String(value);}
        input.setAttribute('aria-label',field.name);input.disabled=!draft.state.writable||!canWrite();
        input.oninput=()=>{draft.raw[field.name]=input.type==='checkbox'?input.checked:input.value;draft.dirty=true;status.textContent='有未保存修改';save.disabled=!canWrite();onChange();};
        if(Object.hasOwn(draft.raw,field.name)){if(input.type==='checkbox')input.checked=draft.raw[field.name];else input.value=draft.raw[field.name];}
        label.append(input);if(field.default_known)label.append(el('small','默认：'+JSON.stringify(field.default),'op-muted'));else if(field.required)label.append(el('small','必填','op-muted'));
        form.append(label);inputs.set(field.name,{input,field});
      }
      if(!draft.state.fields.length)form.append(el('p','这条流程没有需要填写的业务参数。','op-muted'));
      if(draft.state.reason)form.append(el('p',draft.state.reason,'op-muted'));
      const actions=el('footer'),save=el('button','保存参数','primary');save.type='submit';save.disabled=!canWrite()||!draft.dirty||!draft.state.writable;
      if(!canWrite())status.textContent='当前为只读；保存需要项目的参数编辑权限。';
      actions.append(status,save);form.append(actions);panel.append(form);
      form.onsubmit=async event=>{
        event.preventDefault();if(save.disabled)return;
        const target=entry,ticket=++sequence;save.disabled=true;
        try{
          const argumentsValue={};
          for(const [name,{input,field}]of inputs){
            if(input.value===''&&input.type!=='checkbox'){if(field.required)throw new Error('请填写 '+name);continue;}
            argumentsValue[name]=input.type==='checkbox'?input.checked:input.dataset.structured==='true'?JSON.parse(input.value):input.type==='number'?Number(input.value):input.value;
          }
          const result=await request('flow-arguments-save',{}, {entry:target,revision:draft.state.revision,source_sha256:draft.state.source_sha256,arguments:argumentsValue});
          drafts.set(target,{state:result,values:Object.fromEntries(result.fields.filter(f=>f.present).map(f=>[f.name,f.value])),raw:{},dirty:false});
          if(disposed||ticket!==sequence||entry!==target)return;
          await onSaved(result);paint();panel.querySelector('.flow-argument-status').textContent='已保存；请解析更新后的计划。';
        }catch(error){if(!disposed&&ticket===sequence){status.textContent=error.message;save.disabled=false;}}
      };
    }
    return {
      async show(target){entry=target;const ticket=++sequence;panel.hidden=false;
        if(!drafts.has(target)){panel.replaceChildren(el('p','读取业务参数…','op-muted'));try{const state=await request('flow-arguments',{entry:target});if(disposed||ticket!==sequence)return;
          drafts.set(target,{state,values:Object.fromEntries(state.fields.filter(f=>f.present).map(f=>[f.name,f.value])),raw:{},dirty:false});
        }catch(error){if(ticket===sequence)panel.replaceChildren(el('p',error.message,'op-error'));return;}}
        paint();
      },
      changeFlow(target){entry=target;sequence++;hide();},
      dirty,
      visible:()=>!panel.hidden,
      invalidate(target){if(!drafts.get(target)?.dirty)drafts.delete(target);},
      hide,
      dispose(){disposed=true;sequence++;panel.remove();}
    };
  }
  globalThis.DemiFlowArguments=Object.freeze({mount});
})();
