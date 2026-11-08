/* One bounded subscription for the selected immutable run/attempt target. */
(() => {
  'use strict';
  function create({project,api,isCurrent,onSnapshot,onFailure,onDetails=async()=>{}}) {
    let selected=null,serial=0,controller=new AbortController(),timer=null,pending=null;
    let revision=null,failures=0,live=false,disposed=false;
    function clear(){clearTimeout(timer);timer=null;}
    function current(generation,target){return !disposed&&generation===serial&&selected===target&&isCurrent(target);}
    function schedule(){
      clear();
      if(disposed||!selected||!live||document.hidden)return;
      timer=setTimeout(()=>refresh().catch(()=>{}),Math.min(30000,2000*2**failures)*(1+Math.random()*.1));
    }
    async function refresh(){
      if(disposed||!selected||document.hidden)return;
      if(pending)return pending.promise;
      const target=selected,generation=serial,signal=controller.signal;
      const task={};
      task.promise=(async()=>{
        try{
          const value=await api('run-snapshot',{run_id:target.run_id,attempt:target.attempt,...(revision?{revision}:{})},undefined,project,{signal});
          if(!current(generation,target))return;
          if(value.run_id!==target.run_id||(value.availability!=='unsupported'&&value.attempt!==target.attempt))throw new Error('运行观测身份不一致');
          if(value.digest&&value.digest!==target.digest)throw new Error('运行封存依据不一致');
          if(value.revision)revision=value.revision;
          if(!value.unchanged)live=value.availability!=='unsupported'&&value.current_attempt===target.attempt&&
            (['prepared','starting','running','stopping'].includes(value.status)||value.state?.cleanup==='pending');
          await onSnapshot(value,target);
          if(current(generation,target))await onDetails(target);
          failures=0;
        }catch(error){
          if(!current(generation,target)||error.name==='AbortError')return;
          failures=Math.min(4,failures+1);
          if(error.code==='unauthorized')live=false;
          onFailure(error,target);
          throw error;
        }finally{
          if(pending===task)pending=null;
          if(current(generation,target))schedule();
        }
      })();
      pending=task;return task.promise;
    }
    function set(target){
      clear();controller.abort();controller=new AbortController();serial++;revision=null;failures=0;pending=null;
      selected=Object.freeze({...target});live=true;return refresh();
    }
    function stop(){clear();controller.abort();controller=new AbortController();serial++;selected=null;pending=null;live=false;}
    function visibility(){if(document.hidden){clear();controller.abort();controller=new AbortController();serial++;pending=null;}else if(live)refresh().catch(()=>{});}
    document.addEventListener('visibilitychange',visibility);
    return Object.freeze({set,refresh,stop,dispose:()=>{disposed=true;stop();document.removeEventListener('visibilitychange',visibility);}});
  }
  globalThis.ForgeWorkbenchObserver=Object.freeze({create});
})();
