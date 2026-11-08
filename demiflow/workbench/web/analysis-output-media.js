/* Media ownership only. All inspection/bytes/URLs come from trusted adapters. */
(() => {
  'use strict';
  const MIB=1024*1024, limits={image:8*MIB,audio:16*MIB,video:64*MIB};
  const mimes={image:['image/png','image/jpeg','image/webp','image/gif'],audio:['audio/wav','audio/mpeg','audio/ogg','audio/webm'],video:['video/mp4','video/webm','video/ogg']};
  let encoded=0, activePlayer=null;
  const allocations=new Set(),pending=new Set();
  const releaseOwned=dispose=>{if(typeof dispose!=='function')return;try{Promise.resolve(dispose()).catch(()=>{});}catch{/* hard ticket expiry covers failed revocation */}};
  const valid=(kind,ref)=>{
    if(!ref||typeof ref!=='object'||Array.isArray(ref)||!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$/.test(ref.artifact_id)||!/^[a-f0-9]{64}$/.test(ref.sha256)||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>limits[kind]||!mimes[kind]?.includes(ref.mime))throw Error('需要已授权、带摘要的媒体引用');
    if(kind==='image'||kind==='video'){
      if(!Number.isSafeInteger(ref.width)||!Number.isSafeInteger(ref.height)||ref.width<1||ref.height<1||ref.width>8192||ref.height>8192)throw Error('媒体缺少经核对的尺寸');
      if(kind==='image'&&ref.width*ref.height>4*1024*1024)throw Error('图片超过 4M 像素预算');
      if(kind==='video'&&(ref.width>1920||ref.height>1080||!(ref.fps>0&&ref.fps<=30)))throw Error('视频超过 1080p30 或帧率未知');
    }
    if(kind!=='image'&&(!(Number.isFinite(ref.duration_s)&&ref.duration_s>=0&&ref.duration_s<=(kind==='audio'?600:300))||typeof ref.codec!=='string'))throw Error('媒体缺少经核对的时长或 codec');
  };
  const sameReference=(expected,actual)=>{
    for(const key of ['artifact_id','sha256','mime','bytes','output_index'])if(expected?.[key]!==actual?.[key])throw Error('媒体固定引用或摘要与预检结果不一致');
  };
  function closeOtherVideos(except){
    let closed=0;
    for(const owned of [...allocations])if(owned!==except&&owned.kind==='video'&&!owned.closed){owned.close('已切换到其他视频，可在此重新加载');closed++;}
    return closed;
  }
  function create({host,kind,reference,resolveMedia,signal,isCurrent=()=>host.isConnected}){
    let dead=false,attempt=null,generation=0;
    const alive=()=>!dead&&!signal?.aborted&&isCurrent()&&host.isConnected;
    const live=owned=>alive()&&attempt===owned&&!owned.closed;
    const loadLabel=kind==='image'?'预览图片':kind==='audio'?'加载音频':'加载视频';
    const button=document.createElement('button');button.type='button';button.textContent=loadLabel;host.append(button);
    const status=document.createElement('p');status.className='muted small';status.setAttribute('role','status');host.append(status);
    const freeAdmission=owned=>{
      if(owned.reserved){encoded-=owned.reserved;owned.reserved=0;}
      allocations.delete(owned);
    };
    function unload(owned){
      if(!owned)return;
      owned.closed=true;owned.abort.abort();
      if(activePlayer===owned)activePlayer=null;
      if(owned.element){
        const element=owned.element;owned.element=null;
        element.removeEventListener('play',owned.onPlay);element.removeEventListener('error',owned.onError);
        element.removeEventListener('load',owned.onLoad);
        if(kind!=='image'){element.pause();element.removeAttribute('src');element.load();}else element.removeAttribute('src');
        element.remove();
      }
      if(owned.zoom){owned.zoom.onclick=null;owned.zoom.remove();owned.zoom=null;}
      if(owned.viewport){owned.viewport.onkeydown=null;owned.viewport.remove();owned.viewport=null;}
      if(owned.url){URL.revokeObjectURL(owned.url);owned.url=null;}
      if(owned.release){const release=owned.release;owned.release=null;releaseOwned(release);}
      // A cancelled adapter/digest may still retain bytes until its promise
      // settles. Keep that charge (and a bounded pending slot), not a decoder.
      if(!owned.inFlight)freeAdmission(owned);
    }
    function close(reason='预览已关闭'){
      if(dead)return;
      const owned=attempt;attempt=null;generation++;unload(owned);
      if(alive()){button.textContent=loadLabel;button.disabled=false;status.textContent=reason;}
    }
    const dispose=()=>{if(dead)return;dead=true;const owned=attempt;attempt=null;generation++;unload(owned);button.onclick=null;button.disabled=true;signal?.removeEventListener('abort',dispose);};
    const handle={get element(){return attempt?.element||null;},close,dispose};
    signal?.addEventListener('abort',dispose,{once:true});
    async function load(){
      if(!alive()||attempt)return;
      const owned={kind,generation:++generation,abort:new AbortController(),closed:false,inFlight:true,reserved:0,element:null,url:null,release:null,zoom:null,viewport:null,close};
      attempt=owned;button.textContent='取消加载';button.disabled=false;status.textContent='正在核对固定来源与媒体元数据…';
      try{
        if(typeof resolveMedia!=='function')throw Error('当前来源未接入经授权的媒体读取器');
        const beforePrepare=()=>{if(!live(owned))throw new DOMException('媒体加载已关闭','AbortError');return kind==='video'?closeOtherVideos(owned):0;};
        if(kind==='video')beforePrepare();
        if(!reference||!Number.isSafeInteger(reference.bytes)||reference.bytes<1||reference.bytes>limits[kind])throw Error('媒体编码大小未知或超过预算');
        if(pending.size>=4)throw Error('当前媒体读取已达 4 项预算，请稍后重试');
        const admission=kind==='video'?MIB:2*reference.bytes;
        if(encoded+admission>32*MIB)throw Error('当前媒体编码工作集超过 32MiB，请先关闭其他预览');
        owned.reserved=admission;encoded+=admission;allocations.add(owned);pending.add(owned);
        const resolved=await resolveMedia({kind,reference,signal:owned.abort.signal,beforePrepare});
        if(!live(owned)){releaseOwned(resolved?.dispose);return;}
        owned.release=typeof resolved?.dispose==='function'?resolved.dispose:null;
        const ref=resolved?.descriptor;valid(kind,ref);
        sameReference(reference,ref);
        const element=owned.element=document.createElement(kind==='image'?'img':kind);element.className='analysis-media';element.setAttribute('aria-label',kind+' '+ref.artifact_id);
        if(kind==='image'){element.alt='授权图片预览';element.width=Math.min(ref.width,512);element.height=Math.max(1,Math.round(ref.height*element.width/ref.width));element.loading='lazy';element.decoding='async';}
        else{
          element.controls=true;element.preload='none';element.autoplay=false;
          const codec=ref.codec==='h264'?'avc1.42E01E':ref.codec==='vp8'?'vp8':ref.codec==='vp9'?'vp9':ref.codec==='theora'?'theora':ref.codec==='aac'?'mp4a.40.2':ref.codec;
          const tested=ref.mime==='audio/wav'?ref.mime:ref.mime+'; codecs="'+codec+'"';
          if(!element.canPlayType(tested))throw Error('此浏览器缺少 '+ref.codec+' codec，无法预览');
          owned.onPlay=()=>{
            if(!live(owned))return;
            if(activePlayer&&activePlayer!==owned)activePlayer.close('已切换到其他播放器，可在此重新加载');
            activePlayer=owned;
          };
          element.addEventListener('play',owned.onPlay);
        }
        owned.onError=()=>{if(live(owned))close('浏览器解码失败，媒体不可预览；可重新加载');};
        element.addEventListener('error',owned.onError);
        if(resolved.blob instanceof Blob){
          if(kind==='video'||resolved.blob.size!==ref.bytes||resolved.blob.type!==ref.mime)throw Error('媒体 blob 大小或 MIME 不匹配');
          const actual=[...new Uint8Array(await crypto.subtle.digest('SHA-256',await resolved.blob.arrayBuffer()))].map(v=>v.toString(16).padStart(2,'0')).join('');
          if(!live(owned))return;if(actual!==ref.sha256)throw Error('媒体 blob 摘要不匹配');
          owned.url=URL.createObjectURL(resolved.blob);element.src=owned.url;
        }else{
          const target=new URL(resolved.url,location.href);
          if(target.origin!==location.origin||target.username||target.password||target.hash||[...target.searchParams.keys()].some(key=>/token|authorization|secret/i.test(key))||typeof resolved.authorizeURL!=='function'||resolved.authorizeURL(target,{kind,reference,descriptor:ref})!==true)throw Error('媒体 URL 未通过当前 workspace 适配器的固定路由核对');
          element.src=target.href;
        }
        if(!live(owned))return;
        const description=ref.mime+' · '+ref.bytes+' 字节'+(kind==='image'?' · '+ref.width+'×'+ref.height:' · '+ref.codec+' · '+ref.duration_s+' 秒');
        if(kind==='image'){
          let expanded=false;
          const viewport=owned.viewport=document.createElement('div');viewport.className='analysis-image-viewport';viewport.id='analysis-image-'+crypto.randomUUID();viewport.tabIndex=-1;
          const zoom=owned.zoom=document.createElement('button');zoom.type='button';zoom.className='analysis-image-zoom';zoom.textContent='原位放大';zoom.title='在当前区域放大查看图片';
          zoom.setAttribute('aria-controls',viewport.id);zoom.setAttribute('aria-expanded','false');zoom.disabled=!element.complete||!element.naturalWidth;
          viewport.append(element);host.append(zoom,viewport);
          const fit=()=>{
            expanded=false;viewport.dataset.expanded='false';element.style.removeProperty('width');element.style.removeProperty('height');
            viewport.tabIndex=-1;viewport.removeAttribute('role');viewport.removeAttribute('aria-label');
            zoom.textContent='原位放大';zoom.title='在当前区域放大查看图片';zoom.setAttribute('aria-expanded','false');status.textContent=description+' · 适应卡片';viewport.scrollLeft=viewport.scrollTop=0;
          };
          zoom.onclick=()=>{
            if(!live(owned))return;if(expanded){fit();return;}
            const box=element.getBoundingClientRect();if(!box.width||!box.height)return;
            // Magnify the actual fitted image, including small sources and
            // narrow cards. CSS presentation remains within 4096px/4M pixels;
            // the same admitted image, decoded source and URL are retained.
            const scale=Math.min(Math.max(2,256/box.width),4096/Math.max(box.width,box.height),Math.sqrt(4*1024*1024/(box.width*box.height)));
            expanded=true;viewport.dataset.expanded='true';element.style.width=(box.width*scale)+'px';element.style.height='auto';
            viewport.tabIndex=0;viewport.setAttribute('role','region');viewport.setAttribute('aria-label','放大图片，可使用方向键滚动，Esc 缩回');
            zoom.textContent='缩回图片';zoom.title='缩回以适应当前卡片';zoom.setAttribute('aria-expanded','true');status.textContent=description+' · 已放大 '+Math.round(scale*100)+'%（相对预览）；可在图片区域滚动，Esc 缩回';viewport.scrollLeft=viewport.scrollTop=0;
          };
          viewport.onkeydown=event=>{if(event.key==='Escape'&&expanded&&live(owned)){event.preventDefault();fit();zoom.focus();}};
          owned.onLoad=()=>{if(live(owned)){zoom.disabled=false;if(!expanded)status.textContent=description+' · 适应卡片';}};
          element.addEventListener('load',owned.onLoad,{once:true});
          status.textContent=description+(zoom.disabled?' · 图片加载中…':' · 适应卡片');
        }else{host.append(element);status.textContent=description;}
        button.textContent='关闭预览';
      }catch(error){if(live(owned))close(error.message||'媒体不可用');}
      finally{
        owned.inFlight=false;pending.delete(owned);
        if(!live(owned))unload(owned);
      }
    }
    button.onclick=()=>{if(attempt)close();else load();};
    if(signal?.aborted)dispose();
    return handle;
  }
  function createTransport({inspect,readRange,playback}){
    if(typeof inspect!=='function'||typeof readRange!=='function')throw Error('媒体 transport 需要可信的 inspect/readRange');
    return async request=>{
      const inspected=await inspect(request),descriptor=inspected?.descriptor;valid(request.kind,descriptor);sameReference(request.reference,descriptor);
      if(request.signal?.aborted)throw new DOMException('aborted','AbortError');
      if(request.kind==='video'){
        request.beforePrepare?.();
        if(typeof playback!=='function')throw Error('视频 range 尚未接入可认证的浏览器传输；当前 API 的 X-Forge-Token 不能放入 URL');
        return {...await playback({...request,descriptor}),descriptor};
      }
      const chunks=[];let size=0;
      for(let offset=0;offset<descriptor.bytes;offset+=MIB){
        if(request.signal?.aborted)throw new DOMException('aborted','AbortError');
        const end=Math.min(descriptor.bytes-1,offset+MIB-1);
        const response=await readRange({...request,descriptor,start:offset,end});
        if(!(response instanceof Response)||response.status!==206||response.headers.get('Content-Range')!==`bytes ${offset}-${end}/${descriptor.bytes}`||response.headers.get('Content-Type')!==descriptor.mime||!response.body)throw Error('媒体 range 响应身份/范围/MIME 不一致');
        const stream=response.body.getReader();let received=0;
        try{while(true){const {done,value}=await stream.read();if(done)break;if(received+value.byteLength>end-offset+1)throw Error('媒体 range 超过请求预算');received+=value.byteLength;size+=value.byteLength;chunks.push(value);}}
        finally{await stream.cancel().catch(()=>{});stream.releaseLock();}
        if(received!==end-offset+1)throw Error('媒体 range 字节被截断');
      }
      if(size!==descriptor.bytes)throw Error('媒体总字节不一致');
      return {descriptor,blob:new Blob(chunks,{type:descriptor.mime})};
    };
  }
  globalThis.ForgeAnalysisMedia=Object.freeze({create,createTransport,stats:()=>({encodedBytes:encoded,allocations:allocations.size,activePlayers:activePlayer?1:0,pendingLoads:pending.size})});
})();
