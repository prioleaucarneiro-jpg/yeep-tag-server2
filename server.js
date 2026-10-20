const http=require('http');
const {WebSocketServer,WebSocket}=require('ws');
const crypto=require('crypto');
const port=Number(process.env.PORT)||3000;
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'text/plain','Access-Control-Allow-Origin':'*'});res.end('Yeep Tag multiplayer server online');});
const wss=new WebSocketServer({server,maxPayload:4096});
const rooms=new Map();
const roomBlocks=new Map();
const roomItems=new Map();
const projectiles=new Map();
const roomSettings=new Map();
const playerHealth=new Map(); // Room-scoped authoritative placed blocks; reset when last player leaves.
const MAX_BLOCKS=180;
function send(ws,obj){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))}
function broadcast(room,obj,except){for(const ws of rooms.get(room)||[])if(ws!==except)send(ws,obj)}
function remove(ws){if(!ws.room)return;const room=ws.room;rooms.get(room)?.delete(ws);const settings=roomSettings.get(room);if(settings?.host===ws.id){settings.host=[...(rooms.get(room)||[])][0]?.id||'';broadcast(room,{type:'room_settings',settings})}broadcast(room,{type:'leave',id:ws.id},ws);if(!rooms.get(room)?.size){rooms.delete(room);roomBlocks.delete(room);roomItems.delete(room);projectiles.delete(room);playerHealth.delete(room);roomSettings.delete(room)}ws.room=null}
wss.on('connection',ws=>{
 ws.id=crypto.randomUUID();ws.room=null;ws.pose=null;ws.isAlive=true;ws.lastChat=0;ws.lastSwing=0;ws.lastThrow=0;
 ws.on('pong',()=>ws.isAlive=true);
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}
  if(m.type==='join'&&!ws.room){const room=String(m.room||'play').toLowerCase();if(!/^[a-z0-9_.-]{1,32}$/.test(room))return send(ws,{type:'error',message:'Invalid room code'});
   const members=rooms.get(room)||new Set();if(members.size>=24)return send(ws,{type:'error',message:'Room full (24 players)'});
   rooms.set(room,members);ws.room=room; if(!roomSettings.has(room))roomSettings.set(room,{npcs:false,mode:['tag','sandbox','modded'].includes(m.mode)?m.mode:'tag',host:ws.id});const hp=playerHealth.get(room)||new Map();playerHealth.set(room,hp);hp.set(ws.id,1000);send(ws,{type:'welcome',id:ws.id,peers:[...members].map(p=>({id:p.id,pose:p.pose})),blocks:[...(roomBlocks.get(room)||new Map()).values()],items:[...(roomItems.get(room)||new Map()).values()],weapons:[...(projectiles.get(room)||new Map()).values()],settings:roomSettings.get(room)});members.add(ws);broadcast(room,{type:'peer',id:ws.id,pose:null},ws);
  }else if(m.type==='room_settings'&&ws.room){
   const settings=roomSettings.get(ws.room);if(!settings||settings.host!==ws.id)return;
   if(typeof m.npcs==='boolean')settings.npcs=m.npcs;
   broadcast(ws.room,{type:'room_settings',settings});send(ws,{type:'room_settings',settings});
  }else if(m.type==='npc_state'&&ws.room){
   const settings=roomSettings.get(ws.room);if(!settings?.npcs||settings.host!==ws.id)return;
   if(!Array.isArray(m.npcs)||m.npcs.length>32)return;
   const npcs=m.npcs.filter(n=>Number.isInteger(n.i)&&n.i>=0&&n.i<32&&Array.isArray(n.p)&&n.p.length===3&&n.p.every(x=>Number.isFinite(x)&&Math.abs(x)<100)).map(n=>({i:n.i,p:n.p,y:Number.isFinite(n.y)?n.y:0,dead:!!n.dead,inf:!!n.inf}));
   broadcast(ws.room,{type:'npc_state',npcs},ws);
  }else if(m.type==='block_add'&&ws.room){
   const b=m.block;if(!b||typeof b!=='object')return;
   const nums=['x0','x1','y0','y1','z0','z1'];
   if(!Number.isInteger(b.i)||b.i<0||b.i>64||!nums.every(k=>typeof b[k]==='number'&&Number.isFinite(b[k])&&Math.abs(b[k])<100))return;
   if(b.x1<=b.x0||b.y1<=b.y0||b.z1<=b.z0||b.x1-b.x0>12||b.y1-b.y0>12||b.z1-b.z0>12)return;
   const blocks=roomBlocks.get(ws.room)||new Map();roomBlocks.set(ws.room,blocks);
   if(blocks.size>=MAX_BLOCKS)return send(ws,{type:'block_error',message:'Shared block limit reached'});
   const id=crypto.randomUUID(),block={id,i:b.i,...Object.fromEntries(nums.map(k=>[k,b[k]]))};
   blocks.set(id,block);send(ws,{type:'block_added',block});broadcast(ws.room,{type:'block_added',block},ws);
  }else if(m.type==='block_remove'&&ws.room){
   const id=String(m.id||'');const blocks=roomBlocks.get(ws.room);if(!blocks?.has(id))return;
   blocks.delete(id);broadcast(ws.room,{type:'block_removed',id});send(ws,{type:'block_removed',id});
  }else if(m.type==='item_drop'&&ws.room){
   const kind=String(m.kind||''),p=m.p;
   if(!['bj','rose','snow','bat','pin'].includes(kind)||!Array.isArray(p)||p.length!==3||!p.every(v=>Number.isFinite(v)&&Math.abs(v)<100)||!ws.pose)return;
   if(Math.hypot(...p.map((v,i)=>v-ws.pose.p[i]))>3)return;
   const items=roomItems.get(ws.room)||new Map();roomItems.set(ws.room,items);
   if(items.size>=80)return send(ws,{type:'item_error',message:'Room item limit reached'});
   const item={id:crypto.randomUUID(),kind,p,owner:ws.id};items.set(item.id,item);
   broadcast(ws.room,{type:'item_added',item});send(ws,{type:'item_added',item});
  }else if(m.type==='item_pickup'&&ws.room){
   const id=String(m.id||''),items=roomItems.get(ws.room),item=items?.get(id);
   if(!item||!ws.pose)return;
   if(Math.hypot(...item.p.map((v,i)=>v-ws.pose.p[i]))>2.3)return;
   items.delete(id);broadcast(ws.room,{type:'item_removed',id,by:ws.id});send(ws,{type:'item_removed',id,by:ws.id});
  }else if(m.type==='weapon_throw'&&ws.room){
   const now=Date.now();if(now-ws.lastThrow<180)return;ws.lastThrow=now;
   const a=m.p,v=m.v,kind=String(m.kind||'');
   const vec=q=>Array.isArray(q)&&q.length===3&&q.every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<500);
   if(!['bj','rose','snow','bat','pin'].includes(kind)||!vec(a)||!vec(v)||!ws.pose)return;
   const d=Math.hypot(a[0]-ws.pose.p[0],a[1]-ws.pose.p[1],a[2]-ws.pose.p[2]);
   if(d>3||Math.hypot(...v)>28)return;
   const ps=projectiles.get(ws.room)||new Map();projectiles.set(ws.room,ps);
   if(ps.size>=120)return;
   const id=crypto.randomUUID(),o={id,owner:ws.id,kind,p:[...a],v:[...v],age:0};ps.set(id,o);
   broadcast(ws.room,{type:'weapon_spawn',weapon:o},ws);
  }else if(m.type==='weapon_swing'&&ws.room){
   const now=Date.now();if(now-ws.lastSwing<550)return;ws.lastSwing=now;
   const target=String(m.target||''),kind=String(m.kind||'');
   if(!['bj','bat','rose','snow','pin'].includes(kind)||!ws.pose)return;
   const victim=[...(rooms.get(ws.room)||[])].find(p=>p.id===target);
   if(!victim?.pose||victim===ws)return;
   if(Math.hypot(...ws.pose.p.map((x,i)=>x-victim.pose.p[i]))>1.9)return;
   weaponHit(ws.room,ws.id,victim.id,kind);
  }else if(m.type==='chat'&&ws.room){
   const now=Date.now();if(now-ws.lastChat<750)return;ws.lastChat=now;
   const message=String(m.message||'').replace(/[\x00-\x1f\x7f]/g,' ').trim().slice(0,180);if(!message)return;
   const name=String(m.name||ws.pose?.n||'Yeep').replace(/[\x00-\x1f\x7f]/g,' ').trim().slice(0,14)||'Yeep';
   broadcast(ws.room,{type:'chat',id:ws.id,name,message},ws);
  }else if(m.type==='pose'&&ws.room&&m.pose&&typeof m.pose==='object'){
   const p=m.pose;
   const vec=a=>Array.isArray(a)&&a.length===3&&a.every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<500);
   if(!vec(p.p)||!vec(p.l)||!vec(p.r))return;
   const pose={p:p.p,l:p.l,r:p.r,y:p.y,tl:p.tl,ry:p.ry,hq:p.hq,f:p.f,hk:Array.isArray(p.hk)?p.hk.slice(0,2).map(k=>['bj','rose','snow','bat','pin'].includes(k)?k:''):['',''],c:p.c,n:String(p.n||'Yeep').slice(0,14),it:p.it,tg:p.tg,tgn:p.tgn};
   ws.pose=pose;broadcast(ws.room,{type:'peer',id:ws.id,pose},ws);
  }
 });
 ws.on('close',()=>remove(ws));ws.on('error',()=>remove(ws));
});
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},30000).unref();
function weaponHit(room,attacker,target,kind){
 const hp=playerHealth.get(room);if(!hp)return;
 const dmg=kind==='bat'?250:kind==='bj'?500:kind==='rose'?100:0;
 const before=hp.get(target)??1000,after=before-dmg,ko=after<=0;
 hp.set(target,ko?1000:after);
 const victim=[...(rooms.get(room)||[])].find(w=>w.id===target);
 if(victim)send(victim,{type:'weapon_hit',attacker,kind,damage:dmg,ko});
 broadcast(room,{type:'weapon_impact',attacker,target,kind,ko});
}
setInterval(()=>{
 for(const [room,ps] of projectiles){
  const members=[...(rooms.get(room)||[])];
  for(const [id,o] of ps){
   const dt=.05;o.age+=dt;const previous=[...o.p];o.v[1]-=9.81*dt;
   o.p=o.p.map((n,i)=>n+o.v[i]*dt);
   let victim=null;
   for(const w of members){if(w.id===o.owner||!w.pose)continue;
    const p=w.pose.p;
    const center=[p[0],p[1]-.35,p[2]];const delta=o.p.map((v,i)=>v-previous[i]);
    const len2=delta.reduce((sum,v)=>sum+v*v,0);
    const t=len2?Math.max(0,Math.min(1,delta.reduce((sum,v,i)=>sum+v*(center[i]-previous[i]),0)/len2)):0;
    const distance=Math.hypot(...center.map((v,i)=>v-(previous[i]+delta[i]*t)));
    if(distance<.75){victim=w;break}
   }
   if(victim){weaponHit(room,o.owner,victim.id,o.kind);broadcast(room,{type:'weapon_despawn',id});ps.delete(id)}
   else if(o.age>7||o.p[1]<-6){broadcast(room,{type:'weapon_despawn',id});ps.delete(id)}
  }
 }
},50).unref();
server.listen(port,'0.0.0.0',()=>console.log('Yeep Tag server listening on '+port));
