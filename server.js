const http=require('http');
const {WebSocketServer,WebSocket}=require('ws');
const crypto=require('crypto');
const port=Number(process.env.PORT)||3000;
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'text/plain','Access-Control-Allow-Origin':'*'});res.end('Yeep Tag multiplayer server online');});
const wss=new WebSocketServer({server,maxPayload:4096});
const rooms=new Map();
function send(ws,obj){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))}
function broadcast(room,obj,except){for(const ws of rooms.get(room)||[])if(ws!==except)send(ws,obj)}
function remove(ws){if(!ws.room)return;const room=ws.room;rooms.get(room)?.delete(ws);broadcast(room,{type:'leave',id:ws.id},ws);if(!rooms.get(room)?.size)rooms.delete(room);ws.room=null}
wss.on('connection',ws=>{
 ws.id=crypto.randomUUID();ws.room=null;ws.pose=null;ws.isAlive=true;ws.lastChat=0;
 ws.on('pong',()=>ws.isAlive=true);
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}
  if(m.type==='join'&&!ws.room){const room=String(m.room||'play').toLowerCase();if(!/^[a-z0-9_.-]{1,32}$/.test(room))return send(ws,{type:'error',message:'Invalid room code'});
   const members=rooms.get(room)||new Set();if(members.size>=24)return send(ws,{type:'error',message:'Room full (24 players)'});
   rooms.set(room,members);ws.room=room;send(ws,{type:'welcome',id:ws.id,peers:[...members].map(p=>({id:p.id,pose:p.pose}))});members.add(ws);broadcast(room,{type:'peer',id:ws.id,pose:null},ws);
  }else if(m.type==='chat'&&ws.room){
   const now=Date.now();if(now-ws.lastChat<750)return;ws.lastChat=now;
   const message=String(m.message||'').replace(/[\x00-\x1f\x7f]/g,' ').trim().slice(0,180);if(!message)return;
   const name=String(m.name||ws.pose?.n||'Yeep').replace(/[\x00-\x1f\x7f]/g,' ').trim().slice(0,14)||'Yeep';
   broadcast(ws.room,{type:'chat',id:ws.id,name,message},ws);
  }else if(m.type==='pose'&&ws.room&&m.pose&&typeof m.pose==='object'){
   const p=m.pose;
   const vec=a=>Array.isArray(a)&&a.length===3&&a.every(n=>typeof n==='number'&&Number.isFinite(n)&&Math.abs(n)<500);
   if(!vec(p.p)||!vec(p.l)||!vec(p.r))return;
   const pose={p:p.p,l:p.l,r:p.r,y:p.y,tl:p.tl,ry:p.ry,hq:p.hq,f:p.f,c:p.c,n:String(p.n||'Yeep').slice(0,14),it:p.it,tg:p.tg,tgn:p.tgn};
   ws.pose=pose;broadcast(ws.room,{type:'peer',id:ws.id,pose},ws);
  }
 });
 ws.on('close',()=>remove(ws));ws.on('error',()=>remove(ws));
});
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},30000).unref();
server.listen(port,'0.0.0.0',()=>console.log('Yeep Tag server listening on '+port));
