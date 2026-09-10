'use strict';
const http=require('node:http');
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const PORT=Number(process.env.PORT||8080), HOST=process.env.HOST||'0.0.0.0';
const GAME=path.join(__dirname,'SUPER-LUDO-Online-FINAL.html');
const rooms=new Map();
function roomCode(){let c;do{c=crypto.randomBytes(4).toString('hex').slice(0,5).toUpperCase()}while(rooms.has(c));return c}
function cleanName(n){return String(n??'Player').trim().slice(0,24)||'Player'}
function publicRoom(r){return {code:r.code,maxPlayers:6,started:r.started,players:r.players.map(p=>({id:p.id,name:p.name,color:p.color,connected:p.ws?.readyState===1}))}}
function frame(text){const p=Buffer.from(text),n=p.length;let h;if(n<126)h=Buffer.from([0x81,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}return Buffer.concat([h,p])}
function send(ws,msg){if(ws&&ws._ludoOpen===true)ws.write(frame(JSON.stringify(msg)))}
function broadcast(r,msg,except=null){for(const p of r.players)if(p.ws!==except)send(p.ws,msg)}
function sendRoom(r){r.players.forEach((p,i)=>send(p.ws,{type:'room',room:publicRoom(r),seat:i,host:i===0}))}
function remove(ws){const code=ws.room;if(!code)return;const r=rooms.get(code);if(!r)return;const i=r.players.findIndex(p=>p.ws===ws);if(i<0)return;r.players[i].ws=null;if(!r.started){r.players.splice(i,1);if(!r.players.length){rooms.delete(code);return}sendRoom(r)}else{broadcast(r,{type:'player',seat:i,connected:false,name:r.players[i].name})}}
function handle(ws,m){
 if(!m||typeof m.type!=='string')return send(ws,{type:'error',message:'Invalid request.'});
 if(m.type==='create'){
   if(ws.room)return send(ws,{type:'error',message:'Already in a room.'});
   const r={code:roomCode(),started:false,game:null,players:[]};
   const p={id:crypto.randomUUID(),name:cleanName(m.name),color:0,ws};r.players.push(p);rooms.set(r.code,r);ws.room=r.code;ws.player=p;return sendRoom(r);
 }
 if(m.type==='join'){
   if(ws.room)return send(ws,{type:'error',message:'Already in a room.'});
   const r=rooms.get(String(m.code||'').trim().toUpperCase());
   if(!r)return send(ws,{type:'error',message:'Room not found. Check the 5-character code.'});
   if(r.started)return send(ws,{type:'error',message:'That game has already started.'});
   if(r.players.length>=6)return send(ws,{type:'error',message:'Room is full. Maximum 6 players.'});
   const seat=r.players.length,p={id:crypto.randomUUID(),name:cleanName(m.name),color:seat,ws};r.players.push(p);ws.room=r.code;ws.player=p;return sendRoom(r);
 }
 const r=rooms.get(ws.room);if(!r)return send(ws,{type:'error',message:'You are not in a room.'});
 const seat=r.players.findIndex(p=>p.ws===ws);
 if(m.type==='leave'){ws.end();return}
 if(m.type==='start'){
   if(seat!==0)return send(ws,{type:'error',message:'Only the host can start the game.'});
   if(r.players.length<2)return send(ws,{type:'error',message:'At least 2 players are required.'});
   if(r.started)return send(ws,{type:'error',message:'Game already started.'});
   r.started=true;r.game=m.game;
   r.players.forEach((p,i)=>send(p.ws,{type:'started',room:r.code,seat:i,host:i===0,game:r.game}));return;
 }
 if(m.type==='state'){
   if(!r.started)return;
   // The host is authoritative for state broadcasts in this version.
   r.game=m.game;return broadcast(r,{type:'state',game:r.game},ws);
 }
 if(m.type==='ping')return send(ws,{type:'pong'});
}
const server=http.createServer((req,res)=>{
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/health'){res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'});return res.end(JSON.stringify({ok:true,rooms:rooms.size,maxPlayers:6}))}
 if(u.pathname==='/'||u.pathname==='/super-ludo.html'||u.pathname==='/SUPER-LUDO-Online-FINAL.html'){
   if(!fs.existsSync(GAME)){res.writeHead(500);return res.end('Game file missing.')}
   res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});return fs.createReadStream(GAME).pipe(res);
 }
 res.writeHead(404);res.end('Not found');
});
server.on('upgrade',(req,socket)=>{
 if(req.url!=='/ws'){socket.destroy();return}
 const key=req.headers['sec-websocket-key'];if(!key){socket.destroy();return}
 const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
 socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
 socket._ludoOpen=true;socket.buffer=Buffer.alloc(0);
 socket.on('data',buf=>{
   socket.buffer=Buffer.concat([socket.buffer,buf]);
   while(socket.buffer.length>=2){
     const b1=socket.buffer[0],b2=socket.buffer[1];let len=b2&127,off=2;
     if(len===126){if(socket.buffer.length<4)break;len=socket.buffer.readUInt16BE(2);off=4}
     else if(len===127){if(socket.buffer.length<10)break;len=Number(socket.buffer.readBigUInt64BE(2));off=10}
     const masked=!!(b2&128);if(masked)off+=4;if(socket.buffer.length<off+len)break;
     const mask=masked?socket.buffer.subarray(off-4,off):null,start=off,data=Buffer.from(socket.buffer.subarray(start,start+len));
     socket.buffer=socket.buffer.subarray(start+len);if(mask)for(let i=0;i<data.length;i++)data[i]^=mask[i%4];
     const opcode=b1&15;if(opcode===8){socket._ludoOpen=false;socket.end();return}if(opcode!==1)continue;
     try{handle(socket,JSON.parse(data.toString()))}catch{send(socket,{type:'error',message:'Invalid message.'})}
   }
 });
 socket.on('close',()=>{socket._ludoOpen=false;remove(socket)});socket.on('end',()=>{socket._ludoOpen=false;remove(socket)});socket.on('error',()=>{socket._ludoOpen=false;remove(socket)});
});
server.listen(PORT,HOST,()=>console.log(`SUPER LUDO online server listening on http://${HOST}:${PORT}`));
