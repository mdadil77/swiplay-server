import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';
import { Chess } from 'chess.js';

dotenv.config();
const app = express();
const httpServer = createServer(app);
const allowedOrigins = process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) : '*';

app.use(cors({ origin: allowedOrigins }));
app.use(express.static('public'));

const io = new Server(httpServer, { cors: { origin: allowedOrigins, methods: ["GET", "POST"] }, maxHttpBufferSize: 1e6 });

const Store = {
  players: new Map(), chatHistory: [], queues: { swipe: [], chess: [], ludo: [], snake: [], word: [] },
  rooms: new Map(), leaderboard: { swipe: { today: [], week: [], all: [] }, chess: { today: [], week: [], all: [] } }
};

const sanitizeText = (txt, max) => String(txt || '').replace(/[\u0000-\u001F\u007F-\u009F]/g, "").trim().substring(0, max);
const generateRoomCode = () => { const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; return Array.from({length:4}, () => c[Math.floor(Math.random()*c.length)]).join(''); };

const getRoomForSocket = (socketId) => { for (const r of Store.rooms.values()) { if (r.players.some(p => p.socketId === socketId)) return r; } return null; };
const clearRoomTimers = (room) => { if(room.endTimer) clearTimeout(room.endTimer); if(room.turnTimer) clearTimeout(room.turnTimer); if(room.afkTimer) clearTimeout(room.afkTimer); };

// LUDO & SNAKE CONSTANTS
const LUDO_TOTAL = 25;
const SNAKE_TOTAL = 64;
const SNAKES = { 16: 6, 47: 26, 49: 11, 56: 53, 62: 19, 64: 60 };
const LADDERS = { 1: 38, 4: 14, 9: 31, 21: 42, 28: 84, 36: 44, 51: 67, 71: 91, 80: 100 };
const WORDS = ['apple','banana','cherry','dragon','elephant','falcon','guitar','hunter','island','jacket','knight','lemon','monkey','ninja','ocean','pirate','queen','rabbit','snake','tiger','umbrella','vampire','wizard','xylophone','yacht','zebra','rocket','planet','cyber','magic'];

io.on('connection', (socket) => {
  socket.on('hello', (profile) => {
    try {
      let { id, name, avatar, avatarType, gender } = profile;
      name = sanitizeText(name, 14); if (name.length < 2) name = "Player";
      Store.players.set(socket.id, { socketId: socket.id, userId: String(id), name, avatar, avatarType, gender, status: 'online' });
      socket.emit('chat:history', Store.chatHistory);
    } catch (e) {}
  });

  socket.on('chat:send', (text) => {
    const p = Store.players.get(socket.id); if(!p) return;
    const msg = { user: { id: p.userId, name: p.name, avatar: p.avatar }, text: sanitizeText(text, 200), time: Date.now() };
    Store.chatHistory.push(msg); if(Store.chatHistory.length>50) Store.chatHistory.shift();
    io.emit('chat:message', msg);
  });

  socket.on('match:find', ({ gameId }) => {
    const p = Store.players.get(socket.id); if(!p || !Store.queues[gameId]) return;
    const q = Store.queues[gameId];
    const oppIdx = q.findIndex(x => x.userId !== p.userId);
    if (oppIdx !== -1) {
      const opp = q.splice(oppIdx, 1)[0];
      const code = generateRoomCode();
      Store.rooms.set(code, { id: code, gameId, players: [], status: 'waiting', createdAt: Date.now() });
      socket.emit('match:found', { room: code, gameId });
      io.to(opp.socketId).emit('match:found', { room: code, gameId });
    } else { q.push({ socketId: socket.id, userId: p.userId }); }
  });

  socket.on('match:cancel', () => { Object.values(Store.queues).forEach(q => { const i = q.findIndex(x => x.socketId === socket.id); if(i!==-1) q.splice(i,1); }); });

  socket.on('room:create', (data) => {
    const p = Store.players.get(socket.id); if(!p) return;
    const code = generateRoomCode();
    Store.rooms.set(code, { id: code, type: 'friend', gameId: data.gameId || 'swipe', players: [], status: 'waiting', createdAt: Date.now() });
    socket.emit('room:created', code);
  });

  socket.on('room:join', (code) => {
    const room = Store.rooms.get(code.toUpperCase());
    if(!room) return socket.emit('error:toast', "Room not found");
    socket.emit('match:found', { room: room.id, gameId: room.gameId });
  });

  socket.on('invite:send', ({ toUserId, gameId }) => {
    const p1 = Store.players.get(socket.id); const p2 = Array.from(Store.players.values()).find(x => x.userId === toUserId);
    if(p2) io.to(p2.socketId).emit('invite:receive', { from: { id: p1.userId, name: p1.name }, gameId });
  });
  socket.on('invite:accept', ({ fromUserId, gameId }) => {
    const p1 = Array.from(Store.players.values()).find(x => x.userId === fromUserId); if(!p1) return;
    const code = generateRoomCode();
    Store.rooms.set(code, { id: code, type: 'friend', gameId, players: [], status: 'waiting', createdAt: Date.now() });
    socket.emit('match:found', { room: code, gameId }); io.to(p1.socketId).emit('match:found', { room: code, gameId });
  });

  socket.on('game:ready', ({ roomId }) => {
    const p = Store.players.get(socket.id); const room = Store.rooms.get(roomId);
    if(!p || !room || room.status !== 'waiting') return;
    socket.join(roomId);
    let rp = room.players.find(x => x.userId === p.userId);
    if(!rp) { if(room.players.length >= 2) return; rp = { socketId: socket.id, userId: p.userId, ready: true, profile: { name: p.name, avatar: p.avatar } }; room.players.push(rp); }
    else { rp.socketId = socket.id; rp.ready = true; }

    if(room.players.length === 2 && room.players.every(x => x.ready)) {
      room.status = 'playing';
      const startAt = Date.now() + 3000;
      const profiles = room.players.map(x => ({ userId: x.userId, profile: x.profile }));
      
      if (room.gameId === 'chess') {
        if(!room.chess) { room.chess = new Chess(); room.players[0].color = 'w'; room.players[1].color = 'b'; }
        profiles.forEach(pr => pr.color = room.players.find(x=>x.userId===pr.userId).color);
        room.clocks = room.clocks || { w: 300000, b: 300000 };
        room.lastMoveTime = startAt;
        io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now(), gameId: 'chess', fen: room.chess.fen(), clocks: room.clocks });
        room.endTimer = setInterval(() => {
          if(room.status !== 'playing') return clearInterval(room.endTimer);
          const elapsed = Date.now() - room.lastMoveTime; const turn = room.chess.turn();
          if(room.clocks[turn] - elapsed <= 0) { room.clocks[turn]=0; endChess(room, turn==='w'?'b':'w', 'timeout'); }
        }, 1000);
      } 
      else if (room.gameId === 'ludo' || room.gameId === 'snake') {
        room.turn = room.players[0].userId;
        room.afkMisses = { [room.players[0].userId]: 0, [room.players[1].userId]: 0 };
        if(room.gameId === 'ludo') room.positions = { [room.players[0].userId]: 0, [room.players[1].userId]: 0 };
        else if(room.gameId === 'snake') room.positions = { [room.players[0].userId]: 0, [room.players[1].userId]: 0 };
        
        io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now(), gameId: room.gameId });
        startTurnTimer(room);
      }
      else if (room.gameId === 'word') {
        room.scores = { [room.players[0].userId]: 0, [room.players[1].userId]: 0 };
        room.currentWord = WORDS[Math.floor(Math.random()*WORDS.length)];
        io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now(), gameId: 'word' });
        setTimeout(() => { io.to(room.id).emit('word:next', room.currentWord); }, 3000);
        room.endTimer = setTimeout(() => endWord(room), 63000);
      }
    }
  });

  function startTurnTimer(room) {
    clearTimeout(room.turnTimer);
    room.turnTimer = setTimeout(() => {
      if (room.status !== 'playing') return;
      // AFK TIMEOUT
      room.afkMisses[room.turn]++;
      if (room.afkMisses[room.turn] >= 3) {
        const winner = room.players.find(p => p.userId !== room.turn).userId;
        const stateData = { myPos: 0, oppPos: 0, turn: null, winner };
        io.to(room.id).emit(`${room.gameId}:state`, stateData); // Force end
        room.status = 'ended';
        return;
      }
      // Auto Roll for them
      handleRoll(room, room.turn);
    }, 15000); // 15 seconds per turn
  }

  socket.on('ludo:roll', () => { const r = getRoomForSocket(socket.id); if(r && r.gameId==='ludo') handleRoll(r, Store.players.get(socket.id).userId); });
  socket.on('snake:roll', () => { const r = getRoomForSocket(socket.id); if(r && r.gameId==='snake') handleRoll(r, Store.players.get(socket.id).userId); });

  function handleRoll(room, userId) {
    if (room.status !== 'playing' || room.turn !== userId) return;
    clearTimeout(room.turnTimer);

    const roll = Math.floor(Math.random() * 6) + 1;
    room.positions[userId] += roll;
    
    let total = room.gameId === 'ludo' ? LUDO_TOTAL : SNAKE_TOTAL;
    if(room.gameId === 'snake') {
      if (SNAKES[room.positions[userId]]) room.positions[userId] = SNAKES[room.positions[userId]];
      if (LADDERS[room.positions[userId]]) room.positions[userId] = LADDERS[room.positions[userId]];
    }

    if (room.positions[userId] >= total) {
      room.positions[userId] = total;
      const p1 = room.players[0].userId; const p2 = room.players[1].userId;
      const stateData = { 
        myPos: room.positions[p1], oppPos: room.positions[p2], 
        turn: null, winner: userId 
      };
      io.to(room.id).emit(`${room.gameId}:state`, stateData);
      room.status = 'ended';
      return;
    }

    room.turn = room.players.find(p => p.userId !== userId).userId;
    const p1 = room.players[0].userId; const p2 = room.players[1].userId;
    io.to(room.id).emit(`${room.gameId}:state`, { 
      myPos: room.positions[p1], oppPos: room.positions[p2], turn: room.turn 
    });
    startTurnTimer(room);
  }

  socket.on('word:typed', () => {
    const room = getRoomForSocket(socket.id); if(!room || room.gameId !== 'word' || room.status !== 'playing') return;
    const userId = Store.players.get(socket.id).userId;
    if (room.scores[userId] !== undefined) {
      room.scores[userId]++;
      room.currentWord = WORDS[Math.floor(Math.random()*WORDS.length)];
      const p1 = room.players[0].userId; const p2 = room.players[1].userId;
      io.to(room.id).emit('word:state', { myScore: room.scores[p1], oppScore: room.scores[p2] });
      io.to(room.id).emit('word:next', room.currentWord);
    }
  });

  socket.on('chess:move', (move) => {
    const room = getRoomForSocket(socket.id); if(!room || room.status !== 'playing' || room.gameId !== 'chess') return;
    const p = room.players.find(x => x.socketId === socket.id); if(!p || p.color !== room.chess.turn()) return;
    try {
      const res = room.chess.move(move);
      if(res) {
        const now = Date.now(); room.clocks[p.color] -= (now - room.lastMoveTime); if(room.clocks[p.color] < 0) room.clocks[p.color] = 0;
        room.lastMoveTime = now;
        io.to(room.id).emit('chess:state', { fen: room.chess.fen(), clocks: room.clocks, lastMove: {from: res.from, to: res.to} });
        if(room.chess.in_checkmate()) endChess(room, p.color, 'checkmate');
        else if(room.chess.in_draw()) endChess(room, null, 'draw');
      }
    } catch(e) {}
  });

  function endChess(room, winColor, reason) {
    room.status = 'ended'; clearInterval(room.endTimer);
    const winner = room.players.find(p => p.color === winColor);
    io.to(room.id).emit('match:end', { winnerId: winner ? winner.userId : null, reason });
  }

  function endWord(room) {
    room.status = 'ended';
    const p1 = room.players[0].userId; const p2 = room.players[1].userId;
    let winner = null;
    if (room.scores[p1] > room.scores[p2]) winner = p1;
    else if (room.scores[p2] > room.scores[p1]) winner = p2;
    io.to(room.id).emit('word:state', { myScore: room.scores[p1], oppScore: room.scores[p2], winner });
  }

  socket.on('game:chat', (text) => {
    const room = getRoomForSocket(socket.id); const p = Store.players.get(socket.id);
    if(room && p) io.to(room.id).emit('game:chat_receive', { senderId: p.userId, name: p.name, text: sanitizeText(text, 150) });
  });

  socket.on('disconnect', () => {
    Object.values(Store.queues).forEach(q => { const i = q.findIndex(x=>x.socketId===socket.id); if(i!==-1) q.splice(i,1); });
    const room = getRoomForSocket(socket.id); const p = Store.players.get(socket.id);
    if(room && p && room.status === 'playing') {
      const opp = room.players.find(x => x.userId !== p.userId);
      if(opp) {
        io.to(room.id).emit('error:toast', "Opponent disconnected. You win.");
        if (room.gameId === 'chess') endChess(room, opp.color, 'disconnect');
        else if(room.gameId === 'ludo' || room.gameId === 'snake') io.to(room.id).emit(`${room.gameId}:state`, { myPos: 0, oppPos: 0, winner: opp.userId });
        else if(room.gameId === 'word') io.to(room.id).emit('word:state', { myScore: 0, oppScore: 0, winner: opp.userId });
      }
    }
    Store.players.delete(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Server running on ${PORT}`));
