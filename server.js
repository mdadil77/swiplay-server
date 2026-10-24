import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';
import { Chess } from 'chess.js';

dotenv.config();

const app = express();
const httpServer = createServer(app);

const allowedOrigins = process.env.ALLOWED_ORIGINS 
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) 
  : '*';

app.use(cors({ origin: allowedOrigins }));

const io = new Server(httpServer, {
  cors: { origin: allowedOrigins, methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e6
});

const Store = {
  players: new Map(),
  chatHistory: [],
  queues: { swipe: [], chess: [] }, 
  rooms: new Map(),
  leaderboard: { swipe: { today: [], week: [], all: [] }, chess: { today: [], week: [], all: [] } }
};

const sanitizeText = (txt, max) => String(txt || '').replace(/[\u0000-\u001F\u007F-\u009F]/g, "").trim().substring(0, max);
const BAD_WORDS = ['fuck', 'shit', 'bitch', 'dick', 'cunt', 'bastard'];
const filterProfanity = (text) => BAD_WORDS.reduce((t, w) => t.replace(new RegExp(`\\b${w}\\b`, 'gi'), '***'), text);
const generateRoomCode = () => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({length: 4}, () => chars[Math.floor(Math.random() * chars.length)]).join('');
};

const broadcastPlayers = () => {
  const active = Array.from(Store.players.values()).map(p => ({
    id: p.userId, name: p.name, avatar: p.avatar, avatarType: p.avatarType, gender: p.gender, status: p.status
  }));
  io.emit('players:update', active);
};

let playerBroadcastTimer = null;
const triggerPlayerUpdate = () => {
  if (!playerBroadcastTimer) {
    playerBroadcastTimer = setTimeout(() => { broadcastPlayers(); playerBroadcastTimer = null; }, 500);
  }
};

app.get('/health', (req, res) => res.json({ ok: true, players: Store.players.size }));

io.on('connection', (socket) => {
  try {
    const rateLimits = new Map();
    const checkRateLimit = (action, limitMs, maxHits = 1) => {
      const now = Date.now();
      const history = rateLimits.get(action) || [];
      const validHistory = history.filter(t => now - t < limitMs);
      if (validHistory.length >= maxHits) return false;
      validHistory.push(now);
      rateLimits.set(action, validHistory);
      return true;
    };

    socket.on('hello', (profile) => {
      try {
        if (typeof profile !== 'object') return;
        let { id, name, avatar, avatarType, gender, isGuest } = profile;
        name = sanitizeText(name, 14); if (name.length < 2) name = "Player";
        if (!['male', 'female', 'none'].includes(gender)) gender = 'none';
        if (avatarType === 'photo' && avatar && avatar.length > 40000) { avatarType = 'emoji'; avatar = '🤖'; }

        Store.players.set(socket.id, {
          socketId: socket.id, userId: String(id), name, avatar: sanitizeText(avatar, 40000), avatarType, gender, isGuest: !!isGuest, status: 'online', lastPing: Date.now()
        });
        socket.emit('chat:history', Store.chatHistory);
        triggerPlayerUpdate();
        socket.emit('leaderboard:update', Store.leaderboard);
      } catch (e) {}
    });

    socket.on('chat:send', (text) => {
      try {
        const player = Store.players.get(socket.id); if (!player) return;
        if (!checkRateLimit('chat_fast', 1000, 1) || !checkRateLimit('chat_burst', 30000, 10)) return socket.emit('error:toast', "Typing too fast.");
        const cleanText = filterProfanity(sanitizeText(text, 200)); if (!cleanText) return;

        const msg = { id: Math.random().toString(36).substr(2, 9), user: { id: player.userId, name: player.name, avatar: player.avatar, avatarType: player.avatarType, gender: player.gender }, text: cleanText, time: Date.now() };
        Store.chatHistory.push(msg); if (Store.chatHistory.length > 50) Store.chatHistory.shift();
        io.emit('chat:message', msg);
      } catch (e) {}
    });

    socket.on('game:chat', (text) => {
      try {
        const player = Store.players.get(socket.id); if (!player) return;
        if (!checkRateLimit('in_game_chat', 1500, 1)) return; 
        const cleanText = filterProfanity(sanitizeText(text, 150)); if (!cleanText) return;
        
        const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
        if (room && (room.status === 'playing' || room.status === 'ended')) {
          io.to(room.id).emit('game:chat_receive', { senderId: player.userId, name: player.name, text: cleanText });
        }
      } catch(e) {}
    });

    socket.on('room:create', (data) => {
      try {
        const player = Store.players.get(socket.id); if (!player) return;
        if (!checkRateLimit('room_create', 2000)) return;
        let code; do { code = generateRoomCode(); } while (Store.rooms.has(code));
        
        const gameId = data && data.gameId ? data.gameId : 'swipe';
        Store.rooms.set(code, { id: code, type: 'friend', gameId, players: [], status: 'waiting', createdAt: Date.now() });
        socket.emit('room:created', code);
      } catch (e) {}
    });

    socket.on('room:join', (code) => {
      try {
        const player = Store.players.get(socket.id); if (!player) return;
        const cleanCode = sanitizeText(code, 4).toUpperCase();
        const room = Store.rooms.get(cleanCode);
        if (!room) return socket.emit('error:toast', "Room not found.");
        if (room.status !== 'waiting' || room.players.length >= 2) return socket.emit('error:toast', "Room is full or started.");
        if (room.players.find(p => p.userId === player.userId)) return;
        socket.emit('match:found', { room: room.id, gameId: room.gameId });
      } catch (e) {}
    });

    socket.on('match:find', ({ gameId }) => {
      try {
        const player = Store.players.get(socket.id); if (!player || !Store.queues[gameId]) return;
        const q = Store.queues[gameId];
        if (q.find(p => p.userId === player.userId)) return;

        const opponentIdx = q.findIndex(p => p.userId !== player.userId);
        if (opponentIdx !== -1) {
          const opponent = q.splice(opponentIdx, 1)[0];
          const code = generateRoomCode();
          Store.rooms.set(code, { id: code, type: 'random', gameId, players: [], status: 'waiting', createdAt: Date.now() });
          socket.emit('match:found', { room: code, gameId });
          io.to(opponent.socketId).emit('match:found', { room: code, gameId });
        } else {
          q.push({ socketId: socket.id, userId: player.userId });
        }
      } catch (e) {}
    });

    socket.on('match:cancel', () => {
      try { Object.values(Store.queues).forEach(q => { const idx = q.findIndex(p => p.socketId === socket.id); if (idx !== -1) q.splice(idx, 1); }); } catch(e) {}
    });

    socket.on('invite:send', ({ toUserId, gameId }) => {
      try {
        const p1 = Store.players.get(socket.id); if (!p1 || p1.userId === toUserId) return;
        if (!checkRateLimit('invite_send', 5000)) return socket.emit('error:toast', "Please wait before inviting.");
        const p2 = Array.from(Store.players.values()).find(p => p.userId === toUserId);
        if (!p2) return socket.emit('error:toast', "Player is offline.");
        if (p2.status === 'in-game') return socket.emit('error:toast', "Player is busy.");
        io.to(p2.socketId).emit('invite:receive', { from: { id: p1.userId, name: p1.name }, gameId, inviteId: Math.random().toString(36).substr(2, 9) });
        socket.emit('toast', "Invite sent!");
      } catch(e) {}
    });

    socket.on('invite:accept', ({ fromUserId, gameId }) => {
      try {
        const p1 = Array.from(Store.players.values()).find(p => p.userId === fromUserId);
        if (!p1) return socket.emit('error:toast', "Player is no longer online.");
        const code = generateRoomCode();
        Store.rooms.set(code, { id: code, type: 'friend', gameId, players: [], status: 'waiting', createdAt: Date.now() });
        socket.emit('match:found', { room: code, gameId });
        io.to(p1.socketId).emit('match:found', { room: code, gameId });
      } catch(e) {}
    });

    const DIRS = ['UP', 'DOWN', 'LEFT', 'RIGHT'];

    socket.on('game:ready', ({ roomId }) => {
      try {
        const player = Store.players.get(socket.id); if (!player) return;
        const room = Store.rooms.get(roomId); if (!room || room.status !== 'waiting') return;

        socket.join(roomId);
        player.status = 'in-game'; triggerPlayerUpdate();

        let rPlayer = room.players.find(p => p.userId === player.userId);
        if (!rPlayer) {
          if (room.players.length >= 2) return; // Block 3rd player stealing seat
          rPlayer = { socketId: socket.id, userId: player.userId, ready: true, profile: { name: player.name, avatar: player.avatar, avatarType: player.avatarType, gender: player.gender }, rematchVote: false };
          room.players.push(rPlayer);
        } else {
          rPlayer.socketId = socket.id; rPlayer.ready = true;
        }

        if (room.players.length === 2 && room.players.every(p => p.ready)) {
          room.status = 'playing';
          const startAt = Date.now() + 3000;
          const profiles = room.players.map(p => ({ userId: p.userId, profile: p.profile }));

          if (room.gameId === 'chess') {
            if (!room.chess) {
              room.chess = new Chess();
              room.players[0].color = room.players[0].color || 'w';
              room.players[1].color = room.players[1].color || 'b';
            }
            profiles.forEach(prof => {
              const rp = room.players.find(p => p.userId === prof.userId);
              if (rp) prof.color = rp.color;
            });
            
            room.clocks = room.clocks || { w: 300000, b: 300000 }; 
            room.lastMoveTime = room.lastMoveTime || startAt;

            io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now(), gameId: 'chess', fen: room.chess.fen(), clocks: room.clocks });
            
            room.endTimer = setInterval(() => {
              if (room.status !== 'playing') return clearInterval(room.endTimer);
              const elapsed = Date.now() - room.lastMoveTime;
              const turnColor = room.chess.turn();
              if (room.clocks[turnColor] - elapsed <= 0) {
                room.clocks[turnColor] = 0;
                endChessMatch(room, turnColor === 'w' ? 'b' : 'w', 'timeout');
              }
            }, 1000);
          } 
          else {
            room.endTime = startAt + 30000;
            room.players.forEach(p => { p.score = 0; p.combo = 0; p.bestCombo = 0; p.hits = 0; p.misses = 0; p.reactions = []; p.target = DIRS[Math.floor(Math.random()*4)]; p.targetIssuedAt = startAt; p.lockUntil = 0; });
            io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now(), gameId: 'swipe' });
            room.players.forEach(p => io.to(p.socketId).emit('game:target', { dir: p.target }));
            room.endTimer = setTimeout(() => endSwipeMatch(room), 33500);
          }
        }
      } catch (e) {}
    });

    socket.on('game:swipe', ({ dir }) => {
      try {
        const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
        if (!room || room.status !== 'playing' || room.gameId !== 'swipe') return;
        const pState = room.players.find(p => p.socketId === socket.id);
        const now = Date.now();
        
        if (now < pState.lockUntil || now < pState.targetIssuedAt + 60 || now > room.endTime) return;

        if (dir === pState.target) {
          const rt = now - pState.targetIssuedAt; pState.reactions.push(rt); pState.hits++; pState.combo++; pState.bestCombo = Math.max(pState.bestCombo, pState.combo);
          pState.score += 10 + (Math.min(Math.max(0, pState.combo - 1), 10) * 2) + (rt < 400 ? 4 : rt < 700 ? 2 : 0);
          pState.target = DIRS[Math.floor(Math.random() * 4)]; pState.targetIssuedAt = now;
          io.to(pState.socketId).emit('game:target', { dir: pState.target });
        } else {
          pState.misses++; pState.combo = 0; pState.score = Math.max(0, pState.score - 5); pState.lockUntil = now + 250;
        }
        io.to(room.id).emit('game:state', room.players.map(p => ({ userId: p.userId, score: p.score, combo: p.combo })));
      } catch (e) {}
    });

    socket.on('chess:move', (move) => {
      try {
        if (typeof move !== 'object' || move === null) return;
        const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
        if (!room || room.status !== 'playing' || room.gameId !== 'chess') return;
        const player = room.players.find(p => p.socketId === socket.id);
        if (!player || player.color !== room.chess.turn()) return;

        // Apply move temporarily to check validation
        const moveRes = room.chess.move(move);
        if (moveRes) {
          // It's legal! Mutate clock
          const now = Date.now();
          const elapsed = now - room.lastMoveTime;
          room.clocks[player.color] -= elapsed;
          if (room.clocks[player.color] < 0) room.clocks[player.color] = 0;
          room.lastMoveTime = now;
          
          io.to(room.id).emit('chess:state', {
            fen: room.chess.fen(),
            clocks: room.clocks,
            lastMove: { from: moveRes.from, to: moveRes.to },
            turn: room.chess.turn(),
            lastMoveTime: now 
          });

          if (room.chess.in_checkmate()) {
            endChessMatch(room, player.color, 'checkmate');
          } else if (room.chess.in_draw() || room.chess.in_stalemate() || room.chess.in_threefold_repetition()) {
            endChessMatch(room, null, 'draw');
          }
        }
      } catch (e) {}
    });

    socket.on('chess:resign', () => {
      const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
      if (!room || room.status !== 'playing' || room.gameId !== 'chess') return;
      const player = room.players.find(p => p.socketId === socket.id);
      endChessMatch(room, player.color === 'w' ? 'b' : 'w', 'resign');
    });

    function endSwipeMatch(room) {
      if (room.status === 'ended') return;
      room.status = 'ended'; clearTimeout(room.endTimer);
      const results = room.players.map(p => ({ userId: p.userId, score: p.score, bestCombo: p.bestCombo, hits: p.hits, misses: p.misses, avgReaction: p.reactions.length ? Math.round(p.reactions.reduce((a,b)=>a+b,0)/p.reactions.length) : 0 }));
      let winnerId = null; if (results.length === 2 && results[0].score !== results[1].score) winnerId = results[0].score > results[1].score ? results[0].userId : results[1].userId;
      io.to(room.id).emit('match:end', { results, winnerId });
      updateLeaderboard('swipe', room);
    }

    function endChessMatch(room, winnerColor, reason) {
      if (room.status === 'ended') return;
      room.status = 'ended'; clearInterval(room.endTimer);
      
      let winnerId = null;
      if (winnerColor) {
        const winner = room.players.find(p => p.color === winnerColor);
        if (winner) winnerId = winner.userId;
      }
      
      room.players.forEach(p => {
        p.score = p.userId === winnerId ? 100 : (winnerId === null ? 50 : 0);
        p.reason = reason;
      });

      const results = room.players.map(p => ({
        userId: p.userId, score: p.score, reason: p.reason
      }));

      io.to(room.id).emit('match:end', { results, winnerId, reason });
      updateLeaderboard('chess', room);
    }

    socket.on('match:rematch', () => {
      try {
        const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
        if (!room || room.status !== 'ended') return;
        const pState = room.players.find(p => p.socketId === socket.id);
        pState.rematchVote = true;
        if (room.players.every(p => p.rematchVote)) {
          room.status = 'waiting';
          room.players.forEach(p => { p.ready = false; p.rematchVote = false; });
          io.to(room.id).emit('match:rematch_start');
        }
      } catch(e) {}
    });

    function updateLeaderboard(gameId, room) {
      let updatedLb = false;
      room.players.forEach(p => {
        const realPlayer = Array.from(Store.players.values()).find(rp => rp.userId === p.userId);
        if (realPlayer && !realPlayer.isGuest && (p.score > 0 || gameId === 'chess')) {
          let sc = p.score;
          if (gameId === 'chess') {
              const wId = room.players.find(x => x.score === 100)?.userId;
              sc = (p.userId === wId) ? 10 : (wId === undefined ? 5 : 0);
              if (sc === 0) return; 
          }
          Store.leaderboard[gameId].today.push({ name: realPlayer.name, avatar: realPlayer.avatar, gender: realPlayer.gender, score: sc, time: Date.now() });
          updatedLb = true;
        }
      });
      if (updatedLb) {
        Store.leaderboard[gameId].today.sort((a,b) => b.score - a.score);
        Store.leaderboard[gameId].today = Store.leaderboard[gameId].today.slice(0, 10);
        io.emit('leaderboard:update', Store.leaderboard);
      }
    }

    socket.on('disconnect', () => {
      try {
        Object.values(Store.queues).forEach(q => { const idx = q.findIndex(p => p.socketId === socket.id); if (idx !== -1) q.splice(idx, 1); });
        Store.rooms.forEach((room, roomId) => {
          const pIdx = room.players.findIndex(p => p.socketId === socket.id);
          if (pIdx !== -1) {
            if (room.status === 'playing' || room.status === 'waiting') {
              room.disconnectTimer = setTimeout(() => {
                const stillMissing = room.players.find(p => p.socketId === socket.id);
                if (stillMissing && room.status !== 'ended') {
                  io.to(roomId).emit('error:toast', "Opponent disconnected. Match forfeited.");
                  if (room.gameId === 'chess') {
                    const opp = room.players.find(p => p.socketId !== socket.id);
                    endChessMatch(room, opp ? opp.color : null, 'disconnect');
                  }
                  else endSwipeMatch(room);
                }
              }, 10000);
            } else if (room.status === 'ended') {
              io.to(roomId).emit('error:toast', "Opponent left the room.");
            }
          }
        });
        Store.players.delete(socket.id); triggerPlayerUpdate();
      } catch(e) {}
    });
  } catch (err) {
    console.error("Critical Connection Guard Caught:", err);
  }
});

setInterval(() => {
  const now = Date.now();
  for (const [id, room] of Store.rooms.entries()) {
    if (room.status === 'waiting' && (now - room.createdAt > 10 * 60 * 1000)) Store.rooms.delete(id);
  }
}, 60000);

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Swiplay Server running on port ${PORT}`));
