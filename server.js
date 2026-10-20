import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const httpServer = createServer(app);

const allowedOrigins = process.env.ALLOWED_ORIGINS 
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) 
  : '*';

app.use(cors({ origin: allowedOrigins }));

const io = new Server(httpServer, {
  cors: { origin: allowedOrigins, methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e6 // 1MB payload limit
});

// --- IN-MEMORY DATA STORES ---
const Store = {
  players: new Map(),       // socketId -> player profile
  chatHistory: [],          // Last 50 messages
  queues: { swipe: [] },    // gameId -> array of waiting players {socket, userId}
  rooms: new Map(),         // roomId -> room state (for both friend codes and matches)
  leaderboard: {            // Simplified leaderboard store
    swipe: { today: [], week: [], all: [] }
  }
};

// --- UTILITIES ---
const sanitizeText = (txt, max) => String(txt || '').replace(/[\u0000-\u001F\u007F-\u009F]/g, "").trim().substring(0, max);
const BAD_WORDS = ['fuck', 'shit', 'bitch', 'dick', 'cunt', 'bastard'];
const filterProfanity = (text) => BAD_WORDS.reduce((t, w) => t.replace(new RegExp(`\\b${w}\\b`, 'gi'), '***'), text);
const generateRoomCode = () => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // No O,0,1,I
  return Array.from({length: 4}, () => chars[Math.floor(Math.random() * chars.length)]).join('');
};

const broadcastPlayers = () => {
  const active = Array.from(Store.players.values()).map(p => ({
    id: p.userId, name: p.name, avatar: p.avatar, avatarType: p.avatarType, gender: p.gender, status: p.status
  }));
  io.emit('players:update', active);
};

// Throttle player broadcasts
let playerBroadcastTimer = null;
const triggerPlayerUpdate = () => {
  if (!playerBroadcastTimer) {
    playerBroadcastTimer = setTimeout(() => {
      broadcastPlayers();
      playerBroadcastTimer = null;
    }, 500); // max 2 per second
  }
};

// --- HEALTH CHECK ---
app.get('/health', (req, res) => {
  res.json({ ok: true, players: Store.players.size });
});

// --- SOCKET CONNECTION ---
io.on('connection', (socket) => {
  
  // Rate limiting map per socket
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

  // 1. PRESENCE
  socket.on('hello', (profile) => {
    try {
      if (typeof profile !== 'object') return;
      let { id, name, avatar, avatarType, gender, isGuest } = profile;
      
      name = sanitizeText(name, 14);
      if (name.length < 2) name = "Player";
      if (!['male', 'female', 'none'].includes(gender)) gender = 'none';
      if (avatarType === 'photo' && avatar && avatar.length > 40000) {
        avatarType = 'emoji'; avatar = '🤖'; // Enforce ~30KB limit
      }

      Store.players.set(socket.id, {
        socketId: socket.id,
        userId: String(id),
        name, avatar: sanitizeText(avatar, 40000), avatarType, gender,
        isGuest: !!isGuest,
        status: 'online', // 'online' | 'in-game'
        lastPing: Date.now()
      });

      socket.emit('chat:history', Store.chatHistory);
      triggerPlayerUpdate();
      socket.emit('leaderboard:update', Store.leaderboard);
    } catch (e) { console.error("Hello error", e); }
  });

  // 2. CHAT
  socket.on('chat:send', (text) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      
      // 1 per sec, 10 per 30 sec
      if (!checkRateLimit('chat_fast', 1000, 1) || !checkRateLimit('chat_burst', 30000, 10)) {
        socket.emit('error:toast', "You are typing too fast.");
        return;
      }

      const cleanText = filterProfanity(sanitizeText(text, 200));
      if (!cleanText) return;

      const msg = {
        id: Math.random().toString(36).substr(2, 9),
        user: { id: player.userId, name: player.name, avatar: player.avatar, avatarType: player.avatarType, gender: player.gender },
        text: cleanText,
        time: Date.now()
      };

      Store.chatHistory.push(msg);
      if (Store.chatHistory.length > 100) Store.chatHistory.shift();

      io.emit('chat:message', msg);
    } catch (e) { console.error("Chat error", e); }
  });

  // 3. FRIEND ROOMS
  socket.on('room:create', () => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      if (!checkRateLimit('room_create', 2000)) return;

      let code;
      do { code = generateRoomCode(); } while (Store.rooms.has(code));

      Store.rooms.set(code, {
        id: code,
        type: 'friend',
        gameId: 'swipe',
        players: [], // will hold { socketId, userId, ready, ...gameStats }
        status: 'waiting', // waiting, playing, ended
        createdAt: Date.now()
      });
      socket.emit('room:created', code);
    } catch (e) { console.error("Create room error", e); }
  });

  socket.on('room:join', (code) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      const cleanCode = sanitizeText(code, 4).toUpperCase();
      const room = Store.rooms.get(cleanCode);

      if (!room) return socket.emit('error:toast', "Room not found.");
      if (room.status !== 'waiting' || room.players.length >= 2) return socket.emit('error:toast', "Room is full or already started.");
      
      // Prevent joining own room twice from same tab
      if (room.players.find(p => p.userId === player.userId)) return;

      socket.emit('match:found', { room: room.id });
    } catch (e) { console.error("Join room error", e); }
  });

  // 4. RANDOM MATCHMAKING
  socket.on('match:find', ({ gameId }) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player || !Store.queues[gameId]) return;
      
      const q = Store.queues[gameId];
      if (q.find(p => p.userId === player.userId)) return; // Already in queue

      // Look for opponent
      const opponentIdx = q.findIndex(p => p.userId !== player.userId);
      
      if (opponentIdx !== -1) {
        // Match found!
        const opponent = q.splice(opponentIdx, 1)[0];
        const code = generateRoomCode();
        Store.rooms.set(code, {
          id: code, type: 'random', gameId,
          players: [], status: 'waiting', createdAt: Date.now()
        });
        
        // Notify both
        socket.emit('match:found', { room: code });
        io.to(opponent.socketId).emit('match:found', { room: code });
      } else {
        q.push({ socketId: socket.id, userId: player.userId });
      }
    } catch (e) { console.error("Match find error", e); }
  });

  socket.on('match:cancel', () => {
    try {
      Object.values(Store.queues).forEach(q => {
        const idx = q.findIndex(p => p.socketId === socket.id);
        if (idx !== -1) q.splice(idx, 1);
      });
    } catch(e) {}
  });

  // 5. INVITES
  socket.on('invite:send', ({ toUserId, gameId }) => {
    try {
      const p1 = Store.players.get(socket.id);
      if (!p1 || p1.userId === toUserId) return;
      if (!checkRateLimit('invite_send', 5000)) return socket.emit('error:toast', "Please wait before sending another invite.");

      const p2 = Array.from(Store.players.values()).find(p => p.userId === toUserId);
      if (!p2) return socket.emit('error:toast', "Player is offline.");
      if (p2.status === 'in-game') return socket.emit('error:toast', "Player is busy in a match.");

      io.to(p2.socketId).emit('invite:receive', { 
        from: { id: p1.userId, name: p1.name }, 
        gameId,
        inviteId: Math.random().toString(36).substr(2, 9)
      });
      socket.emit('toast', "Invite sent!");
    } catch(e) {}
  });

  socket.on('invite:accept', ({ fromUserId, gameId }) => {
    try {
      const p1 = Array.from(Store.players.values()).find(p => p.userId === fromUserId);
      if (!p1) return socket.emit('error:toast', "Player is no longer online.");

      const code = generateRoomCode();
      Store.rooms.set(code, {
        id: code, type: 'friend', gameId, players: [], status: 'waiting', createdAt: Date.now()
      });

      socket.emit('match:found', { room: code });
      io.to(p1.socketId).emit('match:found', { room: code });
    } catch(e) {}
  });

  // 6. GAME ENGINE (SWIPE GAME)
  const DIRS = ['UP', 'DOWN', 'LEFT', 'RIGHT'];

  socket.on('game:ready', ({ roomId }) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      const room = Store.rooms.get(roomId);
      if (!room || room.status !== 'waiting') return;

      socket.join(roomId);
      
      // Update status
      player.status = 'in-game';
      triggerPlayerUpdate();

      // Add to room state if not present (handles reconnects loosely)
      let rPlayer = room.players.find(p => p.userId === player.userId);
      if (!rPlayer) {
        rPlayer = {
          socketId: socket.id, userId: player.userId, ready: true,
          profile: { name: player.name, avatar: player.avatar, avatarType: player.avatarType, gender: player.gender },
          score: 0, combo: 0, bestCombo: 0, hits: 0, misses: 0, reactions: [],
          target: DIRS[Math.floor(Math.random() * 4)], targetIssuedAt: 0, lockUntil: 0,
          rematchVote: false
        };
        room.players.push(rPlayer);
      } else {
        rPlayer.socketId = socket.id;
        rPlayer.ready = true;
      }

      // Check if both ready
      if (room.players.length === 2 && room.players.every(p => p.ready)) {
        room.status = 'playing';
        const startAt = Date.now() + 3000;
        room.endTime = startAt + 30000; // 30 seconds

        const profiles = room.players.map(p => ({ userId: p.userId, profile: p.profile }));
        io.to(room.id).emit('match:start', { startAt, profiles, serverNow: Date.now() });

        // Send first targets
        room.players.forEach(p => {
          p.targetIssuedAt = startAt;
          io.to(p.socketId).emit('game:target', { dir: p.target });
        });

        // Schedule end
        room.endTimer = setTimeout(() => endMatch(room), 33500); // 3s prep + 30s match + 500ms buffer
      }
    } catch (e) { console.error("Game ready error", e); }
  });

  socket.on('game:swipe', ({ dir }) => {
    try {
      const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
      if (!room || room.status !== 'playing') return;
      
      const pState = room.players.find(p => p.socketId === socket.id);
      const now = Date.now();
      
      if (now < pState.lockUntil || now < pState.targetIssuedAt + 60) return; // Anti-cheat & locks
      if (now > room.endTime) return;

      if (dir === pState.target) {
        // Correct!
        const rt = now - pState.targetIssuedAt;
        pState.reactions.push(rt);
        pState.hits++;
        pState.combo++;
        pState.bestCombo = Math.max(pState.bestCombo, pState.combo);
        
        const speedBonus = rt < 400 ? 4 : rt < 700 ? 2 : 0;
        const comboBonus = Math.min(Math.max(0, pState.combo - 1), 10) * 2;
        pState.score += 10 + comboBonus + speedBonus;

        pState.target = DIRS[Math.floor(Math.random() * 4)];
        pState.targetIssuedAt = now;
        
        io.to(pState.socketId).emit('game:target', { dir: pState.target });
      } else {
        // Wrong!
        pState.misses++;
        pState.combo = 0;
        pState.score = Math.max(0, pState.score - 5);
        pState.lockUntil = now + 250;
      }

      // Broadcast state
      const stateUpdate = room.players.map(p => ({ userId: p.userId, score: p.score, combo: p.combo }));
      io.to(room.id).emit('game:state', stateUpdate);

    } catch (e) { console.error("Swipe error", e); }
  });

  socket.on('match:rematch', () => {
    try {
      const room = Array.from(Store.rooms.values()).find(r => r.players.some(p => p.socketId === socket.id));
      if (!room || room.status !== 'ended') return;

      const pState = room.players.find(p => p.socketId === socket.id);
      pState.rematchVote = true;

      // Both voted?
      if (room.players.every(p => p.rematchVote)) {
        room.status = 'waiting';
        room.players.forEach(p => {
          p.ready = false; p.score = 0; p.combo = 0; p.bestCombo = 0; p.hits = 0; p.misses = 0; p.reactions = []; p.lockUntil = 0; p.rematchVote = false; p.target = DIRS[Math.floor(Math.random() * 4)];
        });
        io.to(room.id).emit('match:rematch_start');
      }
    } catch(e) {}
  });

  function endMatch(room, forfeitUserId = null) {
    if (room.status === 'ended') return;
    room.status = 'ended';
    clearTimeout(room.endTimer);

    const results = room.players.map(p => ({
      userId: p.userId, score: p.score, bestCombo: p.bestCombo, 
      hits: p.hits, misses: p.misses, 
      avgReaction: p.reactions.length ? Math.round(p.reactions.reduce((a,b)=>a+b,0)/p.reactions.length) : 0
    }));

    // Determine winner
    let winnerId = null;
    if (results.length === 2 && results[0].score !== results[1].score) {
      winnerId = results[0].score > results[1].score ? results[0].userId : results[1].userId;
    }
    if (forfeitUserId) { const other = results.find(r => r.userId !== forfeitUserId); winnerId = other ? other.userId : null; }

    io.to(room.id).emit('match:end', { results, winnerId });

    // Update Leaderboard
    let updatedLb = false;
    room.players.forEach(p => {
      const realPlayer = Array.from(Store.players.values()).find(rp => rp.userId === p.userId);
      if (realPlayer && !realPlayer.isGuest && p.score > 0) {
        Store.leaderboard.swipe.today.push({
          name: realPlayer.name, avatar: realPlayer.avatar,avatarType: realPlayer.avatarType, gender: realPlayer.gender, score: p.score, time: Date.now()
        });
        updatedLb = true;
      }
    });

    if (updatedLb) {
      // Sort and truncate
      Store.leaderboard.swipe.today.sort((a,b) => b.score - a.score);
      Store.leaderboard.swipe.today = Store.leaderboard.swipe.today.slice(0, 10);
      io.emit('leaderboard:update', Store.leaderboard);
    }
  }

  // 7. LEADERBOARD REQUEST
  socket.on('leaderboard:get', () => {
    socket.emit('leaderboard:update', Store.leaderboard);
  });

  // 8. DISCONNECT
  socket.on('disconnect', () => {
    try {
      const player = Store.players.get(socket.id);
      
      // Remove from queues
      Object.values(Store.queues).forEach(q => {
        const idx = q.findIndex(p => p.socketId === socket.id);
        if (idx !== -1) q.splice(idx, 1);
      });

      // Handle active rooms
      Store.rooms.forEach((room, roomId) => {
        const pIdx = room.players.findIndex(p => p.socketId === socket.id);
        if (pIdx !== -1) {
          if (room.status === 'playing' || room.status === 'waiting') {
            // Start 10s grace period for forfeit
            room.disconnectTimer = setTimeout(() => {
              const stillMissing = room.players.find(p => p.socketId === socket.id);
              if (stillMissing && room.status !== 'ended') {
                // Forfeit
                io.to(roomId).emit('error:toast', "Opponent disconnected. Match forfeited.");
                endMatch(room, stillMissing.userId);
              }
            }, 10000);
          } else if (room.status === 'ended') {
            io.to(roomId).emit('error:toast', "Opponent left the room.");
          }
        }
      });

      Store.players.delete(socket.id);
      triggerPlayerUpdate();
    } catch(e) { console.error("Disconnect error", e); }
  });

});

// --- CRON / CLEANUP ---
setInterval(() => {
  const now = Date.now();
  // Cleanup empty/old rooms
  for (const [id, room] of Store.rooms.entries()) {
    if (room.status === 'waiting' && (now - room.createdAt > 10 * 60 * 1000)) {
      Store.rooms.delete(id);
    }
  }
}, 60000);

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`Swiplay Server running on port ${PORT}`);
});
