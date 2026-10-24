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

// Serve the client (public/index.html) from the same origin — no CORS headaches
app.use(express.static('public'));

const io = new Server(httpServer, {
  cors: { origin: allowedOrigins, methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e6,
  pingTimeout: 20000,
  pingInterval: 25000,
});

// ------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------
const CHESS_START_MS = 5 * 60 * 1000;
const SWIPE_DURATION_MS = 30000;
const COUNTDOWN_MS = 3000;
const RECONNECT_GRACE_MS = 15000;
const ENDED_ROOM_TTL_MS = 5 * 60 * 1000;
const WAITING_ROOM_TTL_MS = 10 * 60 * 1000;
const LEADERBOARD_MAX = 100;
const LEADERBOARD_EMIT_MAX = 10;

// ------------------------------------------------------------------
// Store
// ------------------------------------------------------------------
const Store = {
  players: new Map(),          // socketId -> player
  usersByUserId: new Map(),    // userId -> player (latest socket)
  chatHistory: [],
  queues: { swipe: [], chess: [] },
  rooms: new Map(),            // roomId -> room
  socketToRoom: new Map(),     // socketId -> roomId
  leaderboard: {
    swipe: { today: [], week: [], all: [] },
    chess: { today: [], week: [], all: [] },
  },
};

// ------------------------------------------------------------------
// Utils
// ------------------------------------------------------------------
const CONTROL_CHARS = /[\u0000-\u001F\u007F-\u009F]/g;
const sanitizeText = (txt, max) =>
  String(txt ?? '').replace(CONTROL_CHARS, '').trim().substring(0, max);

const BAD_WORDS = ['fuck', 'shit', 'bitch', 'dick', 'cunt', 'bastard'];
const BAD_RE = new RegExp(`\\b(${BAD_WORDS.join('|')})\\b`, 'gi');
const filterProfanity = (text) => text.replace(BAD_RE, '***');

const generateRoomCode = () => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (let attempt = 0; attempt < 50; attempt++) {
    const code = Array.from({ length: 4 }, () =>
      chars[Math.floor(Math.random() * chars.length)]
    ).join('');
    if (!Store.rooms.has(code)) return code;
  }
  return Date.now().toString(36).toUpperCase().slice(-6);
};

const getRoomForSocket = (socketId) => {
  const roomId = Store.socketToRoom.get(socketId);
  return roomId ? Store.rooms.get(roomId) : null;
};

const bindSocketToRoom = (socket, roomId) => {
  Store.socketToRoom.set(socket.id, roomId);
  socket.join(roomId);
};

const unbindSocketFromRoom = (socketId) => {
  Store.socketToRoom.delete(socketId);
};

// ------------------------------------------------------------------
// Player broadcast (debounced)
// ------------------------------------------------------------------
const broadcastPlayers = () => {
  const active = Array.from(Store.players.values()).map(p => ({
    id: p.userId,
    name: p.name,
    avatar: p.avatar,
    avatarType: p.avatarType,
    gender: p.gender,
    status: p.status,
  }));
  io.emit('players:update', active);
};

let playerBroadcastTimer = null;
const triggerPlayerUpdate = () => {
  if (playerBroadcastTimer) return;
  playerBroadcastTimer = setTimeout(() => {
    playerBroadcastTimer = null;
    broadcastPlayers();
  }, 500);
};

// ------------------------------------------------------------------
// Room cleanup
// ------------------------------------------------------------------
const clearRoomTimers = (room) => {
  if (!room) return;
  if (room.endTimer) {
    clearInterval(room.endTimer);
    clearTimeout(room.endTimer);
    room.endTimer = null;
  }
  if (room.disconnectTimers) {
    for (const t of room.disconnectTimers.values()) clearTimeout(t);
    room.disconnectTimers.clear();
  }
};

const destroyRoom = (roomId) => {
  const room = Store.rooms.get(roomId);
  if (!room) return;
  clearRoomTimers(room);
  for (const p of room.players) {
    if (Store.socketToRoom.get(p.socketId) === roomId) {
      Store.socketToRoom.delete(p.socketId);
    }
  }
  Store.rooms.delete(roomId);
};

// ------------------------------------------------------------------
// Leaderboard
// ------------------------------------------------------------------
const trimLB = (lb) => ({
  today: lb.today.slice(0, LEADERBOARD_EMIT_MAX),
  week: lb.week.slice(0, LEADERBOARD_EMIT_MAX),
  all: lb.all.slice(0, LEADERBOARD_EMIT_MAX),
});

const pushLeaderboardEntry = (gameId, entry) => {
  const lb = Store.leaderboard[gameId];
  ['today', 'week', 'all'].forEach(k => {
    lb[k].push(entry);
    lb[k].sort((a, b) => b.score - a.score || a.time - b.time);
    if (lb[k].length > LEADERBOARD_MAX) lb[k].length = LEADERBOARD_MAX;
  });
};

const updateLeaderboard = (gameId, room, outcome) => {
  const { winnerId } = outcome;
  const isDraw = winnerId === null;

  let updated = false;
  room.players.forEach(p => {
    const liveUser = Store.usersByUserId.get(p.userId);
    if (!liveUser || liveUser.isGuest) return;

    let sc = 0;
    if (gameId === 'chess') {
      if (isDraw) sc = 0;
      else if (p.userId === winnerId) sc = 10;
    } else {
      sc = p.score || 0;
    }
    if (sc <= 0) return;

    pushLeaderboardEntry(gameId, {
      name: liveUser.name,
      avatar: liveUser.avatar,
      avatarType: liveUser.avatarType,
      gender: liveUser.gender,
      score: sc,
      time: Date.now(),
    });
    updated = true;
  });

  if (updated) {
    io.emit('leaderboard:update', {
      swipe: trimLB(Store.leaderboard.swipe),
      chess: trimLB(Store.leaderboard.chess),
    });
  }
};

// ------------------------------------------------------------------
// Match enders
// ------------------------------------------------------------------
const scheduleRoomDestroy = (roomId) => {
  const room = Store.rooms.get(roomId);
  if (!room) return;
  room.destroyAt = Date.now() + ENDED_ROOM_TTL_MS;
};

const endSwipeMatch = (room, reason = 'timeout') => {
  if (room.status === 'ended') return;
  room.status = 'ended';
  clearRoomTimers(room);

  const results = room.players.map(p => ({
    userId: p.userId,
    score: p.score || 0,
    bestCombo: p.bestCombo || 0,
    hits: p.hits || 0,
    misses: p.misses || 0,
    avgReaction: p.reactions && p.reactions.length
      ? Math.round(p.reactions.reduce((a, b) => a + b, 0) / p.reactions.length)
      : 0,
  }));

  let winnerId = null;
  if (results.length === 2 && results[0].score !== results[1].score) {
    winnerId = results[0].score > results[1].score
      ? results[0].userId
      : results[1].userId;
  }

  io.to(room.id).emit('match:end', { results, winnerId, reason });
  updateLeaderboard('swipe', room, { winnerId, isDraw: winnerId === null, reason });
  scheduleRoomDestroy(room.id);
};

const endChessMatch = (room, winnerColor, reason) => {
  if (room.status === 'ended') return;
  room.status = 'ended';
  clearRoomTimers(room);

  let winnerId = null;
  if (winnerColor) {
    const winner = room.players.find(p => p.color === winnerColor);
    if (winner) winnerId = winner.userId;
  }

  const results = room.players.map(p => ({
    userId: p.userId,
    reason,
  }));

  io.to(room.id).emit('match:end', { results, winnerId, reason });
  updateLeaderboard('chess', room, { winnerId, isDraw: winnerId === null, reason });
  scheduleRoomDestroy(room.id);
};

// ------------------------------------------------------------------
// HTTP
// ------------------------------------------------------------------
app.get('/health', (req, res) =>
  res.json({
    ok: true,
    players: Store.players.size,
    rooms: Store.rooms.size,
    queues: { swipe: Store.queues.swipe.length, chess: Store.queues.chess.length },
  })
);

// ------------------------------------------------------------------
// Socket handling
// ------------------------------------------------------------------
io.on('connection', (socket) => {
  const rateLimits = new Map();
  const checkRateLimit = (action, limitMs, maxHits = 1) => {
    const now = Date.now();
    const history = (rateLimits.get(action) || []).filter(t => now - t < limitMs);
    if (history.length >= maxHits) return false;
    history.push(now);
    rateLimits.set(action, history);
    return true;
  };

  // ---------------------------------------------------------------
  // hello / identity
  // ---------------------------------------------------------------
  socket.on('hello', (profile) => {
    try {
      if (!profile || typeof profile !== 'object') return;
      let { id, name, avatar, avatarType, gender, isGuest } = profile;
      name = sanitizeText(name, 14);
      if (name.length < 2) name = 'Player';
      if (!['male', 'female', 'none'].includes(gender)) gender = 'none';
      if (avatarType === 'photo' && avatar && String(avatar).length > 40000) {
        avatarType = 'emoji';
        avatar = '🤖';
      }

      const userId = String(id || socket.id);

      // If this user already had an active session on another socket, migrate.
      const existing = Store.usersByUserId.get(userId);
      if (existing && existing.socketId !== socket.id) {
        const oldSocketId = existing.socketId;
        Store.players.delete(oldSocketId);
        unbindSocketFromRoom(oldSocketId);
      }

      const player = {
        socketId: socket.id,
        userId,
        name,
        avatar: sanitizeText(avatar, 40000),
        avatarType,
        gender,
        isGuest: !!isGuest,
        status: 'online',
        lastPing: Date.now(),
      };

      Store.players.set(socket.id, player);
      Store.usersByUserId.set(userId, player);

      // stale queue cleanup for this userId
      Object.values(Store.queues).forEach(q => {
        for (let i = q.length - 1; i >= 0; i--) {
          if (q[i].userId === userId && q[i].socketId !== socket.id) q.splice(i, 1);
        }
      });

      socket.emit('chat:history', Store.chatHistory);
      socket.emit('leaderboard:update', {
        swipe: trimLB(Store.leaderboard.swipe),
        chess: trimLB(Store.leaderboard.chess),
      });
      triggerPlayerUpdate();
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Chat
  // ---------------------------------------------------------------
  socket.on('chat:send', (text) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      if (
        !checkRateLimit('chat_fast', 1000, 1) ||
        !checkRateLimit('chat_burst', 30000, 10)
      ) {
        return socket.emit('error:toast', 'Typing too fast.');
      }
      const cleanText = filterProfanity(sanitizeText(text, 200));
      if (!cleanText) return;

      const msg = {
        id: Math.random().toString(36).slice(2, 11),
        user: {
          id: player.userId,
          name: player.name,
          avatar: player.avatar,
          avatarType: player.avatarType,
          gender: player.gender,
        },
        text: cleanText,
        time: Date.now(),
      };
      Store.chatHistory.push(msg);
      if (Store.chatHistory.length > 50) Store.chatHistory.shift();
      io.emit('chat:message', msg);
    } catch (e) {}
  });

  socket.on('game:chat', (text) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      if (!checkRateLimit('in_game_chat', 1500, 1)) return;
      const cleanText = filterProfanity(sanitizeText(text, 150));
      if (!cleanText) return;

      const room = getRoomForSocket(socket.id);
      if (room && (room.status === 'playing' || room.status === 'ended')) {
        io.to(room.id).emit('game:chat_receive', {
          senderId: player.userId,
          name: player.name,
          text: cleanText,
        });
      }
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Rooms
  // ---------------------------------------------------------------
  socket.on('room:create', (data) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      if (!checkRateLimit('room_create', 2000)) return;

      const gameId = data && data.gameId === 'chess' ? 'chess' : 'swipe';
      const code = generateRoomCode();
      Store.rooms.set(code, {
        id: code,
        type: 'friend',
        gameId,
        players: [],
        status: 'waiting',
        createdAt: Date.now(),
      });
      socket.emit('room:created', code);
    } catch (e) {}
  });

  socket.on('room:join', (code) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      const cleanCode = sanitizeText(code, 6).toUpperCase();
      const room = Store.rooms.get(cleanCode);
      if (!room) return socket.emit('error:toast', 'Room not found.');
      if (room.status !== 'waiting' || room.players.length >= 2) {
        return socket.emit('error:toast', 'Room is full or started.');
      }
      if (room.players.some(p => p.userId === player.userId)) return;
      socket.emit('match:found', { room: room.id, gameId: room.gameId });
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Matchmaking
  // ---------------------------------------------------------------
  socket.on('match:find', ({ gameId } = {}) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player || !Store.queues[gameId]) return;
      const q = Store.queues[gameId];

      if (q.some(p => p.userId === player.userId)) return;

      for (let i = q.length - 1; i >= 0; i--) {
        if (q[i].userId === player.userId) q.splice(i, 1);
      }

      const opponentIdx = q.findIndex(p => p.userId !== player.userId);
      if (opponentIdx !== -1) {
        const opponent = q.splice(opponentIdx, 1)[0];
        const code = generateRoomCode();
        Store.rooms.set(code, {
          id: code,
          type: 'random',
          gameId,
          players: [],
          status: 'waiting',
          createdAt: Date.now(),
        });
        socket.emit('match:found', { room: code, gameId });
        io.to(opponent.socketId).emit('match:found', { room: code, gameId });
      } else {
        q.push({ socketId: socket.id, userId: player.userId });
      }
    } catch (e) {}
  });

  socket.on('match:cancel', () => {
    try {
      Object.values(Store.queues).forEach(q => {
        for (let i = q.length - 1; i >= 0; i--) {
          if (q[i].socketId === socket.id) q.splice(i, 1);
        }
      });
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Invites
  // ---------------------------------------------------------------
  socket.on('invite:send', ({ toUserId, gameId } = {}) => {
    try {
      const p1 = Store.players.get(socket.id);
      if (!p1 || p1.userId === toUserId) return;
      if (!checkRateLimit('invite_send', 5000)) {
        return socket.emit('error:toast', 'Please wait before inviting.');
      }
      const p2 = Store.usersByUserId.get(String(toUserId));
      if (!p2) return socket.emit('error:toast', 'Player is offline.');
      if (p2.status === 'in-game') return socket.emit('error:toast', 'Player is busy.');
      io.to(p2.socketId).emit('invite:receive', {
        from: {
          id: p1.userId,
          name: p1.name,
          avatar: p1.avatar,
          avatarType: p1.avatarType,
          gender: p1.gender,
        },
        gameId: gameId === 'chess' ? 'chess' : 'swipe',
        inviteId: Math.random().toString(36).slice(2, 11),
      });
      socket.emit('toast', 'Invite sent!');
    } catch (e) {}
  });

  socket.on('invite:accept', ({ fromUserId, gameId } = {}) => {
    try {
      if (!checkRateLimit('invite_accept', 3000, 1)) return;
      const p1 = Store.usersByUserId.get(String(fromUserId));
      if (!p1) return socket.emit('error:toast', 'Player is no longer online.');
      if (p1.status === 'in-game') return socket.emit('error:toast', 'Player is busy.');
      const code = generateRoomCode();
      Store.rooms.set(code, {
        id: code,
        type: 'friend',
        gameId: gameId === 'chess' ? 'chess' : 'swipe',
        players: [],
        status: 'waiting',
        createdAt: Date.now(),
      });
      socket.emit('match:found', { room: code, gameId });
      io.to(p1.socketId).emit('match:found', { room: code, gameId });
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Game ready (join seat + start + reconnect)
  // ---------------------------------------------------------------
  socket.on('game:ready', ({ roomId } = {}) => {
    try {
      const player = Store.players.get(socket.id);
      if (!player) return;
      const room = Store.rooms.get(String(roomId || '').toUpperCase());
      if (!room) return;

      // ---- RECONNECT path ----
      if (room.status === 'playing' || room.status === 'ended') {
        const existing = room.players.find(p => p.userId === player.userId);
        if (!existing) return;
        existing.socketId = socket.id;
        player.status = 'in-game';
        triggerPlayerUpdate();
        bindSocketToRoom(socket, room.id);

        if (room.disconnectTimers && room.disconnectTimers.has(player.userId)) {
          clearTimeout(room.disconnectTimers.get(player.userId));
          room.disconnectTimers.delete(player.userId);
        }

        if (room.gameId === 'chess' && room.chess) {
          socket.emit('match:start', {
            startAt: Date.now(),
            profiles: room.players.map(p => ({ userId: p.userId, profile: p.profile, color: p.color })),
            serverNow: Date.now(),
            gameId: 'chess',
            fen: room.chess.fen(),
            clocks: room.clocks,
            resumed: true,
          });
        } else if (room.gameId === 'swipe') {
          socket.emit('match:start', {
            startAt: Math.max(Date.now(), room.endTime - SWIPE_DURATION_MS),
            profiles: room.players.map(p => ({ userId: p.userId, profile: p.profile })),
            serverNow: Date.now(),
            gameId: 'swipe',
            resumed: true,
          });
        }
        return;
      }

      // ---- FRESH join ----
      if (room.status !== 'waiting') return;

      player.status = 'in-game';
      triggerPlayerUpdate();
      bindSocketToRoom(socket, room.id);

      let rPlayer = room.players.find(p => p.userId === player.userId);
      if (!rPlayer) {
        if (room.players.length >= 2) {
          unbindSocketFromRoom(socket.id);
          return;
        }
        rPlayer = {
          socketId: socket.id,
          userId: player.userId,
          ready: true,
          profile: {
            name: player.name,
            avatar: player.avatar,
            avatarType: player.avatarType,
            gender: player.gender,
          },
          rematchVote: false,
        };
        room.players.push(rPlayer);
      } else {
        rPlayer.socketId = socket.id;
        rPlayer.ready = true;
      }

      if (room.players.length === 2 && room.players.every(p => p.ready)) {
        startRoom(room);
      }
    } catch (e) {
      console.error('game:ready error', e);
    }
  });

  // ---------------------------------------------------------------
  // Swipe input
  // ---------------------------------------------------------------
  socket.on('game:swipe', ({ dir } = {}) => {
    try {
      const room = getRoomForSocket(socket.id);
      if (!room || room.status !== 'playing' || room.gameId !== 'swipe') return;
      const pState = room.players.find(p => p.socketId === socket.id);
      if (!pState) return;
      const now = Date.now();
      if (now < pState.lockUntil || now < pState.targetIssuedAt + 60 || now > room.endTime) return;

      const DIRS = ['UP', 'DOWN', 'LEFT', 'RIGHT'];
      if (dir === pState.target) {
        const rt = now - pState.targetIssuedAt;
        pState.reactions.push(rt);
        pState.hits++;
        pState.combo++;
        pState.bestCombo = Math.max(pState.bestCombo, pState.combo);
        pState.score +=
          10 +
          (Math.min(Math.max(0, pState.combo - 1), 10) * 2) +
          (rt < 400 ? 4 : rt < 700 ? 2 : 0);
        pState.target = DIRS[Math.floor(Math.random() * 4)];
        pState.targetIssuedAt = now;
        io.to(pState.socketId).emit('game:target', { dir: pState.target });
      } else {
        pState.misses++;
        pState.combo = 0;
        pState.score = Math.max(0, pState.score - 5);
        pState.lockUntil = now + 250;
      }
      io.to(room.id).emit('game:state', room.players.map(p => ({
        userId: p.userId,
        score: p.score,
        combo: p.combo,
      })));
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Chess move
  // ---------------------------------------------------------------
  socket.on('chess:move', (move) => {
    try {
      if (!move || typeof move !== 'object') return;
      const room = getRoomForSocket(socket.id);
      if (!room || room.status !== 'playing' || room.gameId !== 'chess') return;
      const player = room.players.find(p => p.socketId === socket.id);
      if (!player || !room.chess || player.color !== room.chess.turn()) return;

      const safeMove = {
        from: sanitizeText(move.from, 2),
        to: sanitizeText(move.to, 2),
      };
      if (move.promotion) safeMove.promotion = sanitizeText(move.promotion, 1);

      let moveRes;
      try {
        moveRes = room.chess.move(safeMove);
      } catch (e) {
        return;
      }
      if (!moveRes) return;

      const now = Date.now();
      const elapsed = Math.max(0, now - room.lastMoveTime);
      room.clocks[player.color] = Math.max(0, room.clocks[player.color] - elapsed);
      room.lastMoveTime = now;

      io.to(room.id).emit('chess:state', {
        fen: room.chess.fen(),
        clocks: room.clocks,
        lastMove: { from: moveRes.from, to: moveRes.to },
        turn: room.chess.turn(),
        lastMoveTime: now,
      });

      if (room.chess.in_checkmate()) {
        endChessMatch(room, player.color, 'checkmate');
      } else if (
        room.chess.in_draw() ||
        room.chess.in_stalemate() ||
        room.chess.in_threefold_repetition() ||
        room.chess.insufficient_material()
      ) {
        endChessMatch(room, null, 'draw');
      }
    } catch (e) {
      console.error('chess:move error', e);
    }
  });

  socket.on('chess:resign', () => {
    try {
      const room = getRoomForSocket(socket.id);
      if (!room || room.status !== 'playing' || room.gameId !== 'chess') return;
      const player = room.players.find(p => p.socketId === socket.id);
      if (!player) return;
      endChessMatch(room, player.color === 'w' ? 'b' : 'w', 'resign');
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Rematch
  // ---------------------------------------------------------------
  socket.on('match:rematch', () => {
    try {
      if (!checkRateLimit('rematch', 1000, 1)) return;
      const room = getRoomForSocket(socket.id);
      if (!room || room.status !== 'ended') return;
      const pState = room.players.find(p => p.socketId === socket.id);
      if (!pState) return;
      pState.rematchVote = true;

      if (room.players.every(p => p.rematchVote)) {
        room.status = 'waiting';
        room.players.forEach(p => {
          p.ready = false;
          p.rematchVote = false;
        });
        delete room.chess;
        room.clocks = { w: CHESS_START_MS, b: CHESS_START_MS };
        room.lastMoveTime = null;
        room.endTime = null;
        room.endTimer = null;
        room.destroyAt = null;
        clearRoomTimers(room);
        io.to(room.id).emit('match:rematch_start');
      }
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Disconnect
  // ---------------------------------------------------------------
  socket.on('disconnect', () => {
    try {
      Object.values(Store.queues).forEach(q => {
        for (let i = q.length - 1; i >= 0; i--) {
          if (q[i].socketId === socket.id) q.splice(i, 1);
        }
      });

      const player = Store.players.get(socket.id);
      const room = getRoomForSocket(socket.id);

      if (room && player) {
        const rPlayer = room.players.find(p => p.userId === player.userId);
        if (rPlayer) {
          if (room.status === 'waiting') {
            room.players = room.players.filter(p => p.userId !== player.userId);
            if (room.players.length === 0) destroyRoom(room.id);
          } else if (room.status === 'playing') {
            if (!room.disconnectTimers) room.disconnectTimers = new Map();
            const timer = setTimeout(() => {
              const still = room.players.find(p => p.userId === player.userId);
              if (still && room.status === 'playing') {
                io.to(room.id).emit('error:toast', 'Opponent disconnected. Match forfeited.');
                if (room.gameId === 'chess') {
                  const opp = room.players.find(p => p.userId !== player.userId);
                  endChessMatch(room, opp ? opp.color : null, 'disconnect');
                } else {
                  endSwipeMatch(room, 'disconnect');
                }
              }
              room.disconnectTimers.delete(player.userId);
            }, RECONNECT_GRACE_MS);
            room.disconnectTimers.set(player.userId, timer);
          } else if (room.status === 'ended') {
            io.to(room.id).emit('error:toast', 'Opponent left the room.');
          }
        }
      }

      unbindSocketFromRoom(socket.id);
      Store.players.delete(socket.id);
      if (player && Store.usersByUserId.get(player.userId)?.socketId === socket.id) {
        Store.usersByUserId.delete(player.userId);
      }
      triggerPlayerUpdate();
    } catch (e) {}
  });

  // ---------------------------------------------------------------
  // Start a room
  // ---------------------------------------------------------------
  function startRoom(room) {
    room.status = 'playing';
    const startAt = Date.now() + COUNTDOWN_MS;
    const profiles = room.players.map(p => ({
      userId: p.userId,
      profile: p.profile,
      color: p.color,
    }));

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

      room.clocks = room.clocks || { w: CHESS_START_MS, b: CHESS_START_MS };
      room.lastMoveTime = startAt;

      io.to(room.id).emit('match:start', {
        startAt,
        profiles,
        serverNow: Date.now(),
        gameId: 'chess',
        fen: room.chess.fen(),
        clocks: room.clocks,
      });

      clearRoomTimers(room);
      room.endTimer = setInterval(() => {
        if (room.status !== 'playing') {
          clearRoomTimers(room);
          return;
        }
        const now = Date.now();
        if (now < startAt) return;
        const elapsed = now - room.lastMoveTime;
        const turnColor = room.chess.turn();
        if (room.clocks[turnColor] - elapsed <= 0) {
          room.clocks[turnColor] = 0;
          endChessMatch(room, turnColor === 'w' ? 'b' : 'w', 'timeout');
        }
      }, 1000);
    } else {
      const DIRS = ['UP', 'DOWN', 'LEFT', 'RIGHT'];
      room.endTime = startAt + SWIPE_DURATION_MS;
      room.players.forEach(p => {
        p.score = 0;
        p.combo = 0;
        p.bestCombo = 0;
        p.hits = 0;
        p.misses = 0;
        p.reactions = [];
        p.target = DIRS[Math.floor(Math.random() * 4)];
        p.targetIssuedAt = startAt;
        p.lockUntil = 0;
      });

      io.to(room.id).emit('match:start', {
        startAt,
        profiles,
        serverNow: Date.now(),
        gameId: 'swipe',
      });

      room.players.forEach(p =>
        io.to(p.socketId).emit('game:target', { dir: p.target })
      );

      clearRoomTimers(room);
      room.endTimer = setTimeout(
        () => endSwipeMatch(room, 'time'),
        SWIPE_DURATION_MS + COUNTDOWN_MS + 500
      );
    }
  }
});

// ------------------------------------------------------------------
// Periodic sweep
// ------------------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of Store.rooms.entries()) {
    if (room.status === 'waiting' && now - room.createdAt > WAITING_ROOM_TTL_MS) {
      destroyRoom(id);
    } else if (room.status === 'ended') {
      const destroyAt = room.destroyAt || (now + ENDED_ROOM_TTL_MS);
      if (now >= destroyAt) destroyRoom(id);
    }
  }
}, 60 * 1000);

// ------------------------------------------------------------------
// Start
// ------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Swiplay Server running on port ${PORT}`));
