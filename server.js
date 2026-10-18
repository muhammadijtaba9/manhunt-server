// Manhunt realtime server: rooms, roles, GPS rules, tagging, jail, timer, win logic.
// All state lives in memory (rooms are deleted when a match ends or everyone leaves).
const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { distance } = require("./geo");

const PORT = process.env.PORT || 3000;

// ---- Game constants ----
const TAG_RANGE_M = 12;            // hunter must be this close to tag
const JAIL_RANGE_M = 15;           // runner must be this close to jail to free prisoners
const OUTSIDE_LIMIT_S = 20;        // seconds outside the area before auto-caught
const HUNTER_PING_MS = 30000;      // hunters see runner positions only this often
const RELEASE_COOLDOWN_MS = 20000; // per-runner cooldown on jailbreaks
const IMMUNITY_MS = 10000;         // freed runners can't be tagged for a moment
const POS_MAX_AGE_MS = 15000;      // ignore stale positions for tagging
const MAX_SPEED_MPS = 12;          // anti-cheat: ignore impossible jumps
const MAX_PLAYERS = 20;
const END_ROOM_TTL_MS = 15 * 60 * 1000;

const app = express();
app.use(cors());
app.get("/", (req, res) => res.json({ ok: true, game: "manhunt", rooms: rooms.size }));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const rooms = new Map();      // code -> room
const socketRoom = new Map(); // socket.id -> code

// ---- Helpers ----
const reply = (cb, obj) => typeof cb === "function" && cb(obj);
const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, Number(n) || lo));
const validPos = (p) =>
  p && Number.isFinite(p.lat) && Number.isFinite(p.lng) &&
  Math.abs(p.lat) <= 90 && Math.abs(p.lng) <= 180;

function genCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code;
  do {
    code = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
  } while (rooms.has(code));
  return code;
}

function log(room, text) {
  room.events.push({ t: Date.now(), text });
  if (room.events.length > 30) room.events.shift();
}

function newPlayer(id, name, role = "runner") {
  return {
    id, name, role, connected: true, pos: null,
    jailed: false, outsideSince: null, lastRelease: 0, immuneUntil: 0,
  };
}

function connectedPlayers(room) {
  return [...room.players.values()].filter((p) => p.connected);
}

function timeRef(room) {
  return room.paused ? room.pausedAt : Date.now();
}

function phaseOf(room) {
  const ref = timeRef(room);
  return ref < room.startedAt + room.settings.headStartSec * 1000 ? "headstart" : "hunt";
}

// ---- State sent to each player (position visibility rules live here) ----
function stateFor(room, viewer) {
  const playing = room.status === "playing";
  const ref = timeRef(room);
  const players = [];

  for (const p of room.players.values()) {
    let pos = null;
    if (room.status !== "lobby" && p.connected) {
      if (p.id === viewer.id) pos = p.pos;
      else if (p.jailed) pos = room.area.jail;                 // prisoners are at the jail
      else if (p.role === "hunter") pos = p.pos;               // hunters are always visible (live)
      else if (viewer.role === "runner") pos = p.pos;          // runners see teammates live
      else pos = room.snapshot[p.id] || null;                  // hunters see runners only on ping
    }
    if (room.status === "ended") pos = p.pos;                  // reveal everyone at the end
    players.push({
      id: p.id, name: p.name, role: p.role, jailed: p.jailed,
      connected: p.connected, immune: p.immuneUntil > Date.now(),
      pos: pos ? { lat: pos.lat, lng: pos.lng } : null,
    });
  }

  const out = {
    code: room.code,
    status: room.status,
    hostId: room.hostId,
    settings: room.settings,
    area: room.area,
    players,
    events: room.events.slice(-8),
    winner: room.winner,
    reason: room.reason,
    paused: room.paused,
    pausedBy: room.paused ? room.players.get(room.pausedBy)?.name || "someone" : null,
    you: {
      id: viewer.id,
      role: viewer.role,
      jailed: viewer.jailed,
      outsideFor: viewer.outsideSince ? Math.floor((Date.now() - viewer.outsideSince) / 1000) : 0,
      outsideLimit: OUTSIDE_LIMIT_S,
    },
    rules: { TAG_RANGE_M, JAIL_RANGE_M, HUNTER_PING_MS },
  };

  if (playing) {
    out.phase = phaseOf(room);
    out.remainingMs = Math.max(0, room.endsAt - ref);
    out.headStartMs = Math.max(0, room.startedAt + room.settings.headStartSec * 1000 - ref);
    out.pingInMs = Math.max(0, room.lastPing + HUNTER_PING_MS - ref);
  }
  return out;
}

function broadcast(room) {
  for (const p of room.players.values()) {
    if (p.connected) io.to(p.id).emit("state", stateFor(room, p));
  }
}

function deleteRoom(room) {
  clearTimeout(room.ttl);
  for (const p of room.players.values()) socketRoom.delete(p.id);
  rooms.delete(room.code);
}

function endGame(room, winner, reason) {
  if (room.status !== "playing") return;
  room.status = "ended";
  room.winner = winner;
  room.reason = reason;
  room.paused = false;
  log(room, `Game over: ${winner} win. ${reason}`);
  room.ttl = setTimeout(() => deleteRoom(room), END_ROOM_TTL_MS);
}

function checkWin(room) {
  if (room.status !== "playing") return;
  const hunters = connectedPlayers(room).filter((p) => p.role === "hunter");
  const runners = connectedPlayers(room).filter((p) => p.role === "runner");
  if (!hunters.length) return endGame(room, "runners", "All hunters left.");
  if (!runners.length) return endGame(room, "hunters", "No runners left.");
  if (runners.every((r) => r.jailed)) endGame(room, "hunters", "Every runner was caught.");
}

function removePlayer(room, id) {
  const p = room.players.get(id);
  if (!p) return;
  socketRoom.delete(id);
  io.sockets.sockets.get(id)?.leave(room.code);

  if (room.status === "lobby") room.players.delete(id);
  else { p.connected = false; log(room, `${p.name} left the game.`); }

  if (!connectedPlayers(room).length) return deleteRoom(room);
  if (room.hostId === id) room.hostId = connectedPlayers(room)[0].id;
  if (room.status === "playing") checkWin(room);
  broadcast(room);
}

function ctx(socket) {
  const room = rooms.get(socketRoom.get(socket.id));
  const me = room?.players.get(socket.id);
  return room && me ? { room, me } : null;
}

function joinSocket(socket, room, me) {
  socket.join(room.code);
  socketRoom.set(socket.id, room.code);
  room.players.set(socket.id, me);
}

function leaveCurrent(socket) {
  const c = ctx(socket);
  if (c) removePlayer(c.room, socket.id);
}

// ---- Socket events ----
io.on("connection", (socket) => {
  const cleanName = (n) => String(n || "").trim().slice(0, 16) || "Player";

  socket.on("create_room", ({ name } = {}, cb) => {
    leaveCurrent(socket);
    const code = genCode();
    const room = {
      code, hostId: socket.id, status: "lobby",
      players: new Map(), snapshot: {}, events: [],
      settings: { radius: 200, durationMin: 20, headStartSec: 60 },
      area: null, startedAt: 0, endsAt: 0, lastPing: 0,
      paused: false, pausedAt: 0, pausedBy: null,
      winner: null, reason: null, ttl: null,
    };
    rooms.set(code, room);
    joinSocket(socket, room, newPlayer(socket.id, cleanName(name)));
    reply(cb, { ok: true, code });
    broadcast(room);
  });

  socket.on("join_room", ({ code, name } = {}, cb) => {
    const room = rooms.get(String(code || "").trim().toUpperCase());
    if (!room) return reply(cb, { ok: false, error: "Room not found." });
    if (room.status !== "lobby") return reply(cb, { ok: false, error: "That game already started." });
    if (room.players.size >= MAX_PLAYERS) return reply(cb, { ok: false, error: "Room is full." });
    leaveCurrent(socket);
    const me = newPlayer(socket.id, cleanName(name));
    joinSocket(socket, room, me);
    log(room, `${me.name} joined.`);
    reply(cb, { ok: true, code: room.code });
    broadcast(room);
  });

  socket.on("set_role", ({ role } = {}, cb) => {
    const c = ctx(socket);
    if (!c || c.room.status !== "lobby") return reply(cb, { ok: false });
    if (role !== "runner" && role !== "hunter") return reply(cb, { ok: false });
    c.me.role = role;
    reply(cb, { ok: true });
    broadcast(c.room);
  });

  socket.on("settings", (s = {}, cb) => {
    const c = ctx(socket);
    if (!c || c.room.status !== "lobby" || c.room.hostId !== socket.id)
      return reply(cb, { ok: false, error: "Only the host can change settings." });
    c.room.settings = {
      radius: clamp(s.radius ?? c.room.settings.radius, 50, 2000),
      durationMin: clamp(s.durationMin ?? c.room.settings.durationMin, 5, 120),
      headStartSec: clamp(s.headStartSec ?? c.room.settings.headStartSec, 0, 300),
    };
    reply(cb, { ok: true });
    broadcast(c.room);
  });

  socket.on("start_game", ({ center } = {}, cb) => {
    const c = ctx(socket);
    if (!c || c.room.hostId !== socket.id || c.room.status !== "lobby")
      return reply(cb, { ok: false, error: "Only the host can start." });
    const { room } = c;
    const all = connectedPlayers(room);
    if (!all.some((p) => p.role === "hunter") || !all.some((p) => p.role === "runner"))
      return reply(cb, { ok: false, error: "Need at least 1 hunter and 1 runner." });
    if (!validPos(center))
      return reply(cb, { ok: false, error: "Host GPS position not available yet." });

    const now = Date.now();
    room.area = { center: { lat: center.lat, lng: center.lng }, jail: { lat: center.lat, lng: center.lng }, radius: room.settings.radius };
    for (const p of room.players.values()) {
      p.jailed = false; p.outsideSince = null; p.immuneUntil = 0; p.lastRelease = 0;
    }
    room.snapshot = {};
    room.status = "playing";
    room.startedAt = now;
    room.endsAt = now + room.settings.durationMin * 60000;
    room.lastPing = 0;
    room.paused = false;
    room.winner = null; room.reason = null;
    log(room, "Game started! Runners, hide!");
    reply(cb, { ok: true });
    broadcast(room);
  });

  socket.on("location", (p) => {
    const c = ctx(socket);
    if (!c || !validPos(p)) return;
    const { me } = c;
    const now = Date.now();
    // Anti-cheat: reject teleporting (mock locations / bad GPS jumps)
    if (me.pos && now - me.pos.t < 30000) {
      const dt = Math.max((now - me.pos.t) / 1000, 0.5);
      if (distance(me.pos, p) / dt > MAX_SPEED_MPS) return;
    }
    me.pos = { lat: p.lat, lng: p.lng, acc: Number(p.accuracy) || 0, t: now };
  });

  socket.on("tag", (_, cb) => {
    const c = ctx(socket);
    if (!c) return reply(cb, { ok: false, error: "Not in a game." });
    const { room, me } = c;
    if (room.status !== "playing") return reply(cb, { ok: false, error: "Game is not running." });
    if (room.paused) return reply(cb, { ok: false, error: "Game is paused." });
    if (phaseOf(room) === "headstart") return reply(cb, { ok: false, error: "Head start: runners are still hiding." });
    if (me.role !== "hunter") return reply(cb, { ok: false, error: "Only hunters can tag." });
    const now = Date.now();
    if (!me.pos || now - me.pos.t > POS_MAX_AGE_MS) return reply(cb, { ok: false, error: "Your GPS is not ready." });

    let best = null, bestD = Infinity;
    for (const r of room.players.values()) {
      if (r.role !== "runner" || r.jailed || !r.connected || r.immuneUntil > now) continue;
      if (!r.pos || now - r.pos.t > POS_MAX_AGE_MS) continue;
      const d = distance(me.pos, r.pos);
      // allow a little tolerance for GPS error
      const tol = Math.min(me.pos.acc || 0, 8) / 2 + Math.min(r.pos.acc || 0, 8) / 2;
      if (d <= TAG_RANGE_M + tol && d < bestD) { best = r; bestD = d; }
    }
    if (!best) return reply(cb, { ok: false, error: `No runner within ${TAG_RANGE_M} m.` });

    best.jailed = true;
    best.outsideSince = null;
    log(room, `${me.name} caught ${best.name}!`);
    io.to(best.id).emit("alert", { type: "tagged", text: `${me.name} caught you! Head to jail and wait for a teammate.` });
    reply(cb, { ok: true, caught: best.name });
    checkWin(room);
    broadcast(room);
  });

  socket.on("release", (_, cb) => {
    const c = ctx(socket);
    if (!c) return reply(cb, { ok: false, error: "Not in a game." });
    const { room, me } = c;
    const now = Date.now();
    if (room.status !== "playing" || room.paused) return reply(cb, { ok: false, error: "Game is not running." });
    if (me.role !== "runner" || me.jailed) return reply(cb, { ok: false, error: "Only free runners can do that." });
    if (!me.pos) return reply(cb, { ok: false, error: "Your GPS is not ready." });
    if (distance(me.pos, room.area.jail) > JAIL_RANGE_M) return reply(cb, { ok: false, error: `Get within ${JAIL_RANGE_M} m of the jail.` });
    if (now - me.lastRelease < RELEASE_COOLDOWN_MS) return reply(cb, { ok: false, error: "Jailbreak is on cooldown." });
    const prisoners = [...room.players.values()].filter((p) => p.jailed && p.connected);
    if (!prisoners.length) return reply(cb, { ok: false, error: "Nobody is in jail." });

    me.lastRelease = now;
    for (const p of prisoners) {
      p.jailed = false;
      p.immuneUntil = now + IMMUNITY_MS;
      io.to(p.id).emit("alert", { type: "freed", text: `${me.name} freed you! Run!` });
    }
    log(room, `${me.name} broke ${prisoners.length} out of jail!`);
    reply(cb, { ok: true, freed: prisoners.length });
    broadcast(room);
  });

  socket.on("pause", (_, cb) => {
    const c = ctx(socket);
    if (!c || c.room.status !== "playing" || c.room.paused) return reply(cb, { ok: false });
    c.room.paused = true;
    c.room.pausedAt = Date.now();
    c.room.pausedBy = socket.id;
    log(c.room, `${c.me.name} paused the game.`);
    reply(cb, { ok: true });
    broadcast(c.room);
  });

  socket.on("resume", (_, cb) => {
    const c = ctx(socket);
    if (!c || !c.room.paused) return reply(cb, { ok: false });
    const { room } = c;
    if (socket.id !== room.pausedBy && socket.id !== room.hostId)
      return reply(cb, { ok: false, error: "Only the host or the person who paused can resume." });
    const delta = Date.now() - room.pausedAt;
    room.startedAt += delta; room.endsAt += delta; room.lastPing += delta;
    for (const p of room.players.values()) {
      if (p.outsideSince) p.outsideSince += delta;
      if (p.immuneUntil) p.immuneUntil += delta;
    }
    room.paused = false;
    log(room, `${c.me.name} resumed the game.`);
    reply(cb, { ok: true });
    broadcast(room);
  });

  socket.on("sos", (_, cb) => {
    const c = ctx(socket);
    if (!c) return reply(cb, { ok: false });
    log(c.room, `SOS from ${c.me.name}!`);
    for (const p of c.room.players.values()) {
      if (p.id !== socket.id && p.connected)
        io.to(p.id).emit("alert", { type: "sos", text: `SOS: ${c.me.name} needs help!` });
    }
    reply(cb, { ok: true });
    broadcast(c.room);
  });

  socket.on("back_to_lobby", (_, cb) => {
    const c = ctx(socket);
    if (!c || c.room.hostId !== socket.id || c.room.status !== "ended") return reply(cb, { ok: false });
    const { room } = c;
    clearTimeout(room.ttl);
    for (const p of [...room.players.values()]) {
      if (!p.connected) room.players.delete(p.id);
      else { p.jailed = false; p.pos = null; p.outsideSince = null; }
    }
    room.status = "lobby"; room.snapshot = {}; room.winner = null; room.reason = null; room.area = null;
    reply(cb, { ok: true });
    broadcast(room);
  });

  socket.on("leave_room", () => {
    leaveCurrent(socket);
    socket.emit("left");
  });

  socket.on("disconnect", () => leaveCurrent(socket));
});

// ---- Game loop: 1 tick per second ----
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (room.status !== "playing") continue;
    if (!room.paused) {
      if (phaseOf(room) === "hunt" && now - room.lastPing >= HUNTER_PING_MS) {
        room.snapshot = {};
        for (const p of room.players.values()) {
          if (p.role === "runner" && !p.jailed && p.connected && p.pos)
            room.snapshot[p.id] = { lat: p.pos.lat, lng: p.pos.lng };
        }
        room.lastPing = now;
      }
      // Geofence: runners outside too long are caught
      for (const p of room.players.values()) {
        if (!p.connected || !p.pos || p.jailed) continue;
        const outside = distance(p.pos, room.area.center) > room.area.radius;
        if (!outside) { p.outsideSince = null; continue; }
        if (!p.outsideSince) p.outsideSince = now;
        if (p.role === "runner" && now - p.outsideSince > OUTSIDE_LIMIT_S * 1000) {
          p.jailed = true; p.outsideSince = null;
          log(room, `${p.name} left the play area and was caught!`);
          io.to(p.id).emit("alert", { type: "tagged", text: "You left the play area and were caught!" });
        }
      }
      if (room.endsAt - now <= 0) endGame(room, "runners", "Time ran out. At least one runner escaped!");
      else checkWin(room);
    }
    broadcast(room);
  }
}, 1000);

server.listen(PORT, "0.0.0.0", () => console.log(`Manhunt server running on port ${PORT}`));
